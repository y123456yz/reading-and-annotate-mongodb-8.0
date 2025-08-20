/**
 *    Copyright (C) 2020-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/db/s/range_deletion_util.h"

#include <boost/move/utility_core.hpp>
#include <boost/optional/optional.hpp>
#include <string>
#include <utility>
#include <vector>

#include "mongo/base/status_with.h"
#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/admission/execution_admission_context.h"
#include "mongo/db/concurrency/exception_util.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/exec/delete_stage.h"
#include "mongo/db/keypattern.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/ops/write_ops_gen.h"
#include "mongo/db/query/explain_options.h"
#include "mongo/db/query/index_bounds.h"
#include "mongo/db/query/internal_plans.h"
#include "mongo/db/query/plan_executor.h"
#include "mongo/db/query/plan_explainer.h"
#include "mongo/db/query/plan_yield_policy.h"
#include "mongo/db/repl/read_concern_args.h"
#include "mongo/db/s/balancer_stats_registry.h"
#include "mongo/db/s/range_deleter_service.h"
#include "mongo/db/s/range_deletion_task_gen.h"
#include "mongo/db/s/shard_key_index_util.h"
#include "mongo/db/s/sharding_runtime_d_params_gen.h"
#include "mongo/db/s/sharding_statistics.h"
#include "mongo/db/s/sharding_util.h"
#include "mongo/db/shard_role.h"
#include "mongo/db/transaction_resources.h"
#include "mongo/logv2/log.h"
#include "mongo/logv2/log_attr.h"
#include "mongo/logv2/log_component.h"
#include "mongo/logv2/redaction.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/platform/compiler.h"
#include "mongo/rpc/get_status_from_command_result.h"
#include "mongo/rpc/reply_interface.h"
#include "mongo/rpc/unique_message.h"
#include "mongo/util/concurrency/admission_context.h"
#include "mongo/util/database_name_util.h"
#include "mongo/util/duration.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/namespace_string_util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kShardingRangeDeleter

namespace mongo {
namespace {

MONGO_FAIL_POINT_DEFINE(hangBeforeDoingDeletion);
MONGO_FAIL_POINT_DEFINE(hangAfterDoingDeletion);
MONGO_FAIL_POINT_DEFINE(suspendRangeDeletion);
MONGO_FAIL_POINT_DEFINE(throwWriteConflictExceptionInDeleteRange);
MONGO_FAIL_POINT_DEFINE(throwInternalErrorInDeleteRange);
MONGO_FAIL_POINT_DEFINE(hangInDeleteRangeDeletionOnRecipientInterruptible);
MONGO_FAIL_POINT_DEFINE(hangInDeleteRangeDeletionOnRecipientThenSimulateErrorUninterruptible);
MONGO_FAIL_POINT_DEFINE(hangInDeleteRangeDeletionLocallyThenSimulateErrorUninterruptible);
MONGO_FAIL_POINT_DEFINE(hangInReadyRangeDeletionOnRecipientThenSimulateErrorUninterruptible);
MONGO_FAIL_POINT_DEFINE(hangInReadyRangeDeletionLocallyInterruptible);
MONGO_FAIL_POINT_DEFINE(hangInReadyRangeDeletionLocallyThenSimulateErrorUninterruptible);
/**
 * Performs the deletion of up to numDocsToRemovePerBatch entries within the range in progress. Must
 * be called under the collection lock.
 *
 * Returns the number of documents deleted, 0 if done with the range, or bad status if deleting
 * the range failed.
 */
StatusWith<int> deleteNextBatch(OperationContext* opCtx,
                                const CollectionAcquisition& collection,
                                BSONObj const& keyPattern,
                                ChunkRange const& range,
                                int numDocsToRemovePerBatch) {
    invariant(collection.exists());

    auto const nss = collection.nss();
    auto const uuid = collection.uuid();

    // The IndexChunk has a keyPattern that may apply to more than one index - we need to
    // select the index and get the full index keyPattern here.
    const auto shardKeyIdx = findShardKeyPrefixedIndex(
        opCtx, collection.getCollectionPtr(), keyPattern, /*requireSingleKey=*/false);
    if (!shardKeyIdx) {
        // Do not log that the shard key is missing for hashed shard key patterns.
        if (!ShardKeyPattern::isHashedPatternEl(keyPattern.firstElement())) {
            LOGV2_ERROR(23765,
                        "Unable to find range shard key index",
                        "keyPattern"_attr = keyPattern,
                        logAttrs(nss));

            // When a shard key index is not found, the range deleter moves the task to the bottom
            // of the range deletion queue. This sleep is aimed at avoiding logging too aggressively
            // in order to prevent log files to increase too much in size.
            opCtx->sleepFor(Seconds(5));
        }

        iasserted(ErrorCodes::IndexNotFound,
                  str::stream() << "Unable to find shard key index"
                                << " for " << nss.toStringForErrorMsg() << " and key pattern `"
                                << keyPattern.toString() << "'");
    }

    const auto rangeDeleterPriority = rangeDeleterHighPriority.load()
        ? AdmissionContext::Priority::kExempt
        : AdmissionContext::Priority::kLow;

    ScopedAdmissionPriority<ExecutionAdmissionContext> priority{opCtx, rangeDeleterPriority};

    // Extend bounds to match the index we found
    const KeyPattern indexKeyPattern(shardKeyIdx->keyPattern());
    const auto extend = [&](const auto& key) {
        return Helpers::toKeyFormat(indexKeyPattern.extendRangeBound(key, false));
    };

    const auto min = extend(range.getMin());
    const auto max = extend(range.getMax());

    LOGV2_DEBUG(6180601,
                1,
                "Begin removal of range",
                logAttrs(nss),
                "collectionUUID"_attr = uuid,
                "range"_attr = redact(range.toString()));

    auto deleteStageParams = std::make_unique<DeleteStageParams>();
    deleteStageParams->fromMigrate = true;
    deleteStageParams->isMulti = true;
    deleteStageParams->returnDeleted = true;

    auto exec =
        InternalPlanner::deleteWithShardKeyIndexScan(opCtx,
                                                     collection,
                                                     std::move(deleteStageParams),
                                                     *shardKeyIdx,
                                                     min,
                                                     max,
                                                     BoundInclusion::kIncludeStartKeyOnly,
                                                     PlanYieldPolicy::YieldPolicy::YIELD_AUTO,
                                                     InternalPlanner::FORWARD);

    if (MONGO_unlikely(hangBeforeDoingDeletion.shouldFail())) {
        LOGV2(23768, "Hit hangBeforeDoingDeletion failpoint");
        hangBeforeDoingDeletion.pauseWhileSet(opCtx);
    }

    long long bytesDeleted = 0;
    int numDocsDeleted = 0;
    do {
        BSONObj deletedObj;

        if (throwWriteConflictExceptionInDeleteRange.shouldFail()) {
            throwWriteConflictException(
                str::stream() << "Hit failpoint '"
                              << throwWriteConflictExceptionInDeleteRange.getName() << "'.");
        }

        if (throwInternalErrorInDeleteRange.shouldFail()) {
            uasserted(ErrorCodes::InternalError, "Failing for test");
        }

        PlanExecutor::ExecState state;
        try {
            state = exec->getNext(&deletedObj, nullptr);
        } catch (const DBException& ex) {
            auto&& explainer = exec->getPlanExplainer();
            auto&& [stats, _] =
                explainer.getWinningPlanStats(ExplainOptions::Verbosity::kExecStats);
            LOGV2_WARNING(6180602,
                          "Cursor error while trying to delete range",
                          logAttrs(nss),
                          "collectionUUID"_attr = uuid,
                          "range"_attr = redact(range.toString()),
                          "stats"_attr = redact(stats),
                          "error"_attr = redact(ex.toStatus()));
            throw;
        }

        if (state == PlanExecutor::IS_EOF) {
            break;
        }

        bytesDeleted += deletedObj.objsize();
        invariant(PlanExecutor::ADVANCED == state);
    } while (++numDocsDeleted < numDocsToRemovePerBatch);

    ShardingStatistics::get(opCtx).countDocsDeletedByRangeDeleter.addAndFetch(numDocsDeleted);
    ShardingStatistics::get(opCtx).countBytesDeletedByRangeDeleter.addAndFetch(bytesDeleted);

    return numDocsDeleted;
}

void ensureRangeDeletionTaskStillExists(OperationContext* opCtx,
                                        const UUID& collectionUuid,
                                        const ChunkRange& range) {
    // While at this point we are guaranteed for our operation context to be killed if there is a
    // step-up or stepdown, it is still possible that a stepdown and a subsequent step-up happened
    // prior to acquiring the global IX lock. The range deletion task document prevents a moveChunk
    // operation from migrating an overlapping range to this shard. If the range deletion task
    // document has already been deleted, then it is possible for the range in the user collection
    // to now be owned by this shard and for proceeding with the range deletion to result in data
    // corruption. The scheme for checking whether the range deletion task document still exists
    // relies on the executor only having a single thread and that thread being solely responsible
    // for deleting the range deletion task document.
    PersistentTaskStore<RangeDeletionTask> store(NamespaceString::kRangeDeletionNamespace);
    const auto query =
        BSON(RangeDeletionTask::kCollectionUuidFieldName
             << collectionUuid << RangeDeletionTask::kRangeFieldName + "." + ChunkRange::kMinKey
             << range.getMin() << RangeDeletionTask::kRangeFieldName + "." + ChunkRange::kMaxKey
             << range.getMax() << RangeDeletionTask::kPendingFieldName << BSON("$exists" << false));
    auto count = store.count(opCtx, query);

    uassert(ErrorCodes::RangeDeletionAbandonedBecauseTaskDocumentDoesNotExist,
            "Range deletion task no longer exists",
            count > 0);

    // We are now guaranteed that either (a) the range deletion task document will continue to exist
    // for the lifetime of this operation context, or (b) this operation context will be killed if
    // it is possible for the range deletion task document to have been deleted while we weren't
    // holding any locks.
}

void markRangeDeletionTaskAsProcessing(OperationContext* opCtx,
                                       const UUID& collectionUuid,
                                       const ChunkRange& range) {
    PersistentTaskStore<RangeDeletionTask> store(NamespaceString::kRangeDeletionNamespace);
    const auto query =
        BSON(RangeDeletionTask::kCollectionUuidFieldName
             << collectionUuid << RangeDeletionTask::kRangeFieldName + "." + ChunkRange::kMinKey
             << range.getMin() << RangeDeletionTask::kRangeFieldName + "." + ChunkRange::kMaxKey
             << range.getMax() << RangeDeletionTask::kPendingFieldName << BSON("$exists" << false));

    static const auto update =
        BSON("$set" << BSON(RangeDeletionTask::kProcessingFieldName
                            << true << RangeDeletionTask::kWhenToCleanFieldName
                            << CleanWhen_serializer(CleanWhenEnum::kNow)));

    try {
        store.update(opCtx, query, update, WriteConcerns::kLocalWriteConcern);
    } catch (const ExceptionFor<ErrorCodes::NoMatchingDocument>&) {
        // The collection may have been dropped or the document could have been manually deleted
    }
}

std::vector<RangeDeletionTask> getPersistentRangeDeletionTasks(OperationContext* opCtx,
                                                               const NamespaceString& nss) {
    std::vector<RangeDeletionTask> tasks;

    PersistentTaskStore<RangeDeletionTask> store(NamespaceString::kRangeDeletionNamespace);
    auto query = BSON(RangeDeletionTask::kNssFieldName
                      << NamespaceStringUtil::serialize(nss, SerializationContext::stateDefault()));

    store.forEach(opCtx, query, [&](const RangeDeletionTask& deletionTask) {
        tasks.push_back(deletionTask);
        return true;
    });

    return tasks;
}

BSONObj getQueryFilterForRangeDeletionTask(const UUID& collectionUuid, const ChunkRange& range) {
    return BSON(RangeDeletionTask::kCollectionUuidFieldName
                << collectionUuid << RangeDeletionTask::kRangeFieldName + "." + ChunkRange::kMinKey
                << range.getMin() << RangeDeletionTask::kRangeFieldName + "." + ChunkRange::kMaxKey
                << range.getMax());
}

// Add `migrationId` to the query filter in order to be resilient to delayed network retries: only
// relying on collection's UUID and range may lead to undesired updates/deletes on tasks created by
// future migrations.
BSONObj getQueryFilterForRangeDeletionTaskOnRecipient(const UUID& collectionUuid,
                                                      const ChunkRange& range,
                                                      const UUID& migrationId) {
    return getQueryFilterForRangeDeletionTask(collectionUuid, range)
        .addFields(BSON(RangeDeletionTask::kIdFieldName << migrationId));
}


}  // namespace

namespace rangedeletionutil {

Status deleteRangeInBatches(OperationContext* opCtx,
                            const DatabaseName& dbName,
                            const UUID& collectionUuid,
                            const BSONObj& keyPattern,
                            const ChunkRange& range) {
    suspendRangeDeletion.pauseWhileSet(opCtx);

    bool allDocsRemoved = false;
    // Delete all batches in this range unless a stepdown error occurs. Do not yield the
    // executor to ensure that this range is fully deleted before another range is
    // processed.
    while (!allDocsRemoved) {
        try {
            int numDocsToRemovePerBatch = rangeDeleterBatchSize.load();
            if (numDocsToRemovePerBatch <= 0) {
                numDocsToRemovePerBatch = kRangeDeleterBatchSizeDefault;
            }

            Milliseconds delayBetweenBatches(rangeDeleterBatchDelayMS.load());

            ensureRangeDeletionTaskStillExists(opCtx, collectionUuid, range);

            markRangeDeletionTaskAsProcessing(opCtx, collectionUuid, range);

            int numDeleted;
            const auto nss = [&]() {
                try {
                    const auto nssOrUuid = NamespaceStringOrUUID{dbName, collectionUuid};
                    const auto collection =
                        acquireCollection(opCtx,
                                          {nssOrUuid,
                                           AcquisitionPrerequisites::kPretendUnsharded,
                                           repl::ReadConcernArgs::get(opCtx),
                                           AcquisitionPrerequisites::kWrite},
                                          MODE_IX);

                    LOGV2_DEBUG(6777800,
                                1,
                                "Starting batch deletion",
                                logAttrs(collection.nss()),
                                "collectionUUID"_attr = collectionUuid,
                                "range"_attr = redact(range.toString()),
                                "numDocsToRemovePerBatch"_attr = numDocsToRemovePerBatch,
                                "delayBetweenBatches"_attr = delayBetweenBatches);

                    numDeleted = uassertStatusOK(deleteNextBatch(
                        opCtx, collection, keyPattern, range, numDocsToRemovePerBatch));

                    return collection.nss();
                } catch (const ExceptionFor<ErrorCodes::NamespaceNotFound>&) {
                    // Throw specific error code that stops range deletions in case of errors
                    uasserted(
                        ErrorCodes::RangeDeletionAbandonedBecauseCollectionWithUUIDDoesNotExist,
                        "Collection has been dropped since enqueuing this range "
                        "deletion task. No need to delete documents.");
                }
            }();

            persistUpdatedNumOrphans(opCtx, collectionUuid, range, -numDeleted);

            if (MONGO_unlikely(hangAfterDoingDeletion.shouldFail())) {
                hangAfterDoingDeletion.pauseWhileSet(opCtx);
            }

            LOGV2_DEBUG(23769,
                        1,
                        "Deleted documents in pass",
                        "numDeleted"_attr = numDeleted,
                        logAttrs(nss),
                        "collectionUUID"_attr = collectionUuid,
                        "range"_attr = redact(range.toString()));

            if (numDeleted > 0) {
                // (SERVER-62368) The range-deleter executor is mono-threaded, so
                // sleeping synchronously for `delayBetweenBatches` ensures that no
                // other batch is going to be cleared up before the expected delay.
                opCtx->sleepFor(delayBetweenBatches);
            }

            allDocsRemoved = numDeleted < numDocsToRemovePerBatch;
        } catch (const DBException& ex) {
            // Errors other than those indicating stepdown and those that indicate that the
            // range deletion can no longer occur should be retried.
            auto errorCode = ex.code();
            if (errorCode ==
                    ErrorCodes::RangeDeletionAbandonedBecauseCollectionWithUUIDDoesNotExist ||
                errorCode == ErrorCodes::RangeDeletionAbandonedBecauseTaskDocumentDoesNotExist ||
                errorCode == ErrorCodes::IndexNotFound ||
                errorCode == ErrorCodes::KeyPatternShorterThanBound ||
                ErrorCodes::isShutdownError(errorCode) ||
                ErrorCodes::isNotPrimaryError(errorCode) ||
                !opCtx->checkForInterruptNoAssert().isOK()) {
                return ex.toStatus();
            };
        }
    }
    return Status::OK();
}

void snapshotRangeDeletionsForRename(OperationContext* opCtx,
                                     const NamespaceString& fromNss,
                                     const NamespaceString& toNss) {
    // Clear out eventual snapshots associated with the target collection: always restart from a
    // clean state in case of stepdown or primary killed.
    PersistentTaskStore<RangeDeletionTask> store(NamespaceString::kRangeDeletionForRenameNamespace);
    store.remove(opCtx,
                 BSON(RangeDeletionTask::kNssFieldName << NamespaceStringUtil::serialize(
                          toNss, SerializationContext::stateDefault())));

    auto rangeDeletionTasks = getPersistentRangeDeletionTasks(opCtx, fromNss);
    for (auto& task : rangeDeletionTasks) {
        // Associate task to the new namespace
        task.setNss(toNss);
        // Assign a new id to prevent duplicate key conflicts with the source range deletion task
        task.setId(UUID::gen());
        store.add(opCtx, task);
    }
}

void restoreRangeDeletionTasksForRename(OperationContext* opCtx, const NamespaceString& nss) {
    PersistentTaskStore<RangeDeletionTask> rangeDeletionsForRenameStore(
        NamespaceString::kRangeDeletionForRenameNamespace);
    PersistentTaskStore<RangeDeletionTask> rangeDeletionsStore(
        NamespaceString::kRangeDeletionNamespace);

    const auto query = BSON(RangeDeletionTask::kNssFieldName << NamespaceStringUtil::serialize(
                                nss, SerializationContext::stateDefault()));

    rangeDeletionsForRenameStore.forEach(opCtx, query, [&](const RangeDeletionTask& deletionTask) {
        // Upsert the range deletion document so that:
        // - If no document for the same range exists, a task will be registered by the range
        // deleter insert observer.
        // - If a document for the same range already exists, no new task will be registered on
        // the range deleter service because the update observer only registers when the update
        // action is 'unset the pending field'.
        auto& uuid = deletionTask.getCollectionUuid();
        auto& range = deletionTask.getRange();
        auto upsertQuery =
            BSON(RangeDeletionTask::kCollectionUuidFieldName
                 << uuid << RangeDeletionTask::kRangeFieldName + "." + ChunkRange::kMinKey
                 << range.getMin() << RangeDeletionTask::kRangeFieldName + "." + ChunkRange::kMaxKey
                 << range.getMax());
        // Remove _id because it's an immutable field so it can't be part of an update.
        // But include it as part of the upsert because the _id field is expected to be a uuid
        // (as opposed to the default OID) in case a new document is inserted.
        auto upsertDocument = deletionTask.toBSON().removeField(RangeDeletionTask::kIdFieldName);
        rangeDeletionsStore.upsert(opCtx,
                                   upsertQuery,
                                   BSON("$set"
                                        << upsertDocument << "$setOnInsert"
                                        << BSON(RangeDeletionTask::kIdFieldName << UUID::gen())));
        return true;
    });
}

void deleteRangeDeletionTasksForRename(OperationContext* opCtx,
                                       const NamespaceString& fromNss,
                                       const NamespaceString& toNss) {
    // Delete already restored snapshots associated to the target collection
    PersistentTaskStore<RangeDeletionTask> rangeDeletionsForRenameStore(
        NamespaceString::kRangeDeletionForRenameNamespace);
    rangeDeletionsForRenameStore.remove(
        opCtx,
        BSON(RangeDeletionTask::kNssFieldName
             << NamespaceStringUtil::serialize(toNss, SerializationContext::stateDefault())));
}

/**
 * persistUpdatedNumOrphans 函数的作用：
 * 持久化更新分片迁移或范围删除过程中某个chunk范围内孤儿文档数量的变化到本地config.rangeDeletions集合。
 *
 * 核心功能：
 * 1. 原子性地将孤儿文档数量变化（changeInOrphans）累加到对应的RangeDeletionTask文档。
 * 2. 保证在迁移/删除过程中孤儿计数的准确性，便于后续清理和统计。
 * 3. 支持写冲突重试，确保高并发环境下的可靠性。
 * 4. 通过BalancerStatsRegistry同步更新全局孤儿计数统计。
 * 5. 兼容升级/降级场景，无孤儿计数字段时安全忽略。
 *
 * 使用场景：
 * - chunk迁移增量应用时，目标分片插入/删除文档后更新孤儿计数。
 * - 范围删除任务批量删除文档后，及时持久化孤儿计数变化。
 * - 迁移和清理流程中，保证孤儿文档统计的持久一致性。
 *
 * 参数说明：
 * @param opCtx 操作上下文，包含事务、锁等状态信息
 * @param collectionUuid 目标集合的UUID
 * @param range 迁移或删除的chunk范围
 * @param changeInOrphans 本次操作导致的孤儿文档数量变化（正数为增加，负数为减少）
 */
void persistUpdatedNumOrphans(OperationContext* opCtx,
                              const UUID& collectionUuid,
                              const ChunkRange& range,
                              long long changeInOrphans) {
    // 构造查询条件：定位目标集合和chunk范围对应的RangeDeletionTask文档
    const auto query = getQueryFilterForRangeDeletionTask(collectionUuid, range);
    try {
        // 创建持久化任务存储实例，指向config.rangeDeletions集合
        PersistentTaskStore<RangeDeletionTask> store(NamespaceString::kRangeDeletionNamespace);

        // 范围删除锁保护，确保并发安全
        ScopedRangeDeleterLock rangeDeleterLock(opCtx, LockMode::MODE_IX);

        // 写冲突重试：在高并发环境下保证原子性和可靠性
        writeConflictRetry(
            opCtx, "updateOrphanCount", NamespaceString::kRangeDeletionNamespace, [&] {
                // 原子更新：将changeInOrphans累加到numOrphanDocs字段
                store.update(opCtx,
                             query,
                             BSON("$inc" << BSON(RangeDeletionTask::kNumOrphanDocsFieldName
                                                 << changeInOrphans)),
                             WriteConcerns::kLocalWriteConcern);
            });

        // 同步更新全局孤儿计数统计（用于监控和均衡器决策）
        BalancerStatsRegistry::get(opCtx)->updateOrphansCount(collectionUuid, changeInOrphans);
    } catch (const ExceptionFor<ErrorCodes::NoMatchingDocument>&) {
        // 兼容升级/降级场景：无孤儿计数字段时安全忽略，不影响主流程
    }

void removePersistentRangeDeletionTask(OperationContext* opCtx,
                                       const UUID& collectionUuid,
                                       const ChunkRange& range) {
    PersistentTaskStore<RangeDeletionTask> store(NamespaceString::kRangeDeletionNamespace);

    auto overlappingRangeDeletionsQuery = BSON(
        RangeDeletionTask::kCollectionUuidFieldName
        << collectionUuid << RangeDeletionTask::kRangeFieldName + "." + ChunkRange::kMinKey << GTE
        << range.getMin() << RangeDeletionTask::kRangeFieldName + "." + ChunkRange::kMaxKey << LTE
        << range.getMax());
    store.remove(opCtx, overlappingRangeDeletionsQuery);
}

void removePersistentRangeDeletionTasksByUUID(OperationContext* opCtx, const UUID& collectionUuid) {
    DBDirectClient dbClient(opCtx);

    auto query = BSON(RangeDeletionTask::kCollectionUuidFieldName << collectionUuid);
    auto commandResponse = dbClient.runCommand([&] {
        write_ops::DeleteCommandRequest deleteOp(NamespaceString::kRangeDeletionNamespace);

        deleteOp.setDeletes({[&] {
            write_ops::DeleteOpEntry entry;
            entry.setQ(query);
            entry.setMulti(true);
            return entry;
        }()});

        return deleteOp.serialize({});
    }());

    const auto commandReply = commandResponse->getCommandReply();
    uassertStatusOK(getStatusFromWriteCommandReply(commandReply));
}

BSONObj overlappingRangeDeletionsQuery(const ChunkRange& range, const UUID& uuid) {
    return BSON(RangeDeletionTask::kCollectionUuidFieldName
                << uuid << RangeDeletionTask::kRangeFieldName + "." + ChunkRange::kMinKey << LT
                << range.getMax() << RangeDeletionTask::kRangeFieldName + "." + ChunkRange::kMaxKey
                << GT << range.getMin());
}

size_t checkForConflictingDeletions(OperationContext* opCtx,
                                    const ChunkRange& range,
                                    const UUID& uuid) {
    PersistentTaskStore<RangeDeletionTask> store(NamespaceString::kRangeDeletionNamespace);

    return store.count(opCtx, overlappingRangeDeletionsQuery(range, uuid));
}

/**
 * 本地持久化范围删除任务函数 - MongoDB分片环境下的范围清理任务存储
 * 
 * 核心功能：
 * 1. 将分片迁移或chunk移动产生的范围删除任务持久化到本地存储
 * 2. 确保范围删除任务在节点重启后能够恢复和继续执行
 * 3. 防止重复的范围删除任务通过DuplicateKey检测提供幂等性保证
 * 4. 支持自定义写关注级别，确保任务持久化的可靠性要求
 * 5. 维护config.rangeDeletions集合中的任务文档完整性
 * 
 * 设计原理：
 * - 持久化存储：使用PersistentTaskStore确保任务在故障后可恢复
 * - 幂等性保证：通过捕获DuplicateKey异常防止重复任务创建
 * - 写关注支持：允许调用者指定数据持久化的可靠性级别
 * - 错误转换：将存储层错误转换为更具描述性的应用层错误
 * 
 * 适用场景：
 * - chunk迁移完成后需要清理源分片上的孤立文档
 * - 分片重新平衡过程中的范围数据清理任务调度
 * - 节点故障恢复时重建未完成的范围删除任务队列
 * - 分片拓扑变更导致的数据重分布清理工作
 * 
 * 重要约束：
 * - 任务ID必须全局唯一，通常使用迁移ID作为标识符
 * - 范围删除任务必须包含完整的集合UUID和chunk范围信息
 * - 调用者需确保在适当的锁保护下执行以避免并发冲突
 * - 写关注级别需要根据业务要求选择合适的持久化保证
 * 
 * @param opCtx 操作上下文，包含事务、会话、锁等状态信息
 * @param deletionTask 要持久化的范围删除任务对象，包含目标范围、集合UUID等信息
 * @param writeConcern 写关注选项，指定数据持久化的可靠性级别（如majority、local等）
 * @throws 31375 如果检测到重复的迁移ID，抛出匿名错误防止任务重复
 * @throws DBException 其他数据库操作异常会直接向上传播
 */
void persistRangeDeletionTaskLocally(OperationContext* opCtx,
                                     const RangeDeletionTask& deletionTask,
                                     const WriteConcernOptions& writeConcern) {
    // 创建持久化任务存储实例，指向config.rangeDeletions集合
    // 这个集合专门用于存储分片环境下的范围删除任务
    PersistentTaskStore<RangeDeletionTask> store(NamespaceString::kRangeDeletionNamespace);
    
    try {
        // 尝试将范围删除任务添加到持久化存储中
        // 核心操作：将deletionTask序列化并插入到config.rangeDeletions集合
        // 
        // 插入的文档结构大致如下：
        // {
        //   "_id": <migrationId>,                    // 唯一标识符，通常是迁移UUID
        //   "nss": "<database>.<collection>",        // 目标集合的命名空间
        //   "collectionUuid": <UUID>,               // 集合的唯一标识符
        //   "range": {                              // 要删除的文档范围
        //     "min": <BSONObj>,                     // 范围最小值（包含）
        //     "max": <BSONObj>                      // 范围最大值（不包含）
        //   },
        //   "whenToClean": "now"|"delayed",         // 清理时机策略
        //   "pending": true,                        // 是否处于等待状态
        //   "processing": false,                    // 是否正在处理中
        //   "numOrphanDocs": <long>                 // 孤立文档数量估计
        // }
        store.add(opCtx, deletionTask, writeConcern);
        
    } catch (const ExceptionFor<ErrorCodes::DuplicateKey>&) {
        // 重复键异常处理：当尝试插入具有相同_id的任务时触发
        // 这通常发生在以下场景：
        // 1. 网络重试导致的重复请求
        // 2. 故障恢复时重复创建已存在的任务
        // 3. 并发迁移操作使用了相同的迁移ID
        //
        // Convert a DuplicateKey error to an anonymous error.
        // 将DuplicateKey错误转换为匿名错误，提供更详细的上下文信息
        uasserted(31375,
                  str::stream() << "While attempting to write range deletion task for migration "
                                << ", found document with the same migration id. Attempted range "
                                   "deletion task: "
                                << deletionTask.toBSON());
        
        // 错误信息包含：
        // - 具体的操作描述（写入范围删除任务）
        // - 冲突的原因（相同的迁移ID）
        // - 尝试插入的完整任务内容（用于调试）
        //
        // 这种设计的优势：
        // 1. 提供详细的错误上下文，便于问题诊断
        // 2. 使用匿名错误码，避免客户端对特定错误的依赖
        // 3. 包含完整的任务信息，支持重试或手动处理
    }
    
    // 函数成功完成时的状态：
    // 1. 范围删除任务已成功持久化到config.rangeDeletions集合
    // 2. 任务将被RangeDeleterService发现并加入执行队列
    // 3. 后续的范围删除操作将根据任务配置自动执行
    // 4. 任务状态变更（如从pending到processing）将同步更新到存储
}

long long retrieveNumOrphansFromShard(OperationContext* opCtx,
                                      const ShardId& shardId,
                                      const UUID& migrationId) {
    const auto recipientShard =
        uassertStatusOK(Grid::get(opCtx)->shardRegistry()->getShard(opCtx, shardId));
    FindCommandRequest findCommand(NamespaceString::kRangeDeletionNamespace);
    findCommand.setFilter(BSON("_id" << migrationId));
    findCommand.setReadConcern(BSONObj());
    Shard::QueryResponse rangeDeletionResponse =
        uassertStatusOK(recipientShard->runExhaustiveCursorCommand(
            opCtx,
            ReadPreferenceSetting(ReadPreference::PrimaryOnly),
            NamespaceString::kRangeDeletionNamespace.dbName(),
            findCommand.toBSON(BSONObj()),
            Milliseconds(-1)));
    if (rangeDeletionResponse.docs.empty()) {
        // In case of shutdown/stepdown, the recipient may have already deleted its range deletion
        // document. A previous call to this function will have already returned the correct number
        // of orphans, so we can simply return 0.
        LOGV2_DEBUG(6376301,
                    2,
                    "No matching document found for migration",
                    "recipientId"_attr = shardId,
                    "migrationId"_attr = migrationId);
        return 0;
    }
    const auto numOrphanDocsElem =
        rangeDeletionResponse.docs[0].getField(RangeDeletionTask::kNumOrphanDocsFieldName);
    return numOrphanDocsElem.safeNumberLong();
}

boost::optional<KeyPattern> getShardKeyPatternFromRangeDeletionTask(OperationContext* opCtx,
                                                                    const UUID& migrationId) {
    DBDirectClient client(opCtx);
    FindCommandRequest findCommand(NamespaceString::kRangeDeletionNamespace);
    findCommand.setFilter(BSON("_id" << migrationId));
    auto cursor = client.find(std::move(findCommand));
    if (!cursor->more()) {
        // If the range deletion task doesn't exist then the migration must have been aborted, so
        // we won't need the shard key pattern anyways.
        return boost::none;
    }
    auto rdt = RangeDeletionTask::parse(IDLParserContext("MigrationRecovery"), cursor->next());
    return rdt.getKeyPattern();
}

void deleteRangeDeletionTaskOnRecipient(OperationContext* opCtx,
                                        const ShardId& recipientId,
                                        const UUID& collectionUuid,
                                        const ChunkRange& range,
                                        const UUID& migrationId) {
    const auto queryFilter =
        getQueryFilterForRangeDeletionTaskOnRecipient(collectionUuid, range, migrationId);
    write_ops::DeleteCommandRequest deleteOp(NamespaceString::kRangeDeletionNamespace);
    write_ops::DeleteOpEntry query(queryFilter, false /*multi*/);
    deleteOp.setDeletes({query});

    hangInDeleteRangeDeletionOnRecipientInterruptible.pauseWhileSet(opCtx);

    auto cmd = deleteOp.toBSON(
        BSON(WriteConcernOptions::kWriteConcernField << WriteConcernOptions::Majority));
    sharding_util::invokeCommandOnShardWithIdempotentRetryPolicy(
        opCtx, recipientId, NamespaceString::kRangeDeletionNamespace.dbName(), cmd);

    if (hangInDeleteRangeDeletionOnRecipientThenSimulateErrorUninterruptible.shouldFail()) {
        hangInDeleteRangeDeletionOnRecipientThenSimulateErrorUninterruptible.pauseWhileSet(opCtx);
        uasserted(ErrorCodes::InternalError,
                  "simulate an error response when deleting range deletion on recipient");
    }
}

void deleteRangeDeletionTaskLocally(OperationContext* opCtx,
                                    const UUID& collectionUuid,
                                    const ChunkRange& range,
                                    const WriteConcernOptions& writeConcern) {
    PersistentTaskStore<RangeDeletionTask> store(NamespaceString::kRangeDeletionNamespace);
    const auto query = getQueryFilterForRangeDeletionTask(collectionUuid, range);
    store.remove(opCtx, query, writeConcern);

    if (hangInDeleteRangeDeletionLocallyThenSimulateErrorUninterruptible.shouldFail()) {
        hangInDeleteRangeDeletionLocallyThenSimulateErrorUninterruptible.pauseWhileSet(opCtx);
        uasserted(ErrorCodes::InternalError,
                  "simulate an error response when deleting range deletion locally");
    }
}

void markAsReadyRangeDeletionTaskLocally(OperationContext* opCtx,
                                         const UUID& collectionUuid,
                                         const ChunkRange& range) {
    PersistentTaskStore<RangeDeletionTask> store(NamespaceString::kRangeDeletionNamespace);
    const auto query =
        BSON(RangeDeletionTask::kCollectionUuidFieldName
             << collectionUuid << RangeDeletionTask::kRangeFieldName + "." + ChunkRange::kMinKey
             << range.getMin() << RangeDeletionTask::kRangeFieldName + "." + ChunkRange::kMaxKey
             << range.getMax());
    auto update = BSON("$unset" << BSON(RangeDeletionTask::kPendingFieldName << ""));

    hangInReadyRangeDeletionLocallyInterruptible.pauseWhileSet(opCtx);
    try {
        store.update(opCtx, query, update);
    } catch (const ExceptionFor<ErrorCodes::NoMatchingDocument>&) {
        // If we are recovering the migration, the range-deletion may have already finished. So its
        // associated document may already have been removed.
    }

    if (hangInReadyRangeDeletionLocallyThenSimulateErrorUninterruptible.shouldFail()) {
        hangInReadyRangeDeletionLocallyThenSimulateErrorUninterruptible.pauseWhileSet(opCtx);
        uasserted(ErrorCodes::InternalError,
                  "simulate an error response when initiating range deletion locally");
    }
}

void markAsReadyRangeDeletionTaskOnRecipient(OperationContext* opCtx,
                                             const ShardId& recipientId,
                                             const UUID& collectionUuid,
                                             const ChunkRange& range,
                                             const UUID& migrationId) {
    write_ops::UpdateCommandRequest updateOp(NamespaceString::kRangeDeletionNamespace);
    const auto queryFilter =
        getQueryFilterForRangeDeletionTaskOnRecipient(collectionUuid, range, migrationId);
    auto updateModification =
        write_ops::UpdateModification(write_ops::UpdateModification::parseFromClassicUpdate(
            BSON("$unset" << BSON(RangeDeletionTask::kPendingFieldName << "") << "$set"
                          << BSON(RangeDeletionTask::kWhenToCleanFieldName
                                  << CleanWhen_serializer(CleanWhenEnum::kNow)))));
    write_ops::UpdateOpEntry updateEntry(queryFilter, updateModification);
    updateEntry.setMulti(false);
    updateEntry.setUpsert(false);
    updateOp.setUpdates({updateEntry});

    sharding_util::retryIdempotentWorkAsPrimaryUntilSuccessOrStepdown(
        opCtx, "ready remote range deletion", [&](OperationContext* newOpCtx) {
            auto cmd = updateOp.toBSON(
                BSON(WriteConcernOptions::kWriteConcernField << WriteConcernOptions::Majority));
            try {
                sharding_util::invokeCommandOnShardWithIdempotentRetryPolicy(
                    newOpCtx, recipientId, NamespaceString::kRangeDeletionNamespace.dbName(), cmd);
            } catch (const ExceptionFor<ErrorCodes::ShardNotFound>& exShardNotFound) {
                LOGV2_DEBUG(4620232,
                            1,
                            "Failed to mark range deletion task on recipient shard as ready",
                            "collectionUuid"_attr = collectionUuid,
                            "range"_attr = range,
                            "error"_attr = exShardNotFound);
                return;
            }

            if (hangInReadyRangeDeletionOnRecipientThenSimulateErrorUninterruptible.shouldFail()) {
                hangInReadyRangeDeletionOnRecipientThenSimulateErrorUninterruptible.pauseWhileSet(
                    newOpCtx);
                uasserted(ErrorCodes::InternalError,
                          "simulate an error response when initiating range deletion on recipient");
            }
        });
}
}  // namespace rangedeletionutil
}  // namespace mongo
