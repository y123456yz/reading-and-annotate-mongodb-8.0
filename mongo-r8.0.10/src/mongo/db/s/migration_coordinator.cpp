/**
 *    Copyright (C) 2019-present MongoDB, Inc.
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

#include "mongo/db/s/migration_coordinator.h"

#include <boost/none.hpp>
#include <boost/optional.hpp>
#include <string>

#include <boost/move/utility_core.hpp>
#include <boost/optional/optional.hpp>

#include "mongo/base/error_codes.h"
#include "mongo/base/status.h"
#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/timestamp.h"
#include "mongo/db/catalog_raii.h"
#include "mongo/db/concurrency/lock_manager_defs.h"
#include "mongo/db/logical_time.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/persistent_task_store.h"
#include "mongo/db/s/collection_sharding_runtime.h"
#include "mongo/db/s/migration_util.h"
#include "mongo/db/s/range_deleter_service.h"
#include "mongo/db/s/range_deletion_task_gen.h"
#include "mongo/db/s/range_deletion_util.h"
#include "mongo/db/session/logical_session_id_helpers.h"
#include "mongo/db/vector_clock.h"
#include "mongo/db/vector_clock_mutable.h"
#include "mongo/db/write_concern_options.h"
#include "mongo/logv2/log.h"
#include "mongo/logv2/log_attr.h"
#include "mongo/logv2/log_component.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/duration.h"
#include "mongo/util/fail_point.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kShardingMigration

namespace mongo {

MONGO_FAIL_POINT_DEFINE(hangBeforeMakingCommitDecisionDurable);
MONGO_FAIL_POINT_DEFINE(hangBeforeMakingAbortDecisionDurable);

MONGO_FAIL_POINT_DEFINE(hangBeforeSendingCommitDecision);
MONGO_FAIL_POINT_DEFINE(hangBeforeSendingAbortDecision);

MONGO_FAIL_POINT_DEFINE(hangBeforeForgettingMigrationAfterCommitDecision);
MONGO_FAIL_POINT_DEFINE(hangBeforeForgettingMigrationAfterAbortDecision);

namespace {

LogicalSessionId getSystemLogicalSessionId() {
    static auto lsid = makeSystemLogicalSessionId();
    return lsid;
}

TxnNumber getNextTxnNumber() {
    static AtomicWord<TxnNumber> nextTxnNumber{0};
    return nextTxnNumber.fetchAndAdd(2);
}

}  // namespace

namespace migrationutil {

MigrationCoordinator::MigrationCoordinator(MigrationSessionId sessionId,
                                           ShardId donorShard,
                                           ShardId recipientShard,
                                           NamespaceString collectionNamespace,
                                           UUID collectionUuid,
                                           ChunkRange range,
                                           ChunkVersion preMigrationChunkVersion,
                                           const KeyPattern& shardKeyPattern,
                                           bool waitForDelete)
    : _migrationInfo(UUID::gen(),
                     std::move(sessionId),
                     getSystemLogicalSessionId(),
                     getNextTxnNumber(),
                     std::move(collectionNamespace),
                     collectionUuid,
                     std::move(donorShard),
                     std::move(recipientShard),
                     std::move(range),
                     std::move(preMigrationChunkVersion)),
      _shardKeyPattern(shardKeyPattern),
      _waitForDelete(waitForDelete) {}

MigrationCoordinator::MigrationCoordinator(const MigrationCoordinatorDocument& doc)
    : _migrationInfo(doc) {}

MigrationCoordinator::~MigrationCoordinator() = default;

const UUID& MigrationCoordinator::getMigrationId() const {
    return _migrationInfo.getId();
}

const LogicalSessionId& MigrationCoordinator::getLsid() const {
    return _migrationInfo.getLsid();
}

TxnNumber MigrationCoordinator::getTxnNumber() const {
    return _migrationInfo.getTxnNumber();
}

void MigrationCoordinator::setShardKeyPattern(const boost::optional<KeyPattern>& shardKeyPattern) {
    _shardKeyPattern = shardKeyPattern;
}

/**
 * MigrationCoordinator::startMigration 函数的作用：
 * 启动迁移过程的初始化阶段，建立迁移所需的持久化状态和元数据。
 * 
 * 核心功能：
 * 1. 持久化协调器状态：将迁移协调器文档写入 config.migrationCoordinators 集合
 * 2. 创建源端删除任务：在源分片创建 pending 状态的范围删除任务
 * 3. 故障恢复准备：确保在节点故障后能够恢复迁移状态
 * 4. 元数据一致性：设置分片键模式、时间戳等关键元数据
 * 5. 写关注点保证：使用 majority 写关注点确保数据持久化
 * 
 * 持久化组件：
 * - config.migrationCoordinators：存储完整的迁移协调器状态
 * - config.rangeDeletions：存储源端的范围删除任务（pending 状态）
 * 
 * 执行顺序：
 * 1. 首先持久化迁移协调器文档，建立迁移的身份标识
 * 2. 然后创建源端范围删除任务，为后续清理做准备
 * 3. 所有操作使用强一致性写关注点确保持久化
 * 
 * 状态设置：
 * - 范围删除任务设置为 pending：防止立即执行删除
 * - 设置集群时间戳：确保时间一致性
 * - 配置清理策略：根据 waitForDelete 参数决定清理时机
 * 
 * 该函数是迁移协调器生命周期的第一个阶段，为后续的数据克隆、决策制定和状态清理奠定基础。
 */
void MigrationCoordinator::startMigration(OperationContext* opCtx) {
    // 持久化迁移协调器文档：
    // 功能：将协调器的完整状态信息写入 config.migrationCoordinators 集合
    // 目的：建立迁移的身份标识和核心元数据，支持故障恢复
    // 内容：包含迁移ID、会话信息、源端和目标端分片、chunk范围等
    LOGV2_DEBUG(
        23889, 2, "Persisting migration coordinator doc", "migrationDoc"_attr = _migrationInfo);
    migrationutil::persistMigrationCoordinatorLocally(opCtx, _migrationInfo);

    // 持久化源端范围删除任务：
    // 功能：在源分片创建范围删除任务，为迁移完成后的数据清理做准备
    // 状态：设置为 pending，防止在迁移完成前执行删除操作
    // 重要性：确保在迁移失败时能够正确清理或保留数据
    LOGV2_DEBUG(23890,
                2,
                "Persisting range deletion task on donor",
                "migrationId"_attr = _migrationInfo.getId());
    
    // 构建范围删除任务对象：
    // 参数设置：
    // - 迁移ID：关联到特定的迁移操作
    // - 命名空间和集合UUID：标识要操作的集合
    // - 源分片ID：标识执行删除的分片
    // - 范围：要删除的文档范围
    // - 清理时机：根据 waitForDelete 参数决定立即或延迟清理
    RangeDeletionTask donorDeletionTask(_migrationInfo.getId(),
                                        _migrationInfo.getNss(),
                                        _migrationInfo.getCollectionUuid(),
                                        _migrationInfo.getDonorShardId(),
                                        _migrationInfo.getRange(),
                                        _waitForDelete ? CleanWhenEnum::kNow
                                                       : CleanWhenEnum::kDelayed);
    
    // 设置任务为 pending 状态：
    // 功能：防止任务立即被执行，确保迁移过程的正确顺序
    // 原理：pending 任务不会被范围删除服务自动处理
    // 时机：只有在迁移决策确定后才会被标记为 ready
    donorDeletionTask.setPending(true);
    
    // 设置分片键模式：
    // 功能：为范围删除任务提供分片键信息
    // 用途：删除操作需要分片键来正确识别和过滤文档
    // 要求：分片键模式必须在此时已经设置
    donorDeletionTask.setKeyPattern(*_shardKeyPattern);
    
    // 设置集群时间戳：
    // 功能：为删除任务设置当前的集群时间戳
    // 目的：确保时间一致性，支持基于时间的操作排序
    // 来源：从向量时钟获取当前的集群时间
    const auto currentTime = VectorClock::get(opCtx)->getTime();
    donorDeletionTask.setTimestamp(currentTime.clusterTime().asTimestamp());
    
    // 持久化范围删除任务：
    // 功能：将构建的删除任务写入 config.rangeDeletions 集合
    // 写关注点：使用 majority 写关注点确保数据持久化
    // 超时设置：使用分片操作的标准超时时间
    // 本地性：写入本地（源分片）的 rangeDeletions 集合
    rangedeletionutil::persistRangeDeletionTaskLocally(
        opCtx, donorDeletionTask, WriteConcerns::kMajorityWriteConcernShardingTimeout);
}

void MigrationCoordinator::setMigrationDecision(DecisionEnum decision) {
    LOGV2_DEBUG(23891,
                2,
                "MigrationCoordinator setting migration decision",
                "decision"_attr = (decision == DecisionEnum::kCommitted ? "committed" : "aborted"),
                "migrationId"_attr = _migrationInfo.getId());
    _migrationInfo.setDecision(decision);
}


boost::optional<SharedSemiFuture<void>> MigrationCoordinator::completeMigration(
    OperationContext* opCtx) {
    auto decision = _migrationInfo.getDecision();
    if (!decision) {
        LOGV2(
            23892,
            "Migration completed without setting a decision. This node might have "
            "started stepping down or shutting down after having initiated commit against the "
            "config server but before having found out if the commit succeeded. The new primary of "
            "this replica set will complete the migration coordination.",
            "migrationId"_attr = _migrationInfo.getId());
        return boost::none;
    }

    LOGV2(23893,
          "MigrationCoordinator delivering decision to self and to recipient",
          "decision"_attr = (decision == DecisionEnum::kCommitted ? "committed" : "aborted"),
          "migrationId"_attr = _migrationInfo.getId());

    if (!_releaseRecipientCriticalSectionFuture) {
        launchReleaseRecipientCriticalSection(opCtx);
    }

    // Persist the config time before the migration decision to ensure that in case of stepdown
    // next filtering metadata refresh on the new primary will always include the effect of this
    // migration.
    VectorClockMutable::get(opCtx)->waitForDurableConfigTime().get(opCtx);

    boost::optional<SharedSemiFuture<void>> cleanupCompleteFuture = boost::none;

    switch (*decision) {
        case DecisionEnum::kAborted:
            _abortMigrationOnDonorAndRecipient(opCtx);
            hangBeforeForgettingMigrationAfterAbortDecision.pauseWhileSet();
            break;
        case DecisionEnum::kCommitted:
            cleanupCompleteFuture = _commitMigrationOnDonorAndRecipient(opCtx);
            hangBeforeForgettingMigrationAfterCommitDecision.pauseWhileSet();
            break;
    }

    forgetMigration(opCtx);

    return cleanupCompleteFuture;
}

SharedSemiFuture<void> MigrationCoordinator::_commitMigrationOnDonorAndRecipient(
    OperationContext* opCtx) {
    hangBeforeMakingCommitDecisionDurable.pauseWhileSet();

    LOGV2_DEBUG(
        23894, 2, "Making commit decision durable", "migrationId"_attr = _migrationInfo.getId());
    migrationutil::persistCommitDecision(opCtx, _migrationInfo);

    _waitForReleaseRecipientCriticalSectionFutureIgnoreShardNotFound(opCtx);

    LOGV2_DEBUG(23895,
                2,
                "Bumping transaction number on recipient shard for commit",
                logAttrs(_migrationInfo.getNss()),
                "recipientShardId"_attr = _migrationInfo.getRecipientShardId(),
                "lsid"_attr = _migrationInfo.getLsid(),
                "currentTxnNumber"_attr = _migrationInfo.getTxnNumber(),
                "migrationId"_attr = _migrationInfo.getId());
    migrationutil::advanceTransactionOnRecipient(opCtx,
                                                 _migrationInfo.getRecipientShardId(),
                                                 _migrationInfo.getLsid(),
                                                 _migrationInfo.getTxnNumber());

    hangBeforeSendingCommitDecision.pauseWhileSet();

    LOGV2_DEBUG(6376300,
                2,
                "Retrieving number of orphan documents from recipient",
                "migrationId"_attr = _migrationInfo.getId());

    const auto numOrphans = rangedeletionutil::retrieveNumOrphansFromShard(
        opCtx, _migrationInfo.getRecipientShardId(), _migrationInfo.getId());

    if (numOrphans > 0) {
        rangedeletionutil::persistUpdatedNumOrphans(
            opCtx, _migrationInfo.getCollectionUuid(), _migrationInfo.getRange(), numOrphans);
    }

    LOGV2_DEBUG(23896,
                2,
                "Deleting range deletion task on recipient",
                "migrationId"_attr = _migrationInfo.getId());
    rangedeletionutil::deleteRangeDeletionTaskOnRecipient(opCtx,
                                                          _migrationInfo.getRecipientShardId(),
                                                          _migrationInfo.getCollectionUuid(),
                                                          _migrationInfo.getRange(),
                                                          _migrationInfo.getId());

    RangeDeletionTask deletionTask(_migrationInfo.getId(),
                                   _migrationInfo.getNss(),
                                   _migrationInfo.getCollectionUuid(),
                                   _migrationInfo.getDonorShardId(),
                                   _migrationInfo.getRange(),
                                   _waitForDelete ? CleanWhenEnum::kNow : CleanWhenEnum::kDelayed);
    const auto currentTime = VectorClock::get(opCtx)->getTime();
    deletionTask.setTimestamp(currentTime.clusterTime().asTimestamp());
    // In multiversion migration recovery scenarios, we may not have the key pattern.
    if (_shardKeyPattern) {
        deletionTask.setKeyPattern(*_shardKeyPattern);
    }


    auto waitForActiveQueriesToComplete = [&]() {
        AutoGetCollection autoColl(opCtx, deletionTask.getNss(), MODE_IS);
        return CollectionShardingRuntime::assertCollectionLockedAndAcquireShared(
                   opCtx, deletionTask.getNss())
            ->getOngoingQueriesCompletionFuture(deletionTask.getCollectionUuid(),
                                                deletionTask.getRange())
            .semi();
    }();


    // Register the range deletion task as pending in order to get the completion future
    const auto rangeDeleterService = RangeDeleterService::get(opCtx);
    rangeDeleterService->getRangeDeleterServiceInitializationFuture().get(opCtx);
    auto rangeDeletionCompletionFuture =
        rangeDeleterService->registerTask(deletionTask,
                                          std::move(waitForActiveQueriesToComplete),
                                          false /* fromStepUp*/,
                                          true /* pending */);

    LOGV2_DEBUG(6555800,
                2,
                "Marking range deletion task on donor as ready for processing",
                "rangeDeletion"_attr = deletionTask);

    // Mark the range deletion task document as non-pending in order to unblock the previously
    // registered range deletion
    rangedeletionutil::markAsReadyRangeDeletionTaskLocally(
        opCtx, deletionTask.getCollectionUuid(), deletionTask.getRange());

    return rangeDeletionCompletionFuture;
}

void MigrationCoordinator::_abortMigrationOnDonorAndRecipient(OperationContext* opCtx) {
    hangBeforeMakingAbortDecisionDurable.pauseWhileSet();

    LOGV2_DEBUG(
        23899, 2, "Making abort decision durable", "migrationId"_attr = _migrationInfo.getId());
    migrationutil::persistAbortDecision(opCtx, _migrationInfo);

    hangBeforeSendingAbortDecision.pauseWhileSet();

    _waitForReleaseRecipientCriticalSectionFutureIgnoreShardNotFound(opCtx);

    // Ensure removing the local range deletion document to prevent incoming migrations with
    // overlapping ranges to hang.
    LOGV2_DEBUG(23901,
                2,
                "Deleting range deletion task on donor",
                "migrationId"_attr = _migrationInfo.getId());
    rangedeletionutil::deleteRangeDeletionTaskLocally(
        opCtx, _migrationInfo.getCollectionUuid(), _migrationInfo.getRange());

    try {
        LOGV2_DEBUG(23900,
                    2,
                    "Bumping transaction number on recipient shard for abort",
                    logAttrs(_migrationInfo.getNss()),
                    "recipientShardId"_attr = _migrationInfo.getRecipientShardId(),
                    "lsid"_attr = _migrationInfo.getLsid(),
                    "currentTxnNumber"_attr = _migrationInfo.getTxnNumber(),
                    "migrationId"_attr = _migrationInfo.getId());
        migrationutil::advanceTransactionOnRecipient(opCtx,
                                                     _migrationInfo.getRecipientShardId(),
                                                     _migrationInfo.getLsid(),
                                                     _migrationInfo.getTxnNumber());
    } catch (const ExceptionFor<ErrorCodes::ShardNotFound>& exShardNotFound) {
        LOGV2_DEBUG(4620231,
                    1,
                    "Failed to advance transaction number on recipient shard for abort and/or "
                    "marking range deletion task on recipient as ready for processing",
                    logAttrs(_migrationInfo.getNss()),
                    "migrationId"_attr = _migrationInfo.getId(),
                    "recipientShardId"_attr = _migrationInfo.getRecipientShardId(),
                    "currentTxnNumber"_attr = _migrationInfo.getTxnNumber(),
                    "error"_attr = exShardNotFound);
    }

    LOGV2_DEBUG(23902,
                2,
                "Marking range deletion task on recipient as ready for processing",
                "migrationId"_attr = _migrationInfo.getId());
    rangedeletionutil::markAsReadyRangeDeletionTaskOnRecipient(opCtx,
                                                               _migrationInfo.getRecipientShardId(),
                                                               _migrationInfo.getCollectionUuid(),
                                                               _migrationInfo.getRange(),
                                                               _migrationInfo.getId());
}

void MigrationCoordinator::forgetMigration(OperationContext* opCtx) {
    LOGV2_DEBUG(23903,
                2,
                "Deleting migration coordinator document",
                "migrationId"_attr = _migrationInfo.getId());

    PersistentTaskStore<MigrationCoordinatorDocument> store(
        NamespaceString::kMigrationCoordinatorsNamespace);
    store.remove(opCtx,
                 BSON(MigrationCoordinatorDocument::kIdFieldName << _migrationInfo.getId()),
                 WriteConcernOptions{1, WriteConcernOptions::SyncMode::UNSET, Seconds(0)});
}

/**
 * MigrationCoordinator::launchReleaseRecipientCriticalSection
 * 该函数用于在迁移元数据提交到 config server 成功后，异步通知目标分片释放关键区域（critical section）。
 * 主要作用是：通过异步任务，调用目标分片的接口，解除对迁移 chunk 范围的写操作阻塞，
 * 让目标分片完成迁移收尾并恢复正常业务写入。
 */
void MigrationCoordinator::launchReleaseRecipientCriticalSection(OperationContext* opCtx) {
    // 异步发起释放关键区域的请求，保存 future 以便后续跟踪任务完成状态
    _releaseRecipientCriticalSectionFuture =
        migrationutil::launchReleaseCriticalSectionOnRecipientFuture(
            opCtx,
            _migrationInfo.getRecipientShardId(),      // 目标分片ID
            _migrationInfo.getNss(),                   // 迁移集合命名空间
            _migrationInfo.getMigrationSessionId());    // 迁移会话ID
}

void MigrationCoordinator::_waitForReleaseRecipientCriticalSectionFutureIgnoreShardNotFound(
    OperationContext* opCtx) {
    invariant(_releaseRecipientCriticalSectionFuture);
    try {
        _releaseRecipientCriticalSectionFuture->get(opCtx);
    } catch (const ExceptionFor<ErrorCodes::ShardNotFound>& exShardNotFound) {
        LOGV2(5899100,
              "Failed to releaseCriticalSectionOnRecipient",
              "shardId"_attr = _migrationInfo.getRecipientShardId(),
              "error"_attr = exShardNotFound);
    }
}

}  // namespace migrationutil
}  // namespace mongo
