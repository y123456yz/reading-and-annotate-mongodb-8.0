/**
 *    Copyright (C) 2022-present MongoDB, Inc.
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

#include "mongo/db/catalog/collection_write_path.h"

#include <boost/move/utility_core.hpp>
#include <boost/optional/optional.hpp>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>

#include "mongo/base/error_codes.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonelement.h"
#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/bsontypes.h"
#include "mongo/bson/simple_bsonelement_comparator.h"
#include "mongo/bson/timestamp.h"
#include "mongo/crypto/fle_crypto_types.h"
#include "mongo/db/catalog/capped_collection_maintenance.h"
#include "mongo/db/catalog/clustered_collection_options_gen.h"
#include "mongo/db/catalog/collection_options.h"
#include "mongo/db/catalog/collection_options_gen.h"
#include "mongo/db/catalog/document_validation.h"
#include "mongo/db/catalog/local_oplog_info.h"
#include "mongo/db/concurrency/d_concurrency.h"
#include "mongo/db/concurrency/exception_util.h"
#include "mongo/db/concurrency/lock_manager_defs.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/exec/write_stage_common.h"
#include "mongo/db/feature_flag.h"
#include "mongo/db/op_observer/op_observer.h"
#include "mongo/db/record_id_helpers.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/db/s/operation_sharding_state.h"
#include "mongo/db/server_options.h"
#include "mongo/db/service_context.h"
#include "mongo/db/storage/duplicate_key_error_info.h"
#include "mongo/db/storage/index_entry_comparison.h"
#include "mongo/db/storage/key_format.h"
#include "mongo/db/storage/record_data.h"
#include "mongo/db/storage/record_store.h"
#include "mongo/db/storage/recovery_unit.h"
#include "mongo/db/storage/storage_parameters_gen.h"
#include "mongo/db/transaction_resources.h"
#include "mongo/logv2/log.h"
#include "mongo/logv2/log_attr.h"
#include "mongo/logv2/log_component.h"
#include "mongo/platform/compiler.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/decorable.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/namespace_string_util.h"
#include "mongo/util/str.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStorage

namespace mongo {
namespace collection_internal {
namespace {

// This failpoint throws a WriteConflictException after a successful call to
// insertDocumentForBulkLoader
MONGO_FAIL_POINT_DEFINE(failAfterBulkLoadDocInsert);

//  This fail point injects insertion failures for all collections unless a collection name is
//  provided in the optional data object during configuration:
//  data: {
//      collectionNS: <fully-qualified collection namespace>,
//  }
MONGO_FAIL_POINT_DEFINE(failCollectionInserts);

// Used to pause after inserting collection data and calling the opObservers.  Inserts to
// replicated collections that are not part of a multi-statement transaction will have generated
// their OpTime and oplog entry. Supports parameters to limit pause by namespace and by _id
// of first data item in an insert (must be of type string):
//  data: {
//      collectionNS: <fully-qualified collection namespace>,
//      first_id: <string>
//  }
MONGO_FAIL_POINT_DEFINE(hangAfterCollectionInserts);

// This fail point introduces corruption to documents during insert.
MONGO_FAIL_POINT_DEFINE(corruptDocumentOnInsert);

// This fail point manually forces the RecordId to be of a given value during insert.
MONGO_FAIL_POINT_DEFINE(explicitlySetRecordIdOnInsert);

// This fail point skips deletion of the record, so that the deletion call would only delete the
// index keys.
MONGO_FAIL_POINT_DEFINE(skipDeleteRecord);

bool compareSafeContentElem(const BSONObj& oldDoc, const BSONObj& newDoc) {
    if (newDoc.hasField(kSafeContent) != oldDoc.hasField(kSafeContent)) {
        return false;
    }
    if (!newDoc.hasField(kSafeContent)) {
        return true;
    }

    return newDoc.getField(kSafeContent).binaryEqual(oldDoc.getField(kSafeContent));
}

std::vector<OplogSlot> reserveOplogSlotsForRetryableFindAndModify(OperationContext* opCtx) {
    // For retryable findAndModify running in a multi-document transaction, we will reserve the
    // oplog entries when the transaction prepares or commits without prepare.
    if (opCtx->inMultiDocumentTransaction()) {
        return {};
    }

    // We reserve oplog slots here, expecting the slot with the greatest timestmap (say TS) to be
    // used as the oplog timestamp. Tenant migrations and resharding will forge no-op image oplog
    // entries and set the timestamp for these synthetic entries to be TS - 1.
    auto oplogInfo = LocalOplogInfo::get(opCtx);
    auto slots = oplogInfo->getNextOpTimes(opCtx, 2);
    uassertStatusOK(
        shard_role_details::getRecoveryUnit(opCtx)->setTimestamp(slots.back().getTimestamp()));
    return slots;
}

/**
 * Returns an array of 'fromMigrate' values for a range of insert operations.
 * The 'fromMigrate' oplog entry field is used to identify operations that are a result
 * of chunk migration and should not generate change stream events.
 * Accepts a default 'fromMigrate' value that determines if there is a need to check
 * each insert operation individually.
 * See SERVER-62581 and SERVER-65858.
 */
std::vector<bool> makeFromMigrateForInserts(
    OperationContext* opCtx,
    const NamespaceString& nss,
    const std::vector<InsertStatement>::const_iterator begin,
    const std::vector<InsertStatement>::const_iterator end,
    bool defaultFromMigrate) {
    auto count = std::distance(begin, end);
    std::vector fromMigrate(count, defaultFromMigrate);
    if (defaultFromMigrate) {
        return fromMigrate;
    }

    // 'fromMigrate' is an oplog entry field. If we do not need to write this operation to
    // the oplog, there is no reason to proceed with the orphan document check.
    if (repl::ReplicationCoordinator::get(opCtx)->isOplogDisabledFor(opCtx, nss)) {
        return fromMigrate;
    }

    // Overriding the 'fromMigrate' flag makes sense only for requests coming from clients
    // directly connected to shards.
    if (OperationShardingState::isComingFromRouter(opCtx)) {
        return fromMigrate;
    }

    // This is used to check whether the write should be performed, and if so, any other
    // behavior that should be done as part of the write (e.g. skipping it because it affects an
    // orphan document).
    write_stage_common::PreWriteFilter preWriteFilter(opCtx, nss);

    for (decltype(count) i = 0; i < count; i++) {
        auto& insertStmt = begin[i];
        if (preWriteFilter.computeAction(Document(insertStmt.doc)) ==
            write_stage_common::PreWriteFilter::Action::kWriteAsFromMigrate) {
            LOGV2_DEBUG(7458900,
                        3,
                        "Marking insert operation of orphan document with the 'fromMigrate' flag "
                        "to prevent a wrong change stream event",
                        "namespace"_attr = nss,
                        "document"_attr = insertStmt.doc);

            fromMigrate[i] = true;
        }
    }

    return fromMigrate;
}

bool isCappedCollectionWithIndex(const CollectionPtr& collection) {
    return collection->isCapped() && collection->getIndexCatalog()->haveAnyIndexes();
}

Status allowedToInsertDocuments(OperationContext* opCtx,
                                const CollectionPtr& collection,
                                size_t batchSize) {
    if (batchSize > 1 && isCappedCollectionWithIndex(collection)) {
        // We require that inserts to indexed capped collections be done one-at-a-time to avoid the
        // possibility that a later document causes an earlier document to be deleted before it can
        // be indexed.
        if (shouldDeferCappedDeletesToOplogApplication(opCtx, collection)) {
            // However, the logic to do these deletes only runs when the inserts are originally
            // performed (i.e. on the primary). When doing oplog application, the secondary will
            // later apply those delete oplogs that were originally generated by the primary, so
            // even batched inserts will be indexed before they can be deleted.
            return Status::OK();
        }
        return {ErrorCodes::OperationCannotBeBatched,
                "Can't batch inserts into indexed capped collections"};
    }
    return Status::OK();
}

/**
 * 文档插入底层实现函数 - MongoDB集合写入的核心执行引擎
 * 
 * 核心功能：
 * 1. 执行底层文档插入操作，包括存储引擎写入、索引更新和oplog记录
 * 2. 处理特殊集合类型的写入约束，如上限集合的并发控制和容量管理
 * 3. 管理RecordId生成策略，支持聚簇集合、副本记录ID和普通集合
 * 4. 集成OpObserver事件系统，触发oplog生成、变更流和复制逻辑
 * 5. 实施分片环境下的孤儿文档检测和fromMigrate标记处理
 * 
 * 设计原理：
 * - 分层处理：锁检查 -> RecordId生成 -> 存储写入 -> 索引更新 -> 事件通知
 * - 批量优化：一次性处理多个文档，减少系统调用和锁争用开销
 * - 异常安全：完整的错误处理和回滚机制，确保数据一致性
 * - 存储抽象：支持不同存储引擎的RecordId策略和写入模式
 * 
 * 适用场景：
 * - 常规插入操作的最终执行阶段
 * - 副本集oplog应用的底层写入
 * - 分片数据迁移的目标端写入
 * - 初始化同步和恢复过程的批量写入
 * 
 * 重要约束：
 * - 调用者必须持有适当的集合锁（MODE_IX或更强）
 * - 索引上限集合强制单文档插入以避免删除-插入竞争
 * - 聚簇集合要求文档包含聚簇键字段
 * - 非复制集合不触发OpObserver事件通知
 * 
 * @param opCtx 操作上下文，包含事务状态、锁信息、复制设置等
 * @param collection 目标集合指针，提供存储和索引访问接口
 * @param begin 插入语句迭代器开始位置，指向第一个待插入文档
 * @param end 插入语句迭代器结束位置，标记批次边界
 * @param opDebug 调试信息收集器，用于性能统计和监控（可选）
 * @param fromMigrate 是否来自分片迁移，影响变更流和oplog标记
 * @return Status 操作结果，成功返回OK，失败返回具体错误码和描述
 */
Status insertDocumentsImpl(OperationContext* opCtx,
                           const CollectionPtr& collection,
                           const std::vector<InsertStatement>::const_iterator begin,
                           const std::vector<InsertStatement>::const_iterator end,
                           OpDebug* opDebug,
                           bool fromMigrate) {
    // 获取集合命名空间：用于锁验证、日志记录和错误报告
    const auto& nss = collection->ns();

    // 锁状态断言：确保调用者持有适当的锁级别
    // 支持的锁模式：
    // 1. MODE_IX：普通集合的意向排他锁
    // 2. 写锁：oplog集合需要更强的锁保护
    // 3. 租户锁：变更集合需要租户级别的意向排他锁
    dassert(shard_role_details::getLocker(opCtx)->isCollectionLockedForMode(nss, MODE_IX) ||
            (nss.isOplog() && shard_role_details::getLocker(opCtx)->isWriteLocked()) ||
            (nss.isChangeCollection() && nss.tenantId() &&
             shard_role_details::getLocker(opCtx)->isLockHeldForMode(
                 {ResourceType::RESOURCE_TENANT, *nss.tenantId()}, MODE_IX)));

    // 批次大小计算：确定当前批次包含的文档数量
    // 用途：
    // 1. 批次大小验证（如上限集合的单文档限制）
    // 2. 容器预分配以优化性能
    // 3. 性能统计和监控数据收集
    const size_t count = std::distance(begin, end);

    // 插入权限检查：验证当前批次是否允许插入
    // 主要检查：索引上限集合的批量插入限制
    // 原因：避免删除-插入操作的竞争条件
    auto allowStatus = allowedToInsertDocuments(opCtx, collection, count);
    if (!allowStatus.isOK()) {
        return allowStatus;
    }

    // 上限集合并发控制：获取元数据排他锁以序列化写入
    // 适用条件：需要上限锁的复制集合（通常是非聚簇上限集合）
    // 目的：
    // 1. 保证插入顺序在从节点上的一致性（SERVER-21483）
    // 2. 防止主节点并发度超过从节点，帮助从节点跟上复制
    // 3. 保护'_cappedFirstRecord'等关键元数据
    // 注意：聚簇上限集合由于聚簇键的单调性，天然保证插入顺序，无需此锁
    if (collection->needsCappedLock()) {
        // X-lock the metadata resource for this replicated, non-clustered capped collection until
        // the end of the WUOW. Non-clustered capped collections require writes to be serialized on
        // the secondary in order to guarantee insertion order (SERVER-21483); this exclusive access
        // to the metadata resource prevents the primary from executing with more concurrency than
        // secondaries - thus helping secondaries keep up - and protects '_cappedFirstRecord'. See
        // SERVER-21646. On the other hand, capped clustered collections with a monotonically
        // increasing cluster key natively guarantee preservation of the insertion order, and don't
        // need serialisation. We allow concurrent inserts for clustered capped collections.
        Lock::ResourceLock heldUntilEndOfWUOW{opCtx, ResourceId(RESOURCE_METADATA, nss), MODE_X};
    }

    // Record容器初始化：为存储引擎写入准备数据结构
    // 预分配容量以避免动态扩容的性能开销
    std::vector<Record> records;
    records.reserve(count);
    std::vector<Timestamp> timestamps;
    timestamps.reserve(count);

    // 上限集合RecordId预留机制：处理需要上限快照的集合
    // 条件：使用上限快照 且 调用者未提供RecordId
    // 目的：预先分配RecordId并在CappedVisibilityObserver中注册，处理可见性
    std::vector<RecordId> cappedRecordIds;
    // For capped collections requiring capped snapshots, usually RecordIds are reserved and
    // registered here to handle visibility. If the RecordId is provided by the caller, it is
    // assumed the caller already reserved and properly registered the inserts in the
    // CappedVisibilityObserver.
    if (collection->usesCappedSnapshots() && begin->recordId.isNull()) {
        cappedRecordIds = collection->reserveCappedRecordIds(opCtx, count);
    }

    // Record构建循环：为每个文档生成Record对象
    size_t i = 0;
    for (auto it = begin; it != end; it++, i++) {
        const auto& doc = it->doc;

        // RecordId生成策略：根据集合类型选择合适的ID生成方式
        RecordId recordId;
        if (collection->isClustered()) {
            // 聚簇集合：基于聚簇键生成RecordId
            // 要求：存储引擎必须使用String格式的键
            // 过程：从文档中提取聚簇键字段，生成对应的RecordId
            invariant(collection->getRecordStore()->keyFormat() == KeyFormat::String);
            recordId = uassertStatusOK(
                record_id_helpers::keyForDoc(doc,
                                             collection->getClusteredInfo()->getIndexSpec(),
                                             collection->getDefaultCollator()));
        } else if (!it->replicatedRecordId.isNull()) {
            // 副本记录ID：用于复制RecordId的集合
            // 场景：某些特殊集合需要在主从节点间保持相同的RecordId
            // The 'replicatedRecordId' being set indicates that this insert belongs to a replicated
            // recordId collection, and we need to use the given recordId while inserting.
            recordId = it->replicatedRecordId;
        } else if (!it->recordId.isNull()) {
            // 测试指定RecordId：通常仅在测试环境中使用
            // 用途：避免为上限集合自动生成RecordId，便于测试控制
            // This case would only normally be called in a testing circumstance to avoid
            // automatically generating record ids for capped collections.
            recordId = it->recordId;
        } else if (cappedRecordIds.size()) {
            // 预留的上限集合RecordId：使用之前预留的ID
            recordId = std::move(cappedRecordIds[i]);
        }

        // 文档损坏故障点：测试框架支持，模拟文档损坏场景
        // 功能：插入截断的记录（一半大小），用于测试错误处理
        if (MONGO_unlikely(corruptDocumentOnInsert.shouldFail())) {
            // Insert a truncated record that is half the expected size of the source document.
            records.emplace_back(
                Record{std::move(recordId), RecordData(doc.objdata(), doc.objsize() / 2)});
            timestamps.emplace_back(it->oplogSlot.getTimestamp());
            continue;
        }

        // RecordId强制设置故障点：测试框架支持，手动指定RecordId
        // 用途：在特定测试场景中覆盖自动生成的RecordId
        explicitlySetRecordIdOnInsert.execute([&](const BSONObj& data) {
            const auto docToMatch = data["doc"].Obj();
            if (doc.woCompare(docToMatch) == 0) {
                {
                    auto ridValue = data["rid"].safeNumberInt();
                    recordId = RecordId(ridValue);
                }
            }
        });

        // 构建最终的Record对象：包含RecordId、文档数据和时间戳
        records.emplace_back(Record{std::move(recordId), RecordData(doc.objdata(), doc.objsize())});
        timestamps.emplace_back(it->oplogSlot.getTimestamp());
    }


    // 先写数据，再写索引，最后写oplog

    // 存储引擎写入：将所有记录批量写入存储引擎
    // 核心操作：
    // 1. 将文档数据持久化到存储引擎
    // 2. 分配最终的RecordId（如果之前未指定）
    // 3. 处理重复键错误和其他存储引擎异常
    Status status = collection->getRecordStore()->insertRecords(opCtx, &records, timestamps);

    if (!status.isOK()) {
        // 聚簇集合重复键错误处理：转换为用户友好的错误消息
        // 目的：保持与索引重复键错误的一致性，维护用户代码兼容性
        if (auto extraInfo = status.extraInfo<DuplicateKeyErrorInfo>();
            extraInfo && collection->isClustered()) {
            // Generate a useful error message that is consistent with duplicate key error messages
            // on indexes. This transforms the error from a duplicate clustered key error into a
            // duplicate key error. We have to perform this in order to maintain compatibility with
            // already existing user code.
            const auto& rId = extraInfo->getDuplicateRid();
            const auto& foundValue = extraInfo->getFoundValue();
            invariant(rId,
                      "Clustered Collections must return the RecordId when returning a duplicate "
                      "key error");
            BSONObj obj = record_id_helpers::toBSONAs(*rId, "");
            status = buildDupKeyErrorStatus(obj,
                                            NamespaceString(collection->ns()),
                                            "" /* indexName */,
                                            BSON("_id" << 1),
                                            collection->getCollectionOptions().collation,
                                            DuplicateKeyErrorInfo::FoundValue{foundValue});
        }
        return status;
    }

    // BsonRecord构建：为索引更新准备BSON记录
    // 目的：将存储引擎记录转换为索引系统需要的格式
    std::vector<BsonRecord> bsonRecords;
    bsonRecords.reserve(count);
    int recordIndex = 0;
    for (auto it = begin; it != end; it++) {
        // 获取存储引擎分配的最终RecordId
        RecordId loc = records[recordIndex++].id;
        
        // RecordId范围验证：确保Long格式的RecordId在有效范围内
        if (collection->getRecordStore()->keyFormat() == KeyFormat::Long) {
            invariant(RecordId::minLong() < loc);
            invariant(loc < RecordId::maxLong());
        }

        // 构建BsonRecord：包含位置、时间戳和文档引用
        BsonRecord bsonRecord = {
            std::move(loc), Timestamp(it->oplogSlot.getTimestamp()), &(it->doc)};
        bsonRecords.emplace_back(std::move(bsonRecord));
    }

    // 复制RecordId收集：为OpObserver准备RecordId列表
    // 条件：集合启用了RecordId复制
    // 用途：在oplog条目中包含RecordId信息，支持精确的复制
    // An empty vector of recordIds is ignored by the OpObserver. When non-empty,
    // the OpObserver will add recordIds to the generated oplog entries.
    std::vector<RecordId> recordIds;
    if (collection->areRecordIdsReplicated()) {
        recordIds.reserve(count);
        for (const auto& r : records) {
            recordIds.push_back(r.id);
        }
    }

    // 索引更新：批量更新所有相关索引
    // 过程：
    // 1. 为每个文档生成索引键
    // 2. 将键插入到对应的索引结构中
    // 3. 处理唯一约束和重复键检测
    // 4. 统计插入的索引键数量
    int64_t keysInserted = 0;
    status =
        collection->getIndexCatalog()->indexRecords(opCtx, collection, bsonRecords, &keysInserted);
    if (!status.isOK()) {
        return status;
    }

    // 性能统计更新：记录插入的索引键数量用于监控
    if (opDebug) {
        opDebug->additiveMetrics.incrementKeysInserted(keysInserted);
        // 'opDebug' may be deleted at rollback time in case of multi-document transaction.
        // 回滚处理：在事务回滚时调整统计数据
        // 注意：多文档事务中opDebug可能在回滚时被删除，需要特殊处理
        if (!opCtx->inMultiDocumentTransaction()) {
            shard_role_details::getRecoveryUnit(opCtx)->onRollback(
                [opDebug, keysInserted](OperationContext*) {
                    opDebug->additiveMetrics.incrementKeysInserted(-keysInserted);
                });
        }
    }

    // OpObserver事件通知：触发oplog记录、变更流和其他观察者
    // 跳过条件：隐式复制的集合（如local数据库中的特殊集合）
    if (!nss.isImplicitlyReplicated()) {
        opCtx->getServiceContext()->getOpObserver()->onInserts(
            opCtx,
            collection,
            begin,
            end,
            recordIds,
            /*fromMigrate=*/makeFromMigrateForInserts(opCtx, nss, begin, end, fromMigrate),
            /*defaultFromMigrate=*/fromMigrate);
    }

    // 上限集合容量管理：确保集合大小不超过配置的最大值
    // 机制：如果插入导致集合超出容量，删除最老的文档
    // 参数：传入第一个新记录的ID作为容量检查的起点
    cappedDeleteUntilBelowConfiguredMaximum(opCtx, collection, records.begin()->id);

    // 返回成功状态：所有插入操作和相关处理已完成
    return Status::OK();
}

}  // namespace

Status insertDocumentForBulkLoader(OperationContext* opCtx,
                                   const CollectionPtr& collection,
                                   const BSONObj& doc,
                                   RecordId replicatedRecordId,
                                   const OnRecordInsertedFn& onRecordInserted) {
    const auto& nss = collection->ns();

    auto status = checkFailCollectionInsertsFailPoint(nss, doc);
    if (!status.isOK()) {
        return status;
    }

    status = collection->checkValidationAndParseResult(opCtx, doc);
    if (!status.isOK()) {
        return status;
    }

    dassert(shard_role_details::getLocker(opCtx)->isCollectionLockedForMode(nss, MODE_IX) ||
            (nss.isOplog() && shard_role_details::getLocker(opCtx)->isWriteLocked()));

    // The replicatedRecordId must be provided if the collection has recordIdsReplicated:true and it
    // must not be provided if the collection has recordIdsReplicated:false
    invariant(collection->areRecordIdsReplicated() != replicatedRecordId.isNull(),
              str::stream() << "Unexpected recordId value for collection with ns: '"
                            << collection->ns().toStringForErrorMsg() << "', uuid: '"
                            << collection->uuid());

    RecordId recordId = replicatedRecordId;
    if (collection->isClustered()) {
        invariant(collection->getRecordStore()->keyFormat() == KeyFormat::String);
        recordId = uassertStatusOK(record_id_helpers::keyForDoc(
            doc, collection->getClusteredInfo()->getIndexSpec(), collection->getDefaultCollator()));
    }

    // Using timestamp 0 for these inserts, which are non-oplog so we don't have an appropriate
    // timestamp to use.
    StatusWith<RecordId> loc = collection->getRecordStore()->insertRecord(
        opCtx, recordId, doc.objdata(), doc.objsize(), Timestamp());

    if (!loc.isOK())
        return loc.getStatus();

    status = onRecordInserted(loc.getValue());

    if (MONGO_unlikely(failAfterBulkLoadDocInsert.shouldFail())) {
        LOGV2(20290,
              "Failpoint failAfterBulkLoadDocInsert enabled. Throwing "
              "WriteConflictException",
              logAttrs(nss));
        throwWriteConflictException(str::stream() << "Hit failpoint '"
                                                  << failAfterBulkLoadDocInsert.getName() << "'.");
    }

    std::vector<InsertStatement> inserts;
    OplogSlot slot;
    // Fetch a new optime now, if necessary.
    auto replCoord = repl::ReplicationCoordinator::get(opCtx);
    if (!replCoord->isOplogDisabledFor(opCtx, nss)) {
        auto slots = LocalOplogInfo::get(opCtx)->getNextOpTimes(opCtx, 1);
        slot = slots.back();
    }
    inserts.emplace_back(kUninitializedStmtId, doc, slot);

    // During initial sync, there are no recordIds to be passed to the OpObserver to
    // include in oplog entries, as we don't generate oplog entries.
    opCtx->getServiceContext()->getOpObserver()->onInserts(
        opCtx,
        collection,
        inserts.begin(),
        inserts.end(),
        /*recordIds=*/{},
        /*fromMigrate=*/std::vector<bool>(inserts.size(), false),
        /*defaultFromMigrate=*/false);

    cappedDeleteUntilBelowConfiguredMaximum(opCtx, collection, loc.getValue());

    // Capture the recordStore here instead of the CollectionPtr object itself, because the record
    // store's lifetime is controlled by the collection IX lock held on the write paths, whereas the
    // CollectionPtr is just a front to the collection and its lifetime is shorter
    shard_role_details::getRecoveryUnit(opCtx)->onCommit(
        [recordStore = collection->getRecordStore()](OperationContext*,
                                                     boost::optional<Timestamp>) {
            recordStore->notifyCappedWaitersIfNeeded();
        });

    return loc.getStatus();
}

/**
 * 批量文档插入核心函数 - MongoDB集合写入路径的主要入口点
 * 
 * 核心功能：
 * 1. 执行批量文档插入操作，支持高效的多文档原子性写入
 * 2. 实施全面的文档验证机制，包括结构验证、加密字段检查等
 * 3. 处理特殊集合类型的插入约束，如索引上限集合的单文档限制
 * 4. 集成OpObserver事件通知，支持oplog记录和变更流生成
 * 5. 管理上限集合的容量维护和等待者通知机制
 * 
 * 设计原理：
 * - 分层验证：从故障点检查到文档验证再到加密字段验证的层次化处理
 * - 快照一致性：通过快照ID确保操作过程中的一致性状态
 * - 异常安全：完整的错误处理和资源清理机制
 * - 性能优化：批量处理减少系统调用和锁开销
 * 
 * 适用场景：
 * - 客户端批量插入操作的后端实现
 * - 副本集oplog应用的文档插入
 * - 迁移和同步过程中的数据写入
 * - 初始化同步期间的批量数据加载
 * 
 * 重要约束：
 * - 要求调用者持有适当的集合锁（MODE_IX或写锁）
 * - 索引上限集合限制批量大小为1，防止删除-插入竞争
 * - 加密集合禁止包含预留的安全内容字段
 * - 需要_id索引的集合要求所有文档包含_id字段
 * 
 * @param opCtx 操作上下文，包含事务状态、锁信息、复制设置等
 * @param collection 目标集合指针，提供集合元数据和存储访问接口
 * @param begin 插入语句迭代器开始位置，指向第一个待插入文档
 * @param end 插入语句迭代器结束位置，标记批次边界
 * @param opDebug 调试信息收集器，用于性能分析和监控（可选）
 * @param fromMigrate 是否来自迁移操作，影响oplog记录和变更流处理
 * @return Status 操作结果状态，成功返回OK，失败返回具体错误信息
 */
Status insertDocuments(OperationContext* opCtx,
                       const CollectionPtr& collection,
                       std::vector<InsertStatement>::const_iterator begin,
                       std::vector<InsertStatement>::const_iterator end,
                       OpDebug* opDebug,
                       bool fromMigrate) {
    // 获取集合命名空间：用于后续的权限检查、日志记录和错误报告
    const auto& nss = collection->ns();

    // 故障点检查：测试框架支持，允许在特定集合上模拟插入失败
    // 用途：
    // 1. 集成测试中验证错误处理路径的正确性
    // 2. 压力测试期间模拟各种故障场景
    // 3. 开发阶段调试复杂的插入流程
    auto status = checkFailCollectionInsertsFailPoint(nss, (begin != end ? begin->doc : BSONObj()));
    if (!status.isOK()) {
        return status;
    }

    // Should really be done in the collection object at creation and updated on index create.
    // _id索引存在性检查：确定集合是否具有_id索引
    // 重要性：
    // 1. 有_id索引的集合要求所有文档必须包含_id字段
    // 2. 这是MongoDB文档唯一性保证的基础
    // 3. 影响后续的文档验证逻辑
    // 注释说明：理想情况下应该在集合创建时完成并在索引创建时更新
    const bool hasIdIndex = collection->getIndexCatalog()->findIdIndex(opCtx);

    // 文档级验证循环：逐个检查批次中的每个文档
    for (auto it = begin; it != end; it++) {
        // _id字段必需性验证：有_id索引的集合必须包含_id字段
        if (hasIdIndex && it->doc["_id"].eoo()) {
            return Status(ErrorCodes::InternalError,
                          str::stream()
                              << "Collection::insertDocument got document without _id for ns:"
                              << nss.toStringForErrorMsg());
        }

        // 文档结构和模式验证：执行JSON Schema验证、数据类型检查等
        // 功能：
        // 1. 验证文档是否符合集合定义的JSON Schema
        // 2. 检查数据类型的正确性和约束条件
        // 3. 验证必需字段的存在性
        // 4. 检查字段值的范围和格式约束
        auto status = collection->checkValidationAndParseResult(opCtx, it->doc);
        if (!status.isOK()) {
            return status;
        }

        // 获取文档验证设置：确定当前的验证策略和安全策略
        auto& validationSettings = DocumentValidationSettings::get(opCtx);

        // 加密字段冲突检查：防止用户文档包含系统预留的安全字段
        // 场景：启用客户端字段级加密(CSFLE)的集合
        // 限制原因：
        // 1. kSafeContent是MongoDB内部用于存储加密元数据的预留字段
        // 2. 用户文档不应包含此字段，避免与加密系统冲突
        // 3. 临时重新分片集合是例外，因为它们可能包含系统生成的数据
        if (collection->getCollectionOptions().encryptedFieldConfig &&
            !collection->ns().isTemporaryReshardingCollection() &&
            !validationSettings.isSchemaValidationDisabled() &&
            !validationSettings.isSafeContentValidationDisabled() &&
            it->doc.hasField(kSafeContent)) {
            return Status(ErrorCodes::BadValue,
                          str::stream()
                              << "Cannot insert a document with field name " << kSafeContent);
        }
    }

    // 快照一致性基线：记录操作开始时的快照ID
    // 目的：
    // 1. 确保整个插入操作过程中使用一致的数据视图
    // 2. 检测并发操作是否影响了当前事务的一致性
    // 3. 为调试并发问题提供重要的诊断信息
    const SnapshotId sid = shard_role_details::getRecoveryUnit(opCtx)->getSnapshotId();

    // 核心插入逻辑：调用底层实现函数执行实际的插入操作
    // insertDocumentsImpl职责：
    // 1. 执行锁检查和批次大小验证
    // 2. 生成RecordId并写入存储引擎
    // 3. 更新所有相关的索引结构
    // 4. 触发OpObserver事件（oplog、变更流等）
    // 5. 处理上限集合的容量管理
    status = insertDocumentsImpl(opCtx, collection, begin, end, opDebug, fromMigrate);
    if (!status.isOK()) {
        return status;
    }
    
    // 快照一致性验证：确保操作过程中快照ID未发生变化
    // 断言意义：
    // 1. 验证插入操作没有触发不当的快照切换
    // 2. 确保MVCC（多版本并发控制）机制正确工作
    // 3. 检测存储引擎层面的一致性问题
    invariant(sid == shard_role_details::getRecoveryUnit(opCtx)->getSnapshotId());

    // Capture the recordStore here instead of the CollectionPtr object itself, because the record
    // store's lifetime is controlled by the collection IX lock held on the write paths, whereas the
    // CollectionPtr is just a front to the collection and its lifetime is shorter
    // 上限集合等待者通知机制：在事务提交时通知等待空间的读取者
    // 设计考虑：
    // 1. 捕获recordStore而不是CollectionPtr，因为recordStore的生命周期由写入路径持有的IX锁控制
    // 2. CollectionPtr只是集合的前端，生命周期较短
    // 3. onCommit确保只在成功提交后才通知等待者
    // 4. 上限集合的读取者可能在等待新数据到达
    shard_role_details::getRecoveryUnit(opCtx)->onCommit(
        [recordStore = collection->getRecordStore()](OperationContext*,
                                                     boost::optional<Timestamp>) {
            // 通知上限集合等待者：唤醒可能正在等待新数据的读取操作
            // 应用场景：
            // 1. 尾部游标（tailable cursor）等待新文档
            // 2. 变更流等待新的变更事件
            // 3. 实时应用监听集合变化
            recordStore->notifyCappedWaitersIfNeeded();
        });

    // 调试故障点：在插入完成后暂停执行，用于测试和调试
    // 支持的参数：
    // 1. collectionNS: 指定特定集合的命名空间
    // 2. first_id: 指定第一个文档的_id值（必须是字符串类型）
    // 使用场景：
    // 1. 测试插入操作与其他操作的并发交互
    // 2. 验证插入后的系统状态
    // 3. 调试复杂的多操作流程
    hangAfterCollectionInserts.executeIf(
        [&](const BSONObj& data) {
            // 构建等待条件描述信息
            const auto& firstIdElem = data["first_id"];
            std::string whenFirst;
            if (firstIdElem) {
                whenFirst += " when first _id is ";
                whenFirst += firstIdElem.str();
            }
            // 记录暂停信息并等待故障点被禁用
            LOGV2(20289,
                  "hangAfterCollectionInserts fail point enabled. Blocking "
                  "until fail point is disabled.",
                  "ns"_attr = nss,
                  "whenFirst"_attr = whenFirst);
            hangAfterCollectionInserts.pauseWhileSet(opCtx);
        },
        [&](const BSONObj& data) {
            // 故障点触发条件检查：
            // 1. 解析故障点配置中的集合命名空间
            const auto fpNss = NamespaceStringUtil::parseFailPointData(data, "collectionNS");
            const auto& firstIdElem = data["first_id"];
            // If the failpoint specifies no collection or matches the existing one, hang.
            // 2. 检查是否应该触发故障点：
            //    - 故障点未指定集合或匹配当前集合
            //    - 未指定first_id或匹配第一个文档的_id
            return (fpNss.isEmpty() || nss == fpNss) &&
                (!firstIdElem ||
                 (begin != end && firstIdElem.type() == mongo::String &&
                  begin->doc["_id"].str() == firstIdElem.str()));
        });

    // 返回成功状态：所有插入操作已完成
    return Status::OK();
}

Status insertDocument(OperationContext* opCtx,
                      const CollectionPtr& collection,
                      const InsertStatement& doc,
                      OpDebug* opDebug,
                      bool fromMigrate) {
    std::vector<InsertStatement> docs;
    docs.push_back(doc);
    return insertDocuments(opCtx, collection, docs.begin(), docs.end(), opDebug, fromMigrate);
}

Status checkFailCollectionInsertsFailPoint(const NamespaceString& ns, const BSONObj& firstDoc) {
    Status s = Status::OK();
    failCollectionInserts.executeIf(
        [&](const BSONObj& data) {
            const std::string msg = str::stream()
                << "Failpoint (failCollectionInserts) has been enabled (" << data
                << "), so rejecting insert (first doc): " << firstDoc;
            LOGV2(20287,
                  "Failpoint (failCollectionInserts) has been enabled, so rejecting insert",
                  "data"_attr = data,
                  "document"_attr = firstDoc);
            s = {ErrorCodes::FailPointEnabled, msg};
        },
        [&](const BSONObj& data) {
            // If the failpoint specifies no collection or matches the existing one, fail.
            const auto fpNss = NamespaceStringUtil::parseFailPointData(data, "collectionNS");
            return fpNss.isEmpty() || ns == fpNss;
        });
    return s;
}

void updateDocument(OperationContext* opCtx,
                    const CollectionPtr& collection,
                    const RecordId& oldLocation,
                    const Snapshotted<BSONObj>& oldDoc,
                    const BSONObj& newDoc,
                    const BSONObj* opDiff,
                    bool* indexesAffected,
                    OpDebug* opDebug,
                    CollectionUpdateArgs* args) {
    {
        auto status = collection->checkValidationAndParseResult(opCtx, newDoc);
        if (!status.isOK()) {
            if (validationLevelOrDefault(collection->getCollectionOptions().validationLevel) ==
                ValidationLevelEnum::strict) {
                uassertStatusOK(status);
            }
            // moderate means we have to check the old doc
            auto oldDocStatus = collection->checkValidationAndParseResult(opCtx, oldDoc.value());
            if (oldDocStatus.isOK()) {
                // transitioning from good -> bad is not ok
                uassertStatusOK(status);
            }
            // bad -> bad is ok in moderate mode
        }
    }

    auto& validationSettings = DocumentValidationSettings::get(opCtx);
    if (collection->getCollectionOptions().encryptedFieldConfig &&
        !collection->ns().isTemporaryReshardingCollection() &&
        !validationSettings.isSchemaValidationDisabled() &&
        !validationSettings.isSafeContentValidationDisabled()) {

        uassert(ErrorCodes::BadValue,
                str::stream() << "New document and old document both need to have " << kSafeContent
                              << " field.",
                compareSafeContentElem(oldDoc.value(), newDoc));
    }

    dassert(
        shard_role_details::getLocker(opCtx)->isCollectionLockedForMode(collection->ns(), MODE_IX));
    invariant(oldDoc.snapshotId() == shard_role_details::getRecoveryUnit(opCtx)->getSnapshotId());
    invariant(newDoc.isOwned());

    if (collection->needsCappedLock()) {
        Lock::ResourceLock heldUntilEndOfWUOW{
            opCtx, ResourceId(RESOURCE_METADATA, collection->ns()), MODE_X};
    }

    SnapshotId sid = shard_role_details::getRecoveryUnit(opCtx)->getSnapshotId();

    BSONElement oldId = oldDoc.value()["_id"];
    // We accept equivalent _id according to the collation defined in the collection. 'foo' and
    // 'Foo' could be equivalent but not byte-identical according to the collation of the
    // collection.
    BSONElementComparator eltCmp{BSONElementComparator::FieldNamesMode::kConsider,
                                 collection->getDefaultCollator()};
    if (!oldId.eoo() && eltCmp.evaluate(oldId != newDoc["_id"]))
        uasserted(13596, "in Collection::updateDocument _id mismatch");

    args->changeStreamPreAndPostImagesEnabledForCollection =
        collection->isChangeStreamPreAndPostImagesEnabled();

    if (collection->areRecordIdsReplicated()) {
        args->replicatedRecordId = oldLocation;
    }

    OplogUpdateEntryArgs onUpdateArgs(args, collection);
    const bool setNeedsRetryImageOplogField =
        args->storeDocOption != CollectionUpdateArgs::StoreDocOption::None;
    if (args->oplogSlots.empty() && setNeedsRetryImageOplogField && args->retryableWrite) {
        onUpdateArgs.retryableFindAndModifyLocation =
            RetryableFindAndModifyLocation::kSideCollection;
        // If the update is part of a retryable write and we expect to be storing the pre- or
        // post-image in a side collection, then we must reserve oplog slots in advance. We
        // expect to use the reserved oplog slots as follows, where TS is the greatest
        // timestamp of 'oplogSlots':
        // TS - 1: Tenant migrations and resharding will forge no-op image oplog entries and set
        //         the entry timestamps to TS - 1.
        // TS:     The timestamp given to the update oplog entry.
        args->oplogSlots = reserveOplogSlotsForRetryableFindAndModify(opCtx);
    } else {
        // Retryable findAndModify commands should not reserve oplog slots before entering this
        // function since tenant migrations and resharding rely on always being able to set
        // timestamps of forged pre- and post- image entries to timestamp of findAndModify - 1.
        invariant(!(args->retryableWrite && setNeedsRetryImageOplogField));
    }

    uassertStatusOK(collection->getRecordStore()->updateRecord(
        opCtx, oldLocation, newDoc.objdata(), newDoc.objsize()));

    // don't update the indexes if kUpdateNoIndexes has been specified.
    if (opDiff != kUpdateNoIndexes) {
        int64_t keysInserted = 0;
        int64_t keysDeleted = 0;

        uassertStatusOK(collection->getIndexCatalog()->updateRecord(opCtx,
                                                                    collection,
                                                                    args->preImageDoc,
                                                                    newDoc,
                                                                    opDiff,
                                                                    oldLocation,
                                                                    &keysInserted,
                                                                    &keysDeleted));
        if (indexesAffected) {
            *indexesAffected = (keysInserted > 0 || keysDeleted > 0);
        }

        if (opDebug) {
            opDebug->additiveMetrics.incrementKeysInserted(keysInserted);
            opDebug->additiveMetrics.incrementKeysDeleted(keysDeleted);
            // 'opDebug' may be deleted at rollback time in case of multi-document transaction.
            if (!opCtx->inMultiDocumentTransaction()) {
                shard_role_details::getRecoveryUnit(opCtx)->onRollback(
                    [opDebug, keysInserted, keysDeleted](OperationContext*) {
                        opDebug->additiveMetrics.incrementKeysInserted(-keysInserted);
                        opDebug->additiveMetrics.incrementKeysDeleted(-keysDeleted);
                    });
            }
        }
    }

    invariant(sid == shard_role_details::getRecoveryUnit(opCtx)->getSnapshotId());
    args->updatedDoc = newDoc;

    opCtx->getServiceContext()->getOpObserver()->onUpdate(opCtx, onUpdateArgs);
}

StatusWith<BSONObj> updateDocumentWithDamages(OperationContext* opCtx,
                                              const CollectionPtr& collection,
                                              const RecordId& loc,
                                              const Snapshotted<BSONObj>& oldDoc,
                                              const char* damageSource,
                                              const mutablebson::DamageVector& damages,
                                              const BSONObj* opDiff,
                                              bool* indexesAffected,
                                              OpDebug* opDebug,
                                              CollectionUpdateArgs* args) {
    dassert(
        shard_role_details::getLocker(opCtx)->isCollectionLockedForMode(collection->ns(), MODE_IX));
    invariant(oldDoc.snapshotId() == shard_role_details::getRecoveryUnit(opCtx)->getSnapshotId());
    invariant(collection->updateWithDamagesSupported());

    if (collection->areRecordIdsReplicated()) {
        args->replicatedRecordId = loc;
    }

    OplogUpdateEntryArgs onUpdateArgs(args, collection);
    const bool setNeedsRetryImageOplogField =
        args->storeDocOption != CollectionUpdateArgs::StoreDocOption::None;
    if (args->oplogSlots.empty() && setNeedsRetryImageOplogField && args->retryableWrite) {
        onUpdateArgs.retryableFindAndModifyLocation =
            RetryableFindAndModifyLocation::kSideCollection;
        // If the update is part of a retryable write and we expect to be storing the pre- or
        // post-image in a side collection, then we must reserve oplog slots in advance. We
        // expect to use the reserved oplog slots as follows, where TS is the greatest
        // timestamp of 'oplogSlots':
        // TS - 1: Tenant migrations and resharding will forge no-op image oplog entries and set
        //         the entry timestamps to TS - 1.
        // TS:     The timestamp given to the update oplog entry.
        args->oplogSlots = reserveOplogSlotsForRetryableFindAndModify(opCtx);
    } else {
        // Retryable findAndModify commands should not reserve oplog slots before entering this
        // function since tenant migrations and resharding rely on always being able to set
        // timestamps of forged pre- and post- image entries to timestamp of findAndModify - 1.
        invariant(!(args->retryableWrite && setNeedsRetryImageOplogField));
    }

    RecordData oldRecordData(oldDoc.value().objdata(), oldDoc.value().objsize());
    StatusWith<RecordData> recordData = collection->getRecordStore()->updateWithDamages(
        opCtx, loc, oldRecordData, damageSource, damages);
    if (!recordData.isOK())
        return recordData.getStatus();
    BSONObj newDoc = std::move(recordData.getValue()).releaseToBson().getOwned();

    args->updatedDoc = newDoc;
    args->changeStreamPreAndPostImagesEnabledForCollection =
        collection->isChangeStreamPreAndPostImagesEnabled();

    // don't update the indexes if kUpdateNoIndexes has been specified.
    if (opDiff != kUpdateNoIndexes) {
        int64_t keysInserted = 0;
        int64_t keysDeleted = 0;

        uassertStatusOK(collection->getIndexCatalog()->updateRecord(opCtx,
                                                                    collection,
                                                                    oldDoc.value(),
                                                                    args->updatedDoc,
                                                                    opDiff,
                                                                    loc,
                                                                    &keysInserted,
                                                                    &keysDeleted));
        if (indexesAffected) {
            *indexesAffected = (keysInserted > 0 || keysDeleted > 0);
        }

        if (opDebug) {
            opDebug->additiveMetrics.incrementKeysInserted(keysInserted);
            opDebug->additiveMetrics.incrementKeysDeleted(keysDeleted);
            // 'opDebug' may be deleted at rollback time in case of multi-document transaction.
            if (!opCtx->inMultiDocumentTransaction()) {
                shard_role_details::getRecoveryUnit(opCtx)->onRollback(
                    [opDebug, keysInserted, keysDeleted](OperationContext*) {
                        opDebug->additiveMetrics.incrementKeysInserted(-keysInserted);
                        opDebug->additiveMetrics.incrementKeysDeleted(-keysDeleted);
                    });
            }
        }
    }

    opCtx->getServiceContext()->getOpObserver()->onUpdate(opCtx, onUpdateArgs);
    return newDoc;
}

void deleteDocument(OperationContext* opCtx,
                    const CollectionPtr& collection,
                    StmtId stmtId,
                    const RecordId& loc,
                    OpDebug* opDebug,
                    bool fromMigrate,
                    bool noWarn,
                    StoreDeletedDoc storeDeletedDoc,
                    CheckRecordId checkRecordId,
                    RetryableWrite retryableWrite) {
    Snapshotted<BSONObj> doc = collection->docFor(opCtx, loc);
    deleteDocument(opCtx,
                   collection,
                   doc,
                   stmtId,
                   loc,
                   opDebug,
                   fromMigrate,
                   noWarn,
                   storeDeletedDoc,
                   checkRecordId);
}

void deleteDocument(OperationContext* opCtx,
                    const CollectionPtr& collection,
                    Snapshotted<BSONObj> doc,
                    StmtId stmtId,
                    const RecordId& loc,
                    OpDebug* opDebug,
                    bool fromMigrate,
                    bool noWarn,
                    StoreDeletedDoc storeDeletedDoc,
                    CheckRecordId checkRecordId,
                    RetryableWrite retryableWrite) {
    const auto& nss = collection->ns();

    if (collection->isCapped() && opCtx->inMultiDocumentTransaction()) {
        uasserted(ErrorCodes::IllegalOperation,
                  "Cannot remove from a capped collection in a multi-document transaction");
    }

    if (collection->needsCappedLock()) {
        Lock::ResourceLock heldUntilEndOfWUOW{opCtx, ResourceId(RESOURCE_METADATA, nss), MODE_X};
    }

    OplogDeleteEntryArgs deleteArgs;
    if (collection->areRecordIdsReplicated()) {
        deleteArgs.replicatedRecordId = loc;
    }

    // TODO(SERVER-80956): remove this call.
    opCtx->getServiceContext()->getOpObserver()->aboutToDelete(
        opCtx, collection, doc.value(), &deleteArgs);

    invariant(doc.value().isOwned(),
              str::stream() << "Document to delete is not owned: snapshot id: " << doc.snapshotId()
                            << " document: " << doc.value());

    deleteArgs.fromMigrate = fromMigrate;
    deleteArgs.changeStreamPreAndPostImagesEnabledForCollection =
        collection->isChangeStreamPreAndPostImagesEnabled();

    const bool shouldRecordPreImageForRetryableWrite =
        storeDeletedDoc == StoreDeletedDoc::On && retryableWrite == RetryableWrite::kYes;
    if (shouldRecordPreImageForRetryableWrite) {
        deleteArgs.retryableFindAndModifyLocation = RetryableFindAndModifyLocation::kSideCollection;
        deleteArgs.retryableFindAndModifyOplogSlots =
            reserveOplogSlotsForRetryableFindAndModify(opCtx);
    }

    int64_t keysDeleted = 0;
    collection->getIndexCatalog()->unindexRecord(
        opCtx, collection, doc.value(), loc, noWarn, &keysDeleted, checkRecordId);

    if (MONGO_unlikely(skipDeleteRecord.shouldFail())) {
        LOGV2_DEBUG(8096000,
                    3,
                    "Skipping deleting record in deleteDocument",
                    "recordId"_attr = loc,
                    "doc"_attr = doc.value().toString());
    } else {
        collection->getRecordStore()->deleteRecord(opCtx, loc);
    }

    opCtx->getServiceContext()->getOpObserver()->onDelete(
        opCtx, collection, stmtId, doc.value(), deleteArgs);

    if (opDebug) {
        opDebug->additiveMetrics.incrementKeysDeleted(keysDeleted);
        // 'opDebug' may be deleted at rollback time in case of multi-document transaction.
        if (!opCtx->inMultiDocumentTransaction()) {
            shard_role_details::getRecoveryUnit(opCtx)->onRollback(
                [opDebug, keysDeleted](OperationContext*) {
                    opDebug->additiveMetrics.incrementKeysDeleted(-keysDeleted);
                });
        }
    }
}

}  // namespace collection_internal
}  // namespace mongo
