/**
 *    Copyright (C) 2018-present MongoDB, Inc.
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

#include "mongo/db/s/migration_source_manager.h"

#include <absl/container/node_hash_map.h>
#include <boost/move/utility_core.hpp>
#include <boost/none.hpp>
#include <boost/optional/optional.hpp>
#include <mutex>
#include <string>
#include <tuple>
#include <type_traits>
#include <vector>

#include "mongo/base/error_codes.h"
#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonelement.h"
#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/timestamp.h"
#include "mongo/client/read_preference.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog_raii.h"
#include "mongo/db/client.h"
#include "mongo/db/concurrency/lock_manager_defs.h"
#include "mongo/db/database_name.h"
#include "mongo/db/keypattern.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/persistent_task_store.h"
#include "mongo/db/read_concern.h"
#include "mongo/db/repl/optime.h"
#include "mongo/db/repl/read_concern_args.h"
#include "mongo/db/repl/read_concern_level.h"
#include "mongo/db/repl/repl_client_info.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/db/s/auto_split_vector.h"
#include "mongo/db/s/chunk_operation_precondition_checks.h"
#include "mongo/db/s/commit_chunk_migration_gen.h"
#include "mongo/db/s/migration_chunk_cloner_source.h"
#include "mongo/db/s/migration_coordinator.h"
#include "mongo/db/s/migration_coordinator_document_gen.h"
#include "mongo/db/s/migration_util.h"
#include "mongo/db/s/range_deletion_util.h"
#include "mongo/db/s/shard_filtering_metadata_refresh.h"
#include "mongo/db/s/shard_metadata_util.h"
#include "mongo/db/s/sharding_logging.h"
#include "mongo/db/s/sharding_runtime_d_params_gen.h"
#include "mongo/db/s/sharding_statistics.h"
#include "mongo/db/s/type_shard_collection.h"
#include "mongo/db/s/type_shard_collection_gen.h"
#include "mongo/db/service_context.h"
#include "mongo/db/storage/recovery_unit.h"
#include "mongo/db/timeseries/bucket_catalog/bucket_catalog.h"
#include "mongo/db/transaction_resources.h"
#include "mongo/db/write_concern.h"
#include "mongo/logv2/log.h"
#include "mongo/logv2/log_attr.h"
#include "mongo/logv2/log_component.h"
#include "mongo/logv2/redaction.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/platform/compiler.h"
#include "mongo/s/catalog/sharding_catalog_client.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/catalog_cache_loader.h"
#include "mongo/s/chunk.h"
#include "mongo/s/chunk_manager.h"
#include "mongo/s/client/shard.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/grid.h"
#include "mongo/s/shard_key_pattern.h"
#include "mongo/s/shard_version_factory.h"
#include "mongo/s/sharding_state.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/clock_source.h"
#include "mongo/util/decorable.h"
#include "mongo/util/duration.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/namespace_string_util.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/str.h"
#include "mongo/util/time_support.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kShardingMigration

namespace mongo {
namespace {

const auto msmForCsr = CollectionShardingRuntime::declareDecoration<MigrationSourceManager*>();

// Wait at most this much time for the recipient to catch up sufficiently so critical section can be
// entered
const Hours kMaxWaitToEnterCriticalSectionTimeout(6);
const char kWriteConcernField[] = "writeConcern";
const WriteConcernOptions kMajorityWriteConcern(WriteConcernOptions::kMajority,
                                                WriteConcernOptions::SyncMode::UNSET,
                                                WriteConcernOptions::kWriteConcernTimeoutMigration);

/*
 * Calculates the max or min bound perform split+move in case the chunk in question is splittable.
 * If the chunk is not splittable, returns the bound of the existing chunk for the max or min.Finds
 * a max bound if needMaxBound is true and a min bound if forward is false.
 * MigrationSourceManager::MigrationSourceManager调用
 */
BSONObj computeOtherBound(OperationContext* opCtx,
                          const NamespaceString& nss,
                          const BSONObj& min,
                          const BSONObj& max,
                          const ShardKeyPattern& skPattern,
                          const long long maxChunkSizeBytes,
                          bool needMaxBound) {
    auto [splitKeys, _] = autoSplitVector(
        opCtx, nss, skPattern.toBSON(), min, max, maxChunkSizeBytes, 1, needMaxBound);
    if (splitKeys.size()) {
        return std::move(splitKeys.front());
    }
    
    return needMaxBound ? max : min;
}

/**
 * If `max` is the max bound of some chunk, returns that chunk. Otherwise, returns the chunk that
 * contains the key `max`.
 */
Chunk getChunkForMaxBound(const ChunkManager& cm, const BSONObj& max) {
    boost::optional<Chunk> chunkWithMaxBound;
    cm.forEachChunk([&](const auto& chunk) {
        if (chunk.getMax().woCompare(max) == 0) {
            chunkWithMaxBound.emplace(chunk);
            return false;
        }
        return true;
    });
    if (chunkWithMaxBound) {
        return *chunkWithMaxBound;
    }
    return cm.findIntersectingChunkWithSimpleCollation(max);
}

MONGO_FAIL_POINT_DEFINE(moveChunkHangAtStep1);
MONGO_FAIL_POINT_DEFINE(moveChunkHangAtStep2);
MONGO_FAIL_POINT_DEFINE(moveChunkHangAtStep3);
MONGO_FAIL_POINT_DEFINE(moveChunkHangAtStep4);
MONGO_FAIL_POINT_DEFINE(moveChunkHangAtStep5);
MONGO_FAIL_POINT_DEFINE(moveChunkHangAtStep6);

MONGO_FAIL_POINT_DEFINE(failMigrationCommit);
MONGO_FAIL_POINT_DEFINE(hangBeforeEnteringCriticalSection);
MONGO_FAIL_POINT_DEFINE(hangBeforeLeavingCriticalSection);
MONGO_FAIL_POINT_DEFINE(migrationCommitNetworkError);
MONGO_FAIL_POINT_DEFINE(hangBeforePostMigrationCommitRefresh);

}  // namespace

MigrationSourceManager* MigrationSourceManager::get(const CollectionShardingRuntime& csr) {
    return msmForCsr(csr);
}

std::shared_ptr<MigrationChunkClonerSource> MigrationSourceManager::getCurrentCloner(
    const CollectionShardingRuntime& csr) {
    auto msm = get(csr);
    if (!msm)
        return nullptr;
    return msm->_cloneDriver;
}

//ShardsvrMoveRangeCommand::_runImpl中构造 MigrationSourceManager


/**
 * MigrationSourceManager::MigrationSourceManager 构造函数的作用：
 * 创建和初始化 chunk 迁移源端管理器，负责整个迁移过程的协调和状态管理。
 * 
 * 核心功能：
 * 1. 迁移参数验证：验证并设置迁移请求的各项参数，包括源分片、目标分片、chunk范围等
 * 2. 元数据检查和锁定：获取并验证集合的分片元数据，确保迁移条件满足
 * 3. 冲突检测和处理：检查是否存在重叠的范围删除任务，等待冲突解决
 * 4. 范围边界计算：对于 moveRange 操作，自动计算缺失的最大值或最小值边界
 * 5. 注册管理器：在集合分片运行时注册当前管理器，建立迁移上下文
 * 6. 状态初始化：设置初始迁移状态和相关的时间统计、版本信息
 * 
 * 预检查项目：
 * - 分片键模式验证：确保请求的范围与分片键模式兼容
 * - chunk 边界验证：确保请求的范围在有效的 chunk 边界内
 * - 并发操作检查：确保没有冲突的迁移或删除操作
 * - 权限和状态验证：确保集合允许迁移操作
 * 
 * 资源管理：
 * - 异常安全：通过 ScopeGuard 确保异常时正确清理资源
 * - 锁管理：合理使用锁层次，避免死锁
 * - 状态跟踪：建立完整的迁移状态跟踪机制
 * 
 * 该构造函数是整个 chunk 迁移流程的起点，为后续的克隆、提交等阶段奠定基础。
 */
// ShardsvrMoveRangeCommand::_runImpl 中构造 MigrationSourceManager
MigrationSourceManager::MigrationSourceManager(OperationContext* opCtx,
                                               ShardsvrMoveRange&& request,
                                               WriteConcernOptions&& writeConcern,
                                               ConnectionString donorConnStr,
                                               HostAndPort recipientHost)
    : _opCtx(opCtx),
      _args(request),
      _writeConcern(writeConcern),
      _donorConnStr(std::move(donorConnStr)),
      _recipientHost(std::move(recipientHost)),
      _stats(ShardingStatistics::get(_opCtx)),
      _critSecReason(BSON("command"
                          << "moveChunk"
                          << "fromShard" << _args.getFromShard() << "toShard"
                          << _args.getToShard())),
      _moveTimingHelper(_opCtx,
                        "from",
                        _args.getCommandParameter(),
                        _args.getMin(),
                        _args.getMax(),
                        6,  // Total number of steps
                        _args.getToShard(),
                        _args.getFromShard()) {
    // 前置条件检查：确保构造时没有持有任何锁，避免死锁和不必要的锁争用
    invariant(!shard_role_details::getLocker(_opCtx)->isLocked());
    
    // Since the MigrationSourceManager is registered on the CSR from the constructor, another
    // thread can get it and abort the migration (and get a reference to the completion promise's
    // future). When this happens, since we throw an exception from the constructor, the destructor
    // will not run, so we have to do complete it here, otherwise we get a BrokenPromise
    // TODO (SERVER-92531): Use existing clean up infrastructure when aborting in early stages
    // 异常安全保护：由于 MigrationSourceManager 在构造函数中就会注册到 CSR 上，
    // 其他线程可能获取它并中止迁移（并获得完成 promise 的 future 引用）。
    // 当这种情况发生时，由于我们从构造函数抛出异常，析构函数不会运行，
    // 所以我们必须在这里完成它，否则会得到 BrokenPromise
    ScopeGuard scopedGuard([&] { _completion.emplaceValue(); });
    
    // {"t":{"$date":"2025-07-29T19:47:55.204+08:00"},"s":"I",  "c":"MIGRATE",  "id":22016,   "svc":"S", "ctx":"MoveChunk","msg":"Starting chunk migration donation","attr":{"requestParameters":{"_shardsvrMoveRange":"benchmark.yyztest","toShard":"shard2ReplSet","min":{"_id":{"$minKey":1}},"waitForDelete":false,"epoch":{"$oid":"6888b23d9b404274eb601360"},"collectionTimestamp":{"$timestamp":{"t":1753788989,"i":10}},"fromShard":"shard3ReplSet","maxChunkSizeBytes":10485760,"forceJumbo":0,"secondaryThrottle":false}}}
    // 记录迁移开始日志：包含完整的请求参数，用于调试和审计
    LOGV2(22016,
          "Starting chunk migration donation",
          "requestParameters"_attr = redact(_args.toBSON({})));

    // 完成第一步时间统计并检查测试断点
    _moveTimingHelper.done(1);
    moveChunkHangAtStep1.pauseWhileSet();

    // Make sure the latest placement version is recovered as of the time of the invocation of the
    // command.
    // 确保在命令调用时恢复最新的放置版本：
    // 功能：强制刷新当前分片的路由表缓存
    // 目的：确保迁移基于最新的集群元数据进行
    // 参数：boost::none 表示强制刷新而不检查特定版本
    onCollectionPlacementVersionMismatch(_opCtx, nss(), boost::none);

    // 获取当前分片ID，用于后续的迁移验证和日志记录
    const auto shardId = ShardingState::get(opCtx)->shardId();

    // Complete any unfinished migration pending recovery
    // 完成任何待恢复的未完成迁移：
    // 功能：清理和恢复之前中断的迁移操作
    // 重要性：确保不会有多个并发迁移操作干扰
    {
        // 排空待恢复的迁移：处理之前因故障中断的迁移
        migrationutil::drainMigrationsPendingRecovery(opCtx);

        // Since the moveChunk command is holding the ActiveMigrationRegistry and we just drained
        // all migrations pending recovery, now there cannot be any document in
        // config.migrationCoordinators.
        // 由于 moveChunk 命令持有 ActiveMigrationRegistry，并且我们刚刚排空了
        // 所有待恢复的迁移，现在 config.migrationCoordinators 中不能有任何文档。
        
        // 验证迁移协调器集合为空：确保没有残留的迁移状态
        PersistentTaskStore<MigrationCoordinatorDocument> store(
            NamespaceString::kMigrationCoordinatorsNamespace);
        invariant(store.count(opCtx) == 0);
    }

    // Compute the max or min bound in case only one is set (moveRange)
    // 计算最大或最小边界，以防只设置了一个（moveRange 操作）：
    // 功能：对于部分指定范围的 moveRange 操作，自动计算缺失的边界
    // 场景：用户只指定了 min 或只指定了 max，需要自动确定另一个边界
    if (!_args.getMax().has_value() || !_args.getMin().has_value()) {

        // 获取集合元数据：用于边界计算和验证
        const auto metadata = [&]() {
            // 获取集合的共享锁，允许并发读取但防止结构变更
            AutoGetCollection autoColl(_opCtx, nss(), MODE_IS);
            // 获取集合分片运行时的共享访问权限
            const auto scopedCsr =
                CollectionShardingRuntime::assertCollectionLockedAndAcquireShared(opCtx, nss());
            // 检查集合身份一致性，确保版本匹配
            const auto [metadata, _] = checkCollectionIdentity(
                _opCtx, nss(), _args.getEpoch(), _args.getCollectionTimestamp());
            return metadata;
        }();

        // 如果未指定最大值，根据最小值计算最大值，_shardsvrMoveRange 中不会携带max
        if (!_args.getMax().has_value()) {
            const auto& min = *_args.getMin();

            // 获取 chunk 管理器和包含最小值的 chunk
            const auto cm = metadata.getChunkManager();
            // 从路由表中获取min对应的chunk， 这样就可以获取该chunk的max
            const auto owningChunk = cm->findIntersectingChunkWithSimpleCollation(min);
            
            // 计算最优的最大边界：
            // 考虑分片键模式、chunk 大小限制等因素
            // needMaxBound=true 表示需要计算最大边界
            const auto max = computeOtherBound(_opCtx,
                                               nss(),
                                               min,
                                               // 这是这个chunk的max
                                               owningChunk.getMax(),
                                               cm->getShardKeyPattern(),
                                               _args.getMaxChunkSizeBytes(),
                                               true /* needMaxBound */);

            // 线程安全地更新参数：使用互斥锁保护并发访问
            stdx::lock_guard<Latch> lg(_mutex);
            _args.getMoveRangeRequestBase().setMax(max);
            _moveTimingHelper.setMax(max);
        } 
        // 如果未指定最小值，根据最大值计算最小值
        else if (!_args.getMin().has_value()) {
            const auto& max = *_args.getMax();

            // 获取最大边界对应的 chunk
            const auto cm = metadata.getChunkManager();
            const auto owningChunk = getChunkForMaxBound(*cm, max);
            
            // 计算最优的最小边界：
            // needMaxBound=false 表示需要计算最小边界
            const auto min = computeOtherBound(_opCtx,
                                               nss(),
                                               owningChunk.getMin(),
                                               max,
                                               cm->getShardKeyPattern(),
                                               _args.getMaxChunkSizeBytes(),
                                               false /* needMaxBound */);

            // 线程安全地更新参数
            stdx::lock_guard<Latch> lg(_mutex);
            _args.getMoveRangeRequestBase().setMin(min);
            _moveTimingHelper.setMin(min);
        }
    }

    // Snapshot the committed metadata from the time the migration starts and register the
    // MigrationSourceManager on the CSR.
    // 从迁移开始时快照已提交的元数据，并在 CSR 上注册 MigrationSourceManager：
    // 功能：获取集合元数据快照并注册迁移管理器
    // 原子性：整个操作在排他锁保护下进行，确保一致性
    const auto [collectionMetadata, collectionIndexInfo, collectionUUID] = [&] {
        // TODO (SERVER-71444): Fix to be interruptible or document exception.
        // 使用不可中断锁守护：确保关键操作的原子性
        UninterruptibleLockGuard noInterrupt(_opCtx);  // NOLINT.
        // 获取集合的共享锁进行元数据读取
        AutoGetCollection autoColl(_opCtx, nss(), MODE_IS);
        // 获取集合分片运行时的排他访问权限
        auto scopedCsr =
            CollectionShardingRuntime::assertCollectionLockedAndAcquireExclusive(opCtx, nss());

        // 检查并获取集合身份信息：确保集合版本一致性
        auto [metadata, indexInfo] = checkCollectionIdentity(
            _opCtx, nss(), _args.getEpoch(), _args.getCollectionTimestamp());

        // 获取集合UUID：用于标识特定的集合实例
        UUID collectionUUID = autoColl.getCollection()->uuid();

        // Atomically (still under the CSR lock held above) check whether migrations are allowed and
        // register the MigrationSourceManager on the CSR. This ensures that interruption due to the
        // change of allowMigrations to false will properly serialise and not allow any new MSMs to
        // be running after the change.
        // 原子地（仍在上面持有的 CSR 锁下）检查是否允许迁移并在 CSR 上注册 MigrationSourceManager。
        // 这确保由于 allowMigrations 更改为 false 而导致的中断将正确序列化，
        // 并且在更改后不允许任何新的 MSM 运行。
        
        // 检查集合是否允许迁移操作
        uassert(ErrorCodes::ConflictingOperationInProgress,
                "Collection is undergoing changes so moveChunk is not allowed.",
                metadata.allowMigrations());

        // 注册当前迁移源管理器到集合分片运行时
        // 这建立了迁移上下文，允许其他组件感知正在进行的迁移
        _scopedRegisterer.emplace(this, *scopedCsr);

        // 返回获取的元数据信息
        return std::make_tuple(
            std::move(metadata), std::move(indexInfo), std::move(collectionUUID));
    }();

    // Drain the execution/cancellation of any existing range deletion task overlapping with the
    // targeted range (a task issued by a previous migration may still be present when the migration
    // gets interrupted post-commit).
    // 排空与目标范围重叠的任何现有范围删除任务的执行/取消
    // （当迁移在提交后被中断时，之前迁移发出的任务可能仍然存在）。
    
    // 构建目标迁移范围
    const ChunkRange range(*_args.getMin(), *_args.getMax());
    // 设置范围删除等待的截止时间
    const auto rangeDeletionWaitDeadline = opCtx->getServiceContext()->getFastClockSource()->now() +
        Milliseconds(drainOverlappingRangeDeletionsOnStartTimeoutMS.load());
        
    // CollectionShardingRuntime::waitForClean() allows to sync on tasks already registered on the
    // RangeDeleterService, but may miss pending ones in case this code runs after a failover. The
    // enclosing while loop allows to address such a gap.
    // CollectionShardingRuntime::waitForClean() 允许同步已在 RangeDeleterService 上注册的任务，
    // 但在故障转移后运行此代码时可能会错过待处理的任务。
    // 包围的 while 循环允许解决这种间隙。
    
    // 冲突检测和等待循环：确保没有重叠的删除任务
    while (rangedeletionutil::checkForConflictingDeletions(opCtx, range, collectionUUID)) {
        // 检查范围删除器是否被禁用
        uassert(ErrorCodes::ResumableRangeDeleterDisabled,
                "Failing migration because the disableResumableRangeDeleter server "
                "parameter is set to true on the donor shard, which contains range "
                "deletion tasks overlapping with the incoming range.",
                !disableResumableRangeDeleter.load());

        // 记录延迟原因：帮助调试和监控
        LOGV2(9197000,
              "Migration start deferred because the requested range overlaps with one or more "
              "ranges already scheduled for deletion",
              logAttrs(nss()),
              "range"_attr = redact(range.toString()));

        // 等待冲突的范围删除任务完成
        auto status = CollectionShardingRuntime::waitForClean(
            opCtx, nss(), collectionUUID, range, rangeDeletionWaitDeadline);

        // 检查是否超时
        if (status.isOK() &&
            opCtx->getServiceContext()->getFastClockSource()->now() >= rangeDeletionWaitDeadline) {
            status = Status(
                ErrorCodes::ExceededTimeLimit,
                "Failed to start new migration - a conflicting range deletion is still pending");
        }

        // 确保等待操作成功
        uassertStatusOK(status);

        // If the filtering metadata was cleared while the range deletion task was ongoing, then
        // 'waitForClean' would return immediately even though there really is an ongoing range
        // deletion task. For that case, we loop again until there is no conflicting task in
        // config.rangeDeletions
        // 如果在范围删除任务进行时清除了过滤元数据，那么即使确实有正在进行的范围删除任务，
        // 'waitForClean' 也会立即返回。对于这种情况，我们再次循环直到 config.rangeDeletions 中没有冲突任务
        
        // 短暂休眠后重新检查，避免忙等待
        opCtx->sleepFor(Milliseconds(1000));
    }

    // 验证分片键模式：确保请求的范围与分片键兼容
    checkShardKeyPattern(_opCtx,
                         nss(),
                         collectionMetadata,
                         collectionIndexInfo,
                         ChunkRange(*_args.getMin(), *_args.getMax()));
                         
    // 验证范围边界：确保请求的范围在有效的 chunk 边界内
    checkRangeWithinChunk(_opCtx,
                          nss(),
                          collectionMetadata,
                          collectionIndexInfo,
                          ChunkRange(*_args.getMin(), *_args.getMax()));

    // 保存关键的版本和标识信息：用于后续的一致性检查
    _collectionEpoch = _args.getEpoch();
    _collectionUUID = collectionUUID;
    _collectionTimestamp = _args.getCollectionTimestamp();

    // 获取当前 chunk 的版本信息：用于版本控制和冲突检测
    _chunkVersion = collectionMetadata.getChunkManager()
                        ->findIntersectingChunkWithSimpleCollation(*_args.getMin())
                        .getLastmod();

    // 完成第二步时间统计并检查测试断点
    _moveTimingHelper.done(2);
    moveChunkHangAtStep2.pauseWhileSet();
    
    // 取消异常保护：表示构造函数成功完成
    scopedGuard.dismiss();
}

MigrationSourceManager::~MigrationSourceManager() {
    invariant(!_cloneDriver);
    _stats.totalDonorMoveChunkTimeMillis.addAndFetch(_entireOpTimer.millis());

    if (_state == kDone) {
        _completion.emplaceValue();
    } else {
        std::string errMsg = "Migration not completed";
        if (_coordinator) {
            const auto& migrationId = _coordinator->getMigrationId();
            errMsg = str::stream() << "Migration " << migrationId << " not completed";
        }
        auto status = Status{ErrorCodes::Interrupted, errMsg};
        _completion.setError(status);
    }
}


/**
 * MigrationSourceManager::startClone 的作用：
 * 启动 chunk 迁移的数据克隆阶段，这是迁移状态机的第三个阶段（创建 → 克隆 → 追赶 → 临界区 → 提交）。
 * 
 * 核心功能：
 * 1. 迁移状态管理：将迁移状态从 kCreated 转换为 kCloning，标记克隆阶段开始
 * 2. 克隆驱动器创建：初始化 MigrationChunkClonerSource，负责实际的数据传输
 * 3. 迁移协调器初始化：创建 MigrationCoordinator 管理整个迁移生命周期
 * 4. 日志记录：记录迁移开始事件到 config.changelog 集合，用于审计和监控
 * 5. 读关注点设置：确保克隆过程中的数据一致性和可见性
 * 6. 数据克隆启动：调用克隆驱动器开始实际的数据传输过程
 * 
 * 执行流程：
 * 1. 状态验证和错误处理准备
 * 2. 统计计数器更新和变更日志记录
 * 3. 元数据获取和集合锁定
 * 4. 克隆驱动器创建和注册
 * 5. 迁移协调器初始化
 * 6. 路由信息刷新和读关注点设置
 * 7. 协调器和克隆驱动器启动
 * 
 * 该方法是 chunk 迁移数据传输阶段的核心入口，确保了迁移过程的正确性和一致性。
 */
void MigrationSourceManager::startClone() {
    // 确保调用时没有持有任何锁，避免死锁
    invariant(!shard_role_details::getLocker(_opCtx)->isLocked());
    // 确保当前状态为 kCreated，即迁移刚刚创建完成
    invariant(_state == kCreated);
    
    // 错误处理守护：如果函数执行过程中发生异常，自动清理资源
    ScopeGuard scopedGuard([&] { _cleanupOnError(); });
    
    // 更新统计计数器：记录开始的迁移数量
    // 这个计数器用于监控和性能分析，包括成功和失败的迁移
    _stats.countDonorMoveChunkStarted.addAndFetch(1);

    // 记录迁移开始事件到 config.changelog 集合
    // 这是一个持久化的变更日志，用于审计、监控和问题追踪
    // 使用 majority 写关注点确保日志在大多数节点上持久化
    uassertStatusOK(ShardingLogging::get(_opCtx)->logChangeChecked(
        _opCtx,
        "moveChunk.start",  // 事件类型
        nss(),             // 命名空间
        BSON("min" << *_args.getMin() << "max" << *_args.getMax() << "from" << _args.getFromShard()
                   << "to" << _args.getToShard()),  // 迁移详细信息
        ShardingCatalogClient::kMajorityWriteConcern));  // 写关注点

    // 重置克隆和提交阶段的计时器，用于性能监控
    _cloneAndCommitTimer.reset();

    // 获取复制集配置，确定是否启用了复制
    auto replCoord = repl::ReplicationCoordinator::get(_opCtx);
    auto replEnabled = replCoord->getSettings().isReplSet();

    {
        // 获取当前集合的元数据并检查冲突错误
        // 这确保迁移开始时集合状态是一致的
        const auto metadata = _getCurrentMetadataAndCheckForConflictingErrors();

        // 获取集合锁：
        // - 如果启用复制：使用 MODE_IX (意向排他锁)，允许并发读
        // - 如果未启用复制：使用 MODE_X (排他锁)，独占访问
        // 设置锁获取超时，避免无限等待
        AutoGetCollection autoColl(_opCtx,
                                   nss(),
                                   replEnabled ? MODE_IX : MODE_X,
                                   AutoGetCollection::Options{}.deadline(
                                       _opCtx->getServiceContext()->getPreciseClockSource()->now() +
                                       Milliseconds(migrationLockAcquisitionMaxWaitMS.load())));

        // 获取集合分片运行时的排他访问权限
        // 这确保对集合分片状态的修改是原子性的
        auto scopedCsr =
            CollectionShardingRuntime::assertCollectionLockedAndAcquireExclusive(_opCtx, nss());

        // Having the metadata manager registered on the collection sharding state is what indicates
        // that a chunk on that collection is being migrated to the OpObservers. With an active
        // migration, write operations require the cloner to be present in order to track changes to
        // the chunk which needs to be transmitted to the recipient.
        // 在集合分片状态上注册元数据管理器表明该集合上的 chunk 正在迁移给 OpObservers。
        // 有了活跃的迁移，写操作需要克隆器的存在来跟踪需要传输给接收方的 chunk 变更。
        {
            // 保护克隆驱动器的创建和赋值，确保线程安全
            stdx::lock_guard<Latch> lg(_mutex);
            
            // 创建 chunk 克隆源驱动器：负责实际的数据传输逻辑
            // 包含迁移参数、写关注点、分片键模式、网络连接信息等
            _cloneDriver = std::make_shared<MigrationChunkClonerSource>(_opCtx,
                                                                        _args,              // 迁移参数
                                                                        _writeConcern,      // 写关注点
                                                                        metadata.getKeyPattern(),  // 分片键模式
                                                                        _donorConnStr,      // 源分片连接串
                                                                        _recipientHost);    // 目标分片主机
        }

        // 创建迁移协调器：管理整个迁移过程的状态和协调
        // 包含会话ID、分片信息、集合信息、chunk版本等关键元数据
        _coordinator.emplace(_cloneDriver->getSessionId(),     // 会话标识
                             _args.getFromShard(),             // 源分片
                             _args.getToShard(),               // 目标分片
                             nss(),                            // 命名空间
                             *_collectionUUID,                 // 集合UUID
                             ChunkRange(*_args.getMin(), *_args.getMax()),  // chunk范围
                             *_chunkVersion,                   // chunk版本
                             KeyPattern(metadata.getKeyPattern()),  // 键模式
                             _args.getWaitForDelete());        // 是否等待删除

        // 更新迁移状态为克隆中
        _state = kCloning;
    }

    // Refreshing the collection routing information after starting the clone driver will give us a
    // stable view on whether the recipient is owning other chunks of the collection (a condition
    // that will be later evaluated).
    // 在启动克隆驱动器后刷新集合路由信息将给我们一个稳定的视图，
    // 了解接收方是否拥有该集合的其他 chunk（这是一个稍后会被评估的条件）。
    uassertStatusOK(Grid::get(_opCtx)->catalogCache()->getCollectionRoutingInfoWithPlacementRefresh(
        _opCtx, nss()));

    // 如果启用了复制，设置适当的读关注点
    if (replEnabled) {
        // 创建本地读关注点参数，使用最后应用的操作时间
        // 这确保克隆过程看到一致的数据视图
        auto const readConcernArgs = repl::ReadConcernArgs(
            replCoord->getMyLastAppliedOpTime(),           // 最后应用的操作时间
            repl::ReadConcernLevel::kLocalReadConcern);    // 本地读关注级别
        
        // 等待读关注点满足，确保数据可见性
        uassertStatusOK(waitForReadConcern(_opCtx, readConcernArgs, DatabaseName(), false));

        // 设置准备冲突行为，处理事务相关的读冲突
        setPrepareConflictBehaviorForReadConcern(
            _opCtx, readConcernArgs, PrepareConflictBehavior::kEnforce);
    }

    // 启动迁移协调器：初始化迁移状态并持久化相关信息
    _coordinator->startMigration(_opCtx);

    // 启动实际的数据克隆过程
    // 传入迁移ID、会话信息和事务号，确保克隆过程的可追踪性和一致性
    // MigrationChunkClonerSource::startClone() 方法会处理数据传输、增量同步等逻辑
    uassertStatusOK(_cloneDriver->startClone(_opCtx,
                                             _coordinator->getMigrationId(),   // 迁移唯一标识
                                             _coordinator->getLsid(),          // 逻辑会话ID
                                             _coordinator->getTxnNumber()));   // 事务号

    // 更新时间统计：标记第3步完成
    _moveTimingHelper.done(3);
    
    // 测试断点：允许在步骤3暂停，用于测试和调试
    moveChunkHangAtStep3.pauseWhileSet();
    
    // 取消错误清理守护，表示函数成功执行
    scopedGuard.dismiss();
}

/**
 * MigrationSourceManager::awaitToCatchUp 函数的作用：
 * 等待接收端克隆过程追赶上源端的数据变更，确保进入关键区域前数据同步差距在可接受范围内。
 * 
 * 核心功能：
 * 1. 克隆完成度检查：等待克隆驱动器确认接收端已经获取了足够的初始数据
 * 2. 增量同步验证：确保接收端处理的增量变更（xferMods）已经追赶到合适水平
 * 3. 关键区域准备：为进入迁移的关键区域阶段做准备，此阶段将阻塞写操作
 * 4. 时间统计更新：记录克隆阶段的耗时并重置计时器为下一阶段准备
 * 5. 状态转换管理：将迁移状态从 kCloning 转换为 kCloneCaughtUp
 * 
 * 等待策略：
 * - 阻塞等待：调用克隆驱动器的等待方法直到条件满足
 * - 超时保护：设置最大等待时间（默认6小时）防止无限等待
 * - 智能判断：克隆驱动器内部评估何时适合进入关键区域
 * 
 * 性能考虑：
 * - 平衡数据完整性和迁移效率：不需要100%同步，只需达到可管理的差距
 * - 减少关键区域时间：通过预先同步减少后续阻塞写操作的时间
 * - 网络和I/O优化：在网络条件允许的情况下尽可能多地传输数据
 * 
 * 错误处理：
 * - 超时处理：如果在指定时间内无法达到同步条件则失败
 * - 网络异常：处理与接收端通信过程中的网络问题
 * - 状态异常：确保迁移状态的一致性和正确性
 * 
 * 该函数是迁移状态机中的关键检查点，确保在进入影响性能的关键区域前数据已基本同步。
 */
void MigrationSourceManager::awaitToCatchUp() {
    // 前置条件检查：确保调用时没有持有任何锁，避免死锁风险
    // 这是必要的，因为后续可能需要进行网络通信和长时间等待
    invariant(!shard_role_details::getLocker(_opCtx)->isLocked());
    
    // 状态验证：确保当前处于克隆阶段，这是进入追赶阶段的前置条件
    // kCloning 状态表示数据克隆正在进行中，可以开始评估同步进度
    invariant(_state == kCloning);
    
    // 错误处理守护：如果函数执行过程中发生异常，自动清理资源
    // 确保迁移状态的一致性，避免资源泄漏和不一致状态
    ScopeGuard scopedGuard([&] { _cleanupOnError(); });
    
    // 克隆阶段时间统计更新：
    // 功能：记录从克隆开始到当前时刻的总耗时
    // 用途：性能监控、调优分析、运维观察
    // 时机：在进入等待阶段前记录，确保统计的准确性
    _stats.totalDonorChunkCloneTimeMillis.addAndFetch(_cloneAndCommitTimer.millis());
    
    // 重置克隆和提交计时器：
    // 目的：为下一个阶段（关键区域）的时间统计做准备
    // 重要性：分阶段的时间统计有助于识别性能瓶颈
    _cloneAndCommitTimer.reset();

    // Block until the cloner deems it appropriate to enter the critical section.
    // 阻塞等待直到克隆器认为适合进入关键区域：
    // 
    // 核心逻辑：调用克隆驱动器的等待方法，让其内部判断同步进度
    // 参数1：操作上下文，提供中断机制和资源管理
    // 参数2：最大等待时间（kMaxWaitToEnterCriticalSectionTimeout = 6小时）
    // 
    // 内部机制：
    // - 克隆驱动器会检查接收端的克隆进度
    // - 评估未传输的增量变更（xferMods）数量
    // - 考虑网络条件和传输速率
    // - 确保进入关键区域后能够快速完成剩余同步
    // 
    // 等待条件（由克隆驱动器内部决定）：
    // - 初始数据克隆基本完成
    // - 增量变更队列长度在可控范围内
    // - 接收端响应正常，网络状况良好
    // - 预估关键区域时间在可接受范围内
    uassertStatusOK(_cloneDriver->awaitUntilCriticalSectionIsAppropriate(
        _opCtx, kMaxWaitToEnterCriticalSectionTimeout));

    // 状态转换：从克隆中转换为克隆已追赶
    // 意义：标记初始数据传输和大部分增量同步已完成
    // 后续：下一步将进入关键区域，开始阻塞写操作
    _state = kCloneCaughtUp;
    
    // 完成第4步时间统计：
    // 步骤4：awaitToCatchUp 阶段完成
    // 用途：迁移进度跟踪和性能分析
    _moveTimingHelper.done(4);
    
    // 测试断点：允许在步骤4暂停，用于测试和调试
    // 在生产环境中此断点通常不会激活
    moveChunkHangAtStep4.pauseWhileSet(_opCtx);
    
    // 取消错误清理守护：表示函数成功执行完成
    // 如果执行到这里，说明同步检查通过，可以安全进入下一阶段
    scopedGuard.dismiss();
}

/**
 * MigrationSourceManager::enterCriticalSection
 * 该函数用于在分片迁移流程中，源分片进入关键区域（critical section）。
 * 进入关键区域后，源分片会阻塞对迁移 chunk 范围的所有写操作，确保数据一致性，
 * 并为迁移所有权切换做准备。此操作会通知副本集成员刷新路由信息，保证迁移期间的因果一致性。
 * migrationSourceManager.enterCriticalSection();
 */
void MigrationSourceManager::enterCriticalSection() {
    // 保证当前没有持有锁，防止死锁或状态异常
    invariant(!shard_role_details::getLocker(_opCtx)->isLocked());
    // 保证迁移状态为 kCloneCaughtUp，确保已完成数据克隆和追赶
    invariant(_state == kCloneCaughtUp);

    // 构造异常安全保护，若后续步骤出错可自动清理
    ScopeGuard scopedGuard([&] { _cleanupOnError(); });

    // 统计并累加本次迁移的克隆阶段耗时
    _stats.totalDonorChunkCloneTimeMillis.addAndFetch(_cloneAndCommitTimer.millis());
    // 重置计时器，为关键区域阶段重新计时
    _cloneAndCommitTimer.reset();

    // 测试挂点，可用于调试或模拟关键区域前的暂停
    hangBeforeEnteringCriticalSection.pauseWhileSet();

    // 获取集合的分片路由信息
    const auto [cm, _] =
        uassertStatusOK(Grid::get(_opCtx)->catalogCache()->getCollectionRoutingInfo(_opCtx, nss()));

    // 检查目标分片是否首次拥有 chunk，如果是则写入 oplog 事件用于 change stream
    if (!cm.getVersion(_args.getToShard()).isSet()) {
        migrationutil::notifyChangeStreamsOnRecipientFirstChunk(
            _opCtx, nss(), _args.getFromShard(), _args.getToShard(), _collectionUUID);

        // 等待上述 oplog 事件被多数派节点确认，保证副本集一致性
        WriteConcernResult ignoreResult;
        auto latestOpTime = repl::ReplClientInfo::forClient(_opCtx->getClient()).getLastOp();
        uassertStatusOK(waitForWriteConcern(
            _opCtx, latestOpTime, WriteConcerns::kMajorityWriteConcernNoTimeout, &ignoreResult));
    }

    // 记录进入关键区域的日志，便于性能分析和问题排查
    LOGV2_DEBUG_OPTIONS(4817402,
                        2,
                        {logv2::LogComponent::kShardMigrationPerf},
                        "Starting critical section",
                        "migrationId"_attr = _coordinator->getMigrationId());

    // 设置关键区域标志，阻塞迁移范围的写操作
    _critSec.emplace(_opCtx, nss(), _critSecReason);

    // 更新迁移状态为关键区域
    _state = kCriticalSection;

    // 向副本集持久化关键区域信号，促使副本集成员刷新路由表并阻塞在关键区域
    // 这样可以保证因果一致性，防止 stale mongos 访问到未完成迁移的数据
    uassertStatusOKWithContext(
        shardmetadatautil::updateShardCollectionsEntry(
            _opCtx,
            BSON(ShardCollectionType::kNssFieldName
                 << NamespaceStringUtil::serialize(nss(), SerializationContext::stateDefault())),
            BSON("$inc" << BSON(ShardCollectionType::kEnterCriticalSectionCounterFieldName << 1)),
            false /*upsert*/),
        "Persist critical section signal for secondaries");

    // 记录成功进入关键区域的日志
    LOGV2(22017,
          "Migration successfully entered critical section",
          "migrationId"_attr = _coordinator->getMigrationId());

    // 关键区域进入成功，撤销异常保护
    scopedGuard.dismiss();
}

/**
 * MigrationSourceManager::commitChunkOnRecipient 函数的作用：
 * 通知接收端提交 chunk 迁移，完成数据传输的最终确认阶段，确保接收端已准备好接管 chunk 的所有权。
 * 
 * 核心功能：
 * 1. 最终数据传输：通知接收端获取并应用所有剩余的增量变更数据
 * 2. 提交确认：等待接收端确认所有数据已成功接收并持久化
 * 3. 统计信息收集：获取接收端的克隆统计信息，用于审计和监控
 * 4. 状态转换管理：将迁移状态从 kCriticalSection 转换为 kCloneCompleted
 * 5. 错误处理和恢复：处理提交过程中的异常并启动异步恢复机制
 * 
 * 执行条件：
 * - 必须在关键区域（Critical Section）中执行，此时写操作已被阻塞
 * - 源端和接收端都已进入迁移的最终阶段
 * - 所有初始数据和大部分增量数据已传输完成
 * 
 * 提交过程：
 * - 调用克隆驱动器的 commitClone 方法
 * - 接收端执行最终的数据同步和验证
 * - 接收端准备接管 chunk 的读写责任
 * - 返回详细的克隆统计信息
 * 
 * 错误处理：
 * - 支持故障点测试模拟提交失败
 * - 提交失败时自动启动异步恢复流程
 * - 确保迁移状态的一致性和可恢复性
 * 
 * 时间节点：
 * - 在进入关键区域后、配置服务器元数据提交前执行
 * - 是迁移流程中数据传输的最后一步
 * - 为后续的元数据更新奠定基础
 * 
 * 该函数是 chunk 迁移关键路径上的重要环节，确保数据传输的完整性和一致性。
 */
void MigrationSourceManager::commitChunkOnRecipient() {
    // 前置条件检查：确保调用时没有持有任何锁，避免死锁风险
    // 这是必要的，因为可能需要进行网络通信和等待操作
    invariant(!shard_role_details::getLocker(_opCtx)->isLocked());
    
    // 状态验证：确保当前处于关键区域阶段
    // kCriticalSection 状态表示写操作已被阻塞，可以安全进行最终提交
    invariant(_state == kCriticalSection);
    
    // 错误处理和恢复守护：
    // 功能1：如果提交过程中发生异常，自动清理资源
    // 功能2：启动异步恢复机制，确保迁移最终能够完成或回滚
    ScopeGuard scopedGuard([&] {
        _cleanupOnError();  // 清理当前迁移状态
        // 启动异步恢复流程：在后台持续尝试恢复迁移直到成功或主节点降级
        migrationutil::asyncRecoverMigrationUntilSuccessOrStepDown(_opCtx,
                                                                   _args.getCommandParameter());
    });

    // Tell the recipient shard to fetch the latest changes.
    // 通知接收端分片获取最新的变更：
    // 
    // 核心操作：调用克隆驱动器的提交方法
    // 内部机制：
    // - 发送 _recvChunkCommit 命令到接收端
    // - 接收端执行最终的数据同步和验证
    // - 接收端准备接管 chunk 的所有权
    // - 返回详细的克隆统计信息
    // 
    // 执行内容：
    // - 传输所有剩余的增量变更（xferMods）
    // - 传输剩余的会话数据（session data）
    // - 接收端执行数据完整性验证
    // - 接收端持久化所有接收的数据
    auto commitCloneStatus = _cloneDriver->commitClone(_opCtx);

    // 故障点测试：模拟提交失败的情况
    // 功能：允许在测试环境中验证错误处理和恢复机制
    // 条件：故障点被激活且正常情况下提交会成功
    if (MONGO_unlikely(failMigrationCommit.shouldFail()) && commitCloneStatus.isOK()) {
        // 人为制造提交失败状态，用于测试异常处理路径
        commitCloneStatus = {ErrorCodes::InternalError,
                             "Failing _recvChunkCommit due to failpoint."};
    }

    // 提交结果验证：确保接收端成功确认了数据提交
    // 失败处理：如果提交失败，抛出异常并触发错误处理流程
    uassertStatusOKWithContext(commitCloneStatus, "commit clone failed");
    
    // 克隆统计信息保存：
    // 功能：从接收端返回的响应中提取克隆统计信息
    // 内容：包括克隆的文档数量、字节数、会话数据等详细统计
    // 用途：用于迁移完成后的日志记录、监控和审计
    _recipientCloneCounts = commitCloneStatus.getValue()["counts"].Obj().getOwned();

    // 状态转换：从关键区域转换为克隆已完成
    // 意义：标记数据传输阶段完全结束，准备进行元数据更新
    // 后续：下一步将向配置服务器提交元数据变更
    _state = kCloneCompleted;
    
    // 完成第5步时间统计：
    // 步骤5：commitChunkOnRecipient 阶段完成
    // 用途：迁移性能分析和进度跟踪
    _moveTimingHelper.done(5);
    
    // 测试断点：允许在步骤5暂停，用于测试和调试
    // 在生产环境中此断点通常不会激活
    moveChunkHangAtStep5.pauseWhileSet();
    
    // 取消错误处理守护：表示提交成功完成
    // 如果执行到这里，说明接收端已成功确认数据接收，可以继续下一阶段
    scopedGuard.dismiss();
}

/*
该函数负责在迁移最后阶段将 chunk 元数据变更提交到 config server，完成所有权切换。
包括进入关键区域、发送元数据变更请求、处理响应、刷新本地元数据、通知目标分片释放关键区域、清理本地状态和 orphaned 数据等。
保证集群元数据与实际数据分布一致，是迁移流程安全收尾和一致性的关键步骤。
*/
void MigrationSourceManager::commitChunkMetadataOnConfig() {
    // 确保没有持有锁，防止死锁
    invariant(!shard_role_details::getLocker(_opCtx)->isLocked());
    // 确保迁移状态为克隆完成
    invariant(_state == kCloneCompleted);

    // 构造异常保护，迁移失败时自动清理本地状态并尝试恢复
    ScopeGuard scopedGuard([&] {
        _cleanupOnError();
        migrationutil::asyncRecoverMigrationUntilSuccessOrStepDown(_opCtx, nss());
    });

    // 如果源分片还有剩余 chunk，提升其中一个 chunk 的版本，通知其他节点刷新元数据
    BSONObjBuilder builder;
    {
        const auto metadata = _getCurrentMetadataAndCheckForConflictingErrors();

        auto migratedChunk = MigratedChunkType(*_chunkVersion, *_args.getMin(), *_args.getMax());

        CommitChunkMigrationRequest request(nss(),
                                            _args.getFromShard(),
                                            _args.getToShard(),
                                            migratedChunk,
                                            metadata.getCollPlacementVersion());

        request.serialize({}, &builder);
        builder.append(kWriteConcernField, kMajorityWriteConcern.toBSON());
    }

    // 进入关键区域的提交阶段，阻塞读写操作
    _critSec->enterCommitPhase();

    // 更新迁移状态为正在提交到 config
    _state = kCommittingOnConfig;

    Timer t;

    // 向 config server 发送 chunk 元数据变更请求
    auto commitChunkMigrationResponse =
        Grid::get(_opCtx)->shardRegistry()->getConfigShard()->runCommandWithFixedRetryAttempts(
            _opCtx,
            ReadPreferenceSetting{ReadPreference::PrimaryOnly},
            DatabaseName::kAdmin,
            builder.obj(),
            Shard::RetryPolicy::kIdempotent);

    // 测试挂点，模拟网络错误
    if (MONGO_unlikely(migrationCommitNetworkError.shouldFail())) {
        commitChunkMigrationResponse = Status(
            ErrorCodes::InternalError, "Failpoint 'migrationCommitNetworkError' generated error");
    }

    // 检查 config server 响应状态
    Status migrationCommitStatus =
        Shard::CommandResponse::getEffectiveStatus(commitChunkMigrationResponse);

    if (!migrationCommitStatus.isOK()) {
        {
            // 清除本地分片过滤元数据
            UninterruptibleLockGuard noInterrupt(_opCtx);  // NOLINT.
            AutoGetCollection autoColl(_opCtx, nss(), MODE_IX);
            CollectionShardingRuntime::assertCollectionLockedAndAcquireExclusive(_opCtx, nss())
                ->clearFilteringMetadata(_opCtx);
        }
        scopedGuard.dismiss();
        _cleanup(false);
        migrationutil::asyncRecoverMigrationUntilSuccessOrStepDown(_opCtx, nss());
        uassertStatusOK(migrationCommitStatus);
    }

    // 异步通知目标分片释放关键区域
    _coordinator->launchReleaseRecipientCriticalSection(_opCtx);

    hangBeforePostMigrationCommitRefresh.pauseWhileSet();

    try {
        LOGV2_DEBUG_OPTIONS(4817404,
                            2,
                            {logv2::LogComponent::kShardMigrationPerf},
                            "Starting post-migration commit refresh on the shard",
                            "migrationId"_attr = _coordinator->getMigrationId());

        // 强制刷新本地分片过滤元数据
        forceShardFilteringMetadataRefresh(_opCtx, nss());

        LOGV2_DEBUG_OPTIONS(4817405,
                            2,
                            {logv2::LogComponent::kShardMigrationPerf},
                            "Finished post-migration commit refresh on the shard",
                            "migrationId"_attr = _coordinator->getMigrationId());
    } catch (const DBException& ex) {
        LOGV2_DEBUG_OPTIONS(4817410,
                            2,
                            {logv2::LogComponent::kShardMigrationPerf},
                            "Finished post-migration commit refresh on the shard with error",
                            "migrationId"_attr = _coordinator->getMigrationId(),
                            "error"_attr = redact(ex));
        {
            // 清除本地分片过滤元数据
            UninterruptibleLockGuard noInterrupt(_opCtx);  // NOLINT.
            AutoGetCollection autoColl(_opCtx, nss(), MODE_IX);
            CollectionShardingRuntime::assertCollectionLockedAndAcquireExclusive(_opCtx, nss())
                ->clearFilteringMetadata(_opCtx);
        }
        scopedGuard.dismiss();
        _cleanup(false);
        // 尝试恢复 chunk 版本
        onCollectionPlacementVersionMismatchNoExcept(_opCtx, nss(), boost::none).ignore();
        throw;
    }

    // 检查是否迁移了 donor 上的最后一个 chunk，写入 oplog 事件用于 change stream
    const auto refreshedMetadata = _getCurrentMetadataAndCheckForConflictingErrors();
    if (!refreshedMetadata.getChunkManager()->getVersion(_args.getFromShard()).isSet()) {
        migrationutil::notifyChangeStreamsOnDonorLastChunk(
            _opCtx, nss(), _args.getFromShard(), _collectionUUID);
    }

    LOGV2(22018,
          "Migration succeeded and updated collection placement version",
          "updatedCollectionPlacementVersion"_attr = refreshedMetadata.getCollPlacementVersion(),
          "migrationId"_attr = _coordinator->getMigrationId());

    // 如果是时序集合，清理迁移出去的桶
    if (nss().isTimeseriesBucketsCollection()) {
        auto& bucketCatalog = timeseries::bucket_catalog::BucketCatalog::get(_opCtx);
        clear(bucketCatalog, _collectionUUID.get());
    }

    // 标记迁移决策为已提交
    _coordinator->setMigrationDecision(DecisionEnum::kCommitted);

    hangBeforeLeavingCriticalSection.pauseWhileSet();

    scopedGuard.dismiss();

    // 统计关键区域提交阶段耗时
    _stats.totalCriticalSectionCommitTimeMillis.addAndFetch(t.millis());

    LOGV2(6107801,
          "Exiting commit critical section",
          "migrationId"_attr = _coordinator->getMigrationId(),
          "durationMillis"_attr = t.millis());

    // 退出关键区域并确保所有状态已持久化
    _cleanup(true);

    // 记录迁移日志到 config.changelog
    ShardingLogging::get(_opCtx)->logChange(
        _opCtx,
        "moveChunk.commit",
        nss(),
        BSON("min" << *_args.getMin() << "max" << *_args.getMax() << "from" << _args.getFromShard()
                   << "to" << _args.getToShard() << "counts" << *_recipientCloneCounts),
        ShardingCatalogClient::kMajorityWriteConcern);

    const ChunkRange range(*_args.getMin(), *_args.getMax());

    std::string orphanedRangeCleanUpErrMsg = str::stream()
        << "Moved chunks successfully but failed to clean up " << nss().toStringForErrorMsg()
        << " range " << redact(range.toString()) << " due to: ";

    // 如果需要等待删除 orphaned 数据，则阻塞直到清理完成
    if (_args.getWaitForDelete()) {
        LOGV2(22019,
              "Waiting for migration cleanup after chunk commit",
              logAttrs(nss()),
              "range"_attr = redact(range.toString()),
              "migrationId"_attr = _coordinator->getMigrationId());

        Status deleteStatus = _cleanupCompleteFuture
            ? _cleanupCompleteFuture->getNoThrow(_opCtx)
            : Status(ErrorCodes::Error(5089002),
                     "Not honouring the 'waitForDelete' request because migration coordinator "
                     "cleanup didn't succeed");
        if (!deleteStatus.isOK()) {
            uasserted(ErrorCodes::OrphanedRangeCleanUpFailed,
                      orphanedRangeCleanUpErrMsg + redact(deleteStatus));
        }
    }

    _moveTimingHelper.done(6);
    moveChunkHangAtStep6.pauseWhileSet();
}

void MigrationSourceManager::_cleanupOnError() noexcept {
    if (_state == kDone) {
        return;
    }

    ShardingLogging::get(_opCtx)->logChange(
        _opCtx,
        "moveChunk.error",
        _args.getCommandParameter(),
        BSON("min" << *_args.getMin() << "max" << *_args.getMax() << "from" << _args.getFromShard()
                   << "to" << _args.getToShard()),
        ShardingCatalogClient::kMajorityWriteConcern);

    _cleanup(true);
}

SharedSemiFuture<void> MigrationSourceManager::abort() {
    stdx::lock_guard<Client> lk(*_opCtx->getClient());
    _opCtx->markKilled();
    _stats.countDonorMoveChunkAbortConflictingIndexOperation.addAndFetch(1);

    return _completion.getFuture();
}


/**
 * MigrationSourceManager::_getCurrentMetadataAndCheckForConflictingErrors 的作用：
 * 获取当前集合的分片元数据并检查是否存在可能导致迁移冲突的错误状态。
 * 
 * 核心功能：
 * 1. 元数据获取：安全地获取集合当前的分片元数据信息
 * 2. 状态一致性检查：验证集合的分片状态在迁移过程中未被并发操作修改
 * 3. 时间戳/纪元验证：确保迁移开始时的集合版本与当前版本一致
 * 4. 并发冲突检测：检测可能影响迁移正确性的并发操作
 * 5. 异常安全保证：通过不可中断锁确保元数据读取的原子性
 * 
 * 检查类型：
 * - 集合状态清除检查：防止分片状态被并发清理操作清除
 * - 时间戳一致性检查：验证集合版本的时间戳未发生变化
 * - 纪元一致性检查：验证集合版本的纪元未发生变化
 * - 分片状态验证：确保集合仍处于分片状态
 * 
 * 该方法是迁移过程中关键的安全检查点，确保迁移操作基于正确和一致的元数据进行。
 */
CollectionMetadata MigrationSourceManager::_getCurrentMetadataAndCheckForConflictingErrors() {
    // 获取当前集合的分片元数据
    // 使用 lambda 表达式封装获取逻辑，确保锁的正确管理
    auto metadata = [&] {
        // TODO (SERVER-71444): Fix to be interruptible or document exception.
        // 使用不可中断锁守护：确保元数据读取操作的原子性
        // 防止在读取过程中被中断，保证数据一致性
        UninterruptibleLockGuard noInterrupt(_opCtx);  // NOLINT.
        
        // 获取集合的共享锁：MODE_IS（意向共享锁）允许并发读取
        // 这里只需要读取元数据，不需要修改集合状态
        AutoGetCollection autoColl(_opCtx, _args.getCommandParameter(), MODE_IS);
        
        // 获取集合分片运行时的共享访问权限
        // 确保能够安全地读取当前的分片元数据
        const auto scopedCsr = CollectionShardingRuntime::assertCollectionLockedAndAcquireShared(
            _opCtx, _args.getCommandParameter());

        // 获取当前已知的元数据（如果存在）
        // 这个调用是非阻塞的，如果元数据不可用会返回 boost::none
        const auto optMetadata = scopedCsr->getCurrentMetadataIfKnown();
        
        // 检查分片状态是否被并发操作清除
        // 如果元数据为空，说明集合的分片状态可能被其他操作清理了
        uassert(ErrorCodes::ConflictingOperationInProgress,
                "The collection's sharding state was cleared by a concurrent operation",
                optMetadata);
        
        // 返回有效的元数据对象
        return *optMetadata;
    }();
    
    // 执行版本一致性检查：确保迁移期间集合版本未发生变化
    
    // 如果迁移开始时记录了集合时间戳，进行时间戳一致性检查
    if (_collectionTimestamp) {
        // 验证当前集合的时间戳与迁移开始时记录的时间戳一致
        // 时间戳变化意味着集合的版本发生了重大变更
        uassert(ErrorCodes::ConflictingOperationInProgress,
                str::stream()
                    << "The collection's timestamp has changed since the migration began. Expected "
                       "timestamp: "
                    << _collectionTimestamp->toStringPretty() << ", but found: "
                    << (metadata.isSharded()
                            ? metadata.getCollPlacementVersion().getTimestamp().toStringPretty()
                            : "unsharded collection"),
                // 检查条件：
                // 1. 集合必须仍然是分片的
                // 2. 当前时间戳必须与记录的时间戳匹配
                metadata.isSharded() &&
                    *_collectionTimestamp == metadata.getCollPlacementVersion().getTimestamp());
    } else {
        // 如果没有时间戳（旧版本兼容），使用纪元进行一致性检查
        // 纪元变化同样表示集合版本的重大变更
        uassert(
            ErrorCodes::ConflictingOperationInProgress,
            str::stream()
                << "The collection's epoch has changed since the migration began. Expected epoch: "
                << _collectionEpoch->toString() << ", but found: "
                << (metadata.isSharded() ? metadata.getCollPlacementVersion().toString()
                                         : "unsharded collection"),
            // 检查条件：
            // 1. 集合必须仍然是分片的
            // 2. 当前纪元必须与记录的纪元匹配
            metadata.isSharded() && metadata.getCollPlacementVersion().epoch() == _collectionEpoch);
    }

    // 返回验证后的元数据，确保其一致性和有效性
    return metadata;
}

/**
 * MigrationSourceManager::_cleanup
 * 该函数用于在迁移流程结束后（无论成功或失败）清理源分片的迁移状态和资源。
 * 核心作用是：退出关键区域、取消克隆操作、更新迁移状态、清理元数据和统计信息，确保迁移流程安全收尾。
 * 如果迁移成功，还会完成迁移的最终步骤；如果失败，则清理相关状态并准备恢复。
 */
void MigrationSourceManager::_cleanup(bool completeMigration) noexcept {
    // 确保迁移状态未标记为完成
    invariant(_state != kDone);

    // 退出关键区域并释放资源
    auto cloneDriver = [&]() {
        // 取消注册集合的分片状态，并退出迁移关键区域
        UninterruptibleLockGuard noInterrupt(_opCtx);  // 防止中断
        AutoGetCollection autoColl(_opCtx, nss(), MODE_IX);
        auto scopedCsr =
            CollectionShardingRuntime::assertCollectionLockedAndAcquireExclusive(_opCtx, nss());

        if (_state != kCreated) {
            invariant(_cloneDriver);  // 确保克隆驱动器存在
        }

        _critSec.reset();  // 退出关键区域
        return std::move(_cloneDriver);  // 返回克隆驱动器
    }();

    // 如果当前状态处于关键区域或提交阶段，记录关键区域完成日志
    if (_state == kCriticalSection || _state == kCloneCompleted || _state == kCommittingOnConfig) {
        LOGV2_DEBUG_OPTIONS(4817403,
                            2,
                            {logv2::LogComponent::kShardMigrationPerf},
                            "Finished critical section",
                            "migrationId"_attr = _coordinator->getMigrationId());

        LOGV2(6107802,
              "Finished critical section",
              "migrationId"_attr = _coordinator->getMigrationId(),
              "durationMillis"_attr = _cloneAndCommitTimer.millis());
    }

    // 以下清理操作可能会阻塞或获取其他锁，因此在退出集合锁后执行
    if (cloneDriver) {
        cloneDriver->cancelClone(_opCtx);  // 取消克隆操作
    }

    try {
        // 如果迁移状态已达到克隆阶段，执行迁移协调器的清理操作
        if (_state >= kCloning) {
            invariant(_coordinator);
            if (_state < kCommittingOnConfig) {
                _coordinator->setMigrationDecision(DecisionEnum::kAborted);  // 标记迁移为中止
            }

            // 创建新的操作上下文，用于后续操作
            auto newClient = _opCtx->getServiceContext()
                                 ->getService(ClusterRole::ShardServer)
                                 ->makeClient("MigrationCoordinator");
            AlternativeClientRegion acr(newClient);
            auto newOpCtxPtr = cc().makeOperationContext();
            auto newOpCtx = newOpCtxPtr.get();

            // 如果处于关键区域或提交阶段，刷新路由表缓存并确保元数据持久化
            if (_state >= kCriticalSection && _state <= kCommittingOnConfig) {
                _stats.totalCriticalSectionTimeMillis.addAndFetch(_cloneAndCommitTimer.millis());

                // 等待路由表缓存更新持久化到磁盘，确保所有节点的分片版本一致
                CatalogCacheLoader::get(newOpCtx).waitForCollectionFlush(newOpCtx, nss());
            }

            // 如果迁移成功，完成迁移的最终步骤
            if (completeMigration) {
                _cleanupCompleteFuture = _coordinator->completeMigration(newOpCtx);
            }
        }

        // 更新迁移状态为完成
        _state = kDone;
    } catch (const DBException& ex) {
        // 如果迁移完成失败，记录警告日志并清理元数据
        LOGV2_WARNING(5089001,
                      "Failed to complete the migration",
                      "chunkMigrationRequestParameters"_attr = redact(_args.toBSON({})),
                      "error"_attr = redact(ex),
                      "migrationId"_attr = _coordinator->getMigrationId());

        // 清除本地分片过滤元数据，准备恢复
        UninterruptibleLockGuard noInterrupt(_opCtx);  // 防止中断
        AutoGetCollection autoColl(_opCtx, nss(), MODE_IX);
        CollectionShardingRuntime::assertCollectionLockedAndAcquireExclusive(_opCtx, nss())
            ->clearFilteringMetadata(_opCtx);
    }
}

BSONObj MigrationSourceManager::getMigrationStatusReport(
    const CollectionShardingRuntime::ScopedSharedCollectionShardingRuntime& scopedCsrLock) const {

    // Important: This method is being called from a thread other than the main one, therefore we
    // have to protect with `_mutex` any write to the attributes read by getMigrationStatusReport()
    // like `_args` or `_cloneDriver`
    stdx::lock_guard<Latch> lg(_mutex);

    boost::optional<long long> sessionOplogEntriesToBeMigratedSoFar;
    boost::optional<long long> sessionOplogEntriesSkippedSoFarLowerBound;
    if (_cloneDriver) {
        sessionOplogEntriesToBeMigratedSoFar =
            _cloneDriver->getSessionOplogEntriesToBeMigratedSoFar();
        sessionOplogEntriesSkippedSoFarLowerBound =
            _cloneDriver->getSessionOplogEntriesSkippedSoFarLowerBound();
    }

    return migrationutil::makeMigrationStatusDocumentSource(
        _args.getCommandParameter(),
        _args.getFromShard(),
        _args.getToShard(),
        true,
        _args.getMin().value_or(BSONObj()),
        _args.getMax().value_or(BSONObj()),
        sessionOplogEntriesToBeMigratedSoFar,
        sessionOplogEntriesSkippedSoFarLowerBound);
}

MigrationSourceManager::ScopedRegisterer::ScopedRegisterer(MigrationSourceManager* msm,
                                                           CollectionShardingRuntime& csr)
    : _msm(msm) {
    invariant(nullptr == std::exchange(msmForCsr(csr), msm));
}

MigrationSourceManager::ScopedRegisterer::~ScopedRegisterer() {
    // TODO (SERVER-71444): Fix to be interruptible or document exception.
    UninterruptibleLockGuard noInterrupt(_msm->_opCtx);  // NOLINT.
    AutoGetCollection autoColl(_msm->_opCtx, _msm->_args.getCommandParameter(), MODE_IX);
    auto scopedCsr = CollectionShardingRuntime::assertCollectionLockedAndAcquireExclusive(
        _msm->_opCtx, _msm->_args.getCommandParameter());
    invariant(_msm == std::exchange(msmForCsr(*scopedCsr), nullptr));
}

}  // namespace mongo
