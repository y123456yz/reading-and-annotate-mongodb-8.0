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
    invariant(!shard_role_details::getLocker(_opCtx)->isLocked());
    // Since the MigrationSourceManager is registered on the CSR from the constructor, another
    // thread can get it and abort the migration (and get a reference to the completion promise's
    // future). When this happens, since we throw an exception from the constructor, the destructor
    // will not run, so we have to do complete it here, otherwise we get a BrokenPromise
    // TODO (SERVER-92531): Use existing clean up infrastructure when aborting in early stages
    ScopeGuard scopedGuard([&] { _completion.emplaceValue(); });

    LOGV2(22016,
          "Starting chunk migration donation",
          "requestParameters"_attr = redact(_args.toBSON({})));

    _moveTimingHelper.done(1);
    moveChunkHangAtStep1.pauseWhileSet();

    // Make sure the latest placement version is recovered as of the time of the invocation of the
    // command.
    onCollectionPlacementVersionMismatch(_opCtx, nss(), boost::none);

    const auto shardId = ShardingState::get(opCtx)->shardId();

    // Complete any unfinished migration pending recovery
    {
        migrationutil::drainMigrationsPendingRecovery(opCtx);

        // Since the moveChunk command is holding the ActiveMigrationRegistry and we just drained
        // all migrations pending recovery, now there cannot be any document in
        // config.migrationCoordinators.
        PersistentTaskStore<MigrationCoordinatorDocument> store(
            NamespaceString::kMigrationCoordinatorsNamespace);
        invariant(store.count(opCtx) == 0);
    }

    // Compute the max or min bound in case only one is set (moveRange)
    if (!_args.getMax().has_value() || !_args.getMin().has_value()) {

        const auto metadata = [&]() {
            AutoGetCollection autoColl(_opCtx, nss(), MODE_IS);
            const auto scopedCsr =
                CollectionShardingRuntime::assertCollectionLockedAndAcquireShared(opCtx, nss());
            const auto [metadata, _] = checkCollectionIdentity(
                _opCtx, nss(), _args.getEpoch(), _args.getCollectionTimestamp());
            return metadata;
        }();

        if (!_args.getMax().has_value()) {
            const auto& min = *_args.getMin();

            const auto cm = metadata.getChunkManager();
            const auto owningChunk = cm->findIntersectingChunkWithSimpleCollation(min);
            const auto max = computeOtherBound(_opCtx,
                                               nss(),
                                               min,
                                               owningChunk.getMax(),
                                               cm->getShardKeyPattern(),
                                               _args.getMaxChunkSizeBytes(),
                                               true /* needMaxBound */);

            stdx::lock_guard<Latch> lg(_mutex);
            _args.getMoveRangeRequestBase().setMax(max);
            _moveTimingHelper.setMax(max);
        } else if (!_args.getMin().has_value()) {
            const auto& max = *_args.getMax();

            const auto cm = metadata.getChunkManager();
            const auto owningChunk = getChunkForMaxBound(*cm, max);
            const auto min = computeOtherBound(_opCtx,
                                               nss(),
                                               owningChunk.getMin(),
                                               max,
                                               cm->getShardKeyPattern(),
                                               _args.getMaxChunkSizeBytes(),
                                               false /* needMaxBound */);

            stdx::lock_guard<Latch> lg(_mutex);
            _args.getMoveRangeRequestBase().setMin(min);
            _moveTimingHelper.setMin(min);
        }
    }

    // Snapshot the committed metadata from the time the migration starts and register the
    // MigrationSourceManager on the CSR.
    const auto [collectionMetadata, collectionIndexInfo, collectionUUID] = [&] {
        // TODO (SERVER-71444): Fix to be interruptible or document exception.
        UninterruptibleLockGuard noInterrupt(_opCtx);  // NOLINT.
        AutoGetCollection autoColl(_opCtx, nss(), MODE_IS);
        auto scopedCsr =
            CollectionShardingRuntime::assertCollectionLockedAndAcquireExclusive(opCtx, nss());

        auto [metadata, indexInfo] = checkCollectionIdentity(
            _opCtx, nss(), _args.getEpoch(), _args.getCollectionTimestamp());

        UUID collectionUUID = autoColl.getCollection()->uuid();

        // Atomically (still under the CSR lock held above) check whether migrations are allowed and
        // register the MigrationSourceManager on the CSR. This ensures that interruption due to the
        // change of allowMigrations to false will properly serialise and not allow any new MSMs to
        // be running after the change.
        uassert(ErrorCodes::ConflictingOperationInProgress,
                "Collection is undergoing changes so moveChunk is not allowed.",
                metadata.allowMigrations());

        _scopedRegisterer.emplace(this, *scopedCsr);

        return std::make_tuple(
            std::move(metadata), std::move(indexInfo), std::move(collectionUUID));
    }();

    // Drain the execution/cancellation of any existing range deletion task overlapping with the
    // targeted range (a task issued by a previous migration may still be present when the migration
    // gets interrupted post-commit).
    const ChunkRange range(*_args.getMin(), *_args.getMax());
    const auto rangeDeletionWaitDeadline = opCtx->getServiceContext()->getFastClockSource()->now() +
        Milliseconds(drainOverlappingRangeDeletionsOnStartTimeoutMS.load());
    // CollectionShardingRuntime::waitForClean() allows to sync on tasks already registered on the
    // RangeDeleterService, but may miss pending ones in case this code runs after a failover. The
    // enclosing while loop allows to address such a gap.
    while (rangedeletionutil::checkForConflictingDeletions(opCtx, range, collectionUUID)) {
        uassert(ErrorCodes::ResumableRangeDeleterDisabled,
                "Failing migration because the disableResumableRangeDeleter server "
                "parameter is set to true on the donor shard, which contains range "
                "deletion tasks overlapping with the incoming range.",
                !disableResumableRangeDeleter.load());

        LOGV2(9197000,
              "Migration start deferred because the requested range overlaps with one or more "
              "ranges already scheduled for deletion",
              logAttrs(nss()),
              "range"_attr = redact(range.toString()));

        auto status = CollectionShardingRuntime::waitForClean(
            opCtx, nss(), collectionUUID, range, rangeDeletionWaitDeadline);

        if (status.isOK() &&
            opCtx->getServiceContext()->getFastClockSource()->now() >= rangeDeletionWaitDeadline) {
            status = Status(
                ErrorCodes::ExceededTimeLimit,
                "Failed to start new migration - a conflicting range deletion is still pending");
        }

        uassertStatusOK(status);

        // If the filtering metadata was cleared while the range deletion task was ongoing, then
        // 'waitForClean' would return immediately even though there really is an ongoing range
        // deletion task. For that case, we loop again until there is no conflicting task in
        // config.rangeDeletions
        opCtx->sleepFor(Milliseconds(1000));
    }

    checkShardKeyPattern(_opCtx,
                         nss(),
                         collectionMetadata,
                         collectionIndexInfo,
                         ChunkRange(*_args.getMin(), *_args.getMax()));
    checkRangeWithinChunk(_opCtx,
                          nss(),
                          collectionMetadata,
                          collectionIndexInfo,
                          ChunkRange(*_args.getMin(), *_args.getMax()));

    _collectionEpoch = _args.getEpoch();
    _collectionUUID = collectionUUID;
    _collectionTimestamp = _args.getCollectionTimestamp();

    _chunkVersion = collectionMetadata.getChunkManager()
                        ->findIntersectingChunkWithSimpleCollation(*_args.getMin())
                        .getLastmod();

    _moveTimingHelper.done(2);
    moveChunkHangAtStep2.pauseWhileSet();
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

void MigrationSourceManager::awaitToCatchUp() {
    invariant(!shard_role_details::getLocker(_opCtx)->isLocked());
    invariant(_state == kCloning);
    ScopeGuard scopedGuard([&] { _cleanupOnError(); });
    _stats.totalDonorChunkCloneTimeMillis.addAndFetch(_cloneAndCommitTimer.millis());
    _cloneAndCommitTimer.reset();

    // Block until the cloner deems it appropriate to enter the critical section.
    uassertStatusOK(_cloneDriver->awaitUntilCriticalSectionIsAppropriate(
        _opCtx, kMaxWaitToEnterCriticalSectionTimeout));

    _state = kCloneCaughtUp;
    _moveTimingHelper.done(4);
    moveChunkHangAtStep4.pauseWhileSet(_opCtx);
    scopedGuard.dismiss();
}

void MigrationSourceManager::enterCriticalSection() {
    invariant(!shard_role_details::getLocker(_opCtx)->isLocked());
    invariant(_state == kCloneCaughtUp);
    ScopeGuard scopedGuard([&] { _cleanupOnError(); });
    _stats.totalDonorChunkCloneTimeMillis.addAndFetch(_cloneAndCommitTimer.millis());
    _cloneAndCommitTimer.reset();

    hangBeforeEnteringCriticalSection.pauseWhileSet();

    const auto [cm, _] =
        uassertStatusOK(Grid::get(_opCtx)->catalogCache()->getCollectionRoutingInfo(_opCtx, nss()));

    // Check that there are no chunks on the recepient shard. Write an oplog event for change
    // streams if this is the first migration to the recipient.
    if (!cm.getVersion(_args.getToShard()).isSet()) {
        migrationutil::notifyChangeStreamsOnRecipientFirstChunk(
            _opCtx, nss(), _args.getFromShard(), _args.getToShard(), _collectionUUID);

        // Wait for the above 'migrateChunkToNewShard' oplog message to be majority acknowledged.
        WriteConcernResult ignoreResult;
        auto latestOpTime = repl::ReplClientInfo::forClient(_opCtx->getClient()).getLastOp();
        uassertStatusOK(waitForWriteConcern(
            _opCtx, latestOpTime, WriteConcerns::kMajorityWriteConcernNoTimeout, &ignoreResult));
    }

    LOGV2_DEBUG_OPTIONS(4817402,
                        2,
                        {logv2::LogComponent::kShardMigrationPerf},
                        "Starting critical section",
                        "migrationId"_attr = _coordinator->getMigrationId());

    _critSec.emplace(_opCtx, nss(), _critSecReason);

    _state = kCriticalSection;

    // Persist a signal to secondaries that we've entered the critical section. This is will cause
    // secondaries to refresh their routing table when next accessed, which will block behind the
    // critical section. This ensures causal consistency by preventing a stale mongos with a cluster
    // time inclusive of the migration config commit update from accessing secondary data.
    // Note: this write must occur after the critSec flag is set, to ensure the secondary refresh
    // will stall behind the flag.
    uassertStatusOKWithContext(
        shardmetadatautil::updateShardCollectionsEntry(
            _opCtx,
            BSON(ShardCollectionType::kNssFieldName
                 << NamespaceStringUtil::serialize(nss(), SerializationContext::stateDefault())),
            BSON("$inc" << BSON(ShardCollectionType::kEnterCriticalSectionCounterFieldName << 1)),
            false /*upsert*/),
        "Persist critical section signal for secondaries");

    LOGV2(22017,
          "Migration successfully entered critical section",
          "migrationId"_attr = _coordinator->getMigrationId());

    scopedGuard.dismiss();
}

void MigrationSourceManager::commitChunkOnRecipient() {
    invariant(!shard_role_details::getLocker(_opCtx)->isLocked());
    invariant(_state == kCriticalSection);
    ScopeGuard scopedGuard([&] {
        _cleanupOnError();
        migrationutil::asyncRecoverMigrationUntilSuccessOrStepDown(_opCtx,
                                                                   _args.getCommandParameter());
    });

    // Tell the recipient shard to fetch the latest changes.
    auto commitCloneStatus = _cloneDriver->commitClone(_opCtx);

    if (MONGO_unlikely(failMigrationCommit.shouldFail()) && commitCloneStatus.isOK()) {
        commitCloneStatus = {ErrorCodes::InternalError,
                             "Failing _recvChunkCommit due to failpoint."};
    }

    uassertStatusOKWithContext(commitCloneStatus, "commit clone failed");
    _recipientCloneCounts = commitCloneStatus.getValue()["counts"].Obj().getOwned();

    _state = kCloneCompleted;
    _moveTimingHelper.done(5);
    moveChunkHangAtStep5.pauseWhileSet();
    scopedGuard.dismiss();
}

void MigrationSourceManager::commitChunkMetadataOnConfig() {
    invariant(!shard_role_details::getLocker(_opCtx)->isLocked());
    invariant(_state == kCloneCompleted);

    ScopeGuard scopedGuard([&] {
        _cleanupOnError();
        migrationutil::asyncRecoverMigrationUntilSuccessOrStepDown(_opCtx, nss());
    });

    // If we have chunks left on the FROM shard, bump the version of one of them as well. This will
    // change the local collection major version, which indicates to other processes that the chunk
    // metadata has changed and they should refresh.
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

    // Read operations must begin to wait on the critical section just before we send the commit
    // operation to the config server
    _critSec->enterCommitPhase();

    _state = kCommittingOnConfig;

    Timer t;

    auto commitChunkMigrationResponse =
        Grid::get(_opCtx)->shardRegistry()->getConfigShard()->runCommandWithFixedRetryAttempts(
            _opCtx,
            ReadPreferenceSetting{ReadPreference::PrimaryOnly},
            DatabaseName::kAdmin,
            builder.obj(),
            Shard::RetryPolicy::kIdempotent);

    if (MONGO_unlikely(migrationCommitNetworkError.shouldFail())) {
        commitChunkMigrationResponse = Status(
            ErrorCodes::InternalError, "Failpoint 'migrationCommitNetworkError' generated error");
    }

    Status migrationCommitStatus =
        Shard::CommandResponse::getEffectiveStatus(commitChunkMigrationResponse);

    if (!migrationCommitStatus.isOK()) {
        {
            // TODO (SERVER-71444): Fix to be interruptible or document exception.
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

    // Asynchronously tell the recipient to release its critical section
    _coordinator->launchReleaseRecipientCriticalSection(_opCtx);

    hangBeforePostMigrationCommitRefresh.pauseWhileSet();

    try {
        LOGV2_DEBUG_OPTIONS(4817404,
                            2,
                            {logv2::LogComponent::kShardMigrationPerf},
                            "Starting post-migration commit refresh on the shard",
                            "migrationId"_attr = _coordinator->getMigrationId());

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
            // TODO (SERVER-71444): Fix to be interruptible or document exception.
            UninterruptibleLockGuard noInterrupt(_opCtx);  // NOLINT.
            AutoGetCollection autoColl(_opCtx, nss(), MODE_IX);
            CollectionShardingRuntime::assertCollectionLockedAndAcquireExclusive(_opCtx, nss())
                ->clearFilteringMetadata(_opCtx);
        }
        scopedGuard.dismiss();
        _cleanup(false);
        // Best-effort recover of the chunk version.
        onCollectionPlacementVersionMismatchNoExcept(_opCtx, nss(), boost::none).ignore();
        throw;
    }

    // Migration succeeded

    const auto refreshedMetadata = _getCurrentMetadataAndCheckForConflictingErrors();
    // Check if there are no chunks left on donor shard. Write an oplog event for change streams if
    // the last chunk migrated off the donor.
    if (!refreshedMetadata.getChunkManager()->getVersion(_args.getFromShard()).isSet()) {
        migrationutil::notifyChangeStreamsOnDonorLastChunk(
            _opCtx, nss(), _args.getFromShard(), _collectionUUID);
    }


    LOGV2(22018,
          "Migration succeeded and updated collection placement version",
          "updatedCollectionPlacementVersion"_attr = refreshedMetadata.getCollPlacementVersion(),
          "migrationId"_attr = _coordinator->getMigrationId());

    // If the migration has succeeded, clear the BucketCatalog so that the buckets that got migrated
    // out are no longer updatable.
    if (nss().isTimeseriesBucketsCollection()) {
        auto& bucketCatalog = timeseries::bucket_catalog::BucketCatalog::get(_opCtx);
        clear(bucketCatalog, _collectionUUID.get());
    }

    _coordinator->setMigrationDecision(DecisionEnum::kCommitted);

    hangBeforeLeavingCriticalSection.pauseWhileSet();

    scopedGuard.dismiss();

    _stats.totalCriticalSectionCommitTimeMillis.addAndFetch(t.millis());

    LOGV2(6107801,
          "Exiting commit critical section",
          "migrationId"_attr = _coordinator->getMigrationId(),
          "durationMillis"_attr = t.millis());

    // Exit the critical section and ensure that all the necessary state is fully persisted before
    // scheduling orphan cleanup.
    _cleanup(true);

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

void MigrationSourceManager::_cleanup(bool completeMigration) noexcept {
    invariant(_state != kDone);

    auto cloneDriver = [&]() {
        // Unregister from the collection's sharding state and exit the migration critical section.
        // TODO (SERVER-71444): Fix to be interruptible or document exception.
        UninterruptibleLockGuard noInterrupt(_opCtx);  // NOLINT.
        AutoGetCollection autoColl(_opCtx, nss(), MODE_IX);
        auto scopedCsr =
            CollectionShardingRuntime::assertCollectionLockedAndAcquireExclusive(_opCtx, nss());

        if (_state != kCreated) {
            invariant(_cloneDriver);
        }

        _critSec.reset();
        return std::move(_cloneDriver);
    }();

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

    // The cleanup operations below are potentially blocking or acquire other locks, so perform them
    // outside of the collection X lock

    if (cloneDriver) {
        cloneDriver->cancelClone(_opCtx);
    }

    try {
        if (_state >= kCloning) {
            invariant(_coordinator);
            if (_state < kCommittingOnConfig) {
                _coordinator->setMigrationDecision(DecisionEnum::kAborted);
            }

            auto newClient = _opCtx->getServiceContext()
                                 ->getService(ClusterRole::ShardServer)
                                 ->makeClient("MigrationCoordinator");
            AlternativeClientRegion acr(newClient);
            auto newOpCtxPtr = cc().makeOperationContext();
            auto newOpCtx = newOpCtxPtr.get();

            if (_state >= kCriticalSection && _state <= kCommittingOnConfig) {
                _stats.totalCriticalSectionTimeMillis.addAndFetch(_cloneAndCommitTimer.millis());

                // Wait for the updates to the cache of the routing table to be fully written to
                // disk. This way, we ensure that all nodes from a shard which donated a chunk will
                // always be at the placement version of the last migration it performed.
                //
                // If the metadata is not persisted before clearing the 'inMigration' flag below, it
                // is possible that the persisted metadata is rolled back after step down, but the
                // write which cleared the 'inMigration' flag is not, a secondary node will report
                // itself at an older placement version.
                CatalogCacheLoader::get(newOpCtx).waitForCollectionFlush(newOpCtx, nss());
            }
            if (completeMigration) {
                // This can be called on an exception path after the OperationContext has been
                // interrupted, so use a new OperationContext. Note, it's valid to call
                // getServiceContext on an interrupted OperationContext.
                _cleanupCompleteFuture = _coordinator->completeMigration(newOpCtx);
            }
        }

        _state = kDone;
    } catch (const DBException& ex) {
        LOGV2_WARNING(5089001,
                      "Failed to complete the migration",
                      "chunkMigrationRequestParameters"_attr = redact(_args.toBSON({})),
                      "error"_attr = redact(ex),
                      "migrationId"_attr = _coordinator->getMigrationId());
        // Something went really wrong when completing the migration just unset the metadata and let
        // the next op to recover.
        // TODO (SERVER-71444): Fix to be interruptible or document exception.
        UninterruptibleLockGuard noInterrupt(_opCtx);  // NOLINT.
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
