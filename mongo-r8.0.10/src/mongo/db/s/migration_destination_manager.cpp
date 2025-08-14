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


#include <boost/move/utility_core.hpp>
#include <boost/none.hpp>
#include <boost/optional/optional.hpp>
// IWYU pragma: no_include "cxxabi.h"
#include <array>
#include <list>
#include <mutex>
#include <ratio>
#include <type_traits>
#include <utility>
#include <vector>

#include "mongo/base/error_codes.h"
#include "mongo/bson/bsonelement.h"
#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/bsontypes.h"
#include "mongo/client/read_preference.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/cancelable_operation_context.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/collection_catalog.h"
#include "mongo/db/catalog/collection_options.h"
#include "mongo/db/catalog/database.h"
#include "mongo/db/catalog/document_validation.h"
#include "mongo/db/catalog/index_catalog.h"
#include "mongo/db/catalog_raii.h"
#include "mongo/db/client.h"
#include "mongo/db/concurrency/exception_util.h"
#include "mongo/db/concurrency/lock_manager_defs.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/feature_flag.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/index_builds_coordinator.h"
#include "mongo/db/keypattern.h"
#include "mongo/db/logical_time.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/ops/delete.h"
#include "mongo/db/ops/update_result.h"
#include "mongo/db/persistent_task_store.h"
#include "mongo/db/repl/read_concern_args.h"
#include "mongo/db/repl/read_concern_level.h"
#include "mongo/db/repl/repl_client_info.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/db/s/collection_metadata.h"
#include "mongo/db/s/collection_sharding_runtime.h"
#include "mongo/db/s/migration_batch_fetcher.h"
#include "mongo/db/s/migration_destination_manager.h"
#include "mongo/db/s/migration_util.h"
#include "mongo/db/s/move_timing_helper.h"
#include "mongo/db/s/operation_sharding_state.h"
#include "mongo/db/s/range_deletion_task_gen.h"
#include "mongo/db/s/range_deletion_util.h"
#include "mongo/db/s/shard_filtering_metadata_refresh.h"
#include "mongo/db/s/sharding_index_catalog_ddl_util.h"
#include "mongo/db/s/sharding_recovery_service.h"
#include "mongo/db/s/sharding_runtime_d_params_gen.h"
#include "mongo/db/s/sharding_statistics.h"
#include "mongo/db/s/start_chunk_clone_request.h"
#include "mongo/db/server_options.h"
#include "mongo/db/service_context.h"
#include "mongo/db/session/session_catalog.h"
#include "mongo/db/session/session_catalog_mongod.h"
#include "mongo/db/shard_role.h"
#include "mongo/db/storage/write_unit_of_work.h"
#include "mongo/db/transaction/transaction_participant.h"
#include "mongo/db/transaction_resources.h"
#include "mongo/db/vector_clock.h"
#include "mongo/db/write_block_bypass.h"
#include "mongo/db/write_concern.h"
#include "mongo/executor/task_executor_pool.h"
#include "mongo/logv2/log.h"
#include "mongo/logv2/log_attr.h"
#include "mongo/logv2/log_component.h"
#include "mongo/logv2/log_options.h"
#include "mongo/logv2/redaction.h"
#include "mongo/platform/compiler.h"
#include "mongo/rpc/get_status_from_command_result.h"
#include "mongo/s/catalog/sharding_catalog_client.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/catalog/type_index_catalog_gen.h"
#include "mongo/s/catalog/type_shard.h"
#include "mongo/s/catalog_cache_loader.h"
#include "mongo/s/client/shard.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/cluster_commands_helpers.h"
#include "mongo/s/grid.h"
#include "mongo/s/index_version.h"
#include "mongo/s/resharding/resharding_feature_flag_gen.h"
#include "mongo/s/shard_key_pattern.h"
#include "mongo/s/sharding_feature_flags_gen.h"
#include "mongo/s/sharding_index_catalog_cache.h"
#include "mongo/s/sharding_state.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/clock_source.h"
#include "mongo/util/database_name_util.h"
#include "mongo/util/debug_util.h"
#include "mongo/util/decorable.h"
#include "mongo/util/duration.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/namespace_string_util.h"
#include "mongo/util/out_of_line_executor.h"
#include "mongo/util/producer_consumer_queue.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/str.h"
#include "mongo/util/time_support.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kShardingMigration

namespace mongo {
namespace {

MONGO_FAIL_POINT_DEFINE(hangMigrationRecipientBeforeWaitingNoIndexBuildInProgress);

const auto getMigrationDestinationManager =
    ServiceContext::declareDecoration<MigrationDestinationManager>();

const WriteConcernOptions kMajorityWriteConcern(WriteConcernOptions::kMajority,
                                                // Note: Even though we're setting UNSET here,
                                                // kMajority implies JOURNAL if journaling is
                                                // supported by mongod and
                                                // writeConcernMajorityJournalDefault is set to true
                                                // in the ReplSetConfig.
                                                WriteConcernOptions::SyncMode::UNSET,
                                                WriteConcernOptions::kNoWaiting);

BSONObj makeLocalReadConcernWithAfterClusterTime(Timestamp afterClusterTime) {
    return BSON(repl::ReadConcernArgs::kReadConcernFieldName << BSON(
                    repl::ReadConcernArgs::kLevelFieldName
                    << repl::readConcernLevels::toString(repl::ReadConcernLevel::kLocalReadConcern)
                    << repl::ReadConcernArgs::kAfterClusterTimeFieldName << afterClusterTime));
}

void checkOutSessionAndVerifyTxnState(OperationContext* opCtx) {
    auto mongoDSessionCatalog = MongoDSessionCatalog::get(opCtx);
    mongoDSessionCatalog->checkOutUnscopedSession(opCtx);
    TransactionParticipant::get(opCtx).beginOrContinue(
        opCtx,
        {*opCtx->getTxnNumber()},
        boost::none /* autocommit */,
        TransactionParticipant::TransactionActions::kNone);
}

template <typename Callable>
constexpr bool returnsVoid() {
    return std::is_void_v<std::invoke_result_t<Callable>>;
}

// Yields the checked out session before running the given function. If the function runs without
// throwing, will reacquire the session and verify it is still valid to proceed with the migration.
template <typename Callable, std::enable_if_t<!returnsVoid<Callable>(), int> = 0>
auto runWithoutSession(OperationContext* opCtx, Callable&& callable) {
    auto mongoDSessionCatalog = MongoDSessionCatalog::get(opCtx);
    mongoDSessionCatalog->checkInUnscopedSession(opCtx,
                                                 OperationContextSession::CheckInReason::kYield);

    auto retVal = callable();

    // The below code can throw, so it cannot run in a scope guard.
    opCtx->checkForInterrupt();
    checkOutSessionAndVerifyTxnState(opCtx);

    return retVal;
}

// Same as runWithoutSession above but takes a void function.
template <typename Callable, std::enable_if_t<returnsVoid<Callable>(), int> = 0>
void runWithoutSession(OperationContext* opCtx, Callable&& callable) {
    auto mongoDSessionCatalog = MongoDSessionCatalog::get(opCtx);
    mongoDSessionCatalog->checkInUnscopedSession(opCtx,
                                                 OperationContextSession::CheckInReason::kYield);

    callable();

    // The below code can throw, so it cannot run in a scope guard.
    opCtx->checkForInterrupt();
    checkOutSessionAndVerifyTxnState(opCtx);
}

/**
 * Returns a human-readabale name of the migration manager's state.
 */
std::string stateToString(MigrationDestinationManager::State state) {
    switch (state) {
        case MigrationDestinationManager::kReady:
            return "ready";
        case MigrationDestinationManager::kClone:
            return "clone";
        case MigrationDestinationManager::kCatchup:
            return "catchup";
        case MigrationDestinationManager::kSteady:
            return "steady";
        case MigrationDestinationManager::kCommitStart:
            return "commitStart";
        case MigrationDestinationManager::kEnteredCritSec:
            return "enteredCriticalSection";
        case MigrationDestinationManager::kExitCritSec:
            return "exitCriticalSection";
        case MigrationDestinationManager::kDone:
            return "done";
        case MigrationDestinationManager::kFail:
            return "fail";
        case MigrationDestinationManager::kAbort:
            return "abort";
        default:
            MONGO_UNREACHABLE;
    }
}

bool isInRange(const BSONObj& obj,
               const BSONObj& min,
               const BSONObj& max,
               const BSONObj& shardKeyPattern) {
    ShardKeyPattern shardKey(shardKeyPattern);
    BSONObj k = shardKey.extractShardKeyFromDoc(obj);
    return k.woCompare(min) >= 0 && k.woCompare(max) < 0;
}

/**
 * Checks if an upsert of a remote document will override a local document with the same _id but in
 * a different range on this shard. Must be in WriteContext to avoid races and DBHelper errors.
 *
 * TODO: Could optimize this check out if sharding on _id.
 */
bool willOverrideLocalId(OperationContext* opCtx,
                         const NamespaceString& nss,
                         BSONObj min,
                         BSONObj max,
                         BSONObj shardKeyPattern,
                         BSONObj remoteDoc,
                         BSONObj* localDoc) {
    *localDoc = BSONObj();
    if (Helpers::findById(opCtx, nss, remoteDoc, *localDoc)) {
        return !isInRange(*localDoc, min, max, shardKeyPattern);
    }

    return false;
}

/**
 * Returns true if the majority of the nodes and the nodes corresponding to the given writeConcern
 * (if not empty) have applied till the specified lastOp.
 */
bool opReplicatedEnough(OperationContext* opCtx,
                        const repl::OpTime& lastOpApplied,
                        const WriteConcernOptions& writeConcern) {
    WriteConcernResult writeConcernResult;
    writeConcernResult.wTimedOut = false;

    Status majorityStatus =
        waitForWriteConcern(opCtx, lastOpApplied, kMajorityWriteConcern, &writeConcernResult);
    if (!majorityStatus.isOK()) {
        if (!writeConcernResult.wTimedOut) {
            uassertStatusOK(majorityStatus);
        }
        return false;
    }

    // Enforce the user specified write concern after "majority" so it covers the union of the 2
    // write concerns in case the user's write concern is stronger than majority
    WriteConcernOptions userWriteConcern(writeConcern);
    userWriteConcern.wTimeout = WriteConcernOptions::kNoWaiting;
    writeConcernResult.wTimedOut = false;

    Status userStatus =
        waitForWriteConcern(opCtx, lastOpApplied, userWriteConcern, &writeConcernResult);
    if (!userStatus.isOK()) {
        if (!writeConcernResult.wTimedOut) {
            uassertStatusOK(userStatus);
        }
        return false;
    }
    return true;
}

/**
 * Create the migration clone request BSON object to send to the source shard.
 *
 * 'sessionId' unique identifier for this migration.
 */
BSONObj createMigrateCloneRequest(const NamespaceString& nss, const MigrationSessionId& sessionId) {
    BSONObjBuilder builder;
    builder.append("_migrateClone",
                   NamespaceStringUtil::serialize(nss, SerializationContext::stateDefault()));
    sessionId.append(&builder);
    return builder.obj();
}

/**
 * Create the migration transfer mods request BSON object to send to the source shard.
 *
 * 'sessionId' unique identifier for this migration.
 */
BSONObj createTransferModsRequest(const NamespaceString& nss, const MigrationSessionId& sessionId) {
    BSONObjBuilder builder;
    builder.append("_transferMods",
                   NamespaceStringUtil::serialize(nss, SerializationContext::stateDefault()));
    sessionId.append(&builder);
    return builder.obj();
}

BSONObj criticalSectionReason(const MigrationSessionId& sessionId) {
    BSONObjBuilder builder;
    builder.append("recvChunk", 1);
    sessionId.append(&builder);
    return builder.obj();
}

bool migrationRecipientRecoveryDocumentExists(OperationContext* opCtx,
                                              const MigrationSessionId& sessionId) {
    PersistentTaskStore<MigrationRecipientRecoveryDocument> store(
        NamespaceString::kMigrationRecipientsNamespace);

    return store.count(opCtx,
                       BSON(MigrationRecipientRecoveryDocument::kMigrationSessionIdFieldName
                            << sessionId.toString())) > 0;
}

void replaceShardingIndexCatalogInShardIfNeeded(OperationContext* opCtx,
                                                const NamespaceString& nss,
                                                const UUID& uuid) {
    auto currentShardHasAnyChunks = [&]() -> bool {
        AutoGetCollection autoColl(opCtx, nss, MODE_IS);
        const auto scsr =
            CollectionShardingRuntime::assertCollectionLockedAndAcquireShared(opCtx, nss);
        const auto optMetadata = scsr->getCurrentMetadataIfKnown();
        return optMetadata && optMetadata->currentShardHasAnyChunks();
    }();

    // Early return, this shard already contains chunks, so there is no need for consolidate.
    if (currentShardHasAnyChunks) {
        return;
    }

    auto [_, optSii] = uassertStatusOK(
        Grid::get(opCtx)->catalogCache()->getCollectionRoutingInfoWithIndexRefresh(opCtx, nss));

    if (optSii) {
        std::vector<IndexCatalogType> indexes;
        optSii->forEachIndex([&](const auto& index) {
            indexes.push_back(index);
            return true;
        });
        replaceCollectionShardingIndexCatalog(
            opCtx, nss, uuid, optSii->getCollectionIndexes().indexVersion(), indexes);
    } else {
        clearCollectionShardingIndexCatalog(opCtx, nss, uuid);
    }
}

// Throws if this configShard is currently draining.
void checkConfigShardIsNotDraining(OperationContext* opCtx) {
    DBDirectClient dbClient(opCtx);
    const auto thisShardId = ShardingState::get(opCtx)->shardId();
    const auto doc = dbClient.findOne(NamespaceString::kConfigsvrShardsNamespace,
                                      BSON(ShardType::name << thisShardId));
    uassert(ErrorCodes::ShardNotFound, "Shard has been removed", !doc.isEmpty());

    const auto shardDoc = uassertStatusOK(ShardType::fromBSON(doc));
    uassert(ErrorCodes::ShardNotFound, "Shard is currently draining", !shardDoc.getDraining());
}

// Enabling / disabling these fail points pauses / resumes MigrateStatus::_go(), the thread which
// receives a chunk migration from the donor.
MONGO_FAIL_POINT_DEFINE(migrateThreadHangAtStep1);
MONGO_FAIL_POINT_DEFINE(migrateThreadHangAtStep2);
MONGO_FAIL_POINT_DEFINE(migrateThreadHangAtStep3);
MONGO_FAIL_POINT_DEFINE(migrateThreadHangAtStep4);
MONGO_FAIL_POINT_DEFINE(migrateThreadHangAtStep5);
MONGO_FAIL_POINT_DEFINE(migrateThreadHangAtStep6);
MONGO_FAIL_POINT_DEFINE(migrateThreadHangAfterSteadyTransition);
MONGO_FAIL_POINT_DEFINE(migrateThreadHangAtStep7);

MONGO_FAIL_POINT_DEFINE(failMigrationOnRecipient);
MONGO_FAIL_POINT_DEFINE(failMigrationReceivedOutOfRangeOperation);
MONGO_FAIL_POINT_DEFINE(migrationRecipientFailPostCommitRefresh);

}  // namespace

const ReplicaSetAwareServiceRegistry::Registerer<MigrationDestinationManager> mdmRegistry(
    "MigrationDestinationManager");

MigrationDestinationManager::MigrationDestinationManager() = default;

MigrationDestinationManager::~MigrationDestinationManager() = default;

MigrationDestinationManager* MigrationDestinationManager::get(ServiceContext* serviceContext) {
    return &getMigrationDestinationManager(serviceContext);
}

MigrationDestinationManager* MigrationDestinationManager::get(OperationContext* opCtx) {
    return &getMigrationDestinationManager(opCtx->getServiceContext());
}

MigrationDestinationManager::State MigrationDestinationManager::getState() const {
    stdx::lock_guard<Latch> sl(_mutex);
    return _state;
}

void MigrationDestinationManager::_setState(State newState) {
    stdx::lock_guard<Latch> sl(_mutex);
    _state = newState;
    _stateChangedCV.notify_all();
}

void MigrationDestinationManager::_setStateFail(StringData msg) {
    LOGV2(21998, "Error during migration", "error"_attr = redact(msg));
    {
        stdx::lock_guard<Latch> sl(_mutex);
        _errmsg = msg.toString();
        _state = kFail;
        _stateChangedCV.notify_all();
    }

    if (_sessionMigration) {
        _sessionMigration->forceFail(msg);
    }
}

void MigrationDestinationManager::_setStateFailWarn(StringData msg) {
    LOGV2_WARNING(22010, "Error during migration", "error"_attr = redact(msg));
    {
        stdx::lock_guard<Latch> sl(_mutex);
        _errmsg = msg.toString();
        _state = kFail;
        _stateChangedCV.notify_all();
    }

    if (_sessionMigration) {
        _sessionMigration->forceFail(msg);
    }
}

bool MigrationDestinationManager::isActive() const {
    stdx::lock_guard<Latch> lk(_mutex);
    return _isActive(lk);
}

bool MigrationDestinationManager::_isActive(WithLock) const {
    return _sessionId.has_value();
}

void MigrationDestinationManager::report(BSONObjBuilder& b,
                                         OperationContext* opCtx,
                                         bool waitForSteadyOrDone) {
    if (waitForSteadyOrDone) {
        stdx::unique_lock<Latch> lock(_mutex);
        try {
            opCtx->waitForConditionOrInterruptFor(_stateChangedCV, lock, Seconds(1), [&]() -> bool {
                return _state != kReady && _state != kClone && _state != kCatchup;
            });
        } catch (...) {
            // Ignoring this error because this is an optional parameter and we catch timeout
            // exceptions later.
        }
        b.append("waited", true);
    }
    stdx::lock_guard<Latch> sl(_mutex);

    b.appendBool("active", _sessionId.has_value());

    if (_sessionId) {
        b.append("sessionId", _sessionId->toString());
    }

    b.append("ns", NamespaceStringUtil::serialize(_nss, SerializationContext::stateDefault()));
    b.append("from", _fromShardConnString.toString());
    b.append("fromShardId", _fromShard.toString());
    b.append("min", _min);
    b.append("max", _max);
    b.append("shardKeyPattern", _shardKeyPattern);
    b.append(StartChunkCloneRequest::kSupportsCriticalSectionDuringCatchUp, true);

    b.append("state", stateToString(_state));

    if (_state == kFail) {
        invariant(!_errmsg.empty());
        b.append("errmsg", _errmsg);
    }

    BSONObjBuilder bb(b.subobjStart("counts"));
    bb.append("cloned", _getNumCloned());
    bb.append("clonedBytes", _getNumBytesCloned());
    bb.append("catchup", _numCatchup);
    bb.append("steady", _numSteady);
    bb.done();
}

BSONObj MigrationDestinationManager::getMigrationStatusReport(
    const CollectionShardingRuntime::ScopedSharedCollectionShardingRuntime& scopedCsrLock) {
    stdx::lock_guard<Latch> lk(_mutex);
    if (_isActive(lk)) {
        boost::optional<long long> sessionOplogEntriesMigrated;
        if (_sessionMigration) {
            sessionOplogEntriesMigrated = _sessionMigration->getSessionOplogEntriesMigrated();
        }

        return migrationutil::makeMigrationStatusDocumentDestination(
            _nss, _fromShard, _toShard, false, _min, _max, sessionOplogEntriesMigrated);
    } else {
        return BSONObj();
    }
}

/**
 * MigrationDestinationManager::start 函数的作用：
 * 在目标分片上启动 chunk 迁移接收过程的入口函数，初始化迁移状态并启动迁移线程。
 * 
 * 核心功能：
 * 1. 迁移状态初始化：设置迁移会话、源/目标分片、chunk范围等核心参数
 * 2. 线程安全管理：等待前一个迁移线程完成，确保同一时间只有一个迁移在进行
 * 3. 会话迁移启动：初始化SessionCatalogMigrationDestination处理事务和可重试写操作
 * 4. 迁移线程创建：启动后台迁移线程执行实际的数据接收和处理工作
 * 5. 并发控制保护：通过ScopedReceiveChunk确保迁移操作的互斥性和原子性
 * 6. 写关注配置：设置适当的写关注级别确保数据持久性和一致性
 * 7. 进度跟踪初始化：设置共享状态用于监控迁移进度和性能统计
 * 
 * 启动流程：
 * - 线程同步：等待之前的会话迁移线程和迁移线程完成
 * - 状态重置：清理之前的迁移状态，初始化新的迁移参数
 * - 参数设置：从StartChunkCloneRequest中提取并设置迁移配置
 * - 会话创建：初始化会话迁移组件处理事务相关数据
 * - 线程启动：创建并启动后台迁移驱动线程
 * 
 * 参数说明：
 * @param opCtx 操作上下文，提供认证、权限、事务等执行环境
 * @param nss 目标集合的命名空间（数据库名.集合名）
 * @param scopedReceiveChunk 接收chunk的作用域锁，确保迁移的唯一性
 * @param cloneRequest 克隆请求对象，包含源分片、迁移ID、chunk范围等信息
 * @param writeConcern 写关注选项，控制数据持久性和复制确认级别
 * 
 * 线程安全特性：
 * - 互斥访问：通过ScopedReceiveChunk防止并发迁移操作
 * - 线程等待：确保前一个迁移完成后才开始新的迁移
 * - 状态保护：使用互斥锁保护内部状态变量的并发访问
 * - 取消机制：支持通过CancellationToken中断迁移操作
 * 
 * 错误处理：
 * - 状态验证：确保迁移管理器处于正确的初始状态
 * - 资源清理：失败时自动清理已分配的资源
 * - 异常安全：通过RAII模式确保资源的正确管理
 * 
 * 性能特性：
 * - 并行支持：根据源分片能力决定是否启用并行数据获取
 * - 内存管理：设置克隆进度共享状态用于内存使用监控
 * - 统计收集：启动性能统计计数器用于监控和调优
 * 
 * 该函数是chunk迁移接收端的核心入口，为后续的数据克隆、增量同步、关键区域管理等阶段奠定基础。
 * RecvChunkStartCommand::errmsgRun 中调用执行
 */
Status MigrationDestinationManager::start(OperationContext* opCtx,
                                          const NamespaceString& nss,
                                          ScopedReceiveChunk scopedReceiveChunk,
                                          const StartChunkCloneRequest& cloneRequest,
                                          const WriteConcernOptions& writeConcern) {
    // Wait for the session migration thread and the migrate thread to finish. Do not hold the
    // _mutex while waiting since it could lead to deadlock. It is safe to join _sessionMigration
    // and _migrateThreadHandle without holding the _mutex since they are only (re)set in start()
    // and restoreRecoveredMigrationState() and both of them require a ScopedReceiveChunk which
    // guarantees that there can only be one start() and restoreRecoveredMigrationState() call at
    // any given time.
    // 等待会话迁移线程和迁移线程完成：
    // 不在等待期间持有_mutex以避免死锁风险
    // 由于ScopedReceiveChunk的保护，可以安全地join这些线程而无需持有互斥锁
    // ScopedReceiveChunk确保同一时间只能有一个start()或restoreRecoveredMigrationState()调用
    if (_sessionMigration && _sessionMigration->joinable()) {
        // 等待前一个迁移的会话迁移线程完成
        // 会话迁移负责传输事务和可重试写操作相关的数据
        LOGV2_DEBUG(8991402,
                    2,
                    "Start waiting for the session migration thread for the previous migration to "
                    "complete before starting a new migration",
                    "previousMigrationSessionId"_attr = _sessionMigration->getMigrationSessionId(),
                    "nextMigrationSessionId"_attr = cloneRequest.getSessionId());
        _sessionMigration->join();
        LOGV2_DEBUG(8991403,
                    2,
                    "Finished waiting for the session migration thread for the previous migration "
                    "to complete before starting a new migration");
    }
    if (_migrateThreadHandle.joinable()) {
        // 等待前一个迁移的主迁移线程完成
        // 主迁移线程负责数据克隆、增量同步、关键区域管理等核心工作
        LOGV2_DEBUG(8991404,
                    2,
                    "Start waiting for the migrate thread for the previous migration to "
                    "complete before starting a new migration",
                    "previousMigrationId"_attr = _migrationId,
                    "nextMigrationId"_attr = cloneRequest.getMigrationId());
        _migrateThreadHandle.join();
        LOGV2_DEBUG(8991405,
                    2,
                    "Finished waiting for the migrate thread for the previous migration to "
                    "complete before starting a new migration");
    }

    // 获取互斥锁保护内部状态变量的并发访问
    // 确保迁移状态的一致性和线程安全
    stdx::lock_guard<Latch> lk(_mutex);
    
    // 前置条件验证：确保迁移管理器处于正确的初始状态
    invariant(!_sessionId);           // 不应该有活跃的会话ID
    invariant(!_scopedReceiveChunk);  // 不应该有活跃的接收chunk作用域锁

    // 初始化迁移状态：重置为就绪状态，准备开始新的迁移
    _state = kReady;                // 设置状态为就绪
    _stateChangedCV.notify_all();   // 通知等待状态变化的线程
    _errmsg = "";                   // 清空错误消息

    // 迁移标识符设置：从克隆请求中提取核心标识信息
    _migrationId = cloneRequest.getMigrationId();  // 迁移的唯一标识符
    _lsid = cloneRequest.getLsid();                // 逻辑会话ID，用于事务管理
    _txnNumber = cloneRequest.getTxnNumber();      // 事务号，确保操作的唯一性

    // 并行获取器支持配置：根据源分片的能力决定是否启用并行数据获取
    // 并行获取可以提高数据传输效率，但需要源分片支持
    _parallelFetchersSupported = cloneRequest.parallelFetchingSupported();

    // 迁移参数配置：设置chunk迁移的核心参数
    _nss = nss;                                    // 目标集合命名空间
    _fromShard = cloneRequest.getFromShardId();    // 源分片标识符
    // 获取源分片连接信息：用于后续的网络通信
    _fromShardConnString =
        uassertStatusOK(Grid::get(opCtx)->shardRegistry()->getShard(opCtx, _fromShard))
            ->getConnString();
    _toShard = cloneRequest.getToShardId();        // 目标分片标识符
    _min = cloneRequest.getMinKey();               // chunk的最小边界键
    _max = cloneRequest.getMaxKey();               // chunk的最大边界键
    _shardKeyPattern = cloneRequest.getShardKeyPattern();  // 分片键模式

    // 写关注配置：控制数据持久性和复制确认要求
    // 写关注确保数据在指定数量的节点上持久化后才认为写操作成功
    _writeConcern = writeConcern;

    // chunk状态初始化：标记chunk尚未被标记为待处理状态
    // 在迁移过程中，chunk会被标记为待处理以防止并发操作
    _chunkMarkedPending = false;

    // 迁移进度共享状态：创建用于跨线程共享迁移进度信息的对象
    // 用于监控克隆进度、内存使用情况等性能指标
    _migrationCloningProgress = std::make_shared<MigrationCloningProgressSharedState>();

    // 统计计数器初始化：重置增量同步和稳定状态的计数器
    _numCatchup = 0;    // 追赶阶段的操作计数
    _numSteady = 0;     // 稳定状态的操作计数

    // 会话和作用域设置：设置迁移会话ID和接收chunk的作用域锁
    _sessionId = cloneRequest.getSessionId();           // 迁移会话的唯一标识符
    _scopedReceiveChunk = std::move(scopedReceiveChunk); // 转移接收chunk的所有权

    // Promise对象初始化：用于线程间通信和同步
    // 这些promise用于协调迁移的不同阶段
    invariant(!_canReleaseCriticalSectionPromise);
    // 关键区域释放promise：用于通知何时可以释放关键区域
    _canReleaseCriticalSectionPromise = std::make_unique<SharedPromise<void>>();

    invariant(!_migrateThreadFinishedPromise);
    // 迁移线程完成promise：用于通知迁移线程的最终状态
    _migrateThreadFinishedPromise = std::make_unique<SharedPromise<State>>();

    // Reset the cancellationSource at the start of every migration to avoid accumulating memory.
    // 重置取消源：在每次迁移开始时重置以避免内存累积
    // 取消源用于在需要时中断迁移操作
    auto newCancellationSource = CancellationSource();
    std::swap(_cancellationSource, newCancellationSource);

    // 会话迁移组件初始化：创建负责迁移事务和可重试写操作数据的组件
    // 会话迁移确保事务状态和可重试写操作在分片间正确传输
    _sessionMigration = std::make_unique<SessionCatalogMigrationDestination>(
        _nss, _fromShard, *_sessionId, _cancellationSource.token());
    
    // 性能统计更新：增加接收端迁移开始的计数
    // 用于监控和分析分片迁移的性能和频率
    ShardingStatistics::get(opCtx).countRecipientMoveChunkStarted.addAndFetch(1);

    // 迁移线程启动：创建并启动后台线程执行实际的迁移工作
    // 迁移线程将执行数据克隆、增量同步、关键区域管理等核心操作
    _migrateThreadHandle = stdx::thread([this, cancellationToken = _cancellationSource.token()]() {
        // MigrationDestinationManager::_migrateThread
        _migrateThread(cancellationToken);
    });

    return Status::OK();
}

Status MigrationDestinationManager::restoreRecoveredMigrationState(
    OperationContext* opCtx,
    ScopedReceiveChunk scopedReceiveChunk,
    const MigrationRecipientRecoveryDocument& recoveryDoc) {
    // Wait for the migrate thread to finish. Do not hold the _mutex while waiting since it could
    // lead to deadlock. It is safe to join _migrateThreadHandle without holding the _mutex since it
    // is only (re)set in start() and restoreRecoveredMigrationState() and both of them require a
    // ScopedReceiveChunk which guarantees that there can only be one start() and
    // restoreRecoveredMigrationState() call at any given time. It is not necessary to wait for
    // session migration thread since by design the recovery doc cannot exist if the session
    // migration has not finished.
    if (_migrateThreadHandle.joinable()) {
        LOGV2_DEBUG(
            8991406,
            2,
            "Start waiting for the existing migrate thread to complete before recovering it",
            "migrationId"_attr = _migrationId);
        _migrateThreadHandle.join();
        LOGV2_DEBUG(
            8991407,
            2,
            "Finished waiting for the existing migrate thread to complete before recovering it");
    }

    stdx::lock_guard<Latch> lk(_mutex);
    invariant(!_sessionId);

    _scopedReceiveChunk = std::move(scopedReceiveChunk);
    _nss = recoveryDoc.getNss();
    _migrationId = recoveryDoc.getId();
    _sessionId = recoveryDoc.getMigrationSessionId();
    _min = recoveryDoc.getRange().getMin();
    _max = recoveryDoc.getRange().getMax();
    _lsid = recoveryDoc.getLsid();
    _txnNumber = recoveryDoc.getTxnNumber();
    _state = kCommitStart;

    invariant(!_canReleaseCriticalSectionPromise);
    _canReleaseCriticalSectionPromise = std::make_unique<SharedPromise<void>>();

    invariant(!_migrateThreadFinishedPromise);
    _migrateThreadFinishedPromise = std::make_unique<SharedPromise<State>>();

    LOGV2(6064500, "Recovering migration recipient", "sessionId"_attr = *_sessionId);

    _migrateThreadHandle = stdx::thread([this, cancellationToken = _cancellationSource.token()]() {
        _migrateThread(cancellationToken, true /* skipToCritSecTaken */);
    });

    return Status::OK();
}

repl::OpTime MigrationDestinationManager::fetchAndApplyBatch(
    OperationContext* opCtx,
    std::function<bool(OperationContext*, BSONObj)> applyBatchFn,
    std::function<bool(OperationContext*, BSONObj*)> fetchBatchFn) {

    SingleProducerSingleConsumerQueue<BSONObj>::Options options;
    options.maxQueueDepth = 1;

    SingleProducerSingleConsumerQueue<BSONObj> batches(options);
    repl::OpTime lastOpApplied;

    stdx::thread applicationThread{[&] {
        Client::initThread("batchApplier", opCtx->getService(), Client::noSession());
        auto executor =
            Grid::get(opCtx->getServiceContext())->getExecutorPool()->getFixedExecutor();
        auto applicationOpCtx = CancelableOperationContext(
            cc().makeOperationContext(), opCtx->getCancellationToken(), executor);

        ScopeGuard consumerGuard([&] {
            batches.closeConsumerEnd();
            lastOpApplied =
                repl::ReplClientInfo::forClient(applicationOpCtx->getClient()).getLastOp();
        });

        try {
            while (true) {
                DisableDocumentValidation documentValidationDisabler(
                    applicationOpCtx.get(),
                    DocumentValidationSettings::kDisableSchemaValidation |
                        DocumentValidationSettings::kDisableInternalValidation);
                auto nextBatch = batches.pop(applicationOpCtx.get());
                if (!applyBatchFn(applicationOpCtx.get(), nextBatch)) {
                    return;
                }
            }
        } catch (...) {
            ClientLock lk(opCtx->getClient());
            opCtx->getServiceContext()->killOperation(lk, opCtx, ErrorCodes::Error(51008));
            LOGV2(21999, "Batch application failed", "error"_attr = redact(exceptionToStatus()));
        }
    }};


    {
        ScopeGuard applicationThreadJoinGuard([&] {
            batches.closeProducerEnd();
            applicationThread.join();
        });

        while (true) {
            BSONObj nextBatch;
            bool emptyBatch = fetchBatchFn(opCtx, &nextBatch);
            try {
                batches.push(nextBatch.getOwned(), opCtx);
                if (emptyBatch) {
                    break;
                }
            } catch (const ExceptionFor<ErrorCodes::ProducerConsumerQueueEndClosed>&) {
                break;
            }
        }
    }  // This scope ensures that the guard is destroyed

    // This check is necessary because the consumer thread uses killOp to propagate errors to the
    // producer thread (this thread)
    opCtx->checkForInterrupt();
    return lastOpApplied;
}

Status MigrationDestinationManager::abort(const MigrationSessionId& sessionId) {
    stdx::lock_guard<Latch> sl(_mutex);

    if (!_sessionId) {
        return Status::OK();
    }

    if (!_sessionId->matches(sessionId)) {
        return {ErrorCodes::CommandFailed,
                str::stream() << "received abort request from a stale session "
                              << sessionId.toString() << ". Current session is "
                              << _sessionId->toString()};
    }

    _state = kAbort;
    _stateChangedCV.notify_all();
    _errmsg = "aborted";

    return Status::OK();
}

void MigrationDestinationManager::abortWithoutSessionIdCheck() {
    stdx::lock_guard<Latch> sl(_mutex);
    _state = kAbort;
    _stateChangedCV.notify_all();
    _errmsg = "aborted without session id check";
}

Status MigrationDestinationManager::startCommit(const MigrationSessionId& sessionId) {
    stdx::unique_lock<Latch> lock(_mutex);

    const auto convergenceTimeout = Milliseconds(defaultConfigCommandTimeoutMS.load()) +
        Milliseconds(defaultConfigCommandTimeoutMS.load()) / 4;

    // The donor may have started the commit while the recipient is still busy processing
    // the last batch of mods sent in the catch up phase. Allow some time for synching up.
    auto deadline = Date_t::now() + convergenceTimeout;

    while (_state == kCatchup) {
        if (stdx::cv_status::timeout ==
            _stateChangedCV.wait_until(lock, deadline.toSystemTimePoint())) {
            return {ErrorCodes::CommandFailed,
                    str::stream() << "startCommit timed out waiting for the catch up completion. "
                                  << "Sender's session is " << sessionId.toString()
                                  << ". Current session is "
                                  << (_sessionId ? _sessionId->toString() : "none.")};
        }
    }

    if (_state != kSteady) {
        return {ErrorCodes::CommandFailed,
                str::stream() << "Migration startCommit attempted when not in STEADY state."
                              << " Sender's session is " << sessionId.toString()
                              << (_sessionId ? (". Current session is " + _sessionId->toString())
                                             : ". No active session on this shard.")};
    }

    // In STEADY state we must have active migration
    invariant(_sessionId);

    // This check guards against the (unusual) situation where the current donor shard has stalled,
    // during which the recipient shard crashed or timed out, and then began serving as a recipient
    // or donor for another migration.
    if (!_sessionId->matches(sessionId)) {
        return {ErrorCodes::CommandFailed,
                str::stream() << "startCommit received commit request from a stale session "
                              << sessionId.toString() << ". Current session is "
                              << _sessionId->toString()};
    }

    _sessionMigration->finish();
    _state = kCommitStart;
    _stateChangedCV.notify_all();

    // Assigning a timeout slightly higher than the one used for network requests to the config
    // server. Enough time to retry at least once in case of network failures (SERVER-51397).
    deadline = Date_t::now() + convergenceTimeout;

    while (_state == kCommitStart) {
        if (stdx::cv_status::timeout ==
            _stateChangedCV.wait_until(lock, deadline.toSystemTimePoint())) {
            _errmsg = str::stream() << "startCommit timed out waiting, " << _sessionId->toString();
            _state = kFail;
            _stateChangedCV.notify_all();
            return {ErrorCodes::CommandFailed, _errmsg};
        }
    }
    if (_state != kEnteredCritSec) {
        return {ErrorCodes::CommandFailed,
                "startCommit failed, final data failed to transfer or failed to enter critical "
                "section"};
    }

    return Status::OK();
}

Status MigrationDestinationManager::exitCriticalSection(OperationContext* opCtx,
                                                        const MigrationSessionId& sessionId) {
    SharedSemiFuture<State> threadFinishedFuture;
    {
        stdx::unique_lock<Latch> lock(_mutex);
        if (!_sessionId || !_sessionId->matches(sessionId)) {
            LOGV2_DEBUG(5899104,
                        2,
                        "Request to exit recipient critical section does not match current session",
                        "requested"_attr = sessionId,
                        "current"_attr = _sessionId);

            // No need to hold _mutex from here on. Release it because the lines below will acquire
            // other locks and holding the mutex could lead to deadlocks.
            lock.unlock();

            if (migrationRecipientRecoveryDocumentExists(opCtx, sessionId)) {
                // This node may have stepped down and interrupted the migrateThread, which reset
                // _sessionId. But the critical section may not have been released so it will be
                // recovered by the new primary.
                return {ErrorCodes::CommandFailed,
                        "Recipient migration recovery document still exists"};
            }

            // Ensure the command's wait for writeConcern will until the recovery document is
            // deleted.
            repl::ReplClientInfo::forClient(opCtx->getClient()).setLastOpToSystemLastOpTime(opCtx);

            return Status::OK();
        }

        if (_state < kEnteredCritSec) {
            return {ErrorCodes::CommandFailed,
                    "recipient critical section has not yet been entered"};
        }

        // Fulfill the promise to let the migrateThread release the critical section.
        invariant(_canReleaseCriticalSectionPromise);
        if (!_canReleaseCriticalSectionPromise->getFuture().isReady()) {
            _canReleaseCriticalSectionPromise->emplaceValue();
        }

        threadFinishedFuture = _migrateThreadFinishedPromise->getFuture();
    }

    // Wait for the migrateThread to finish
    const auto threadFinishState = threadFinishedFuture.get(opCtx);

    if (threadFinishState != kDone) {
        return {ErrorCodes::CommandFailed, "exitCriticalSection failed"};
    }

    LOGV2_DEBUG(
        5899105, 2, "Succeeded releasing recipient critical section", "requested"_attr = sessionId);

    return Status::OK();
}

MigrationDestinationManager::IndexesAndIdIndex MigrationDestinationManager::getCollectionIndexes(
    OperationContext* opCtx,
    const NamespaceString& nss,
    const ShardId& fromShardId,
    const boost::optional<CollectionRoutingInfo>& cri,
    boost::optional<Timestamp> afterClusterTime,
    bool expandSimpleCollation) {
    auto fromShard =
        uassertStatusOK(Grid::get(opCtx)->shardRegistry()->getShard(opCtx, fromShardId));

    std::vector<BSONObj> donorIndexSpecs;
    BSONObj donorIdIndexSpec;

    // Get the collection indexes and options from the donor shard.

    // Do not hold any locks while issuing remote calls.
    invariant(!shard_role_details::getLocker(opCtx)->isLocked());

    auto cmd = BSON("listIndexes" << nss.coll());
    if (cri) {
        cmd = appendShardVersion(cmd, cri->getShardVersion(fromShardId));
    }
    if (afterClusterTime) {
        cmd = cmd.addFields(makeLocalReadConcernWithAfterClusterTime(*afterClusterTime));
    }

    expandSimpleCollation = expandSimpleCollation &&
        resharding::gFeatureFlagReshardingImprovements.isEnabled(
            serverGlobalParams.featureCompatibility.acquireFCVSnapshot());

    // Get indexes by calling listIndexes against the donor.
    auto indexes = uassertStatusOK(
        fromShard->runExhaustiveCursorCommand(opCtx,
                                              ReadPreferenceSetting(ReadPreference::PrimaryOnly),
                                              nss.dbName(),
                                              cmd,
                                              Milliseconds(-1)));
    for (auto&& spec : indexes.docs) {
        if (spec[IndexDescriptor::kClusteredFieldName]) {
            // The 'clustered' index is implicitly created upon clustered collection creation.
            continue;
        }

        if (auto indexNameElem = spec[IndexDescriptor::kIndexNameFieldName];
            indexNameElem.type() == BSONType::String &&
            indexNameElem.valueStringData() == "_id_"_sd) {
            // The _id index always uses the collection's default collation and so there is no need
            // to add the collation field to attempt to disambiguate.
            donorIdIndexSpec = spec;
        } else if (expandSimpleCollation && !spec[IndexDescriptor::kCollationFieldName]) {
            BSONObjBuilder builder;
            for (auto&& [fieldName, elem] : spec) {
                if (fieldName != IndexDescriptor::kOriginalSpecFieldName ||
                    elem.Obj().hasField(IndexDescriptor::kCollationFieldName)) {
                    builder.append(elem);
                    continue;
                }

                BSONObjBuilder originalSpecBuilder{
                    builder.subobjStart(IndexDescriptor::kOriginalSpecFieldName)};
                originalSpecBuilder.appendElements(elem.Obj());
                originalSpecBuilder.append(IndexDescriptor::kCollationFieldName,
                                           CollationSpec::kSimpleSpec);
            }
            builder.append(IndexDescriptor::kCollationFieldName, CollationSpec::kSimpleSpec);
            spec = builder.obj();
        }

        donorIndexSpecs.push_back(spec);
    }

    return {donorIndexSpecs, donorIdIndexSpec};
}


MigrationDestinationManager::CollectionOptionsAndUUID
MigrationDestinationManager::getCollectionOptions(OperationContext* opCtx,
                                                  const NamespaceStringOrUUID& nssOrUUID,
                                                  boost::optional<Timestamp> afterClusterTime) {
    const auto dbInfo =
        uassertStatusOK(Grid::get(opCtx)->catalogCache()->getDatabase(opCtx, nssOrUUID.dbName()));
    return getCollectionOptions(
        opCtx, nssOrUUID, dbInfo->getPrimary(), dbInfo->getVersion(), afterClusterTime);
}

MigrationDestinationManager::CollectionOptionsAndUUID
MigrationDestinationManager::getCollectionOptions(OperationContext* opCtx,
                                                  const NamespaceStringOrUUID& nssOrUUID,
                                                  const ShardId& fromShardId,
                                                  const boost::optional<DatabaseVersion>& dbVersion,
                                                  boost::optional<Timestamp> afterClusterTime) {
    auto fromShard =
        uassertStatusOK(Grid::get(opCtx)->shardRegistry()->getShard(opCtx, fromShardId));

    BSONObj fromOptions;

    auto cmd = nssOrUUID.isNamespaceString()
        ? BSON("listCollections" << 1 << "filter" << BSON("name" << nssOrUUID.nss().coll()))
        : BSON("listCollections" << 1 << "filter" << BSON("info.uuid" << nssOrUUID.uuid()));

    if (dbVersion) {
        cmd = appendDbVersionIfPresent(cmd, *dbVersion);
    }

    if (afterClusterTime) {
        cmd = cmd.addFields(makeLocalReadConcernWithAfterClusterTime(*afterClusterTime));
    }

    // Get collection options by calling listCollections against the from shard.
    auto infosRes = uassertStatusOK(
        fromShard->runExhaustiveCursorCommand(opCtx,
                                              ReadPreferenceSetting(ReadPreference::PrimaryOnly),
                                              nssOrUUID.dbName(),
                                              cmd,
                                              Milliseconds(-1)));

    auto infos = infosRes.docs;
    uassert(ErrorCodes::NamespaceNotFound,
            str::stream() << "expected listCollections against the primary shard for "
                          << nssOrUUID.toStringForErrorMsg() << " to return 1 entry, but got "
                          << infos.size() << " entries",
            infos.size() == 1);


    BSONObj entry = infos.front();

    // The entire options include both the settable options under the 'options' field in the
    // listCollections response, and the UUID under the 'info' field.
    BSONObjBuilder fromOptionsBob;

    if (entry["options"].isABSONObj()) {
        fromOptionsBob.appendElements(entry["options"].Obj());
    }

    BSONObj info;
    if (entry["info"].isABSONObj()) {
        info = entry["info"].Obj();
    }

    uassert(ErrorCodes::InvalidUUID,
            str::stream() << "The from shard did not return a UUID for collection "
                          << nssOrUUID.toStringForErrorMsg()
                          << " as part of its listCollections response: " << entry
                          << ", but this node expects to see a UUID.",
            !info["uuid"].eoo());

    auto fromUUID = info["uuid"].uuid();

    fromOptionsBob.append(info["uuid"]);
    fromOptions = fromOptionsBob.obj();

    return {fromOptions, UUID::fromCDR(fromUUID)};
}

void MigrationDestinationManager::_dropLocalIndexesIfNecessary(
    OperationContext* opCtx,
    const NamespaceString& nss,
    const CollectionOptionsAndIndexes& collectionOptionsAndIndexes) {
    bool dropNonDonorIndexes = [&]() -> bool {
        AutoGetCollection autoColl(opCtx, nss, MODE_IS);
        const auto scopedCsr =
            CollectionShardingRuntime::assertCollectionLockedAndAcquireShared(opCtx, nss);
        // Only attempt to drop a collection's indexes if we have valid metadata and the collection
        // is sharded
        if (auto optMetadata = scopedCsr->getCurrentMetadataIfKnown()) {
            const auto& metadata = *optMetadata;
            if (metadata.isSharded()) {
                return !metadata.currentShardHasAnyChunks();
            }
        }
        return false;
    }();

    if (dropNonDonorIndexes) {
        // Determine which indexes exist on the local collection that don't exist on the donor's
        // collection.
        DBDirectClient client(opCtx);
        const bool includeBuildUUIDs = false;
        const int options = 0;
        auto indexes = client.getIndexSpecs(nss, includeBuildUUIDs, options);
        for (auto&& recipientIndex : indexes) {
            bool dropIndex = true;
            for (auto&& donorIndex : collectionOptionsAndIndexes.indexSpecs) {
                if (recipientIndex.woCompare(donorIndex) == 0) {
                    dropIndex = false;
                    break;
                }
            }
            // If the local index doesn't exist on the donor and isn't the _id index, drop it.
            auto indexNameElem = recipientIndex[IndexDescriptor::kIndexNameFieldName];
            if (indexNameElem.type() == BSONType::String && dropIndex &&
                !IndexDescriptor::isIdIndexPattern(
                    recipientIndex[IndexDescriptor::kKeyPatternFieldName].Obj())) {
                BSONObj info;
                if (!client.runCommand(
                        nss.dbName(),
                        BSON("dropIndexes" << nss.coll() << "index" << indexNameElem),
                        info))
                    uassertStatusOK(getStatusFromCommandResult(info));
            }
        }
    }
}

void MigrationDestinationManager::cloneCollectionIndexesAndOptions(
    OperationContext* opCtx,
    const NamespaceString& nss,
    const CollectionOptionsAndIndexes& collectionOptionsAndIndexes) {
    {
        // 1. Create the collection (if it doesn't already exist) and create any indexes we are
        // missing (auto-heal indexes).

        // Checks that the collection's UUID matches the donor's.
        auto checkUUIDsMatch = [&](const Collection* collection) {
            uassert(ErrorCodes::NotWritablePrimary,
                    str::stream() << "Unable to create collection " << nss.toStringForErrorMsg()
                                  << " because the node is not primary",
                    repl::ReplicationCoordinator::get(opCtx)->canAcceptWritesFor(opCtx, nss));

            uassert(ErrorCodes::InvalidUUID,
                    str::stream()
                        << "Cannot create collection " << nss.toStringForErrorMsg()
                        << " because we already have an identically named collection with UUID "
                        << collection->uuid() << ", which differs from the donor's UUID "
                        << collectionOptionsAndIndexes.uuid
                        << ". Manually drop the collection on this shard if it contains data from "
                           "a previous incarnation of "
                        << nss.toStringForErrorMsg(),
                    collection->uuid() == collectionOptionsAndIndexes.uuid);
        };

        bool isFirstMigration = [&] {
            AutoGetCollection collection(opCtx, nss, MODE_IS);
            const auto scopedCsr =
                CollectionShardingRuntime::assertCollectionLockedAndAcquireShared(opCtx, nss);
            if (auto optMetadata = scopedCsr->getCurrentMetadataIfKnown()) {
                const auto& metadata = *optMetadata;
                return metadata.isSharded() && !metadata.currentShardHasAnyChunks();
            }
            return false;
        }();

        // Check if there are missing indexes on the recipient shard from the donor.
        // If it is the first migration, do not consider in-progress index builds. Otherwise,
        // consider in-progress index builds as ready. Then, if there are missing indexes and the
        // collection is not empty, fail the migration. On the other hand, if the collection is
        // empty, wait for index builds to finish if it is the first migration.
        bool waitForInProgressIndexBuildCompletion = false;

        auto checkEmptyOrGetMissingIndexesFromDonor = [&](const CollectionPtr& collection) {
            auto indexCatalog = collection->getIndexCatalog();
            auto indexSpecs = indexCatalog->removeExistingIndexesNoChecks(
                opCtx, collection, collectionOptionsAndIndexes.indexSpecs, !isFirstMigration);
            if (!indexSpecs.empty()) {
                // Only allow indexes to be copied if the collection does not have any documents.
                uassert(ErrorCodes::CannotCreateCollection,
                        str::stream()
                            << "aborting, shard is missing " << indexSpecs.size() << " indexes and "
                            << "collection is not empty. Non-trivial "
                            << "index creation should be scheduled manually",
                        collection->isEmpty(opCtx));

                // If it is the first migration, mark waitForInProgressIndexBuildCompletion as true
                // to wait for index builds to be finished after releasing the locks.
                waitForInProgressIndexBuildCompletion = isFirstMigration;
            }
            return indexSpecs;
        };

        {
            AutoGetCollection collection(opCtx, nss, MODE_IS);

            if (collection) {
                checkUUIDsMatch(collection.getCollection().get());
                auto indexSpecs =
                    checkEmptyOrGetMissingIndexesFromDonor(collection.getCollection());
                if (indexSpecs.empty()) {
                    return;
                }
            }
        }

        // Before taking the exclusive database lock for cloning the remaining indexes, wait for
        // index builds to finish if it is the first migration.
        if (waitForInProgressIndexBuildCompletion) {
            if (MONGO_unlikely(
                    hangMigrationRecipientBeforeWaitingNoIndexBuildInProgress.shouldFail())) {
                LOGV2(7677900, "Hanging before waiting for in-progress index builds to finish");
                hangMigrationRecipientBeforeWaitingNoIndexBuildInProgress.pauseWhileSet();
            }

            IndexBuildsCoordinator::get(opCtx)->awaitNoIndexBuildInProgressForCollection(
                opCtx, collectionOptionsAndIndexes.uuid);
        }

        // Take the exclusive database lock if the collection does not exist or indexes are missing
        // (needs auto-heal).
        AutoGetDb autoDb(opCtx, nss.dbName(), MODE_X);
        auto db = autoDb.ensureDbExists(opCtx);

        auto collection = CollectionCatalog::get(opCtx)->lookupCollectionByNamespace(opCtx, nss);
        auto fromMigrate = true;
        if (collection) {
            checkUUIDsMatch(collection);
        } else {
            if (auto collectionByUUID = CollectionCatalog::get(opCtx)->lookupCollectionByUUID(
                    opCtx, collectionOptionsAndIndexes.uuid)) {
                uasserted(5860300,
                          str::stream()
                              << "Cannot create collection " << nss.toStringForErrorMsg()
                              << " with UUID " << collectionOptionsAndIndexes.uuid
                              << " because it conflicts with the UUID of an existing collection "
                              << collectionByUUID->ns().toStringForErrorMsg());
            }

            // We do not have a collection by this name. Create it with the donor's options.
            OperationShardingState::ScopedAllowImplicitCollectionCreate_UNSAFE
                unsafeCreateCollection(opCtx, /* forceCSRAsUnknownAfterCollectionCreation */ true);
            WriteUnitOfWork wuow(opCtx);
            CollectionOptions collectionOptions = uassertStatusOK(
                CollectionOptions::parse(collectionOptionsAndIndexes.options,
                                         CollectionOptions::ParseKind::parseForStorage));
            const bool createDefaultIndexes = true;
            uassertStatusOK(db->userCreateNS(opCtx,
                                             nss,
                                             collectionOptions,
                                             createDefaultIndexes,
                                             collectionOptionsAndIndexes.idIndexSpec,
                                             fromMigrate));
            wuow.commit();
            collection = CollectionCatalog::get(opCtx)->lookupCollectionByNamespace(opCtx, nss);
        }

        auto indexSpecs = checkEmptyOrGetMissingIndexesFromDonor(CollectionPtr(collection));
        if (!indexSpecs.empty()) {
            WriteUnitOfWork wunit(opCtx);
            CollectionWriter collWriter(opCtx, collection->uuid());
            IndexBuildsCoordinator::get(opCtx)->createIndexesOnEmptyCollection(
                opCtx, collWriter, indexSpecs, fromMigrate);
            wunit.commit();
        }
    }
}

/**
 * MigrationDestinationManager::_migrateThread 函数的作用：
 * 迁移目标分片的后台线程执行函数，负责在独立线程中运行完整的chunk接收迁移流程。
 * 
 * 核心功能：
 * 1. 线程生命周期管理：初始化迁移专用的客户端和操作上下文
 * 2. 迁移会话管理：维护与源分片的迁移会话，确保事务一致性
 * 3. 恢复机制协调：处理迁移过程中的中断和恢复场景
 * 4. 迁移驱动调度：调用_migrateDriver执行实际的迁移工作
 * 5. 异常处理和重试：捕获迁移过程中的异常并启动恢复流程
 * 6. 资源清理管理：确保迁移完成后正确清理会话和资源
 * 7. 状态通知机制：通过Promise机制通知其他组件迁移结果
 * 
 * 线程特性：
 * - 独立线程：在单独的线程中运行，不阻塞主线程
 * - 取消支持：响应CancellationToken的取消信号
 * - 会话隔离：使用专用的会话上下文确保迁移操作的隔离性
 * - 恢复能力：支持在故障后自动恢复迁移状态
 * 
 * 执行模式：
 * - 正常模式：从头开始执行完整的迁移流程
 * - 恢复模式：跳过已完成的阶段，从关键区域开始恢复
 * - 循环重试：在发生错误时自动进入恢复模式重试
 * 
 * 参数说明：
 * @param cancellationToken 取消令牌，用于响应外部的取消请求
 * @param skipToCritSecTaken 是否跳过前期阶段直接进入关键区域（恢复场景）
 * 
 * 会话管理：
 * - 检出迁移会话：确保在整个迁移期间持有会话
 * - 事务号验证：防止源分片故障转移后的事务号冲突
 * - 会话释放：迁移完成后正确释放会话资源
 * 
 * 错误处理：
 * - 异常捕获：捕获迁移过程中的所有异常
 * - 恢复判断：根据恢复文档存在性决定是否需要恢复
 * - 状态设置：将错误信息设置到迁移状态中
 * 
 * 线程安全：
 * - 独立客户端：为线程创建独立的客户端上下文
 * - 状态保护：通过互斥锁保护共享状态的访问
 * - Promise通知：使用线程安全的Promise机制通信
 * 
 * 该函数是迁移接收端的后台工作核心，确保迁移操作不阻塞主服务器线程。
 */
void MigrationDestinationManager::_migrateThread(CancellationToken cancellationToken,
                                                 bool skipToCritSecTaken) {
    // 前置条件验证：确保迁移会话ID已设置
    // 这是迁移开始的基本要求，标识迁移的唯一性
    invariant(_sessionId);

    // 初始化迁移线程：创建专用的客户端和服务上下文
    // 线程名："migrateThread" 便于调试和监控
    // 服务类型：ShardServer 表示这是分片服务器上的操作
    Client::initThread("migrateThread",
                       getGlobalServiceContext()->getService(ClusterRole::ShardServer));
    auto client = Client::getCurrent();
    
    // 恢复状态标志：标记当前是否处于恢复模式
    // 初始为false，在异常处理后会设置为true进行恢复重试
    bool recovering = false;
    
    // 主迁移循环：持续执行直到迁移完成或被取消
    // 支持在发生错误时自动重试和恢复
    while (true) {
        // 创建可取消的操作上下文：
        // 目的：为迁移操作提供独立的执行环境和取消机制
        // 组件：
        // - executor: 固定执行器，用于异步操作调度
        // - cancellationToken: 取消令牌，响应外部取消请求
        // - client: 当前线程的客户端上下文
        const auto executor =
            Grid::get(client->getServiceContext())->getExecutorPool()->getFixedExecutor();
        auto uniqueOpCtx =
            CancelableOperationContext(client->makeOperationContext(), cancellationToken, executor);
        auto opCtx = uniqueOpCtx.get();

        // 权限授予：如果启用了认证，为操作上下文授予内部权限
        // 目的：确保迁移操作能够访问所需的系统资源和集合
        // 范围：内部系统级别的操作权限
        if (AuthorizationManager::get(opCtx->getService())->isAuthEnabled()) {
            AuthorizationSession::get(opCtx->getClient())->grantInternalAuthorization(opCtx);
        }

        try {
            // 恢复检查：如果处于恢复模式，检查恢复文档是否存在
            if (recovering) {
                // 恢复文档存在性检查：确定是否需要执行恢复操作
                // 如果恢复文档不存在，说明迁移已经完成或被清理，无需恢复
                if (!migrationRecipientRecoveryDocumentExists(opCtx, *_sessionId)) {
                    // No need to run any recovery.
                    // 无需运行任何恢复操作，退出循环
                    break;
                }
            }

            // The outer OperationContext is used to hold the session checked out for the
            // duration of the recipient's side of the migration. This guarantees that if the
            // donor shard has failed over, then the new donor primary cannot bump the
            // txnNumber on this session while this node is still executing the recipient side
            // (which is important because otherwise, this node may create orphans after the
            // range deletion task on this node has been processed). The recipient will periodically
            // yield this session, but will verify the txnNumber has not changed before continuing,
            // preserving the guarantee that orphans cannot be created after the txnNumber is
            // advanced.
            // 迁移会话管理：
            // 外层操作上下文用于在接收端迁移期间持有已检出的会话
            // 这保证了如果源分片发生故障转移，新的源主节点无法在此节点仍在执行接收端操作时
            // 提升此会话的txnNumber（这很重要，否则在处理范围删除任务后可能创建孤儿数据）
            // 接收端会定期释放此会话，但在继续之前会验证txnNumber未发生变化，
            // 保证在txnNumber提升后不会创建孤儿数据
            {
                // 设置会话上下文：将逻辑会话ID和事务号绑定到操作上下文
                // 目的：确保迁移操作在正确的会话和事务上下文中执行
                auto lk = stdx::lock_guard(*opCtx->getClient());
                opCtx->setLogicalSessionId(_lsid);      // 设置逻辑会话ID
                opCtx->setTxnNumber(_txnNumber);        // 设置事务号
            }

            // 检出迁移会话：从会话目录中检出当前迁移的会话
            // 目的：独占访问会话状态，防止并发修改
            // 生命周期：在整个迁移过程中保持检出状态
            auto mongoDSessionCatalog = MongoDSessionCatalog::get(opCtx);
            auto sessionTxnState = mongoDSessionCatalog->checkOutSession(opCtx);

            // 事务参与者初始化：获取并初始化事务参与者
            // 功能：管理与此会话关联的事务状态
            // 模式：继续现有事务或开始新事务（如果需要）
            auto txnParticipant = TransactionParticipant::get(opCtx);
            txnParticipant.beginOrContinue(opCtx,
                                           {*opCtx->getTxnNumber()},            // 事务号
                                           boost::none /* autocommit */,       // 不自动提交
                                           TransactionParticipant::TransactionActions::kNone); // 无特殊操作
            
            // 调用迁移驱动器：执行实际的迁移工作
            // 参数说明：
            // - opCtx: 操作上下文，包含会话和事务信息
            // - skipToCritSecTaken || recovering: 是否跳过前期阶段或处于恢复模式
            _migrateDriver(opCtx, skipToCritSecTaken || recovering);
            
        } catch (...) {
            // 异常处理：捕获迁移过程中的所有异常
            // 错误状态设置：将异常信息转换为可读的错误消息并设置迁移状态为失败
            _setStateFail(str::stream() << "migrate failed: " << redact(exceptionToStatus()));

            // 取消检查：如果不是因为取消令牌导致的异常，则进入恢复模式
            // 目的：区分正常取消和异常失败，对异常失败进行恢复重试
            if (!cancellationToken.isCanceled()) {
                // Run recovery if needed.
                // 如果需要，运行恢复逻辑
                recovering = true;
                continue;  // 继续循环，进入恢复模式
            }
        }

        // 正常完成：如果没有异常且不需要恢复，退出主循环
        break;
    }

    // 迁移线程清理阶段：清理会话状态和通知其他组件
    // 
    // 获取互斥锁：保护共享状态的并发访问
    stdx::lock_guard<Latch> lk(_mutex);
    
    // 会话状态重置：清理迁移会话相关的状态
    _sessionId.reset();           // 重置会话ID，表示迁移不再活跃
    _scopedReceiveChunk.reset();  // 重置接收chunk的作用域锁，释放独占访问
    
    // 通知等待线程：唤醒所有等待迁移状态变化的线程
    // 用于同步那些等待迁移完成的操作
    _isActiveCV.notify_all();

    // If we reached this point without having set _canReleaseCriticalSectionPromise we must be on
    // an error path. Just set the promise with error because it is illegal to leave it unset on
    // destruction.
    // 关键区域释放Promise处理：
    // 如果到达此处而未设置_canReleaseCriticalSectionPromise，说明处于错误路径
    // 必须设置Promise为错误状态，因为在析构时保持未设置状态是非法的
    invariant(_canReleaseCriticalSectionPromise);
    if (!_canReleaseCriticalSectionPromise->getFuture().isReady()) {
        // 设置Promise为取消错误，明确表示关键区域释放被中断
        _canReleaseCriticalSectionPromise->setError(
            {ErrorCodes::CallbackCanceled, "explicitly breaking release critical section promise"});
    }
    _canReleaseCriticalSectionPromise.reset();  // 重置Promise指针

    // 迁移线程完成Promise处理：
    // 通知等待迁移线程完成的组件，并传递最终的迁移状态
    // 这是线程间同步的重要机制
    invariant(_migrateThreadFinishedPromise);
    _migrateThreadFinishedPromise->emplaceValue(_state);  // 设置最终状态值
    _migrateThreadFinishedPromise.reset();                // 重置Promise指针
}

/**
 * MigrationDestinationManager::_migrateDriver 函数的作用：
 * 目标分片迁移驱动器的核心执行函数，负责协调整个chunk接收端的迁移流程。
 * 
 * 核心职责：
 * 1. 迁移流程控制：管理从准备阶段到完成的完整迁移生命周期
 * 2. 数据接收协调：与源分片协调进行初始克隆、增量同步和会话数据迁移
 * 3. 关键区域管理：控制进入和退出关键区域，确保数据一致性
 * 4. 状态同步管理：维护迁移状态机，处理状态转换和错误恢复
 * 5. 写关注处理：确保数据复制满足指定的写关注要求
 * 6. 索引和集合创建：在目标分片创建必要的集合结构和索引
 * 7. 范围删除任务：管理重叠范围的清理和新范围的删除任务
 * 8. 会话迁移：协调事务和可重试写操作的会话数据传输
 * 
 * 执行阶段：
 * - 阶段1：清理重叠的范围删除任务
 * - 阶段2：创建集合和索引结构
 * - 阶段3：插入待处理的范围删除任务
 * - 阶段4：执行初始数据克隆
 * - 阶段5：执行增量数据同步（catch-up）
 * - 阶段6：等待提交信号并进入稳定状态
 * - 阶段7：会话数据迁移完成
 * - 阶段8：进入关键区域并等待释放信号
 * 
 * 参数说明：
 * @param outerOpCtx 外层操作上下文，持有迁移会话的生命周期
 * @param skipToCritSecTaken 是否跳过前面阶段直接进入关键区域（用于恢复场景）
 * 
 * 错误处理：
 * - 支持迁移中断后的恢复机制
 * - 处理网络故障和超时情况
 * - 维护状态一致性和资源清理
 * 
 * 并发安全：
 * - 使用会话机制防止并发操作冲突
 * - 通过关键区域确保原子性操作
 * - 支持主从切换场景下的状态恢复
 * 
 * 该函数是MongoDB分片迁移接收端的核心控制器，确保数据迁移的可靠性和一致性。
 */
void MigrationDestinationManager::_migrateDriver(OperationContext* outerOpCtx,
                                                 bool skipToCritSecTaken) {
    // 前置条件检查：确保迁移管理器处于活跃状态
    invariant(isActive());
    invariant(_sessionId);           // 必须有有效的迁移会话ID
    invariant(_scopedReceiveChunk);  // 必须持有接收chunk的作用域锁
    invariant(!_min.isEmpty());     // chunk的最小边界不能为空
    invariant(!_max.isEmpty());     // chunk的最大边界不能为空

    // 性能监控和时间统计相关变量
    boost::optional<Timer> timeInCriticalSection;  // 关键区域时间统计
    boost::optional<MoveTimingHelper> timing;      // 迁移各阶段时间统计
    
    // 错误消息设置守护：确保在销毁时将错误信息传递给时间统计助手
    mongo::ScopeGuard timingSetMsgGuard{[this, &timing] {
        // Set the error message to MoveTimingHelper just before it is destroyed. The destructor
        // sends that message (among other things) to the ShardingLogging.
        if (timing) {
            stdx::lock_guard<Latch> sl(_mutex);
            timing->setCmdErrMsg(_errmsg);  // 设置错误消息到时间统计中
        }
    }};

    // 判断是否需要执行完整的迁移流程（非恢复模式）
    if (!skipToCritSecTaken) {
        // If this is a configShard, throw if we are draining. This is to avoid creating the
        // db/collections on the local catalog once we have already completed cleanup after drain.
        // 配置分片draining状态检查：如果是配置分片且有分片正在draining，则抛出异常
        // 目的：避免在drain清理完成后再次创建数据库/集合
        if (serverGlobalParams.clusterRole.has(ClusterRole::ConfigServer)) {
            checkConfigShardIsNotDraining(outerOpCtx);
        }

        // 初始化时间统计助手：记录迁移的8个主要步骤
        timing.emplace(outerOpCtx, "to", _nss, _min, _max, 8 /* steps */, _toShard, _fromShard);

        // 记录迁移开始日志
        LOGV2(22000,
              "Starting receiving end of chunk migration",
              "chunkMin"_attr = redact(_min),
              "chunkMax"_attr = redact(_max),
              logAttrs(_nss),
              "fromShard"_attr = _fromShard,
              "sessionId"_attr = *_sessionId,
              "migrationId"_attr = _migrationId->toBSON());

        // 中止状态检查：如果迁移已被中止，则提前返回
        const auto initialState = getState();
        if (initialState == kAbort) {
            LOGV2_ERROR(22013,
                        "Migration abort requested before the migration started",
                        "migrationId"_attr = _migrationId->toBSON());
            return;
        }
        invariant(initialState == kReady);  // 确保初始状态为就绪

        // 获取源分片的集合选项和索引信息
        // 用于在目标分片创建相同的集合结构
        auto donorCollectionOptionsAndIndexes = [&]() -> CollectionOptionsAndIndexes {
            // 获取集合选项和UUID
            auto [collOptions, uuid] =
                getCollectionOptions(outerOpCtx, _nss, _fromShard, boost::none, boost::none);
            // 获取索引信息
            auto [indexes, idIndex] =
                getCollectionIndexes(outerOpCtx, _nss, _fromShard, boost::none, boost::none);
            return {uuid, indexes, idIndex, collOptions};
        }();

        // 存储集合UUID，用于后续的范围删除任务等操作
        _collectionUuid = donorCollectionOptionsAndIndexes.uuid;

        // 获取源分片的连接对象，用于后续的网络通信
        auto fromShard = uassertStatusOK(
            Grid::get(outerOpCtx)->shardRegistry()->getShard(outerOpCtx, _fromShard));

        // 定义迁移的chunk范围
        const ChunkRange range(_min, _max);

        // 1. Ensure any data which might have been left orphaned in the range being moved has been
        // deleted.
        // 阶段1：确保迁移范围内的孤儿数据已被删除
        // 计算等待范围删除完成的截止时间
        
        /* 重叠的范围删除任务处理逻辑举例：
        // 时间线：T1 - 之前的迁移失败
        // 源分片：shard1，目标分片：shard2
        // 迁移范围：{userId: {$gte: 100, $lt: 200}}
        
        // T1: 迁移过程中失败，目标分片 shard2 上留下了部分数据和删除任务
        // config.rangeDeletions 中存在：
        {
          _id: ObjectId("..."),
          nss: "myapp.users", 
          collectionUuid: UUID("12345678-1234-5678-9abc-123456789abc"),
          donorShardId: "shard1",
          range: {
            min: {userId: 100},
            max: {userId: 200}
          },
          whenToClean: "now",
          pending: false  // 正在执行删除
        }
        
        // T2: 新的迁移请求到达 shard2
        // 新迁移范围：{userId: {$gte: 150, $lt: 250}}
        // 
        // 重叠检测：
        // 现有删除范围：[100, 200)
        // 新迁移范围：  [150, 250)
        // 重叠区域：    [150, 200)  ← 这就是重叠的范围删除！ */
        const auto rangeDeletionWaitDeadline =
            outerOpCtx->getServiceContext()->getFastClockSource()->now() +
            Milliseconds(drainOverlappingRangeDeletionsOnStartTimeoutMS.load());

        // 循环等待重叠的范围删除任务完成
        while (runWithoutSession(outerOpCtx, [&] {
            return rangedeletionutil::checkForConflictingDeletions(
                outerOpCtx, range, donorCollectionOptionsAndIndexes.uuid);
        })) {
            // 检查范围删除器是否被禁用
            uassert(ErrorCodes::ResumableRangeDeleterDisabled,
                    "Failing migration because the disableResumableRangeDeleter server "
                    "parameter is set to true on the recipient shard, which contains range "
                    "deletion tasks overlapping the incoming range.",
                    !disableResumableRangeDeleter.load());

            // 记录等待重叠范围删除完成的日志
            LOGV2(22001,
                  "Migration paused because the requested range overlaps with a range already "
                  "scheduled for deletion",
                  logAttrs(_nss),
                  "range"_attr = redact(range.toString()),
                  "migrationId"_attr = _migrationId->toBSON());

            // 等待范围清理完成
            auto status =
                CollectionShardingRuntime::waitForClean(outerOpCtx,
                                                        _nss,
                                                        donorCollectionOptionsAndIndexes.uuid,
                                                        range,
                                                        rangeDeletionWaitDeadline);

            // 处理等待结果
            if (!status.isOK() && status != ErrorCodes::ExceededTimeLimit) {
                _setStateFail(redact(status.toString()));
                return;
            }

            // 超时检查：确保在截止时间内完成范围清理
            uassert(
                ErrorCodes::ExceededTimeLimit,
                "Migration failed because the orphans cleanup routine didn't clear yet a portion "
                "of the range being migrated that was previously owned by the recipient "
                "shard.",
                status != ErrorCodes::ExceededTimeLimit &&
                    outerOpCtx->getServiceContext()->getFastClockSource()->now() <
                        rangeDeletionWaitDeadline);

            // If the filtering metadata was cleared while the range deletion task was ongoing, then
            // 'waitForClean' would return immediately even though there really is an ongoing range
            // deletion task. For that case, we loop again until there is no conflicting task in
            // config.rangeDeletions
            // 防止过滤元数据清理导致的误判：继续循环直到确实没有冲突的删除任务
            outerOpCtx->sleepFor(Milliseconds(1000));
        }

        // 完成第1步时间统计
        timing->done(1);
        migrateThreadHangAtStep1.pauseWhileSet();


        // 2. Create the parent collection and its indexes, if needed.
        // 阶段2：创建父集合及其索引（如果需要）
        // The conventional usage of retryable writes is to assign statement id's to all of
        // the writes done as part of the data copying so that _recvChunkStart is
        // conceptually a retryable write batch. However, we are using an alternate approach to do
        // those writes under an AlternativeClientRegion because 1) threading the statement id's
        // through to all the places where they are needed would make this code more complex, and 2)
        // some of the operations, like creating the collection or building indexes, are not
        // currently supported in retryable writes.
        // 设置操作上下文标志：允许在主从切换时中断
        outerOpCtx->setAlwaysInterruptAtStepDownOrUp_UNSAFE();
        {
            // 创建新的客户端和操作上下文用于集合和索引创建
            auto newClient = outerOpCtx->getServiceContext()
                                 ->getService(ClusterRole::ShardServer)
                                 ->makeClient("MigrationCoordinator");
            AlternativeClientRegion acr(newClient);  // 切换到新的客户端上下文
            auto executor =
                Grid::get(outerOpCtx->getServiceContext())->getExecutorPool()->getFixedExecutor();
            auto altOpCtx = CancelableOperationContext(
                cc().makeOperationContext(), outerOpCtx->getCancellationToken(), executor);

            // Enable write blocking bypass to allow migrations to create the collection and indexes
            // even when user writes are blocked.
            // 启用写阻塞绕过：允许在用户写操作被阻塞时仍能创建集合和索引
            WriteBlockBypass::get(altOpCtx.get()).set(true);

            // 删除本地不需要的索引（如果存在的话）
            _dropLocalIndexesIfNecessary(altOpCtx.get(), _nss, donorCollectionOptionsAndIndexes);
            
            // 克隆集合的索引和选项到目标分片
            cloneCollectionIndexesAndOptions(
                altOpCtx.get(), _nss, donorCollectionOptionsAndIndexes);

            // Get the global indexes and install them.
            // yang add todo xxxxxx  8.0全局索引分享
            // 获取并安装全局索引（如果功能启用）
            if (feature_flags::gGlobalIndexesShardingCatalog.isEnabled(
                    serverGlobalParams.featureCompatibility.acquireFCVSnapshot())) {
                replaceShardingIndexCatalogInShardIfNeeded(
                    altOpCtx.get(), _nss, donorCollectionOptionsAndIndexes.uuid);
            }

            // 完成第2步时间统计
            timing->done(2);
            migrateThreadHangAtStep2.pauseWhileSet();
        }

        {
            // 3. Insert a pending range deletion task for the incoming range.
            // 阶段3：为传入的范围插入待处理的范围删除任务
            // 创建接收端范围删除任务：标记为待处理状态
            // 任务正是用来在迁移失败／中断时“回滚”目标分片上已写入的脏数据的
            RangeDeletionTask recipientDeletionTask(*_migrationId,
                                                    _nss,
                                                    donorCollectionOptionsAndIndexes.uuid,
                                                    _fromShard,
                                                    range,
                                                    CleanWhenEnum::kNow);
            recipientDeletionTask.setPending(true);  // 设置为待处理状态
            
            // 设置时间戳和分片键模式
            const auto currentTime = VectorClock::get(outerOpCtx)->getTime();
            recipientDeletionTask.setTimestamp(currentTime.clusterTime().asTimestamp());
            recipientDeletionTask.setKeyPattern(KeyPattern(_shardKeyPattern));

            // It is illegal to wait for write concern with a session checked out, so persist the
            // range deletion task with an immediately satsifiable write concern and then wait for
            // majority after yielding the session.
            // 持久化范围删除任务：使用立即满足的写关注，然后在释放会话后等待多数派确认
            rangedeletionutil::persistRangeDeletionTaskLocally(
                outerOpCtx, recipientDeletionTask, WriteConcernOptions());

            // 在无会话状态下等待多数派写关注
            runWithoutSession(outerOpCtx, [&] {
                WriteConcernResult ignoreResult;
                auto latestOpTime =
                    repl::ReplClientInfo::forClient(outerOpCtx->getClient()).getLastOp();
                uassertStatusOK(
                    waitForWriteConcern(outerOpCtx,
                                        latestOpTime,
                                        WriteConcerns::kMajorityWriteConcernShardingTimeout,
                                        &ignoreResult));
            });

            // 完成第3步时间统计
            timing->done(3);
            migrateThreadHangAtStep3.pauseWhileSet();
        }

        // 创建用于数据迁移的新客户端和操作上下文
        auto newClient = outerOpCtx->getServiceContext()
                             ->getService(ClusterRole::ShardServer)
                             ->makeClient("MigrationCoordinator");
        AlternativeClientRegion acr(newClient);
        auto executor =
            Grid::get(outerOpCtx->getServiceContext())->getExecutorPool()->getFixedExecutor();
        auto newOpCtxPtr = CancelableOperationContext(
            cc().makeOperationContext(), outerOpCtx->getCancellationToken(), executor);
        auto opCtx = newOpCtxPtr.get();
        repl::OpTime lastOpApplied;  // 记录最后应用的操作时间
        
        {
            // 4. Initial bulk clone
            // 阶段4：初始批量克隆
            _setState(kClone);  // 设置状态为克隆中

            // 启动会话迁移服务
            // SessionCatalogMigrationDestination::start
            _sessionMigration->start(opCtx->getServiceContext());

            _chunkMarkedPending = true;  // no lock needed, only the migrate thread looks.
                                         // 标记chunk为待处理状态

            {
                // Destructor of MigrationBatchFetcher is non-trivial. Therefore,
                // this scope has semantic significance.
                // MigrationBatchFetcher的析构函数不是简单的，因此这个作用域具有语义意义
                // 创建迁移批次获取器：负责从源分片获取数据并插入到本地
                MigrationBatchFetcher<MigrationBatchInserter> fetcher{
                    outerOpCtx,                          // 外层操作上下文
                    opCtx,                               // 内层操作上下文
                    _nss,                                // 命名空间
                    *_sessionId,                         // 会话ID
                    _writeConcern,                       // 写关注
                    _fromShard,                          // 源分片
                    range,                               // 迁移范围
                    *_migrationId,                       // 迁移ID
                    *_collectionUuid,                    // 集合UUID
                    _migrationCloningProgress,           // 克隆进度共享状态
                    _parallelFetchersSupported,         // 是否支持并行获取器
                    chunkMigrationFetcherMaxBufferedSizeBytesPerThread.load()  // 最大缓冲区大小
                };
                // 执行获取和调度插入操作， 这里会循环发送 _migrateClone 命令给源分片从而获取数据
                fetcher.fetchAndScheduleInsertion();
            }
            
            // 检查操作是否被中断
            opCtx->checkForInterrupt();
            
            // 获取克隆过程中的最大操作时间
            lastOpApplied = _migrationCloningProgress->getMaxOptime();

            // 完成第4步时间统计
            timing->done(4);
            migrateThreadHangAtStep4.pauseWhileSet();

            // 故障点检查：模拟接收端迁移失败
            if (MONGO_unlikely(failMigrationOnRecipient.shouldFail())) {
                _setStateFail(str::stream() << "failing migration after cloning " << _getNumCloned()
                                            << " docs due to failMigrationOnRecipient failpoint");
                return;
            }
        }

        // 创建传输修改请求：用于获取增量数据
        const BSONObj xferModsRequest = createTransferModsRequest(_nss, *_sessionId);

        {
            // 5. Do bulk of mods
            // 阶段5：执行大量修改操作（增量同步）
            _setState(kCatchup);  // 设置状态为追赶中

            // 定义批次获取函数：从源分片获取增量修改数据
            auto fetchBatchFn = [&](OperationContext* opCtx, BSONObj* nextBatch) {
                // 向源分片发送_transferMods命令获取增量数据
                auto commandResponse = uassertStatusOKWithContext(
                    fromShard->runCommand(opCtx,
                                          ReadPreferenceSetting(ReadPreference::PrimaryOnly),
                                          DatabaseName::kAdmin,
                                          xferModsRequest,
                                          Shard::RetryPolicy::kNoRetry),
                    "_transferMods failed: ");

                // 检查命令执行状态
                uassertStatusOKWithContext(
                    Shard::CommandResponse::getEffectiveStatus(commandResponse),
                    "_transferMods failed: ");

                *nextBatch = commandResponse.response;
                // 返回true表示这是空批次（没有更多数据）
                return nextBatch->getField("size").number() == 0;
            };

            // 定义修改应用函数：将增量修改应用到本地集合
            auto applyModsFn = [&](OperationContext* opCtx, BSONObj nextBatch) {
                // 如果批次大小为0，结束追赶阶段
                if (nextBatch["size"].number() == 0) {
                    // There are no more pending modifications to be applied. End the catchup phase
                    return false;
                }

                // 应用迁移操作到本地
                if (!_applyMigrateOp(opCtx, nextBatch)) {
                    return true;
                }
                
                // 更新追赶阶段的字节统计
                ShardingStatistics::get(opCtx).countBytesClonedOnCatchUpOnRecipient.addAndFetch(
                    nextBatch["size"].number());

                // 等待复制完成的最大迭代次数
                const int maxIterations = 3600 * 50;

                int i;
                for (i = 0; i < maxIterations; i++) {
                    // 检查操作是否被中断
                    opCtx->checkForInterrupt();
                    outerOpCtx->checkForInterrupt();

                    // 检查迁移是否被中止
                    uassert(
                        ErrorCodes::CommandFailed,
                        str::stream()
                            << "Migration aborted while waiting for replication at catch up stage, "
                            << _migrationId->toBSON(),
                        getState() != kAbort);

                    // 检查操作是否已复制到足够多的节点
                    if (runWithoutSession(outerOpCtx, [&] {
                            return opReplicatedEnough(opCtx, lastOpApplied, _writeConcern);
                        })) {
                        return true;
                    }

                    // 记录从节点跟上困难的警告
                    if (i > 100) {
                        LOGV2(22003,
                              "secondaries having hard time keeping up with migrate",
                              "migrationId"_attr = _migrationId->toBSON());
                    }

                    sleepmillis(20);  // 短暂休眠后重试
                }

                // 如果达到最大迭代次数仍未复制完成，则失败
                uassert(ErrorCodes::CommandFailed,
                        "Secondary can't keep up with migrate",
                        i != maxIterations);

                return true;
            };

            // 执行获取和应用批次的循环
            auto updatedTime = fetchAndApplyBatch(opCtx, applyModsFn, fetchBatchFn);
            lastOpApplied = (updatedTime == repl::OpTime()) ? lastOpApplied : updatedTime;

            // 完成第5步时间统计
            timing->done(5);
            migrateThreadHangAtStep5.pauseWhileSet();
        }

        {
            // Pause to wait for replication. This will prevent us from going into critical section
            // until we're ready.
            // 暂停等待复制完成：这将防止我们在准备好之前进入关键区域

            LOGV2(22004,
                  "Waiting for replication to catch up before entering critical section",
                  "migrationId"_attr = _migrationId->toBSON());
            LOGV2_DEBUG_OPTIONS(4817411,
                                2,
                                {logv2::LogComponent::kShardMigrationPerf},
                                "Starting majority commit wait on recipient",
                                "migrationId"_attr = _migrationId->toBSON());

            // 在无会话状态下等待多数派复制完成
            runWithoutSession(outerOpCtx, [&] {
                auto awaitReplicationResult =
                    repl::ReplicationCoordinator::get(opCtx)->awaitReplication(
                        opCtx, lastOpApplied, WriteConcerns::kMajorityWriteConcernShardingTimeout);
                uassertStatusOKWithContext(awaitReplicationResult.status,
                                           awaitReplicationResult.status.codeString());
            });

            LOGV2(22005,
                  "Chunk data replicated successfully.",
                  "migrationId"_attr = _migrationId->toBSON());
            LOGV2_DEBUG_OPTIONS(4817412,
                                2,
                                {logv2::LogComponent::kShardMigrationPerf},
                                "Finished majority commit wait on recipient",
                                "migrationId"_attr = _migrationId->toBSON());
        }

        {
            // 6. Wait for commit
            // 阶段6：等待提交信号
            _setState(kSteady);  // 设置状态为稳定
            migrateThreadHangAfterSteadyTransition.pauseWhileSet();

            bool transferAfterCommit = false;  // 标记是否在提交后进行了传输
            
            // 循环处理稳定状态和提交开始状态
            while (getState() == kSteady || getState() == kCommitStart) {
                opCtx->checkForInterrupt();
                outerOpCtx->checkForInterrupt();

                // Make sure we do at least one transfer after recv'ing the commit message. If we
                // aren't sure that at least one transfer happens *after* our state changes to
                // COMMIT_START, there could be mods still on the FROM shard that got logged
                // *after* our _transferMods but *before* the critical section.
                // 确保在收到提交消息后至少进行一次传输
                // 防止在关键区域之前遗漏源分片上的修改操作
                if (getState() == kCommitStart) {
                    transferAfterCommit = true;
                }

                // 继续从源分片获取增量修改
                auto res = uassertStatusOKWithContext(
                    fromShard->runCommand(opCtx,
                                          ReadPreferenceSetting(ReadPreference::PrimaryOnly),
                                          DatabaseName::kAdmin,
                                          xferModsRequest,
                                          Shard::RetryPolicy::kNoRetry),
                    "_transferMods failed in STEADY STATE: ");

                uassertStatusOKWithContext(Shard::CommandResponse::getEffectiveStatus(res),
                                           "_transferMods failed in STEADY STATE: ");

                auto mods = res.response;

                // 如果有修改数据，应用它们
                if (mods["size"].number() > 0) {
                    (void)_applyMigrateOp(opCtx, mods);
                    lastOpApplied = repl::ReplClientInfo::forClient(opCtx->getClient()).getLastOp();
                    continue;
                }

                // 检查是否被中止
                if (getState() == kAbort) {
                    LOGV2(22006,
                          "Migration aborted while transferring mods",
                          "migrationId"_attr = _migrationId->toBSON());
                    return;
                }

                // We know we're finished when:
                // 1) The from side has told us that it has locked writes (COMMIT_START)
                // 2) We've checked at least one more time for un-transmitted mods
                // 完成条件判断：
                // 1) 源端已通知写操作被锁定（COMMIT_START状态）
                // 2) 至少再检查一次未传输的修改
                if (getState() == kCommitStart && transferAfterCommit == true) {
                    // 刷新待处理的写操作
                    if (runWithoutSession(outerOpCtx, [&] {
                            return _flushPendingWrites(opCtx, lastOpApplied);
                        })) {
                        break;  // 刷新成功，退出循环
                    }
                }

                // Only sleep if we aren't committing
                // 只有在非提交状态时才休眠
                if (getState() == kSteady)
                    sleepmillis(10);
            }

            // 检查最终状态是否为失败或中止
            if (getState() == kFail || getState() == kAbort) {
                _setStateFail("timed out waiting for commit");
                return;
            }

            // 完成第6步时间统计
            timing->done(6);
            migrateThreadHangAtStep6.pauseWhileSet();
        }

        // 等待会话迁移完成
        runWithoutSession(outerOpCtx, [&] { _sessionMigration->join(); });
        
        // 检查会话迁移是否发生错误
        if (_sessionMigration->getState() ==
            SessionCatalogMigrationDestination::State::ErrorOccurred) {
            _setStateFail(redact(_sessionMigration->getErrMsg()));
            return;
        }

        // 完成第7步时间统计
        timing->done(7);
        migrateThreadHangAtStep7.pauseWhileSet();

        // 创建关键区域原因对象
        const auto critSecReason = criticalSectionReason(*_sessionId);

        // 进入关键区域的准备工作
        runWithoutSession(outerOpCtx, [&] {
            // Persist the migration recipient recovery document so that in case of failover, the
            // new primary will resume the MigrationDestinationManager and retake the critical
            // section.
            // 持久化迁移接收端恢复文档：确保故障转移时新主节点能够恢复迁移状态
            migrationutil::persistMigrationRecipientRecoveryDocument(
                opCtx, {*_migrationId, _nss, *_sessionId, range, _fromShard, _lsid, _txnNumber});

            LOGV2_DEBUG(5899113,
                        2,
                        "Persisted migration recipient recovery document",
                        "sessionId"_attr = _sessionId);

            // Enter critical section. Ensure it has been majority commited before _recvChunkCommit
            // returns success to the donor, so that if the recipient steps down, the critical
            // section is kept taken while the donor commits the migration.
            // 进入关键区域：确保在_recvChunkCommit向源端返回成功之前已多数派提交
            // 这样如果接收端降级，关键区域仍被占用，直到源端提交迁移
            ShardingRecoveryService::get(opCtx)->acquireRecoverableCriticalSectionBlockWrites(
                opCtx, _nss, critSecReason, ShardingCatalogClient::kMajorityWriteConcern);

            LOGV2(5899114, "Entered migration recipient critical section", logAttrs(_nss));
            timeInCriticalSection.emplace();  // 开始计时关键区域时间
        });

        // 检查进入关键区域后的状态
        if (getState() == kFail || getState() == kAbort) {
            _setStateFail("timed out waiting for critical section acquisition");
        }

        {
            // Make sure we don't overwrite a FAIL or ABORT state.
            // 确保不覆盖FAIL或ABORT状态
            stdx::lock_guard<Latch> sl(_mutex);
            if (_state != kFail && _state != kAbort) {
                _state = kEnteredCritSec;  // 设置状态为已进入关键区域
                _stateChangedCV.notify_all();
            }
        }
    } else {
        // 恢复模式：跳过前面阶段直接处理关键区域
        outerOpCtx->setAlwaysInterruptAtStepDownOrUp_UNSAFE();
        auto newClient = outerOpCtx->getServiceContext()
                             ->getService(ClusterRole::ShardServer)
                             ->makeClient("MigrationCoordinator");
        AlternativeClientRegion acr(newClient);
        auto executor =
            Grid::get(outerOpCtx->getServiceContext())->getExecutorPool()->getFixedExecutor();
        auto newOpCtxPtr = CancelableOperationContext(
            cc().makeOperationContext(), outerOpCtx->getCancellationToken(), executor);
        auto opCtx = newOpCtxPtr.get();

        // 重新获取可恢复的关键区域
        ShardingRecoveryService::get(opCtx)->acquireRecoverableCriticalSectionBlockWrites(
            opCtx,
            _nss,
            criticalSectionReason(*_sessionId),
            ShardingCatalogClient::kMajorityWriteConcern);

        LOGV2_DEBUG(6064501,
                    2,
                    "Reacquired migration recipient critical section",
                    "sessionId"_attr = *_sessionId);

        {
            stdx::lock_guard<Latch> sl(_mutex);
            _state = kEnteredCritSec;  // 设置状态为已进入关键区域
            _stateChangedCV.notify_all();
        }

        LOGV2(6064503, "Recovered migration recipient", "sessionId"_attr = *_sessionId);
    }

    // 最终阶段：等待关键区域释放信号并完成迁移
    outerOpCtx->setAlwaysInterruptAtStepDownOrUp_UNSAFE();
    auto newClient = outerOpCtx->getServiceContext()
                         ->getService(ClusterRole::ShardServer)
                         ->makeClient("MigrationCoordinator");
    AlternativeClientRegion acr(newClient);
    auto executor =
        Grid::get(outerOpCtx->getServiceContext())->getExecutorPool()->getFixedExecutor();
    auto newOpCtxPtr = CancelableOperationContext(
        cc().makeOperationContext(), outerOpCtx->getCancellationToken(), executor);
    auto opCtx = newOpCtxPtr.get();

    // 确保关键区域时间计时器已初始化
    if (skipToCritSecTaken) {
        timeInCriticalSection.emplace();
    }
    invariant(timeInCriticalSection);

    // Wait until signaled to exit the critical section and then release it.
    // 等待退出关键区域的信号，然后释放关键区域
    runWithoutSession(outerOpCtx, [&] {
        awaitCriticalSectionReleaseSignalAndCompleteMigration(opCtx, *timeInCriticalSection);
    });

    // 设置最终状态为完成
    _setState(kDone);

    // 完成第8步时间统计（如果有时间统计助手）
    if (timing) {
        timing->done(8);
    }
}

bool MigrationDestinationManager::_applyMigrateOp(OperationContext* opCtx, const BSONObj& xfer) {
    bool didAnything = false;
    long long changeInOrphans = 0;
    long long totalDocs = 0;

    // Deleted documents
    if (xfer["deleted"].isABSONObj()) {
        BSONObjIterator i(xfer["deleted"].Obj());
        while (i.more()) {
            totalDocs++;
            const auto collection = acquireCollection(
                opCtx,
                CollectionAcquisitionRequest(_nss,
                                             AcquisitionPrerequisites::kPretendUnsharded,
                                             repl::ReadConcernArgs::get(opCtx),
                                             AcquisitionPrerequisites::kWrite),
                MODE_IX);
            uassert(ErrorCodes::ConflictingOperationInProgress,
                    str::stream() << "Collection " << _nss.toStringForErrorMsg()
                                  << " was dropped in the middle of the migration",
                    collection.exists());

            BSONObj id = i.next().Obj();

            // Do not apply delete if doc does not belong to the chunk being migrated
            BSONObj fullObj;
            if (Helpers::findById(opCtx, _nss, id, fullObj)) {
                if (!isInRange(fullObj, _min, _max, _shardKeyPattern)) {
                    if (MONGO_unlikely(failMigrationReceivedOutOfRangeOperation.shouldFail())) {
                        MONGO_UNREACHABLE;
                    }
                    continue;
                }
            }

            writeConflictRetry(opCtx, "transferModsDeletes", _nss, [&] {
                deleteObjects(opCtx,
                              collection,
                              id,
                              true /* justOne */,
                              false /* god */,
                              true /* fromMigrate */);
            });

            changeInOrphans--;
            didAnything = true;
        }
    }

    // Inserted or updated documents
    if (xfer["reload"].isABSONObj()) {
        BSONObjIterator i(xfer["reload"].Obj());
        while (i.more()) {
            totalDocs++;
            auto collection = acquireCollection(
                opCtx,
                CollectionAcquisitionRequest(_nss,
                                             AcquisitionPrerequisites::kPretendUnsharded,
                                             repl::ReadConcernArgs::get(opCtx),
                                             AcquisitionPrerequisites::kWrite),
                MODE_IX);
            uassert(ErrorCodes::ConflictingOperationInProgress,
                    str::stream() << "Collection " << _nss.toStringForErrorMsg()
                                  << " was dropped in the middle of the migration",
                    collection.exists());

            BSONObj updatedDoc = i.next().Obj();

            // do not apply insert/update if doc does not belong to the chunk being migrated
            if (!isInRange(updatedDoc, _min, _max, _shardKeyPattern)) {
                if (MONGO_unlikely(failMigrationReceivedOutOfRangeOperation.shouldFail())) {
                    MONGO_UNREACHABLE;
                }
                continue;
            }

            BSONObj localDoc;
            if (willOverrideLocalId(
                    opCtx, _nss, _min, _max, _shardKeyPattern, updatedDoc, &localDoc)) {
                // Exception will abort migration cleanly
                LOGV2_ERROR_OPTIONS(
                    16977,
                    {logv2::UserAssertAfterLog()},
                    "Cannot migrate chunk because the local document has the same _id as the "
                    "reloaded remote document",
                    "localDoc"_attr = redact(localDoc),
                    "remoteDoc"_attr = redact(updatedDoc),
                    "migrationId"_attr = _migrationId->toBSON());
            }

            // We are in write lock here, so sure we aren't killing
            writeConflictRetry(opCtx, "transferModsUpdates", _nss, [&] {
                auto res = Helpers::upsert(opCtx, collection, updatedDoc, true);
                if (!res.upsertedId.isEmpty()) {
                    changeInOrphans++;
                }
            });

            didAnything = true;
        }
    }

    if (changeInOrphans != 0) {
        rangedeletionutil::persistUpdatedNumOrphans(
            opCtx, *_collectionUuid, ChunkRange(_min, _max), changeInOrphans);
    }

    ShardingStatistics::get(opCtx).countDocsClonedOnCatchUpOnRecipient.addAndFetch(totalDocs);

    return didAnything;
}

bool MigrationDestinationManager::_flushPendingWrites(OperationContext* opCtx,
                                                      const repl::OpTime& lastOpApplied) {
    if (!opReplicatedEnough(opCtx, lastOpApplied, _writeConcern)) {
        repl::OpTime op(lastOpApplied);
        static Occasionally sampler;
        if (sampler.tick()) {
            LOGV2(22007,
                  "Migration commit waiting for majority replication; waiting until the last "
                  "operation applied has been replicated",
                  logAttrs(_nss),
                  "chunkMin"_attr = redact(_min),
                  "chunkMax"_attr = redact(_max),
                  "lastOpApplied"_attr = op,
                  "migrationId"_attr = _migrationId->toBSON());
        }
        return false;
    }

    LOGV2(22008,
          "Migration commit succeeded flushing to secondaries",
          logAttrs(_nss),
          "chunkMin"_attr = redact(_min),
          "chunkMax"_attr = redact(_max),
          "migrationId"_attr = _migrationId->toBSON());

    return true;
}

void MigrationDestinationManager::awaitCriticalSectionReleaseSignalAndCompleteMigration(
    OperationContext* opCtx, const Timer& timeInCriticalSection) {
    // Wait until the migrate thread is signaled to release the critical section
    LOGV2_DEBUG(5899111, 3, "Waiting for release critical section signal");
    invariant(_canReleaseCriticalSectionPromise);
    _canReleaseCriticalSectionPromise->getFuture().get(opCtx);

    _setState(kExitCritSec);

    // Refresh the filtering metadata
    LOGV2_DEBUG(5899112, 3, "Refreshing filtering metadata before exiting critical section");

    bool refreshFailed = false;
    try {
        if (MONGO_unlikely(migrationRecipientFailPostCommitRefresh.shouldFail())) {
            uasserted(ErrorCodes::InternalError, "skipShardFilteringMetadataRefresh failpoint");
        }

        forceShardFilteringMetadataRefresh(opCtx, _nss);
    } catch (const DBException& ex) {
        LOGV2_DEBUG(5899103,
                    2,
                    "Post-migration commit refresh failed on recipient",
                    "migrationId"_attr = _migrationId,
                    "error"_attr = redact(ex));
        refreshFailed = true;
    }

    if (refreshFailed) {
        AutoGetCollection autoColl(opCtx, _nss, MODE_IX);
        CollectionShardingRuntime::assertCollectionLockedAndAcquireExclusive(opCtx, _nss)
            ->clearFilteringMetadata(opCtx);
    }

    // Release the critical section
    LOGV2_DEBUG(5899110, 3, "Exiting critical section");
    const auto critSecReason = criticalSectionReason(*_sessionId);

    ShardingRecoveryService::get(opCtx)->releaseRecoverableCriticalSection(
        opCtx,
        _nss,
        critSecReason,
        ShardingCatalogClient::kMajorityWriteConcern,
        ShardingRecoveryService::NoCustomAction());

    const auto timeInCriticalSectionMs = timeInCriticalSection.millis();
    ShardingStatistics::get(opCtx).totalRecipientCriticalSectionTimeMillis.addAndFetch(
        timeInCriticalSectionMs);

    LOGV2(5899108,
          "Exited migration recipient critical section",
          logAttrs(_nss),
          "durationMillis"_attr = timeInCriticalSectionMs);

    // Wait for the updates to the catalog cache to be written to disk before removing the
    // recovery document. This ensures that on case of stepdown, the new primary will know of a
    // placement version inclusive of the migration. NOTE: We rely on the
    // deleteMigrationRecipientRecoveryDocument call below to wait for the CatalogCache on-disk
    // persistence to be majority committed.
    CatalogCacheLoader::get(opCtx).waitForCollectionFlush(opCtx, _nss);

    // Delete the recovery document
    migrationutil::deleteMigrationRecipientRecoveryDocument(opCtx, *_migrationId);
}

void MigrationDestinationManager::onStepUpBegin(OperationContext* opCtx, long long term) {
    stdx::lock_guard<Latch> sl(_mutex);
    auto newCancellationSource = CancellationSource();
    std::swap(_cancellationSource, newCancellationSource);
}

void MigrationDestinationManager::onStepDown() {
    boost::optional<SharedSemiFuture<State>> migrateThreadFinishedFuture;
    {
        stdx::lock_guard<Latch> sl(_mutex);
        // Cancel any migrateThread work.
        _cancellationSource.cancel();

        if (_migrateThreadFinishedPromise) {
            migrateThreadFinishedFuture = _migrateThreadFinishedPromise->getFuture();
        }
    }

    // Wait for the migrateThread to finish.
    if (migrateThreadFinishedFuture) {
        LOGV2(8991401,
              "Waiting for migrate thread to finish on stepdown",
              "migrationId"_attr = _migrationId);
        migrateThreadFinishedFuture->wait();
    }
}

}  // namespace mongo
