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

#include "mongo/db/s/migration_chunk_cloner_source.h"

#include <absl/container/node_hash_map.h>
#include <absl/strings/string_view.h>
#include <boost/move/utility_core.hpp>
#include <boost/none.hpp>
#include <boost/optional/optional.hpp>
#include <fmt/format.h>
// IWYU pragma: no_include "cxxabi.h"
#include <algorithm>
#include <cstdint>
#include <iterator>
#include <limits>
#include <string>
#include <utility>

#include "mongo/base/error_codes.h"
#include "mongo/base/status.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonelement.h"
#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/oid.h"
#include "mongo/bson/util/builder.h"
#include "mongo/db/basic_types.h"
#include "mongo/db/catalog/index_catalog.h"
#include "mongo/db/catalog/index_catalog_entry.h"
#include "mongo/db/catalog_raii.h"
#include "mongo/db/client.h"
#include "mongo/db/concurrency/lock_manager_defs.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/index/index_access_method.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/keypattern.h"
#include "mongo/db/ops/write_ops_retryability.h"
#include "mongo/db/query/index_bounds.h"
#include "mongo/db/query/plan_yield_policy.h"
#include "mongo/db/query/query_knobs_gen.h"
#include "mongo/db/repl/oplog_entry_gen.h"
#include "mongo/db/repl/optime.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/db/s/collection_metadata.h"
#include "mongo/db/s/collection_sharding_runtime.h"
#include "mongo/db/s/migration_source_manager.h"
#include "mongo/db/s/shard_key_index_util.h"
#include "mongo/db/s/sharding_runtime_d_params_gen.h"
#include "mongo/db/s/sharding_statistics.h"
#include "mongo/db/s/start_chunk_clone_request.h"
#include "mongo/db/service_context.h"
#include "mongo/db/session/logical_session_id_helpers.h"
#include "mongo/db/shard_id.h"
#include "mongo/db/transaction_resources.h"
#include "mongo/executor/remote_command_request.h"
#include "mongo/executor/remote_command_response.h"
#include "mongo/executor/task_executor.h"
#include "mongo/executor/task_executor_pool.h"
#include "mongo/idl/idl_parser.h"
#include "mongo/logv2/log.h"
#include "mongo/logv2/log_attr.h"
#include "mongo/logv2/log_component.h"
#include "mongo/logv2/redaction.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/platform/compiler.h"
#include "mongo/rpc/get_status_from_command_result.h"
#include "mongo/s/balancer_configuration.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/grid.h"
#include "mongo/s/request_types/migration_secondary_throttle_options.h"
#include "mongo/stdx/unordered_set.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/elapsed_tracker.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/namespace_string_util.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/str.h"
#include "mongo/util/time_support.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kSharding

namespace mongo {
namespace {

using namespace fmt::literals;

const char kRecvChunkStatus[] = "_recvChunkStatus";
const char kRecvChunkCommit[] = "_recvChunkCommit";
const char kRecvChunkAbort[] = "_recvChunkAbort";

const int kMaxObjectPerChunk{250000};
const Hours kMaxWaitToCommitCloneForJumboChunk(6);

MONGO_FAIL_POINT_DEFINE(failTooMuchMemoryUsed);
MONGO_FAIL_POINT_DEFINE(hangAfterProcessingDeferredXferMods);

/**
 * Returns true if the given BSON object in the shard key value pair format is within the given
 * range.
 */
bool isShardKeyValueInRange(const BSONObj& shardKeyValue, const BSONObj& min, const BSONObj& max) {
    return shardKeyValue.woCompare(min) >= 0 && shardKeyValue.woCompare(max) < 0;
}

/**
 * Returns true if the given BSON document is within the given chunk range.
 */
bool isDocInRange(const BSONObj& obj,
                  const BSONObj& min,
                  const BSONObj& max,
                  const ShardKeyPattern& shardKeyPattern) {
    return isShardKeyValueInRange(shardKeyPattern.extractShardKeyFromDoc(obj), min, max);
}

/**
 * createRequestWithSessionId
 * 该函数用于构造分片迁移相关命令的 BSON 请求对象，包含命令名、命名空间、迁移会话ID和是否等待稳态参数。
 * 主要用于源分片与目标分片进行迁移状态查询等 RPC 通信时，确保请求中包含唯一标识本次迁移的关键信息。
 */
BSONObj createRequestWithSessionId(StringData commandName,
                                   const NamespaceString& nss,
                                   const MigrationSessionId& sessionId,
                                   bool waitForSteadyOrDone = false) {
    BSONObjBuilder builder;
    // 命令名字段，值为序列化后的命名空间字符串（如 "db.collection"）
    builder.append(commandName,
                   NamespaceStringUtil::serialize(nss, SerializationContext::stateDefault()));
    // 是否等待目标分片进入稳定或完成状态
    builder.append("waitForSteadyOrDone", waitForSteadyOrDone);
    // 追加迁移会话ID相关字段
    sessionId.append(&builder);
    // 返回最终构造的 BSON 请求对象
    return builder.obj();
}

BSONObj getDocumentKeyFromReplOperation(const repl::ReplOperation& replOperation) {
    switch (replOperation.getOpType()) {
        case repl::OpTypeEnum::kInsert:
        case repl::OpTypeEnum::kDelete:
            return replOperation.getObject();
        case repl::OpTypeEnum::kUpdate:
            return *replOperation.getObject2();
        default:
            MONGO_UNREACHABLE;
    }
    MONGO_UNREACHABLE;
}

char getOpCharForCrudOpType(repl::OpTypeEnum opType) {
    switch (opType) {
        case repl::OpTypeEnum::kInsert:
            return 'i';
        case repl::OpTypeEnum::kUpdate:
            return 'u';
        case repl::OpTypeEnum::kDelete:
            return 'd';
        default:
            MONGO_UNREACHABLE;
    }
    MONGO_UNREACHABLE;
}

std::vector<repl::ReplOperation> convertVector(const std::vector<repl::OplogEntry>& input) {
    std::vector<repl::ReplOperation> convertedOps;
    convertedOps.reserve(input.size());

    std::transform(
        input.cbegin(), input.cend(), std::back_inserter(convertedOps), [](const auto& op) {
            auto durableReplOp = op.getDurableReplOperation();
            if (!durableReplOp.isOwned()) {
                durableReplOp = repl::DurableReplOperation::parseOwned(
                    IDLParserContext{"MigrationChunkClonerSource_toOwnedDurableReplOperation"},
                    durableReplOp.toBSON());
            }

            return repl::ReplOperation(std::move(durableReplOp));
        });

    return convertedOps;
}

}  // namespace

LogTransactionOperationsForShardingHandler::LogTransactionOperationsForShardingHandler(
    LogicalSessionId lsid,
    const std::vector<repl::OplogEntry>& stmts,
    repl::OpTime prepareOrCommitOpTime)
    : LogTransactionOperationsForShardingHandler(
          std::move(lsid), convertVector(stmts), std::move(prepareOrCommitOpTime)) {}

LogTransactionOperationsForShardingHandler::LogTransactionOperationsForShardingHandler(
    LogicalSessionId lsid,
    const std::vector<repl::ReplOperation>& stmts,
    repl::OpTime prepareOrCommitOpTime)
    : _lsid(std::move(lsid)),
      _stmts(stmts),
      _prepareOrCommitOpTime(std::move(prepareOrCommitOpTime)) {}

void LogTransactionOperationsForShardingHandler::commit(OperationContext* opCtx,
                                                        boost::optional<Timestamp>) {
    std::set<NamespaceString> namespacesTouchedByTransaction;

    // Inform the session migration subsystem that a transaction has committed for the given
    // namespace.
    auto addToSessionMigrationOptimeQueueIfNeeded =
        [&namespacesTouchedByTransaction, lsid = _lsid](MigrationChunkClonerSource* const cloner,
                                                        const NamespaceString& nss,
                                                        const repl::OpTime opTime) {
            if (isInternalSessionForNonRetryableWrite(lsid)) {
                // Transactions inside internal sessions for non-retryable writes are not
                // retryable so there is no need to transfer the write history to the
                // recipient.
                return;
            }
            if (namespacesTouchedByTransaction.find(nss) == namespacesTouchedByTransaction.end()) {
                cloner->_addToSessionMigrationOptimeQueue(
                    opTime, SessionCatalogMigrationSource::EntryAtOpTimeType::kTransaction);

                namespacesTouchedByTransaction.emplace(nss);
            }
        };

    for (const auto& stmt : _stmts) {
        auto opType = stmt.getOpType();

        // Skip every noop entry except for a WouldChangeOwningShard (WCOS) sentinel noop entry
        // since for an internal transaction for a retryable WCOS findAndModify that is an upsert,
        // the applyOps oplog entry on the old owning shard would not have the insert entry; so if
        // we skip the noop entry here, the write history for the internal transaction would not get
        // transferred to the recipient since the _prepareOrCommitOpTime would not get added to the
        // session migration opTime queue below, and this would cause the write to execute again if
        // there is a retry after the migration.
        if (opType == repl::OpTypeEnum::kNoop &&
            !isWouldChangeOwningShardSentinelOplogEntry(stmt)) {
            continue;
        }

        const auto& nss = stmt.getNss();
        auto opCtx = cc().getOperationContext();

        // TODO (SERVER-71444): Fix to be interruptible or document exception.
        UninterruptibleLockGuard noInterrupt(opCtx);  // NOLINT.
        const auto scopedCss =
            CollectionShardingRuntime::assertCollectionLockedAndAcquireShared(opCtx, nss);

        auto clonerPtr = MigrationSourceManager::getCurrentCloner(*scopedCss);
        if (!clonerPtr) {
            continue;
        }
        auto* const cloner = dynamic_cast<MigrationChunkClonerSource*>(clonerPtr.get());

        if (isWouldChangeOwningShardSentinelOplogEntry(stmt)) {
            addToSessionMigrationOptimeQueueIfNeeded(cloner, nss, _prepareOrCommitOpTime);
            continue;
        }

        auto preImageDocKey = getDocumentKeyFromReplOperation(stmt);

        auto idElement = preImageDocKey["_id"];
        if (idElement.eoo()) {
            LOGV2_WARNING(21994,
                          "Received a document without an _id and will ignore that document",
                          "documentKey"_attr = redact(preImageDocKey));
            continue;
        }

        if (opType == repl::OpTypeEnum::kUpdate) {
            auto const& shardKeyPattern = cloner->_shardKeyPattern;
            auto preImageShardKeyValues =
                shardKeyPattern.extractShardKeyFromDocumentKey(preImageDocKey);

            // If prepare was performed from another term, we will not have the post image doc key
            // since it is not persisted in the oplog.
            auto postImageDocKey = stmt.getPostImageDocumentKey();
            if (!postImageDocKey.isEmpty()) {
                if (!cloner->_processUpdateForXferMod(preImageDocKey, postImageDocKey)) {
                    // We don't need to add this op to session migration if neither post or pre
                    // image doc falls within the chunk range.
                    continue;
                }
            } else {
                // We can't perform reads here using the same recovery unit because the transaction
                // is already committed. We instead defer performing the reads when xferMods command
                // is called. Also allow this op to be added to session migration since we can't
                // tell whether post image doc will fall within the chunk range. If it turns out
                // both preImage and postImage doc don't fall into the chunk range, it is not wrong
                // for this op to be added to session migration, but it will result in wasted work
                // and unneccesary extra oplog storage on the destination.
                cloner->_deferProcessingForXferMod(preImageDocKey);
            }
        } else {
            cloner->_addToTransferModsQueue(idElement.wrap(), getOpCharForCrudOpType(opType), {});
        }

        addToSessionMigrationOptimeQueueIfNeeded(cloner, nss, _prepareOrCommitOpTime);
    }
}

MigrationChunkClonerSource::MigrationChunkClonerSource(OperationContext* opCtx,
                                                       const ShardsvrMoveRange& request,
                                                       const WriteConcernOptions& writeConcern,
                                                       const BSONObj& shardKeyPattern,
                                                       ConnectionString donorConnStr,
                                                       HostAndPort recipientHost)
    : _args(request),
      _writeConcern(writeConcern),
      _shardKeyPattern(shardKeyPattern),
      _sessionId(MigrationSessionId::generate(_args.getFromShard().toString(),
                                              _args.getToShard().toString())),
      _donorConnStr(std::move(donorConnStr)),
      _recipientHost(std::move(recipientHost)),
      _forceJumbo(_args.getForceJumbo() != ForceJumbo::kDoNotForce) {
    auto const replCoord = repl::ReplicationCoordinator::get(opCtx);
    uassert(8393800,
            "cannot start migration, shard must run as replica sets",
            replCoord->getSettings().isReplSet());

    _sessionCatalogSource = std::make_unique<SessionCatalogMigrationSource>(
        opCtx, nss(), ChunkRange(getMin(), getMax()), _shardKeyPattern.getKeyPattern());
}

MigrationChunkClonerSource::~MigrationChunkClonerSource() {
    invariant(_state == kDone);
}

/**
 * MigrationChunkClonerSource::startClone() 函数的作用：
 * 启动 chunk 迁移的数据克隆阶段，初始化数据传输的基础设施和状态。
 * 
 * 核心功能：
 * 1. 会话数据初始化：设置会话目录源，准备会话相关数据的迁移
 * 2. 文档记录ID收集：扫描并存储要迁移的文档记录ID，为后续批量传输做准备
 * 3. 巨型块检测：检测超大 chunk 并启用特殊的直接扫描模式
 * 4. 接收端通信：向接收端发送 startChunkClone 命令，启动接收端的克隆准备
 * 5. 状态管理：将克隆器状态从 kNew 转换为 kCloning
 * 
 * 执行阶段：
 * - 预处理阶段：初始化会话目录，设置预备冲突行为
 * - 数据扫描阶段：收集要克隆的文档记录ID或检测巨型块
 * - 通信阶段：与接收端建立迁移会话
 * - 状态转换阶段：更新克隆器状态为活跃克隆状态
 * 
 * 特殊处理：
 * - 巨型块：对于超过大小限制的块，使用直接索引扫描模式
 * - 会话数据：初始化会话目录源以支持会话相关数据的迁移
 * - 预备冲突：忽略预备事务冲突，确保获取一致的数据快照
 * - 写关注点：根据节流配置设置适当的写关注点级别
 * 
 * 错误处理：
 * - ChunkTooBig：超大块在强制模式下转为巨型块处理
 * - 通信失败：接收端启动失败时返回相应错误
 * - 状态检查：确保克隆器处于正确的初始状态
 * 
 * 该函数是整个迁移过程的数据传输启动点，为后续的批量数据传输奠定基础。
 * 源分片调用 MigrationSourceManager::startClone-》MigrationChunkClonerSource::startClone
 */
Status MigrationChunkClonerSource::startClone(OperationContext* opCtx,
                                              const UUID& migrationId,
                                              const LogicalSessionId& lsid,
                                              TxnNumber txnNumber) {
    // 状态验证：确保克隆器处于初始状态，且操作上下文没有持有锁
    // 这是启动克隆的前提条件，避免在不一致状态下开始迁移
    invariant(_state == kNew);
    invariant(!shard_role_details::getLocker(opCtx)->isLocked());

    // 初始化会话目录源：
    // 功能：为迁移过程中的会话数据传输做准备
    // 参数：使用提供的逻辑会话ID来跟踪和迁移会话相关的操作日志
    // 目的：确保可重试写操作和事务的会话状态能够正确迁移
    _sessionCatalogSource->init(opCtx, lsid);

    // Prime up the session migration source if there are oplog entries to migrate.
    // 预加载会话迁移源：如果存在需要迁移的操作日志条目，则进行预处理
    // 功能：获取第一批会话相关的操作日志条目
    // 优化：提前准备数据，减少后续传输时的延迟
    _sessionCatalogSource->fetchNextOplog(opCtx);

    {
        // Ignore prepare conflicts when we load ids of currently available documents. This is
        // acceptable because we will track changes made by prepared transactions at transaction
        // commit time.
        // 忽略预备冲突：在加载当前可用文档ID时忽略预备事务冲突
        // 原理：预备事务的变更会在事务提交时被跟踪，因此可以安全忽略
        // 目的：获取一致的文档快照，避免被未提交的预备事务阻塞
        auto originalPrepareConflictBehavior =
            shard_role_details::getRecoveryUnit(opCtx)->getPrepareConflictBehavior();

        // 确保在函数退出时恢复原始的预备冲突行为
        // 使用 RAII 模式保证资源清理的可靠性
        ON_BLOCK_EXIT([&] {
            shard_role_details::getRecoveryUnit(opCtx)->setPrepareConflictBehavior(
                originalPrepareConflictBehavior);
        });

        // 设置忽略预备冲突的行为
        // 这允许我们在数据扫描期间不被预备事务阻塞
        shard_role_details::getRecoveryUnit(opCtx)->setPrepareConflictBehavior(
            PrepareConflictBehavior::kIgnoreConflicts);

        // 存储当前记录ID：
        // 功能：扫描要迁移的 chunk 范围，收集所有文档的记录ID
        // 用途：为批量数据传输提供文档定位信息
        // 优化：预先收集ID可以提高传输效率，避免重复扫描
        auto storeCurrentRecordIdStatus = _storeCurrentRecordId(opCtx);
        
        // 巨型块检测和处理：
        // 条件：如果块太大且启用了强制迁移，则切换到巨型块模式
        // 巨型块模式：使用直接索引扫描而不是预存储的记录ID
        if (storeCurrentRecordIdStatus == ErrorCodes::ChunkTooBig && _forceJumbo) {
            stdx::lock_guard<Latch> sl(_mutex);
            // 启用巨型块克隆状态：创建特殊的克隆状态对象
            // 这将改变后续的数据传输策略，使用流式扫描而非批量ID
            _jumboChunkCloneState.emplace();
        } else if (!storeCurrentRecordIdStatus.isOK()) {
            // 其他错误：如果不是 ChunkTooBig 错误，则直接返回失败
            // jumbo chunk如果不允许迁移，这里直接返回，也就是不做迁移
            if (!_forceJumbo) {
                return storeCurrentRecordIdStatus;
            }
        }
    }

    // Tell the recipient shard to start cloning
    // 通知接收端分片开始克隆：构建并发送 startChunkClone 命令
    BSONObjBuilder cmdBuilder;

    // 节流配置检查：
    // 功能：根据迁移参数决定是否启用二级节点写入节流
    // 目的：在网络带宽有限时控制迁移对集群性能的影响
    const bool isThrottled = _args.getSecondaryThrottle();
    
    // 二级节流选项配置：
    // 启用节流：使用指定的写关注点来控制写入速度
    // 关闭节流：使用默认的快速写入模式
    MigrationSecondaryThrottleOptions secondaryThrottleOptions = isThrottled
        ? MigrationSecondaryThrottleOptions::createWithWriteConcern(_writeConcern)
        : MigrationSecondaryThrottleOptions::create(MigrationSecondaryThrottleOptions::kOff);

    // 构建 _recvChunkStart 命令：, 目标集群通过 RecvChunkStartCommand::errmsgRun 接收该命令并处理
    // 功能：创建发送给接收端的完整命令对象
    // 参数：包含迁移所需的所有元数据和配置信息
    StartChunkCloneRequest::appendAsCommand(&cmdBuilder,
                                            nss(),                    // 集合命名空间
                                            migrationId,              // 迁移唯一标识
                                            lsid,                     // 逻辑会话ID
                                            txnNumber,                // 事务号
                                            _sessionId,               // 迁移会话ID
                                            _donorConnStr,            // 源分片连接字符串
                                            _args.getFromShard(),     // 源分片ID
                                            _args.getToShard(),       // 目标分片ID
                                            getMin(),                 // chunk 最小边界
                                            getMax(),                 // chunk 最大边界
                                            _shardKeyPattern.toBSON(), // 分片键模式
                                            secondaryThrottleOptions); // 节流选项

    // Commands sent to shards that accept writeConcern, must always have writeConcern. So if the
    // StartChunkCloneRequest didn't add writeConcern (from secondaryThrottle), then we add the
    // internal server default writeConcern.
    // 写关注点确保：发送给分片的命令必须包含写关注点
    // 逻辑：如果节流选项没有添加写关注点，则使用内部服务器默认写关注点
    // 目的：确保命令的一致性和可靠性
    if (!cmdBuilder.hasField(WriteConcernOptions::kWriteConcernField)) {
        cmdBuilder.append(WriteConcernOptions::kWriteConcernField,
                          WriteConcernOptions::kInternalWriteDefault);
    }

    // 发送命令到接收端：
    // 功能：通过网络向接收端分片发送 startChunkClone 命令
    // 超时：使用配置的网络超时时间
    // 重试：如果网络失败，调用方负责重试逻辑
    auto startChunkCloneResponseStatus = _callRecipient(opCtx, cmdBuilder.obj());
    if (!startChunkCloneResponseStatus.isOK()) {
        // 命令失败：返回网络或接收端错误
        return startChunkCloneResponseStatus.getStatus();
    }

    // TODO (Kal): Setting the state to kCloning below means that if cancelClone was called we will
    // send a cancellation command to the recipient. The reason to limit the cases when we send
    // cancellation is for backwards compatibility with 3.2 nodes, which cannot differentiate
    // between cancellations for different migration sessions. It is thus possible that a second
    // migration from different donor, but the same recipient would certainly abort an already
    // running migration.
    // 状态转换：将克隆器状态从 kNew 更新为 kCloning
    // 注意：此状态变更意味着如果调用 cancelClone，将向接收端发送取消命令
    // 兼容性：限制发送取消命令的情况是为了与 3.2 节点的向后兼容性
    // 风险：不同源分片的第二次迁移可能会中止正在运行的迁移
    stdx::lock_guard<Latch> sl(_mutex);
    _state = kCloning;

    return Status::OK();
}

/**
 * MigrationChunkClonerSource::awaitUntilCriticalSectionIsAppropriate 函数的作用：
 * 等待并判断何时适合进入迁移的关键区域（Critical Section），确保在阻塞写操作前数据同步达到最佳状态。
 * 
 * 核心功能：
 * 1. 迁移模式判断：区分手动强制巨型块迁移和常规迁移的处理策略
 * 2. 关键区域准入控制：调用状态检查机制判断是否可以安全进入关键区域
 * 3. 巨型块特殊处理：对手动强制的巨型块采用立即进入策略
 * 4. 状态验证：确保克隆器处于正确的克隆状态
 * 5. 锁状态检查：验证调用时没有持有任何锁，避免死锁风险
 * 
 * 关键区域意义：
 * - 写操作阻塞：进入关键区域后将阻塞对源分片的写操作
 * - 最终同步：在阻塞状态下完成剩余数据的最终传输
 * - 原子切换：确保chunk所有权的原子性转移
 * - 性能影响：最小化阻塞写操作的时间窗口
 * 
 * 决策策略：
 * - 手动强制巨型块：立即进入关键区域，整个克隆过程在关键区域完成
 * - 常规迁移：等待接收端克隆进度达到阈值后进入关键区域
 * - 智能判断：基于数据同步进度、内存使用、会话状态等多维度评估
 * 
 * 性能优化：
 * - 减少阻塞时间：通过预先同步最大化数据传输，最小化关键区域时间
 * - 平衡策略：在数据完整性和系统可用性之间找到最佳平衡点
 * - 自适应阈值：基于chunk大小和网络条件动态调整进入时机
 * 
 * 错误处理：
 * - 状态验证：确保迁移状态的一致性和正确性
 * - 超时保护：防止无限等待导致的资源占用
 * - 前置条件检查：验证调用环境的正确性
 * 
 * 该函数是迁移过程中的关键决策点，直接影响迁移的性能和可用性。
 */
Status MigrationChunkClonerSource::awaitUntilCriticalSectionIsAppropriate(
    OperationContext* opCtx, Milliseconds maxTimeToWait) {
    // 状态前置条件验证：
    // 条件1：确保当前处于克隆状态，这是进入关键区域的必要前提
    // 条件2：确保调用时没有持有任何锁，避免在等待过程中出现死锁
    // 重要性：这些检查确保函数在正确的上下文中被调用
    invariant(_state == kCloning);
    invariant(!shard_role_details::getLocker(opCtx)->isLocked());
    
    // If this migration is manual migration that specified "force", enter the critical section
    // immediately. This means the entire cloning phase will be done under the critical section.
    // 手动强制巨型块迁移的特殊处理：
    // 条件：存在巨型块克隆状态 且 迁移参数指定为手动强制模式
    // 策略：立即进入关键区域，不等待任何同步条件
    // 原因：手动强制模式表示管理员明确要求迁移超大块，接受性能影响
    // 后果：整个克隆阶段将在关键区域（写操作被阻塞）下完成
    if (_jumboChunkCloneState && _args.getForceJumbo() == ForceJumbo::kForceManual) {
        return Status::OK();  // 立即返回成功，允许进入关键区域
    }

    // 常规迁移的状态检查策略：
    // 功能：调用接收端状态检查机制，等待合适的进入时机
    // 参数1：操作上下文，提供中断机制和资源管理
    // 参数2：最大等待时间，防止无限等待（通常为6小时）
    // 
    // 内部逻辑（由 _checkRecipientCloningStatus 实现）：
    // - 轮询接收端的克隆状态和进度
    // - 评估未传输数据量是否在可接受范围内
    // - 检查会话迁移是否完成或接近完成
    // - 监控源端内存使用情况
    // - 基于多维度指标判断最佳进入时机
    //
    // 成功条件：
    // - 接收端状态为 "steady" 或在阈值内的 "catchup"
    // - 未传输修改操作在可管理范围内
    // - 会话数据迁移基本完成
    // - 源端内存使用未超过限制
    return _checkRecipientCloningStatus(opCtx, maxTimeToWait);
}


StatusWith<BSONObj> MigrationChunkClonerSource::commitClone(OperationContext* opCtx) {
    invariant(_state == kCloning);
    invariant(!shard_role_details::getLocker(opCtx)->isLocked());
    if (_jumboChunkCloneState && _forceJumbo) {
        if (_args.getForceJumbo() == ForceJumbo::kForceManual) {
            auto status = _checkRecipientCloningStatus(opCtx, kMaxWaitToCommitCloneForJumboChunk);
            if (!status.isOK()) {
                return status;
            }
        } else {
            invariant(PlanExecutor::IS_EOF == _jumboChunkCloneState->clonerState);
            invariant(!_cloneList.hasMore());
        }
    }

    _sessionCatalogSource->onCommitCloneStarted();

    auto responseStatus = _callRecipient(opCtx, [&] {
        BSONObjBuilder builder;
        builder.append(kRecvChunkCommit,
                       NamespaceStringUtil::serialize(nss(), SerializationContext::stateDefault()));
        _sessionId.append(&builder);
        return builder.obj();
    }());

    if (responseStatus.isOK()) {
        _cleanup(true);

        if (_sessionCatalogSource->hasMoreOplog()) {
            return {ErrorCodes::SessionTransferIncomplete,
                    "destination shard finished committing but there are still some session "
                    "metadata that needs to be transferred"};
        }

        return responseStatus;
    }

    cancelClone(opCtx);
    return responseStatus.getStatus();
}

void MigrationChunkClonerSource::cancelClone(OperationContext* opCtx) noexcept {
    invariant(!shard_role_details::getLocker(opCtx)->isLocked());

    _sessionCatalogSource->onCloneCleanup();

    switch (_state) {
        case kDone:
            break;
        case kCloning: {
            const auto status =
                _callRecipient(opCtx,
                               createRequestWithSessionId(kRecvChunkAbort, nss(), _sessionId))
                    .getStatus();
            if (!status.isOK()) {
                LOGV2(21991, "Failed to cancel migration", "error"_attr = redact(status));
            }
            [[fallthrough]];
        }
        case kNew:
            _cleanup(false);
            break;
        default:
            MONGO_UNREACHABLE;
    }
}

void MigrationChunkClonerSource::onInsertOp(OperationContext* opCtx,
                                            const BSONObj& insertedDoc,
                                            const repl::OpTime& opTime) {
    dassert(shard_role_details::getLocker(opCtx)->isCollectionLockedForMode(nss(), MODE_IX));

    BSONElement idElement = insertedDoc["_id"];
    if (idElement.eoo()) {
        LOGV2_WARNING(21995,
                      "logInsertOp received a document without an _id field and will ignore that "
                      "document",
                      "insertedDoc"_attr = redact(insertedDoc));
        return;
    }

    if (!isDocInRange(insertedDoc, getMin(), getMax(), _shardKeyPattern)) {
        return;
    }

    if (!_addedOperationToOutstandingOperationTrackRequests()) {
        return;
    }

    _addToTransferModsQueue(idElement.wrap(), 'i', opCtx->getTxnNumber() ? opTime : repl::OpTime());
    _decrementOutstandingOperationTrackRequests();
}

void MigrationChunkClonerSource::onUpdateOp(OperationContext* opCtx,
                                            boost::optional<BSONObj> preImageDoc,
                                            const BSONObj& postImageDoc,
                                            const repl::OpTime& opTime) {
    dassert(shard_role_details::getLocker(opCtx)->isCollectionLockedForMode(nss(), MODE_IX));

    BSONElement idElement = postImageDoc["_id"];
    if (idElement.eoo()) {
        LOGV2_WARNING(
            21996,
            "logUpdateOp received a document without an _id field and will ignore that document",
            "postImageDoc"_attr = redact(postImageDoc));
        return;
    }

    if (!isDocInRange(postImageDoc, getMin(), getMax(), _shardKeyPattern)) {
        // If the preImageDoc is not in range but the postImageDoc was, we know that the document
        // has changed shard keys and no longer belongs in the chunk being cloned. We will model
        // the deletion of the preImage document so that the destination chunk does not receive an
        // outdated version of this document.
        if (preImageDoc && isDocInRange(*preImageDoc, getMin(), getMax(), _shardKeyPattern)) {
            onDeleteOp(opCtx, getDocumentKey(_shardKeyPattern, *preImageDoc), opTime);
        }
        return;
    }

    if (!_addedOperationToOutstandingOperationTrackRequests()) {
        return;
    }

    _addToTransferModsQueue(idElement.wrap(), 'u', opCtx->getTxnNumber() ? opTime : repl::OpTime());
    _decrementOutstandingOperationTrackRequests();
}

void MigrationChunkClonerSource::onDeleteOp(OperationContext* opCtx,
                                            const DocumentKey& documentKey,
                                            const repl::OpTime& opTime) {
    dassert(shard_role_details::getLocker(opCtx)->isCollectionLockedForMode(nss(), MODE_IX));

    const auto shardKeyAndId = documentKey.getShardKeyAndId();

    BSONElement idElement = documentKey.getId()["_id"];
    if (idElement.eoo()) {
        LOGV2_WARNING(
            21997,
            "logDeleteOp received a document without an _id field and will ignore that document",
            "deletedDocShardKeyAndId"_attr = redact(shardKeyAndId));
        return;
    }

    if (!documentKey.getShardKey()) {
        LOGV2_WARNING(8023600,
                      "logDeleteOp received a document without the shard key field and will ignore "
                      "that document",
                      "deletedDocShardKeyAndId"_attr = redact(shardKeyAndId));
        return;
    }

    const auto shardKeyValue =
        _shardKeyPattern.extractShardKeyFromDocumentKey(*documentKey.getShardKey());
    if (!isShardKeyValueInRange(shardKeyValue, getMin(), getMax())) {
        return;
    }

    if (!_addedOperationToOutstandingOperationTrackRequests()) {
        return;
    }

    _addToTransferModsQueue(
        documentKey.getId(), 'd', opCtx->getTxnNumber() ? opTime : repl::OpTime());
    _decrementOutstandingOperationTrackRequests();
}

void MigrationChunkClonerSource::_addToSessionMigrationOptimeQueue(
    const repl::OpTime& opTime,
    SessionCatalogMigrationSource::EntryAtOpTimeType entryAtOpTimeType) {
    if (!opTime.isNull()) {
        _sessionCatalogSource->notifyNewWriteOpTime(opTime, entryAtOpTimeType);
    }
}

void MigrationChunkClonerSource::_addToTransferModsQueue(const BSONObj& idObj,
                                                         const char op,
                                                         const repl::OpTime& opTime) {
    switch (op) {
        case 'd': {
            stdx::lock_guard<Latch> sl(_mutex);
            _deleted.push_back(idObj);
            ++_untransferredDeletesCounter;
            _memoryUsed += idObj.firstElement().size() + 5;
        } break;

        case 'i':
        case 'u': {
            stdx::lock_guard<Latch> sl(_mutex);
            _reload.push_back(idObj);
            ++_untransferredUpsertsCounter;
            _memoryUsed += idObj.firstElement().size() + 5;
        } break;

        default:
            MONGO_UNREACHABLE;
    }

    _addToSessionMigrationOptimeQueue(
        opTime, SessionCatalogMigrationSource::EntryAtOpTimeType::kRetryableWrite);
}

bool MigrationChunkClonerSource::_addedOperationToOutstandingOperationTrackRequests() {
    stdx::unique_lock<Latch> lk(_mutex);
    if (!_acceptingNewOperationTrackRequests) {
        return false;
    }

    _incrementOutstandingOperationTrackRequests(lk);
    return true;
}

void MigrationChunkClonerSource::_drainAllOutstandingOperationTrackRequests(
    stdx::unique_lock<Latch>& lk) {
    invariant(_state == kDone);
    _acceptingNewOperationTrackRequests = false;
    _allOutstandingOperationTrackRequestsDrained.wait(
        lk, [&] { return _outstandingOperationTrackRequests == 0; });
}


void MigrationChunkClonerSource::_incrementOutstandingOperationTrackRequests(WithLock) {
    invariant(_acceptingNewOperationTrackRequests);
    ++_outstandingOperationTrackRequests;
}

void MigrationChunkClonerSource::_decrementOutstandingOperationTrackRequests() {
    stdx::lock_guard<Latch> sl(_mutex);
    --_outstandingOperationTrackRequests;
    if (_outstandingOperationTrackRequests == 0) {
        _allOutstandingOperationTrackRequestsDrained.notify_all();
    }
}

/**
 * MigrationChunkClonerSource::_nextCloneBatchFromIndexScan 函数的作用：
 * 专门用于巨型块(jumbo chunk)迁移的直接索引扫描数据获取函数。
 * 
 * 核心功能：
 * 1. 巨型块专用处理：为超过常规大小限制的chunk提供流式数据传输能力
 * 2. 直接索引扫描：使用分片键索引直接扫描而非预存储的记录ID
 * 3. 流式数据传输：避免将所有记录ID加载到内存，支持超大数据量的迁移
 * 4. 执行器状态管理：管理索引扫描执行器的生命周期和状态恢复
 * 5. 批次大小控制：动态控制单次传输批次以避免BSON大小限制
 * 6. 性能监控统计：记录克隆进度和传输量统计信息
 * 
 * 适用场景：
 * - 强制迁移的巨型块：_forceJumbo=true 且 _jumboChunkCloneState 存在
 * - 超大数据集：无法将所有记录ID预加载到内存的chunk
 * - 内存受限环境：需要控制内存使用量的迁移场景
 * - 手动强制迁移：管理员明确指定强制迁移超大块的情况
 * 
 * 工作原理：
 * - 创建或恢复索引扫描执行器进行范围扫描
 * - 逐个获取chunk范围内的文档并验证有效性
 * - 控制批次大小避免超出BSON最大用户大小限制
 * - 支持执行器状态的保存和恢复以支持中断恢复
 * - 使用让步机制定期释放CPU资源
 * 
 * 性能特点：
 * - 内存使用可控：不需要预存储大量记录ID
 * - 流式处理：边扫描边传输，减少内存峰值
 * - 支持让步：定期让步CPU避免长时间阻塞
 * - 状态持久化：支持中断后恢复扫描位置
 * 
 * 与常规迁移对比：
 * - 常规迁移：预存储记录ID + 随机访问（内存效率高，适合中小型chunk）
 * - 巨型块迁移：直接索引扫描 + 流式传输（CPU效率高，适合超大chunk）
 * 
 * 错误处理：
 * - 执行器异常：捕获并重新抛出带上下文的异常
 * - 中断支持：响应操作上下文的中断请求
 * - 状态管理：确保执行器状态的正确保存和恢复
 * 
 * 该函数是巨型块迁移的核心实现，使得MongoDB能够迁移任意大小的chunk。
 */
void MigrationChunkClonerSource::_nextCloneBatchFromIndexScan(OperationContext* opCtx,
                                                              const CollectionPtr& collection,
                                                              BSONArrayBuilder* arrBuilder) {
    // 让步跟踪器初始化：
    // 功能：创建基于时间和迭代次数的让步控制器
    // 参数1：快速时钟源，提供高精度时间测量
    // 参数2：让步迭代间隔，从全局查询配置参数加载
    // 参数3：让步时间周期，从全局查询配置参数加载（毫秒）
    // 目的：定期让步CPU资源，避免长时间阻塞其他操作，确保系统响应性
    ElapsedTracker tracker(opCtx->getServiceContext()->getFastClockSource(),
                           internalQueryExecYieldIterations.load(),
                           Milliseconds(internalQueryExecYieldPeriodMS.load()));

    // 执行器创建或恢复分支：
    // 条件：如果巨型块克隆状态中没有执行器，则创建新的索引扫描执行器
    // 原因：首次调用时需要创建执行器，后续调用时重用现有执行器
    if (!_jumboChunkCloneState->clonerExec) {
        // 创建索引扫描执行器：
        // 功能：基于分片键索引创建范围扫描的计划执行器
        // 参数1：操作上下文，提供事务和中断支持
        // 参数2：集合对象，用于访问集合数据和索引
        // 参数3：索引扫描选项，IXSCAN_FETCH表示扫描并获取完整文档
        // 返回：包含计划执行器的StatusWith对象
        // 用途：执行器负责按分片键顺序扫描chunk范围内的所有文档
        auto exec = uassertStatusOK(_getIndexScanExecutor(
            opCtx, collection, InternalPlanner::IndexScanOptions::IXSCAN_FETCH));
        
        // 存储执行器：将创建的执行器保存到巨型块克隆状态中
        // 目的：后续批次调用时可以重用同一个执行器，保持扫描连续性
        _jumboChunkCloneState->clonerExec = std::move(exec);
    } else {
        // 执行器恢复分支：如果执行器已存在，则恢复其运行状态
        // 原因：执行器在前一个批次结束时被分离，需要重新附加到操作上下文
        
        // 重新附加到操作上下文：
        // 功能：将执行器与当前操作上下文关联
        // 目的：执行器需要操作上下文来访问存储引擎和处理中断
        _jumboChunkCloneState->clonerExec->reattachToOperationContext(opCtx);
        
        // 恢复执行器状态：
        // 功能：恢复执行器的内部状态，包括索引扫描位置
        // 参数：集合对象的引用，用于重新建立与集合的连接
        // 重要性：确保扫描从上次中断的位置继续，避免数据重复或遗漏
        _jumboChunkCloneState->clonerExec->restoreState(&collection);
    }

    // 执行状态初始化：定义执行器的当前执行状态
    // 用途：跟踪执行器的返回状态（ADVANCED/IS_EOF/ERROR等）
    PlanExecutor::ExecState execState;
    
    try {
        // 文档变量声明：用于存储执行器返回的文档数据
        BSONObj obj;
        RecordId recordId;  // 记录ID（在IXSCAN_FETCH模式下不使用）
        
        // 主扫描循环：持续从执行器获取文档直到满足退出条件
        // 执行器模式：IXSCAN_FETCH 模式下会自动获取完整文档内容
        while (PlanExecutor::ADVANCED ==
               (execState = _jumboChunkCloneState->clonerExec->getNext(&obj, nullptr))) {

            // 执行状态更新：在互斥锁保护下更新巨型块克隆状态
            // 目的：记录当前执行器状态，用于监控和状态查询
            stdx::unique_lock<Latch> lk(_mutex);
            _jumboChunkCloneState->clonerState = execState;
            lk.unlock();

            // 中断检查：检查操作上下文是否收到中断请求
            // 重要性：长时间运行的扫描操作需要支持中断以保持系统响应性
            // 异常：如果收到中断请求，会抛出异常终止扫描过程
            opCtx->checkForInterrupt();

            // Use the builder size instead of accumulating the document sizes directly so
            // that we take into consideration the overhead of BSONArray indices.
            // BSON大小限制检查：
            // 策略：使用构建器大小而不是直接累积文档大小，以考虑BSONArray索引的开销
            // 条件1：arrBuilder->arrSize() - 数组中已有文档（确保有进展）
            // 条件2：大小检查 - 当前长度 + 文档大小 + 1024字节缓冲 > BSON最大用户大小
            // 操作：如果超出大小限制，将文档推回执行器并退出循环
            if (arrBuilder->arrSize() &&
                (arrBuilder->len() + obj.objsize() + 1024) > BSONObjMaxUserSize) {
                // 文档推回执行器：
                // 功能：将当前文档推回执行器的结果队列
                // 目的：确保下次批次调用时可以继续处理此文档，避免数据丢失
                // 原理：执行器内部维护一个推回缓冲区用于此目的
                _jumboChunkCloneState->clonerExec->stashResult(obj);
                break;  // 退出循环，结束当前批次
            }

            // 文档添加到传输批次：
            // 操作：将文档添加到BSON数组构建器中
            // 结果：文档将包含在返回给接收端的批次中
            arrBuilder->append(obj);

            // 克隆统计更新：在互斥锁保护下更新已克隆文档计数
            // 目的：跟踪巨型块的克隆进度，用于监控和日志记录
            lk.lock();
            _jumboChunkCloneState->docsCloned++;
            lk.unlock();

            // 全局统计更新：
            // 功能：更新分片系统的全局克隆统计信息
            // 统计1：源端克隆文档计数增加1
            // 统计2：源端克隆字节数增加文档大小
            // 用途：集群级别的迁移监控和性能分析
            ShardingStatistics::get(opCtx).countDocsClonedOnDonor.addAndFetch(1);
            ShardingStatistics::get(opCtx).countBytesClonedOnDonor.addAndFetch(obj.objsize());
        }
        
    } catch (DBException& exception) {
        exception.addContext("Executor error while scanning for documents belonging to chunk");
        throw;
   }

    stdx::unique_lock<Latch> lk(_mutex);
    _jumboChunkCloneState->clonerState = execState;
    lk.unlock();

    _jumboChunkCloneState->clonerExec->saveState();
    _jumboChunkCloneState->clonerExec->detachFromOperationContext();
}
/**
 * MigrationChunkClonerSource::_nextCloneBatchFromCloneRecordIds 函数的作用：
 * 基于预存储的记录ID获取下一批克隆文档，是常规大小chunk的高效数据传输核心实现。
 * 
 * 核心功能：
 * 1. 基于记录ID的随机访问：使用预先收集的记录ID进行高效的文档访问
 * 2. 批次大小控制：动态控制单次传输的数据量，避免超出BSON大小限制
 * 3. 范围边界验证：确保传输的文档仍在目标chunk的分片键范围内
 * 4. 让步机制管理：定期让步CPU资源，维持系统响应性
 * 5. 文档溢出处理：支持文档溢出到下次批次，优化传输效率
 * 
 * 传输策略：
 * - 预取优化：基于预扫描的记录ID，避免重复索引扫描
 * - 随机访问：直接通过记录ID访问文档，效率高于索引扫描
 * - 批次优化：考虑BSON数组索引开销，精确控制传输大小
 * - 范围过滤：运行时验证文档是否仍在迁移范围内
 * 
 * 性能优化：
 * - 让步策略：基于时间和迭代次数的双重让步控制
 * - 内存管理：通过溢出机制避免单次传输过大
 * - 统计更新：实时更新克隆进度和传输量统计
 * - 异常记录：跟踪已删除或移出范围的文档数量
 * 
 * 数据一致性：
 * - 时点一致性：基于记录ID确保访问时点的一致性
 * - 范围检查：验证文档分片键是否仍在目标范围内
 * - 存在性验证：处理在记录ID收集后被删除的文档
 * - 变更追踪：通过OpObserver机制追踪并发写操作
 * 
 * 错误处理：
 * - 文档缺失：处理记录ID对应文档已被删除的情况
 * - 范围变更：处理文档分片键值变更导致的范围移出
 * - 中断处理：响应操作上下文的中断请求
 * - 统计维护：准确记录各种异常情况的统计信息
 * 
 * 该函数专门用于常规大小的chunk，通过预存储记录ID实现高效的批量数据传输。
 * 源分片: 记录需要迁移chunk的RecordId， MigrationChunkClonerSource::startClone->MigrationChunkClonerSource::_storeCurrentRecordId
 * 源分片收到目标分片发来的_migrateClone请求: 根据上面记录的RecordId获取对应数据返回给目标分片 MigrationChunkClonerSource::startClone->MigrationChunkClonerSource::_nextCloneBatchFromCloneRecordIds
 */
void MigrationChunkClonerSource::_nextCloneBatchFromCloneRecordIds(OperationContext* opCtx,
                                                                   const CollectionPtr& collection,
                                                                   BSONArrayBuilder* arrBuilder) {
    // 让步跟踪器初始化：
    // 功能：创建基于时间和迭代次数的让步控制器
    // 参数1：快速时钟源，提供高精度时间测量
    // 参数2：让步迭代间隔，从全局配置参数加载
    // 参数3：让步时间周期，从全局配置参数加载（毫秒）
    // 目的：定期让步CPU资源，避免长时间阻塞其他操作
    ElapsedTracker tracker(opCtx->getServiceContext()->getFastClockSource(),
                           internalQueryExecYieldIterations.load(),
                           Milliseconds(internalQueryExecYieldPeriodMS.load()));

    // 主文档获取循环：持续从记录ID列表中获取文档直到满足退出条件
    while (true) {
        // 不存在记录计数器：跟踪记录ID对应的文档已不存在的数量
        // 用途：统计在记录ID收集后被删除的文档数量
        int recordsNoLongerExist = 0;
        
        // 获取下一个待处理文档：
        // 功能：从克隆列表中获取下一个记录ID对应的文档
        // 参数1：操作上下文，提供事务和中断支持
        // 参数2：集合对象，用于文档访问
        // 参数3：不存在记录计数器的指针，用于统计已删除文档
        // 返回：包装的文档对象，可能为空表示没有更多文档
        auto docInFlight = _cloneList.getNextDoc(opCtx, collection, &recordsNoLongerExist);

        // 不存在记录统计更新：
        // 条件：如果有记录ID对应的文档已不存在
        // 操作：在互斥锁保护下更新全局统计计数器
        // 原因：文档可能在记录ID收集后被删除或移动
        if (recordsNoLongerExist) {
            stdx::lock_guard lk(_mutex);
            _numRecordsPassedOver += recordsNoLongerExist;
        }

        // 获取文档内容：从包装对象中提取实际的文档数据
        const auto& doc = docInFlight->getDoc();
        if (!doc) {
            // 文档为空表示没有更多数据：
            // 情况：所有记录ID都已处理完毕或没有有效文档
            // 操作：退出循环，结束当前批次的数据收集
            break;
        }

        // We must always make progress in this method by at least one document because empty
        // return indicates there is no more initial clone data.
        // 让步条件检查：
        // 前提：必须在此方法中至少处理一个文档，因为空返回表示没有更多初始克隆数据
        // 条件1：arrBuilder->arrSize() - 数组中已有文档（确保有进展）
        // 条件2：tracker.intervalHasElapsed() - 让步时间间隔已过
        // 操作：如果满足条件，将当前文档插入溢出队列并退出循环
        // 当某个fetcher线程发现当前文档无法放入16MB的响应中时：将文档放回溢出队列，供下次请求使用
        if (arrBuilder->arrSize() && tracker.intervalHasElapsed()) {
            _cloneList.insertOverflowDoc(*doc);
            break;
        }

        // Do not send documents that are no longer in the chunk range being moved. This can
        // happen when document shard key value of the document changed after the initial
        // index scan during cloning. This is needed because the destination is very
        // conservative in processing xferMod deletes and won't delete docs that are not in
        // the range of the chunk being migrated.
        // 文档范围验证：
        // 目的：不发送不再位于正在移动的chunk范围内的文档
        // 原因：文档的分片键值可能在克隆期间的初始索引扫描之后发生了变化
        // 必要性：目标分片在处理xferMod删除时非常保守，不会删除不在迁移chunk范围内的文档
        // 检查：验证文档的分片键值是否仍在指定的最小值和最大值范围内
        if (!isDocInRange(
                doc->value(), _args.getMin().value(), _args.getMax().value(), _shardKeyPattern)) {
            {
                // 范围外文档统计更新：
                // 操作：在互斥锁保护下增加超出范围的文档计数
                // 原因：文档分片键值变更导致不再属于当前chunk
                stdx::lock_guard lk(_mutex);
                _numRecordsPassedOver++;
            }
            continue;  // 跳过此文档，继续处理下一个
        }

        // Use the builder size instead of accumulating the document sizes directly so
        // that we take into consideration the overhead of BSONArray indices.
        // BSON大小限制检查：
        // 策略：使用构建器大小而不是直接累积文档大小，以考虑BSONArray索引的开销
        // 条件1：arrBuilder->arrSize() - 数组中已有文档
        // 条件2：大小检查 - 当前长度 + 文档大小 + 1024字节缓冲 > BSON最大用户大小
        // 操作：如果超出大小限制，将文档插入溢出队列并退出循环
        if (arrBuilder->arrSize() &&
            (arrBuilder->len() + doc->value().objsize() + 1024) > BSONObjMaxUserSize) {
            _cloneList.insertOverflowDoc(*doc);
            break;
        }

        {
            // 成功克隆统计更新：
            // 操作：在互斥锁保护下增加成功克隆的记录计数
            // 时机：文档通过所有验证并准备添加到传输批次时
            stdx::lock_guard lk(_mutex);
            _numRecordsCloned++;
        }

        // 文档添加到传输批次：
        // 操作：将文档添加到BSON数组构建器中
        // 结果：文档将包含在返回给接收端的批次中
        arrBuilder->append(doc->value());
        
        // 全局统计更新：
        // 功能：更新分片系统的全局克隆统计信息
        // 统计1：源端克隆文档计数增加1
        // 统计2：源端克隆字节数增加文档大小
        // 用途：监控和性能分析
        ShardingStatistics::get(opCtx).countDocsClonedOnDonor.addAndFetch(1);
        ShardingStatistics::get(opCtx).countBytesClonedOnDonor.addAndFetch(doc->value().objsize());
    }
}

uint64_t MigrationChunkClonerSource::getCloneBatchBufferAllocationSize() {
    stdx::lock_guard<Latch> sl(_mutex);
    if (_jumboChunkCloneState && _forceJumbo)
        return static_cast<uint64_t>(BSONObjMaxUserSize);

    return std::min(static_cast<uint64_t>(BSONObjMaxUserSize),
                    _averageObjectSizeForCloneRecordIds * _cloneList.size());
}

/**
 * MigrationChunkClonerSource::nextCloneBatch 函数的作用：
 * 获取下一批要迁移的文档数据，是 chunk 迁移过程中数据传输的核心函数。
 * 
 * 核心功能：
 * 1. 批量数据获取：从源分片获取一批文档数据用于传输到目标分片
 * 2. 传输模式选择：根据 chunk 大小选择不同的数据获取策略
 * 3. 内存优化管理：控制单次传输的数据量，避免内存溢出
 * 4. 并发安全保证：在共享锁保护下安全地读取集合数据
 * 5. 统计信息更新：记录克隆进度和传输量统计
 * 
 * 传输策略：
 * - 常规块模式：使用预存储的记录ID进行高效的随机访问
 * - 巨型块模式：使用直接索引扫描进行流式数据传输
 * - 自适应选择：根据 _jumboChunkCloneState 和 _forceJumbo 标志自动选择
 * 
 * 性能优化：
 * - 让步机制：定期让步CPU避免长时间阻塞其他操作
 * - 批次大小控制：动态调整传输批次大小以优化网络效率
 * - 范围验证：确保只传输在目标chunk范围内的文档
 * - 溢出文档处理：支持文档溢出到下次批次传输
 * 
 * 数据一致性：
 * - 读取时点一致性：在一致的读取时点获取文档数据
 * - 范围边界检查：验证文档仍在迁移chunk的范围内
 * - 记录存在性验证：处理在扫描后被删除的文档
 * 
 * 错误处理：
 * - 异常捕获：捕获并转换数据库异常为状态码
 * - 集合验证：确保目标集合仍然存在且可访问
 * - 中断处理：响应操作上下文的中断请求
 * 
 * 该函数是 InitialCloneCommand 的核心依赖，负责实际的数据获取和传输准备。
 * // InitialCloneCommand::run调用，获取本次需要迁移的 doc 添加到 arrBuilder 中。
 * 
*  目标分片：MigrationBatchFetcher<Inserter>::_fetchBatch 发送 _migrateClone 请求给源分片
*  源分片：InitialCloneCommand::run 接收 _migrateClone 并执行数据获取，
 */

Status MigrationChunkClonerSource::nextCloneBatch(OperationContext* opCtx,
                                                  const CollectionPtr& collection,
                                                  BSONArrayBuilder* arrBuilder) {
    // 集合锁验证：确保调用者持有集合的意向共享锁
    // 目的：保证在数据读取期间集合不会被删除或重命名
    // 锁模式：MODE_IS 允许并发读取但阻止结构性变更
    dassert(shard_role_details::getLocker(opCtx)->isCollectionLockedForMode(nss(), MODE_IS));

    // If this chunk is too large to store records in _cloneRecordIds and the command args specify
    // to attempt to move it, scan the collection directly.
    // 巨型块处理分支：
    // 条件：chunk 过大无法在 _cloneRecordIds 中存储记录且命令参数指定尝试移动
    // 策略：直接扫描集合而不是使用预存储的记录ID
    // 优势：避免内存溢出，支持超大chunk的迁移
    if (_jumboChunkCloneState && _forceJumbo) {
        try {
            // 调用巨型块专用的索引扫描方法：
            // 功能：使用索引扫描逐步获取chunk范围内的文档
            // 特点：流式处理，内存使用可控，支持让步机制
            // 状态管理：维护扫描进度，支持中断和恢复
            _nextCloneBatchFromIndexScan(opCtx, collection, arrBuilder);
            return Status::OK();
        } catch (const DBException& ex) {
            // 异常处理：捕获数据库异常并转换为状态对象
            // 常见异常：索引不存在、集合被删除、权限不足等
            return ex.toStatus();
        }
    }

    // 常规块处理分支：
    // 策略：使用预存储在 _cloneRecordIds 中的记录ID进行随机访问
    // 优势：访问效率高，批次控制精确，内存使用预期
    // 适用：中小型chunk，记录ID可以完全加载到内存中
    _nextCloneBatchFromCloneRecordIds(opCtx, collection, arrBuilder);
    return Status::OK();
}

bool MigrationChunkClonerSource::_processUpdateForXferMod(const BSONObj& preImageDocKey,
                                                          const BSONObj& postImageDocKey) {
    auto const& minKey = _args.getMin().value();
    auto const& maxKey = _args.getMax().value();

    auto postShardKeyValues = _shardKeyPattern.extractShardKeyFromDocumentKey(postImageDocKey);
    fassert(6836100, !postShardKeyValues.isEmpty());

    auto opType = repl::OpTypeEnum::kUpdate;
    auto idElement = preImageDocKey["_id"];

    if (!isShardKeyValueInRange(postShardKeyValues, minKey, maxKey)) {
        // If the preImageDoc is not in range but the postImageDoc was, we know that the
        // document has changed shard keys and no longer belongs in the chunk being cloned.
        // We will model the deletion of the preImage document so that the destination chunk
        // does not receive an outdated version of this document.

        auto preImageShardKeyValues =
            _shardKeyPattern.extractShardKeyFromDocumentKey(preImageDocKey);
        fassert(6836101, !preImageShardKeyValues.isEmpty());

        if (!isShardKeyValueInRange(preImageShardKeyValues, minKey, maxKey)) {
            return false;
        }

        opType = repl::OpTypeEnum::kDelete;
        idElement = postImageDocKey["_id"];
    }

    _addToTransferModsQueue(idElement.wrap(), getOpCharForCrudOpType(opType), {});

    return true;
}

void MigrationChunkClonerSource::_deferProcessingForXferMod(const BSONObj& preImageDocKey) {
    stdx::lock_guard<Latch> sl(_mutex);
    _deferredReloadOrDeletePreImageDocKeys.push_back(preImageDocKey.getOwned());
    _deferredUntransferredOpsCounter++;
}

void MigrationChunkClonerSource::_processDeferredXferMods(OperationContext* opCtx) {
    std::vector<BSONObj> deferredReloadOrDeletePreImageDocKeys;

    {
        stdx::unique_lock lk(_mutex);
        deferredReloadOrDeletePreImageDocKeys.swap(_deferredReloadOrDeletePreImageDocKeys);
    }

    for (const auto& preImageDocKey : deferredReloadOrDeletePreImageDocKeys) {
        auto idElement = preImageDocKey["_id"];
        BSONObj newerVersionDoc;
        if (!Helpers::findById(opCtx, this->nss(), BSON("_id" << idElement), newerVersionDoc)) {
            // If the document can no longer be found, this means that another later op must have
            // deleted it. That delete would have been captured by the xferMods so nothing else to
            // do here.
            continue;
        }

        auto postImageDocKey =
            CollectionMetadata::extractDocumentKey(&_shardKeyPattern, newerVersionDoc);
        static_cast<void>(_processUpdateForXferMod(preImageDocKey, postImageDocKey));
    }

    hangAfterProcessingDeferredXferMods.execute([&](const auto& data) {
        if (!deferredReloadOrDeletePreImageDocKeys.empty()) {
            hangAfterProcessingDeferredXferMods.pauseWhileSet();
        }
    });
}

/**
 * MigrationChunkClonerSource::nextModsBatch 函数的作用：
 * 获取下一批增量修改操作数据，是chunk迁移过程中传输增量变更的核心函数。
 * 
 * 核心功能：
 * 1. 增量数据批次传输：收集并返回迁移过程中发生的插入、更新、删除操作
 * 2. 延迟修改处理：处理在事务提交时无法确定后置镜像的延迟更新操作
 * 3. 因果顺序保证：确保删除和更新操作按照因果顺序被消费和传输
 * 4. 批次大小控制：动态控制单次传输的数据量，避免超出BSON大小限制
 * 5. 快照一致性管理：放弃陈旧快照，确保读取的数据至少与最新更新一样新
 * 
 * 数据类型处理：
 * - deleted数组：包含被删除文档的ID信息
 * - reload数组：包含需要重新加载的完整文档（插入和更新操作）
 * - size字段：传输数据的总字节大小
 * 
 * 处理策略：
 * - 删除优先：总是先消费删除缓冲区，再消费更新缓冲区
 * - 原子快照：在单个锁下获取删除和更新列表的"快照"
 * - 延迟处理：优先处理延迟的xfer修改操作
 * - 剩余恢复：将未消费的ID重新放回缓冲区
 * 
 * 一致性保证：
 * - 因果顺序维护：如果删除在因果上先于对同一文档的更新，则没有问题
 * - 快照更新：确保快照至少与这些更新一样新
 * - 原子操作：删除和更新列表的获取在单个锁下完成
 * 
 * 性能优化：
 * - 批量传输：减少网络往返次数
 * - 大小控制：避免单次传输过大导致的内存问题
 * - 状态缓存：通过计数器跟踪未传输的操作数量
 * - 快照管理：及时放弃陈旧快照释放资源
 * 
 * 错误处理：
 * - 状态验证：确保所有克隆数据都已被消耗
 * - 数据完整性：通过xferMods函数处理文档获取和验证
 * - 资源清理：确保未处理的数据重新加入队列
 * 
 * 该函数与TransferModsCommand配合工作，是增量数据迁移的核心实现。
 * 
 * 参数说明：
 * @param opCtx 操作上下文，提供事务和中断支持
 * @param builder BSON对象构建器，用于构建响应数据
 * 
 * 返回值：
 * @return Status 操作执行状态，成功时包含增量修改数据
 * 
 // * 目标分片: MigrationDestinationManager::_migrateDriver->createTransferModsRequest 发送 “_transferMods” 请求
// * 源分片收到请求后：TransferModsCommand::run->MigrationChunkClonerSource::nextModsBatch
 */
Status MigrationChunkClonerSource::nextModsBatch(OperationContext* opCtx, BSONObjBuilder* builder) {
    // 集合锁验证：确保调用者持有集合的意向共享锁
    // 目的：保证在数据读取期间集合不会被删除或重命名
    // 锁模式：MODE_IS 允许并发读取但阻止结构性变更
    dassert(shard_role_details::getLocker(opCtx)->isCollectionLockedForMode(nss(), MODE_IS));

    // 延迟修改操作处理：
    // 功能：处理在事务提交时由于缺少后置镜像而延迟的更新操作
    // 时机：在处理常规修改队列之前优先处理延迟操作
    // 目的：确保所有修改操作都能被正确识别和处理
    _processDeferredXferMods(opCtx);

    // 临时列表声明：
    // 功能：创建本地临时列表用于存储从队列中取出的操作ID
    // 设计：使用本地列表避免长时间持有互斥锁
    std::list<BSONObj> deleteList;  // 删除操作ID列表
    std::list<BSONObj> updateList;  // 更新/插入操作ID列表

    {
        // All clone data must have been drained before starting to fetch the incremental changes.
        // 前置条件验证：所有克隆数据必须在开始获取增量变更之前被消耗完毕
        // 原因：确保迁移过程的正确阶段顺序，避免数据重复或遗漏
        stdx::unique_lock<Latch> lk(_mutex);
        invariant(!_cloneList.hasMore());

        // The "snapshot" for delete and update list must be taken under a single lock. This is to
        // ensure that we will preserve the causal order of writes. Always consume the delete
        // buffer first, before the update buffer. If the delete is causally before the update to
        // the same doc, then there's no problem since we consume the delete buffer first. If the
        // delete is causally after, we will not be able to see the document when we attempt to
        // fetch it, so it's also ok.
        // 原子快照获取：
        // 重要性：删除和更新列表的"快照"必须在单个锁下获取
        // 目的：确保保持写操作的因果顺序
        // 策略：总是先消费删除缓冲区，再消费更新缓冲区
        // 
        // 因果顺序分析：
        // 情况1：删除在因果上先于对同一文档的更新
        //   - 处理：先消费删除缓冲区，因此没有问题
        // 情况2：删除在因果上后于更新
        //   - 处理：尝试获取文档时将无法看到该文档，这也是正确的
        //   - 原因：文档已被删除，获取失败是预期行为
        
        // 删除操作转移：将内部删除队列的所有元素移动到本地列表
        // splice操作：高效地转移元素所有权，避免拷贝开销
        // 结果：_deleted队列被清空，所有删除操作ID转移到deleteList
        deleteList.splice(deleteList.cbegin(), _deleted);
        
        // 更新操作转移：将内部重载队列的所有元素移动到本地列表  
        // splice操作：原子性地转移所有插入和更新操作ID
        // 结果：_reload队列被清空，所有操作ID转移到updateList
        updateList.splice(updateList.cbegin(), _reload);
    }

    // It's important to abandon any open snapshots before processing updates so that we are sure
    // that our snapshot is at least as new as those updates. It's possible for a stale snapshot to
    // still be open from reads performed by _processDeferredXferMods(), above.
    // 快照一致性管理：
    // 重要性：在处理更新之前放弃任何打开的快照，确保快照至少与这些更新一样新
    // 原因：可能存在来自上面_processDeferredXferMods()执行的读取所打开的陈旧快照
    // 操作：放弃当前快照，强制后续读取使用最新的数据快照
    // 目的：保证数据读取的时点一致性和最新性
    shard_role_details::getRecoveryUnit(opCtx)->abandonSnapshot();

    // 删除操作数组构建：
    // 功能：创建包含删除操作的BSON子数组
    // 策略：使用无操作函数直接返回ID文档，无需额外处理
    BSONArrayBuilder arrDel(builder->subarrayStart("deleted"));
    
    // 无操作函数定义：
    // 功能：对于删除操作，直接返回ID文档而不进行任何转换
    // 参数：idDoc - 文档ID，fullDoc - 输出的完整文档
    // 返回：总是返回true表示处理成功
    auto noopFn = [](BSONObj idDoc, BSONObj* fullDoc) {
        *fullDoc = idDoc;  // 直接将ID文档作为完整文档
        return true;
    };
    
    // 删除操作传输：
    // 功能：将删除列表中的操作ID传输到数组构建器中
    // 参数1：数组构建器指针，用于添加删除的文档ID
    // 参数2：删除列表指针，包含所有待传输的删除操作ID
    // 参数3：初始大小为0，因为这是第一个处理的数组
    // 参数4：无操作函数，直接使用ID文档
    // 返回：传输的总文档大小
    long long totalDocSize = xferMods(&arrDel, &deleteList, 0, noopFn);
    arrDel.done();  // 完成删除数组的构建

    // 更新操作条件处理：
    // 条件：只有当删除列表为空时才处理更新列表
    // 原因：如果删除列表未完全处理完毕，优先完成删除操作的传输
    // 策略：避免在单个批次中混合过多不同类型的操作
    if (deleteList.empty()) {
        // 更新操作数组构建：
        // 功能：创建包含重新加载操作的BSON子数组
        // 内容：插入和更新操作需要传输完整的文档内容
        BSONArrayBuilder arrUpd(builder->subarrayStart("reload"));
        
        // 文档查找包装函数：
        // 功能：为更新操作提供文档查找能力
        // 捕获：操作上下文和当前对象指针，用于文档查找
        // 逻辑：根据文档ID在集合中查找完整的文档内容
        auto findByIdWrapper = [opCtx, this](BSONObj idDoc, BSONObj* fullDoc) {
            return Helpers::findById(opCtx, this->nss(), idDoc, *fullDoc);
        };
        
        // 更新操作传输：
        // 功能：将更新列表中的操作ID对应的完整文档传输到数组中
        // 参数1：数组构建器指针，用于添加完整文档
        // 参数2：更新列表指针，包含需要重新加载的文档ID
        // 参数3：当前总大小，基于删除操作的传输大小
        // 参数4：文档查找函数，用于根据ID获取完整文档
        // 返回：更新后的总文档大小
        totalDocSize = xferMods(&arrUpd, &updateList, totalDocSize, findByIdWrapper);
        arrUpd.done();  // 完成更新数组的构建
    }

    // 传输大小记录：
    // 功能：在响应中记录本次传输的总数据大小
    // 用途：接收端可以基于此信息进行传输统计和性能监控
    builder->append("size", totalDocSize);

    // Put back remaining ids we didn't consume
    // 剩余操作恢复：将未消费的ID重新放回内部队列
    // 原因：某些操作可能因为批次大小限制而未被处理
    // 策略：确保未处理的操作在下次调用时能被继续处理
    stdx::unique_lock<Latch> lk(_mutex);
    
    // 删除操作恢复：
    // 操作：将未处理的删除操作ID重新插入到内部删除队列的开头
    // splice：高效地转移所有剩余元素，保持操作顺序
    // 计数更新：更新未传输删除操作的计数器
    _deleted.splice(_deleted.cbegin(), deleteList);
    _untransferredDeletesCounter = _deleted.size();
    
    // 更新操作恢复：
    // 操作：将未处理的更新/插入操作ID重新插入到内部重载队列的开头
    // splice：保持操作的原始顺序，确保后续处理的一致性
    // 计数更新：更新未传输更新操作的计数器
    _reload.splice(_reload.cbegin(), updateList);
    _untransferredUpsertsCounter = _reload.size();
    
    // 延迟操作计数更新：
    // 功能：更新延迟处理的操作计数器
    // 用途：跟踪仍需要延迟处理的操作数量，用于内存使用估算
    _deferredUntransferredOpsCounter = _deferredReloadOrDeletePreImageDocKeys.size();

    return Status::OK();
}

void MigrationChunkClonerSource::_cleanup(bool wasSuccessful) {
    stdx::unique_lock<Latch> lk(_mutex);
    _state = kDone;

    _drainAllOutstandingOperationTrackRequests(lk);

    if (wasSuccessful) {
        invariant(_reload.empty());
        invariant(_deleted.empty());
        invariant(_deferredReloadOrDeletePreImageDocKeys.empty());
    }

    _reload.clear();
    _untransferredUpsertsCounter = 0;
    _deleted.clear();
    _untransferredDeletesCounter = 0;
    _deferredReloadOrDeletePreImageDocKeys.clear();
    _deferredUntransferredOpsCounter = 0;
}


/**
 * MigrationChunkClonerSource::_callRecipient 函数的作用：
 * 向接收端分片发送迁移相关命令并等待响应的核心网络通信函数。
 * 
 * 核心功能：
 * 1. 异步命令执行：使用任务执行器异步发送远程命令到接收端分片
 * 2. 响应等待机制：等待接收端的命令执行结果，支持操作中断
 * 3. 错误处理和重试：处理网络错误、命令错误和中断异常
 * 4. 资源管理：确保回调函数的正确清理和任务取消
 * 5. 状态验证：验证命令执行结果的有效性
 * 
 * 使用场景：
 * - startChunkClone：启动接收端的克隆准备
 * - _recvChunkStatus：查询接收端的克隆进度和状态
 * - _recvChunkCommit：提交迁移事务
 * - _recvChunkAbort：中止迁移操作
 * 
 * 异步执行模式：
 * - 使用 TaskExecutor 进行异步网络通信
 * - 通过回调函数处理响应结果
 * - 支持操作上下文的中断机制
 * 
 * 错误处理策略：
 * - 调度失败：返回调度错误状态
 * - 等待中断：取消任务并安全等待回调完成
 * - 网络错误：返回网络通信错误
 * - 命令错误：从响应中提取并返回命令执行错误
 * 
 * 资源安全保证：
 * - RAII 模式：自动管理任务生命周期
 * - 中断安全：即使被中断也能正确清理资源
 * - 回调安全：确保本地变量的有效性
 * 
 * 该函数是迁移过程中所有分片间通信的统一入口点，确保网络通信的可靠性和一致性。
 * 源分片： MigrationChunkClonerSource::_callRecipient 发送 recvChunkStatus 请求
 * 目标分片: RecvChunkStatusCommand::run->MigrationDestinationManager::report 接收 recvChunkStatus 请求处理
 */
StatusWith<BSONObj> MigrationChunkClonerSource::_callRecipient(OperationContext* opCtx,
                                                               const BSONObj& cmdObj) {
    // 响应状态初始化：
    // 功能：创建响应状态对象，初始化为错误状态
    // 目的：确保在异步回调执行前有明确的初始状态
    // 线程安全：通过引用捕获在回调中更新状态
    executor::RemoteCommandResponse responseStatus(
        Status{ErrorCodes::InternalError, "Uninitialized value"});

    // 获取任务执行器：
    // 功能：从全局执行器池中获取固定执行器用于网络通信
    // 特点：使用固定执行器确保一致的网络行为和性能特征
    // 资源：执行器负责管理网络连接池和异步任务调度
    auto executor = Grid::get(getGlobalServiceContext())->getExecutorPool()->getFixedExecutor();
    
    // 调度远程命令：
    // 功能：向接收端分片异步发送命令
    // 参数：目标主机、数据库、命令对象、操作上下文
    // 回调：命令完成时更新 responseStatus
    auto scheduleStatus = executor->scheduleRemoteCommand(
        executor::RemoteCommandRequest(_recipientHost,          // 接收端主机地址
                                      DatabaseName::kAdmin,     // 目标数据库（admin）
                                      cmdObj,                   // 要执行的命令对象
                                      nullptr),                 // 元数据（此处为空）
        // 异步回调函数：在命令完成时被调用
        // 功能：将远程命令的执行结果保存到 responseStatus 中
        // 线程安全：executor 保证回调的线程安全性
        [&responseStatus](const executor::TaskExecutor::RemoteCommandCallbackArgs& args) {
            responseStatus = args.response;  // 保存响应结果
        });

    // TODO: Update RemoteCommandTargeter on NotWritablePrimary errors.
    // 调度失败处理：
    // 功能：检查命令调度是否成功
    // 失败原因：执行器资源不足、网络配置错误等
    // 返回：调度错误状态，不等待执行结果
    if (!scheduleStatus.isOK()) {
        return scheduleStatus.getStatus();
    }

    // 获取回调句柄：
    // 功能：获取已调度任务的句柄，用于后续的等待和取消操作
    // 用途：支持任务取消和状态查询
    auto cbHandle = scheduleStatus.getValue();

    try {
        // 等待命令执行完成：
        // 功能：阻塞等待远程命令执行完成
        // 中断支持：如果操作上下文被中断，会抛出异常
        // 超时：使用操作上下文配置的超时时间
        executor->wait(cbHandle, opCtx);
        
    } catch (const DBException& ex) {
        // If waiting for the response is interrupted, then we still have a callback out and
        // registered with the TaskExecutor to run when the response finally does come back.
        // Since the callback references local state, cbResponse, it would be invalid for the
        // callback to run after leaving the this function. Therefore, we cancel the callback
        // and wait uninterruptably for the callback to be run.
        // 中断异常处理：
        // 问题：等待响应被中断时，回调仍然注册在任务执行器中
        // 风险：回调引用本地状态 responseStatus，函数退出后引用失效
        // 解决：取消回调并不可中断地等待回调完成，确保资源安全
        
        // 取消任务：通知执行器取消远程命令执行
        // 注意：这不会立即停止网络通信，只是标记任务为取消状态
        executor->cancel(cbHandle);
        
        // 不可中断等待：
        // 功能：等待回调函数完成执行，即使任务被取消
        // 必要性：确保回调不会在函数退出后访问无效的本地变量
        // 安全性：使用不可中断等待避免资源泄漏
        executor->wait(cbHandle);
        
        // 返回中断异常：向调用方报告操作被中断
        return ex.toStatus();
    }

    // 网络响应状态检查：
    // 功能：检查网络层面的通信是否成功
    // 失败情况：网络超时、连接断开、DNS解析失败等
    // 区别：这是网络层错误，不是命令执行错误
    if (!responseStatus.isOK()) {
        return responseStatus.status;
    }

    // 命令执行状态检查：
    // 功能：从命令响应中提取命令级别的执行状态
    // 作用：检查远程分片是否成功执行了命令
    // 错误类型：权限错误、参数错误、业务逻辑错误等
    Status commandStatus = getStatusFromCommandResult(responseStatus.data);
    if (!commandStatus.isOK()) {
        return commandStatus;
    }

    // 成功响应处理：
    // 功能：返回命令执行的响应数据
    // 所有权：创建响应数据的独立副本，确保数据有效性
    // 用途：调用方可以从响应中提取具体的业务数据
    return responseStatus.data.getOwned();
}

/**
 * MigrationChunkClonerSource::_getIndexScanExecutor 函数的作用：
 * 为chunk迁移创建基于分片键索引的范围扫描执行器，是迁移过程中数据获取的基础设施。
 * 
 * 核心功能：
 * 1. 分片键索引查找：在集合中找到与分片键模式匹配的索引
 * 2. 范围边界扩展：将chunk的min/max边界扩展到匹配索引的完整键模式
 * 3. 索引扫描执行器创建：基于扩展后的范围创建高效的索引扫描执行器
 * 4. 多键索引支持：允许在分片键字段上使用多键索引（基于分片键单值不变式）
 * 5. 让步策略配置：配置自动让步策略以保持系统响应性
 * 
 * 使用场景：
 * - 巨型块迁移：为_nextCloneBatchFromIndexScan提供流式扫描能力
 * - 记录ID收集：为_storeCurrentRecordId提供索引扫描基础
 * - 范围验证：确保扫描严格限制在指定的chunk范围内
 * 
 * 索引选择策略：
 * - 分片键前缀匹配：查找以分片键为前缀的索引
 * - 多键索引兼容：基于分片键单值特性允许多键索引
 * - 性能优化：选择最适合范围扫描的索引结构
 * 
 * 范围处理机制：
 * - 边界扩展：将用户指定的min/max扩展到索引的完整键模式
 * - 格式转换：将BSON对象转换为索引键格式
 * - 包含性控制：使用半开区间[min, max)确保精确的范围控制
 * 
 * 让步和性能：
 * - 自动让步：配置YIELD_AUTO策略定期释放锁和让步CPU
 * - 中断支持：支持操作上下文的中断机制
 * - 方向优化：使用FORWARD方向进行顺序扫描
 * 
 * 错误处理：
 * - 索引缺失：如果找不到合适的分片键索引则返回错误
 * - 参数验证：验证输入参数的有效性
 * 
 * 该函数是chunk迁移中索引扫描的统一入口点，为不同的迁移策略提供高效的数据访问能力。
 */
StatusWith<std::unique_ptr<PlanExecutor, PlanExecutor::Deleter>>
MigrationChunkClonerSource::_getIndexScanExecutor(OperationContext* opCtx,
                                                  const CollectionPtr& collection,
                                                  InternalPlanner::IndexScanOptions scanOption) {
    // Allow multiKey based on the invariant that shard keys must be single-valued. Therefore, any
    // multi-key index prefixed by shard key cannot be multikey over the shard key fields.
    // 分片键索引查找：
    // 多键索引支持：基于分片键必须是单值的不变式，允许多键索引
    // 原理：任何以分片键为前缀的多键索引在分片键字段上不能是多键的
    // 参数1：操作上下文，提供事务和中断支持
    // 参数2：集合对象，用于访问集合的索引目录
    // 参数3：分片键模式的BSON表示，用于匹配索引前缀
    // 参数4：requireSingleKey=false，允许多键索引（基于上述不变式）
    // 返回：找到的分片键前缀索引，如果未找到则返回nullptr
    const auto shardKeyIdx = findShardKeyPrefixedIndex(opCtx,
                                                       collection,
                                                       _shardKeyPattern.toBSON(),
                                                       /*requireSingleKey=*/false);
    if (!shardKeyIdx) {
        // 索引缺失错误：无法找到与分片键模式匹配的索引
        // 这通常表示集合结构问题或分片配置错误
        return {ErrorCodes::IndexNotFound,
                str::stream() << "can't find index with prefix " << _shardKeyPattern.toBSON()
                              << " in storeCurrentRecordId for " << nss().toStringForErrorMsg()};
    }

    // Assume both min and max non-empty, append MinKey's to make them fit chosen index
    // 范围边界扩展处理：
    // 假设：min和max都非空，追加MinKey使它们适配选择的索引
    // 目的：将用户指定的chunk边界扩展到索引的完整键模式
    
    // 创建键模式对象：基于找到的索引键模式
    // 作用：提供键扩展和格式转换的工具方法
    const KeyPattern kp(shardKeyIdx->keyPattern());

    // 最小边界扩展和格式化：
    // extendRangeBound：将chunk的最小边界扩展到索引的完整键模式
    // 参数1：getMin() - chunk的原始最小边界
    // 参数2：false - 表示这是最小边界（不是最大边界）
    // toKeyFormat：将扩展后的边界转换为索引键的内部格式
    // 结果：适用于索引扫描的格式化最小边界
    BSONObj min = Helpers::toKeyFormat(kp.extendRangeBound(getMin(), false));
    
    // 最大边界扩展和格式化：
    // extendRangeBound：将chunk的最大边界扩展到索引的完整键模式  
    // 参数1：getMax() - chunk的原始最大边界
    // 参数2：false - 表示这是最大边界的处理方式
    // toKeyFormat：将扩展后的边界转换为索引键的内部格式
    // 结果：适用于索引扫描的格式化最大边界
    BSONObj max = Helpers::toKeyFormat(kp.extendRangeBound(getMax(), false));

    // We can afford to yield here because any change to the base data that we might miss is already
    // being queued and will migrate in the 'transferMods' stage.
    // 索引扫描执行器创建：
    // 让步策略说明：这里可以进行让步，因为我们可能错过的任何基础数据变更
    // 已经被排队并将在'transferMods'阶段进行迁移
    // 
    // 参数详解：
    // - opCtx：操作上下文，提供事务管理和中断支持
    // - &collection：集合对象引用，用于数据访问
    // - *shardKeyIdx：分片键索引描述符，指定要使用的索引
    // - min：格式化的最小边界，定义扫描范围的起始点
    // - max：格式化的最大边界，定义扫描范围的结束点
    // - BoundInclusion::kIncludeStartKeyOnly：边界包含策略
    //   表示包含起始键但不包含结束键，即半开区间[min, max)
    //   这确保了chunk边界的精确控制，避免重复或遗漏数据
    // - PlanYieldPolicy::YieldPolicy::YIELD_AUTO：自动让步策略
    //   执行器会根据时间和操作数量自动进行让步
    //   在长时间运行的扫描中定期释放锁和让步CPU
    // - InternalPlanner::Direction::FORWARD：扫描方向为正向
    //   按照索引键的升序进行扫描，这通常是最高效的方式
    // - scanOption：扫描选项，由调用方指定（IXSCAN_DEFAULT或IXSCAN_FETCH）
    //   IXSCAN_DEFAULT：只返回索引键和记录ID
    //   IXSCAN_FETCH：返回完整的文档内容
    // 
    // 返回：包含计划执行器的StatusWith对象
    // 成功时：返回配置好的索引扫描执行器
    // 失败时：返回错误状态，包含具体的失败原因
    return InternalPlanner::shardKeyIndexScan(opCtx,
                                              &collection,
                                              *shardKeyIdx,
                                              min,
                                              max,
                                              BoundInclusion::kIncludeStartKeyOnly,
                                              PlanYieldPolicy::YieldPolicy::YIELD_AUTO,
                                              InternalPlanner::Direction::FORWARD,
                                              scanOption);
}

/**
 * MigrationChunkClonerSource::_storeCurrentRecordId 函数的作用：
 * 预扫描要迁移的chunk范围并存储文档记录ID，为高效的批量数据传输做准备。
 * 
 * 核心功能：
 * 1. 记录ID预收集：通过索引扫描收集chunk范围内所有文档的物理记录ID
 * 2. 巨型块检测：检测超过大小限制的chunk并进行相应处理
 * 3. 性能统计收集：计算平均文档大小、平均对象ID大小等性能参数
 * 4. 内存管理评估：评估克隆过程中的内存使用情况
 * 5. 传输策略选择：为后续的数据传输选择最优的访问策略
 * 
 * 工作流程：
 * - 索引扫描：使用分片键索引扫描chunk范围内的所有文档
 * - 记录ID收集：将每个文档的物理记录ID存储在内存中的集合里
 * - 大小检测：统计文档数量，检测是否为巨型块
 * - 统计计算：计算集合和索引的平均大小信息
 * - 结果存储：将收集的记录ID存储到克隆列表中
 * 
 * 巨型块处理：
 * - 大小阈值：基于平均文档大小和最大chunk大小计算阈值
 * - 强制模式：如果启用强制迁移，巨型块将使用直接扫描模式
 * - 错误返回：非强制模式下巨型块会返回ChunkTooBig错误
 * 
 * 性能优化：
 * - 预取策略：预先收集记录ID避免后续重复索引扫描
 * - 随机访问：基于记录ID的直接文档访问比索引扫描更高效
 * - 内存评估：提供准确的内存使用预估以优化批次大小
 * - 统计驱动：基于实际数据特征优化传输参数
 * 
 * 错误处理：
 * - 索引缺失：检查分片键索引和_id索引的存在性
 * - 中断支持：定期检查操作上下文的中断状态
 * - 异常捕获：处理执行器扫描过程中的异常
 * 
 * 该函数是常规大小chunk高效迁移的基础，通过预收集记录ID实现高性能的批量数据访问。
// 源分片 MigrationChunkClonerSource::startClone->MigrationChunkClonerSource::_storeCurrentRecordId 中会返回 ErrorCodes::ChunkTooBig
// config server接收到应答后在 processRebalanceResponse 中识别ErrorCodes::ChunkTooBig，然后把该chunk标记为jumbo
 
 * 源分片: 记录需要迁移chunk的RecordId， MigrationChunkClonerSource::startClone->MigrationChunkClonerSource::_storeCurrentRecordId
 * 源分片收到目标分片发来的_migrateClone请求: 根据上面记录的RecordId获取对应数据返回给目标分片 MigrationChunkClonerSource::startClone->MigrationChunkClonerSource::_nextCloneBatchFromCloneRecordIds
*/
Status MigrationChunkClonerSource::_storeCurrentRecordId(OperationContext* opCtx) {
    // 获取集合访问权限：以意向共享模式锁定集合
    // MODE_IS：允许并发读取，但阻止删除或重命名操作
    // 目的：确保在扫描期间集合结构保持稳定
    AutoGetCollection collection(opCtx, nss(), MODE_IS);
    if (!collection) {
        // 集合不存在错误：迁移过程中集合被删除
        return {ErrorCodes::NamespaceNotFound,
                str::stream() << "Collection " << nss().toStringForErrorMsg()
                              << " does not exist."};
    }

    // 获取索引扫描执行器：
    // 功能：创建基于分片键索引的范围扫描执行器
    // 参数：操作上下文、集合、默认索引扫描选项
    // 用途：按照分片键顺序扫描chunk范围内的所有文档
    auto swExec = _getIndexScanExecutor(
        opCtx, collection.getCollection(), InternalPlanner::IndexScanOptions::IXSCAN_DEFAULT);
    if (!swExec.isOK()) {
        return swExec.getStatus();
    }
    auto exec = std::move(swExec.getValue());

    // Use the average object size to estimate how many objects a full chunk would carry do that
    // while traversing the chunk's range using the sharding index, below there's a fair amount of
    // slack before we determine a chunk is too large because object sizes will vary.
    // 巨型块检测参数计算：
    // 使用平均对象大小估算一个完整chunk可以容纳多少个对象
    // 在使用分片索引遍历chunk范围时进行检测
    // 由于对象大小会变化，在确定chunk过大之前会有相当大的松弛空间
    unsigned long long maxRecsWhenFull;  // chunk满载时的最大记录数
    long long avgRecSize;                // 平均记录大小

    // 集合统计信息获取：计算平均文档大小
    const long long totalRecs = collection->numRecords(opCtx);
    if (totalRecs > 0) {
        // 平均记录大小计算：总数据大小 / 总记录数
        avgRecSize = collection->dataSize(opCtx) / totalRecs;
        // The calls to numRecords() and dataSize() are not atomic so it is possible that the data
        // size becomes smaller than the number of records between the two calls, which would result
        // in average record size of zero
        // 零值保护：numRecords()和dataSize()调用不是原子的
        // 可能出现数据大小在两次调用之间变得小于记录数的情况，导致平均记录大小为零
        if (avgRecSize == 0) {
            avgRecSize = BSONObj::kMinBSONLength;  // 使用最小BSON长度作为默认值
        }
        
        // 最大记录数阈值计算：
        // 基于最大chunk大小和平均记录大小计算能容纳的最大记录数
        // 至少为1，避免除零错误
        maxRecsWhenFull = std::max(_args.getMaxChunkSizeBytes() / avgRecSize, 1LL);
        maxRecsWhenFull = 2 * maxRecsWhenFull;  // pad some slack - 添加2倍松弛空间
    } else {
        // 空集合处理：设置默认值
        avgRecSize = 0;
        maxRecsWhenFull = kMaxObjectPerChunk + 1;  // 默认最大对象数 + 1
    }

    // Do a full traversal of the chunk and don't stop even if we think it is a large chunk we want
    // the number of records to better report, in that case.
    // 完整遍历chunk范围：
    // 即使认为这是一个大chunk也不要停止，我们希望获得记录数以便更好地报告
    bool isLargeChunk = false;           // 是否为大chunk标志
    unsigned long long recCount = 0;     // 实际记录计数

    try {
        BSONObj obj;              // 当前文档对象
        RecordId recordId;        // 当前记录ID
        RecordIdSet recordIdSet;  // 记录ID集合，用于存储所有收集的记录ID

        // 主扫描循环：遍历执行器返回的所有文档
        while (PlanExecutor::ADVANCED == exec->getNext(&obj, &recordId)) {
            // 中断检查：定期检查操作是否被中断
            // 长时间运行的操作需要支持中断以保持系统响应性
            Status interruptStatus = opCtx->checkForInterruptNoAssert();
            if (!interruptStatus.isOK()) {
                return interruptStatus;
            }

            // 记录ID收集：如果不是大chunk，将记录ID添加到集合中
            // 目的：为后续的随机访问做准备
            if (!isLargeChunk) {
                recordIdSet.insert(recordId);
            }

            // 大chunk检测：当记录数超过阈值时标记为大chunk
            if (++recCount > maxRecsWhenFull) {
                isLargeChunk = true;

                // 强制迁移处理：如果启用强制迁移，清空记录ID集合并退出
                // 原因：大chunk将使用直接索引扫描而不是预存储的记录ID
                if (_forceJumbo) {
                    recordIdSet.clear();
                    break;
                }
            }
        }

        // 记录ID列表填充：将收集的记录ID移动到克隆列表中
        // std::move：避免大量记录ID的拷贝开销
        // 结果：克隆列表包含按分片键顺序排列的所有记录ID
        _cloneList.populateList(std::move(recordIdSet));
        
    } catch (DBException& exception) {
        // 执行器错误处理：为异常添加上下文信息
        exception.addContext("Executor error while scanning for documents belonging to chunk");
        throw;
    }

    // 集合平均对象大小获取：
    // 功能：获取集合级别的平均对象大小统计
    // 用途：用于内存使用估算和批次大小优化
    const uint64_t collectionAverageObjectSize = collection->averageObjectSize(opCtx);

    // 平均对象ID大小计算初始化
    uint64_t averageObjectIdSize = 0;
    const uint64_t defaultObjectIdSize = OID::kOIDSize;  // 默认ObjectId大小

    // For clustered collection, an index on '_id' is not required.
    // 对象ID大小统计：对于聚集集合，'_id'索引不是必需的
    if (totalRecs > 0 && !collection->isClustered()) {
        // _id索引查找：获取_id索引以计算平均ID大小
        const auto idIdx = collection->getIndexCatalog()->findIdIndex(opCtx);
        if (!idIdx || !idIdx->getEntry()) {
            // _id索引缺失错误：非聚集集合必须有_id索引
            return {ErrorCodes::IndexNotFound,
                    str::stream() << "can't find index '_id' in storeCurrentRecordId for "
                                  << nss().toStringForErrorMsg()};
        }

        // 平均对象ID大小计算：索引空间使用量 / 总记录数
        // 目的：准确估算ID字段的存储开销
        averageObjectIdSize =
            idIdx->getEntry()->accessMethod()->getSpaceUsedBytes(opCtx) / totalRecs;
    }

    // 大chunk错误检查：如果是大chunk且未启用强制模式，返回错误
    if (isLargeChunk) {
        return {
            ErrorCodes::ChunkTooBig,
            str::stream() << "Cannot move chunk: the maximum number of documents for a chunk is "
                          << maxRecsWhenFull << ", the maximum chunk size is "
                          << _args.getMaxChunkSizeBytes() << ", average document size is "
                          << avgRecSize << ". Found " << recCount << " documents in chunk "
                          << " ns: " << nss().toStringForErrorMsg() << " " << getMin() << " -> "
                          << getMax()};
    }

    // 统计信息存储：在互斥锁保护下更新成员变量
    // 目的：为后续的内存使用估算和性能优化提供数据
    stdx::lock_guard<Latch> lk(_mutex);
    
    // 克隆记录ID的平均对象大小：集合平均大小 + 默认ObjectId大小
    // 用途：估算克隆过程中每个记录的内存使用量
    _averageObjectSizeForCloneRecordIds = collectionAverageObjectSize + defaultObjectIdSize;
    
    // 平均对象ID大小：使用计算值和默认值中的较大者
    // 保护：确保至少使用默认ObjectId大小，避免低估
    _averageObjectIdSize = std::max(averageObjectIdSize, defaultObjectIdSize);
    
    return Status::OK();
}

/**
 * xferMods 函数的作用：
 * chunk迁移过程中增量修改操作的批量传输核心工具函数，负责将修改操作ID转换为实际文档并控制传输批次大小。
 * 
 * 核心功能：
 * 1. 批量文档传输：将修改操作ID列表中的文档批量添加到BSON数组构建器中
 * 2. 大小限制控制：动态控制传输批次大小，避免超出BSON最大用户大小限制
 * 3. 重复文档去重：通过ID集合确保同一文档在单次批次中不会重复传输
 * 4. 文档提取抽象：通过函数对象抽象不同类型操作的文档获取逻辑
 * 5. 内存使用估算：提供准确的传输数据大小统计
 * 
 * 使用场景：
 * - 删除操作传输：将删除操作的文档ID直接传输（使用noopFn）
 * - 更新/插入操作传输：根据ID查找完整文档内容并传输（使用findByIdWrapper）
 * - 批次大小优化：考虑BSON数组索引开销，精确控制传输数据量
 * 
 * 传输策略：
 * - 去重机制：使用ID视图集合避免重复文档传输
 * - 大小预测：预估添加文档后的大小，防止超出BSON限制
 * - 动态终止：当达到大小限制时优雅终止，剩余操作留待下次处理
 * - 固定开销考虑：预留命令固定开销空间，确保完整传输
 * 
 * 性能优化：
 * - 字符串视图：使用absl::string_view避免字符串拷贝开销
 * - 原地去重：通过哈希集合实现高效的文档ID去重
 * - 批量删除：使用迭代器范围删除已处理的操作，提高效率
 * - 内存友好：及时清理已处理的操作，避免内存累积
 * 
 * 错误处理：
 * - 文档缺失：通过extractDocToAppendFn返回值处理文档不存在的情况
 * - 大小超限：安全处理单个文档过大或累积大小超限的情况
 * - 空列表保护：处理空修改列表或初始大小已超限的边界情况
 * 
 * 函数设计：
 * - 通用性：支持不同类型的文档提取策略（删除ID vs 完整文档查找）
 * - 可扩展性：通过函数对象参数支持新的文档处理逻辑
 * - 线程安全：函数本身无状态，依赖调用方的同步控制
 * 
 * 与迁移流程的集成：
 * - nextModsBatch调用：用于处理删除和更新操作的批量传输
 * - 队列管理：配合splice操作实现高效的队列元素转移
 * - 状态恢复：支持未处理操作的回滚和重新处理
 * 
 * 该函数是chunk迁移增量数据传输的核心工具，确保了修改操作的高效、可靠传输。
 * 
 * 参数说明：
 * @param arr BSON数组构建器，用于收集要传输的文档
 * @param modsList 修改操作ID列表，包含需要处理的文档ID
 * @param initialSize 初始大小，用于累积计算总传输大小
 * @param extractDocToAppendFn 文档提取函数，定义如何从ID获取实际文档
 * 
 * 返回值：
 * @return long long 传输数据的总字节大小
 */
long long xferMods(BSONArrayBuilder* arr,
                   std::list<BSONObj>* modsList,
                   long long initialSize,
                   std::function<bool(BSONObj, BSONObj*)> extractDocToAppendFn) {
    // BSON大小限制常量：
    // 功能：定义单次传输的最大BSON对象大小限制
    // 值：BSONObjMaxUserSize，通常为16MB减去一些保留空间
    // 用途：确保传输的数据不超过MongoDB的BSON大小限制
    const long long maxSize = BSONObjMaxUserSize;

    // 边界条件检查：
    // 条件1：modsList->empty() - 修改操作列表为空，没有数据需要传输
    // 条件2：initialSize > maxSize - 初始大小已超过最大限制
    // 处理：直接返回初始大小，避免无效的处理逻辑
    // 优化：早期返回减少不必要的计算开销
    if (modsList->empty() || initialSize > maxSize) {
        return initialSize;
    }

    // 重复文档去重集合：
    // 功能：存储已处理的文档ID视图，避免在单次批次中重复传输同一文档
    // 类型：使用absl::string_view作为键，避免字符串拷贝开销
    // 原理：多个修改操作可能针对同一文档，需要去重以避免数据重复
    // 性能：字符串视图比完整字符串拷贝更高效
    stdx::unordered_set<absl::string_view> addedSet;
    
    // 修改操作迭代器：
    // 功能：遍历修改操作列表，处理每个操作ID
    // 初始化：指向列表的开始位置
    // 用途：后续用于批量删除已处理的操作
    auto iter = modsList->begin();
    
    // 主处理循环：遍历所有修改操作直到列表结束
    for (; iter != modsList->end(); ++iter) {
        // 当前操作ID提取：
        // 功能：获取当前迭代器指向的文档ID对象
        // 格式：通常是包含_id字段的BSON对象
        // 用途：用于后续的文档查找和去重检查
        auto idDoc = *iter;
        
        // 文档ID视图创建：
        // 功能：创建文档ID的字符串视图，用于高效的去重检查
        // 参数1：idDoc.objdata() - BSON对象的原始字节数据指针
        // 参数2：idDoc.objsize() - BSON对象的字节大小
        // 优势：避免字符串拷贝，直接使用原始内存进行比较
        absl::string_view idDocView(idDoc.objdata(), idDoc.objsize());

        // 重复文档检查：
        // 功能：检查当前文档ID是否已经在本批次中处理过
        // 逻辑：如果在addedSet中找不到，说明是新文档，需要处理
        // 目的：确保同一文档在单次批次中只被传输一次
        if (addedSet.find(idDocView) == addedSet.end()) {
            // 标记文档已处理：
            // 操作：将文档ID视图添加到已处理集合中
            // 结果：后续遇到相同ID的操作将被跳过
            addedSet.insert(idDocView);
            
            // 文档提取变量：
            // 功能：存储从ID提取的完整文档内容
            // 初始化：空BSON对象，由extractDocToAppendFn填充
            BSONObj fullDoc;
            
            // 文档提取操作：
            // 功能：调用提供的函数对象从文档ID提取实际文档内容
            // 参数1：idDoc - 文档ID对象
            // 参数2：&fullDoc - 输出参数，存储提取的完整文档
            // 返回值：true表示成功提取，false表示文档不存在或提取失败
            // 
            // 函数对象的不同实现：
            // - 删除操作：noopFn直接返回ID文档，无需查找
            // - 更新操作：findByIdWrapper根据ID查找完整文档内容
            if (extractDocToAppendFn(idDoc, &fullDoc)) {
                // Use the builder size instead of accumulating the document sizes directly so
                // that we take into consideration the overhead of BSONArray indices.
                // BSON大小限制检查：
                // 策略：使用构建器大小而不是直接累积文档大小，以考虑BSONArray索引的开销
                // 条件1：arr->arrSize() - 确保数组中已有文档（保证批次有进展）
                // 条件2：大小检查 - 当前长度 + 文档大小 + 固定开销 > 最大大小
                // 固定开销：kFixedCommandOverhead 考虑命令结构的额外开销
                // 退出条件：如果添加当前文档会超出大小限制，则退出循环
                if (arr->arrSize() &&
                    (arr->len() + fullDoc.objsize() + kFixedCommandOverhead) > maxSize) {
                    // 大小超限处理：
                    // 操作：退出循环，结束当前批次的处理
                    // 结果：当前文档不会被添加，将在下次批次中处理
                    // 重要性：确保传输的数据不会超出MongoDB的限制
                    break;
                }
                
                // 文档添加到传输批次：
                // 操作：将提取的完整文档添加到BSON数组构建器中
                // 结果：文档将包含在传输给接收端的批次中
                // 顺序：按照修改操作列表的顺序添加文档
                arr->append(fullDoc);
            }
            // 注意：如果extractDocToAppendFn返回false（文档不存在），
            // 不会添加任何内容到数组中，但操作仍会被标记为已处理
        }
        // 注意：如果文档ID已在addedSet中（重复文档），
        // 直接跳过，不进行任何处理，继续下一个操作
    }

    // 传输大小计算：
    // 功能：获取BSON数组构建器的当前长度作为总传输大小
    // 包含：所有添加的文档数据 + BSONArray的索引开销
    // 用途：返回给调用方，用于统计和日志记录
    long long totalSize = arr->len();
    
    // 已处理操作清理：
    // 功能：从修改操作列表中删除已处理的操作
    // 范围：从列表开始到当前迭代器位置（不包含当前迭代器指向的元素）
    // 结果：未处理的操作保留在列表中，下次调用时继续处理
    // 性能：使用范围删除比逐个删除更高效
    modsList->erase(modsList->begin(), iter);

    // 返回传输总大小：
    // 内容：当前批次传输的总字节数
    // 用途：调用方可以基于此信息进行统计、日志记录和性能监控
    return totalSize;
}

/**
 * _checkRecipientCloningStatus 函数的作用：
 * 监控和检查接收端分片的克隆状态，确定何时可以进入关键区域（Critical Section）。
 * 
 * 核心功能：
 * 1. 状态轮询监控：定期查询接收端分片的克隆进度和状态
 * 2. 关键区域准入控制：判断何时可以安全地进入关键区域开始最终提交
 * 3. 内存使用监控：监控源端的内存使用情况，防止内存溢出
 * 4. 会话迁移协调：确保会话数据也完成迁移或达到可接受的阈值
 * 5. 进度日志记录：记录迁移进度信息，便于监控和调试
 * 
 * 关键区域准入条件：
 * - 接收端状态为 "steady"：表示接收端已完成初始克隆并保持稳定
 * - 会话数据同步：会话目录源处于追赶阶段且未传输数据量为0
 * - 残留数据检查：确保源端没有剩余未克隆的文档
 * - 内存限制控制：源端内存使用量不超过阈值
 * 
 * 优化策略：
 * - 支持在追赶阶段进入关键区域：当未传输数据量在阈值内时提前进入
 * - 动态调整等待时间：基于响应状态调整轮询间隔
 * - 巨型块特殊处理：对强制迁移的巨型块采用特殊监控策略
 * 
 * 异常处理：
 * - 超时控制：在指定时间内未达到条件则返回超时错误
 * - 状态验证：验证接收端返回的迁移会话信息
 * - 内存保护：检测内存使用过高时中止迁移
 * - 中断处理：响应操作上下文的中断请求
 * 
 * 该函数是迁移过程中的关键控制点，确保在适当的时机进入关键区域以完成迁移。
 * 源分片： MigrationChunkClonerSource::awaitUntilCriticalSectionIsAppropriate
 */
Status MigrationChunkClonerSource::_checkRecipientCloningStatus(OperationContext* opCtx,
                                                                Milliseconds maxTimeToWait) {
    // 记录监控开始时间：用于超时控制
    const auto startTime = Date_t::now();
    int iteration = 0;
    
    // 主监控循环：持续检查直到满足条件或超时
    // maxTimeToWait 默认 const Hours kMaxWaitToCommitCloneForJumboChunk(6); 6小时
    while ((Date_t::now() - startTime) < maxTimeToWait) { 
        // 向接收端发送状态查询请求 _recvChunkStatus：
        // 功能：获取接收端当前的克隆状态和进度信息
        // 参数：waitForSteadyOrDone=true 表示等待接收端达到稳定或完成状态
        auto responseStatus = _callRecipient(
            opCtx, createRequestWithSessionId(kRecvChunkStatus, nss(), _sessionId, true));
        if (!responseStatus.isOK()) {
            return responseStatus.getStatus().withContext(
                "Failed to contact recipient shard to monitor data transfer");
        }

        const BSONObj& res = responseStatus.getValue();
        
        // 动态等待调整：
        // 功能：如果接收端没有等待，则使用指数退避策略
        // 目的：减少不必要的网络请求，优化性能
        if (!res["waited"].boolean()) {
            sleepmillis(1LL << std::min(iteration, 10));
        }
        iteration++;

        // 会话迁移状态检查：
        // 功能：获取会话目录迁移的当前状态和待传输数据量
        // 重要性：会话数据的完整迁移是迁移成功的必要条件
        const auto sessionCatalogSourceInCatchupPhase = _sessionCatalogSource->inCatchupPhase();
        const auto estimateUntransferredSessionsSize = sessionCatalogSourceInCatchupPhase
            ? _sessionCatalogSource->untransferredCatchUpDataSize()
            : std::numeric_limits<int64_t>::max();

        stdx::lock_guard<Latch> sl(_mutex);

        // 未传输修改操作大小估算：
        // 功能：计算源端还有多少数据未传输到接收端
        // 组成：删除操作 * 平均ID大小 + (更新操作 + 延迟操作) * 平均文档大小
        // 用途：判断是否可以进入关键区域的重要指标
        int64_t untransferredModsSizeBytes = _untransferredDeletesCounter * _averageObjectIdSize +
            (_untransferredUpsertsCounter + _deferredUntransferredOpsCounter) *
                _averageObjectSizeForCloneRecordIds;

        // 进度日志记录：根据迁移类型记录不同的进度信息
        if (_forceJumbo && _jumboChunkCloneState) {
            // 巨型块迁移进度：记录已克隆文档数量
            LOGV2(21992,
                  "moveChunk data transfer progress",
                  "response"_attr = redact(res),
                  "memoryUsedBytes"_attr = _memoryUsed,
                  "docsCloned"_attr = _jumboChunkCloneState->docsCloned,
                  "untransferredModsSizeBytes"_attr = untransferredModsSizeBytes);
        } else {
            // 常规迁移进度：记录剩余待克隆文档数量
            LOGV2(21993,
                  "moveChunk data transfer progress",
                  "response"_attr = redact(res),
                  "memoryUsedBytes"_attr = _memoryUsed,
                  // 预估算一下该 chunk 还有多少文档未迁移 
                  "docsRemainingToClone"_attr =
                      _cloneList.size() - _numRecordsCloned - _numRecordsPassedOver,
                  "untransferredModsSizeBytes"_attr = untransferredModsSizeBytes);
        }

        // 标准关键区域准入检查：
        // 条件：接收端状态为 "steady" 且会话处于追赶阶段且无未传输会话数据
        // 目的：确保接收端已完成初始克隆并且会话数据也完全同步
        if (res["state"].String() == "steady" && sessionCatalogSourceInCatchupPhase &&
            estimateUntransferredSessionsSize == 0) {
            // 残留数据检查：确保源端没有剩余的克隆数据
            if (_cloneList.hasMore() ||
                (_jumboChunkCloneState && _forceJumbo &&
                 PlanExecutor::IS_EOF != _jumboChunkCloneState->clonerState)) {
                return {ErrorCodes::OperationIncomplete,
                        str::stream() << "Unable to enter critical section because the recipient "
                                         "shard thinks all data is cloned while there are still "
                                         "documents remaining"};
            }

            return Status::OK();
        }

        // 接收端功能支持检查：
        // 功能：检查接收端是否支持在追赶阶段进入关键区域的优化特性
        // 版本兼容性：确保新旧版本之间的兼容性
        bool supportsCriticalSectionDuringCatchUp = false;
        if (auto featureSupportedField =
                res[StartChunkCloneRequest::kSupportsCriticalSectionDuringCatchUp]) {
            if (!featureSupportedField.booleanSafe()) {
                return {ErrorCodes::Error(563070),
                        str::stream()
                            << "Illegal value for "
                            << StartChunkCloneRequest::kSupportsCriticalSectionDuringCatchUp};
            }
            supportsCriticalSectionDuringCatchUp = true;
        }

        // 优化的关键区域准入检查：
        // 条件：接收端状态为 "steady" 或 "catchup"，且支持追赶阶段关键区域特性
        // 优势：允许在接收端仍在追赶时就进入关键区域，减少整体迁移时间
        if ((res["state"].String() == "steady" || res["state"].String() == "catchup") &&
            sessionCatalogSourceInCatchupPhase && supportsCriticalSectionDuringCatchUp) {
            // 未传输数据百分比计算：
            // 功能：计算剩余未传输数据占最大块大小的百分比
            // 目的：判断是否达到可以进入关键区域的阈值
            auto estimatedUntransferredChunkPercentage =
                (std::min(_args.getMaxChunkSizeBytes(), untransferredModsSizeBytes) * 100) /
                _args.getMaxChunkSizeBytes();
            
            // 会话数据大小限制计算：
            // 功能：基于块大小计算允许的最大未传输会话数据量
            // 策略：根据块大小比例调整会话数据限制
            int64_t maxUntransferredSessionsSize = BSONObjMaxUserSize *
                _args.getMaxChunkSizeBytes() / ChunkSizeSettingsType::kDefaultMaxChunkSizeBytes;
                
            // 阈值检查：判断是否可以提前进入关键区域
            if (estimatedUntransferredChunkPercentage <
                    maxCatchUpPercentageBeforeBlockingWrites.load() &&
                estimateUntransferredSessionsSize < maxUntransferredSessionsSize) {
                // The recipient is sufficiently caught-up with the writes on the donor.
                // Block writes, so that it can drain everything.
                // 接收端已充分追赶源端的写入操作，可以阻塞写入以完成剩余数据传输
                LOGV2(5630700,
                      "moveChunk data transfer within threshold to allow write blocking",
                      "_untransferredUpsertsCounter"_attr = _untransferredUpsertsCounter,
                      "_untransferredDeletesCounter"_attr = _untransferredDeletesCounter,
                      "_deferredUntransferredOpsCounter"_attr = _deferredUntransferredOpsCounter,
                      "_averageObjectSizeForCloneRecordIds"_attr =
                          _averageObjectSizeForCloneRecordIds,
                      "_averageObjectIdSize"_attr = _averageObjectIdSize,
                      "untransferredModsSizeBytes"_attr = untransferredModsSizeBytes,
                      "untransferredSessionDataInBytes"_attr = estimateUntransferredSessionsSize,
                      "maxChunksSizeBytes"_attr = _args.getMaxChunkSizeBytes(),
                      "_sessionId"_attr = _sessionId.toString());
                return Status::OK();
            }
        }

        // 失败状态检查：处理接收端报告的迁移失败
        if (res["state"].String() == "fail") {
            return {ErrorCodes::OperationFailed,
                    str::stream() << "Data transfer error: " << res["errmsg"].str()};
        }

        // 迁移会话ID验证：
        // 功能：确保接收端返回的会话ID与当前迁移会话匹配
        // 安全性：防止多个并发迁移之间的干扰
        auto migrationSessionIdStatus = MigrationSessionId::extractFromBSON(res);
        if (!migrationSessionIdStatus.isOK()) {
            return {ErrorCodes::OperationIncomplete,
                    str::stream() << "Unable to retrieve the id of the migration session due to "
                                  << migrationSessionIdStatus.getStatus().toString()};
        }

        // 迁移参数一致性检查：
        // 功能：验证接收端返回的迁移参数与源端的参数是否一致
        // 检查项：命名空间、源分片、范围边界、会话ID等
        // 目的：检测并发迁移或迁移中止导致的状态不一致
        if (NamespaceStringUtil::deserialize(
                boost::none, res["ns"].str(), SerializationContext::stateDefault()) != nss() ||
            (res.hasField("fromShardId")
                 ? (res["fromShardId"].str() != _args.getFromShard().toString())
                 : (res["from"].str() != _donorConnStr.toString())) ||
            !res["min"].isABSONObj() || res["min"].Obj().woCompare(getMin()) != 0 ||
            !res["max"].isABSONObj() || res["max"].Obj().woCompare(getMax()) != 0 ||
            !_sessionId.matches(migrationSessionIdStatus.getValue())) {
            // This can happen when the destination aborted the migration and received another
            // recvChunk before this thread sees the transition to the abort state. This is
            // currently possible only if multiple migrations are happening at once. This is an
            // unfortunate consequence of the shards not being able to keep track of multiple
            // incoming and outgoing migrations.
            return {ErrorCodes::OperationIncomplete,
                    "Destination shard aborted migration because a new one is running"};
        }

        // 内存使用限制检查：
        // 功能：监控源端内存使用情况，防止内存溢出
        // 条件：非手动强制巨型块迁移 且 (内存使用超过500MB 或 测试故障点触发)
        // 保护：确保迁移过程不会消耗过多系统资源
        if (_args.getForceJumbo() != ForceJumbo::kForceManual &&
            (_memoryUsed > 500 * 1024 * 1024 ||
             (_jumboChunkCloneState && MONGO_unlikely(failTooMuchMemoryUsed.shouldFail())))) {
            // This is too much memory for us to use so we're going to abort the migration
            return {ErrorCodes::ExceededMemoryLimit,
                    "Aborting migration because of high memory usage"};
        }

        // 中断检查：响应操作上下文的中断请求
        Status interruptStatus = opCtx->checkForInterruptNoAssert();
        if (!interruptStatus.isOK()) {
            return interruptStatus;
        }
    }

    // 超时处理：在指定时间内未达到准入条件
    return {ErrorCodes::ExceededTimeLimit, "Timed out waiting for the cloner to catch up"};
}

/**
 * MigrationChunkClonerSource::nextSessionMigrationBatch 函数的作用：
 * 获取下一批会话迁移相关的oplog条目，用于在chunk迁移过程中传输会话和事务数据。
 * 
 * 核心功能：
 * 1. 会话oplog批次收集：从会话目录源获取需要迁移的会话相关oplog条目
 * 2. 批次大小控制：动态控制单次传输的oplog数据量，避免超出BSON大小限制
 * 3. 写关注管理：标识需要等待大多数节点确认的关键oplog条目
 * 4. 迭代式数据获取：持续获取oplog直到没有更多数据或达到批次大小限制
 * 5. 内存优化：考虑BSONArray索引开销，精确控制传输数据量
 * 
 * 使用场景：
 * - 在chunk迁移过程中传输会话相关的oplog数据
 * - 确保可重试写操作和事务的会话状态正确迁移
 * - 支持MongoDB多文档事务在分片环境中的数据一致性
 * 
 * 数据类型：
 * - 可重试写操作的oplog条目：确保写操作的幂等性
 * - 事务相关的oplog条目：保证事务操作的原子性
 * - 会话生命周期事件：维护会话状态的连续性
 * 
 * 性能优化：
 * - 批量传输：减少网络往返次数，提高传输效率
 * - 大小控制：避免单次传输过大导致的内存问题
 * - 懒加载：按需获取oplog，避免不必要的资源消耗
 * - 状态缓存：利用会话目录源的内部缓存机制
 * 
 * 写关注策略：
 * - 条件性等待：只对需要强一致性的oplog等待大多数确认
 * - 性能平衡：在数据一致性和传输性能之间找到最佳平衡
 * - 时间戳管理：返回需要等待写关注的最新oplog时间戳
 * 
 * 错误处理：
 * - 大小超限保护：防止单个批次超出BSON最大用户大小
 * - 数据完整性：确保oplog条目的完整性和有序性
 * - 状态一致性：维护会话目录源的状态一致性
 * 
 * 该函数与fetchNextSessionMigrationBatch配合工作，是会话数据迁移的核心实现。
 * 
 * 参数说明：
 * @param opCtx 操作上下文，提供事务和中断支持
 * @param arrBuilder BSON数组构建器，用于收集oplog条目
 * 
 * 返回值：
 * @return boost::optional<repl::OpTime> 需要等待写关注的最新oplog时间戳，如果不需要等待则为空
 */
boost::optional<repl::OpTime> MigrationChunkClonerSource::nextSessionMigrationBatch(
    OperationContext* opCtx, BSONArrayBuilder* arrBuilder) {
    // 写关注时间戳初始化：
    // 功能：存储需要等待大多数节点确认的最新oplog操作时间戳
    // 用途：在批次传输完成后，调用方可以使用此时间戳等待写关注
    // 策略：只有标记为需要等待大多数确认的oplog才会更新此时间戳
    repl::OpTime opTimeToWaitIfWaitingForMajority;
    
    // chunk范围定义：
    // 功能：创建当前迁移chunk的范围对象
    // 用途：用于验证oplog条目是否属于当前迁移的chunk范围
    // 组成：包含最小边界和最大边界的范围定义
    const ChunkRange range(getMin(), getMax());

    // 主oplog获取循环：
    // 条件：持续循环直到会话目录源没有更多oplog可获取
    // 目的：收集当前批次的所有可用会话oplog条目
    while (_sessionCatalogSource->hasMoreOplog()) {
        // 获取最后获取的oplog结果：
        // 功能：从会话目录源获取上次fetchNextOplog()调用的结果
        // 内容：包含oplog条目、是否需要等待大多数确认等信息
        // 缓存机制：会话目录源内部维护获取结果的缓存
        auto result = _sessionCatalogSource->getLastFetchedOplog();

        // oplog有效性检查：
        // 条件：如果获取的结果中没有有效的oplog条目
        // 原因：可能是内部缓存为空或者需要继续获取下一个oplog
        // 处理：触发下一次oplog获取并继续循环
        if (!result.oplog) {
            // 获取下一个oplog：
            // 功能：从oplog集合中获取下一个需要迁移的会话相关条目
            // 状态更新：更新会话目录源的内部状态和缓存
            // 过滤：只获取与当前chunk范围相关的oplog条目
            _sessionCatalogSource->fetchNextOplog(opCtx);
            continue;  // 继续下一次循环尝试
        }

        // oplog时间戳提取：
        // 功能：从oplog条目中提取操作时间戳
        // 用途：用于后续的写关注等待和时间戳比较
        // 重要性：确保oplog条目按时间戳顺序处理
        auto newOpTime = result.oplog->getOpTime();
        
        // oplog文档序列化：
        // 功能：将oplog条目转换为BSON文档格式
        // 目的：准备用于网络传输的序列化数据
        // 格式：标准的oplog条目BSON格式，包含所有必要字段
        auto oplogDoc = result.oplog->getEntry().toBSON();

        // Use the builder size instead of accumulating the document sizes directly so that we
        // take into consideration the overhead of BSONArray indices.
        // BSON大小限制检查：
        // 策略：使用构建器大小而不是直接累积文档大小，以考虑BSONArray索引的开销
        // 条件1：arrBuilder->arrSize() - 确保数组中已有oplog条目（保证有进展）
        // 条件2：大小检查 - 当前长度 + oplog文档大小 + 1024字节缓冲 > BSON最大用户大小
        // 保护：防止单个批次超出BSON大小限制，确保网络传输的可靠性
        if (arrBuilder->arrSize() &&
            (arrBuilder->len() + oplogDoc.objsize() + 1024) > BSONObjMaxUserSize) {
            // 批次大小超限处理：
            // 操作：退出循环，结束当前批次的oplog收集
            // 结果：当前oplog条目将在下次调用时被处理
            // 优势：避免传输过大的批次，优化网络性能
            break;
        }

        // oplog条目添加到批次：
        // 操作：将序列化的oplog文档添加到BSON数组构建器中
        // 结果：oplog条目将包含在返回给目标分片的批次中
        // 顺序：按照从会话目录源获取的顺序添加，保持时间戳顺序
        arrBuilder->append(oplogDoc);

        // 获取下一个oplog：
        // 功能：推进会话目录源到下一个oplog条目
        // 状态更新：更新内部游标位置和缓存状态
        // 准备：为下次循环迭代准备下一个oplog条目
        _sessionCatalogSource->fetchNextOplog(opCtx);

        // 写关注需求检查：
        // 条件：如果当前oplog条目需要等待大多数节点确认
        // 用途：确保关键的会话操作在大多数节点上持久化
        // 例子：事务提交操作、重要的会话状态变更等
        if (result.shouldWaitForMajority) {
            // 写关注时间戳更新：
            // 条件：如果当前oplog的时间戳比已记录的时间戳更新
            // 操作：更新需要等待写关注的最新时间戳
            // 结果：调用方将等待此时间戳对应的操作在大多数节点上确认
            if (opTimeToWaitIfWaitingForMajority < newOpTime) {
                opTimeToWaitIfWaitingForMajority = newOpTime;
            }
        }
    }

    // 返回写关注时间戳：
    // 内容：如果批次中包含需要等待大多数确认的oplog，返回最新的时间戳
    // 用途：调用方可以使用此时间戳调用waitForWriteConcern()等待确认
    // 优化：如果所有oplog都不需要等待大多数，返回空值以避免不必要的等待
    return boost::make_optional(opTimeToWaitIfWaitingForMajority);
}

std::shared_ptr<Notification<bool>>
MigrationChunkClonerSource::getNotificationForNextSessionMigrationBatch() {
    return _sessionCatalogSource->getNotificationForNewOplog();
}

boost::optional<long long>
MigrationChunkClonerSource::getSessionOplogEntriesSkippedSoFarLowerBound() {
    return _sessionCatalogSource->getSessionOplogEntriesSkippedSoFarLowerBound();
}

boost::optional<long long> MigrationChunkClonerSource::getSessionOplogEntriesToBeMigratedSoFar() {
    return _sessionCatalogSource->getSessionOplogEntriesToBeMigratedSoFar();
}

MigrationChunkClonerSource::CloneList::DocumentInFlightWithLock::DocumentInFlightWithLock(
    WithLock lock, MigrationChunkClonerSource::CloneList& clonerList)
    : _inProgressReadToken(
          std::make_unique<MigrationChunkClonerSource::CloneList::InProgressReadToken>(
              lock, clonerList)) {}

void MigrationChunkClonerSource::CloneList::DocumentInFlightWithLock::setDoc(
    boost::optional<Snapshotted<BSONObj>> doc) {
    _doc = std::move(doc);
}

std::unique_ptr<MigrationChunkClonerSource::CloneList::DocumentInFlightWhileNotInLock>
MigrationChunkClonerSource::CloneList::DocumentInFlightWithLock::release() {
    invariant(_inProgressReadToken);

    return std::make_unique<MigrationChunkClonerSource::CloneList::DocumentInFlightWhileNotInLock>(
        std::move(_inProgressReadToken), std::move(_doc));
}

MigrationChunkClonerSource::CloneList::DocumentInFlightWhileNotInLock::
    DocumentInFlightWhileNotInLock(
        std::unique_ptr<CloneList::InProgressReadToken> inProgressReadToken,
        boost::optional<Snapshotted<BSONObj>> doc)
    : _inProgressReadToken(std::move(inProgressReadToken)), _doc(std::move(doc)) {}

void MigrationChunkClonerSource::CloneList::DocumentInFlightWhileNotInLock::setDoc(
    boost::optional<Snapshotted<BSONObj>> doc) {
    _doc = std::move(doc);
}

const boost::optional<Snapshotted<BSONObj>>&
MigrationChunkClonerSource::CloneList::DocumentInFlightWhileNotInLock::getDoc() {
    return _doc;
}

MigrationChunkClonerSource::CloneList::InProgressReadToken::InProgressReadToken(
    WithLock withLock, CloneList& cloneList)
    : _cloneList(cloneList) {
    _cloneList._startedOneInProgressRead(withLock);
}

MigrationChunkClonerSource::CloneList::InProgressReadToken::~InProgressReadToken() {
    _cloneList._finishedOneInProgressRead();
}

MigrationChunkClonerSource::CloneList::CloneList() {
    _recordIdsIter = _recordIds.begin();
}

void MigrationChunkClonerSource::CloneList::populateList(RecordIdSet recordIds) {
    stdx::lock_guard lk(_mutex);
    _recordIds = std::move(recordIds);
    _recordIdsIter = _recordIds.begin();
}

void MigrationChunkClonerSource::CloneList::insertOverflowDoc(Snapshotted<BSONObj> doc) {
    stdx::lock_guard lk(_mutex);
    invariant(_inProgressReads >= 1);
    _overflowDocs.push_back(std::move(doc));
}

bool MigrationChunkClonerSource::CloneList::hasMore() const {
    stdx::lock_guard lk(_mutex);
    return _recordIdsIter != _recordIds.cend() && _inProgressReads > 0;
}


/**
 * MigrationChunkClonerSource::CloneList::getNextDoc 函数的作用：
 * 从克隆列表中安全地获取下一个需要迁移的文档，是chunk迁移过程中文档访问的核心函数。
 * 
 * 核心功能：
 * 1. 文档访问协调：在多线程环境下安全地从记录ID集合或溢出队列中获取文档
 * 2. 溢出文档优先级：优先处理溢出队列中的文档，确保数据传输的连续性
 * 3. 记录ID迭代：按顺序遍历预扫描的记录ID集合，进行文档的随机访问
 * 4. 文档存在性验证：处理记录ID对应的文档在扫描后被删除的情况
 * 5. 并发读取管理：通过读取令牌机制管理正在进行的读取操作数量
 * 6. 完成状态检测：检测所有文档是否已被处理完毕
 * 
 * 工作流程：
 * - 等待可用文档：使用条件变量等待溢出文档、记录ID或读取完成
 * - 溢出文档优先：如果存在溢出文档，优先返回溢出队列中的文档
 * - 记录ID处理：从记录ID迭代器获取下一个记录ID并进行文档查找
 * - 文档查找：根据记录ID在集合中查找对应的文档数据
 * - 缺失处理：统计并处理记录ID对应文档已不存在的情况
 * 
 * 并发安全机制：
 * - 互斥锁保护：保护内部数据结构的并发访问
 * - 读取令牌：通过RAII模式管理正在进行的读取操作计数
 * - 条件变量：协调多线程之间的等待和通知
 * - 原子操作：确保状态变更的原子性
 * 
 * 性能优化特性：
 * - 溢出队列：支持文档溢出机制，优化批次传输的大小控制
 * - 懒加载：按需加载文档，避免不必要的内存占用
 * - 预取优化：基于预扫描的记录ID，减少重复索引扫描
 * - 快照隔离：返回快照版本的文档，确保读取一致性
 * 
 * 错误处理：
 * - 文档缺失：处理记录ID对应文档已被删除的情况
 * - 中断支持：响应操作上下文的中断请求
 * - 资源清理：通过RAII机制确保资源的正确清理
 * 
 * 该函数是_nextCloneBatchFromCloneRecordIds的核心依赖，负责实际的文档访问和并发控制。
 * 
 * 多线程执行时序
时间点T1：三个线程同时调用getNextDoc()

Fetcher线程A：
lk.lock();                          // 获取锁
RecordId nextId = *_recordIdsIter;  // nextId = RecordId(1)
++_recordIdsIter;                   // 迭代器现在指向RecordId(2)
lk.unlock();                        // 释放锁
// 去获取RecordId(1)对应的文档

Fetcher线程B（紧接着获取锁）：
lk.lock();                          // 获取锁  
RecordId nextId = *_recordIdsIter;  // nextId = RecordId(2)
++_recordIdsIter;                   // 迭代器现在指向RecordId(3)
lk.unlock();                        // 释放锁
// 去获取RecordId(2)对应的文档


Fetcher线程C（最后获取锁）：
lk.lock();                          // 获取锁
RecordId nextId = *_recordIdsIter;  // nextId = RecordId(3)  
++_recordIdsIter;                   // 迭代器现在指向RecordId(4)
lk.unlock();                        // 释放锁
// 去获取RecordId(3)对应的文档

由于互斥锁的保护，每个线程获取的RecordId都是唯一的，绝对不会重复。

MigrationChunkClonerSource::_nextCloneBatchFromCloneRecordIds 调用
 */
std::unique_ptr<MigrationChunkClonerSource::CloneList::DocumentInFlightWhileNotInLock>
MigrationChunkClonerSource::CloneList::getNextDoc(OperationContext* opCtx,
                                                  const CollectionPtr& collection,
                                                  int* numRecordsNoLongerExist) {
    // 主文档获取循环：持续尝试获取文档直到成功或无更多文档
    while (true) {
        // 获取互斥锁：保护对内部数据结构的并发访问
        // 包括：_recordIdsIter, _overflowDocs, _inProgressReads 等共享状态
        stdx::unique_lock lk(_mutex);
        
        // 并发读取计数验证：确保正在进行的读取操作数量非负
        // _inProgressReads 跟踪当前有多少个线程正在执行文档读取操作
        invariant(_inProgressReads >= 0);
        
        // 记录ID变量声明：用于存储从迭代器获取的下一个记录ID
        RecordId nextRecordId;

        // 等待可用文档条件：
        // 使用条件变量等待以下任一条件满足：
        // 1. _recordIdsIter != _recordIds.end() - 记录ID迭代器未达到末尾
        // 2. !_overflowDocs.empty() - 溢出文档队列非空
        // 3. _inProgressReads == 0 - 没有正在进行的读取操作（表示处理完成）
        // 支持中断：如果操作上下文被中断，会抛出异常
        opCtx->waitForConditionOrInterrupt(_moreDocsCV, lk, [&]() {
            return _recordIdsIter != _recordIds.end() || !_overflowDocs.empty() ||
                _inProgressReads == 0;
        });

        // 创建文档飞行对象（带锁版本）：
        // 功能：创建一个管理正在处理文档的RAII对象
        // 作用：自动增加_inProgressReads计数，确保并发读取状态的正确跟踪
        // 设计：使用RAII模式确保即使异常也能正确清理计数
        DocumentInFlightWithLock docInFlight(lk, *this);

        // One of the following must now be true (corresponding to the three if conditions):
        //   1.  There is a document in the overflow set
        //   2.  The iterator has not reached the end of the record id set
        //   3.  The overflow set is empty, the iterator is at the end, and
        //       no threads are holding a document.  This condition indicates
        //       that there are no more docs to return for the cloning phase.
        
        // 溢出文档优先处理分支：
        // 条件：溢出队列中存在文档
        // 策略：优先处理溢出文档，确保之前因批次大小限制而延迟的文档能被及时处理
        // 优势：维护文档处理的顺序性，避免数据丢失
        if (!_overflowDocs.empty()) {
            // 设置溢出文档：从溢出队列头部取出文档并设置到飞行对象中
            // std::move：转移所有权，避免不必要的拷贝开销
            docInFlight.setDoc(std::move(_overflowDocs.front()));
            
            // 移除已处理的溢出文档：从队列头部删除已取出的文档
            _overflowDocs.pop_front();
            
            // 释放并返回：将带锁的飞行对象转换为无锁版本并返回
            // release()：转移所有权，允许在锁外访问文档内容
            return docInFlight.release();
            
        } else if (_recordIdsIter != _recordIds.end()) {
            // 记录ID处理分支：
            // 条件：记录ID迭代器尚未到达末尾
            // 操作：获取下一个记录ID并推进迭代器
            
            // 获取当前记录ID：从迭代器当前位置获取记录ID
            nextRecordId = *_recordIdsIter;
            
            // 推进迭代器：移动到下一个记录ID位置
            // 这确保了每个记录ID只被处理一次
            ++_recordIdsIter;
            
        } else {
            // 完成处理分支：
            // 条件：溢出队列为空 且 记录ID迭代器已到末尾 且 没有正在进行的读取
            // 含义：所有文档都已处理完毕，克隆阶段结束
            // 返回：空的飞行对象，表示没有更多文档可处理
            return docInFlight.release();
        }

        // 释放互斥锁：在进行实际的文档读取前释放锁
        // 目的：避免在IO操作期间持有锁，提高并发性能
        // 安全性：记录ID已获取，后续操作不需要锁保护
        lk.unlock();

        // 转换飞行对象：将带锁版本转换为无锁版本
        // 原因：后续的文档查找操作不需要持有克隆列表的锁
        // 优势：允许其他线程并发访问克隆列表
        auto docInFlightWhileNotLocked = docInFlight.release();

        // 文档查找操作：
        // 功能：根据记录ID在集合中查找对应的文档
        // 参数1：操作上下文，提供事务和中断支持
        // 参数2：记录ID，文档的物理位置标识符
        // 参数3：文档输出参数，包含快照版本的文档数据
        Snapshotted<BSONObj> doc;
        if (collection->findDoc(opCtx, nextRecordId, &doc)) {
            // 文档查找成功分支：
            // 操作：将找到的文档设置到飞行对象中
            // std::move：转移文档所有权，避免拷贝开销
            docInFlightWhileNotLocked->setDoc(std::move(doc));
            
            // 返回包含文档的飞行对象：
            // 结果：调用方可以访问文档内容进行后续处理
            return docInFlightWhileNotLocked;
        }

        // 文档不存在处理：
        // 原因：记录ID对应的文档在扫描后被删除或移动
        // 统计：如果提供了计数器指针，增加不存在记录的计数
        // 继续：继续循环尝试获取下一个文档
        if (numRecordsNoLongerExist) {
            (*numRecordsNoLongerExist)++;
        }
        
        // 继续循环：文档不存在时，继续尝试处理下一个记录ID或溢出文档
        // 这确保了即使部分文档被删除，迁移过程仍能继续进行
    }
}

/**
 * 返回克隆列表中记录ID的数量，也就是一个 chunk 中的文档数量。 配合 MigrationChunkClonerSource::_storeCurrentRecordId 阅读
 */
size_t MigrationChunkClonerSource::CloneList::size() const {
    stdx::unique_lock lk(_mutex);
    return _recordIds.size();
}

void MigrationChunkClonerSource::CloneList::_startedOneInProgressRead(WithLock) {
    _inProgressReads++;
}

void MigrationChunkClonerSource::CloneList::_finishedOneInProgressRead() {
    stdx::lock_guard lk(_mutex);
    _inProgressReads--;
    _moreDocsCV.notify_one();
}

LogInsertForShardingHandler::LogInsertForShardingHandler(NamespaceString nss,
                                                         BSONObj doc,
                                                         repl::OpTime opTime)
    : _nss(std::move(nss)), _doc(doc.getOwned()), _opTime(std::move(opTime)) {}

void LogInsertForShardingHandler::commit(OperationContext* opCtx, boost::optional<Timestamp>) {
    // TODO (SERVER-71444): Fix to be interruptible or document exception.
    UninterruptibleLockGuard noInterrupt(opCtx);  // NOLINT.
    const auto scopedCss =
        CollectionShardingRuntime::assertCollectionLockedAndAcquireShared(opCtx, _nss);

    if (auto cloner = MigrationSourceManager::getCurrentCloner(*scopedCss)) {
        cloner->onInsertOp(opCtx, _doc, _opTime);
    }
}

LogUpdateForShardingHandler::LogUpdateForShardingHandler(NamespaceString nss,
                                                         boost::optional<BSONObj> preImageDoc,
                                                         BSONObj postImageDoc,
                                                         repl::OpTime opTime)
    : _nss(std::move(nss)),
      _preImageDoc(preImageDoc ? preImageDoc->getOwned() : boost::optional<BSONObj>(boost::none)),
      _postImageDoc(postImageDoc.getOwned()),
      _opTime(std::move(opTime)) {}

void LogUpdateForShardingHandler::commit(OperationContext* opCtx, boost::optional<Timestamp>) {
    // TODO (SERVER-71444): Fix to be interruptible or document exception.
    UninterruptibleLockGuard noInterrupt(opCtx);  // NOLINT.
    const auto scopedCss =
        CollectionShardingRuntime::assertCollectionLockedAndAcquireShared(opCtx, _nss);

    if (auto cloner = MigrationSourceManager::getCurrentCloner(*scopedCss)) {
        cloner->onUpdateOp(opCtx, _preImageDoc, _postImageDoc, _opTime);
    }
}

LogDeleteForShardingHandler::LogDeleteForShardingHandler(NamespaceString nss,
                                                         DocumentKey documentKey,
                                                         repl::OpTime opTime)
    : _nss(std::move(nss)), _documentKey(std::move(documentKey)), _opTime(std::move(opTime)) {}

void LogDeleteForShardingHandler::commit(OperationContext* opCtx, boost::optional<Timestamp>) {
    // TODO (SERVER-71444): Fix to be interruptible or document exception.
    UninterruptibleLockGuard noInterrupt(opCtx);  // NOLINT.
    const auto scopedCss =
        CollectionShardingRuntime::assertCollectionLockedAndAcquireShared(opCtx, _nss);

    if (auto cloner = MigrationSourceManager::getCurrentCloner(*scopedCss)) {
        cloner->onDeleteOp(opCtx, _documentKey, _opTime);
    }
}

LogRetryableApplyOpsForShardingHandler::LogRetryableApplyOpsForShardingHandler(
    std::vector<NamespaceString> namespaces, std::vector<repl::OpTime> opTimes)
    : _namespaces(std::move(namespaces)), _opTimes(std::move(opTimes)) {}

void LogRetryableApplyOpsForShardingHandler::commit(OperationContext* opCtx,
                                                    boost::optional<Timestamp>) {
    for (const auto& nss : _namespaces) {
        // TODO (SERVER-71444): Fix to be interruptible or document exception.
        UninterruptibleLockGuard noInterrupt(opCtx);  // NOLINT.
        const auto scopedCss =
            CollectionShardingRuntime::assertCollectionLockedAndAcquireShared(opCtx, nss);

        auto cloner = MigrationSourceManager::getCurrentCloner(*scopedCss);
        if (cloner) {
            for (const auto& opTime : _opTimes) {
                cloner->_addToSessionMigrationOptimeQueue(
                    opTime, SessionCatalogMigrationSource::EntryAtOpTimeType::kRetryableWrite);
            }
        }
    }
}

}  // namespace mongo
