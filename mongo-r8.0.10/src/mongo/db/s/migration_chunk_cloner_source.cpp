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

BSONObj createRequestWithSessionId(StringData commandName,
                                   const NamespaceString& nss,
                                   const MigrationSessionId& sessionId,
                                   bool waitForSteadyOrDone = false) {
    BSONObjBuilder builder;
    builder.append(commandName,
                   NamespaceStringUtil::serialize(nss, SerializationContext::stateDefault()));
    builder.append("waitForSteadyOrDone", waitForSteadyOrDone);
    sessionId.append(&builder);
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
            return storeCurrentRecordIdStatus;
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

    // 构建 startChunkClone 命令：
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

Status MigrationChunkClonerSource::awaitUntilCriticalSectionIsAppropriate(
    OperationContext* opCtx, Milliseconds maxTimeToWait) {
    invariant(_state == kCloning);
    invariant(!shard_role_details::getLocker(opCtx)->isLocked());
    // If this migration is manual migration that specified "force", enter the critical section
    // immediately. This means the entire cloning phase will be done under the critical section.
    if (_jumboChunkCloneState && _args.getForceJumbo() == ForceJumbo::kForceManual) {
        return Status::OK();
    }

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

void MigrationChunkClonerSource::_nextCloneBatchFromIndexScan(OperationContext* opCtx,
                                                              const CollectionPtr& collection,
                                                              BSONArrayBuilder* arrBuilder) {
    ElapsedTracker tracker(opCtx->getServiceContext()->getFastClockSource(),
                           internalQueryExecYieldIterations.load(),
                           Milliseconds(internalQueryExecYieldPeriodMS.load()));

    if (!_jumboChunkCloneState->clonerExec) {
        auto exec = uassertStatusOK(_getIndexScanExecutor(
            opCtx, collection, InternalPlanner::IndexScanOptions::IXSCAN_FETCH));
        _jumboChunkCloneState->clonerExec = std::move(exec);
    } else {
        _jumboChunkCloneState->clonerExec->reattachToOperationContext(opCtx);
        _jumboChunkCloneState->clonerExec->restoreState(&collection);
    }

    PlanExecutor::ExecState execState;
    try {
        BSONObj obj;
        RecordId recordId;
        while (PlanExecutor::ADVANCED ==
               (execState = _jumboChunkCloneState->clonerExec->getNext(&obj, nullptr))) {

            stdx::unique_lock<Latch> lk(_mutex);
            _jumboChunkCloneState->clonerState = execState;
            lk.unlock();

            opCtx->checkForInterrupt();

            // Use the builder size instead of accumulating the document sizes directly so
            // that we take into consideration the overhead of BSONArray indices.
            if (arrBuilder->arrSize() &&
                (arrBuilder->len() + obj.objsize() + 1024) > BSONObjMaxUserSize) {
                _jumboChunkCloneState->clonerExec->stashResult(obj);
                break;
            }

            arrBuilder->append(obj);

            lk.lock();
            _jumboChunkCloneState->docsCloned++;
            lk.unlock();

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

void MigrationChunkClonerSource::_nextCloneBatchFromCloneRecordIds(OperationContext* opCtx,
                                                                   const CollectionPtr& collection,
                                                                   BSONArrayBuilder* arrBuilder) {
    ElapsedTracker tracker(opCtx->getServiceContext()->getFastClockSource(),
                           internalQueryExecYieldIterations.load(),
                           Milliseconds(internalQueryExecYieldPeriodMS.load()));

    while (true) {
        int recordsNoLongerExist = 0;
        auto docInFlight = _cloneList.getNextDoc(opCtx, collection, &recordsNoLongerExist);

        if (recordsNoLongerExist) {
            stdx::lock_guard lk(_mutex);
            _numRecordsPassedOver += recordsNoLongerExist;
        }

        const auto& doc = docInFlight->getDoc();
        if (!doc) {
            break;
        }

        // We must always make progress in this method by at least one document because empty
        // return indicates there is no more initial clone data.
        if (arrBuilder->arrSize() && tracker.intervalHasElapsed()) {
            _cloneList.insertOverflowDoc(*doc);
            break;
        }

        // Do not send documents that are no longer in the chunk range being moved. This can
        // happen when document shard key value of the document changed after the initial
        // index scan during cloning. This is needed because the destination is very
        // conservative in processing xferMod deletes and won't delete docs that are not in
        // the range of the chunk being migrated.
        if (!isDocInRange(
                doc->value(), _args.getMin().value(), _args.getMax().value(), _shardKeyPattern)) {
            {
                stdx::lock_guard lk(_mutex);
                _numRecordsPassedOver++;
            }
            continue;
        }

        // Use the builder size instead of accumulating the document sizes directly so
        // that we take into consideration the overhead of BSONArray indices.
        if (arrBuilder->arrSize() &&
            (arrBuilder->len() + doc->value().objsize() + 1024) > BSONObjMaxUserSize) {
            _cloneList.insertOverflowDoc(*doc);
            break;
        }

        {
            stdx::lock_guard lk(_mutex);
            _numRecordsCloned++;
        }

        arrBuilder->append(doc->value());
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

Status MigrationChunkClonerSource::nextCloneBatch(OperationContext* opCtx,
                                                  const CollectionPtr& collection,
                                                  BSONArrayBuilder* arrBuilder) {
    dassert(shard_role_details::getLocker(opCtx)->isCollectionLockedForMode(nss(), MODE_IS));

    // If this chunk is too large to store records in _cloneRecordIds and the command args specify
    // to attempt to move it, scan the collection directly.
    if (_jumboChunkCloneState && _forceJumbo) {
        try {
            _nextCloneBatchFromIndexScan(opCtx, collection, arrBuilder);
            return Status::OK();
        } catch (const DBException& ex) {
            return ex.toStatus();
        }
    }

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

Status MigrationChunkClonerSource::nextModsBatch(OperationContext* opCtx, BSONObjBuilder* builder) {
    dassert(shard_role_details::getLocker(opCtx)->isCollectionLockedForMode(nss(), MODE_IS));

    _processDeferredXferMods(opCtx);

    std::list<BSONObj> deleteList;
    std::list<BSONObj> updateList;

    {
        // All clone data must have been drained before starting to fetch the incremental changes.
        stdx::unique_lock<Latch> lk(_mutex);
        invariant(!_cloneList.hasMore());

        // The "snapshot" for delete and update list must be taken under a single lock. This is to
        // ensure that we will preserve the causal order of writes. Always consume the delete
        // buffer first, before the update buffer. If the delete is causally before the update to
        // the same doc, then there's no problem since we consume the delete buffer first. If the
        // delete is causally after, we will not be able to see the document when we attempt to
        // fetch it, so it's also ok.
        deleteList.splice(deleteList.cbegin(), _deleted);
        updateList.splice(updateList.cbegin(), _reload);
    }

    // It's important to abandon any open snapshots before processing updates so that we are sure
    // that our snapshot is at least as new as those updates. It's possible for a stale snapshot to
    // still be open from reads performed by _processDeferredXferMods(), above.
    shard_role_details::getRecoveryUnit(opCtx)->abandonSnapshot();

    BSONArrayBuilder arrDel(builder->subarrayStart("deleted"));
    auto noopFn = [](BSONObj idDoc, BSONObj* fullDoc) {
        *fullDoc = idDoc;
        return true;
    };
    long long totalDocSize = xferMods(&arrDel, &deleteList, 0, noopFn);
    arrDel.done();

    if (deleteList.empty()) {
        BSONArrayBuilder arrUpd(builder->subarrayStart("reload"));
        auto findByIdWrapper = [opCtx, this](BSONObj idDoc, BSONObj* fullDoc) {
            return Helpers::findById(opCtx, this->nss(), idDoc, *fullDoc);
        };
        totalDocSize = xferMods(&arrUpd, &updateList, totalDocSize, findByIdWrapper);
        arrUpd.done();
    }

    builder->append("size", totalDocSize);

    // Put back remaining ids we didn't consume
    stdx::unique_lock<Latch> lk(_mutex);
    _deleted.splice(_deleted.cbegin(), deleteList);
    _untransferredDeletesCounter = _deleted.size();
    _reload.splice(_reload.cbegin(), updateList);
    _untransferredUpsertsCounter = _reload.size();
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

StatusWith<std::unique_ptr<PlanExecutor, PlanExecutor::Deleter>>
MigrationChunkClonerSource::_getIndexScanExecutor(OperationContext* opCtx,
                                                  const CollectionPtr& collection,
                                                  InternalPlanner::IndexScanOptions scanOption) {
    // Allow multiKey based on the invariant that shard keys must be single-valued. Therefore, any
    // multi-key index prefixed by shard key cannot be multikey over the shard key fields.
    const auto shardKeyIdx = findShardKeyPrefixedIndex(opCtx,
                                                       collection,
                                                       _shardKeyPattern.toBSON(),
                                                       /*requireSingleKey=*/false);
    if (!shardKeyIdx) {
        return {ErrorCodes::IndexNotFound,
                str::stream() << "can't find index with prefix " << _shardKeyPattern.toBSON()
                              << " in storeCurrentRecordId for " << nss().toStringForErrorMsg()};
    }

    // Assume both min and max non-empty, append MinKey's to make them fit chosen index
    const KeyPattern kp(shardKeyIdx->keyPattern());

    BSONObj min = Helpers::toKeyFormat(kp.extendRangeBound(getMin(), false));
    BSONObj max = Helpers::toKeyFormat(kp.extendRangeBound(getMax(), false));

    // We can afford to yield here because any change to the base data that we might miss is already
    // being queued and will migrate in the 'transferMods' stage.
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

Status MigrationChunkClonerSource::_storeCurrentRecordId(OperationContext* opCtx) {
    AutoGetCollection collection(opCtx, nss(), MODE_IS);
    if (!collection) {
        return {ErrorCodes::NamespaceNotFound,
                str::stream() << "Collection " << nss().toStringForErrorMsg()
                              << " does not exist."};
    }

    auto swExec = _getIndexScanExecutor(
        opCtx, collection.getCollection(), InternalPlanner::IndexScanOptions::IXSCAN_DEFAULT);
    if (!swExec.isOK()) {
        return swExec.getStatus();
    }
    auto exec = std::move(swExec.getValue());

    // Use the average object size to estimate how many objects a full chunk would carry do that
    // while traversing the chunk's range using the sharding index, below there's a fair amount of
    // slack before we determine a chunk is too large because object sizes will vary.
    unsigned long long maxRecsWhenFull;
    long long avgRecSize;

    const long long totalRecs = collection->numRecords(opCtx);
    if (totalRecs > 0) {
        avgRecSize = collection->dataSize(opCtx) / totalRecs;
        // The calls to numRecords() and dataSize() are not atomic so it is possible that the data
        // size becomes smaller than the number of records between the two calls, which would result
        // in average record size of zero
        if (avgRecSize == 0) {
            avgRecSize = BSONObj::kMinBSONLength;
        }
        maxRecsWhenFull = std::max(_args.getMaxChunkSizeBytes() / avgRecSize, 1LL);
        maxRecsWhenFull = 2 * maxRecsWhenFull;  // pad some slack
    } else {
        avgRecSize = 0;
        maxRecsWhenFull = kMaxObjectPerChunk + 1;
    }

    // Do a full traversal of the chunk and don't stop even if we think it is a large chunk we want
    // the number of records to better report, in that case.
    bool isLargeChunk = false;
    unsigned long long recCount = 0;

    try {
        BSONObj obj;
        RecordId recordId;
        RecordIdSet recordIdSet;

        while (PlanExecutor::ADVANCED == exec->getNext(&obj, &recordId)) {
            Status interruptStatus = opCtx->checkForInterruptNoAssert();
            if (!interruptStatus.isOK()) {
                return interruptStatus;
            }

            if (!isLargeChunk) {
                recordIdSet.insert(recordId);
            }

            if (++recCount > maxRecsWhenFull) {
                isLargeChunk = true;

                if (_forceJumbo) {
                    recordIdSet.clear();
                    break;
                }
            }
        }

        _cloneList.populateList(std::move(recordIdSet));
    } catch (DBException& exception) {
        exception.addContext("Executor error while scanning for documents belonging to chunk");
        throw;
    }

    const uint64_t collectionAverageObjectSize = collection->averageObjectSize(opCtx);

    uint64_t averageObjectIdSize = 0;
    const uint64_t defaultObjectIdSize = OID::kOIDSize;

    // For clustered collection, an index on '_id' is not required.
    if (totalRecs > 0 && !collection->isClustered()) {
        const auto idIdx = collection->getIndexCatalog()->findIdIndex(opCtx);
        if (!idIdx || !idIdx->getEntry()) {
            return {ErrorCodes::IndexNotFound,
                    str::stream() << "can't find index '_id' in storeCurrentRecordId for "
                                  << nss().toStringForErrorMsg()};
        }

        averageObjectIdSize =
            idIdx->getEntry()->accessMethod()->getSpaceUsedBytes(opCtx) / totalRecs;
    }

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

    stdx::lock_guard<Latch> lk(_mutex);
    _averageObjectSizeForCloneRecordIds = collectionAverageObjectSize + defaultObjectIdSize;
    _averageObjectIdSize = std::max(averageObjectIdSize, defaultObjectIdSize);
    return Status::OK();
}

long long xferMods(BSONArrayBuilder* arr,
                   std::list<BSONObj>* modsList,
                   long long initialSize,
                   std::function<bool(BSONObj, BSONObj*)> extractDocToAppendFn) {
    const long long maxSize = BSONObjMaxUserSize;

    if (modsList->empty() || initialSize > maxSize) {
        return initialSize;
    }

    stdx::unordered_set<absl::string_view> addedSet;
    auto iter = modsList->begin();
    for (; iter != modsList->end(); ++iter) {
        auto idDoc = *iter;
        absl::string_view idDocView(idDoc.objdata(), idDoc.objsize());

        if (addedSet.find(idDocView) == addedSet.end()) {
            addedSet.insert(idDocView);
            BSONObj fullDoc;
            if (extractDocToAppendFn(idDoc, &fullDoc)) {
                if (arr->arrSize() &&
                    (arr->len() + fullDoc.objsize() + kFixedCommandOverhead) > maxSize) {
                    break;
                }
                arr->append(fullDoc);
            }
        }
    }

    long long totalSize = arr->len();
    modsList->erase(modsList->begin(), iter);

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
 */
Status MigrationChunkClonerSource::_checkRecipientCloningStatus(OperationContext* opCtx,
                                                                Milliseconds maxTimeToWait) {
    // 记录监控开始时间：用于超时控制
    const auto startTime = Date_t::now();
    int iteration = 0;
    
    // 主监控循环：持续检查直到满足条件或超时
    while ((Date_t::now() - startTime) < maxTimeToWait) {
        // 向接收端发送状态查询请求：
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

boost::optional<repl::OpTime> MigrationChunkClonerSource::nextSessionMigrationBatch(
    OperationContext* opCtx, BSONArrayBuilder* arrBuilder) {
    repl::OpTime opTimeToWaitIfWaitingForMajority;
    const ChunkRange range(getMin(), getMax());

    while (_sessionCatalogSource->hasMoreOplog()) {
        auto result = _sessionCatalogSource->getLastFetchedOplog();

        if (!result.oplog) {
            _sessionCatalogSource->fetchNextOplog(opCtx);
            continue;
        }

        auto newOpTime = result.oplog->getOpTime();
        auto oplogDoc = result.oplog->getEntry().toBSON();

        // Use the builder size instead of accumulating the document sizes directly so that we
        // take into consideration the overhead of BSONArray indices.
        if (arrBuilder->arrSize() &&
            (arrBuilder->len() + oplogDoc.objsize() + 1024) > BSONObjMaxUserSize) {
            break;
        }

        arrBuilder->append(oplogDoc);

        _sessionCatalogSource->fetchNextOplog(opCtx);

        if (result.shouldWaitForMajority) {
            if (opTimeToWaitIfWaitingForMajority < newOpTime) {
                opTimeToWaitIfWaitingForMajority = newOpTime;
            }
        }
    }

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

std::unique_ptr<MigrationChunkClonerSource::CloneList::DocumentInFlightWhileNotInLock>
MigrationChunkClonerSource::CloneList::getNextDoc(OperationContext* opCtx,
                                                  const CollectionPtr& collection,
                                                  int* numRecordsNoLongerExist) {
    while (true) {
        stdx::unique_lock lk(_mutex);
        invariant(_inProgressReads >= 0);
        RecordId nextRecordId;

        opCtx->waitForConditionOrInterrupt(_moreDocsCV, lk, [&]() {
            return _recordIdsIter != _recordIds.end() || !_overflowDocs.empty() ||
                _inProgressReads == 0;
        });

        DocumentInFlightWithLock docInFlight(lk, *this);

        // One of the following must now be true (corresponding to the three if conditions):
        //   1.  There is a document in the overflow set
        //   2.  The iterator has not reached the end of the record id set
        //   3.  The overflow set is empty, the iterator is at the end, and
        //       no threads are holding a document.  This condition indicates
        //       that there are no more docs to return for the cloning phase.
        if (!_overflowDocs.empty()) {
            docInFlight.setDoc(std::move(_overflowDocs.front()));
            _overflowDocs.pop_front();
            return docInFlight.release();
        } else if (_recordIdsIter != _recordIds.end()) {
            nextRecordId = *_recordIdsIter;
            ++_recordIdsIter;
        } else {
            return docInFlight.release();
        }

        lk.unlock();

        auto docInFlightWhileNotLocked = docInFlight.release();

        Snapshotted<BSONObj> doc;
        if (collection->findDoc(opCtx, nextRecordId, &doc)) {
            docInFlightWhileNotLocked->setDoc(std::move(doc));
            return docInFlightWhileNotLocked;
        }

        if (numRecordsNoLongerExist) {
            (*numRecordsNoLongerExist)++;
        }
    }
}

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
