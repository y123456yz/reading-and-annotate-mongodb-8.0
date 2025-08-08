/**
 *    Copyright (C) 2021-present MongoDB, Inc.
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

#include "mongo/db/s/balancer/balancer_commands_scheduler_impl.h"

#include <absl/container/node_hash_map.h>
#include <absl/meta/type_traits.h>
#include <boost/move/utility_core.hpp>
#include <boost/none.hpp>
#include <boost/optional/optional.hpp>
// IWYU pragma: no_include "cxxabi.h"
#include <mutex>
#include <type_traits>

#include "mongo/base/error_codes.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonelement.h"
#include "mongo/client/read_preference.h"
#include "mongo/client/remote_command_targeter.h"
#include "mongo/db/client.h"
#include "mongo/db/s/config/sharding_catalog_manager.h"
#include "mongo/db/s/sharding_util.h"
#include "mongo/db/shard_id.h"
#include "mongo/executor/remote_command_request.h"
#include "mongo/executor/task_executor.h"
#include "mongo/executor/task_executor_pool.h"
#include "mongo/idl/idl_parser.h"
#include "mongo/logv2/log.h"
#include "mongo/logv2/log_attr.h"
#include "mongo/logv2/log_component.h"
#include "mongo/logv2/redaction.h"
#include "mongo/platform/compiler.h"
#include "mongo/rpc/get_status_from_command_result.h"
#include "mongo/s/async_requests_sender.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/client/shard.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/grid.h"
#include "mongo/s/request_types/shardsvr_join_migrations_request_gen.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/scopeguard.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kSharding

namespace mongo {
namespace {

MONGO_FAIL_POINT_DEFINE(pauseSubmissionsFailPoint);

void waitForQuiescedCluster(OperationContext* opCtx) {
    const auto executor = Grid::get(opCtx)->getExecutorPool()->getFixedExecutor();
    ShardsvrJoinMigrations joinShardOnMigrationsRequest;
    joinShardOnMigrationsRequest.setDbName(DatabaseName::kAdmin);

    auto unquiescedShardIds = Grid::get(opCtx)->shardRegistry()->getAllShardIds(opCtx);

    const auto responses =
        sharding_util::sendCommandToShards(opCtx,
                                           DatabaseName::kAdmin,
                                           joinShardOnMigrationsRequest.toBSON({}),
                                           unquiescedShardIds,
                                           executor,
                                           false /*throwOnError*/);
    for (const auto& r : responses) {
        auto responseOutcome = r.swResponse.isOK()
            ? getStatusFromCommandResult(r.swResponse.getValue().data)
            : r.swResponse.getStatus();

        if (!responseOutcome.isOK()) {
            LOGV2_WARNING(6648001,
                          "Could not complete _ShardsvrJoinMigrations on shard",
                          "error"_attr = responseOutcome,
                          "shard"_attr = r.shardId);
        }
    }
}


Status processRemoteResponse(const executor::RemoteCommandResponse& remoteResponse) {
    if (!remoteResponse.status.isOK()) {
        return remoteResponse.status;
    }
    auto remoteStatus = getStatusFromCommandResult(remoteResponse.data);
    return remoteStatus.withContext("Command request failed on source shard.");
}

}  // namespace

const std::string MergeChunksCommandInfo::kCommandName = "mergeChunks";
const std::string MergeChunksCommandInfo::kBounds = "bounds";
const std::string MergeChunksCommandInfo::kShardName = "shardName";
const std::string MergeChunksCommandInfo::kEpoch = "epoch";
const std::string MergeChunksCommandInfo::kTimestamp = "timestamp";

const std::string DataSizeCommandInfo::kCommandName = "dataSize";
const std::string DataSizeCommandInfo::kKeyPattern = "keyPattern";
const std::string DataSizeCommandInfo::kMinValue = "min";
const std::string DataSizeCommandInfo::kMaxValue = "max";
const std::string DataSizeCommandInfo::kEstimatedValue = "estimate";
const std::string DataSizeCommandInfo::kMaxSizeValue = "maxSize";

BalancerCommandsSchedulerImpl::BalancerCommandsSchedulerImpl() {}

BalancerCommandsSchedulerImpl::~BalancerCommandsSchedulerImpl() {
    stop();
}

//Balancer::_mainThread()
//启动后台线程 _workerThread ，该线程负责异步处理和下发所有均衡相关命令（如 chunk 迁移、合并等）。
void BalancerCommandsSchedulerImpl::start(OperationContext* opCtx) {
    LOGV2(5847200, "Balancer command scheduler start requested");
    stdx::lock_guard<Latch> lg(_mutex);
    invariant(!_workerThreadHandle.joinable());
    if (!_executor) {
        _executor = std::make_unique<executor::ScopedTaskExecutor>(
            Grid::get(opCtx)->getExecutorPool()->getFixedExecutor());
    }
    _state = SchedulerState::Recovering;

    try {
        //在 Balancer 命令调度器（BalancerCommandsSchedulerImpl）启动时，确保整个分片集群处于“静默”或“安静”状态，即所有分片（shard）上没有正在进行的迁移操作（如 chunk migration）。
        waitForQuiescedCluster(opCtx);
    } catch (const DBException& e) {
        LOGV2_WARNING(
            6648002, "Could not join migration activity on shards", "error"_attr = redact(e));
    }

    LOGV2(6648003, "Balancer scheduler recovery complete. Switching to regular execution");
    _state = SchedulerState::Running;

    _workerThreadHandle = stdx::thread([this] { _workerThread(); });
}

void BalancerCommandsSchedulerImpl::stop() {
    LOGV2(5847201, "Balancer command scheduler stop requested");
    {
        stdx::lock_guard<Latch> lg(_mutex);
        if (_state == SchedulerState::Stopped) {
            return;
        }

        invariant(_workerThreadHandle.joinable());
        _state = SchedulerState::Stopping;
        _stateUpdatedCV.notify_all();
    }
    _workerThreadHandle.join();
}

void BalancerCommandsSchedulerImpl::disableBalancerForCollection(OperationContext* opCtx,
                                                                 const NamespaceString& nss) {
    auto commandInfo = std::make_shared<DisableBalancerCommandInfo>(nss, ShardId::kConfigServerId);

    _buildAndEnqueueNewRequest(opCtx, std::move(commandInfo))
        .then([](const executor::RemoteCommandResponse& remoteResponse) {
            return processRemoteResponse(remoteResponse);
        })
        .getAsync([](auto) {});
}

/**
 * BalancerCommandsSchedulerImpl::requestMoveRange 的作用：
 * 异步提交 chunk 迁移请求到均衡器命令调度器，实现非阻塞的 chunk 迁移操作。
 * 
 * 核心功能：
 * 1. 接收来自均衡器的 ShardsvrMoveRange 命令请求
 * 2. 封装迁移命令信息，包括写入关注点和客户端信息
 * 3. 将请求提交到内部工作线程队列进行异步处理
 * 4. 返回 SemiFuture 对象，供调用方异步等待迁移结果
 * 5. 处理远程命令响应，解析执行状态
 * 
 * 参数说明：
 * @param opCtx 操作上下文，包含请求的认证和会话信息
 * @param request ShardsvrMoveRange 命令对象，包含迁移的完整参数
 * @param secondaryThrottleWC 副本集写入关注点设置，控制写入节流
 * @param issuedByRemoteUser 标识请求是否来自远程用户（影响权限检查）
 * 
 * @return SemiFuture<void> 异步 Future 对象，调用方可等待迁移完成
 * 
 * 该函数是 chunk 迁移命令的统一入口，负责将同步的迁移请求转换为异步执行模式。
 * 请求提交 → 工作线程队列 → 异步执行 → 回调处理
 * _buildAndEnqueueNewRequest() → _workerThread() → _submit() → _applyCommandResponse()
 */
SemiFuture<void> BalancerCommandsSchedulerImpl::requestMoveRange(
    OperationContext* opCtx,
    const ShardsvrMoveRange& request,
    const WriteConcernOptions& secondaryThrottleWC,
    bool issuedByRemoteUser) {
    
    // 根据请求来源决定是否保存外部客户端信息
    // 如果是远程用户发起的请求，需要保存客户端上下文用于权限验证和审计
    auto externalClientInfo =
        issuedByRemoteUser ? boost::optional<ExternalClientInfo>(opCtx) : boost::none;

    // 创建 MoveRangeCommandInfo 封装完整的迁移命令信息
    // 包含：迁移请求参数、写入关注点配置、客户端信息等
    auto commandInfo = std::make_shared<MoveRangeCommandInfo>(
        request,                      // ShardsvrMoveRange 命令对象
        secondaryThrottleWC,          // 副本集写入节流配置
        std::move(externalClientInfo) // 外部客户端信息（可选）
    );

    // 构建并入队新的命令请求到内部工作线程， 最终任务交由 BalancerCommandsScheduler 线程执行
    // _buildAndEnqueueNewRequest 将请求加入队列，由 _workerThread 异步处理
    return _buildAndEnqueueNewRequest(opCtx, std::move(commandInfo))
        .then([](const executor::RemoteCommandResponse& remoteResponse) {
            // 处理远程命令执行结果的回调函数
            // processRemoteResponse 解析响应状态，提取错误信息
            return processRemoteResponse(remoteResponse);
        })
        .semi(); // 转换为 SemiFuture 类型返回
}

SemiFuture<void> BalancerCommandsSchedulerImpl::requestMergeChunks(OperationContext* opCtx,
                                                                   const NamespaceString& nss,
                                                                   const ShardId& shardId,
                                                                   const ChunkRange& chunkRange,
                                                                   const ChunkVersion& version) {

    auto commandInfo = std::make_shared<MergeChunksCommandInfo>(
        nss, shardId, chunkRange.getMin(), chunkRange.getMax(), version);

    return _buildAndEnqueueNewRequest(opCtx, std::move(commandInfo))
        .then([](const executor::RemoteCommandResponse& remoteResponse) {
            return processRemoteResponse(remoteResponse);
        })
        .semi();
}

SemiFuture<DataSizeResponse> BalancerCommandsSchedulerImpl::requestDataSize(
    OperationContext* opCtx,
    const NamespaceString& nss,
    const ShardId& shardId,
    const ChunkRange& chunkRange,
    const ShardVersion& version,
    const KeyPattern& keyPattern,
    bool estimatedValue,
    int64_t maxSize) {
    auto commandInfo = std::make_shared<DataSizeCommandInfo>(nss,
                                                             shardId,
                                                             keyPattern.toBSON(),
                                                             chunkRange.getMin(),
                                                             chunkRange.getMax(),
                                                             estimatedValue,
                                                             maxSize,
                                                             version);

    return _buildAndEnqueueNewRequest(opCtx, std::move(commandInfo))
        .then([](const executor::RemoteCommandResponse& remoteResponse)
                  -> StatusWith<DataSizeResponse> {
            auto responseStatus = processRemoteResponse(remoteResponse);
            if (!responseStatus.isOK()) {
                return responseStatus;
            }
            long long sizeBytes = remoteResponse.data["size"].number();
            long long numObjects = remoteResponse.data["numObjects"].number();
            bool maxSizeReached = remoteResponse.data["maxReached"].trueValue();
            return DataSizeResponse(sizeBytes, numObjects, maxSizeReached);
        })
        .semi();
}

SemiFuture<NumMergedChunks> BalancerCommandsSchedulerImpl::requestMergeAllChunksOnShard(
    OperationContext* opCtx, const NamespaceString& nss, const ShardId& shardId) {
    auto commandInfo = std::make_shared<MergeAllChunksOnShardCommandInfo>(nss, shardId);
    return _buildAndEnqueueNewRequest(opCtx, std::move(commandInfo))
        .then([](const executor::RemoteCommandResponse& remoteResponse)
                  -> StatusWith<NumMergedChunks> {
            auto responseStatus = processRemoteResponse(remoteResponse);
            if (!responseStatus.isOK()) {
                return responseStatus;
            }

            return MergeAllChunksOnShardResponse::parse(
                       IDLParserContext{"MergeAllChunksOnShardResponse"}, remoteResponse.data)
                .getNumMergedChunks();
        })
        .semi();
}

SemiFuture<void> BalancerCommandsSchedulerImpl::requestMoveCollection(
    OperationContext* opCtx,
    const NamespaceString& nss,
    const ShardId& toShardId,
    const ShardId& dbPrimaryShardId,
    const DatabaseVersion& dbVersion) {
    auto commandInfo =
        std::make_shared<MoveCollectionCommandInfo>(nss, toShardId, dbPrimaryShardId, dbVersion);

    return _buildAndEnqueueNewRequest(opCtx, std::move(commandInfo))
        .then([](const executor::RemoteCommandResponse& remoteResponse) {
            return processRemoteResponse(remoteResponse);
        })
        .semi();
}

/**
 * BalancerCommandsSchedulerImpl::_buildAndEnqueueNewRequest 的作用：
 * 构建新的命令请求并将其加入调度器的处理队列，是所有均衡命令进入异步执行流程的统一入口。
 * 
 * 核心功能：
 * 1. 为每个命令请求生成唯一的请求ID，用于跟踪和管理命令生命周期
 * 2. 封装命令信息为 RequestData 对象，包含执行所需的完整元数据
 * 3. 等待调度器就绪状态，确保在正确的时机接受新请求
 * 4. 将请求加入内部队列，由工作线程异步处理
 * 5. 返回 Future 对象，供调用方异步等待命令执行结果
 * 
 * 参数说明：
 * @param opCtx 操作上下文，提供请求的认证和会话信息
 * @param commandInfo 具体的命令信息对象（如 MoveRangeCommandInfo、MergeChunksCommandInfo 等）
 * 
 * @return Future<executor::RemoteCommandResponse> 异步Future对象，包含远程命令执行结果
 * 
 * 该函数实现了从同步接口到异步执行的转换，是调度器架构的关键枢纽：
 * 同步调用 → 请求入队 → 异步执行 → 结果回调
 */
Future<executor::RemoteCommandResponse> BalancerCommandsSchedulerImpl::_buildAndEnqueueNewRequest(
    OperationContext* opCtx, std::shared_ptr<CommandInfo>&& commandInfo) {
    
    // 为每个新请求生成唯一标识符，用于请求跟踪、日志记录和结果关联
    const auto newRequestId = UUID::gen();
    
    /*
    {
    "t": {"$date": "2025-01-15T14:30:25.123+08:00"},
    "s": "D2",
    "c": "SHARDING", 
    "id": 5847202,
    "svc": "S",
    "ctx": "Balancer",
    "msg": "Enqueuing new Balancer command request",
    "attr": {
        "reqId": {
        "uuid": {"$uuid": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"}
        },
        "command": "{ _shardsvrMoveRange: \"ecommerce.orders\", toShard: \"shard0002\", min: { customer_id: 100000 }, max: { customer_id: 200000 }, waitForDelete: false, epoch: ObjectId('65a1b2c3d4e5f67890abcdef'), collectionTimestamp: Timestamp(1705123825, 15), fromShard: \"shard0001\", maxChunkSizeBytes: 67108864, forceJumbo: false, secondaryThrottle: true, writeConcern: { w: \"majority\", wtimeout: 30000 } }",
        "recoveryDocRequired": false
    }
    }
    */
    //{"t":{"$date":"2025-07-29T19:47:59.205+08:00"},"s":"D2", "c":"SHARDING", "id":5847202, "svc":"S", "ctx":"Balancer","msg":"Enqueuing new Balancer command request","attr":{"reqId":{"uuid":{"$uuid":"3cc580e0-5cff-4fc3-b333-8397a5ae996f"}},"command":"{ _shardsvrMoveRange: \"benchmark.yyztest\", toShard: \"shard1ReplSet\", min: { _id: 122475263 }, waitForDelete: false, epoch: ObjectId('6888b23d9b404274eb601360'), collectionTimestamp: Timestamp(1753788989, 10), fromShard: \"shard3ReplSet\", maxChunkSizeBytes: 10485760, forceJumbo: 0, secondaryThrottle: false, writeConcern: { w: 1, wtimeout: 0 } }","recoveryDocRequired":false}}
    // 记录请求入队的详细信息，包括命令内容和是否需要恢复支持
    LOGV2_DEBUG(5847202,
                2,
                "Enqueuing new Balancer command request",
                "reqId"_attr = newRequestId,
                "command"_attr = redact(commandInfo->serialise().toString()),
                "recoveryDocRequired"_attr = commandInfo->requiresRecoveryOnCrash());

    // 创建 RequestData 对象封装请求信息
    // RequestData 包含：请求ID、命令信息、执行状态、结果Future等
    RequestData pendingRequest(newRequestId, std::move(commandInfo));

    // 获取互斥锁，确保对调度器状态和请求队列的线程安全访问
    stdx::unique_lock<Latch> ul(_mutex);
    
    // 等待调度器退出恢复状态，确保在稳定状态下接受新请求
    // 在恢复状态期间，调度器可能正在重建内部状态，不适合处理新请求
    _stateUpdatedCV.wait(ul, [this] { return _state != SchedulerState::Recovering; });
    
    // 获取请求的结果Future，调用方将通过此Future异步等待执行结果
    auto outcomeFuture = pendingRequest.getOutcomeFuture();
    
    // 将请求加入调度器的内部处理队列
    // _enqueueRequest 会根据当前状态决定是否接受请求或直接拒绝
    _enqueueRequest(ul, std::move(pendingRequest));
    
    // 返回Future对象，调用方可以：
    // 1. 立即返回，实现非阻塞调用
    // 2. 调用.get()等待结果  
    // 3. 添加.then()回调处理结果
    return outcomeFuture;
}

/**
 * BalancerCommandsSchedulerImpl::_enqueueRequest 的作用：
 * 将构建好的命令请求加入调度器的内部处理队列，根据调度器当前状态决定是否接受请求。
 * 
 * 核心功能：
 * 1. 检查调度器当前状态，只在运行或恢复状态下接受新请求
 * 2. 避免重复入队：检查请求ID是否已存在，防止重复处理
 * 3. 维护内部数据结构：更新请求映射表和待提交队列
 * 4. 通知工作线程：唤醒等待的工作线程处理新请求
 * 5. 拒绝无效请求：在停止状态下直接拒绝新请求并设置错误状态
 * 
 * 参数说明：
 * @param WithLock 编译时锁检查，确保调用时已持有互斥锁
 * @param request 要入队的请求数据对象，包含完整的命令信息
 * 
 * 该函数是调度器请求管理的核心，确保请求在正确的状态下被接受和处理。
 */
void BalancerCommandsSchedulerImpl::_enqueueRequest(WithLock, RequestData&& request) {
    // 获取请求的唯一标识符，用于后续跟踪和管理
    auto requestId = request.getId();
    
    // 检查调度器状态：只在恢复或运行状态下接受新请求
    if (_state == SchedulerState::Recovering || _state == SchedulerState::Running) {
        // A request with persisted recovery info may be enqueued more than once when received while
        // the node is transitioning from Stopped to Recovering; if this happens, just resolve as a
        // no-op.
        // 防止重复入队：在节点从停止状态转换到恢复状态期间，
        // 带有持久化恢复信息的请求可能被多次入队，此时需要跳过重复请求
        if (_requests.find(requestId) == _requests.end()) {
            // 将请求加入主要的请求映射表，使用移动语义避免拷贝
            // _requests 维护所有活跃请求的完整信息
            _requests.emplace(std::make_pair(requestId, std::move(request)));
            
            // 将请求ID加入待提交队列，供工作线程按顺序处理
            // _unsubmittedRequestIds 记录尚未提交执行的请求队列
            _unsubmittedRequestIds.push_back(requestId);
            
            // 通知等待的工作线程有新请求可处理
            // 唤醒在 _stateUpdatedCV.wait() 上等待的工作线程
            _stateUpdatedCV.notify_all();
        }
        // 如果请求ID已存在，则静默跳过（作为无操作处理）
        // 这种情况通常发生在系统恢复期间的重复请求
    } else {
        // 调度器处于停止或停止中状态，拒绝新请求
        // 直接设置请求结果为错误状态，通知调用方请求被拒绝
        request.setOutcome(Status(ErrorCodes::BalancerInterrupted,
                                  "Request rejected - balancer scheduler is stopped"));
        // 注意：被拒绝的请求不会加入 _requests 映射表，
        // 其 Future 会立即被设置为错误状态供调用方获取
    }
}

/**
 * BalancerCommandsSchedulerImpl::_submit 的作用：
 * 将均衡器命令请求提交到目标分片进行异步执行，是调度器与远程分片通信的核心接口。
 * 
 * 核心功能：
 * 1. 分片路由和发现：通过分片注册表获取目标分片信息和主机地址
 * 2. 远程命令构建：将抽象的命令信息序列化为具体的远程命令请求
 * 3. 异步执行调度：通过TaskExecutor将命令提交到目标分片的主节点
 * 4. 回调机制设置：注册命令完成后的回调处理函数
 * 5. 错误处理和状态管理：处理分片不可用、网络异常等各种失败情况
 * 
 * 执行流程：
 * 1. 分片解析：从分片注册表中查找目标分片
 * 2. 主机定位：获取分片主节点的网络地址
 * 3. 命令序列化：将命令对象转换为BSON格式
 * 4. 异步提交：通过TaskExecutor调度远程命令执行
 * 5. 结果处理：设置异步回调处理命令执行结果
 * 
 * 参数说明：
 * @param opCtx 操作上下文，包含认证、会话和超时信息
 * @param params 命令提交参数，包含请求ID和完整的命令信息
 * 
 * @return CommandSubmissionResult 提交结果，包含请求ID和提交状态
 * 
 * 该函数是调度器架构中连接逻辑命令和物理执行的桥梁，实现了：
 * 命令抽象 → 分片路由 → 远程调度 → 异步执行
 */
CommandSubmissionResult BalancerCommandsSchedulerImpl::_submit(
    OperationContext* opCtx, const CommandSubmissionParameters& params) {
    
    // 记录命令提交开始的调试信息，包含请求ID用于跟踪
    LOGV2_DEBUG(
        5847203, 2, "Balancer command request submitted for execution", "reqId"_attr = params.id);
    
    try {
        // ========== 第一步：分片发现和验证 ==========
        // 通过分片注册表获取目标分片对象
        // Grid::get(opCtx)->shardRegistry() 提供集群分片信息的统一访问接口
        const auto shardWithStatus =
            Grid::get(opCtx)->shardRegistry()->getShard(opCtx, params.commandInfo->getTarget());
        
        // 检查分片获取是否成功：分片可能不存在、已下线或配置错误
        if (!shardWithStatus.isOK()) {
            // 分片获取失败：返回包含错误状态的提交结果
            // 这种情况通常发生在分片配置变更、网络分区或分片下线时
            return CommandSubmissionResult(params.id, shardWithStatus.getStatus());
        }

        // ========== 第二步：目标主机定位 ==========
        // 获取分片主节点的网络地址，均衡器命令必须在主节点执行
        // ReadPreference::PrimaryOnly 确保命令发送到分片的主节点
        const auto shardHostWithStatus = shardWithStatus.getValue()->getTargeter()->findHost(
            opCtx, ReadPreferenceSetting{ReadPreference::PrimaryOnly});
        
        // 检查主机地址获取是否成功：主节点可能不可用或正在选举中
        if (!shardHostWithStatus.isOK()) {
            // 主机地址获取失败：返回包含错误状态的提交结果
            // 这种情况通常发生在复制集选举、主节点故障或网络问题时
            return CommandSubmissionResult(params.id, shardHostWithStatus.getStatus());
        }

        // ========== 第三步：远程命令构建 ==========
        // 构建TaskExecutor所需的远程命令请求对象
        // 包含：目标主机地址、数据库名称、序列化命令、操作上下文
        const executor::RemoteCommandRequest remoteCommand =
            executor::RemoteCommandRequest(shardHostWithStatus.getValue(),  // 目标分片主机地址
                                           params.commandInfo->getTargetDb(), // 目标数据库名
                                           params.commandInfo->serialise(),   // 序列化的BSON命令
                                           opCtx);                           // 操作上下文

        // ========== 第四步：异步回调设置 ==========
        // 定义命令执行完成后的回调函数
        // 使用lambda捕获请求ID，确保回调能正确关联到原始请求
        auto onRemoteResponseReceived =
            [this,                          // 捕获当前调度器实例
             requestId = params.id]         // 捕获请求ID用于结果关联
            (const executor::TaskExecutor::RemoteCommandCallbackArgs& args) {
                // 当远程命令执行完成（成功或失败）时调用此回调
                // _applyCommandResponse 将处理响应并更新请求状态
                _applyCommandResponse(requestId, args.response);
            };

        // ========== 第五步：异步命令调度 ==========
        // 通过TaskExecutor将命令提交到线程池进行异步执行
        // scheduleRemoteCommand 立即返回，不等待命令执行完成
        auto swRemoteCommandHandle =
            (*_executor)->scheduleRemoteCommand(remoteCommand, onRemoteResponseReceived);
        
        // 返回提交结果：包含请求ID和调度状态
        // 注意：这里返回的是提交状态，不是命令执行结果
        // 实际执行结果将通过上面设置的回调函数异步处理
        return CommandSubmissionResult(params.id, swRemoteCommandHandle.getStatus());
        
    } catch (const DBException& e) {
        // ========== 异常处理 ==========
        // 捕获所有数据库相关异常（网络错误、认证失败、超时等）
        // 将异常转换为状态码，确保调用方能正确处理错误情况
        return CommandSubmissionResult(params.id, e.toStatus());
    }
}

void BalancerCommandsSchedulerImpl::_applySubmissionResult(
    WithLock, CommandSubmissionResult&& submissionResult) {
    auto submittedRequestIt = _requests.find(submissionResult.id);
    tassert(8245206,
            "Submission result ID not found in the requests",
            submittedRequestIt != _requests.end());
    auto& submittedRequest = submittedRequestIt->second;
    auto submissionOutcome = submittedRequest.applySubmissionResult(std::move(submissionResult));
    if (!submissionOutcome.isOK()) {
        // The request was resolved as failed on submission time - move it to the complete list.
        _recentlyCompletedRequestIds.emplace_back(submittedRequestIt->first);
        if (_state == SchedulerState::Recovering && --_numRequestsToRecover == 0) {
            LOGV2(5847213, "Balancer scheduler recovery complete. Switching to regular execution");
            _state = SchedulerState::Running;
        }
    }
}

void BalancerCommandsSchedulerImpl::_applyCommandResponse(
    UUID requestId, const executor::RemoteCommandResponse& response) {
    {
        stdx::lock_guard<Latch> lg(_mutex);
        tassert(8245207, "Scheduler is stopped", _state != SchedulerState::Stopped);
        auto requestIt = _requests.find(requestId);
        tassert(8245208, "Request ID is already in use", requestIt != _requests.end());
        auto& request = requestIt->second;
        request.setOutcome(response);
        _recentlyCompletedRequestIds.emplace_back(request.getId());
        if (_state == SchedulerState::Recovering && --_numRequestsToRecover == 0) {
            LOGV2(5847207, "Balancer scheduler recovery complete. Switching to regular execution");
            _state = SchedulerState::Running;
        }
        _stateUpdatedCV.notify_all();
    }
    LOGV2_DEBUG(5847204,
                2,
                "Execution of balancer command request completed",
                "reqId"_attr = requestId,
                "response"_attr = response);
}

/**
 * BalancerCommandsSchedulerImpl::_workerThread 的作用：
 * 均衡器命令调度器的后台工作线程，负责异步处理和执行所有均衡相关命令（如 chunk 迁移、合并等）。
 * 
 * 核心功能：
 * 1. 循环监听和处理命令队列中的待提交请求
 * 2. 将抽象的命令请求转换为具体的远程命令并异步提交
 * 3. 处理命令提交结果，更新请求状态和调度器内部状态
 * 4. 管理请求生命周期：从队列获取 → 提交执行 → 处理结果 → 清理资源
 * 5. 响应调度器状态变更：支持优雅停止和恢复机制
 * 
 * 工作流程：
 * 1. 状态检查和请求规划：等待新的未提交请求或完成的请求
 * 2. 命令提交：将待处理请求转换为远程命令并提交到执行器
 * 3. 结果处理：处理提交结果，更新请求状态
 * 4. 资源清理：清理已完成的请求，释放相关资源
 * 
 * 该线程是调度器的核心执行引擎，实现了生产者-消费者模式的异步命令处理架构。
 */
void BalancerCommandsSchedulerImpl::_workerThread() {
    // 设置线程退出时的清理逻辑：更新调度器状态为停止，通知等待的线程
    ON_BLOCK_EXIT([this] {
        LOGV2(5847208, "Leaving balancer command scheduler thread");
        stdx::lock_guard<Latch> lg(_mutex);
        _state = SchedulerState::Stopped;  // 标记调度器已停止
        _stateUpdatedCV.notify_all();       // 通知所有等待状态变更的线程
    });

    // 初始化工作线程：设置线程名称和服务上下文
    // 工作线程归属于 ShardServer 角色，专门处理分片相关的命令
    Client::initThread("BalancerCommandsScheduler",
                       getGlobalServiceContext()->getService(ClusterRole::ShardServer));

    // This worker thread may perform remote request, so that its operation context must be
    // interruptible. Marking it so here is safe, since the replica set changes are also notified by
    // the Balancer (a PrimaryOnlyService) and tracked through the _state field (which is checked
    // right after).
    // 创建可中断的操作上下文：工作线程可能执行远程请求，需要在复制集状态变更时能被中断
    // 这确保了在主从切换时能够及时响应并停止处理
    auto opCtxHolder = cc().makeOperationContext();
    opCtxHolder.get()->setAlwaysInterruptAtStepDownOrUp_UNSAFE();

    bool stopWorkerRequested = false;
    LOGV2(5847205, "Balancer scheduler thread started");

    // 主工作循环：持续处理命令直到收到停止请求
    while (!stopWorkerRequested) {
        // 定义本轮处理的数据结构
        std::vector<CommandSubmissionParameters> commandsToSubmit;  // 待提交的命令参数
        std::vector<CommandSubmissionResult> submissionResults;     // 命令提交结果

        // 1. Check the internal state and plan for the actions to be taken ont this round.
        // 第一阶段：检查内部状态，规划本轮要执行的操作
        {
            stdx::unique_lock<Latch> ul(_mutex);
            tassert(8245209, "Scheduler is stopped", _state != SchedulerState::Stopped);
            
            // 等待以下任一条件满足才继续执行：
            // 1. 有未提交的请求且不在暂停状态
            // 2. 调度器正在停止
            // 3. 有最近完成的请求需要清理
            _stateUpdatedCV.wait(ul, [this] {
                return ((!_unsubmittedRequestIds.empty() &&
                         !MONGO_likely(pauseSubmissionsFailPoint.shouldFail())) ||
                        _state == SchedulerState::Stopping ||
                        !_recentlyCompletedRequestIds.empty());
            });

            // 清理已完成的请求：从请求映射表中移除，释放内存资源
            for (const auto& requestId : _recentlyCompletedRequestIds) {
                auto it = _requests.find(requestId);
                _requests.erase(it);  // 删除请求数据，释放相关资源
            }
            _recentlyCompletedRequestIds.clear();  // 清空完成列表

            // 处理待提交的请求：根据调度器状态决定提交还是取消
            for (const auto& requestId : _unsubmittedRequestIds) {
                auto& requestData = _requests.at(requestId);
                if (_state != SchedulerState::Stopping) {
                    // 调度器正常运行：准备提交命令
                    commandsToSubmit.push_back(requestData.getSubmissionParameters());
                } else {
                    // 调度器正在停止：取消请求并设置中断错误
                    requestData.setOutcome(
                        Status(ErrorCodes::BalancerInterrupted,
                               "Request cancelled - balancer scheduler is stopping"));
                    _requests.erase(requestId);  // 立即清理被取消的请求
                }
            }
            _unsubmittedRequestIds.clear();  // 清空待提交队列
            stopWorkerRequested = _state == SchedulerState::Stopping;  // 检查是否需要停止
        }

        // 2. Serve the picked up requests, submitting their related commands.
        // 第二阶段：处理选中的请求，提交相关命令到远程分片
        for (auto& submissionInfo : commandsToSubmit) {
            // 为命令附加操作元数据（如客户端信息、认证信息等）
            if (submissionInfo.commandInfo) {
                submissionInfo.commandInfo.get()->attachOperationMetadataTo(opCtxHolder.get());
            }
            
            // 调用 _submit 将命令提交到目标分片，返回提交结果
            submissionResults.push_back(_submit(opCtxHolder.get(), submissionInfo));
            
            // 记录提交失败的命令，便于调试和监控
            if (!submissionResults.back().outcome.isOK()) {
                LOGV2(5847206,
                      "Submission for scheduler command request failed",
                      "reqId"_attr = submissionResults.back().id,
                      "cause"_attr = submissionResults.back().outcome);
            }
        }

        // 3. Process the outcome of each submission.
        // 第三阶段：处理每个命令的提交结果，更新内部状态
        if (!submissionResults.empty()) {
            stdx::lock_guard<Latch> lg(_mutex);
            for (auto& submissionResult : submissionResults) {
                // 应用提交结果：更新请求状态，处理失败情况
                _applySubmissionResult(lg, std::move(submissionResult));
            }
        }
    }
    
    // 工作线程退出前的清理工作：
    
    // Wait for each outstanding command to complete, clean out its resources and leave.
    // 等待所有未完成的命令执行完毕，清理资源后退出
    (*_executor)->shutdown();  // 关闭任务执行器，停止接受新任务
    (*_executor)->join();      // 等待所有正在执行的任务完成

    // 最终清理：重置调度器内部状态
    {
        stdx::unique_lock<Latch> ul(_mutex);
        _requests.clear();                    // 清空所有请求
        _recentlyCompletedRequestIds.clear(); // 清空完成队列
        _executor.reset();                    // 重置执行器
    }
}

}  // namespace mongo
