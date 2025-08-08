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

#pragma once

#include <boost/move/utility_core.hpp>
#include <boost/none.hpp>
#include <boost/optional/optional.hpp>
#include <boost/smart_ptr.hpp>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/api_parameters.h"
#include "mongo/db/database_name.h"
#include "mongo/db/keypattern.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/s/balancer/auto_merger_policy.h"
#include "mongo/db/s/balancer/balancer_commands_scheduler.h"
#include "mongo/db/s/balancer/balancer_policy.h"
#include "mongo/db/s/forwardable_operation_metadata.h"
#include "mongo/db/s/sharding_config_server_parameters_gen.h"
#include "mongo/db/service_context.h"
#include "mongo/db/shard_id.h"
#include "mongo/db/write_concern_options.h"
#include "mongo/executor/remote_command_response.h"
#include "mongo/executor/scoped_task_executor.h"
#include "mongo/platform/mutex.h"
#include "mongo/rpc/get_status_from_command_result.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/catalog/type_collection.h"
#include "mongo/s/chunk_version.h"
#include "mongo/s/client/shard.h"
#include "mongo/s/cluster_commands_helpers.h"
#include "mongo/s/request_types/merge_chunk_request_gen.h"
#include "mongo/s/request_types/migration_secondary_throttle_options.h"
#include "mongo/s/request_types/move_range_request_gen.h"
#include "mongo/s/request_types/sharded_ddl_commands_gen.h"
#include "mongo/s/shard_version.h"
#include "mongo/stdx/condition_variable.h"
#include "mongo/stdx/thread.h"
#include "mongo/stdx/unordered_map.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/concurrency/with_lock.h"
#include "mongo/util/decorable.h"
#include "mongo/util/future.h"
#include "mongo/util/future_impl.h"
#include "mongo/util/namespace_string_util.h"
#include "mongo/util/uuid.h"

namespace mongo {

/**
 * Utility class to extract and hold information describing the remote client that submitted a
 * command.
 */
struct ExternalClientInfo {
    ExternalClientInfo(OperationContext* opCtx)
        : operationMetadata(opCtx), apiParameters(APIParameters::get(opCtx)) {}

    const ForwardableOperationMetadata operationMetadata;
    const APIParameters apiParameters;
};


/**
 * Base class describing the common traits of a shard command associated to a Request
 * received by BalancerCommandSchedulerImpl.
 */
class CommandInfo {
public:
    CommandInfo(const ShardId& targetShardId,
                const NamespaceString& nss,
                boost::optional<ExternalClientInfo>&& clientInfo)
        : _targetShardId(targetShardId), _nss(nss), _clientInfo(clientInfo) {}

    virtual ~CommandInfo() {}

    virtual BSONObj serialise() const = 0;

    virtual bool requiresRecoveryOnCrash() const {
        return false;
    }

    virtual bool requiresRecoveryCleanupOnCompletion() const {
        return false;
    }

    virtual DatabaseName getTargetDb() const {
        return DatabaseName::kAdmin;
    }

    const ShardId& getTarget() const {
        return _targetShardId;
    }

    const NamespaceString& getNameSpace() const {
        return _nss;
    }

    void attachOperationMetadataTo(OperationContext* opCtx) {
        if (_clientInfo) {
            _clientInfo.get().operationMetadata.setOn(opCtx);
        }
    }

    void appendCommandMetadataTo(BSONObjBuilder* commandBuilder) const {
        if (_clientInfo && _clientInfo.get().apiParameters.getParamsPassed()) {
            _clientInfo.get().apiParameters.appendInfo(commandBuilder);
        }
    }

private:
    ShardId _targetShardId;
    NamespaceString _nss;
    boost::optional<ExternalClientInfo> _clientInfo;
};

/**
 * Set of command-specific subclasses of CommandInfo.
 */

class MoveRangeCommandInfo : public CommandInfo {
public:
    MoveRangeCommandInfo(const ShardsvrMoveRange& request,
                         const WriteConcernOptions& writeConcern,
                         boost::optional<ExternalClientInfo>&& clientInfo)
        : CommandInfo(request.getFromShard(), request.getCommandParameter(), std::move(clientInfo)),
          _request(request),
          _wc(writeConcern) {}

    const ShardsvrMoveRange& getMoveRangeRequest() {
        return _request;
    }

    BSONObj serialise() const override {
        BSONObjBuilder commandBuilder;
        _request.serialize(BSON(WriteConcernOptions::kWriteConcernField << _wc.toBSON()),
                           &commandBuilder);
        appendCommandMetadataTo(&commandBuilder);
        return commandBuilder.obj();
    }

private:
    const ShardsvrMoveRange _request;
    const WriteConcernOptions _wc;
};

class DisableBalancerCommandInfo : public CommandInfo {
public:
    DisableBalancerCommandInfo(const NamespaceString& nss, const ShardId& shardId)
        : CommandInfo(shardId, nss, boost::none) {}

    BSONObj serialise() const override {
        BSONObjBuilder updateCmd;
        updateCmd.append("$set", BSON("noBalance" << true));

        const auto updateOp = BatchedCommandRequest::buildUpdateOp(
            CollectionType::ConfigNS,
            BSON(CollectionType::kNssFieldName << NamespaceStringUtil::serialize(
                     getNameSpace(), SerializationContext::stateDefault())) /* query */,
            updateCmd.obj() /* update */,
            false /* upsert */,
            false /* multi */);
        BSONObjBuilder cmdObj(updateOp.toBSON());
        cmdObj.append(WriteConcernOptions::kWriteConcernField,
                      WriteConcernOptions::kInternalWriteDefault);
        return cmdObj.obj();
    }

    DatabaseName getTargetDb() const override {
        return DatabaseName::kConfig;
    }
};

class MergeChunksCommandInfo : public CommandInfo {
public:
    MergeChunksCommandInfo(const NamespaceString& nss,
                           const ShardId& shardId,
                           const BSONObj& lowerBoundKey,
                           const BSONObj& upperBoundKey,
                           const ChunkVersion& version)
        : CommandInfo(shardId, nss, boost::none),
          _lowerBoundKey(lowerBoundKey),
          _upperBoundKey(upperBoundKey),
          _version(version) {}

    BSONObj serialise() const override {
        BSONArrayBuilder boundsArrayBuilder;
        boundsArrayBuilder.append(_lowerBoundKey).append(_upperBoundKey);

        BSONObjBuilder commandBuilder;
        commandBuilder
            .append(kCommandName,
                    NamespaceStringUtil::serialize(getNameSpace(),
                                                   SerializationContext::stateDefault()))
            .appendArray(kBounds, boundsArrayBuilder.arr())
            .append(kShardName, getTarget().toString())
            .append(kEpoch, _version.epoch())
            .append(kTimestamp, _version.getTimestamp());

        _version.serialize(ChunkVersion::kChunkVersionField, &commandBuilder);

        return commandBuilder.obj();
    }

private:
    BSONObj _lowerBoundKey;
    BSONObj _upperBoundKey;
    ChunkVersion _version;

    static const std::string kCommandName;
    static const std::string kBounds;
    static const std::string kShardName;
    static const std::string kEpoch;
    static const std::string kTimestamp;
};

class DataSizeCommandInfo : public CommandInfo {
public:
    DataSizeCommandInfo(const NamespaceString& nss,
                        const ShardId& shardId,
                        const BSONObj& shardKeyPattern,
                        const BSONObj& lowerBoundKey,
                        const BSONObj& upperBoundKey,
                        bool estimatedValue,
                        int64_t maxSize,
                        const ShardVersion& version)
        : CommandInfo(shardId, nss, boost::none),
          _shardKeyPattern(shardKeyPattern),
          _lowerBoundKey(lowerBoundKey),
          _upperBoundKey(upperBoundKey),
          _estimatedValue(estimatedValue),
          _maxSize(maxSize),
          _version(version) {}

    BSONObj serialise() const override {
        BSONObjBuilder commandBuilder;
        commandBuilder
            .append(kCommandName,
                    NamespaceStringUtil::serialize(getNameSpace(),
                                                   SerializationContext::stateDefault()))
            .append(kKeyPattern, _shardKeyPattern)
            .append(kMinValue, _lowerBoundKey)
            .append(kMaxValue, _upperBoundKey)
            .append(kEstimatedValue, _estimatedValue)
            .append(kMaxSizeValue, _maxSize);

        _version.serialize(ShardVersion::kShardVersionField, &commandBuilder);

        return commandBuilder.obj();
    }

private:
    BSONObj _shardKeyPattern;
    BSONObj _lowerBoundKey;
    BSONObj _upperBoundKey;
    bool _estimatedValue;
    int64_t _maxSize;
    ShardVersion _version;

    static const std::string kCommandName;
    static const std::string kKeyPattern;
    static const std::string kMinValue;
    static const std::string kMaxValue;
    static const std::string kEstimatedValue;
    static const std::string kMaxSizeValue;
};

class MergeAllChunksOnShardCommandInfo : public CommandInfo {
public:
    MergeAllChunksOnShardCommandInfo(const NamespaceString& nss, const ShardId& shardId)
        : CommandInfo(shardId, nss, boost::none) {}

    BSONObj serialise() const override {
        ShardSvrMergeAllChunksOnShard req(getNameSpace(), getTarget());
        req.setMaxNumberOfChunksToMerge(AutoMergerPolicy::MAX_NUMBER_OF_CHUNKS_TO_MERGE);
        req.setMaxTimeProcessingChunksMS(autoMergerMaxTimeProcessingChunksMS.load());
        return req.toBSON({});
    }
};

class MoveCollectionCommandInfo : public CommandInfo {
public:
    MoveCollectionCommandInfo(const NamespaceString& nss,
                              const ShardId& toShardId,
                              const ShardId& dbPrimaryShard,
                              const DatabaseVersion& dbVersion)
        : CommandInfo(dbPrimaryShard, nss, boost::none),
          _toShardId(toShardId),
          _dbVersion(dbVersion) {}

    BSONObj serialise() const override {
        auto moveCollectionRequest = cluster::unsplittable::makeMoveCollectionRequest(
            getNameSpace().dbName(),
            getNameSpace(),
            _toShardId,
            ProvenanceEnum::kBalancerMoveCollection);
        return appendDbVersionIfPresent(
            CommandHelpers::appendMajorityWriteConcern(moveCollectionRequest.toBSON({})),
            _dbVersion);
    }

private:
    const ShardId _toShardId;
    const DatabaseVersion _dbVersion;
};

/**
 * Helper data structure for submitting the remote command associated to a BalancerCommandsScheduler
 * Request.
 */
struct CommandSubmissionParameters {
    CommandSubmissionParameters(UUID id, const std::shared_ptr<CommandInfo>& commandInfo)
        : id(id), commandInfo(commandInfo) {}

    CommandSubmissionParameters(CommandSubmissionParameters&& rhs) noexcept
        : id(rhs.id), commandInfo(std::move(rhs.commandInfo)) {}

    const UUID id;
    std::shared_ptr<CommandInfo> commandInfo;
};

/**
 * Helper data structure for storing the outcome of a Command submission.
 */
struct CommandSubmissionResult {
    CommandSubmissionResult(UUID id, const Status& outcome) : id(id), outcome(outcome) {}
    CommandSubmissionResult(CommandSubmissionResult&& rhs) = default;
    CommandSubmissionResult(const CommandSubmissionResult& rhs) = delete;
    UUID id;
    Status outcome;
};

/**
 * The class encapsulating all the properties supporting a request to BalancerCommandsSchedulerImpl
 * as it gets created, executed and completed/cancelled.
 */
class RequestData {
public:
    RequestData(UUID id, std::shared_ptr<CommandInfo>&& commandInfo)
        : _id(id),
          _completedOrAborted(false),
          _commandInfo(std::move(commandInfo)),
          _responsePromise{NonNullPromiseTag{}} {
        tassert(8245210, "CommandInfo is be empty", _commandInfo);
    }

    RequestData(RequestData&& rhs)
        : _id(rhs._id),
          _completedOrAborted(rhs._completedOrAborted),
          _commandInfo(std::move(rhs._commandInfo)),
          _responsePromise(std::move(rhs._responsePromise)) {}

    ~RequestData() = default;

    UUID getId() const {
        return _id;
    }

    CommandSubmissionParameters getSubmissionParameters() const {
        return CommandSubmissionParameters(_id, _commandInfo);
    }

    Status applySubmissionResult(CommandSubmissionResult&& submissionResult) {
        tassert(8245211, "Result ID does not match request ID", _id == submissionResult.id);
        if (_completedOrAborted) {
            // A remote response was already received by the time the submission gets processed.
            // Keep the original outcome and continue the workflow.
            return Status::OK();
        }
        const auto& submissionStatus = submissionResult.outcome;
        if (!submissionStatus.isOK()) {
            // cascade the submission failure
            setOutcome(submissionStatus);
        }
        return submissionStatus;
    }

    const CommandInfo& getCommandInfo() const {
        return *_commandInfo;
    }

    const NamespaceString& getNamespace() const {
        return _commandInfo->getNameSpace();
    }

    bool requiresRecoveryCleanupOnCompletion() const {
        return _commandInfo->requiresRecoveryCleanupOnCompletion();
    }

    Future<executor::RemoteCommandResponse> getOutcomeFuture() {
        return _responsePromise.getFuture();
    }

    void setOutcome(const StatusWith<executor::RemoteCommandResponse>& response) {
        _responsePromise.setFrom(response);
        _completedOrAborted = true;
    }

private:
    RequestData& operator=(const RequestData& rhs) = delete;

    RequestData(const RequestData& rhs) = delete;

    const UUID _id;

    bool _completedOrAborted;

    std::shared_ptr<CommandInfo> _commandInfo;

    Promise<executor::RemoteCommandResponse> _responsePromise;
};

/**
 *  Implementation of BalancerCommandsScheduler, relying on the Notification library
 *  for the management of deferred response to remote commands.
 */
/**
 * BalancerCommandsSchedulerImpl 类详细说明：
 * 
 * 这是MongoDB分片集群负载均衡器命令调度器的具体实现类，负责异步管理和执行所有均衡相关的远程命令。
 * 
 * 核心职责：
 * 1. 异步命令调度：管理chunk迁移、合并、数据大小查询等均衡命令的生命周期
 * 2. 并发执行控制：通过工作线程池和TaskExecutor实现高并发的远程命令执行
 * 3. 请求状态管理：跟踪每个请求从提交到完成的完整状态转换
 * 4. 错误处理和恢复：支持故障恢复、优雅停止和请求取消机制
 * 5. 资源生命周期管理：确保请求资源的正确分配和及时清理
 * 
 * 架构特点：
 * - 生产者-消费者模式：Balancer线程生产任务，工作线程异步消费
 * - Future/Promise异步编程：使用SemiFuture实现非阻塞的结果等待
 * - 线程安全设计：通过互斥锁和条件变量确保多线程安全
 * - 状态机管理：通过SchedulerState枚举管理调度器的不同运行阶段
 */
class BalancerCommandsSchedulerImpl : public BalancerCommandsScheduler {
public:
    BalancerCommandsSchedulerImpl();

    ~BalancerCommandsSchedulerImpl() override;

    /**
     * 启动调度器服务
     * @param opCtx 操作上下文，用于获取服务环境和权限检查
     * 
     * 启动流程：
     * 1. 初始化TaskExecutor执行器，创建网络线程池
     * 2. 启动后台工作线程_workerThread，开始处理命令队列
     * 3. 设置调度器状态为Running，开始接受新的命令请求
     * 4. 如果有未完成的历史请求，进入恢复模式进行重试
     */
    void start(OperationContext* opCtx) override;

    /**
     * 优雅停止调度器服务
     * 
     * 停止流程：
     * 1. 设置状态为Stopping，拒绝新的请求提交
     * 2. 取消所有未提交的请求，返回取消错误
     * 3. 等待正在执行的请求完成或超时
     * 4. 关闭TaskExecutor，释放网络资源
     * 5. 等待工作线程退出，清理所有内部状态
     */
    void stop() override;

    /**
     * 为指定集合禁用负载均衡
     * @param opCtx 操作上下文
     * @param nss 目标集合的命名空间
     * 
     * 实现方式：
     * 向config.collections集合发送更新命令，设置noBalance=true标志
     * 这会阻止Balancer对该集合执行任何chunk迁移操作
     */
    void disableBalancerForCollection(OperationContext* opCtx, const NamespaceString& nss) override;

    /**
     * 异步请求chunk迁移（核心方法）
     * @param opCtx 操作上下文
     * @param request ShardsvrMoveRange命令请求，包含源分片、目标分片、chunk范围等信息
     * @param secondaryThrottleWC 副本集写入关注点，控制数据复制的一致性级别
     * @param issuedByRemoteUser 是否为远程用户发起的请求（影响权限和审计）
     * @return SemiFuture<void> 异步结果，调用方可选择立即返回或等待完成
     * 
     * 执行流程：
     * 1. 封装MoveRangeCommandInfo，包含完整的迁移参数
     * 2. 生成唯一请求ID，创建RequestData对象
     * 3. 将请求加入_unsubmittedRequestIds队列
     * 4. 唤醒_workerThread进行异步处理
     * 5. 立即返回SemiFuture，不阻塞调用方
     */
    SemiFuture<void> requestMoveRange(OperationContext* opCtx,
                                      const ShardsvrMoveRange& request,
                                      const WriteConcernOptions& secondaryThrottleWC,
                                      bool issuedByRemoteUser) override;

    /**
     * 异步请求chunk合并
     * @param opCtx 操作上下文
     * @param nss 集合命名空间
     * @param shardId 目标分片ID
     * @param chunkRange 要合并的chunk范围
     * @param version chunk版本信息，确保操作的一致性
     * @return SemiFuture<void> 异步合并结果
     * 
     * 用途：合并相邻的小chunk，减少元数据开销，优化查询性能
     */
    SemiFuture<void> requestMergeChunks(OperationContext* opCtx,
                                        const NamespaceString& nss,
                                        const ShardId& shardId,
                                        const ChunkRange& chunkRange,
                                        const ChunkVersion& version) override;

    /**
     * 异步查询chunk数据大小
     * @param opCtx 操作上下文
     * @param nss 集合命名空间
     * @param shardId 目标分片ID
     * @param chunkRange chunk范围
     * @param version 分片版本信息
     * @param keyPattern 分片键模式
     * @param estimatedValue 是否使用估计值（快速但不精确）
     * @param maxSize 查询的最大数据量限制
     * @return SemiFuture<DataSizeResponse> 包含数据大小信息的异步结果
     * 
     * 用途：为负载均衡算法提供精确的数据分布信息，支持基于数据量的均衡决策
     */
    SemiFuture<DataSizeResponse> requestDataSize(OperationContext* opCtx,
                                                 const NamespaceString& nss,
                                                 const ShardId& shardId,
                                                 const ChunkRange& chunkRange,
                                                 const ShardVersion& version,
                                                 const KeyPattern& keyPattern,
                                                 bool estimatedValue,
                                                 int64_t maxSize) override;

    /**
     * 异步请求合并分片上的所有可合并chunk
     * @param opCtx 操作上下文
     * @param nss 集合命名空间
     * @param shardId 目标分片ID
     * @return SemiFuture<NumMergedChunks> 返回实际合并的chunk数量
     * 
     * 用途：批量优化分片上的chunk分布，减少碎片化，提高查询效率
     * 限制：受AutoMergerPolicy::MAX_NUMBER_OF_CHUNKS_TO_MERGE约束
     */
    SemiFuture<NumMergedChunks> requestMergeAllChunksOnShard(OperationContext* opCtx,
                                                             const NamespaceString& nss,
                                                             const ShardId& shardId) override;

    /**
     * 异步请求未分片集合迁移
     * @param opCtx 操作上下文
     * @param nss 集合命名空间
     * @param toShardId 目标分片ID
     * @param dbPrimaryShardId 数据库主分片ID
     * @param dbVersion 数据库版本信息
     * @return SemiFuture<void> 异步迁移结果
     * 
     * 用途：将整个未分片集合从一个分片迁移到另一个分片，实现数据库级别的负载均衡
     */
    SemiFuture<void> requestMoveCollection(OperationContext* opCtx,
                                           const NamespaceString& nss,
                                           const ShardId& toShardId,
                                           const ShardId& dbPrimaryShardId,
                                           const DatabaseVersion& dbVersion) override;

private:
    /**
     * 调度器状态枚举：管理调度器的生命周期状态转换
     * 
     * 状态转换图：
     * Stopped → Recovering → Running → Stopping → Stopped
     *   ↑                                           ↓
     *   └─────────────── 完整生命周期 ──────────────┘
     */
    enum class SchedulerState { 
        Recovering,  // 恢复模式：处理上次中断的未完成请求
        Running,     // 运行模式：正常接受和处理新请求
        Stopping,    // 停止中：拒绝新请求，等待现有请求完成
        Stopped      // 已停止：所有资源已释放，可以安全销毁
    };

    /**
     * TaskExecutor执行器：负责实际的网络通信和远程命令执行
     * 
     * 功能：
     * - 管理网络连接池，复用到各分片的连接
     * - 提供异步的scheduleRemoteCommand接口
     * - 支持命令超时、重试和错误处理
     * - 线程安全的并发执行多个远程命令
     */
    std::unique_ptr<executor::ScopedTaskExecutor> _executor{nullptr};

    // Protects the in-memory state of the Scheduler
    // (_state, _requests, _unsubmittedRequestIds, _recentlyCompletedRequests).
    /**
     * 主互斥锁：保护调度器的所有内存状态
     * 
     * 保护的数据结构：
     * - _state: 调度器当前状态
     * - _requests: 所有请求的映射表
     * - _unsubmittedRequestIds: 待提交请求队列
     * - _recentlyCompletedRequestIds: 已完成请求清理队列
     * 
     * 设计原则：
     * - 使用RAII锁管理，确保异常安全
     * - 最小化锁持有时间，减少线程竞争
     * - 支持条件变量，实现高效的线程同步
     */
    Mutex _mutex = MONGO_MAKE_LATCH("BalancerCommandsSchedulerImpl::_mutex");

    /**
     * 调度器当前运行状态
     * 
     * 状态变更规则：
     * - 只能在持有_mutex锁时修改
     * - 每次状态变更后必须调用_stateUpdatedCV.notify_all()
     * - 状态转换必须遵循预定义的状态机图
     */
    SchedulerState _state{SchedulerState::Stopped};

    /**
     * 状态更新条件变量：实现线程间的高效同步
     * 
     * 等待条件：
     * - 工作线程等待新的未提交请求
     * - 客户端线程等待调度器状态变更（如从Recovering到Running）
     * - 停止流程等待所有请求处理完成
     * 
     * 通知时机：
     * - 新请求入队时
     * - 请求完成时
     * - 调度器状态变更时
     */
    stdx::condition_variable _stateUpdatedCV;

    /**
     * 工作线程句柄：后台异步处理线程的管理句柄
     * 
     * 线程职责：
     * 1. 循环监听_unsubmittedRequestIds队列
     * 2. 批量提交远程命令到TaskExecutor
     * 3. 处理命令提交结果和状态更新
     * 4. 清理已完成请求的资源
     * 
     * 生命周期：
     * - 在start()中创建和启动
     * - 在stop()中通知退出并join等待
     * - 支持优雅停止，确保数据一致性
     */
    stdx::thread _workerThreadHandle;

    /**
     * List of all unsubmitted + submitted + completed, but not cleaned up yet requests managed by
     * BalancerCommandsSchedulerImpl, organized by ID.
     */
    /**
     * 请求映射表：管理所有请求的完整生命周期状态
     * 
     * 键：UUID - 每个请求的唯一标识符
     * 值：RequestData - 包含请求的完整信息和状态
     * 
     * 请求状态包括：
     * - 未提交：刚创建，等待工作线程处理
     * - 已提交：已发送到远程分片，等待响应
     * - 已完成：收到响应或发生错误，等待清理
     * 
     * 内存管理：
     * - 使用UUID::Hash优化查找性能
     * - 及时清理已完成请求，避免内存泄漏
     * - 支持请求取消和错误处理
     */
    stdx::unordered_map<UUID, RequestData, UUID::Hash> _requests;

    /**
     * List of request IDs that have not been yet submitted for remote execution.
     */
    /**
     * 未提交请求队列：按FIFO顺序存储待处理的请求ID
     * 
     * 队列操作：
     * - 入队：在_enqueueRequest中添加新请求ID
     * - 出队：在_workerThread中批量处理所有待提交请求
     * - 清空：处理完成后清空队列，等待下一批请求
     * 
     * 并发控制：
     * - 只能在持有_mutex锁时操作
     * - 队列非空时唤醒工作线程
     * - 支持优先级处理（按入队顺序）
     * BalancerCommandsSchedulerImpl::_enqueueRequest入队
     * BalancerCommandsSchedulerImpl::_workerThread()出队执行
     */
    std::vector<UUID> _unsubmittedRequestIds;

    /**
     * List of completed/cancelled requests IDs that may still hold synchronisation resources or
     * persisted state that the scheduler needs to release/clean up.
     */
    /**
     * 已完成请求清理队列：存储需要清理资源的已完成请求ID
     * 
     * 清理时机：
     * - 请求正常完成后
     * - 请求被取消或发生错误后
     * - 调度器停止时批量清理
     * 
     * 清理内容：
     * - 释放Promise/Future资源
     * - 从_requests映射表中移除
     * - 清理相关的同步资源
     * - 记录统计信息和日志
     */
    std::vector<UUID> _recentlyCompletedRequestIds;

    /*
     * Counter of oustanding requests that were interrupted by a prior step-down/crash event,
     * and that the scheduler is currently submitting as part of its initial recovery phase.
     */
    /**
     * 恢复请求计数器：跟踪因故障中断而需要恢复的请求数量
     * 
     * 恢复场景：
     * - 主从切换导致的请求中断
     * - 进程重启后的状态恢复
     * - 网络异常导致的请求失败
     * 
     * 恢复策略：
     * - 重新提交中断的请求
     * - 验证请求的有效性
     * - 更新请求状态和超时时间
     * - 在恢复完成后进入正常运行模式
     */
    size_t _numRequestsToRecover{0};

    /**
     * 构建并入队新请求：创建RequestData对象并加入处理队列
     * @param opCtx 操作上下文
     * @param commandInfo 命令信息的智能指针
     * @return Future<RemoteCommandResponse> 异步响应Future
     * 
     * 处理流程：
     * 1. 生成唯一的UUID作为请求标识符
     * 2. 创建RequestData对象封装请求信息
     * 3. 获取Future对象供调用方等待结果
     * 4. 调用_enqueueRequest将请求加入队列
     * 5. 返回Future给调用方
     */
    Future<executor::RemoteCommandResponse> _buildAndEnqueueNewRequest(
        OperationContext* opCtx, std::shared_ptr<CommandInfo>&& commandInfo);

    /**
     * 入队请求：将RequestData对象添加到处理队列
     * @param WithLock 编译时锁检查，确保调用时持有_mutex锁
     * @param request 要入队的请求数据（移动语义）
     * 
     * 队列管理：
     * 1. 检查调度器状态，拒绝无效状态下的请求
     * 2. 防重复检查，避免同一请求被重复添加
     * 3. 将请求添加到_requests映射表
     * 4. 将请求ID添加到_unsubmittedRequestIds队列
     * 5. 通知工作线程有新请求待处理
     */
    void _enqueueRequest(WithLock, RequestData&& request);

    /**
     * 提交单个命令到远程分片：核心的命令执行逻辑
     * @param opCtx 操作上下文
     * @param data 命令提交参数
     * @return CommandSubmissionResult 命令提交结果
     * 
     * 执行步骤：
     * 1. 通过ShardRegistry解析目标分片信息
     * 2. 获取分片的主节点地址
     * 3. 构建RemoteCommandRequest对象
     * 4. 设置异步回调函数处理响应
     * 5. 调用TaskExecutor.scheduleRemoteCommand异步执行
     * 6. 立即返回提交状态，不等待远程执行完成
     */
    CommandSubmissionResult _submit(OperationContext* opCtx,
                                    const CommandSubmissionParameters& data);

    /**
     * 应用命令提交结果：处理_submit的返回状态
     * @param WithLock 编译时锁检查
     * @param submissionResult 命令提交结果（移动语义）
     * 
     * 结果处理：
     * 1. 根据请求ID查找对应的RequestData
     * 2. 如果提交失败，立即设置请求失败状态
     * 3. 如果提交成功，等待远程响应回调
     * 4. 更新请求的内部状态
     * 5. 记录提交统计信息
     */
    void _applySubmissionResult(WithLock, CommandSubmissionResult&& submissionResult);

    /**
     * 应用远程命令响应：处理TaskExecutor的异步回调
     * @param requestId 请求唯一标识符
     * @param response 远程命令的执行响应
     * 
     * 回调处理：
     * 1. 根据requestId查找对应的RequestData
     * 2. 将response设置为Future的结果
     * 3. 将请求ID添加到_recentlyCompletedRequestIds
     * 4. 通知工作线程进行资源清理
     * 5. 记录命令执行完成的调试日志
     * 
     * 注意：此方法在TaskExecutor的网络线程中调用，需要线程安全
     */
    void _applyCommandResponse(UUID requestId, const executor::RemoteCommandResponse& response);

    /**
     * 工作线程主函数：调度器的核心执行引擎
     * 
     * 主循环逻辑：
     * 1. 等待状态变更或新请求到达
     * 2. 批量处理所有待提交请求
     * 3. 调用_submit异步提交远程命令
     * 4. 处理提交结果和状态更新
     * 5. 清理已完成请求的资源
     * 6. 检查停止条件，支持优雅退出
     * 
     * 错误处理：
     * - 捕获并处理所有异常，防止线程崩溃
     * - 在退出时设置调度器状态为Stopped
     * - 通知所有等待的线程调度器已停止
     */
    void _workerThread();
};

}  // namespace mongo
