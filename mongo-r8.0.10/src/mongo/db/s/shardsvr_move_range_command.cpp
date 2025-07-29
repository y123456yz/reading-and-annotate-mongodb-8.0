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


#include <boost/smart_ptr.hpp>
#include <memory>
#include <string>
#include <tuple>
#include <utility>

#include <boost/move/utility_core.hpp>
#include <boost/optional/optional.hpp>

#include "mongo/base/error_codes.h"
#include "mongo/base/status.h"
#include "mongo/client/read_preference.h"
#include "mongo/client/remote_command_targeter.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/resource_pattern.h"
#include "mongo/db/client.h"
#include "mongo/db/commands.h"
#include "mongo/db/concurrency/d_concurrency.h"
#include "mongo/db/concurrency/lock_manager_defs.h"
#include "mongo/db/database_name.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/repl/member_state.h"
#include "mongo/db/repl/repl_client_info.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/db/s/active_migrations_registry.h"
#include "mongo/db/s/migration_source_manager.h"
#include "mongo/db/s/sharding_ddl_util.h"
#include "mongo/db/s/sharding_statistics.h"
#include "mongo/db/service_context.h"
#include "mongo/db/shard_id.h"
#include "mongo/db/write_concern.h"
#include "mongo/db/write_concern_options.h"
#include "mongo/executor/task_executor_pool.h"
#include "mongo/logv2/log.h"
#include "mongo/logv2/log_attr.h"
#include "mongo/logv2/log_component.h"
#include "mongo/logv2/redaction.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/rpc/op_msg.h"
#include "mongo/s/client/shard.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/grid.h"
#include "mongo/s/request_types/move_range_request_gen.h"
#include "mongo/s/sharding_state.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/decorable.h"
#include "mongo/util/duration.h"
#include "mongo/util/future.h"
#include "mongo/util/future_impl.h"
#include "mongo/util/uuid.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kSharding


namespace mongo {
namespace {

const WriteConcernOptions kMajorityWriteConcern(WriteConcernOptions::kMajority,
                                                // Note: Even though we're setting UNSET here,
                                                // kMajority implies JOURNAL if journaling is
                                                // supported by mongod and
                                                // writeConcernMajorityJournalDefault is set to true
                                                // in the ReplSetConfig.
                                                WriteConcernOptions::SyncMode::UNSET,
                                                WriteConcernOptions::kWriteConcernTimeoutSharding);

// _shardsvrMoveRange命令处理，config server发送给数据迁移的原分片
class ShardsvrMoveRangeCommand final : public TypedCommand<ShardsvrMoveRangeCommand> {
public:
    using Request = ShardsvrMoveRange;

    ShardsvrMoveRangeCommand() : TypedCommand<ShardsvrMoveRangeCommand>(Request::kCommandName) {}

    bool skipApiVersionCheck() const override {
        // Internal command (server to server).
        return true;
    }

    std::string help() const override {
        return "Internal command invoked by the config server to move a chunk/range";
    }

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kNever;
    }

    bool adminOnly() const override {
        return true;
    }

    class Invocation final : public InvocationBase {
    public:
        using InvocationBase::InvocationBase;

        /**
         * ShardsvrMoveRangeCommand::Invocation::typedRun 的作用：
         * 分片服务器端的 chunk 迁移命令处理入口，负责接收并执行来自 Config Server 的 chunk 迁移请求。
         * 
         * 核心功能：
         * 1. 验证分片状态和权限，确保当前节点能够执行分片命令
         * 2. 注册迁移任务到活跃迁移注册表，避免并发迁移冲突
         * 3. 异步执行完整的 chunk 迁移流程（克隆、同步、提交）
         * 4. 处理迁移完成后的范围删除等待（可选）
         * 5. 支持迁移任务的并发控制和重复请求处理
         * 
         * 迁移流程概述：
         * 1. 前置检查：验证分片状态、重新加载分片注册表、检查数据迁移权限
         * 2. 注册迁移：在活跃迁移注册表中注册此次迁移，防止重复执行
         * 3. 执行迁移：通过 MigrationSourceManager 执行完整的迁移状态机
         * 4. 等待删除：可选择等待源分片上的范围删除完成
         * 
         * 该函数是分片间数据迁移的关键入口点，确保了迁移操作的安全性和一致性。
         */
        void typedRun(OperationContext* opCtx) {
            // 验证当前分片状态：确保分片已正确初始化且能够接受分片命令
            // 检查分片身份、配置完整性等前置条件
            ShardingState::get(opCtx)->assertCanAcceptShardedCommands();
        
            // Make sure we're as up-to-date as possible with shard information. This catches the
            // case where we might have changed a shard's host by removing/adding a shard with the
            // same name.
            // 刷新分片注册表：确保拥有最新的分片信息，捕获可能的分片主机变更
            // 这对于处理分片移除/添加同名分片的情况特别重要
            Grid::get(opCtx)->shardRegistry()->reload(opCtx);
        
            // 检查数据迁移权限：验证当前是否允许执行数据迁移操作
            // 可能的限制包括：维护模式、资源不足、配置限制等
            sharding_ddl_util::assertDataMovementAllowed();
        
            // 在活跃迁移注册表中注册此次 chunk 迁移任务
            // 注册机制的作用：
            // 1. 防止同一 chunk 的并发迁移
            // 2. 提供迁移状态跟踪和监控
            // 3. 支持重复请求的幂等处理
            auto scopedMigration = uassertStatusOK(
                ActiveMigrationsRegistry::get(opCtx).registerDonateChunk(opCtx, request()));
        
            // Check if there is an existing migration running and if so, join it
            // 检查是否有现有迁移正在运行，如果有则加入等待
            if (scopedMigration.mustExecute()) {
                // 当前请求是新的迁移任务，需要执行完整的迁移流程
                // 使用异步执行器创建独立的执行上下文，避免阻塞主线程
                auto moveChunkComplete =
                    ExecutorFuture<void>(Grid::get(opCtx)->getExecutorPool()->getFixedExecutor())
                        .then([req = request(),
                               writeConcern = opCtx->getWriteConcern(),
                               scopedMigration = std::move(scopedMigration),
                               serviceContext = opCtx->getServiceContext()]() mutable {
                            // This local variable is created to enforce that the scopedMigration is
                            // destroyed before setting the shared state as ready.
                            // Note that captured objects of the lambda are destroyed by the
                            // executor thread after setting the shared state as ready.
                            // 创建局部变量确保 scopedMigration 在设置共享状态为就绪前被销毁
                            // Lambda 捕获的对象在设置共享状态后由执行器线程销毁
                            auto scopedMigrationLocal(std::move(scopedMigration));
                            
                            // 创建专用的线程客户端上下文用于迁移执行
                            ThreadClient tc("MoveChunk",
                                            serviceContext->getService(ClusterRole::ShardServer));
                            auto uniqueOpCtx = Client::getCurrent()->makeOperationContext();
                            auto executorOpCtx = uniqueOpCtx.get();
                            Status status = {ErrorCodes::InternalError, "Uninitialized value"};
                            
                            try {
                                // 设置操作上下文在主从切换时可被中断
                                // 确保在复制集状态变更时能及时响应
                                executorOpCtx->setAlwaysInterruptAtStepDownOrUp_UNSAFE();
                                
                                {
                                    // Ensure that opCtx will get interrupted in the event of a
                                    // stepdown. This is to ensure that the MigrationSourceManager
                                    // checks that there are no pending migrationCoordinators
                                    // documents (under the ActiveMigrationRegistry lock) on the
                                    // same term during which the migrationCoordinators document
                                    // will be persisted.
                                    // 确保在主从切换事件中操作上下文会被中断
                                    // 这确保 MigrationSourceManager 在持久化 migrationCoordinators 文档的同一任期内
                                    // 检查没有待处理的 migrationCoordinators 文档（在 ActiveMigrationRegistry 锁下）
                                    Lock::GlobalLock lk(executorOpCtx, MODE_IX);
                                    uassert(ErrorCodes::InterruptedDueToReplStateChange,
                                            "Not primary while attempting to start chunk migration "
                                            "donation",
                                            repl::ReplicationCoordinator::get(executorOpCtx)
                                                ->getMemberState()
                                                .primary());
                                }
                                
                                // Note: This internal authorization is tied to the lifetime of the
                                // client.
                                // 注意：此内部授权与客户端的生命周期绑定
                                // 为迁移操作提供内部权限，允许执行系统级操作
                                AuthorizationSession::get(executorOpCtx->getClient())
                                    ->grantInternalAuthorization(executorOpCtx->getClient());
                                
                                // 执行实际的迁移逻辑：委托给 _runImpl 函数
                                _runImpl(executorOpCtx, std::move(req), std::move(writeConcern));
                                status = Status::OK();
                                
                            } catch (const DBException& e) {
                                // 捕获并处理迁移过程中的异常
                                status = e.toStatus();
                                LOGV2_WARNING(23777,
                                              "Error while doing moveChunk",
                                              "error"_attr = redact(status));
        
                                // 特殊处理锁超时错误：更新统计计数器
                                if (status.code() == ErrorCodes::LockTimeout) {
                                    ShardingStatistics::get(executorOpCtx)
                                        .countDonorMoveChunkLockTimeout.addAndFetch(1);
                                }
                            }
        
                            // 通知迁移注册表此次迁移已完成（成功或失败）
                            // 释放迁移锁，允许后续迁移或重试
                            scopedMigrationLocal.signalComplete(status);
                            uassertStatusOK(status);  // 如果迁移失败，向上抛出异常
                        });
                
                // 等待异步迁移任务完成，获取最终结果
                moveChunkComplete.get(opCtx);
                
            } else {
                // 已有相同的迁移任务在执行，等待其完成
                // 这提供了对重复请求的幂等处理能力
                uassertStatusOK(scopedMigration.waitForCompletion(opCtx));
            }
        
            // 可选的范围删除等待逻辑
            if (request().getWaitForDelete()) {
                // Ensure we capture the latest opTime in the system, since range deletion happens
                // asynchronously with a different OperationContext. This must be done after the
                // above join, because each caller must set the opTime to wait for writeConcern for
                // on its own OperationContext.
                // 确保捕获系统中最新的 opTime，因为范围删除是用不同的 OperationContext 异步进行的
                // 这必须在上述 join 之后完成，因为每个调用者必须在自己的 OperationContext 上
                // 设置要等待写入关注点的 opTime
                auto& replClient = repl::ReplClientInfo::forClient(opCtx->getClient());
                replClient.setLastOpToSystemLastOpTime(opCtx);
        
                // 等待范围删除操作达到 majority 写入关注点
                // 确保迁移后的清理工作已在大多数节点上完成
                WriteConcernResult writeConcernResult;
                Status majorityStatus = waitForWriteConcern(
                    opCtx, replClient.getLastOp(), kMajorityWriteConcern, &writeConcernResult);
        
                uassertStatusOKWithContext(
                    majorityStatus, "Failed to wait for range deletions after migration commit");
            }
        }

    private:
        NamespaceString ns() const override {
            return request().getCommandParameter();
        }

        bool supportsWriteConcern() const override {
            return true;
        }

        void doCheckAuthorization(OperationContext* opCtx) const override {
            uassert(ErrorCodes::Unauthorized,
                    "Unauthorized",
                    AuthorizationSession::get(opCtx->getClient())
                        ->isAuthorizedForActionsOnResource(
                            ResourcePattern::forClusterResource(request().getDbName().tenantId()),
                            ActionType::internal));
        }

    /**
     * ShardsvrMoveRangeCommand::_runImpl 的作用：
     * 执行实际的 chunk 迁移操作，是分片间数据迁移的核心实现函数。
     * 
     * 核心功能：
     * 1. 解析和验证源分片和目标分片的连接信息
     * 2. 初始化迁移统计数据，为性能监控做准备
     * 3. 创建并启动 MigrationSourceManager 执行完整的迁移状态机
     * 4. 按序执行迁移的各个阶段：克隆、追赶、临界区、提交
     * 5. 记录详细的迁移统计信息和日志
     * 
     * 迁移状态机流程：
     * 1. startClone()：启动数据克隆阶段，将 chunk 数据复制到目标分片
     * 2. awaitToCatchUp()：等待增量同步完成，确保迁移期间的变更被同步
     * 3. enterCriticalSection()：进入临界区，阻塞对该 chunk 的写操作
     * 4. commitChunkOnRecipient()：在目标分片上提交 chunk 接收
     * 5. commitChunkMetadataOnConfig()：在 Config Server 上更新 chunk 元数据
     * 
     * 该函数通过 MigrationSourceManager 状态机确保了 chunk 迁移的原子性和一致性。
     */
    static void _runImpl(OperationContext* opCtx,
                         ShardsvrMoveRange&& request,
                         WriteConcernOptions&& writeConcern) {//ShardsvrMoveRangeCommand::_runImpl
        // 早期返回检查：如果源分片和目标分片相同，无需执行迁移
        // 这是一种优化，避免不必要的处理开销
        if (request.getFromShard() == request.getToShard()) {
            return;
        }
    
        // Resolve the donor and recipient shards and their connection string
        // 解析源分片和目标分片的连接信息，获取网络连接字符串和主机地址
        auto const shardRegistry = Grid::get(opCtx)->shardRegistry();
    
        // 获取源分片（donor）的连接字符串
        // 用于在迁移过程中标识数据的来源分片
        const auto donorConnStr =
            uassertStatusOK(shardRegistry->getShard(opCtx, request.getFromShard()))
                ->getConnString();
        
        // 获取目标分片（recipient）的主节点主机地址
        // 迁移数据将被发送到这个主机地址
        const auto recipientHost = uassertStatusOK([&] {
            auto recipientShard =
                uassertStatusOK(shardRegistry->getShard(opCtx, request.getToShard()));
    
            // 使用主节点读偏好，确保数据写入到主节点
            return recipientShard->getTargeter()->findHost(
                opCtx, ReadPreferenceSetting{ReadPreference::PrimaryOnly});
        }());
    
        // 记录迁移开始前的统计数据，用于计算本次迁移的具体指标
        // 这些统计数据将在迁移完成后用于计算增量值
        long long totalDocsCloned =
            ShardingStatistics::get(opCtx).countDocsClonedOnDonor.load();        // 已克隆文档总数
        long long totalBytesCloned =
            ShardingStatistics::get(opCtx).countBytesClonedOnDonor.load();       // 已克隆字节总数  
        long long totalCloneTime =
            ShardingStatistics::get(opCtx).totalDonorChunkCloneTimeMillis.load(); // 总克隆时间
    
        // 创建迁移源管理器：负责协调整个迁移过程的状态机
        // 传入迁移请求、写入关注点、源分片连接信息和目标分片主机
        MigrationSourceManager migrationSourceManager(
            opCtx, std::move(request), std::move(writeConcern), donorConnStr, recipientHost);
    
        // 迁移状态机执行序列：按照严格的顺序执行各个阶段
    
        // 阶段1：启动克隆阶段
        // 开始将源分片上的 chunk 数据复制到目标分片
        // 这个阶段允许并发读写，不会阻塞业务操作
        migrationSourceManager.startClone();
        
        // 阶段2：等待追赶完成
        // 同步迁移开始后产生的增量变更，确保目标分片数据最新
        // 这个阶段仍然允许并发操作
        migrationSourceManager.awaitToCatchUp();
        
        // 阶段3：进入临界区
        // 阻塞对该 chunk 范围的所有写操作，确保数据一致性
        // 这是迁移过程中最关键的步骤，会短暂影响写入性能
        migrationSourceManager.enterCriticalSection();
        
        // 阶段4：在目标分片上提交 chunk
        // 通知目标分片 chunk 数据接收完成，可以开始服务请求
        migrationSourceManager.commitChunkOnRecipient();
        
        // 阶段5：在 Config Server 上更新元数据
        // 更新分片集群的路由表，将 chunk 的所有权转移到目标分片
        // 这是迁移的最后一步，完成后迁移正式生效
        migrationSourceManager.commitChunkMetadataOnConfig();
    
        // 计算本次迁移的详细统计信息（增量值）
        // 通过当前值减去开始时的值得到本次迁移的具体指标
        long long docsCloned =
            ShardingStatistics::get(opCtx).countDocsClonedOnDonor.load() - totalDocsCloned;
        long long bytesCloned =
            ShardingStatistics::get(opCtx).countBytesClonedOnDonor.load() - totalBytesCloned;
        long long cloneTime =
            ShardingStatistics::get(opCtx).totalDonorChunkCloneTimeMillis.load() -
            totalCloneTime;
        
        // 获取迁移唯一标识符，用于日志关联和问题追踪
        auto migrationId = migrationSourceManager.getMigrationId();
    
        // 记录迁移完成的详细日志，包含所有关键性能指标
        // 这些信息对于监控、调试和性能分析非常重要
        LOGV2(7627801,
              "Migration finished",
              "migrationId"_attr = migrationId ? migrationId->toString() : "",
              "totalTimeMillis"_attr = migrationSourceManager.getOpTimeMillis(),    // 总耗时
              "docsCloned"_attr = docsCloned,                                        // 克隆文档数
              "bytesCloned"_attr = bytesCloned,                                      // 克隆字节数
              "cloneTime"_attr = cloneTime);                                         // 克隆时间
    }
};
MONGO_REGISTER_COMMAND(ShardsvrMoveRangeCommand).forShard();

}  // namespace
}  // namespace mongo
