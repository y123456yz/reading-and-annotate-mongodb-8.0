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

#include <string>
#include <utility>

#include <boost/move/utility_core.hpp>
#include <boost/none.hpp>
#include <boost/optional/optional.hpp>

#include "mongo/base/error_codes.h"
#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonelement.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/resource_pattern.h"
#include "mongo/db/catalog_raii.h"
#include "mongo/db/commands.h"
#include "mongo/db/concurrency/lock_manager_defs.h"
#include "mongo/db/database_name.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/s/active_migrations_registry.h"
#include "mongo/db/s/chunk_move_write_concern_options.h"
#include "mongo/db/s/collection_metadata.h"
#include "mongo/db/s/collection_sharding_runtime.h"
#include "mongo/db/s/migration_destination_manager.h"
#include "mongo/db/s/migration_session_id.h"
#include "mongo/db/s/shard_filtering_metadata_refresh.h"
#include "mongo/db/s/start_chunk_clone_request.h"
#include "mongo/db/service_context.h"
#include "mongo/db/shard_id.h"
#include "mongo/db/tenant_id.h"
#include "mongo/logv2/log.h"
#include "mongo/logv2/log_attr.h"
#include "mongo/logv2/log_component.h"
#include "mongo/logv2/redaction.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/chunk_version.h"
#include "mongo/s/index_version.h"
#include "mongo/s/shard_version.h"
#include "mongo/s/shard_version_factory.h"
#include "mongo/s/sharding_state.h"
#include "mongo/s/stale_exception.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/database_name_util.h"
#include "mongo/util/future.h"
#include "mongo/util/namespace_string_util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kSharding

namespace mongo {
namespace {

// This shard version is used as the received version in StaleConfigInfo since we do not have
// information about the received version of the operation.
ShardVersion ShardVersionPlacementIgnoredNoIndexes() {
    return ShardVersionFactory::make(ChunkVersion::IGNORED(),
                                     boost::optional<CollectionIndexes>(boost::none));
}

class RecvChunkStartCommand : public ErrmsgCommandDeprecated {
public:
    RecvChunkStartCommand() : ErrmsgCommandDeprecated("_recvChunkStart") {}

    bool skipApiVersionCheck() const override {
        // Internal command (server to server).
        return true;
    }

    std::string help() const override {
        return "internal";
    }

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kNever;
    }

    bool adminOnly() const override {
        return true;
    }

    bool supportsWriteConcern(const BSONObj& cmd) const override {
        // This is required to be true to support moveChunk.
        return true;
    }

    NamespaceString parseNs(const DatabaseName& dbName, const BSONObj& cmdObj) const override {
        return NamespaceStringUtil::deserialize(dbName.tenantId(),
                                                CommandHelpers::parseNsFullyQualified(cmdObj),
                                                SerializationContext::stateDefault());
    }

    Status checkAuthForOperation(OperationContext* opCtx,
                                 const DatabaseName& dbName,
                                 const BSONObj&) const override {
        auto* as = AuthorizationSession::get(opCtx->getClient());
        if (!as->isAuthorizedForActionsOnResource(
                ResourcePattern::forClusterResource(dbName.tenantId()), ActionType::internal)) {
            return {ErrorCodes::Unauthorized, "unauthorized"};
        }

        return Status::OK();
    }

    bool supportsRetryableWrite() const final {
        return true;
    }

    bool shouldCheckoutSession() const final {
        return false;
    }

    /**
     * RecvChunkStartCommand::errmsgRun 函数的作用：
     * 处理 _recvChunkStart 命令，在目标分片上启动 chunk 迁移的接收过程。
     * 
     * 核心功能：
     * 1. 迁移接收初始化：在目标分片上初始化 chunk 迁移的接收端状态
     * 2. 冲突检测和注册：检查并注册迁移操作，确保不与其他迁移冲突
     * 3. 元数据刷新：强制刷新集合路由信息，确保最新的分片元数据
     * 4. 写关注设置：配置适当的写关注级别以确保数据持久性
     * 5. 迁移管理器启动：启动 MigrationDestinationManager 开始接收数据
     * 
     * 执行流程：
     * - 权限验证：确保请求来源有执行内部命令的权限
     * - 参数解析：解析命令中的命名空间、chunk范围、源分片等信息
     * - 冲突注册：在 ActiveMigrationsRegistry 中注册接收操作
     * - 元数据同步：刷新集合分片元数据确保一致性
     * - 目标启动：启动 MigrationDestinationManager 开始数据接收
     * 
     * 安全机制：
     * - 权限检查：验证调用者具有内部操作权限
     * - 冲突预防：确保同一时间只有一个迁移操作在进行
     * - 元数据一致性：通过强制刷新确保分片状态的准确性
     * - 中断支持：支持在主节点降级时中断操作
     * 
     * 错误处理：
     * - 权限错误：返回未授权错误
     * - 冲突错误：如果存在冲突的迁移操作则失败
     * - 元数据错误：处理分片元数据不一致的情况
     * 
     * 该函数是 chunk 迁移流程在目标分片端的入口点，为后续的数据接收和处理做准备。
     */
    bool RecvChunkStartCommand::errmsgRun(OperationContext* opCtx,
                                          const DatabaseName& dbName,
                                          const BSONObj& cmdObj,
                                          std::string& errmsg,
                                          BSONObjBuilder& result) override {
        // 设置中断行为：在主节点降级或升级时总是中断操作
        // 目的：确保迁移操作在节点状态变化时能够及时响应和停止
        // 重要性：避免在非主节点上继续执行写操作相关的迁移流程
        opCtx->setAlwaysInterruptAtStepDownOrUp_UNSAFE();
        
        // 分片状态验证：确保当前节点能够接受分片命令
        // 检查内容：节点是否已正确初始化为分片节点
        // 失败情况：如果节点未正确配置为分片则抛出异常
        ShardingState::get(opCtx)->assertCanAcceptShardedCommands();
    
        // 命名空间解析：从命令对象中提取目标集合的完整命名空间
        // 解析内容：数据库名 + 集合名，支持租户ID
        // 用途：确定要接收 chunk 的目标集合
        auto nss = parseNs(dbName, cmdObj);
    
        // 克隆请求创建：从命令参数中构建 StartChunkCloneRequest 对象
        // 包含信息：源分片ID、迁移会话ID、二级节流设置等
        // 验证机制：uassertStatusOK 确保请求格式正确且完整
        auto cloneRequest = uassertStatusOK(StartChunkCloneRequest::createFromCommand(nss, cmdObj));
    
        // chunk 范围解析：从命令中提取要迁移的 chunk 的范围信息
        // 范围格式：{min: {...}, max: {...}} 定义 chunk 的边界
        // 验证：确保范围格式正确且逻辑有效
        const auto chunkRange = uassertStatusOK(ChunkRange::fromBSON(cmdObj));
    
        // 写关注配置：根据二级节流设置获取有效的写关注选项
        // 目的：确保迁移过程中的写操作具有适当的持久性保证
        // 考虑因素：性能 vs 数据安全性的平衡
        const auto writeConcern =
            uassertStatusOK(ChunkMoveWriteConcernOptions::getEffectiveWriteConcern(
                opCtx, cloneRequest.getSecondaryThrottle()));
    
        // Ensure this shard is not currently receiving or donating any chunks.
        // 迁移冲突检测和注册：确保当前分片没有正在接收或捐赠任何 chunk
        // 
        // 注册机制：在 ActiveMigrationsRegistry 中注册接收操作
        // 冲突检查：验证是否存在重叠的 chunk 范围或相同集合的并发迁移
        // 参数说明：
        // - nss: 目标集合命名空间
        // - chunkRange: 要接收的 chunk 范围
        // - fromShardId: 源分片标识
        // - waitForCompletionOfConflictingOps=false: 不等待冲突操作完成，直接失败
        // 
        // 返回值：scopedReceiveChunk 是一个 RAII 对象，析构时自动注销注册
        // 作用：防止同一分片同时进行多个迁移操作，避免数据不一致
        auto scopedReceiveChunk(
            uassertStatusOK(ActiveMigrationsRegistry::get(opCtx).registerReceiveChunk(
                opCtx,
                nss,
                chunkRange,
                cloneRequest.getFromShardId(),
                false /* waitForCompletionOfConflictingOps*/)));
    
        // We force a refresh immediately after registering this migration to guarantee that this
        // shard will not receive a chunk after refreshing.
        // 强制元数据刷新：在注册迁移后立即强制刷新以确保此分片在刷新后不会接收 chunk
        // 
        // 刷新目的：
        // 1. 获取最新的集合路由信息和分片分布
        // 2. 确保本地元数据与配置服务器保持一致
        // 3. 防止基于过期元数据的错误决策
        // 
        // 参数说明：
        // - opCtx: 操作上下文
        // - nss: 集合命名空间
        // - boost::none: 不指定特定版本，获取最新版本
        // 
        // 重要性：元数据不一致可能导致数据丢失或重复
        onCollectionPlacementVersionMismatch(opCtx, nss, boost::none);
    
        // Wait for the ShardServerCatalogCacheLoader to finish flushing the metadata to the
        // storage. This is not required for correctness, but helps mitigate stalls on secondaries
        // when a shard receives the first chunk for a collection with a large routing table.
        // 等待元数据刷新完成：等待 ShardServerCatalogCacheLoader 完成元数据到存储的刷新
        // 
        // 目的：虽然不是正确性必需的，但有助于减少从节点上的停顿
        // 场景：当分片首次接收具有大型路由表的集合的 chunk 时
        // 
        // 性能优化：
        // - 避免后续操作因等待元数据加载而阻塞
        // - 在从节点上预先加载元数据，减少读操作延迟
        // - 对于大型分片集合特别重要
        CatalogCacheLoader::get(opCtx).waitForCollectionFlush(opCtx, nss);
        
        // 复制客户端信息更新：将最后操作时间设置为系统最后操作时间
        // 目的：确保复制状态的一致性，用于读关注和复制确认
        // 重要性：保证迁移操作在复制集中的正确排序和确认
        repl::ReplClientInfo::forClient(opCtx->getClient()).setLastOpToSystemLastOpTime(opCtx);
    
        // 启动迁移目标管理器：启动实际的数据接收和处理流程
        // 参数传递：
        // - opCtx: 操作上下文，包含认证、权限、事务等信息
        // - nss: 目标集合命名空间
        // - scopedReceiveChunk: 迁移注册令牌，确保独占访问
        // - cloneRequest: 克隆请求对象，包含源分片、会话ID等
        // - writeConcern: 写关注配置，确保数据持久性
        // 
        // 内部流程：
        // 1. 验证请求参数和分片状态
        // 2. 初始化接收端的迁移状态机
        // 3. 准备接收来自源分片的数据传输
        // 4. 设置适当的锁和同步机制
        uassertStatusOK(MigrationDestinationManager::get(opCtx)->start(
            opCtx, nss, std::move(scopedReceiveChunk), cloneRequest, writeConcern));
    
        // 成功响应构建：向调用方返回操作成功的确认
        // 标识：started=true 表示迁移接收流程已成功启动
        // 用途：源分片据此确认可以开始发送数据
        result.appendBool("started", true);
        return true;
    }


};
MONGO_REGISTER_COMMAND(RecvChunkStartCommand).forShard();

class RecvChunkStatusCommand : public BasicCommand {
public:
    RecvChunkStatusCommand() : BasicCommand("_recvChunkStatus") {}

    bool skipApiVersionCheck() const override {
        // Internal command (server to server).
        return true;
    }

    std::string help() const override {
        return "internal";
    }

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kNever;
    }

    bool adminOnly() const override {
        return true;
    }

    bool supportsWriteConcern(const BSONObj& cmd) const override {
        return false;
    }

    Status checkAuthForOperation(OperationContext* opCtx,
                                 const DatabaseName& dbName,
                                 const BSONObj&) const override {
        auto* as = AuthorizationSession::get(opCtx->getClient());
        if (!as->isAuthorizedForActionsOnResource(
                ResourcePattern::forClusterResource(dbName.tenantId()), ActionType::internal)) {
            return {ErrorCodes::Unauthorized, "unauthorized"};
        }

        return Status::OK();
    }

    bool run(OperationContext* opCtx,
             const DatabaseName&,
             const BSONObj& cmdObj,
             BSONObjBuilder& result) override {
        bool waitForSteadyOrDone = cmdObj["waitForSteadyOrDone"].boolean();
        MigrationDestinationManager::get(opCtx)->report(result, opCtx, waitForSteadyOrDone);
        return true;
    }
};
MONGO_REGISTER_COMMAND(RecvChunkStatusCommand).forShard();

class RecvChunkCommitCommand : public BasicCommand {
public:
    RecvChunkCommitCommand() : BasicCommand("_recvChunkCommit") {}

    bool skipApiVersionCheck() const override {
        // Internal command (server to server).
        return true;
    }

    std::string help() const override {
        return "internal";
    }

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kNever;
    }

    bool adminOnly() const override {
        return true;
    }


    bool supportsWriteConcern(const BSONObj& cmd) const override {
        return false;
    }

    Status checkAuthForOperation(OperationContext* opCtx,
                                 const DatabaseName& dbName,
                                 const BSONObj&) const override {
        auto* as = AuthorizationSession::get(opCtx->getClient());
        if (!as->isAuthorizedForActionsOnResource(
                ResourcePattern::forClusterResource(dbName.tenantId()), ActionType::internal)) {
            return {ErrorCodes::Unauthorized, "unauthorized"};
        }

        return Status::OK();
    }

    bool run(OperationContext* opCtx,
             const DatabaseName& dbName,
             const BSONObj& cmdObj,
             BSONObjBuilder& result) override {
        auto const sessionId = uassertStatusOK(MigrationSessionId::extractFromBSON(cmdObj));
        auto const mdm = MigrationDestinationManager::get(opCtx);

        Status const status = mdm->startCommit(sessionId);
        mdm->report(result, opCtx, false);
        if (!status.isOK()) {
            LOGV2(22014, "_recvChunkCommit failed", "error"_attr = redact(status));
            uassertStatusOK(status);
        }
        return true;
    }
};
MONGO_REGISTER_COMMAND(RecvChunkCommitCommand).forShard();

class RecvChunkAbortCommand : public BasicCommand {
public:
    RecvChunkAbortCommand() : BasicCommand("_recvChunkAbort") {}

    bool skipApiVersionCheck() const override {
        // Internal command (server to server).
        return true;
    }

    std::string help() const override {
        return "internal";
    }

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kNever;
    }

    bool adminOnly() const override {
        return true;
    }

    bool supportsWriteConcern(const BSONObj& cmd) const override {
        return false;
    }

    Status checkAuthForOperation(OperationContext* opCtx,
                                 const DatabaseName& dbName,
                                 const BSONObj&) const override {
        auto* as = AuthorizationSession::get(opCtx->getClient());
        if (!as->isAuthorizedForActionsOnResource(
                ResourcePattern::forClusterResource(dbName.tenantId()), ActionType::internal)) {
            return {ErrorCodes::Unauthorized, "unauthorized"};
        }

        return Status::OK();
    }

    bool run(OperationContext* opCtx,
             const DatabaseName&,
             const BSONObj& cmdObj,
             BSONObjBuilder& result) override {
        auto const mdm = MigrationDestinationManager::get(opCtx);

        auto migrationSessionIdStatus(MigrationSessionId::extractFromBSON(cmdObj));

        if (migrationSessionIdStatus.isOK()) {
            Status const status = mdm->abort(migrationSessionIdStatus.getValue());
            mdm->report(result, opCtx, false);
            if (!status.isOK()) {
                LOGV2(22015, "_recvChunkAbort failed", "error"_attr = redact(status));
                uassertStatusOK(status);
            }
        } else if (migrationSessionIdStatus == ErrorCodes::NoSuchKey) {
            mdm->abortWithoutSessionIdCheck();
            mdm->report(result, opCtx, false);
        }

        uassertStatusOK(migrationSessionIdStatus.getStatus());
        return true;
    }
};
MONGO_REGISTER_COMMAND(RecvChunkAbortCommand).forShard();

class RecvChunkReleaseCritSecCommand : public BasicCommand {
public:
    RecvChunkReleaseCritSecCommand() : BasicCommand("_recvChunkReleaseCritSec") {}

    std::string help() const override {
        return "internal";
    }

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kNever;
    }

    bool adminOnly() const override {
        return true;
    }


    bool supportsWriteConcern(const BSONObj& cmd) const override {
        return true;
    }

    Status checkAuthForOperation(OperationContext* opCtx,
                                 const DatabaseName& dbName,
                                 const BSONObj&) const override {
        auto* as = AuthorizationSession::get(opCtx->getClient());
        if (!as->isAuthorizedForActionsOnResource(
                ResourcePattern::forClusterResource(dbName.tenantId()), ActionType::internal)) {
            return {ErrorCodes::Unauthorized, "unauthorized"};
        }

        return Status::OK();
    }

    bool run(OperationContext* opCtx,
             const DatabaseName&,
             const BSONObj& cmdObj,
             BSONObjBuilder& result) override {
        opCtx->setAlwaysInterruptAtStepDownOrUp_UNSAFE();

        CommandHelpers::uassertCommandRunWithMajority(getName(), opCtx->getWriteConcern());
        const auto sessionId = uassertStatusOK(MigrationSessionId::extractFromBSON(cmdObj));

        LOGV2_DEBUG(5899101, 2, "Received _recvChunkReleaseCritSec", "sessionId"_attr = sessionId);

        const auto mdm = MigrationDestinationManager::get(opCtx);
        const auto status = mdm->exitCriticalSection(opCtx, sessionId);
        if (!status.isOK()) {
            LOGV2(5899109, "_recvChunkReleaseCritSec failed", "error"_attr = redact(status));
            uassertStatusOK(status);
        }
        return true;
    }
};
MONGO_REGISTER_COMMAND(RecvChunkReleaseCritSecCommand).forShard();

}  // namespace
}  // namespace mongo
