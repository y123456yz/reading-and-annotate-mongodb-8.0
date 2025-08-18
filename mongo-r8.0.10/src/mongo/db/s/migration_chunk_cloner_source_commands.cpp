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


#include <boost/optional.hpp>
#include <memory>
#include <string>

#include <boost/move/utility_core.hpp>
#include <boost/none.hpp>
#include <boost/optional/optional.hpp>

#include "mongo/base/error_codes.h"
#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/resource_pattern.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog_raii.h"
#include "mongo/db/commands.h"
#include "mongo/db/concurrency/exception_util.h"
#include "mongo/db/concurrency/lock_manager_defs.h"
#include "mongo/db/database_name.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/repl/optime.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/db/repl/replication_process.h"
#include "mongo/db/s/active_migrations_registry.h"
#include "mongo/db/s/collection_sharding_runtime.h"
#include "mongo/db/s/migration_chunk_cloner_source.h"
#include "mongo/db/s/migration_session_id.h"
#include "mongo/db/s/migration_source_manager.h"
#include "mongo/db/service_context.h"
#include "mongo/db/write_concern.h"
#include "mongo/db/write_concern_options.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/concurrency/notification.h"
#include "mongo/util/str.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kSharding


/**
 * This file contains commands, which are specific to the legacy chunk cloner source.
 */
namespace mongo {
namespace {

/**
 * Shortcut class to perform the appropriate checks and acquire the cloner associated with the
 * currently active migration. Uses the currently registered migration for this shard and ensures
 * the session ids match.
 */
class AutoGetActiveCloner {
    AutoGetActiveCloner(const AutoGetActiveCloner&) = delete;
    AutoGetActiveCloner& operator=(const AutoGetActiveCloner&) = delete;

public:
    AutoGetActiveCloner(OperationContext* opCtx,
                        const MigrationSessionId& migrationSessionId,
                        const bool holdCollectionLock) {
        const auto nss = ActiveMigrationsRegistry::get(opCtx).getActiveDonateChunkNss();
        uassert(ErrorCodes::NotYetInitialized, "No active migrations were found", nss);

        // Once the collection is locked, the migration status cannot change
        _autoColl.emplace(opCtx, *nss, MODE_IS);

        uassert(ErrorCodes::NamespaceNotFound,
                str::stream() << "Collection " << nss->toStringForErrorMsg() << " does not exist",
                _autoColl->getCollection());

        uassert(ErrorCodes::NotWritablePrimary,
                "No longer primary when trying to acquire active migrate cloner",
                opCtx->writesAreReplicated() &&
                    repl::ReplicationCoordinator::get(opCtx)->canAcceptWritesFor(opCtx, *nss));

        {
            const auto scopedCsr =
                CollectionShardingRuntime::assertCollectionLockedAndAcquireShared(opCtx, *nss);

            if ((_chunkCloner = MigrationSourceManager::getCurrentCloner(*scopedCsr))) {
                invariant(_chunkCloner);
            } else {
                uasserted(ErrorCodes::IllegalOperation,
                          str::stream() << "No active migrations were found for collection "
                                        << nss->toStringForErrorMsg());
            }
        }

        // Ensure the session ids are correct
        uassert(ErrorCodes::IllegalOperation,
                str::stream() << "Requested migration session id " << migrationSessionId.toString()
                              << " does not match active session id "
                              << _chunkCloner->getSessionId().toString(),
                migrationSessionId.matches(_chunkCloner->getSessionId()));


        if (!holdCollectionLock)
            _autoColl = boost::none;
    }

    const CollectionPtr& getColl() const {
        invariant(_autoColl);
        return _autoColl->getCollection();
    }

    MigrationChunkClonerSource* getCloner() const {
        invariant(_chunkCloner);
        return _chunkCloner.get();
    }

private:
    // Scoped database + collection lock
    boost::optional<AutoGetCollection> _autoColl;

    // Contains the active cloner for the namespace
    std::shared_ptr<MigrationChunkClonerSource> _chunkCloner;
};


/*
完整的迁移时序图
sequenceDiagram
    participant Config as 配置服务器
    participant Source as 源分片
    participant Dest as 目标分片
    participant Mongos as 路由器
    
    Note over Config,Mongos: 阶段1: 迁移启动
    Config->>Source: _shardsvrMoveRange 命令
    Source->>Source: MigrationSourceManager 构造
    Source->>Source: 元数据验证和冲突检测
    Source->>Source: startClone() - 状态转换 kCreated→kCloning
    
    Note over Source,Dest: 阶段2: 数据克隆
    Source->>Dest: _recvChunkStart 命令，Dest RecvChunkStartCommand 接收该命令
    Dest->>Source: 确认启动成功
    
    loop 初始数据克隆循环
        Dest->>Source: _migrateClone 请求
        Source->>Source: nextCloneBatch() 获取数据
        Source->>Dest: 返回文档批次
        Dest->>Dest: 插入文档到本地
    end
    
    Note over Source,Dest: 阶段3: 写操作追踪（并行进行）
    Note right of Source: OpObserver 持续捕获写操作
    Source->>Source: onInsertOp/onUpdateOp/onDeleteOp
    Source->>Source: 添加ID到内存队列
    
    loop 增量同步循环
        Dest->>Source: _transferMods 请求
        Source->>Source: nextModsBatch() 获取修改
        Source->>Dest: 返回增量变更
        Dest->>Dest: 应用修改操作
    end
    
    Note over Source,Dest: 阶段4: 关键区域准入
    Source->>Source: awaitToCatchUp()
    Source->>Source: awaitUntilCriticalSectionIsAppropriate()
    
    loop 状态检查循环
        Source->>Dest: _recvChunkStatus 查询
        Dest->>Source: 返回状态和进度
        Source->>Source: 判断未传输数据是否<5%
    end
    
    Note over Source,Dest: 阶段5: 关键区域（写操作阻塞）
    Source->>Source: enterCriticalSection() - 阻塞写操作
    Source->>Dest: _recvChunkCommit 命令
    Dest->>Source: 确认提交成功
    
    Note over Source,Config: 阶段6: 元数据提交
    Source->>Config: CommitChunkMigration 请求
    Config->>Config: 更新chunk所有权
    Config->>Source: 提交成功确认
    
    Note over Source,Mongos: 阶段7: 清理和通知
    Source->>Source: forceShardFilteringMetadataRefresh()
    Source->>Dest: 异步释放关键区域
    Source->>Source: 退出关键区域，恢复写操作
    Source->>Config: 记录变更日志
    
    Note over Config,Mongos: 路由器自动发现变更并更新路由表
   */


// 接收端接收到发送端发送的 _recvChunkStart 命令后启动克隆流程，接收端通过 RecvChunkStartCommand  MigrationDestinationManager::_migrateDriver 发送 _migrateClone 给发送端，
// 发送端接收到 _migrateClone 命令后，启动克隆流程，调用nextCloneBatch发送数据给客户端

// *  目标分片：MigrationBatchFetcher<Inserter>::_fetchBatch 发送 _migrateClone 请求给源分片
// *  源分片：InitialCloneCommand::run 接收 _migrateClone 并执行数据获取，
class InitialCloneCommand : public BasicCommand {
public:
    InitialCloneCommand() : BasicCommand("_migrateClone") {}

    bool skipApiVersionCheck() const override {
        // Internal command (server to server).
        return true;
    }

    std::string help() const override {
        return "internal";
    }

    bool supportsWriteConcern(const BSONObj& cmd) const override {
        return false;
    }

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kNever;
    }

    bool adminOnly() const override {
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

    /**
     * InitialCloneCommand::run 函数的作用：
     * 处理接收端发送的 _migrateClone 命令，负责源端向目标分片发送初始克隆数据的核心逻辑。
     * 
     * 核心功能：
     * 1. 会话验证：验证迁移会话ID的有效性，确保命令来源合法
     * 2. 活跃克隆器获取：获取当前正在进行迁移的克隆器实例
     * 3. 批量数据收集：通过循环调用优化单次传输的数据量
     * 4. 缓冲区管理：动态调整缓冲区大小以最大化传输效率
     * 5. 数据返回：将收集到的文档数组返回给接收端
     * 
     * 执行流程：
     * - 解析迁移会话ID并验证其合法性
     * - 获取活跃的迁移克隆器和集合锁
     * - 循环调用 nextCloneBatch 直到无法获取更多数据
     * - 优化传输批次大小以减少网络往返次数
     * - 构建响应对象并返回文档数组
     * 
     * 优化策略：
     * - 缓冲区大小优化：使用克隆器推荐的初始缓冲区大小
     * - 循环收集机制：持续收集数据直到批次大小不再增长
     * - 锁持有优化：在数据收集期间持有必要的集合锁
     * 
     * 错误处理：
     * - 会话ID不匹配：返回 IllegalOperation 错误
     * - 集合不存在：返回 NamespaceNotFound 错误
     * - 权限检查失败：返回未授权错误
     * - 克隆器状态异常：返回相应的状态错误
     * 
     * 该函数是 chunk 迁移数据传输的关键入口点，确保数据能够高效、安全地从源分片传输到目标分片。
     */
    bool InitialCloneCommand::run(OperationContext* opCtx,
                                  const DatabaseName&,
                                  const BSONObj& cmdObj,
                                  BSONObjBuilder& result) override { // InitialCloneCommand::run
        // 迁移会话ID提取和验证：
        // 功能：从命令对象中提取迁移会话标识符
        // 安全性：确保命令来自合法的迁移会话，防止非法访问
        // 错误处理：如果会话ID格式不正确或缺失，会抛出异常
        const MigrationSessionId migrationSessionId(
            uassertStatusOK(MigrationSessionId::extractFromBSON(cmdObj)));
    
        // 数组构建器初始化：
        // 目的：用于收集要传输的文档数据
        // 延迟初始化：在获取克隆器后才创建，使用优化的缓冲区大小
        // 类型：boost::optional 允许延迟构造和条件检查
        boost::optional<BSONArrayBuilder> arrBuilder;
    
        // Try to maximize on the size of the buffer, which we are returning in order to have less
        // round-trips
        // 批次大小跟踪变量：
        // 功能：记录上一次迭代时数组的大小，用于循环终止条件判断
        // 优化目标：最大化缓冲区使用率，减少网络往返次数
        // 初始值：-1 确保第一次循环能够执行
        int arrSizeAtPrevIteration = -1;
    
        // 批量数据收集的优化循环：
        // 终止条件1：arrBuilder 未初始化（首次执行）
        // 终止条件2：当前批次大小大于上次迭代（还有数据可收集）
        // 目标：在单次命令响应中传输尽可能多的数据，减少网络往返
        while (!arrBuilder || arrBuilder->arrSize() > arrSizeAtPrevIteration) {
            // 获取活跃克隆器：
            // 功能：验证迁移会话并获取对应的克隆器实例
            // 参数1：操作上下文，用于权限检查和锁管理
            // 参数2：迁移会话ID，确保访问正确的迁移实例
            // 参数3：true 表示需要持有集合锁，保证迁移状态稳定
            // 安全性：自动验证会话ID匹配性和集合存在性
            AutoGetActiveCloner autoCloner(opCtx, migrationSessionId, true);
    
            // 数组构建器的延迟初始化：
            // 时机：在获取到克隆器后进行初始化
            // 优化：使用克隆器推荐的缓冲区分配大小
            // 好处：避免频繁的内存重新分配，提高性能
            if (!arrBuilder) {
                arrBuilder.emplace(autoCloner.getCloner()->getCloneBatchBufferAllocationSize());
            }
    
            // 记录当前迭代的数组大小：
            // 用途：为下次循环条件判断提供基准
            // 逻辑：如果下次调用后大小没有增长，说明没有更多数据
            arrSizeAtPrevIteration = arrBuilder->arrSize();
    
            // ★ 核心数据获取调用：
            // 功能：从克隆器获取下一批要迁移的文档数据
            // 参数1：操作上下文，提供事务和锁上下文
            // 参数2：集合对象，用于数据读取
            // 参数3：数组构建器指针，用于接收文档数据
            // 重要性：这是实际数据传输的关键调用点
            // InitialCloneCommand::run调用，获取本次需要迁移的 doc 添加到 arrBuilder 中。
            uassertStatusOK(autoCloner.getCloner()->nextCloneBatch(
                opCtx, autoCloner.getColl(), arrBuilder.get_ptr()));
        }
    
        // 数组构建器有效性验证：
        // 目的：确保在循环过程中数组构建器已被正确初始化
        // 防护：避免在异常情况下返回空响应
        invariant(arrBuilder);
        
        // 构建命令响应：
        // 功能：将收集到的文档数组添加到响应对象中
        // 字段名："objects" - 接收端期望的文档数组字段
        // 数据：arrBuilder->arr() 返回构建的BSON数组
        // 用途：接收端将解析此数组并插入到本地集合中
        result.appendArray("objects", arrBuilder->arr());
    
        // 命令执行成功标识：
        // 返回值：true 表示命令成功执行
        // 效果：MongoDB 会将 result 对象作为成功响应返回给客户端
        return true;
    }

};
MONGO_REGISTER_COMMAND(InitialCloneCommand).forShard();

class TransferModsCommand : public BasicCommand {
public:
    TransferModsCommand() : BasicCommand("_transferMods") {}

    bool skipApiVersionCheck() const override {
        // Internal command (server to server).
        return true;
    }

    std::string help() const override {
        return "internal";
    }

    bool supportsWriteConcern(const BSONObj& cmd) const override {
        return false;
    }

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kNever;
    }

    bool adminOnly() const override {
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
        const MigrationSessionId migrationSessionId(
            uassertStatusOK(MigrationSessionId::extractFromBSON(cmdObj)));

        AutoGetActiveCloner autoCloner(opCtx, migrationSessionId, true);

        uassertStatusOK(autoCloner.getCloner()->nextModsBatch(opCtx, &result));
        return true;
    }
};
MONGO_REGISTER_COMMAND(TransferModsCommand).forShard();

/**
 * Command for extracting the oplog entries that needs to be migrated for the given migration
 * session id.
 * Note: this command is not stateless. Calling this command has a side-effect of gradually
 * depleting the buffer that contains the oplog entries to be transfered.
 */
class MigrateSessionCommand : public BasicCommand {
public:
    MigrateSessionCommand() : BasicCommand("_getNextSessionMods") {}

    bool skipApiVersionCheck() const override {
        // Internal command (server to server).
        return true;
    }

    std::string help() const override {
        return "internal";
    }

    bool supportsWriteConcern(const BSONObj& cmd) const override {
        return false;
    }

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kNever;
    }

    bool adminOnly() const override {
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

/**
 * fetchNextSessionMigrationBatch 函数的作用：
 * 获取下一批需要传输的会话相关oplog条目并将其附加到给定的数组构建器中。
 * 这是MongoDB分片迁移过程中处理事务和会话数据的核心函数。
 * 
 * 核心功能：
 * 1. 会话oplog批次获取：从克隆器获取下一批会话相关的oplog条目
 * 2. 异步通知机制：当无可用数据时返回通知对象，支持等待新数据
 * 3. 写关注确认：确保获取的oplog条目已在大多数节点上持久化
 * 4. 回滚检测：通过回滚ID验证数据一致性，防止回滚导致的数据错误
 * 5. 写冲突重试：处理并发访问oplog时可能出现的冲突
 * 
 * 返回值说明：
 * - 返回nullptr：成功获取到oplog数据，调用方可以处理arrBuilder中的数据
 * - 返回Notification对象：当前无可用数据，调用方需要等待通知
 *   - 通知值为true：迁移已进入关键区域或中止，无更多数据
 *   - 通知值为false：有新的oplog批次可用，可以重新调用
 * 
 * 安全保障：
 * - 写关注等待：确保oplog条目的持久性和一致性
 * - 回滚检测：防止已回滚的数据被错误迁移
 * - 会话验证：确保访问正确的迁移会话
 * 
 * 使用场景：
 * - 在chunk迁移过程中传输事务相关的oplog数据
 * - 确保多文档事务在分片迁移中的一致性
 * - 支持MongoDB会话数据的安全迁移
 * 
 * 性能优化：
 * - 写冲突重试机制：处理高并发场景下的冲突
 * - 批次处理：减少网络往返次数
 * - 异步通知：避免忙等待，提高系统效率
 * 
 * 该函数是MigrateSessionCommand的核心实现，确保事务和会话数据的完整迁移。
 * 
 * 参数说明：
 * @param opCtx 操作上下文，提供事务和权限信息
 * @param migrationSessionId 迁移会话ID，确保会话一致性
 * @param arrBuilder 数组构建器指针，用于接收oplog数据
 * 
 * @return std::shared_ptr<Notification<bool>> 异步通知对象或nullptr
 */
/**
 * Fetches the next batch of oplog that needs to be transferred and appends it to the given
 * array builder. If it was not able to fetch anything, it will return a non-null notification
 * that will get signalled when new batches comes in or when migration is over. If the boolean
 * value from the notification returns true, then the migration has entered the critical
 * section or aborted and there's no more new batches to fetch.
 */
std::shared_ptr<Notification<bool>> fetchNextSessionMigrationBatch(
    OperationContext* opCtx,
    const MigrationSessionId& migrationSessionId,
    BSONArrayBuilder* arrBuilder) {
    // 操作时间戳：用于后续的写关注等待
    // 如果获取到oplog数据，将包含最新操作的时间戳
    // 用于确保在返回给调用方之前，数据已在大多数节点上持久化
    boost::optional<repl::OpTime> opTime;
    
    // 新oplog通知对象：当没有可用数据时用于异步等待
    // 如果当前没有可用的会话oplog数据，将返回此通知对象
    // 调用方可以通过此对象等待新数据到达或迁移完成的信号
    std::shared_ptr<Notification<bool>> newOplogNotification;

    // 写冲突重试机制：处理并发写操作可能导致的冲突
    // 在高并发环境下，多个操作可能同时访问oplog，导致读取冲突
    // 此机制确保即使发生写冲突，也能重试并最终成功获取数据
    writeConflictRetry(
        opCtx,
        "Fetching session related oplogs for migration",  // 操作描述，用于日志和调试
        NamespaceString::kRsOplogNamespace,               // oplog集合命名空间
        [&]() {
            // 获取活跃克隆器：
            // 参数false表示不需要持有集合锁，因为只是读取会话数据
            // 这避免了在长时间运行的会话数据获取过程中持有不必要的锁
            AutoGetActiveCloner autoCloner(opCtx, migrationSessionId, false);
            
            // ★ 核心会话数据获取调用：
            // 功能：从克隆器获取下一批需要迁移的会话相关oplog条目
            // 参数1：操作上下文，提供事务和权限上下文
            // 参数2：数组构建器指针，用于接收oplog数据
            // 返回值：最新处理的oplog操作时间戳，用于后续写关注等待
            // 重要性：这是实际获取会话oplog数据的关键调用点
            opTime = autoCloner.getCloner()->nextSessionMigrationBatch(opCtx, arrBuilder);

            // 检查是否获取到数据：
            // 如果数组大小为0，说明当前没有可用的会话oplog数据
            // 这种情况下需要获取通知对象，以便等待新数据的到达
            if (arrBuilder->arrSize() == 0) {
                // 获取通知对象：用于等待新的会话迁移批次数据
                // 当有新数据可用或迁移状态改变时，此通知会被触发
                // 这实现了高效的异步等待机制，避免了忙等待
                newOplogNotification =
                    autoCloner.getCloner()->getNotificationForNextSessionMigrationBatch();
            }
        });

    // 如果有通知对象，说明当前没有数据，返回通知让调用方等待
    // 调用方需要通过notification->get()方法等待信号
    // 这种设计实现了非阻塞的数据获取机制
    if (newOplogNotification) {
        return newOplogNotification;
    }

    // If the batch returns something, we wait for write concern to ensure that all the entries
    // in the batch have been majority committed. We then need to check that the rollback id
    // hasn't changed since we started migration, because a change would indicate that some data
    // in this batch may have been rolled back. In this case, we abort the migration.
    // 如果批次返回了数据，我们需要等待写关注以确保批次中的所有条目都已在大多数节点上提交。
    // 然后我们需要检查回滚ID自迁移开始以来是否发生变化，因为变化表明此批次中的某些数据可能已被回滚。
    // 在这种情况下，我们会中止迁移。
    if (opTime) {
        // 写关注等待：确保oplog条目在大多数节点上持久化
        // 这是数据一致性的关键保障，确保获取的oplog条目不会因为副本集状态变化而丢失
        WriteConcernResult wcResult;
        WriteConcernOptions majorityWC{WriteConcernOptions::kMajority,        // 大多数节点确认
                                       WriteConcernOptions::SyncMode::UNSET,   // 使用默认同步模式
                                       WriteConcernOptions::kNoTimeout};       // 无超时限制
        
        // 等待指定操作时间的写关注完成
        // 这确保了当前批次的oplog条目已经安全地复制到大多数副本节点
        // 只有在此步骤完成后，才能安全地将数据传输给目标分片
        uassertStatusOK(waitForWriteConcern(opCtx, opTime.value(), majorityWC, &wcResult));

        // 获取迁移初始化时的回滚ID：用于检测是否发生了回滚
        // 回滚ID是副本集的一个重要标识，当发生回滚时会发生变化
        // 通过比较初始值和当前值，可以检测到数据回滚事件
        auto rollbackIdAtMigrationInit = [&]() {
            AutoGetActiveCloner autoCloner(opCtx, migrationSessionId, false);
            return autoCloner.getCloner()->getRollbackIdAtInit();
        }();

        // The check for rollback id must be done after having waited for majority in order to
        // ensure that whatever was waited on didn't get rolled back.
        // 回滚ID检查必须在等待大多数确认之后进行，以确保等待的内容没有被回滚。
        
        // 获取当前的回滚ID：与初始值进行比较
        // 如果两者不同，说明在迁移过程中发生了回滚，必须中止迁移
        auto rollbackId = repl::ReplicationProcess::get(opCtx)->getRollbackID();
        
        // 回滚检测：如果回滚ID发生变化，说明发生了回滚，必须中止迁移
        // 这是关键的安全检查，防止已回滚的数据被错误地迁移到目标分片
        // 回滚会导致oplog中的某些条目无效，如果不检测可能导致数据不一致
        uassert(50881,
                str::stream() << "rollback detected, rollbackId was "
                              << rollbackIdAtMigrationInit << " but is now " << rollbackId,
                rollbackId == rollbackIdAtMigrationInit);
    }

    // 返回nullptr表示成功获取了数据，不需要等待通知
    // 调用方可以继续处理arrBuilder中的oplog数据
    // 这种返回值设计清晰地区分了"有数据"和"需要等待"两种情况
    return nullptr;
}

    /**
     * MigrateSessionCommand::run 函数的作用：
     * 处理目标分片发送的 _getNextSessionMods 命令，负责提取和传输与会话相关的 oplog 条目。
     * 这是 MongoDB 分片迁移过程中处理事务和会话相关数据的关键命令。
     * 
     * 核心功能：
     * 1. 会话 oplog 提取：获取需要迁移的会话相关 oplog 条目
     * 2. 批次处理：支持分批传输大量会话数据，避免内存溢出
     * 3. 写关注确认：确保 oplog 条目已经在大多数节点上持久化
     * 4. 回滚检测：通过 rollback ID 检测并防止数据回滚导致的不一致
     * 5. 异步通知机制：当没有数据时提供通知机制，支持等待新数据
     * 
     * 使用场景：
     * - 在 chunk 迁移过程中传输会话相关的 oplog 数据
     * - 确保事务操作在迁移过程中的一致性
     * - 支持 MongoDB 的多文档事务在分片环境中的正确迁移
     * 
     * 状态管理：
     * - 该命令不是无状态的：调用会逐渐消耗包含 oplog 条目的缓冲区
     * - 每次调用都会推进内部状态，直到所有相关 oplog 被传输完毕
     * 
     * 安全保障：
     * - 写关注等待：确保 oplog 条目在大多数节点上提交
     * - 回滚检测：防止因回滚导致的数据不一致
     * - 会话验证：确保访问正确的迁移会话
     * 
     * 错误处理：
     * - 会话不匹配：返回 IllegalOperation 错误
     * - 回滚检测：返回错误并中止迁移
     * - 克隆器状态异常：返回相应的状态错误
     * 
     * 与其他命令的关系：
     * - InitialCloneCommand：传输基础数据
     * - TransferModsCommand：传输数据修改
     * - MigrateSessionCommand：传输会话和事务相关数据
     * 
     * 该函数确保了 MongoDB 事务和会话数据在分片迁移过程中的完整性和一致性。
     */
    bool run(OperationContext* opCtx,
             const DatabaseName&,
             const BSONObj& cmdObj,
             BSONObjBuilder& result) override {
        // 迁移会话ID提取和验证：
        // 功能：从命令对象中提取迁移会话标识符
        // 安全性：确保命令来自合法的迁移会话，防止非法访问
        // 错误处理：如果会话ID格式不正确或缺失，会抛出异常
        const MigrationSessionId migrationSessionId(
            uassertStatusOK(MigrationSessionId::extractFromBSON(cmdObj)));

        // 初始化会话批次数组构建器：用于收集会话相关的 oplog 条目
        BSONArrayBuilder arrBuilder;
        // 迁移完成标志：指示迁移是否已进入关键区域或已中止
        bool hasMigrationCompleted = false;

        // 循环获取会话 oplog 批次，直到获得数据或迁移完成
        do {
            // 尝试获取下一批会话迁移数据
            // 如果返回非空通知对象，说明当前没有可用数据，需要等待
            if (auto newOplogNotification =
                    fetchNextSessionMigrationBatch(opCtx, migrationSessionId, &arrBuilder)) {
                // 等待通知信号：可能是新数据到达或迁移进入关键区域/中止
                // get() 方法会阻塞直到收到通知
                // 返回值：true 表示迁移已完成，false 表示有新数据可用
                hasMigrationCompleted = newOplogNotification->get(opCtx);
            } else if (arrBuilder.arrSize() == 0) {
                // If we didn't get a notification and the arrBuilder is empty, that means
                // that the sessionMigration is not active for this migration (most likely
                // because it's not a replica set).
                // 如果没有获得通知且数组构建器为空，说明会话迁移对此迁移不活跃
                // （很可能因为这不是一个副本集环境）
                hasMigrationCompleted = true;
            }
            // 继续循环条件：数组为空且迁移未完成
            // 这确保了函数会持续尝试获取数据，直到有数据返回或迁移结束
        } while (arrBuilder.arrSize() == 0 && !hasMigrationCompleted);

        // 将收集到的 oplog 条目数组添加到响应中
        // 字段名 "oplog"：目标分片期望的会话 oplog 数组字段
        // 即使为空数组，也会返回，让目标分片知道当前状态
        result.appendArray("oplog", arrBuilder.arr());

        // 命令执行成功标识：
        // 返回值：true 表示命令成功执行
        // 效果：MongoDB 会将 result 对象作为成功响应返回给目标分片
        return true;
    }
};
MONGO_REGISTER_COMMAND(MigrateSessionCommand).forShard();

}  // namespace
}  // namespace mongo
