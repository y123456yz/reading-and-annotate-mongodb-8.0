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

// 接收端接收到发送端发送的 _recvChunkStart 命令后启动克隆流程，接收端通过MigrationDestinationManager::_migrateDriver发送_migrateClone给发送端，
// 发送端接收到 _migrateClone 命令后，启动克隆流程，调用nextCloneBatch发送数据给客户端
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
                                  BSONObjBuilder& result) override {
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
        boost::optional<repl::OpTime> opTime;
        std::shared_ptr<Notification<bool>> newOplogNotification;

        writeConflictRetry(
            opCtx,
            "Fetching session related oplogs for migration",
            NamespaceString::kRsOplogNamespace,
            [&]() {
                AutoGetActiveCloner autoCloner(opCtx, migrationSessionId, false);
                opTime = autoCloner.getCloner()->nextSessionMigrationBatch(opCtx, arrBuilder);

                if (arrBuilder->arrSize() == 0) {
                    newOplogNotification =
                        autoCloner.getCloner()->getNotificationForNextSessionMigrationBatch();
                }
            });

        if (newOplogNotification) {
            return newOplogNotification;
        }

        // If the batch returns something, we wait for write concern to ensure that all the entries
        // in the batch have been majority committed. We then need to check that the rollback id
        // hasn't changed since we started migration, because a change would indicate that some data
        // in this batch may have been rolled back. In this case, we abort the migration.
        if (opTime) {
            WriteConcernResult wcResult;
            WriteConcernOptions majorityWC{WriteConcernOptions::kMajority,
                                           WriteConcernOptions::SyncMode::UNSET,
                                           WriteConcernOptions::kNoTimeout};
            uassertStatusOK(waitForWriteConcern(opCtx, opTime.value(), majorityWC, &wcResult));

            auto rollbackIdAtMigrationInit = [&]() {
                AutoGetActiveCloner autoCloner(opCtx, migrationSessionId, false);
                return autoCloner.getCloner()->getRollbackIdAtInit();
            }();

            // The check for rollback id must be done after having waited for majority in order to
            // ensure that whatever was waited on didn't get rolled back.
            auto rollbackId = repl::ReplicationProcess::get(opCtx)->getRollbackID();
            uassert(50881,
                    str::stream() << "rollback detected, rollbackId was "
                                  << rollbackIdAtMigrationInit << " but is now " << rollbackId,
                    rollbackId == rollbackIdAtMigrationInit);
        }

        return nullptr;
    }

    bool run(OperationContext* opCtx,
             const DatabaseName&,
             const BSONObj& cmdObj,
             BSONObjBuilder& result) override {
        const MigrationSessionId migrationSessionId(
            uassertStatusOK(MigrationSessionId::extractFromBSON(cmdObj)));

        BSONArrayBuilder arrBuilder;
        bool hasMigrationCompleted = false;

        do {
            if (auto newOplogNotification =
                    fetchNextSessionMigrationBatch(opCtx, migrationSessionId, &arrBuilder)) {
                hasMigrationCompleted = newOplogNotification->get(opCtx);
            } else if (arrBuilder.arrSize() == 0) {
                // If we didn't get a notification and the arrBuilder is empty, that means
                // that the sessionMigration is not active for this migration (most likely
                // because it's not a replica set).
                hasMigrationCompleted = true;
            }
        } while (arrBuilder.arrSize() == 0 && !hasMigrationCompleted);

        result.appendArray("oplog", arrBuilder.arr());

        return true;
    }
};
MONGO_REGISTER_COMMAND(MigrateSessionCommand).forShard();

}  // namespace
}  // namespace mongo
