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

#pragma once

#include <boost/move/utility_core.hpp>
#include <boost/none.hpp>
#include <boost/optional/optional.hpp>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <set>
#include <vector>

#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/timestamp.h"
#include "mongo/client/connection_string.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/op_observer/op_observer_util.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/query/internal_plans.h"
#include "mongo/db/query/plan_executor.h"
#include "mongo/db/record_id.h"
#include "mongo/db/repl/oplog_entry.h"
#include "mongo/db/repl/optime.h"
#include "mongo/db/s/migration_chunk_cloner_source.h"
#include "mongo/db/s/migration_session_id.h"
#include "mongo/db/s/session_catalog_migration_source.h"
#include "mongo/db/session/logical_session_id.h"
#include "mongo/db/session/logical_session_id_gen.h"
#include "mongo/db/storage/recovery_unit.h"
#include "mongo/db/storage/snapshot.h"
#include "mongo/db/write_concern_options.h"
#include "mongo/platform/mutex.h"
#include "mongo/s/request_types/move_range_request_gen.h"
#include "mongo/s/shard_key_pattern.h"
#include "mongo/stdx/condition_variable.h"
#include "mongo/util/assert_util_core.h"
#include "mongo/util/concurrency/notification.h"
#include "mongo/util/concurrency/with_lock.h"
#include "mongo/util/duration.h"
#include "mongo/util/net/hostandport.h"
#include "mongo/util/time_support.h"
#include "mongo/util/uuid.h"

namespace mongo {

class BSONArrayBuilder;
class BSONObjBuilder;
class Collection;
class CollectionPtr;
class Database;
class MigrationChunkClonerSource;
class RecordId;

// Overhead to prevent mods buffers from being too large
const long long kFixedCommandOverhead = 32 * 1024;

/**
 * Used to commit work for LogOpForSharding. Used to keep track of changes in documents that are
 * part of a chunk being migrated.
 */
class LogTransactionOperationsForShardingHandler final : public RecoveryUnit::Change {
public:
    LogTransactionOperationsForShardingHandler(LogicalSessionId lsid,
                                               const std::vector<repl::OplogEntry>& stmts,
                                               repl::OpTime prepareOrCommitOpTime);

    LogTransactionOperationsForShardingHandler(LogicalSessionId lsid,
                                               const std::vector<repl::ReplOperation>& stmts,
                                               repl::OpTime prepareOrCommitOpTime);

    void commit(OperationContext* opCtx, boost::optional<Timestamp>) override;

    void rollback(OperationContext* opCtx) override{};

private:
    const LogicalSessionId _lsid;
    std::vector<repl::ReplOperation> _stmts;
    const repl::OpTime _prepareOrCommitOpTime;
};

/**
 * Used to keep track of inserts that can be potentially added as xferMods of a migration.
 */
class LogInsertForShardingHandler final : public RecoveryUnit::Change {
public:
    LogInsertForShardingHandler(NamespaceString nss, BSONObj doc, repl::OpTime opTime);

    void commit(OperationContext* opCtx, boost::optional<Timestamp>) override;

    void rollback(OperationContext* opCtx) override {}

private:
    const NamespaceString _nss;
    const BSONObj _doc;
    const repl::OpTime _opTime;
};

/**
 * Used to keep track of updates that can be potentially added as xferMods of a migration.
 */
class LogUpdateForShardingHandler final : public RecoveryUnit::Change {
public:
    LogUpdateForShardingHandler(NamespaceString nss,
                                boost::optional<BSONObj> preImageDoc,
                                BSONObj postImageDoc,
                                repl::OpTime opTime);

    void commit(OperationContext* opCtx, boost::optional<Timestamp>) override;

    void rollback(OperationContext* opCtx) override {}

private:
    const NamespaceString _nss;
    const boost::optional<BSONObj> _preImageDoc;
    const BSONObj _postImageDoc;
    const repl::OpTime _opTime;
};

/**
 * Used to keep track of deletes that can be potentially added as xferMods of a migration.
 */
class LogDeleteForShardingHandler final : public RecoveryUnit::Change {
public:
    LogDeleteForShardingHandler(NamespaceString nss, DocumentKey documentKey, repl::OpTime opTime);

    void commit(OperationContext* opCtx, boost::optional<Timestamp>) override;

    void rollback(OperationContext* opCtx) override {}

private:
    const NamespaceString _nss;
    const DocumentKey _documentKey;
    const repl::OpTime _opTime;
};

/**
 * Used to keep track of retryable applyOps that may include changes in documents that are
 * part of a chunk being migrated.  Only takes care of retryability; the actual operations
 * are broken out and added to the transfer mods  by the other handlers.
 */
class LogRetryableApplyOpsForShardingHandler final : public RecoveryUnit::Change {
public:
    LogRetryableApplyOpsForShardingHandler(std::vector<NamespaceString> namespaces,
                                           std::vector<repl::OpTime> opTimes);

    void commit(OperationContext* opCtx, boost::optional<Timestamp>) override;

    void rollback(OperationContext* opCtx) override{};

private:
    std::vector<NamespaceString> _namespaces;
    std::vector<repl::OpTime> _opTimes;
};

/**
 * This class is responsible for producing chunk documents to be moved from donor to a recipient
 * shard and its methods represent cloning stages. Its lifetime is owned and controlled by a single
 * migration source manager which registers it for notifications from the replication subsystem
 * before calling startClone.
 *
 * Unless explicitly indicated, the methods on this class are not thread-safe.
 *
 * The pattern of using this interface is such that one thread instantiates it and registers it so
 * it begins receiving notifications from the replication subsystem through the
 * on[insert/update/delete]Op methods. It is up to the creator to decide how these methods end up
 * being called, but currently this is done through the CollectionShardingState. The creator then
 * kicks off the cloning as soon as possible by calling startClone.
 */

/**
 * MigrationChunkClonerSource 类的作用：
 * MongoDB 分片迁移中的源端克隆器，负责将 chunk 数据从源分片传输到目标分片的完整生命周期管理。
 * 
 * 核心职责：
 * 1. 数据克隆管理：管理初始数据克隆和增量变更传输的完整流程
 * 2. 写操作追踪：通过 OpObserver 机制捕获迁移期间的并发写操作
 * 3. 会话数据迁移：处理事务和可重试写操作相关的会话数据传输
 * 4. 状态协调：与接收端协调迁移状态，决定何时进入关键区域
 * 5. 内存管理：高效管理迁移过程中的内存使用，防止溢出
 * 6. 网络通信：处理与接收端的所有网络交互和错误恢复
 * 
 * 设计特点：
 * - 双策略支持：常规块使用记录ID随机访问，巨型块使用索引扫描
 * - 异步操作追踪：写操作正常执行，OpObserver异步收集变更用于传输
 * - 内存友好：只存储文档ID，传输时获取完整文档，大幅降低内存使用
 * - 智能批处理：动态调整传输批次大小，优化网络效率
 * - 状态机管理：清晰的状态转换和错误处理机制
 * 
 * 线程安全性：
 * - 大部分方法非线程安全，需要外部同步
 * - CloneList 类是多线程安全的，支持并发访问
 * - 使用互斥锁保护共享状态和队列操作
 * 
 * 使用模式：
 * 1. 构造并注册到复制子系统接收写操作通知
 * 2. 调用 startClone() 启动克隆过程
 * 3. 通过 nextCloneBatch() 传输初始数据
 * 4. 通过 nextModsBatch() 传输增量变更
 * 5. 使用 awaitUntilCriticalSectionIsAppropriate() 等待进入关键区域
 * 6. 调用 commitClone() 完成迁移提交
 * 
 * 性能优化：
 * - 预分配缓冲区大小基于平均文档大小
 * - 并发I/O支持通过记录ID迭代器实现
 * - 让步机制防止长时间占用资源
 * - 去重传输避免同一文档的重复处理
 * 
 * 错误处理：
 * - 支持迁移中断后的清理和恢复
 * - 网络异常时的重试机制
 * - 内存限制检查和保护
 * - 状态一致性验证和修复
 * 
 * 该类是 MongoDB 分片系统中最核心的组件之一，确保了数据迁移的高效性、可靠性和一致性。
 */

/**
 * This class is responsible for producing chunk documents to be moved from donor to a recipient
 * shard and its methods represent cloning stages. Its lifetime is owned and controlled by a single
 * migration source manager which registers it for notifications from the replication subsystem
 * before calling startClone.
 *
 * Unless explicitly indicated, the methods on this class are not thread-safe.
 *
 * The pattern of using this interface is such that one thread instantiates it and registers it so
 * it begins receiving notifications from the replication subsystem through the
 * on[insert/update/delete]Op methods. It is up to the creator to decide how these methods end up
 * being called, but currently this is done through the CollectionShardingState. The creator then
 * kicks off the cloning as soon as possible by calling startClone.
 */
class MigrationChunkClonerSource {
    MigrationChunkClonerSource(const MigrationChunkClonerSource&) = delete;
    MigrationChunkClonerSource& operator=(const MigrationChunkClonerSource&) = delete;

public:
    // 构造函数：初始化迁移克隆器的基本参数
    // 功能：设置迁移请求参数、网络连接信息、分片键模式等核心配置
    // 参数：操作上下文、迁移请求、写关注选项、分片键模式、源端连接串、接收端主机
    MigrationChunkClonerSource(OperationContext* opCtx,
                               const ShardsvrMoveRange& request,
                               const WriteConcernOptions& writeConcern,
                               const BSONObj& shardKeyPattern,
                               ConnectionString donorConnStr,
                               HostAndPort recipientHost);
    
    // 析构函数：确保资源清理和状态一致性
    ~MigrationChunkClonerSource();

    /**
     * Blocking method, which prepares the object for serving as a source for migrations and tells
     * the recipient shard to start cloning. Before calling this method, this chunk cloner must be
     * registered for notifications from the replication subsystem (not checked here).
     *
     * NOTE: Must be called without any locks and must succeed, before any other methods are called
     * (except for cancelClone and [insert/update/delete]Op).
     */
    // 启动克隆：迁移流程的核心启动方法
    // 功能1：初始化会话目录源，准备会话数据迁移
    // 功能2：收集要迁移的记录ID或启用巨型块模式
    // 功能3：通知接收端开始接收克隆数据
    // 功能4：状态转换为 kCloning，激活 OpObserver 写操作收集
    // 前置条件：必须在无锁状态下调用，必须成功执行
    Status startClone(OperationContext* opCtx,
                      const UUID& migrationId,
                      const LogicalSessionId& lsid,
                      TxnNumber txnNumber);

    /**
     * Blocking method, which uses some custom selected logic for deciding whether it is appropriate
     * for the donor shard to enter critical section.
     *
     * If it returns a successful status, the caller must as soon as possible stop writes (by
     * entering critical section). On failure it may return any error. Known errors are:
     *  ExceededTimeLimit - if the maxTimeToWait was exceeded
     *
     * NOTE: Must be called without any locks.
     */
    // 关键区域准入判断：智能决策何时可以进入写操作阻塞的关键区域
    // 功能1：监控接收端克隆状态和进度
    // 功能2：评估未传输数据量是否在可接受阈值内（默认<5%）
    // 功能3：检查会话数据迁移完成度
    // 功能4：内存使用监控和保护（限制500MB）
    // 策略：手动强制巨型块立即进入，常规迁移智能等待
    // 超时：默认6小时最大等待时间
    Status awaitUntilCriticalSectionIsAppropriate(OperationContext* opCtx,
                                                  Milliseconds maxTimeToWait);

    /**
     * Tell the recipient shard to commit the documents it has cloned so far. Must be called only
     * when it has been ensured that there will be no more changes happening to documents on the
     * donor shard. If this is not observed, the recipient might miss changes and thus lose data.
     *
     * This must only be called once and no more methods on the cloner must be used afterwards
     * regardless of whether it succeeds or not.
     *
     * Returns statistics about the move. These are informational only and should not be
     * interpreted by the caller for any means other than reporting.
     *
     * NOTE: Must be called without any locks.
     */
    // 提交克隆：通知接收端提交所有已克隆的数据
    // 功能1：发送最终的增量变更到接收端
    // 功能2：接收端执行数据完整性验证和持久化
    // 功能3：获取详细的克隆统计信息
    // 前置条件：必须确保源端不再有写操作发生
    // 一次性操作：只能调用一次，之后不能再使用其他方法
    // 返回值：包含迁移统计信息的BSONObj，用于日志和监控
    StatusWith<BSONObj> commitClone(OperationContext* opCtx);

    /**
     * Tells the recipient to abort the clone and cleanup any unused data. This method's
     * implementation should be idempotent and never throw.
     *
     * NOTE: Must be called without any locks.
     */
    // 取消克隆：通知接收端中止克隆并清理数据
    // 特性：幂等操作，永不抛出异常
    // 用途：错误恢复、主动中止、资源清理
    void cancelClone(OperationContext* opCtx) noexcept;

    /**
     * Notifies this cloner that an insert happened to the collection, which it owns. It is up to
     * the cloner's implementation to decide what to do with this information and it is valid for
     * the implementation to ignore it.
     *
     * NOTE: Must be called with at least IX lock held on the collection.
     */
    // 插入操作通知：OpObserver调用，处理迁移期间的插入操作
    // 功能1：范围检查，只处理在迁移chunk范围内的文档
    // 功能2：添加文档ID到传输队列（_reload）
    // 功能3：更新内存使用统计和计数器
    // 线程安全：需要至少持有集合的IX锁
    // 性能：异步处理，不影响插入操作的执行性能
    void onInsertOp(OperationContext* opCtx,
                    const BSONObj& insertedDoc,
                    const repl::OpTime& opTime);

    /**
     * Notifies this cloner that an update happened to the collection, which it owns. It is up to
     * the cloner's implementation to decide what to do with this information and it is valid for
     * the implementation to ignore it.
     *
     * NOTE: Must be called with at least IX lock held on the collection.
     */
    // 更新操作通知：处理迁移期间的更新操作，包括分片键变更的特殊情况
    // 功能1：检查更新前后文档的范围归属
    // 功能2：处理分片键变更导致的范围移出情况
    // 功能3：添加文档ID到相应的传输队列
    // 复杂性：需要同时考虑preImage和postImage的范围检查
    void onUpdateOp(OperationContext* opCtx,
                    boost::optional<BSONObj> preImageDoc,
                    const BSONObj& postImageDoc,
                    const repl::OpTime& opTime);
    
    /**
     * Notifies this cloner that a delede happened to the collection, which it owns. It is up to the
     * cloner's implementation to decide what to do with this information and it is valid for the
     * implementation to ignore it.
     *
     * NOTE: Must be called with at least IX lock held on the collection.
     */
    // 删除操作通知：处理迁移期间的删除操作
    // 功能1：验证删除文档是否在迁移范围内
    // 功能2：添加文档ID到删除队列（_deleted）
    // 功能3：更新删除操作计数器
    // 用途：确保接收端也删除相应的文档，保持数据一致性
    void onDeleteOp(OperationContext* opCtx,
                    const DocumentKey& documentKey,
                    const repl::OpTime& opTime);

    /**
     * Returns the migration session id associated with this cloner, so stale sessions can be
     * disambiguated.
     */
    // 获取会话ID：用于区分不同的迁移会话，避免过期会话的混淆
    const MigrationSessionId& getSessionId() const {
        return _sessionId;
    }

    /**
     * Returns the rollback ID recorded at the beginning of session migration. If the underlying
     * SessionCatalogMigrationSource does not exist, that means this node is running as a standalone
     * and doesn't support retryable writes, so we return boost::none.
     */
    // 获取回滚ID：用于会话迁移的一致性检查
    // 返回值：boost::none 表示独立模式不支持可重试写操作
    boost::optional<int> getRollbackIdAtInit() const {
        if (_sessionCatalogSource) {
            return _sessionCatalogSource->getRollbackIdAtInit();
        }
        return boost::none;
    }

    /**
     * Called by the recipient shard. Used to estimate how many more bytes of clone data are
     * remaining in the chunk cloner.
     */
    // 缓冲区大小估算：接收端调用，用于优化传输批次大小
    // 功能：基于平均文档大小和剩余文档数量估算最优缓冲区大小
    // 用途：减少网络往返次数，提高传输效率
    uint64_t getCloneBatchBufferAllocationSize();

    /**
     * Called by the recipient shard. Populates the passed BSONArrayBuilder with a set of documents,
     * which are part of the initial clone sequence. Assumes that there is only one active caller
     * to this method at a time (otherwise, it can cause corruption/crash).
     *
     * Returns OK status on success. If there were documents returned in the result argument, this
     * method should be called more times until the result is empty. If it returns failure, it is
     * not safe to call more methods on this class other than cancelClone.
     *
     * This method will return early if too much time is spent fetching the documents in order to
     * give a chance to the caller to perform some form of yielding. It does not free or acquire any
     * locks on its own.
     *
     * NOTE: Must be called with the collection lock held in at least IS mode.
     */
    // 获取克隆批次：接收端调用，获取初始数据克隆的文档批次
    // 双策略实现：
    // - 常规块：基于预存储记录ID的高效随机访问
    // - 巨型块：使用索引扫描的流式处理
    // 性能优化：
    // - 让步机制防止长时间占用资源
    // - 动态批次大小调整
    // - 并发安全的记录ID迭代
    // 前置条件：必须持有集合的IS锁
    Status nextCloneBatch(OperationContext* opCtx,
                          const CollectionPtr& collection,
                          BSONArrayBuilder* arrBuilder);

    /**
     * Called by the recipient shard. Transfers the accummulated local mods from source to
     * destination. Must not be called before all cloned objects have been fetched through calls to
     * nextCloneBatch.
     *
     * NOTE: Must be called with the collection lock held in at least IS mode.
     */
    // 获取增量修改：传输OpObserver收集的写操作变更
    // 功能1：处理延迟的事务修改操作
    // 功能2：原子性获取删除和更新队列快照
    // 功能3：根据ID重新获取文档的最新版本
    // 优化特性：
    // - 去重处理避免重复传输
    // - 分离删除和更新操作的处理
    // - 智能批次大小控制
    // 前置条件：初始克隆必须完成
    Status nextModsBatch(OperationContext* opCtx, BSONObjBuilder* builder);

    /**
     * Appends to 'arrBuilder' oplog entries which wrote to the currently migrated chunk and contain
     * session information.
     *
     * If this function returns a valid OpTime, this means that the oplog appended are not
     * guaranteed to be majority committed and the caller has to wait for the returned opTime to be
     * majority committed before returning them to the donor shard.
     *
     * If the underlying SessionCatalogMigrationSource does not exist, that means this node is
     * running as a standalone and doesn't support retryable writes, so we return boost::none.
     *
     * This waiting is necessary because session migration is only allowed to send out committed
     * entries, as opposed to chunk migration, which can send out uncommitted documents. With chunk
     * migration, the uncommitted documents will not be visibile until the end of the migration
     * commits, which means that if it fails, they won't be visible, whereas session oplog entries
     * take effect immediately since they are appended to the chain.
     */
    // 获取会话迁移批次：传输事务和可重试写操作的会话数据
    // 功能1：获取包含会话信息的oplog条目
    // 功能2：处理多数派提交的等待逻辑
    // 返回值：需要等待多数派提交的OpTime
    // 重要性：会话迁移只能发送已提交的条目，确保一致性
    boost::optional<repl::OpTime> nextSessionMigrationBatch(OperationContext* opCtx,
                                                            BSONArrayBuilder* arrBuilder);

    /**
     * Returns a notification that can be used to wait for new oplog that needs to be migrated.
     * If the value in the notification returns true, it means that there are no more new batches
     * that needs to be fetched because the migration has already entered the critical section or
     * aborted.
     *
     * Returns nullptr if there is no session migration associated with this migration.
     */
    // 获取通知器：用于等待新的会话迁移批次
    // 返回值：true表示迁移进入关键区域或中止，无需更多批次
    std::shared_ptr<Notification<bool>> getNotificationForNextSessionMigrationBatch();

    // 命名空间获取器：返回迁移的集合命名空间
    const NamespaceString& nss() {
        return _args.getCommandParameter();
    }

    // 最小边界获取器：返回迁移chunk的最小边界
    const BSONObj& getMin() {
        invariant(_args.getMin());
        return *_args.getMin();
    }

    // 最大边界获取器：返回迁移chunk的最大边界
    const BSONObj& getMax() {
        invariant(_args.getMax());
        return *_args.getMax();
    }

    /**
     * Returns the number of session oplog entries that were not found but not sent to the
     * destination shard.
     */
    // 获取跳过的会话条目数：用于监控和调试
    boost::optional<long long> getSessionOplogEntriesSkippedSoFarLowerBound();

    /**
     * Returns the number of session oplog entries that need to be sent to the destination shard.
     */
    // 获取待迁移的会话条目数：用于进度监控
    boost::optional<long long> getSessionOplogEntriesToBeMigratedSoFar();

private:
    friend class LogOpForShardingHandler;
    friend class LogTransactionOperationsForShardingHandler;
    friend class LogRetryableApplyOpsForShardingHandler;

    using RecordIdSet = std::set<RecordId>;

    /**
     * This is responsible for all the logic revolving around handling documents that needs to be
     * cloned.
     *
     * This class is multithread-safe.
     */
    // CloneList 类：多线程安全的文档克隆管理器
    // 职责1：管理需要克隆的记录ID集合
    // 职责2：支持并发访问和文档获取
    // 职责3：处理溢出文档的缓存机制
    // 设计特点：
    // - 使用迭代器支持并发I/O操作
    // - 溢出队列处理超大批次的文档
    // - 进度跟踪和完成条件判断
    class CloneList {
    public:
        /**
         * Simple container that increments the given counter when this is constructed and
         * decrements it when it is destroyed. User of this class is responsible for holding
         * necessary mutexes when counter is being modified.
         */
        // 进度读取令牌：RAII模式管理正在进行的读取操作计数
        // 功能：构造时递增计数器，析构时递减
        // 用途：跟踪并发读取操作，判断克隆完成条件
        class InProgressReadToken {
        public:
            InProgressReadToken(WithLock, CloneList& cloneList);
            InProgressReadToken(const InProgressReadToken&) = delete;
            InProgressReadToken(InProgressReadToken&&) = default;
            ~InProgressReadToken();

        private:
            CloneList& _cloneList;
        };

        /**
         * Container for a document that can be added to the nextCloneBatch call. As long as
         * instances of this object exist, it will prevent getNextDoc from prematurely returning
         * an empty response (which means there are no more docs left to clone).
         *
         * This assumes that _mutex is not being held when it is destroyed.
         */
        // 飞行中文档容器（无锁版本）：在无锁状态下安全持有文档
        // 功能1：防止过早返回空响应
        // 功能2：支持文档的移动和设置
        // 生命周期：与正在进行的读取操作绑定
        class DocumentInFlightWhileNotInLock {
        public:
            DocumentInFlightWhileNotInLock(std::unique_ptr<InProgressReadToken> inProgressReadToken,
                                           boost::optional<Snapshotted<BSONObj>> doc);
            DocumentInFlightWhileNotInLock(const DocumentInFlightWhileNotInLock&) = delete;
            DocumentInFlightWhileNotInLock(DocumentInFlightWhileNotInLock&&) = default;

            void setDoc(boost::optional<Snapshotted<BSONObj>> doc);
            const boost::optional<Snapshotted<BSONObj>>& getDoc();

        private:
            std::unique_ptr<InProgressReadToken> _inProgressReadToken;
            boost::optional<Snapshotted<BSONObj>> _doc;
        };

        /**
         * A variant of the DocumentInFlightWhileNotInLock where the _mutex should be held while it
         * has a document contained within it.
         */
        // 飞行中文档容器（持锁版本）：在持锁状态下操作文档
        // 功能：支持文档的设置和安全释放
        // 用途：在锁保护下进行文档操作，然后转换为无锁版本
        class DocumentInFlightWithLock {
        public:
            DocumentInFlightWithLock(WithLock, CloneList& clonerList);
            DocumentInFlightWithLock(const DocumentInFlightWithLock&) = delete;
            DocumentInFlightWithLock(DocumentInFlightWithLock&&) = default;

            void setDoc(boost::optional<Snapshotted<BSONObj>> doc);

            /**
             * Releases the contained document. Can only be called once for the entire lifetime
             * of this object.
             */
            std::unique_ptr<DocumentInFlightWhileNotInLock> release();

        private:
            std::unique_ptr<InProgressReadToken> _inProgressReadToken;
            boost::optional<Snapshotted<BSONObj>> _doc;
        };

        CloneList();

        /**
         * Overwrites the list of record ids to clone.
         */
        // 填充记录ID列表：设置需要克隆的记录ID集合
        void populateList(RecordIdSet recordIds);

        /**
         * Returns a document to clone. If there are no more documents left to clone,
         * DocumentInFlightWhileNotInLock::getDoc will return boost::none.
         *
         * numRecordsNoLonger exists is an optional parameter that can be used to track
         * the number of recordIds encountered that refers to a document that no longer
         * exists.
         */
        // 获取下一个文档：支持并发访问的文档获取方法
        // 功能1：从记录ID集合或溢出队列获取文档
        // 功能2：跟踪不存在的记录数量
        // 并发安全：支持多线程同时调用
        std::unique_ptr<DocumentInFlightWhileNotInLock> getNextDoc(OperationContext* opCtx,
                                                                   const CollectionPtr& collection,
                                                                   int* numRecordsNoLongerExist);

        /**
         * Put back a document previously obtained from this CloneList instance to the overflow
         * pool.
         */
        // 插入溢出文档：将无法在当前批次传输的文档放回溢出池
        void insertOverflowDoc(Snapshotted<BSONObj> doc);

        /**
         * Returns true if there are more documents to clone.
         */
        // 检查是否还有更多文档需要克隆
        bool hasMore() const;

        /**
         * Returns the size of the populated record ids.
         */
        // 返回记录ID集合的大小
        size_t size() const;

    private:
        /**
         * Increments the counter for inProgressReads.
         */
        // 开始进行中读取：递增正在进行的读取计数
        void _startedOneInProgressRead(WithLock);

        /**
         * Decrements the counter for inProgressReads.
         */
        // 完成进行中读取：递减正在进行的读取计数
        void _finishedOneInProgressRead();

        // 互斥锁：保护所有共享状态
        mutable Mutex _mutex = MONGO_MAKE_LATCH("MigrationChunkClonerSource::CloneList::_mutex");

        // 记录ID集合：存储需要克隆的文档记录ID
        RecordIdSet _recordIds;

        // This iterator is a pointer into the _recordIds set.  It allows concurrent access to
        // the _recordIds set by allowing threads servicing _migrateClone requests to do the
        // following:
        //   1.  Acquire mutex "_mutex" above.
        //   2.  Copy *_recordIdsIter into its local stack frame.
        //   3.  Increment _recordIdsIter
        //   4.  Unlock "_mutex."
        //   5.  Do the I/O to fetch the document corresponding to this record Id.
        //
        // The purpose of this algorithm, is to allow different threads to concurrently start I/O
        // jobs in order to more fully saturate the disk.
        //
        // One issue with this algorithm, is that only 16MB worth of documents can be returned in
        // response to a _migrateClone request.  But, the thread does not know the size of a
        // document until it does the I/O.  At which point, if the document does not fit in the
        // response to _migrateClone request the document must be made available to a different
        // thread servicing a _migrateClone request. To solve this problem, the thread adds the
        // document to the below _overflowDocs deque.
        // 记录ID迭代器：支持并发访问的高效迭代机制
        // 并发算法：
        // 1. 获取互斥锁
        // 2. 复制当前迭代器值到本地
        // 3. 递增迭代器
        // 4. 释放互斥锁
        // 5. 执行I/O操作获取文档
        // 优势：允许多线程并发启动I/O操作，充分利用磁盘性能
        RecordIdSet::iterator _recordIdsIter;

        // This deque stores all documents that must be sent to the destination, but could not fit
        // in the response to a particular _migrateClone request.
        // 溢出文档队列：存储无法在单次请求中传输的文档
        // 用途：解决16MB响应大小限制问题
        // 机制：文档太大时放入溢出队列，下次请求时优先传输
        std::deque<Snapshotted<BSONObj>> _overflowDocs;

        // This integer represents how many documents are being "held" by threads servicing
        // _migrateClone requests. Any document that is "held" by a thread may be added to the
        // _overflowDocs deque if it doesn't fit in the response to a _migrateClone request.
        // This integer is necessary because it gives us a condition on when all documents to be
        // sent to the destination have been exhausted.
        //
        // If (_recordIdsIter == _recordIds.end() && _overflowDocs.empty() &&
        //     _inProgressReads == 0) then all documents have been returned to the destination.
        // 进行中读取计数：跟踪正在被线程"持有"的文档数量
        // 重要性：提供克隆完成的判断条件
        // 完成条件：迭代器到末尾 && 溢出队列为空 && 进行中读取为0
        int64_t _inProgressReads = 0;

        // This condition variable allows us to wait on the following condition:
        //   Either we're done and the above condition is satisfied, or there is some document to
        //   return.
        // 条件变量：等待更多文档可用或克隆完成
        stdx::condition_variable _moreDocsCV;
    };

    // Represents the states in which the cloner can be
    // 克隆器状态枚举：kNew(新建) -> kCloning(克隆中) -> kDone(完成)
    enum State { kNew, kCloning, kDone };

    /**
     * Idempotent method, which cleans up any previously initialized state. It is safe to be called
     * at any time, but no methods should be called after it.
     */
    // 清理方法：幂等的状态清理，可安全多次调用
    void _cleanup(bool wasSuccessful);

    /**
     * Synchronously invokes the recipient shard with the specified command and either returns the
     * command response (if succeeded) or the status, if the command failed.
     */
    // 调用接收端：统一的网络通信方法
    // 功能1：异步发送命令到接收端
    // 功能2：等待响应并处理中断
    // 功能3：错误状态处理和重试机制
    StatusWith<BSONObj> _callRecipient(OperationContext* opCtx, const BSONObj& cmdObj);

    // 获取索引扫描执行器：为巨型块创建索引扫描计划
    StatusWith<std::unique_ptr<PlanExecutor, PlanExecutor::Deleter>> _getIndexScanExecutor(
        OperationContext* opCtx,
        const CollectionPtr& collection,
        InternalPlanner::IndexScanOptions scanOption);

    // 基于索引扫描的批次获取：巨型块的流式处理方法
    void _nextCloneBatchFromIndexScan(OperationContext* opCtx,
                                      const CollectionPtr& collection,
                                      BSONArrayBuilder* arrBuilder);

    // 基于记录ID的批次获取：常规块的高效随机访问方法
    void _nextCloneBatchFromCloneRecordIds(OperationContext* opCtx,
                                           const CollectionPtr& collection,
                                           BSONArrayBuilder* arrBuilder);

    /**
     * Get the recordIds that belong to the chunk migrated and sort them in _cloneRecordIds (to
     * avoid seeking disk later).
     *
     * Returns OK or any error status otherwise.
     */
    // 存储当前记录ID：收集并排序需要克隆的记录ID
    // 优化：排序后可避免后续的磁盘寻道操作
    Status _storeCurrentRecordId(OperationContext* opCtx);

    /**
     * Adds the OpTime to the list of OpTimes for oplog entries that we should consider migrating as
     * part of session migration.
     */
    // 添加到会话迁移队列：将OpTime添加到会话迁移的oplog条目列表
    void _addToSessionMigrationOptimeQueue(
        const repl::OpTime& opTime,
        SessionCatalogMigrationSource::EntryAtOpTimeType entryAtOpTimeType);

    // 事务提交的会话迁移队列处理
    void _addToSessionMigrationOptimeQueueForTransactionCommit(
        const repl::OpTime& opTime,
        SessionCatalogMigrationSource::EntryAtOpTimeType entryAtOpTimeType);

    /*
     * Appends the relevant document changes to the appropriate internal data structures (known
     * colloquially as the 'transfer mods queue'). These structures track document changes that are
     * part of a part of a chunk being migrated. In doing so, this the method also removes the
     * corresponding operation track request from the operation track requests queue.
     */
    // 添加到传输修改队列：OpObserver调用的核心方法
    // 功能1：将文档变更添加到相应的内部数据结构
    // 功能2：区分插入/更新（_reload）和删除（_deleted）操作
    // 功能3：更新内存使用统计和计数器
    // 功能4：移除对应的操作跟踪请求
    void _addToTransferModsQueue(const BSONObj& idObj, char op, const repl::OpTime& opTime);

    /**
     * Adds an operation to the outstanding operation track requests. Returns false if the cloner
     * is no longer accepting new operation track requests.
     */
    // 添加操作跟踪请求：检查是否还接受新的操作跟踪
    bool _addedOperationToOutstandingOperationTrackRequests();

    /**
     * Called to indicate a request to track an operation must be filled. The operations in
     * question indicate a change to a document in the chunk being cloned. Increments a counter
     * residing inside the MigrationChunkClonerSource class.
     *
     * There should always be a one to one match from the number of calls to this function to the
     * number of calls to the corresponding decrement* function.
     *
     * NOTE: This funtion invariants that we are currently accepting new operation track requests.
     * It is up to callers of this function to make sure that will always be the case.
     */
    // 递增操作跟踪请求：必须与递减操作一一对应
    void _incrementOutstandingOperationTrackRequests(WithLock);

    /**
     * Called once a request to track an operation has been filled. The operations in question
     * indicate a change to a document in the chunk being cloned. Decrements a counter residing
     * inside the MigrationChunkClonerSource class.
     *
     * There should always be a one to one match from the number of calls to this function to the
     * number of calls to the corresponding increment* function.
     */
    // 递减操作跟踪请求：完成操作跟踪后调用
    void _decrementOutstandingOperationTrackRequests();

    /**
     * Waits for all outstanding operation track requests to be fulfilled before returning from this
     * function. Should only be used in the cleanup for this class. Should use a lock wrapped
     * around this class's mutex.
     */
    // 等待所有操作跟踪请求完成：清理时使用，确保所有请求都被处理
    void _drainAllOutstandingOperationTrackRequests(stdx::unique_lock<Latch>& lk);

    /**
     * Sends _recvChunkStatus to the recipient shard until it receives 'steady' from the recipient,
     * an error has occurred, or a timeout is hit.
     */
    // 检查接收端克隆状态：核心的状态监控和准入控制方法
    // 功能1：轮询接收端状态直到达到稳定状态
    // 功能2：评估未传输数据量和会话数据大小
    // 功能3：根据阈值判断是否可以进入关键区域
    // 功能4：内存使用监控和保护机制
    Status _checkRecipientCloningStatus(OperationContext* opCtx, Milliseconds maxTimeToWait);

    /**
     * Inspects the pre and post image document keys and determines which xferMods bucket to
     * add a new entry. Returns false if neither pre or post image document keys fall into
     * the chunk boundaries being migrated.
     */
    // 处理更新操作的传输修改：检查更新前后的文档键并决定如何处理
    bool _processUpdateForXferMod(const BSONObj& preImageDocKey, const BSONObj& postImageDocKey);

    /**
     * Defer processing of update ops into xferMods entries to when nextModsBatch is called.
     */
    // 延迟处理传输修改：将更新操作的处理推迟到nextModsBatch调用时
    void _deferProcessingForXferMod(const BSONObj& preImageDocKey);

    /**
     * Converts all deferred update ops captured by the op observer into xferMods entries.
     */
    // 处理延迟的传输修改：将所有延迟的更新操作转换为传输修改条目
    void _processDeferredXferMods(OperationContext* opCtx);

    // The original move range request
    // 原始迁移范围请求：包含所有迁移参数
    const ShardsvrMoveRange _args;

    // The write concern associated with the move range
    // 迁移相关的写关注：确保数据持久性要求
    const WriteConcernOptions _writeConcern;

    // The shard key associated with the namespace
    // 命名空间关联的分片键：用于范围检查和文档路由
    const ShardKeyPattern _shardKeyPattern;

    // The migration session id
    // 迁移会话ID：唯一标识此次迁移会话
    const MigrationSessionId _sessionId;

    // The resolved connection string of the donor shard
    // 源分片的连接字符串：用于标识源端
    const ConnectionString _donorConnStr;

    // The resolved primary of the recipient shard
    // 接收分片的主节点：网络通信目标
    const HostAndPort _recipientHost;

    // 会话目录迁移源：处理事务和可重试写操作的会话数据
    std::unique_ptr<SessionCatalogMigrationSource> _sessionCatalogSource;

    // Protects the entries below
    // 保护下面条目的互斥锁：确保状态和队列操作的线程安全
    mutable Mutex _mutex = MONGO_MAKE_LATCH("MigrationChunkClonerSource::_mutex");

    // The current state of the cloner
    // 克隆器当前状态：状态机管理
    State _state{kNew};

    // 克隆列表：管理需要克隆的文档记录ID
    CloneList _cloneList;

    // 已克隆记录数：统计已处理的记录数量
    RecordIdSet::size_type _numRecordsCloned{0};
    
    // 跳过记录数：统计已跳过的记录数量
    RecordIdSet::size_type _numRecordsPassedOver{0};

    // The estimated average object size during the clone phase. Used for buffer size
    // pre-allocation (initial clone).
    // 克隆阶段的平均对象大小：用于缓冲区预分配优化
    uint64_t _averageObjectSizeForCloneRecordIds{0};

    // The estimated average object _id size during the clone phase.
    // 克隆阶段的平均对象ID大小：用于内存使用估算
    uint64_t _averageObjectIdSize{0};

    // Represents all of the requested but not yet fulfilled operations to be tracked, with regards
    // to the chunk being cloned.
    // 未完成的操作跟踪请求数：确保所有操作都被正确处理
    uint64_t _outstandingOperationTrackRequests{0};

    // Signals to any waiters once all unresolved operation tracking requests have completed.
    // 操作跟踪完成信号：通知等待者所有跟踪请求已完成
    stdx::condition_variable _allOutstandingOperationTrackRequestsDrained;

    // Indicates whether new requests to track an operation are accepted.
    // 是否接受新的操作跟踪请求：控制OpObserver是否继续收集操作
    bool _acceptingNewOperationTrackRequests{true};

    // List of _id of documents that were modified that must be re-cloned (xfer mods)
    // 重新加载队列：存储需要重新克隆的修改文档ID（插入/更新操作）
    std::list<BSONObj> _reload;

    // Amount of upsert xfer mods that have not yet reached the recipient.
    // 未传输的更新插入修改计数：跟踪未发送的操作数量
    size_t _untransferredUpsertsCounter{0};

    // List of _id of documents that were deleted during clone that should be deleted later (xfer
    // mods)
    // 删除队列：存储克隆期间被删除的文档ID
    std::list<BSONObj> _deleted;

    // Amount of delete xfer mods that have not yet reached the recipient.
    // 未传输的删除修改计数：跟踪未发送的删除操作数量，例如迁移某个 chunk 过程中正在删除这个 chunk 的数据，这时候的删除操作需要被记录
    size_t _untransferredDeletesCounter{0};

    // Amount of ops that are yet to be converted to update/delete xferMods.
    // 延迟未传输操作计数：跟踪尚未转换为传输修改的操作数量
    size_t _deferredUntransferredOpsCounter{0};

    // Stores document keys of document that needs to be examined if we need to put in to xferMods
    // list later.
    // 延迟处理文档键列表：存储需要稍后检查是否加入传输修改列表的文档键
    std::vector<BSONObj> _deferredReloadOrDeletePreImageDocKeys;

    // Total bytes in _reload + _deleted (xfer mods)
    // 内存使用总量：跟踪传输修改队列的内存占用
    uint64_t _memoryUsed{0};

    // False if the move chunk request specified ForceJumbo::kDoNotForce, true otherwise.
    // 是否强制巨型块：控制是否允许迁移超大块
    /*
    // 普通 moveChunk：
sh.moveChunk(
  "test.coll",                // ns
  { _id: MinKey },            // chunkRange.find
  "shardB",                   // to
  { forceJumbo: true }        // 额外选项
);

db.adminCommand({
  _shardsvrMoveRange: "test.coll",
  min: { _id: 0 },
  max: { _id: 1000000 },
  fromShard: "shardA",
  toShard: "shardB",
  writeConcern: { w: "majority" },
  forceJumbo: true           // 允许迁移 jumbo chunk
});

全局修改
    // Now set "forceJumbo: true" in config.settings.
    assert.commandWorked(st.s.getDB("config").settings.update(
        {_id: "balancer"}, {$set: {attemptToBalanceJumboChunks: true}}, true));
    */
    // MigrationChunkClonerSource::_storeCurrentRecordId
    const bool _forceJumbo;
    
    // 巨型块克隆状态：处理超大块的特殊状态
    struct JumboChunkCloneState {
        // Plan executor for collection scan used to clone docs.
        // 集合扫描执行器：用于克隆文档的计划执行器
        std::unique_ptr<PlanExecutor, PlanExecutor::Deleter> clonerExec;

        // The current state of 'clonerExec'.
        // 执行器当前状态：跟踪扫描进度
        PlanExecutor::ExecState clonerState;

        // Number docs in jumbo chunk cloned so far
        // 已克隆文档数：统计巨型块中已处理的文档数量
        int docsCloned = 0;
    };

    // Set only once its discovered a chunk is jumbo
    // 巨型块克隆状态：只有在发现块是巨型块时才设置
    boost::optional<JumboChunkCloneState> _jumboChunkCloneState;

protected:
    // 受保护的默认构造函数：用于测试
    MigrationChunkClonerSource();
};

/**
 * Appends to the builder the list of documents either deleted or modified during migration.
 * Entries appended to the builder are removed from the list.
 * Returns the total size of the documents that were appended + initialSize.
 */
// 传输修改函数：将删除或修改的文档列表添加到构建器
// 功能1：去重处理避免重复传输
// 功能2：批次大小控制防止超过16MB限制
// 功能3：支持自定义文档提取函数
// 返回值：追加文档的总大小 + 初始大小
long long xferMods(BSONArrayBuilder* arr,
                   std::list<BSONObj>* modsList,
                   long long initialSize,
                   std::function<bool(BSONObj, BSONObj*)> extractDocToAppendFn);

}  // namespace mongo
