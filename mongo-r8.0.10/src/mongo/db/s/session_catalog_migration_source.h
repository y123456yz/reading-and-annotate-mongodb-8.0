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
#include <boost/optional.hpp>
#include <boost/optional/optional.hpp>
#include <cstdint>
#include <list>
#include <memory>
#include <utility>
#include <vector>

#include "mongo/bson/bsonobj.h"
#include "mongo/client/dbclient_cursor.h"
#include "mongo/db/keypattern.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/repl/oplog_entry.h"
#include "mongo/db/repl/optime.h"
#include "mongo/db/session/logical_session_id_gen.h"
#include "mongo/db/session/session_txn_record_gen.h"
#include "mongo/db/transaction/transaction_history_iterator.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/platform/mutex.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/shard_key_pattern.h"
#include "mongo/util/concurrency/notification.h"
#include "mongo/util/concurrency/with_lock.h"

namespace mongo {

class OperationContext;
class ScopedSession;
class ServiceContext;

/**
 * Provides facilities for extracting oplog entries of writes in a particular namespace that needs
 * to be migrated.
 *
 * This also ensures that oplog returned are majority committed. This is achieved by calling
 * waitForWriteConcern. However, waitForWriteConcern does not support waiting for opTimes of
 * previous terms. To get around this, the waitForWriteConcern is performed in two phases:
 *
 * During init() call phase:
 * 1. Scan the entire config.transactions and extract all the lastWriteOpTime.
 * 2. Insert a no-op oplog entry and wait for it to be majority committed.
 * 3. At this point any writes before should be majority committed (including all the oplog
 *    entries that the collected lastWriteOpTime points to). If the particular oplog with the
 *    opTime cannot be found: it either means that the oplog was truncated or rolled back.
 *
 * New writes/xfer mods phase oplog entries:
 * In this case, caller is responsible for calling waitForWriteConcern. If getLastFetchedOplog
 * returns shouldWaitForMajority == true, it should wait for the highest opTime it has got from
 * getLastFetchedOplog. It should also error if it detects a change of term within a batch since
 * it would be wrong to wait for the highest opTime in this case.
 */
/**
 * SessionCatalogMigrationSource 类的作用：
 * 在MongoDB分片迁移过程中，负责提取和传输会话相关的oplog条目，确保会话状态和事务数据的完整迁移。
 * 
 * 核心功能：
 * 1. 会话数据提取：从config.transactions集合扫描历史会话记录，提取需要迁移的会话oplog
 * 2. 实时增量捕获：通过OpObserver机制实时捕获迁移过程中新产生的会话写操作
 * 3. 双数据源管理：统一管理历史会话数据和新写操作数据两个独立的数据源
 * 4. 写关注协调：确保oplog条目在大多数节点上提交，保证数据一致性
 * 5. 批量传输：提供高效的批量oplog获取接口，优化网络传输性能
 * 
 * 设计架构：
 * - 双轨制数据源：分离历史数据处理和实时数据捕获的逻辑路径
 * - 分离锁设计：使用独立的互斥锁保护不同数据源，减少锁竞争
 * - 异步通知机制：支持非阻塞的数据等待，提高系统响应性
 * - 状态机管理：通过State枚举管理迁移的不同阶段
 * 
 * 处理的会话数据类型：
 * 1. 可重试写操作(RetryableWrite)：保证写操作的幂等性
 * 2. 内部事务(NonRetryableTransaction)：维护事务的原子性
 * 3. 可重试事务(RetryableTransaction)：结合重试和事务特性
 * 
 * 两阶段写关注策略：
 * 初始化阶段：
 * 1. 扫描config.transactions提取所有lastWriteOpTime
 * 2. 插入no-op oplog条目并等待其大多数提交
 * 3. 确保所有历史oplog条目都已大多数提交
 * 
 * 新写操作阶段：
 * 1. 调用方负责调用waitForWriteConcern
 * 2. 如果shouldWaitForMajority为true，需要等待最高opTime
 * 3. 检测任期变化，防止错误的写关注等待
 * 
 * 性能优化：
 * - 迭代器模式：使用SessionOplogIterator高效遍历oplog链
 * - 缓存机制：避免重复的磁盘I/O操作
 * - 预处理缓冲：提前处理oplog条目，减少实时计算开销
 * - 大小估算：提供数据传输量估算，支持智能调度
 * 
 * 并发安全：
 * - 线程安全：通过细粒度锁保护共享状态
 * - 死锁避免：精心设计的锁顺序和分离锁机制
 * - 状态一致性：确保不同阶段状态变更的原子性
 * 
 * 错误处理：
 * - 回滚检测：记录初始rollbackId，检测数据回滚事件
 * - oplog丢失处理：处理oplog截断或回滚导致的条目丢失
 * - 哨兵条目：使用特殊标记处理无法找到的oplog条目
 * 
 * 与分片迁移流程的集成：
 * - 配合MigrationChunkClonerSource实现完整的数据迁移
 * - 支持fetchNextSessionMigrationBatch的批量数据获取
 * - 与write关注机制紧密集成，确保数据持久性
 * 
 * 使用场景：
 * - chunk迁移过程中的会话数据传输
 * - 确保多文档事务在分片间的一致性迁移
 * - 维护可重试写操作的语义正确性
 * - 支持MongoDB会话功能在分片环境中的正常工作
 * 
 * 该类是MongoDB分片系统中处理会话数据迁移的核心组件，确保了现代应用程序
 * 所依赖的事务和会话特性在分片迁移过程中的完整性和一致性。
 */
/**
 * Provides facilities for extracting oplog entries of writes in a particular namespace that needs
 * to be migrated.
 *
 * This also ensures that oplog returned are majority committed. This is achieved by calling
 * waitForWriteConcern. However, waitForWriteConcern does not support waiting for opTimes of
 * previous terms. To get around this, the waitForWriteConcern is performed in two phases:
 *
 * During init() call phase:
 * 1. Scan the entire config.transactions and extract all the lastWriteOpTime.
 * 2. Insert a no-op oplog entry and wait for it to be majority committed.
 * 3. At this point any writes before should be majority committed (including all the oplog
 *    entries that the collected lastWriteOpTime points to). If the particular oplog with the
 *    opTime cannot be found: it either means that the oplog was truncated or rolled back.
 *
 * New writes/xfer mods phase oplog entries:
 * In this case, caller is responsible for calling waitForWriteConcern. If getLastFetchedOplog
 * returns shouldWaitForMajority == true, it should wait for the highest opTime it has got from
 * getLastFetchedOplog. It should also error if it detects a change of term within a batch since
 * it would be wrong to wait for the highest opTime in this case.
 */
class SessionCatalogMigrationSource {
    SessionCatalogMigrationSource(const SessionCatalogMigrationSource&) = delete;
    SessionCatalogMigrationSource& operator=(const SessionCatalogMigrationSource&) = delete;

public:
    // 会话oplog条目类型枚举：
    // 用于区分不同类型的会话操作，支持不同的处理策略
    enum class EntryAtOpTimeType { 
        kTransaction,      // 事务相关的oplog条目
        kRetryableWrite    // 可重试写操作的oplog条目
    };

    /**
     * OplogResult 结构体：封装oplog获取结果
     * 功能：统一返回oplog条目和写关注要求的数据结构
     * 设计：支持空oplog和写关注标识的组合返回
     */
    struct OplogResult {
        OplogResult(boost::optional<repl::OplogEntry> _oplog, bool _shouldWaitForMajority)
            : oplog(std::move(_oplog)), shouldWaitForMajority(_shouldWaitForMajority) {}

        // The oplog fetched.
        // 获取的oplog条目：可能为空，表示当前没有可用的oplog
        boost::optional<repl::OplogEntry> oplog;

        // If this is set to true, oplog returned is not confirmed to be majority committed,
        // so the caller has to explicitly wait for it to be committed to majority.
        // 写关注标识：如果为true，调用方必须显式等待大多数节点确认
        bool shouldWaitForMajority = false;
    };

    /**
     * 构造函数：初始化会话目录迁移源
     * 参数说明：
     * @param opCtx 操作上下文，提供事务和权限信息
     * @param ns 命名空间，指定迁移的集合
     * @param chunk chunk范围，定义迁移的数据边界
     * @param shardKey 分片键模式，用于数据过滤
     */
    SessionCatalogMigrationSource(OperationContext* opCtx,
                                  NamespaceString ns,
                                  ChunkRange chunk,
                                  KeyPattern shardKey);

    /**
     * Gets the session oplog entries to be sent to the destination. The initialization is separated
     * from the constructor to allow the member functions of the SessionCatalogMigrationSource to be
     * called before the initialization step is finished.
     */
    /**
     * 初始化方法：获取要发送到目标的会话oplog条目
     * 设计理念：将初始化与构造函数分离，允许在初始化完成前调用成员函数
     * 功能：
     * 1. 扫描config.transactions集合获取历史会话记录
     * 2. 构建SessionOplogIterator列表用于oplog链遍历
     * 3. 记录初始rollbackId用于后续回滚检测
     * 4. 执行写关注等待确保历史数据的持久性
     */
    void init(OperationContext* opCtx, const LogicalSessionId& migrationLsid);

    /**
     * Returns true if there are more oplog entries to fetch at this moment. Note that new writes
     * can still continue to come in after this has returned false, so it can become true again.
     * Once this has returned false, this means that it has depleted the existing buffer so it
     * is a good time to enter the critical section.
     */
    /**
     * hasMoreOplog方法：检查是否还有更多oplog条目需要获取
     * 重要说明：
     * - 返回false后，新写操作仍可能到来，使其再次变为true
     * - 返回false意味着现有缓冲区已耗尽，是进入关键区域的好时机
     * 实现：检查历史会话数据源和新写操作数据源的状态
     */
    bool hasMoreOplog();

    /**
     * Returns true if the majority committed oplog entries are drained and false otherwise.
     */
    /**
     * inCatchupPhase方法：检查是否大多数提交的oplog条目已耗尽
     * 用途：判断迁移是否已进入追赶阶段
     * 策略：区分历史数据传输和实时数据同步两个阶段
     */
    bool inCatchupPhase();

    /**
     * Returns the estimated bytes of data left to transfer in _newWriteOpTimeList.
     */
    /**
     * untransferredCatchUpDataSize方法：估算_newWriteOpTimeList中剩余传输的数据字节数
     * 用途：为迁移调度提供数据量估算
     * 计算：基于平均会话文档大小和待处理opTime数量
     */
    int64_t untransferredCatchUpDataSize();

    /**
     * Attempts to fetch the next oplog entry. Returns true if it was able to fetch anything.
     */
    /**
     * fetchNextOplog方法：尝试获取下一个oplog条目
     * 返回值：如果成功获取任何内容则返回true
     * 实现逻辑：
     * 1. 优先处理历史会话数据源(_sessionCloneMutex保护)
     * 2. 然后处理新写操作数据源(_newOplogMutex保护)
     * 3. 支持镜像条目的特殊处理(findAndModify等操作)
     */
    bool fetchNextOplog(OperationContext* opCtx);

    /**
     * Returns a notification that can be used to wait for new oplog entries to fetch. Note
     * that this should only be called if hasMoreOplog/fetchNextOplog returned false at
     * least once.
     *
     * If the notification is set to true, then that means that there is no longer a need to
     * fetch more oplog because the data migration has entered the critical section and
     * the buffer for oplog to fetch is empty or the data migration has aborted.
     */
    /**
     * getNotificationForNewOplog方法：返回用于等待新oplog条目的通知对象
     * 使用条件：只有在hasMoreOplog/fetchNextOplog至少返回一次false后才应调用
     * 通知语义：
     * - true：不再需要获取更多oplog(进入关键区域或迁移中止)
     * - false：有新的oplog条目可供获取
     * 设计：支持非阻塞的异步等待机制
     */
    std::shared_ptr<Notification<bool>> getNotificationForNewOplog();

    /**
     * Returns the oplog document that was last fetched by the fetchNextOplog call.
     * Returns an empty object if there are no oplog to fetch.
     */
    /**
     * getLastFetchedOplog方法：返回最后一次fetchNextOplog调用获取的oplog文档
     * 返回值：如果没有oplog可获取则返回空对象
     * 优先级策略：
     * 1. 历史会话镜像条目(_lastFetchedOplogImage)
     * 2. 历史会话原始条目(_lastFetchedOplog) 
     * 3. 新写操作镜像条目(_lastFetchedNewWriteOplogImage)
     * 4. 新写操作原始条目(_lastFetchedNewWriteOplog)
     */
    OplogResult getLastFetchedOplog();

    /**
     * Remembers the oplog timestamp of a new write that just occurred.
     */
    /**
     * notifyNewWriteOpTime方法：记住刚刚发生的新写操作的oplog时间戳
     * 功能：OpObserver回调接口，用于实时捕获新的写操作
     * 参数：
     * @param opTimestamp oplog操作时间戳
     * @param entryAtOpTimeType 条目类型(事务或可重试写)
     * 实现：添加到_newWriteOpTimeList队列，触发通知机制
     */
    void notifyNewWriteOpTime(repl::OpTime opTimestamp, EntryAtOpTimeType entryAtOpTimeType);

    /**
     * Returns the rollback ID recorded at the beginning of session migration.
     */
    /**
     * getRollbackIdAtInit方法：返回会话迁移开始时记录的回滚ID
     * 用途：用于检测迁移过程中是否发生了数据回滚
     * 重要性：确保迁移数据的一致性和可靠性
     */
    int getRollbackIdAtInit() const {
        return _rollbackIdAtInit;
    }

    /**
     * Inform this session migration machinery that the data migration just entered the critical
     * section.
     */
    /**
     * onCommitCloneStarted方法：通知会话迁移机制数据迁移刚进入关键区域
     * 状态变更：从kActive转换到kCommitStarted
     * 影响：停止接受新的写操作通知，准备最终的数据一致性检查
     */
    void onCommitCloneStarted();

    /**
     * Inform this session migration machinery that the data migration just terminated and
     * entering the cleanup phase (can be aborted or committed).
     */
    /**
     * onCloneCleanup方法：通知会话迁移机制数据迁移已终止并进入清理阶段
     * 状态变更：转换到kCleanup状态
     * 适用：迁移中止或成功提交的情况
     * 影响：释放资源，触发清理通知
     */
    void onCloneCleanup();

    /**
     * This function will utilize the shardKeyPattern and chunkRange to evaluate whether or not
     * the oplogEntry is relevant to the migration. If not, the chunk should be skipped and the
     * function will return true. Otherwise the function will return false.
     *
     * If the oplogEntry is of type no-op and it has been rewritten by another migration and it's
     * outside of the chunk range, then it should be skipped. Or if the oplog is a crud operation
     * and it's outside of the chunk range then it should be skipped.
     */
    /**
     * shouldSkipOplogEntry静态方法：评估oplog条目是否与迁移相关
     * 评估逻辑：
     * 1. 使用分片键模式和chunk范围判断oplog条目的相关性
     * 2. no-op类型且被其他迁移重写且在chunk范围外的条目应跳过
     * 3. CRUD操作且在chunk范围外的条目应跳过
     * 返回值：true表示应跳过，false表示应处理
     * 优化：减少不必要的oplog传输，提高迁移效率
     */
    static bool shouldSkipOplogEntry(const mongo::repl::OplogEntry& oplogEntry,
                                     const ShardKeyPattern& shardKeyPattern,
                                     const ChunkRange& chunkRange);

    /**
     * 统计方法：获取迄今为止需要迁移的会话oplog条目数量
     * 用途：监控和调试迁移进度
     */
    long long getSessionOplogEntriesToBeMigratedSoFar();
    
    /**
     * 统计方法：获取迄今为止跳过的会话oplog条目数量下界
     * 说明：由于优化机制，这是一个下界估算值
     * 用途：评估迁移效率和优化效果
     */
    long long getSessionOplogEntriesSkippedSoFarLowerBound();

    /**
     * Given an Oplog entry, extracts the shard key corresponding to the key pattern for insert,
     * update, and delete op types. If the op type is not a CRUD operation, an empty BSONObj()
     * will be returned.
     *
     * For update and delete operations, the Oplog entry will contain an object with the document
     * key.
     *
     * For insert operations, the Oplog entry will contain the original document from which the
     * document key must be extracted
     *
     * Examples:
     *  For KeyPattern {'a.b': 1}
     *   If the oplog entries contains field op='i'
     *     oplog contains: { a : { b : "1" } }
     *   If the oplog entries contains field op='u' or op='d'
     *     oplog contains: { 'a.b': "1" }
     */
    /**
     * extractShardKeyFromOplogEntry静态方法：从oplog条目中提取分片键
     * 处理逻辑：
     * - 插入操作：从原始文档中提取分片键
     * - 更新/删除操作：从文档键中提取分片键
     * - 非CRUD操作：返回空BSON对象
     * 用途：支持oplog条目的chunk范围过滤
     */
    static BSONObj extractShardKeyFromOplogEntry(const ShardKeyPattern& shardKeyPattern,
                                                 const repl::OplogEntry& entry);

private:
    /**
     * An iterator for extracting session write oplogs that need to be cloned during migration.
     */
    /**
     * SessionOplogIterator内部类：用于提取迁移期间需要克隆的会话写oplog的迭代器
     * 功能：
     * 1. 封装TransactionHistoryIterator，提供会话特定的oplog遍历
     * 2. 区分不同类型的会话条目（可重试写、事务等）
     * 3. 处理oplog丢失和哨兵条目的特殊情况
     * 4. 支持prepare和commit事务条目的特殊处理
     */
    class SessionOplogIterator {
    public:
        // 会话条目类型：区分不同类型的会话操作处理逻辑
        enum class EntryType { 
            kRetryableWrite,           // 可重试写操作
            kNonRetryableTransaction,  // 不可重试事务
            kRetryableTransaction      // 可重试事务
        };

        /**
         * 构造函数：基于会话事务记录创建迭代器
         * @param txnRecord 会话事务记录，包含会话状态信息
         * @param expectedRollbackId 期望的回滚ID，用于一致性检查
         */
        SessionOplogIterator(SessionTxnRecord txnRecord, int expectedRollbackId);

        /**
         * Returns the next oplog write that happened in this session, or boost::none if there
         * are no remaining entries for this session.
         *
         * If either:
         *     a) the oplog is lost because the oplog rolled over, or
         *     b) if the oplog entry is a prepare or commitTransaction entry,
         * this will return a sentinel oplog entry instead with type 'n' and o2 field set to
         * Session::kDeadEndSentinel.  This will also mean that next subsequent calls to getNext
         * will return boost::none.
         */
        /**
         * getNext方法：返回此会话中发生的下一个oplog写操作
         * 特殊处理：
         * 1. oplog因截断丢失：返回哨兵条目
         * 2. prepare或commitTransaction条目：返回哨兵条目
         * 哨兵条目格式：type='n', o2=Session::kDeadEndSentinel
         * 后续调用：哨兵条目后的调用将返回boost::none
         */
        boost::optional<repl::OplogEntry> getNext(OperationContext* opCtx);

        /**
         * toBSON方法：序列化会话记录为BSON格式
         * 用途：调试和日志记录
         */
        BSONObj toBSON() const {
            return _record.toBSON();
        }

    private:
        const SessionTxnRecord _record;                          // 会话事务记录
        const int _initialRollbackId;                           // 初始回滚ID
        const EntryType _entryType;                             // 条目类型

        std::unique_ptr<TransactionHistoryIterator> _writeHistoryIterator; // 写历史迭代器
    };

    // 迁移状态枚举：管理迁移的不同阶段
    enum class State { 
        kActive,        // 活跃状态：正常进行迁移
        kCommitStarted, // 提交开始：进入关键区域
        kCleanup        // 清理状态：迁移终止
    };

    ///////////////////////////////////////////////////////////////////////////
    // Methods for extracting the oplog entries from session information.
    // 从会话信息中提取oplog条目的方法

    /**
     * If this returns false, it just means that there are no more oplog entry in the buffer that
     * needs to be moved over. However, there can still be new incoming operations that can add
     * new entries. Also see hasNewWrites.
     */
    /**
     * _hasMoreOplogFromSessionCatalog私有方法：检查会话目录是否还有更多oplog
     * 说明：返回false只意味着缓冲区中没有更多需要移动的oplog条目
     * 注意：仍可能有新的操作添加新条目
     */
    bool _hasMoreOplogFromSessionCatalog();

    /**
     * Attempts to extract the next oplog document by following the oplog chain from the sessions
     * catalog. Returns true if a document was actually fetched.
     */
    /**
     * _fetchNextOplogFromSessionCatalog私有方法：通过跟踪会话目录的oplog链提取下一个oplog文档
     * 实现：遍历SessionOplogIterator列表，处理当前迭代器
     * 返回值：如果实际获取了文档则返回true
     */
    bool _fetchNextOplogFromSessionCatalog(OperationContext* opCtx);

    /**
     * Extracts oplog information from the current writeHistoryIterator to _lastFetchedOplog. This
     * handles insert/update/delete/findAndModify oplog entries.
     *
     * Returns true if current writeHistoryIterator has any oplog entry.
     */
    /**
     * _handleWriteHistory私有方法：从当前writeHistoryIterator提取oplog信息到_lastFetchedOplog
     * 处理：insert/update/delete/findAndModify oplog条目
     * 特殊处理：findAndModify的前置/后置镜像条目
     * 返回值：如果当前writeHistoryIterator有任何oplog条目则返回true
     */
    bool _handleWriteHistory(WithLock, OperationContext* opCtx);

    ///////////////////////////////////////////////////////////////////////////
    // Methods for capturing and extracting oplog entries for new writes.
    // 捕获和提取新写操作oplog条目的方法

    /**
     * Returns true if there are oplog generated by new writes that needs to be fetched.
     */
    /**
     * _hasNewWrites私有方法：检查是否有由新写操作生成的需要获取的oplog
     * 实现：检查_newWriteOpTimeList和未处理缓冲区的状态
     */
    bool _hasNewWrites(WithLock);

    /**
     * Can only be invoked when '_unprocessedNewWriteOplogBuffer' is empty but '_newWriteOpTimeList'
     * is not. Fetches the oplog entry for the next opTime in '_newWriteOpTimeList'. If the oplog
     * entry corresponds to a write against the chunk range being migrated, adds the oplog entry or
     * its inner oplog entries (for applyOps) to '_unprocessedNewWriteOplogBuffer'.
     */
    /**
     * _tryFetchNextNewWriteOplog私有方法：尝试获取_newWriteOpTimeList中下一个opTime的oplog条目
     * 调用条件：'_unprocessedNewWriteOplogBuffer'为空但'_newWriteOpTimeList'不为空
     * 处理逻辑：
     * 1. 获取下一个opTime的oplog条目
     * 2. 检查是否对应正在迁移的chunk范围的写操作
     * 3. 将oplog条目或其内部条目(对于applyOps)添加到未处理缓冲区
     */
    void _tryFetchNextNewWriteOplog(stdx::unique_lock<Latch>& lk, OperationContext* opCtx);

    /**
     * If there is no stashed '_lastFetchedOplog', looks for the next opTime in
     * '_newWriteOpTimeList' that corresponds to a write against this chunk range, fetches its oplog
     * entry and stashes it onto '_lastFetchedOplog' (and '_lastFetchedOplogImage' if applicable).
     * Returns true if such an oplog entry was found and fetched.
     */
    /**
     * _fetchNextNewWriteOplog私有方法：获取下一个新写操作的oplog
     * 逻辑：
     * 1. 如果没有缓存的'_lastFetchedOplog'
     * 2. 在'_newWriteOpTimeList'中查找对应此chunk范围写操作的下一个opTime
     * 3. 获取其oplog条目并缓存到'_lastFetchedOplog'
     * 4. 如果适用，也缓存到'_lastFetchedOplogImage'
     * 返回值：如果找到并获取了这样的oplog条目则返回true
     */
    bool _fetchNextNewWriteOplog(OperationContext* opCtx);

    /**
     * Same as notifyNewWriteOpTime but must be called while holding the _newOplogMutex.
     */
    /**
     * _notifyNewWriteOpTime私有方法：与notifyNewWriteOpTime相同，但必须在持有_newOplogMutex时调用
     * 内部实现：添加到_newWriteOpTimeList，触发通知机制
     */
    void _notifyNewWriteOpTime(WithLock,
                               repl::OpTime opTimestamp,
                               EntryAtOpTimeType entryAtOpTimeType);

    /*
     * Derives retryable write oplog entries from the given retryable internal transaction applyOps
     * oplog entry, or retryable write applyOps entry (with multiOpType() ==
     * kApplyOpsAppliedSeparately), and adds the ones that are related to the migration the given
     * oplog buffer. Must be called while holding the mutex that protects the buffer.
     */
    /**
     * _extractOplogEntriesForRetryableApplyOps私有方法：从可重试内部事务applyOps oplog条目中派生可重试写oplog条目
     * 处理：
     * 1. 可重试内部事务applyOps oplog条目
     * 2. 可重试写applyOps条目(multiOpType() == kApplyOpsAppliedSeparately)
     * 3. 将与迁移相关的条目添加到给定的oplog缓冲区
     * 要求：必须在持有保护缓冲区的互斥锁时调用
     */
    void _extractOplogEntriesForRetryableApplyOps(WithLock,
                                                  const repl::OplogEntry& applyOplogEntry,
                                                  std::vector<repl::OplogEntry>* oplogBuffer);

    // ========== 成员变量：基础配置 ==========
    
    // Namespace for which the migration is happening
    // 迁移发生的命名空间：指定正在迁移的集合
    const NamespaceString _ns;

    // The rollback id just before migration started. This value is needed so that step-down
    // followed by step-up situations can be discovered.
    // 迁移开始前的回滚ID：用于发现降级后升级的情况
    const int _rollbackIdAtInit;

    // chunk范围和分片键模式：定义迁移的数据边界和过滤条件
    const ChunkRange _chunkRange;
    const ShardKeyPattern _keyPattern;

    // ========== 历史会话数据源相关成员变量 ==========
    
    // Protects _sessionOplogIterators, _currentOplogIterator, _lastFetchedOplog,
    // _lastFetchedOplogImage and _unprocessedOplogBuffer.
    // 保护历史会话数据相关的状态变量
    Mutex _sessionCloneMutex =
        MONGO_MAKE_LATCH("SessionCatalogMigrationSource::_sessionCloneMutex");

    // List of remaining session records that needs to be cloned.
    // 需要克隆的剩余会话记录列表
    std::vector<std::unique_ptr<SessionOplogIterator>> _sessionOplogIterators;

    // Points to the current session record being cloned.
    // 指向当前正在克隆的会话记录
    std::unique_ptr<SessionOplogIterator> _currentOplogIterator;

    // Used to store the last fetched and processed oplog entry from _currentOplogIterator. This
    // enables calling get() multiple times.
    // 存储从_currentOplogIterator最后获取和处理的oplog条目
    // 支持多次调用get()方法
    boost::optional<repl::OplogEntry> _lastFetchedOplog;

    // Used to store the pre/post image for _lastFetchedNewWriteOplog if there is one.
    // 存储_lastFetchedOplog的前置/后置镜像(如果有的话)
    boost::optional<repl::OplogEntry> _lastFetchedOplogImage;

    // Used to store the last fetched oplog entries from _currentOplogIterator that have not been
    // processed.
    // 存储从_currentOplogIterator获取但尚未处理的oplog条目
    std::vector<repl::OplogEntry> _unprocessedOplogBuffer;

    // ========== 新写操作数据源相关成员变量 ==========
    
    // Protects _newWriteOpTimeList, _lastFetchedNewWriteOplog, _lastFetchedNewWriteOplogImage,
    // _unprocessedNewWriteOplogBuffer, _state, _newOplogNotification.
    // 保护新写操作相关的状态变量
    Mutex _newOplogMutex = MONGO_MAKE_LATCH("SessionCatalogMigrationSource::_newOplogMutex");

    // The average size of documents in config.transactions.
    // config.transactions中文档的平均大小
    uint64_t _averageSessionDocSize{0};

    // Stores oplog opTime of new writes that are coming in.
    // 存储新写操作的oplog opTime
    std::list<std::pair<repl::OpTime, EntryAtOpTimeType>> _newWriteOpTimeList;

    // Used to store the last fetched and processed oplog entry from _newWriteOpTimeList. This
    // enables calling get() multiple times.
    // 存储从_newWriteOpTimeList最后获取和处理的oplog条目
    // 支持多次调用get()方法
    boost::optional<repl::OplogEntry> _lastFetchedNewWriteOplog;

    // Used to store the pre/post image oplog entry when _lastFetchedNewWriteOplog if there is one.
    // 存储_lastFetchedNewWriteOplog的前置/后置镜像oplog条目(如果有的话)
    boost::optional<repl::OplogEntry> _lastFetchedNewWriteOplogImage;

    // Used to store the last fetched oplog entries from _newWriteOpTimeList that have not been
    // processed.
    // 存储从_newWriteOpTimeList获取但尚未处理的oplog条目
    std::vector<repl::OplogEntry> _unprocessedNewWriteOplogBuffer;

    // ========== 状态管理和通知机制 ==========
    
    // Stores the current state.
    // 存储当前状态
    State _state{State::kActive};

    // Holds the latest request for notification of new oplog entries that needs to be fetched.
    // Sets to true if there is no need to fetch an oplog anymore (for example, because migration
    // aborted).
    // 保存需要获取的新oplog条目的最新通知请求
    // 如果不再需要获取oplog则设置为true(例如迁移中止)
    std::shared_ptr<Notification<bool>> _newOplogNotification;

    // ========== 统计和监控 ==========
    
    // The number of session oplog entries that need to be migrated
    // from the source to the destination
    // 需要从源迁移到目标的会话oplog条目数量
    AtomicWord<long long> _sessionOplogEntriesToBeMigratedSoFar{0};

    // There are optimizations so that we do not send all of the oplog
    // entries to the destination. This stat provides a lower bound on the number of session oplog
    // entries that we did not send to the destination. It is a lower bound because some of the
    // optimizations do not allow us to know the exact number of oplog entries we skipped.
    // 由于优化，我们不会将所有oplog条目发送到目标
    // 此统计提供未发送到目标的会话oplog条目数量的下界
    // 这是下界，因为某些优化不允许我们知道跳过的oplog条目的确切数量
    AtomicWord<long long> _sessionOplogEntriesSkippedSoFarLowerBound{0};
};

}  // namespace mongo
