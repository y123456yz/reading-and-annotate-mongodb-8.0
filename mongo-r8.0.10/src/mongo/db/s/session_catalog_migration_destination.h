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

#include <memory>
#include <string>

#include "mongo/base/status_with.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/auth/cluster_auth_mode.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/repl/oplog_entry.h"
#include "mongo/db/repl/optime.h"
#include "mongo/db/s/migration_session_id.h"
#include "mongo/db/session/logical_session_id.h"
#include "mongo/db/session/logical_session_id_gen.h"
#include "mongo/db/shard_id.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/platform/mutex.h"
#include "mongo/stdx/condition_variable.h"
#include "mongo/stdx/thread.h"
#include "mongo/util/cancellation.h"
#include "mongo/util/concurrency/with_lock.h"

namespace mongo {

class ConnectionString;
class ServiceContext;
class OperationContext;

/**
 * SessionCatalogMigrationDestination 类负责在分片 chunk 迁移过程中从源分片接收和处理会话相关数据。
 * 
 * 主要职责：
 * 1. 会话数据迁移：从源分片拉取 retryable writes 和多文档事务的 oplog 记录
 * 2. 事务一致性保证：确保事务状态在分片间正确传输和重建
 * 3. 可重试写操作处理：维护可重试写操作的幂等性和执行历史
 * 4. 后台线程管理：通过独立线程异步处理会话数据迁移
 * 5. 状态机维护：管理迁移的各个阶段状态转换
 * 6. 错误处理和恢复：处理网络故障、超时等异常情况
 * 7. 生命周期协调：与主迁移流程协调启动、完成和中止操作
 * 
 * 工作流程：
 * - start(): 启动后台线程开始拉取会话数据
 * - _retrieveSessionStateFromSource(): 循环向源分片发送请求获取会话 oplog
 * - _processSessionOplog(): 解析并应用每条会话相关的 oplog 记录
 * - finish(): 通知源分片停止生成新的会话数据
 * - join(): 等待后台线程完成并同步最终状态
 * 
 * 状态管理：
 * - NotStarted: 初始状态，尚未开始迁移
 * - Migrating: 正在迁移会话数据
 * - ReadyToCommit: 已准备好提交，等待最终确认
 * - Committing: 正在提交会话数据
 * - Done: 迁移完成
 * - ErrorOccurred: 发生错误
 * 
 * 线程安全：
 * - 使用互斥锁保护内部状态
 * - 支持取消令牌响应外部中断
 * - 原子计数器跟踪迁移进度
 * 
 * 与迁移主流程的集成：
 * - 在迁移第4阶段启动（_sessionMigration->start()）
 * - 在迁移第6阶段通知完成（_sessionMigration->finish()）
 * - 在迁移第7阶段等待完成（_sessionMigration->join()）
 * 
 * 该类确保了分片迁移过程中会话相关数据的完整性和一致性，
 * 保证事务和可重试写操作在目标分片上能够正确继续执行。
 *
 * Provides infrastructure for retrieving session information that needs to be migrated from
 * the source migration shard.
 */
class SessionCatalogMigrationDestination {
    SessionCatalogMigrationDestination(const SessionCatalogMigrationDestination&) = delete;
    SessionCatalogMigrationDestination& operator=(const SessionCatalogMigrationDestination&) =
        delete;

public:
    enum class State {
        NotStarted,      // 初始状态，尚未开始迁移
        Migrating,       // 正在迁移会话数据
        ReadyToCommit,   // 已准备好提交，等待最终确认
        Committing,      // 正在提交会话数据
        ErrorOccurred,   // 发生错误
        Done,           // 迁移完成
    };

    struct ProcessOplogResult {
        LogicalSessionId sessionId;                    // 逻辑会话ID
        TxnNumber txnNum{kUninitializedTxnNumber};    // 事务号

        repl::OpTime oplogTime;                       // oplog 时间戳
        bool isPrePostImage = false;                  // 是否为前后镜像
    };

    SessionCatalogMigrationDestination(NamespaceString nss,
                                       ShardId fromShard,
                                       MigrationSessionId migrationSessionId,
                                       CancellationToken cancellationToken);
    ~SessionCatalogMigrationDestination();

    /**
     * Spawns a separate thread to initiate the session info transfer to this shard.
     * 启动独立线程开始会话信息传输到当前分片
     * 在迁移第4阶段被调用，开始从源分片拉取会话数据
     */
    void start(ServiceContext* service);

    /**
     * Signals to this object that the source shard will no longer create generate new
     * session info to transfer. In other words, once the source shard returns an empty
     * result for session info to transfer after this call, it is safe for this to stop.
     * 通知此对象源分片将不再生成新的会话信息进行传输
     * 在迁移第6阶段被调用，准备进入提交阶段
     */
    void finish();

    /**
     * Returns true if the thread to initiate the session info transfer has been spawned and is
     * therefore joinable.
     * 检查会话信息传输线程是否已创建并可以被join
     */
    bool joinable() const;

    /**
     * Joins the spawned thread called by start(). Should only be called after finish()
     * was called.
     * 等待start()创建的线程完成，应该在调用finish()之后调用
     * 在迁移第7阶段被调用，确保所有会话数据已成功迁移
     */
    void join();

    /**
     * Forces this into an error state which will also stop session transfer thread.
     * 强制设置为错误状态，这也会停止会话传输线程
     */
    void forceFail(StringData errMsg);

    /**
     * Returns the session id for the migration.
     * 返回迁移的会话ID
     */
    MigrationSessionId getMigrationSessionId() const;

    /**
     * Returns the current state.
     * 返回当前状态
     */
    State getState();

    /**
     * Returns the error message stored. This is only valid when getState() == ErrorOccurred.
     * 返回存储的错误消息，仅在getState() == ErrorOccurred时有效
     */
    std::string getErrMsg();

    /**
     * Returns the number of session oplog entries processed by the _processSessionOplog() method
     * 返回_processSessionOplog()方法处理的会话oplog条目数量
     */
    long long getSessionOplogEntriesMigrated();

private:
    /**
     * 后台线程主函数：从源分片检索会话状态
     * 核心功能：
     * 1. 循环向源分片发送 shardsvrGetSessionCatalogEntryList 命令
     * 2. 获取会话相关的 oplog 记录批次
     * 3. 调用 _processSessionOplog 处理每条记录
     * 4. 当收到 finish() 通知且源分片返回空结果时退出
     */
    void _retrieveSessionStateFromSource(ServiceContext* service);

    /**
     * 处理单条会话 oplog 记录
     * 核心功能：
     * 1. 解析 oplog BSON 中的会话信息（lsid, txnNumber, opTime）
     * 2. 根据 oplog 类型（retryable write 或 transaction commit）进行不同处理
     * 3. 更新本地 SessionCatalog 中的会话状态
     * 4. 返回处理结果，包含下次请求的起始参数
     */
    ProcessOplogResult _processSessionOplog(const BSONObj& oplogBSON,
                                            const ProcessOplogResult& lastResult,
                                            ServiceContext* serviceContext,
                                            CancellationToken cancellationToken);

    /**
     * 设置错误状态并记录错误信息
     */
    void _errorOccurred(StringData errMsg);

    const NamespaceString _nss;                        // 目标命名空间
    const ShardId _fromShard;                          // 源分片标识
    const MigrationSessionId _migrationSessionId;      // 迁移会话ID
    const CancellationToken _cancellationToken;        // 取消令牌，用于响应外部中断

    stdx::thread _thread;                              // 后台工作线程

    // Protects _state and _errMsg.
    Mutex _mutex = MONGO_MAKE_LATCH("SessionCatalogMigrationDestination::_mutex");
    State _state = State::NotStarted;                  // 当前迁移状态
    std::string _errMsg;  // valid only if _state == ErrorOccurred.

    // The number of session oplog entries processed. This is not always equal to the number of
    // session oplog entries comitted because entries may have been processed but not committed
    AtomicWord<long long> _sessionOplogEntriesMigrated{0};
};

}  // namespace mongo