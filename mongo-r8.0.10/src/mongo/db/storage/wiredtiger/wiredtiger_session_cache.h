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

#include <cstddef>
#include <cstdint>
#include <list>
#include <memory>
#include <string>
#include <vector>
#include <wiredtiger.h>

#include "mongo/db/operation_context.h"
#include "mongo/db/storage/journal_listener.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_compiled_configuration.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_snapshot_manager.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/platform/mutex.h"
#include "mongo/stdx/condition_variable.h"
#include "mongo/util/clock_source.h"
#include "mongo/util/time_support.h"
#include "mongo/util/timer.h"

namespace mongo {

class WiredTigerKVEngine;
class WiredTigerSessionCache;
//WiredTigerSession::releaseCursor


/**
 * WiredTigerCachedCursor - WiredTiger 缓存游标类
 * 
 * 该类用于封装缓存的 WiredTiger 游标，包含游标的元数据信息。
 * 用于在 WiredTigerSession 的游标缓存中存储和管理游标。
 */
class WiredTigerCachedCursor {
public:
    /**
     * 构造函数
     * @param id 源 ID，分配给每个 URI 的唯一标识符
     * @param gen 生成代数，用于实现 LRU 淘汰策略
     * @param cursor WiredTiger 游标指针
     * @param config 游标配置字符串，确保配置匹配的游标才能被复用
     */
    WiredTigerCachedCursor(uint64_t id, uint64_t gen, WT_CURSOR* cursor, std::string config)
        : _id(id), _gen(gen), _cursor(cursor), _config(std::move(config)) {}

    uint64_t _id;   // Source ID, assigned to each URI
                    // 源 ID，分配给每个 URI 的唯一标识符
                    // 用于标识游标对应的表或索引

    uint64_t _gen;  // Generation, used to age out old cursors
                    // 生成代数，用于实现游标的老化淘汰
                    // 数值越小表示游标越老，用于 LRU 缓存淘汰策略

    WT_CURSOR* _cursor;  // WiredTiger 游标指针
                         // 实际的 WT_CURSOR 对象，用于执行数据库操作

    std::string _config;  // Cursor config. Do not serve cursors with different configurations
                          // 游标配置字符串，记录创建游标时使用的配置
                          // 确保只有配置完全相同的游标才能被复用
                          // 例如："overwrite=false,read_once=true"
};

/**
 * This is a structure that caches 1 cursor for each uri.
 * The idea is that there is a pool of these somewhere.
 * NOT THREADSAFE
 */
/**
 * WiredTigerSession - WiredTiger 会话封装类
 * 
 * 该类封装了 WiredTiger 的 WT_SESSION，提供游标缓存管理功能。
 * 主要用于减少游标创建/销毁的开销，提高数据库操作性能。
 */
class WiredTigerSession {
public:
    /**
     * Creates a new WT session on the specified connection.
     * 在指定的连接上创建一个新的 WT 会话
     *
     * @param conn WT connection - WiredTiger 连接对象
     * @param epoch In which session cache cleanup epoch was this session instantiated.
     *              会话缓存清理周期，用于判断会话是否过期
     * @param cursorEpoch In which cursor cache cleanup epoch was this session instantiated.
     *                    游标缓存清理周期，用于判断游标是否过期
     */
    WiredTigerSession(WT_CONNECTION* conn, uint64_t epoch = 0, uint64_t cursorEpoch = 0);

    /**
     * Creates a new WT session on the specified connection.
     * 在指定的连接上创建一个新的 WT 会话（带缓存引用）
     *
     * @param conn WT connection - WiredTiger 连接对象
     * @param cache The WiredTigerSessionCache that owns this session.
     *              拥有此会话的会话缓存对象
     * @param epoch In which session cache cleanup epoch was this session instantiated.
     * @param cursorEpoch In which cursor cache cleanup epoch was this session instantiated.
     */
    WiredTigerSession(WT_CONNECTION* conn,
                      WiredTigerSessionCache* cache,
                      uint64_t epoch = 0,
                      uint64_t cursorEpoch = 0);

    ~WiredTigerSession();

    // 获取底层的 WT_SESSION 指针
    WT_SESSION* getSession() const {
        return _session;
    }

    /**
     * Gets a cursor on the table id 'id' with optional configuration, 'config'.
     * 获取指定表 ID 的游标，支持可选配置
     *
     * This may return a cursor from the cursor cache and these cursors should *always* be released
     * into the cache by calling releaseCursor().
     * 此方法可能返回缓存的游标，这些游标必须通过 releaseCursor() 释放回缓存
     */
    WT_CURSOR* getCachedCursor(uint64_t id, const std::string& config);

    /**
     * Create a new cursor and ignore the cache.
     * 创建新游标，忽略缓存
     *
     * The config string specifies optional arguments for the cursor. For example, when
     * the config contains 'read_once=true', this is intended for operations that will be
     * sequentially scanning large amounts of data.
     * 配置字符串指定游标的可选参数。例如，当配置包含 'read_once=true' 时，
     * 表示这是用于顺序扫描大量数据的操作。
     *
     * This will never return a cursor from the cursor cache, and these cursors should *never* be
     * released into the cache by calling releaseCursor(). Use closeCursor() instead.
     * 此方法永远不会返回缓存的游标，这些游标也不应该通过 releaseCursor() 释放到缓存。
     * 应该使用 closeCursor() 关闭。
     */
    WT_CURSOR* getNewCursor(const std::string& uri, const char* config);

    /**
     * Wrapper for getNewCursor() without a config string.
     * getNewCursor() 的无配置字符串包装方法
     */
    WT_CURSOR* getNewCursor(const std::string& uri) {
        return getNewCursor(uri, nullptr);
    }

    /**
     * Release a cursor into the cursor cache and close old cursors if the number of cursors in the
     * cache exceeds wiredTigerCursorCacheSize.
     * 将游标释放到游标缓存，如果缓存中的游标数量超过 wiredTigerCursorCacheSize，则关闭旧游标
     * 
     * The exact cursor config that was used to create the cursor must be provided or subsequent
     * users will retrieve cursors with incorrect configurations.
     * 必须提供创建游标时使用的确切配置，否则后续用户可能获取到配置错误的游标
     *
     * Additionally calls into the WiredTigerKVEngine to see if the SizeStorer needs to be flushed.
     * The SizeStorer gets flushed on a periodic basis.
     * 额外调用 WiredTigerKVEngine 来检查是否需要刷新 SizeStorer（定期刷新）
     */
    void releaseCursor(uint64_t id, WT_CURSOR* cursor, std::string config);

    /**
     * Close a cursor without releasing it into the cursor cache.
     * 关闭游标而不将其释放到游标缓存
     */
    void closeCursor(WT_CURSOR* cursor);

    /**
     * Closes all cached cursors matching the uri.  If the uri is empty,
     * all cached cursors are closed.
     * 关闭所有匹配 URI 的缓存游标。如果 URI 为空，则关闭所有缓存游标
     */
    void closeAllCursors(const std::string& uri);

    // 获取当前已分配出去的游标数量
    int cursorsOut() const {
        return _cursorsOut;
    }

    // 获取缓存中的游标数量
    int cachedCursors() const {
        return _cursors.size();
    }

    // 生成唯一的表 ID
    static uint64_t genTableId();

    /**
     * For special cursors. Guaranteed never to collide with genTableId() ids.
     * 特殊游标的 ID 枚举，保证不会与 genTableId() 生成的 ID 冲突
     */
    enum TableId {
        /* For "metadata:" cursors - 用于元数据游标 */
        kMetadataTableId,
        /* For "metadata:create" cursors - 用于元数据创建游标 */
        kMetadataCreateTableId,
        /* The start of non-special table ids for genTableId() - 非特殊表 ID 的起始值 */
        kLastTableId
    };

    // 设置空闲过期时间
    void setIdleExpireTime(Date_t idleExpireTime) {
        _idleExpireTime = idleExpireTime;
    }

    // 获取空闲过期时间
    Date_t getIdleExpireTime() const {
        return _idleExpireTime;
    }

    // 设置编译配置对象
    void setCompiledConfigurationsPerConnection(CompiledConfigurationsPerConnection* compiled) {
        _compiled = compiled;
    }

    // 获取编译配置对象
    CompiledConfigurationsPerConnection* getCompiledConfigurationsPerConnection() {
        return _compiled;
    }

    /**
     * Reconfigures the session. Stores the config string that undoes this change.
     * 重新配置会话。存储用于撤销此更改的配置字符串
     */
    void reconfigure(const std::string& newConfig, std::string undoConfig);

    /**
     * Reset the configurations for this session to the default. This should be done before we
     * release this session back into the session cache, so that any recovery unit that may use this
     * session in the future knows that the session will have the default configuration.
     * 将此会话的配置重置为默认值。这应该在我们将会话释放回会话缓存之前完成，
     * 以便将来可能使用此会话的任何恢复单元都知道会话将具有默认配置。
     */
    void resetSessionConfiguration();

    // 获取撤销配置字符串集合
    stdx::unordered_set<std::string> getUndoConfigStrings() {
        return _undoConfigStrings;
    }

private:
    friend class WiredTigerSessionCache;
    friend class WiredTigerKVEngine;

    // The cursor cache is a list of pairs that contain an ID and cursor
    // 游标缓存是包含 ID 和游标的列表
    typedef std::list<WiredTigerCachedCursor> CursorCache;

    // Used internally by WiredTigerSessionCache
    // 由 WiredTigerSessionCache 内部使用，获取会话的 epoch
    uint64_t _getEpoch() const {
        return _epoch;
    }

    const uint64_t _epoch;               // 会话的生命周期标记
    WT_SESSION* _session;                // owned - 拥有的 WT 会话指针
    CursorCache _cursors;                // owned - 游标缓存列表
    uint64_t _cursorGen;                 // 游标生成计数器，用于 LRU 管理
    int _cursorsOut;                     // 当前已分配出去的游标数量

    WiredTigerSessionCache* _cache;                  // not owned - 所属的会话缓存
    CompiledConfigurationsPerConnection* _compiled;  // not owned - 编译配置对象

    Date_t _idleExpireTime;              // 空闲过期时间

    // A set that contains the undo config strings for any reconfigurations we might have performed
    // on a session during the lifetime of this recovery unit. We use these to reset the session to
    // its default configuration before returning it to the session cache.
    // 包含在恢复单元生命周期内对会话执行的任何重新配置的撤销配置字符串集合。
    // 在将会话返回到会话缓存之前，我们使用这些字符串将会话重置为默认配置。
    stdx::unordered_set<std::string> _undoConfigStrings;
};

/**
 *  This cache implements a shared pool of WiredTiger sessions with the goal to amortize the
 *  cost of session creation and destruction over multiple uses.
 *  
 *  此缓存实现了 WiredTiger 会话的共享池，目标是通过多次使用来摊销会话创建和销毁的成本。
 */
class WiredTigerSessionCache {
public:
    WiredTigerSessionCache(WiredTigerKVEngine* engine);
    WiredTigerSessionCache(WT_CONNECTION* conn,
                           ClockSource* cs,
                           WiredTigerKVEngine* engine = nullptr);
    ~WiredTigerSessionCache();

    /**
     * This deleter automatically releases WiredTigerSession objects when no longer needed.
     * 此删除器在不再需要时自动释放 WiredTigerSession 对象
     */
    class WiredTigerSessionDeleter {
    public:
        void operator()(WiredTigerSession* session) const;
    };

    /**
     * Specifies what data will get flushed to disk in a WiredTigerSessionCache::waitUntilDurable()
     * call.
     * 指定在 WiredTigerSessionCache::waitUntilDurable() 调用中将哪些数据刷新到磁盘
     */
    enum class Fsync {
        // Flushes only the journal (oplog) to disk.
        // If journaling is disabled, checkpoints all of the data.
        // 仅将日志（oplog）刷新到磁盘。如果禁用日志记录，则检查点所有数据。
        kJournal,
        // Checkpoints data up to the stable timestamp.
        // If journaling is disabled, checkpoints all of the data.
        // 检查点数据直到稳定时间戳。如果禁用日志记录，则检查点所有数据。
        kCheckpointStableTimestamp,
        // Checkpoints all of the data.
        // 检查点所有数据
        kCheckpointAll,
    };

    /**
     * Controls whether or not WiredTigerSessionCache::waitUntilDurable() updates the
     * JournalListener.
     * 控制 WiredTigerSessionCache::waitUntilDurable() 是否更新 JournalListener
     */
    enum class UseJournalListener { kUpdate, kSkip };

    // RAII type to block and unblock the WiredTigerSessionCache to shut down.
    // RAII 类型，用于阻止和解除阻止 WiredTigerSessionCache 关闭
    class BlockShutdown {
    public:
        BlockShutdown(WiredTigerSessionCache* cache) : _cache(cache) {
            _cache->_shuttingDown.fetchAndAdd(1);
        }

        ~BlockShutdown() {
            _cache->_shuttingDown.fetchAndSubtract(1);
        }

    private:
        WiredTigerSessionCache* _cache;
    };

    /**
     * Indicates that WiredTiger should be configured to cache cursors.
     * 指示 WiredTiger 应该配置为缓存游标
     */
    static bool isEngineCachingCursors();

    /**
     * Returns a smart pointer to a previously released session for reuse, or creates a new session.
     * This method must only be called while holding the global lock to avoid races with
     * shuttingDown, but otherwise is thread safe.
     * 
     * 返回可重用的先前释放会话的智能指针，或创建新会话。
     * 此方法必须在持有全局锁时调用以避免与 shuttingDown 的竞争，但其他方面是线程安全的。
     */
    std::unique_ptr<WiredTigerSession, WiredTigerSessionDeleter> getSession();

    /**
     * Get a count of idle sessions in the session cache.
     * 获取会话缓存中空闲会话的数量
     */
    size_t getIdleSessionsCount();

    /**
     * Closes all cached sessions whose idle expiration time has been reached.
     * 关闭所有已达到空闲过期时间的缓存会话
     */
    void closeExpiredIdleSessions(int64_t idleTimeMillis);

    /**
     * Free all cached sessions and ensures that previously acquired sessions will be freed on
     * release.
     * 释放所有缓存的会话，并确保先前获取的会话在释放时被释放
     */
    void closeAll();

    /**
     * Closes all cached cursors matching the uri.  If the uri is empty,
     * all cached cursors are closed.
     * 关闭所有匹配 URI 的缓存游标。如果 URI 为空，则关闭所有缓存游标
     */
    void closeAllCursors(const std::string& uri);

    /**
     * Transitions the cache to shutting down mode. Any already released sessions are freed and
     * any sessions released subsequently are leaked. Must be called while holding the global
     * lock in exclusive mode to avoid races with getSession.
     * 
     * 将缓存转换为关闭模式。释放所有已释放的会话，后续释放的会话将被泄漏。
     * 必须在以独占模式持有全局锁时调用，以避免与 getSession 的竞争。
     */
    void shuttingDown();

    /**
     * True when in the process of shutting down.
     * 正在关闭过程中时返回 true
     */
    bool isShuttingDown();

    /**
     * Restart a previously shut down cache.
     * 重启先前关闭的缓存
     */
    void restart();

    // 检查是否是临时存储引擎
    bool isEphemeral();

    /**
     * Waits until all commits that happened before this call are made durable.
     * 等待直到此调用之前发生的所有提交都持久化
     *
     * Specifying Fsync::kJournal will flush only the (oplog) journal to disk. Callers are
     * serialized by a mutex and will return early if it is discovered that another thread started
     * and completed a flush while they slept.
     * 指定 Fsync::kJournal 将仅将（oplog）日志刷新到磁盘。调用者通过互斥锁序列化，
     * 如果发现另一个线程在他们休眠时启动并完成了刷新，则会提前返回。
     *
     * Specifying Fsync::kCheckpointStableTimestamp will take a checkpoint up to and including the
     * stable timestamp.
     * 指定 Fsync::kCheckpointStableTimestamp 将创建包含稳定时间戳的检查点。
     *
     * Specifying Fsync::kCheckpointAll, or if journaling is disabled with kJournal or
     * kCheckpointStableTimestamp, causes a checkpoint to be taken of all of the data.
     * 指定 Fsync::kCheckpointAll，或者如果使用 kJournal 或 kCheckpointStableTimestamp 
     * 禁用了日志记录，将导致对所有数据进行检查点。
     *
     * Taking a checkpoint has the benefit of persisting unjournaled writes.
     * 创建检查点的好处是持久化未记录日志的写入。
     *
     * 'useListener' controls whether or not the JournalListener is updated with the last durable
     * value of the timestamp that it tracks. The JournalListener's token is fetched before writing
     * out to disk and set afterwards to update the repl layer durable timestamp. The
     * JournalListener operations can throw write interruption errors.
     * 'useListener' 控制是否使用跟踪的时间戳的最后持久值更新 JournalListener。
     * JournalListener 的令牌在写入磁盘之前获取，之后设置以更新复制层持久时间戳。
     * JournalListener 操作可能抛出写入中断错误。
     *
     * Uses a temporary session. Safe to call without any locks, even during shutdown.
     * 使用临时会话。即使在关闭期间也可以安全地调用而无需任何锁。
     */
    void waitUntilDurable(OperationContext* opCtx, Fsync syncType, UseJournalListener useListener);

    /**
     * Waits until a prepared unit of work has ended (either been commited or aborted). This
     * should be used when encountering WT_PREPARE_CONFLICT errors. The caller is required to retry
     * the conflicting WiredTiger API operation. A return from this function does not guarantee that
     * the conflicting transaction has ended, only that one prepared unit of work in the process has
     * signaled that it has ended.
     * 
     * 等待准备的工作单元结束（已提交或中止）。遇到 WT_PREPARE_CONFLICT 错误时应使用此方法。
     * 调用者需要重试冲突的 WiredTiger API 操作。从此函数返回并不保证冲突事务已结束，
     * 只是进程中的一个准备的工作单元已发出结束信号。
     * 
     * Accepts an Interruptible that will throw an AssertionException when interrupted.
     * 接受一个 Interruptible，在中断时将抛出 AssertionException。
     *
     * This method is provided in WiredTigerSessionCache and not RecoveryUnit because all recovery
     * units share the same session cache, and we want a recovery unit on one thread to signal all
     * recovery units waiting for prepare conflicts across all other threads.
     * 此方法在 WiredTigerSessionCache 中提供而不是在 RecoveryUnit 中，因为所有恢复单元
     * 共享同一个会话缓存，我们希望一个线程上的恢复单元向所有其他线程上等待准备冲突的
     * 恢复单元发出信号。
     */
    void waitUntilPreparedUnitOfWorkCommitsOrAborts(Interruptible& interruptible,
                                                    uint64_t lastCount);

    /**
     * Notifies waiters that the caller's perpared unit of work has ended (either committed or
     * aborted).
     * 通知等待者调用者的准备工作单元已结束（已提交或中止）
     */
    void notifyPreparedUnitOfWorkHasCommittedOrAborted();

    // 获取 WiredTiger 连接
    WT_CONNECTION* conn() const {
        return _conn;
    }

    // 获取快照管理器的可变引用
    WiredTigerSnapshotManager& snapshotManager() {
        return _snapshotManager;
    }
    
    // 获取快照管理器的常量引用
    const WiredTigerSnapshotManager& snapshotManager() const {
        return _snapshotManager;
    }

    // 设置日志监听器
    void setJournalListener(JournalListener* jl);

    // 获取 KV 引擎
    WiredTigerKVEngine* getKVEngine() const {
        return _engine;
    }

    // 获取准备提交或中止计数
    std::uint64_t getPrepareCommitOrAbortCount() const {
        return _prepareCommitOrAbortCounter.loadRelaxed();
    }

    // 获取编译配置
    CompiledConfigurationsPerConnection* getCompiledConfigurations() {
        return &_compiledConfigurations;
    }

private:
    friend class WiredTigerSession;
    /**
     * Looks up the journal listener under a mutex along.
     * Returns JournalListener along with an optional token if requested
     * by the UseJournalListener value.
     * 
     * 在互斥锁下查找日志监听器。
     * 如果 UseJournalListener 值请求，则返回 JournalListener 以及可选令牌。
     */
    std::pair<JournalListener*, boost::optional<JournalListener::Token>>
    _getJournalListenerWithToken(OperationContext* opCtx, UseJournalListener useListener);

    WT_CONNECTION* _conn;             // not owned - WiredTiger 连接（不拥有）
    ClockSource* const _clockSource;  // not owned - 时钟源（不拥有）
    WiredTigerKVEngine* _engine;      // not owned, might be NULL - KV 引擎（不拥有，可能为空）
    WiredTigerSnapshotManager _snapshotManager;  // 快照管理器
    CompiledConfigurationsPerConnection _compiledConfigurations;  // 编译配置

    // Used as follows:
    //   The low 31 bits are a count of active calls that need to block shutdown.
    //   The high bit is a flag that is set if and only if we're shutting down.
    // 使用方式如下：
    //   低 31 位是需要阻止关闭的活动调用计数。
    //   高位是一个标志，当且仅当我们正在关闭时设置。
    AtomicWord<unsigned> _shuttingDown{0};
    static const uint32_t kShuttingDownMask = 1 << 31;

    Mutex _cacheLock = MONGO_MAKE_LATCH("WiredTigerSessionCache::_cacheLock");  // 缓存锁
    typedef std::vector<WiredTigerSession*> SessionCache;
    SessionCache _sessions;  // 会话缓存容器

    // Bumped when all open sessions need to be closed
    // 当所有打开的会话需要关闭时递增
    AtomicWord<unsigned long long> _epoch;  // atomic so we can check it outside of the lock

    // Counter and critical section mutex for waitUntilDurable
    // waitUntilDurable 的计数器和临界区互斥锁
    AtomicWord<unsigned> _lastSyncTime;
    Mutex _lastSyncMutex = MONGO_MAKE_LATCH("WiredTigerSessionCache::_lastSyncMutex");

    // Mutex and cond var for waiting on prepare commit or abort.
    // 用于等待准备提交或中止的互斥锁和条件变量
    Mutex _prepareCommittedOrAbortedMutex =
        MONGO_MAKE_LATCH("WiredTigerSessionCache::_prepareCommittedOrAbortedMutex");
    stdx::condition_variable _prepareCommittedOrAbortedCond;
    AtomicWord<std::uint64_t> _prepareCommitOrAbortCounter{0};

    // Protects getting and setting the _journalListener below.
    // 保护下面 _journalListener 的获取和设置
    Mutex _journalListenerMutex = MONGO_MAKE_LATCH("WiredTigerSessionCache::_journalListenerMutex");

    // Notified when we commit to the journal.
    // 提交到日志时通知
    //
    // This variable should be accessed under the _journalListenerMutex above and saved in a local
    // variable before use. That way, we can avoid holding a mutex across calls on the object. It is
    // only allowed to be set once, in order to ensure the memory to which a copy of the pointer
    // points is always valid.
    // 此变量应在上述 _journalListenerMutex 下访问，并在使用前保存在局部变量中。
    // 这样，我们可以避免在对象调用期间持有互斥锁。只允许设置一次，以确保指针副本指向的内存始终有效。
    JournalListener* _journalListener = nullptr;

    WT_SESSION* _waitUntilDurableSession = nullptr;  // owned, and never explicitly closed
                                                     // (uses connection close to clean up)
                                                     // 拥有，从不显式关闭（使用连接关闭进行清理）
    // Tracks the time since the last _waitUntilDurableSession reset().
    // 跟踪自上次 _waitUntilDurableSession reset() 以来的时间
    Timer _timeSinceLastDurabilitySessionReset;

    /**
     * Returns a session to the cache for later reuse. If closeAll was called between getting this
     * session and releasing it, the session is directly released. This method is thread safe.
     * 
     * 将会话返回到缓存以供以后重用。如果在获取此会话和释放它之间调用了 closeAll，
     * 则会话将直接释放。此方法是线程安全的。
     */
    void releaseSession(WiredTigerSession* session);
};

/**
 * A unique handle type for WiredTigerSession pointers obtained from a WiredTigerSessionCache.
 * 从 WiredTigerSessionCache 获取的 WiredTigerSession 指针的唯一句柄类型
 */
typedef std::unique_ptr<WiredTigerSession,
                        typename WiredTigerSessionCache::WiredTigerSessionDeleter>
    UniqueWiredTigerSession;

// 修复消息常量：建议用户阅读有关使用 --repair 启动 MongoDB 的文档
static constexpr char kWTRepairMsg[] =
    "Please read the documentation for starting MongoDB with --repair here: "
    "http://dochub.mongodb.org/core/repair";
}  // namespace mongo
