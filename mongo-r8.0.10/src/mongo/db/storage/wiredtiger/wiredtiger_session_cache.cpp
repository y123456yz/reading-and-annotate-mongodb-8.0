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


#include <cerrno>
#include <cstdlib>
#include <memory>
#include <mutex>

#include <boost/move/utility_core.hpp>
#include <boost/optional/optional.hpp>
#include <wiredtiger.h>

#include "mongo/base/error_codes.h"
#include "mongo/base/status.h"
#include "mongo/db/global_settings.h"
#include "mongo/db/repl/repl_settings.h"
#include "mongo/db/storage/journal_listener.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_kv_engine.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_parameters_gen.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_session_cache.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_session_data.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_util.h"
#include "mongo/logv2/log.h"
#include "mongo/logv2/log_attr.h"
#include "mongo/logv2/log_component.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/duration.h"
#include "mongo/util/str.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStorage

namespace mongo {

/**
 * WiredTigerSession 构造函数（基础版本）
 * 创建一个新的 WiredTiger 会话，使用快照隔离级别
 */
WiredTigerSession::WiredTigerSession(WT_CONNECTION* conn, uint64_t epoch, uint64_t cursorEpoch)
    : _epoch(epoch),                     // 会话所属的生命周期epoch
      _session(nullptr),                 // WT_SESSION 指针，初始为空
      _cursorGen(0),                     // 游标生成计数器，用于LRU管理
      _cursorsOut(0),                    // 当前借出的游标数量
      _compiled(nullptr),                // 编译配置对象，初始为空
      _idleExpireTime(Date_t::min()) {  // 空闲过期时间，初始为最小值
    // 打开一个新的 WiredTiger 会话，使用快照隔离级别
    invariantWTOK(conn->open_session(conn, nullptr, "isolation=snapshot", &_session), nullptr);
}

/**
 * WiredTigerSession 构造函数（带缓存版本）
 * 创建一个新的 WiredTiger 会话，并关联到会话缓存
 */
WiredTigerSession::WiredTigerSession(WT_CONNECTION* conn,
                                     WiredTigerSessionCache* cache,
                                     uint64_t epoch,
                                     uint64_t cursorEpoch)
    : _epoch(epoch),
      _session(nullptr),
      _cursorGen(0),
      _cursorsOut(0),
      _cache(cache),                     // 关联的会话缓存
      _compiled(nullptr),
      _idleExpireTime(Date_t::min()) {
    invariantWTOK(conn->open_session(conn, nullptr, "isolation=snapshot", &_session), nullptr);
    // 设置编译配置，从缓存中获取
    setCompiledConfigurationsPerConnection(cache->getCompiledConfigurations());
}

/**
 * WiredTigerSession 析构函数
 * 关闭 WiredTiger 会话
 */
WiredTigerSession::~WiredTigerSession() {
    if (_session) {
        invariantWTOK(_session->close(_session, nullptr), nullptr);
    }
}

namespace {
/**
 * 内部函数：打开一个 WiredTiger 游标
 * 处理各种错误情况，包括 EBUSY（正在验证）和 ENOENT（未找到）
 */
void _openCursor(WT_SESSION* session,
                 const std::string& uri,
                 const char* config,
                 WT_CURSOR** cursorOut) {
    int ret = session->open_cursor(session, uri.c_str(), nullptr, config, cursorOut);
    if (ret == 0) {
        return;  // 成功打开
    }

    auto status = wtRCToStatus(ret, session);

    if (ret == EBUSY) {
        // This may happen when there is an ongoing full validation, with a call to WT::verify.
        // Other operations which may trigger this include salvage, rollback_to_stable, upgrade,
        // alter, or if there is a bulk cursor open. Mongo (currently) does not run any of
        // these operations concurrently with this code path, except for validation.
        // 当有正在进行的完整验证时可能发生这种情况

        uassertStatusOK(status);
    } else if (ret == ENOENT) {
        uasserted(ErrorCodes::CursorNotFound,
                  str::stream() << "Failed to open a WiredTiger cursor. Reason: " << status
                                << ", uri: " << uri << ", config: " << config);
    }

    // 其他错误被视为致命错误，可能是数据损坏
    LOGV2_FATAL_NOTRACE(50882,
                        "Failed to open WiredTiger cursor. This may be due to data corruption",
                        "uri"_attr = uri,
                        "config"_attr = config,
                        "error"_attr = status,
                        "message"_attr = kWTRepairMsg);
}
}  // namespace

/**
 * 从游标缓存中获取游标
 * 使用 LRU 策略，返回最近使用的匹配游标
 */
WT_CURSOR* WiredTigerSession::getCachedCursor(uint64_t id, const std::string& config) {
    // Find the most recently used cursor
    // 查找最近使用的游标
    for (CursorCache::iterator i = _cursors.begin(); i != _cursors.end(); ++i) {
        // Ensure that all properties of this cursor are identical to avoid mixing cursor
        // configurations. Note that this uses an exact string match, so cursor configurations with
        // parameters in different orders will not be considered equivalent.
        // 确保游标的所有属性都相同，避免混合游标配置
        if (i->_id == id && i->_config == config) {
            WT_CURSOR* c = i->_cursor;
            _cursors.erase(i);  // 从缓存中移除
            _cursorsOut++;      // 增加借出计数
            return c;
        }
    }
    return nullptr;
}

/**
 * 创建新游标，不使用缓存
 */
WT_CURSOR* WiredTigerSession::getNewCursor(const std::string& uri, const char* config) {
    WT_CURSOR* cursor = nullptr;
    _openCursor(_session, uri, config, &cursor);
    _cursorsOut++;
    return cursor;
}

/**
 * 释放游标回缓存
 * 实现 LRU 缓存淘汰策略
 */
void WiredTigerSession::releaseCursor(uint64_t id, WT_CURSOR* cursor, std::string config) {
    // When cleaning up a cursor, we would want to check if the session cache is already in shutdown
    // and prevent the race condition that the shutdown starts after the check.
    // 防止在关闭过程中的竞争条件
    WiredTigerSessionCache::BlockShutdown blockShutdown(_cache);

    // Avoids the cursor already being destroyed during the shutdown. Also, avoids releasing a
    // cursor from an earlier epoch.
    // 避免在关闭期间释放游标，或释放来自早期 epoch 的游标
    if (_cache->isShuttingDown() || _getEpoch() < _cache->_epoch.load()) {
        return;
    }

    invariant(_session);
    invariant(cursor);
    _cursorsOut--;

    // 重置游标状态
    invariantWTOK(cursor->reset(cursor), _session);

    // Cursors are pushed to the front of the list and removed from the back
    // 游标被推到列表前面，从后面移除（LRU）
    _cursors.push_front(WiredTigerCachedCursor(id, _cursorGen++, cursor, std::move(config)));

    // A negative value for wiredTigercursorCacheSize means to use hybrid caching.
    // 负值表示使用混合缓存模式
    std::uint32_t cacheSize = abs(gWiredTigerCursorCacheSize.load());

    // 淘汰最老的游标，保持缓存大小限制
    while (!_cursors.empty() && _cursorGen - _cursors.back()._gen > cacheSize) {
        cursor = _cursors.back()._cursor;
        _cursors.pop_back();
        invariantWTOK(cursor->close(cursor), _session);
    }
}

/**
 * 关闭游标而不放入缓存
 */
void WiredTigerSession::closeCursor(WT_CURSOR* cursor) {
    // 防止关闭期间的竞争条件
    WiredTigerSessionCache::BlockShutdown blockShutdown(_cache);

    if (_cache->isShuttingDown() || _getEpoch() < _cache->_epoch.load()) {
        return;
    }

    invariant(_session);
    invariant(cursor);
    _cursorsOut--;

    invariantWTOK(cursor->close(cursor), _session);
}

/**
 * 关闭所有匹配 URI 的缓存游标
 * 如果 URI 为空，关闭所有游标
 */
void WiredTigerSession::closeAllCursors(const std::string& uri) {
    invariant(_session);

    bool all = (uri == "");
    for (auto i = _cursors.begin(); i != _cursors.end();) {
        WT_CURSOR* cursor = i->_cursor;
        if (cursor && (all || uri == cursor->uri)) {
            invariantWTOK(cursor->close(cursor), _session);
            i = _cursors.erase(i);
        } else
            ++i;
    }
}

/**
 * 重新配置会话
 * 存储撤销配置以便后续恢复
 */
void WiredTigerSession::reconfigure(const std::string& newConfig, std::string undoConfig) {
    if (newConfig == undoConfig) {
        // The undoConfig string is the config string that resets our session back to default
        // settings. If our new configuration is the same as the undoconfig string, then that means
        // that we are either setting our configuration back to default, or that the newConfig
        // string does not change our default values. In this case, we can erase the undoConfig
        // string from our set of undo config strings, since we no longer need to do any work to
        // restore the session to its default configuration.
        // 如果新配置等于撤销配置，说明已经是默认配置，可以删除撤销记录
        _undoConfigStrings.erase(undoConfig);
    } else {
        // Store the config string that will reset our session to its default configuration.
        // 存储可以将会话重置为默认配置的配置字符串
        _undoConfigStrings.emplace(std::move(undoConfig));
    }
    auto wtSession = getSession();
    invariantWTOK(wtSession->reconfigure(wtSession, newConfig.c_str()), wtSession);
}

/**
 * 重置会话配置到默认状态
 * 在会话返回缓存前调用
 */
void WiredTigerSession::resetSessionConfiguration() {
    auto wtSession = getSession();
    for (const std::string& undoConfigString : _undoConfigStrings) {
        invariantWTOK(wtSession->reconfigure(wtSession, undoConfigString.c_str()), wtSession);
    }
    _undoConfigStrings.clear();
}

namespace {
// 全局表 ID 生成器，从 kLastTableId 开始
AtomicWord<unsigned long long> nextTableId(WiredTigerSession::kLastTableId);
}
// static
/**
 * 生成唯一的表 ID
 */
uint64_t WiredTigerSession::genTableId() {
    return nextTableId.fetchAndAdd(1);
}

// -----------------------

/**
 * WiredTigerSessionCache 构造函数（使用引擎）
 */
WiredTigerSessionCache::WiredTigerSessionCache(WiredTigerKVEngine* engine)
    : WiredTigerSessionCache(engine->getConnection(), engine->getClockSource(), engine) {}

/**
 * WiredTigerSessionCache 构造函数（完整版本）
 */
WiredTigerSessionCache::WiredTigerSessionCache(WT_CONNECTION* conn,
                                               ClockSource* cs,
                                               WiredTigerKVEngine* engine)
    : _conn(conn), _clockSource(cs), _engine(engine) {
    // 编译所有配置
    uassertStatusOK(_compiledConfigurations.compileAll(_conn));
}

/**
 * WiredTigerSessionCache 析构函数
 */
WiredTigerSessionCache::~WiredTigerSessionCache() {
    shuttingDown();
}

/**
 * 关闭会话缓存
 * 设置关闭标志并等待所有活动操作完成
 */
void WiredTigerSessionCache::shuttingDown() {
    // Try to atomically set _shuttingDown flag, but just return if another thread was first.
    // 原子设置关闭标志，如果已经设置则直接返回
    if (_shuttingDown.fetchAndBitOr(kShuttingDownMask) & kShuttingDownMask)
        return;

    // Spin as long as there are threads blocking shutdown.
    // 等待所有阻塞关闭的线程完成
    while (_shuttingDown.load() != kShuttingDownMask) {
        sleepmillis(1);
    }

    closeAll();
}

/**
 * 重启会话缓存
 */
void WiredTigerSessionCache::restart() {
    _shuttingDown.fetchAndBitAnd(~kShuttingDownMask);
}

/**
 * 检查是否正在关闭
 */
bool WiredTigerSessionCache::isShuttingDown() {
    return _shuttingDown.load() & kShuttingDownMask;
}

/**
 * 等待数据持久化
 * 支持多种同步模式：日志刷新、稳定时间戳检查点、完整检查点
 */
void WiredTigerSessionCache::waitUntilDurable(OperationContext* opCtx,
                                              Fsync syncType,
                                              UseJournalListener useListener) {
    // For inMemory storage engines, the data is "as durable as it's going to get".
    // That is, a restart is equivalent to a complete node failure.
    // 对于内存存储引擎，数据已经是"最持久"的状态
    if (isEphemeral()) {
        auto [journalListener, token] = _getJournalListenerWithToken(opCtx, useListener);
        if (token) {
            journalListener->onDurable(token.value());
        }
        return;
    }

    // 防止在关闭期间执行
    BlockShutdown blockShutdown(this);

    uassert(ErrorCodes::ShutdownInProgress,
            "Cannot wait for durability because a shutdown is in progress",
            !isShuttingDown());

    // Stable checkpoints are only meaningful in a replica set. Replication sets the "stable
    // timestamp". If the stable timestamp is unset, WiredTiger takes a full checkpoint, which is
    // incidentally what we want. A "true" stable checkpoint (a stable timestamp was set on the
    // WT_CONNECTION, i.e: replication is on) requires `forceCheckpoint` to be true and journaling
    // to be enabled.
    // 稳定检查点只在副本集中有意义
    if (syncType == Fsync::kCheckpointStableTimestamp && getGlobalReplSettings().isReplSet()) {
        invariant(!isEphemeral());
    }

    // When forcing a checkpoint with journaling enabled, don't synchronize with other
    // waiters, as a log flush is much cheaper than a full checkpoint.
    // 强制检查点时不需要与其他等待者同步，因为日志刷新比完整检查点便宜得多
    if ((syncType == Fsync::kCheckpointStableTimestamp || syncType == Fsync::kCheckpointAll) &&
        !isEphemeral()) {
        auto [journalListener, token] = _getJournalListenerWithToken(opCtx, useListener);

        getKVEngine()->forceCheckpoint(syncType == Fsync::kCheckpointStableTimestamp);

        if (token) {
            journalListener->onDurable(token.value());
        }

        LOGV2_DEBUG(22418, 4, "created checkpoint (forced)");
        return;
    }

    auto [journalListener, token] = _getJournalListenerWithToken(opCtx, useListener);

    uint32_t start = _lastSyncTime.load();
    // Do the remainder in a critical section that ensures only a single thread at a time
    // will attempt to synchronize.
    // 确保同一时间只有一个线程执行同步操作
    stdx::unique_lock<Latch> lk(_lastSyncMutex);
    uint32_t current = _lastSyncTime.loadRelaxed();  // synchronized with writes through mutex
    if (current != start) {
        // Someone else synced already since we read lastSyncTime, so we're done!
        // 其他线程已经完成了同步，直接返回

        // Unconditionally unlock mutex here to run operations that do not require synchronization.
        // The JournalListener is the only operation that meets this criteria currently.
        // 解锁后执行不需要同步的操作
        lk.unlock();
        if (token) {
            journalListener->onDurable(token.value());
        }

        return;
    }
    _lastSyncTime.store(current + 1);

    // Nobody has synched yet, so we have to sync ourselves.
    // 没有其他线程同步，我们需要执行同步

    // Initialize on first use.
    // 第一次使用时初始化
    if (!_waitUntilDurableSession) {
        invariantWTOK(
            _conn->open_session(_conn, nullptr, "isolation=snapshot", &_waitUntilDurableSession),
            nullptr);
    }

    // Flush the journal.
    // 刷新日志
    invariantWTOK(_waitUntilDurableSession->log_flush(_waitUntilDurableSession, "sync=on"),
                  _waitUntilDurableSession);
    LOGV2_DEBUG(22419, 4, "flushed journal");

    // The session is reset periodically so that WT doesn't consider it a rogue session and log
    // about it. The session doesn't actually pin any resources that need to be released.
    // 定期重置会话，避免 WT 认为这是一个异常会话
    if (_timeSinceLastDurabilitySessionReset.millis() > (5 * 60 * 1000 /* 5 minutes */)) {
        _waitUntilDurableSession->reset(_waitUntilDurableSession);
        _timeSinceLastDurabilitySessionReset.reset();
    }

    // Unconditionally unlock mutex here to run operations that do not require synchronization.
    // The JournalListener is the only operation that meets this criteria currently.
    lk.unlock();
    if (token) {
        journalListener->onDurable(token.value());
    }
}

/**
 * 等待准备的事务提交或中止
 * 用于处理 WT_PREPARE_CONFLICT 错误
 */
void WiredTigerSessionCache::waitUntilPreparedUnitOfWorkCommitsOrAborts(
    Interruptible& interruptible, std::uint64_t lastCount) {
    // It is possible for a prepared transaction to block on bonus eviction inside WiredTiger after
    // it commits or rolls-back, but this delays it from signalling us to wake up. In the very
    // worst case that the only evictable page is the one pinned by our cursor, AND there are no
    // other prepared transactions committing or aborting, we could reach a deadlock. Since the
    // caller is already expecting spurious wakeups, we impose a large timeout to periodically force
    // the caller to retry its operation.
    // 准备的事务可能在提交或回滚后被 WiredTiger 内部的驱逐操作阻塞
    // 设置超时以避免死锁
    const auto deadline = Date_t::now() + Seconds(1);
    stdx::unique_lock<Latch> lk(_prepareCommittedOrAbortedMutex);
    if (lastCount == _prepareCommitOrAbortCounter.loadRelaxed()) {
        interruptible.waitForConditionOrInterruptUntil(
            _prepareCommittedOrAbortedCond, lk, deadline, [&] {
                return _prepareCommitOrAbortCounter.loadRelaxed() > lastCount;
            });
    }
}

/**
 * 通知准备的事务已提交或中止
 */
void WiredTigerSessionCache::notifyPreparedUnitOfWorkHasCommittedOrAborted() {
    stdx::unique_lock<Latch> lk(_prepareCommittedOrAbortedMutex);
    _prepareCommitOrAbortCounter.fetchAndAdd(1);
    _prepareCommittedOrAbortedCond.notify_all();
}

/**
 * 关闭所有匹配 URI 的游标
 */
void WiredTigerSessionCache::closeAllCursors(const std::string& uri) {
    stdx::lock_guard<Latch> lock(_cacheLock);
    for (SessionCache::iterator i = _sessions.begin(); i != _sessions.end(); i++) {
        (*i)->closeAllCursors(uri);
    }
}

/**
 * 获取空闲会话数量
 */
size_t WiredTigerSessionCache::getIdleSessionsCount() {
    stdx::lock_guard<Latch> lock(_cacheLock);
    return _sessions.size();
}

/**
 * 关闭过期的空闲会话
 * 避免会话缓存无限增长
 */
void WiredTigerSessionCache::closeExpiredIdleSessions(int64_t idleTimeMillis) {
    // Do nothing if session close idle time is set to 0 or less
    // 如果空闲时间设置为0或更少，不执行任何操作
    if (idleTimeMillis <= 0) {
        return;
    }

    auto cutoffTime = _clockSource->now() - Milliseconds(idleTimeMillis);
    SessionCache sessionsToClose;

    {
        stdx::lock_guard<Latch> lock(_cacheLock);
        // Discard all sessions that became idle before the cutoff time
        // 丢弃所有在截止时间之前变为空闲的会话
        for (auto it = _sessions.begin(); it != _sessions.end();) {
            auto session = *it;
            invariant(session->getIdleExpireTime() != Date_t::min());
            if (session->getIdleExpireTime() < cutoffTime) {
                it = _sessions.erase(it);
                sessionsToClose.push_back(session);
            } else {
                ++it;
            }
        }
    }

    // Closing expired idle sessions is expensive, so do it outside of the cache mutex. This helps
    // to avoid periodic operation latency spikes as seen in SERVER-52879.
    // 关闭过期的空闲会话开销很大，所以在缓存互斥锁外执行
    for (auto session : sessionsToClose) {
        delete session;
    }
}

/**
 * 关闭所有会话
 * 增加 epoch 以使所有现有会话失效
 */
void WiredTigerSessionCache::closeAll() {
    // Increment the epoch as we are now closing all sessions with this epoch.
    // 增加 epoch，使所有当前 epoch 的会话失效
    SessionCache swap;

    {
        stdx::lock_guard<Latch> lock(_cacheLock);
        _epoch.fetchAndAdd(1);
        _sessions.swap(swap);
    }

    for (SessionCache::iterator i = swap.begin(); i != swap.end(); i++) {
        delete (*i);
    }
}

/**
 * 检查是否是临时（内存）存储引擎
 */
bool WiredTigerSessionCache::isEphemeral() {
    return _engine && _engine->isEphemeral();
}

/**
 * 获取会话
 * 优先从缓存获取，否则创建新会话
 */
UniqueWiredTigerSession WiredTigerSessionCache::getSession() {
    // We should never be able to get here after _shuttingDown is set, because no new
    // operations should be allowed to start.
    // 关闭后不应该有新操作
    invariant(!(_shuttingDown.load() & kShuttingDownMask));

    {
        stdx::lock_guard<Latch> lock(_cacheLock);
        if (!_sessions.empty()) {
            // Get the most recently used session so that if we discard sessions, we're
            // discarding older ones
            // 获取最近使用的会话，这样丢弃时会丢弃较旧的
            WiredTigerSession* cachedSession = _sessions.back();
            _sessions.pop_back();
            // Reset the idle time
            // 重置空闲时间
            cachedSession->setIdleExpireTime(Date_t::min());
            return UniqueWiredTigerSession(cachedSession);
        }
    }

    // Outside of the cache partition lock, but on release will be put back on the cache
    // 在缓存锁外创建新会话，释放时会放回缓存
    return UniqueWiredTigerSession(new WiredTigerSession(_conn, this, _epoch.load()));
}

/**
 * 释放会话回缓存
 * 检查会话状态并重置配置
 */
void WiredTigerSessionCache::releaseSession(WiredTigerSession* session) {
    invariant(session);

    // 防止在关闭期间释放会话
    BlockShutdown blockShutdown(this);

    uint64_t currentEpoch = _epoch.load();
    if (isShuttingDown() || session->_getEpoch() != currentEpoch) {
        invariant(session->_getEpoch() <= currentEpoch);
        // There is a race condition with clean shutdown, where the storage engine is ripped from
        // underneath OperationContexts, which are not "active" (i.e., do not have any locks), but
        // are just about to delete the recovery unit. See SERVER-16031 for more information. Since
        // shutting down the WT_CONNECTION will close all WT_SESSIONS, we shouldn't also try to
        // directly close this session.
        // 处理关闭时的竞争条件
        session->_session = nullptr;  // Prevents calling _session->close() in destructor.
        delete session;
        return;
    }

    invariant(session->cursorsOut() == 0);

    {
        WT_SESSION* ss = session->getSession();
        uint64_t range;
        // This checks that we are only caching idle sessions and not something which might hold
        // locks or otherwise prevent truncation.
        // 检查我们只缓存空闲会话，而不是持有锁的会话
        invariantWTOK(ss->transaction_pinned_range(ss, &range), ss);
        invariant(range == 0);

        // Release resources in the session we're about to cache.
        // If we are using hybrid caching, then close cursors now and let them
        // be cached at the WiredTiger level.
        // 释放会话中的资源
        // 如果使用混合缓存，现在关闭游标，让它们在 WiredTiger 级别缓存
        if (gWiredTigerCursorCacheSize.load() < 0) {
            session->closeAllCursors("");
        }

        session->resetSessionConfiguration();
        invariantWTOK(ss->reset(ss), ss);
    }

    // Set the time this session got idle at.
    // 设置会话变为空闲的时间
    session->setIdleExpireTime(_clockSource->now());
    {
        stdx::lock_guard<Latch> lock(_cacheLock);
        _sessions.push_back(session);
    }

    if (_engine) {
        _engine->sizeStorerPeriodicFlush();
    }
}

/**
 * 设置日志监听器
 * 只能设置一次
 */
void WiredTigerSessionCache::setJournalListener(JournalListener* jl) {
    stdx::unique_lock<Latch> lk(_journalListenerMutex);

    // A JournalListener can only be set once. Otherwise, accessing a copy of the _journalListener
    // pointer without a mutex would be unsafe.
    // 日志监听器只能设置一次，否则无锁访问指针副本将不安全
    invariant(!_journalListener);

    _journalListener = jl;
}

/**
 * 检查引擎是否缓存游标
 */
bool WiredTigerSessionCache::isEngineCachingCursors() {
    return gWiredTigerCursorCacheSize.load() <= 0;
}

/**
 * WiredTigerSessionDeleter 操作符
 * 自动释放会话回缓存
 */
void WiredTigerSessionCache::WiredTigerSessionDeleter::operator()(
    WiredTigerSession* session) const {
    session->_cache->releaseSession(session);
}

/**
 * 获取日志监听器和令牌
 * 用于持久化通知
 */
std::pair<JournalListener*, boost::optional<JournalListener::Token>>
WiredTigerSessionCache::_getJournalListenerWithToken(OperationContext* opCtx,
                                                     UseJournalListener useListener) {
    auto journalListener = [&]() -> JournalListener* {
        // The JournalListener may not be set immediately, so we must check under a mutex so
        // as not to access the variable while setting a JournalListener. A JournalListener
        // is only allowed to be set once, so using the pointer outside of a mutex is safe.
        // 日志监听器可能不会立即设置，所以必须在互斥锁下检查
        stdx::unique_lock<Latch> lk(_journalListenerMutex);
        return _journalListener;
    }();
    boost::optional<JournalListener::Token> token;
    if (journalListener && useListener == UseJournalListener::kUpdate) {
        // Update a persisted value with the latest write timestamp that is safe across
        // startup recovery in the repl layer. Then report that timestamp as durable to the
        // repl layer below after we have flushed in-memory data to disk.
        // Note: only does a write if primary, otherwise just fetches the timestamp.
        // 更新持久化的时间戳值，用于复制层的启动恢复
        // 注意：只有主节点才会写入，否则只获取时间戳
        token = journalListener->getToken(opCtx);
    }
    return std::make_pair(journalListener, token);
}

}  // namespace mongo
