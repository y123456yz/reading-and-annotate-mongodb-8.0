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
#include <memory>
#include <string>
#include <utility>

#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/s/migration_session_id.h"
#include "mongo/db/service_context.h"
#include "mongo/db/shard_id.h"
#include "mongo/db/transaction_resources.h"
#include "mongo/platform/mutex.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/request_types/move_range_request_gen.h"
#include "mongo/stdx/condition_variable.h"
#include "mongo/stdx/unordered_map.h"
#include "mongo/util/assert_util_core.h"
#include "mongo/util/concurrency/notification.h"

namespace mongo {

class OperationContext;

class ScopedDonateChunk;
class ScopedReceiveChunk;
class ScopedSplitMergeChunk;



/**
 * ActiveMigrationsRegistry 类的作用：
 * MongoDB 分片集群中活跃迁移操作的注册表和并发控制管理器，确保分片间数据迁移的安全性和一致性。
 * 
 * 核心功能：
 * 1. 并发控制：防止同一分片上的多个迁移操作产生冲突
 * 2. 状态管理：跟踪当前活跃的迁移、接收、拆分/合并操作
 * 3. 幂等处理：支持重复请求的自动识别和处理
 * 4. 资源协调：提供基于 RAII 的资源生命周期管理
 * 5. 阻塞机制：支持维护操作期间临时阻塞新的迁移
 * 
 * 并发控制策略：
 * - Move || Move (same chunk)：第二个迁移会加入第一个
 * - Move || Move (different chunks or collections)：第二个迁移返回 ConflictingOperationInProgress 错误
 * - Move || Split/Merge (same collection)：第二个操作会阻塞等待第一个完成
 * - Move/Split/Merge || Split/Merge (different collections)：可以并发进行
 * 
 * 该类实现了非公平锁管理器，是MongoDB分片迁移安全执行的基础设施。
 */

/**
 * This class is used to synchronise all the active routing info operations for chunks owned by this
 * shard. There is only one instance of it per ServiceContext.
 *
 * It implements a non-fair lock manager, which provides the following guarantees:
 *
 *   - Move || Move (same chunk): The second move will join the first
 *   - Move || Move (different chunks or collections): The second move will result in a
 *                                                     ConflictingOperationInProgress error
 *   - Move || Split/Merge (same collection): The second operation will block behind the first
 *   - Move/Split/Merge || Split/Merge (for different collections): Can proceed concurrently
 */
class ActiveMigrationsRegistry {
    ActiveMigrationsRegistry(const ActiveMigrationsRegistry&) = delete;
    ActiveMigrationsRegistry& operator=(const ActiveMigrationsRegistry&) = delete;

public:
    // 构造函数：初始化注册表的内部状态
    // 设置默认值，准备接受迁移操作的注册
    ActiveMigrationsRegistry();
    
    // 析构函数：清理注册表资源
    // 确保所有活跃操作都已完成或被正确取消
    ~ActiveMigrationsRegistry();

    // 单例访问接口：获取与 ServiceContext 关联的注册表实例
    // 每个 ServiceContext 只有一个 ActiveMigrationsRegistry 实例
    static ActiveMigrationsRegistry& get(ServiceContext* service);
    static ActiveMigrationsRegistry& get(OperationContext* opCtx);

    /**
     * These methods can be used to block migrations temporarily. The lock() method will block if
     * there is a migration operation in progress and will return once it is completed. Any
     * subsequent migration operations will return ConflictingOperationInProgress until the unlock()
     * method is called.
     */
    /**
     * 迁移阻塞管理接口：
     * 用于在维护操作期间临时阻塞所有迁移活动
     * 
     * lock() 方法：
     * - 如果有迁移正在进行，会阻塞直到完成
     * - 阻塞期间，任何新的迁移操作都会返回 ConflictingOperationInProgress 错误
     * 
     * unlock() 方法：
     * - 解除阻塞状态，允许新的迁移操作
     * 
     * 使用场景：
     * - 分片下线维护
     * - 集群配置变更
     * - 功能兼容性版本 (FCV) 升级
     */
    void lock(OperationContext* opCtx, StringData reason);
    void unlock(StringData reason);

    /**
     * If there are no migrations or split/merges running on this shard, registers an active
     * migration with the specified arguments. Returns a ScopedDonateChunk, which must be signaled
     * by the caller before it goes out of scope.
     *
     * If there is an active migration already running on this shard and it has the exact same
     * arguments, returns a ScopedDonateChunk. The ScopedDonateChunk can be used to join the
     * already running migration.
     *
     * Otherwise returns a ConflictingOperationInProgress error.
     */
    /**
     * 注册 chunk 发送迁移操作：
     * 
     * 成功情况：
     * - 没有冲突操作时，注册新的迁移并返回 ScopedDonateChunk
     * - ScopedDonateChunk 必须在超出作用域前调用信号通知
     * 
     * 重复请求处理：
     * - 如果已有相同参数的迁移在运行，返回可用于加入现有迁移的 ScopedDonateChunk
     * - 实现自然的幂等性，避免重复执行
     * 
     * 冲突情况：
     * - 存在不同的迁移或其他冲突操作时，返回 ConflictingOperationInProgress 错误
     */
    StatusWith<ScopedDonateChunk> registerDonateChunk(OperationContext* opCtx,
                                                      const ShardsvrMoveRange& args);

    /**
     * Registers an active receive chunk operation with the specified session id and returns a
     * ScopedReceiveChunk. The returned ScopedReceiveChunk object will unregister the migration when
     * it goes out of scope.
     *
     * In case registerReceiveChunk() is called while other operations (a second migration or a
     * registry lock()) are already holding resources of the ActiveMigrationsRegistry, the function
     * will either
     * - wait for such operations to complete and then perform the registration
     * - return a ConflictingOperationInProgress error
     * based on the value of the waitForCompletionOfConflictingOps parameter
     */
    /**
     * 注册 chunk 接收操作：
     * 
     * 参数说明：
     * @param waitForCompletionOfConflictingOps 
     *   - true：等待冲突操作完成后再注册
     *   - false：遇到冲突立即返回错误
     * 
     * 返回 ScopedReceiveChunk 对象：
     * - 自动管理接收操作的生命周期
     * - 超出作用域时自动注销注册
     */
    StatusWith<ScopedReceiveChunk> registerReceiveChunk(OperationContext* opCtx,
                                                        const NamespaceString& nss,
                                                        const ChunkRange& chunkRange,
                                                        const ShardId& fromShardId,
                                                        bool waitForCompletionOfConflictingOps);

    /**
     * If there are no migrations running on this shard, registers an active split or merge
     * operation for the specified namespace and returns a scoped object which will in turn disallow
     * other migrations or splits/merges for the same namespace (but not for other namespaces).
     */
    /**
     * 注册拆分或合并操作：
     * 
     * 成功条件：
     * - 当前分片没有迁移操作正在进行
     * 
     * 返回值：
     * - 返回 ScopedSplitMergeChunk 对象，阻止同一命名空间的其他迁移或拆分/合并
     * - 不同命名空间的操作可以并发进行
     * 
     * 命名空间级别隔离：
     * - 拆分/合并操作只影响特定命名空间
     * - 不会阻塞其他命名空间的操作
     */
    StatusWith<ScopedSplitMergeChunk> registerSplitOrMergeChunk(OperationContext* opCtx,
                                                                const NamespaceString& nss,
                                                                const ChunkRange& chunkRange);

    /**
     * If a migration has been previously registered through a call to registerDonateChunk, returns
     * that namespace. Otherwise returns boost::none.
     */
    /**
     * 查询活跃迁移状态：
     * 
     * @return 如果有活跃的 chunk 发送迁移，返回其命名空间；否则返回 boost::none
     * 
     * 用途：
     * - 状态监控和调试
     * - 冲突检测和避免
     */
    boost::optional<NamespaceString> getActiveDonateChunkNss();

    /**
     * Returns a report on the active migration if there currently is one. Otherwise, returns an
     * empty BSONObj.
     *
     * Takes an IS lock on the namespace of the active migration, if one is active.
     */
    /**
     * 获取活跃迁移状态报告：
     * 
     * 行为：
     * - 如果有活跃迁移，对其命名空间加 IS 锁并返回详细状态
     * - 如果没有活跃迁移，返回空的 BSONObj
     * 
     * 报告内容：
     * - 迁移参数和进度
     * - 执行时间和状态
     * - 错误信息（如果有）
     */
    BSONObj getActiveMigrationStatusReport(OperationContext* opCtx);

private:
    // 友元类：允许 Scoped* 类访问私有清理方法
    friend class ScopedDonateChunk;
    friend class ScopedReceiveChunk;
    friend class ScopedSplitMergeChunk;

    // Describes the state of a currently active moveChunk operation
    /**
     * ActiveMoveChunkState：活跃迁移状态结构
     * 
     * 包含内容：
     * - args：当前活跃操作的确切参数，用于重复请求检测
     * - notification：完成通知事件，支持多个等待者
     */
    struct ActiveMoveChunkState {
        ActiveMoveChunkState(ShardsvrMoveRange inArgs)
            : args(std::move(inArgs)), notification(std::make_shared<Notification<Status>>()) {}

        /**
         * Constructs an error status to return in the case of conflicting operations.
         */
        /**
         * 构造冲突操作的错误状态：
         * 提供详细的冲突信息，包括当前操作的参数和状态
         */
        Status constructErrorStatus() const;

        // Exact arguments of the currently active operation
        // 当前活跃操作的确切参数
        ShardsvrMoveRange args;

        // Notification event that will be signaled when the currently active operation completes
        // 当前活跃操作完成时的通知事件
        std::shared_ptr<Notification<Status>> notification;
    };

    // Describes the state of a currently active receive chunk operation
    /**
     * ActiveReceiveChunkState：活跃接收状态结构
     * 
     * 记录正在接收的 chunk 信息：
     * - 命名空间和范围
     * - 源分片标识
     */
    struct ActiveReceiveChunkState {
        ActiveReceiveChunkState(NamespaceString inNss, ChunkRange inRange, ShardId inFromShardId)
            : nss(std::move(inNss)), range(std::move(inRange)), fromShardId(inFromShardId) {}

        /**
         * Constructs an error status to return in the case of conflicting operations.
         */
        Status constructErrorStatus() const;

        // Namespace for which a chunk is being received
        // 正在接收 chunk 的命名空间
        NamespaceString nss;

        // Bounds of the chunk being migrated
        // 被迁移 chunk 的边界
        ChunkRange range;

        // Id of the shard from which the chunk is being received
        // 发送 chunk 的分片 ID
        ShardId fromShardId;
    };

    // Describes the state of a currently active split or merge operation
    /**
     * ActiveSplitMergeChunkState：活跃拆分/合并状态结构
     * 
     * 记录拆分/合并操作信息：
     * - 操作的命名空间
     * - 操作范围（拆分时为被拆分chunk，合并时为合并范围的结束边界）
     */
    struct ActiveSplitMergeChunkState {
        ActiveSplitMergeChunkState(NamespaceString inNss, ChunkRange inRange)
            : nss(std::move(inNss)), range(std::move(inRange)) {}

        // Namespace for which a chunk is being split or merged
        // 进行拆分或合并的命名空间
        NamespaceString nss;

        // If split, bounds of the chunk being split; if merge, the end bounds of the range being
        // merged
        // 拆分：被拆分chunk的边界；合并：合并范围的结束边界
        ChunkRange range;
    };

    /**
     * Unregisters a previously registered namespace with an ongoing migration. Must only be called
     * if a previous call to registerDonateChunk has succeeded.
     */
    /**
     * 注销先前注册的发送迁移：
     * 只能在 registerDonateChunk 成功调用后使用
     */
    void _clearDonateChunk();

    /**
     * Unregisters a previously registered incoming migration. Must only be called if a previous
     * call to registerReceiveChunk has succeeded.
     */
    /**
     * 注销先前注册的接收迁移：
     * 只能在 registerReceiveChunk 成功调用后使用
     */
    void _clearReceiveChunk();

    /**
     * Unregisters a previously registered split/merge chunk operation. Must only be called if a
     * previous call to registerSplitOrMergeChunk has succeeded.
     */
    /**
     * 注销先前注册的拆分/合并操作：
     * 只能在 registerSplitOrMergeChunk 成功调用后使用
     */
    void _clearSplitMergeChunk(const NamespaceString& nss);

    // Protects the state below
    // 保护以下状态的互斥锁
    Mutex _mutex = MONGO_MAKE_LATCH("ActiveMigrationsRegistry::_mutex");

    // Condition variable which will be signaled whenever any of the states below become false,
    // boost::none or a specific namespace removed from the map.
    // 条件变量：当以下任何状态变为 false、boost::none 或从映射中移除特定命名空间时发出信号
    stdx::condition_variable _chunkOperationsStateChangedCV;

    // Overarching block, which doesn't allow migrations to occur even when there isn't an active
    // migration ongoing. Used during recovery and FCV changes.
    // 总体阻塞标志：即使没有活跃迁移也不允许迁移发生
    // 使用场景：恢复期间和 FCV 变更期间
    bool _migrationsBlocked{false};

    // If there is an active moveChunk operation, this field contains the original request
    // 如果有活跃的 moveChunk 操作，此字段包含原始请求
    boost::optional<ActiveMoveChunkState> _activeMoveChunkState;

    // If there is an active chunk receive operation, this field contains the original session id
    // 如果有活跃的 chunk 接收操作，此字段包含原始会话 ID
    boost::optional<ActiveReceiveChunkState> _activeReceiveChunkState;

    // If there is an active split or merge chunk operation for a particular namespace, this map
    // will contain an entry
    // 如果特定命名空间有活跃的拆分或合并操作，此映射将包含条目
    stdx::unordered_map<NamespaceString, ActiveSplitMergeChunkState> _activeSplitMergeChunkStates;
};


/**
 * MigrationBlockingGuard：迁移阻塞守护类
 * 
 * 核心功能：
 * 1. RAII 模式的迁移阻塞管理：构造时自动锁定迁移，析构时自动解锁
 * 2. 主从切换安全保证：确保在复制集状态变更时能够正确中断和清理
 * 3. 维护操作支持：为分片维护、FCV升级等操作提供迁移阻塞机制
 * 4. 异常安全：即使发生异常也能确保迁移锁正确释放
 * 
 * 使用场景：
 * - 分片下线或上线操作
 * - 功能兼容性版本 (FCV) 变更
 * - 集群拓扑结构调整
 * - 其他需要暂停迁移的维护操作
 * 
 * 安全约束：
 * - 必须在持有全局写锁或设置了中断标志的上下文中使用
 * - 确保在主从切换时能够被正确中断，避免状态不一致
 * 
 * 该类通过RAII模式和严格的安全检查，为MongoDB分片集群的维护操作提供了可靠的迁移控制机制。
 */
class MigrationBlockingGuard {
public:
    // 构造函数：立即锁定迁移注册表，阻塞所有新的迁移操作
    // 参数：
    // - opCtx：操作上下文，用于获取注册表实例和进行安全检查
    // - reason：阻塞原因的描述字符串，用于日志记录和调试
    MigrationBlockingGuard(OperationContext* opCtx, std::string reason)
        : _registry(ActiveMigrationsRegistry::get(opCtx)), _reason(std::move(reason)) {
        // Ensure any thread attempting to use a MigrationBlockingGuard will be interrupted by
        // a stepdown.
        // 确保任何尝试使用 MigrationBlockingGuard 的线程在主从切换时会被中断
        // 这是关键的安全保证：防止在复制集状态变更时出现不一致的状态
        invariant(
            shard_role_details::getLocker(opCtx)->wasGlobalLockTakenInModeConflictingWithWrites() ||
            opCtx->shouldAlwaysInterruptAtStepDownOrUp());
        
        // 调用注册表的 lock 方法，阻塞所有新的迁移操作
        // 如果当前有活跃迁移，会等待其完成后再加锁
        _registry.lock(opCtx, _reason);
    }

    // 析构函数：自动解锁迁移注册表，恢复迁移操作的正常进行
    // RAII 模式确保即使发生异常也能正确清理资源
    ~MigrationBlockingGuard() {
        // 调用注册表的 unlock 方法，移除迁移阻塞状态
        // 允许新的迁移操作继续进行
        _registry.unlock(_reason);
    }

private:
    // 活跃迁移注册表的引用，用于执行锁定和解锁操作
    // 不拥有所有权，注册表由 ServiceContext 管理生命周期
    ActiveMigrationsRegistry& _registry;
    
    // 阻塞原因的描述字符串，用于：
    // - 日志记录：帮助识别哪个操作导致了迁移阻塞
    // - 调试分析：在问题排查时提供上下文信息
    // - 状态报告：在监控系统中显示阻塞原因
    std::string _reason;
};

/**
 * ScopedDonateChunk：分片发送迁移的作用域管理类
 * 
 * 核心功能：
 * 1. 双模式设计：支持 'execute'（执行）和 'join'（加入）两种工作模式
 * 2. 迁移生命周期管理：通过 RAII 模式自动管理迁移的注册和注销
 * 3. 异步完成通知：提供基于 Future/Promise 的异步完成机制
 * 4. 重复请求处理：支持多个客户端加入同一个正在进行的迁移
 * 5. 资源自动清理：析构时自动注销迁移状态，释放注册表资源
 * 
 * 双模式工作机制：
 * - Execute 模式：持有者是迁移的主执行者，必须调用 signalComplete() 通知完成
 * - Join 模式：持有者加入已存在的迁移，必须调用 waitForCompletion() 等待结果
 * 
 * 使用场景：
 * - 新迁移：registerDonateChunk 返回 execute 模式的对象
 * - 重复请求：相同参数的迁移请求返回 join 模式的对象
 * - 幂等处理：自动识别和处理重复的迁移请求
 * 
 * 该类是 MongoDB 分片迁移并发控制和幂等处理的核心组件。
 */
/**
 * Object of this class is returned from the registerDonateChunk call of the active migrations
 * registry. It can exist in two modes - 'execute' and 'join'. See the comments for
 * registerDonateChunk method for more details.
 */
class ScopedDonateChunk {
    ScopedDonateChunk(const ScopedDonateChunk&) = delete;
    ScopedDonateChunk& operator=(const ScopedDonateChunk&) = delete;

public:
    // 构造函数：初始化作用域迁移对象
    // 参数：
    // - registry：活跃迁移注册表指针，用于析构时的清理操作（不拥有所有权）
    // - shouldExecute：执行模式标志，true表示execute模式，false表示join模式
    // - completionNotification：共享的完成通知对象，支持多个等待者
    ScopedDonateChunk(ActiveMigrationsRegistry* registry,
                      bool shouldExecute,
                      std::shared_ptr<Notification<Status>> completionNotification);
    
    // 析构函数：自动清理迁移状态
    // 对于 execute 模式：从注册表中注销迁移，设置完成通知
    // 对于 join 模式：不执行注销操作，仅清理本地状态
    ~ScopedDonateChunk();

    // 移动构造函数：支持对象的高效传递
    // 转移所有资源的所有权，原对象置为无效状态
    ScopedDonateChunk(ScopedDonateChunk&&);
    
    // 移动赋值操作符：支持移动语义的赋值操作
    // 正确处理资源转移，避免重复清理或资源泄露
    ScopedDonateChunk& operator=(ScopedDonateChunk&&);

    /**
     * Returns true if the migration object is in the 'execute' mode. This means that the migration
     * object holder is next in line to execute the moveChunk command. The holder must execute the
     * command and call signalComplete with a status.
     */
    /**
     * 执行模式检查：
     * @return true 表示对象处于 'execute' 模式，持有者必须执行 moveChunk 命令
     * 
     * Execute 模式职责：
     * - 执行完整的 chunk 迁移流程
     * - 处理迁移过程中的所有异常和错误
     * - 调用 signalComplete() 通知所有等待者
     * 
     * Join 模式职责：
     * - 调用 waitForCompletion() 等待主执行者完成
     * - 不执行实际的迁移操作
     */
    bool mustExecute() const {
        return _shouldExecute;
    }

    /**
     * Must only be called if the object is in the 'execute' mode when the moveChunk command was
     * invoked (the command immediately executed). Signals any callers that might be blocked in
     * waitForCompletion.
     */
    /**
     * 完成信号通知：
     * 只能在 'execute' 模式下调用，通知等待中的调用者迁移已完成
     * 
     * 功能：
     * - 设置迁移的最终执行结果（成功或失败）
     * - 唤醒所有在 waitForCompletion() 中等待的线程
     * - 存储完成状态，供析构函数使用
     * 
     * 调用时机：
     * - 迁移成功完成后调用 signalComplete(Status::OK())
     * - 迁移失败时调用 signalComplete(errorStatus)
     * - 必须在对象析构前调用，否则等待者会收到默认错误
     */
    void signalComplete(Status status);

    /**
     * Must only be called if the object is in the 'join' mode. Blocks until the main executor of
     * the moveChunk command calls signalComplete.
     */
    /**
     * 等待完成：
     * 只能在 'join' 模式下调用，阻塞直到主执行者调用 signalComplete
     * 
     * 功能：
     * - 阻塞当前线程，等待迁移完成
     * - 支持操作上下文的中断机制
     * - 返回迁移的最终执行结果
     * 
     * 异常处理：
     * - 支持因主从切换等原因的中断
     * - 超时和取消操作的正确处理
     * - 网络错误和系统故障的传播
     */
    Status waitForCompletion(OperationContext* opCtx);

private:
    // Registry from which to unregister the migration. Not owned.
    // 用于注销迁移的注册表指针（不拥有所有权）
    // 析构时调用注册表的清理方法移除迁移状态
    ActiveMigrationsRegistry* _registry;

    /**
     * Whether the holder is the first in line for a newly started migration (in which case the
     * destructor must unregister) or the caller is joining on an already-running migration
     * (in which case the caller must block and wait for completion).
     */
    /**
     * 执行模式标志：
     * true：持有者是新迁移的第一个执行者（析构函数必须注销）
     * false：调用者加入已运行的迁移（调用者必须阻塞等待完成）
     * 
     * 模式决定了对象的行为：
     * - Execute 模式：负责实际的迁移执行和状态管理
     * - Join 模式：仅等待结果，不参与实际执行
     */
    bool _shouldExecute;

    // This is the future, which will be set at the end of a migration.
    // 迁移结束时设置的 Future 对象
    // 多个 ScopedDonateChunk 对象可能共享同一个通知，实现一对多的完成通知
    std::shared_ptr<Notification<Status>> _completionNotification;

    // This is the outcome of the migration execution, stored when signalComplete() is called and
    // set on the future of the executing ScopedDonateChunk object when this gets destroyed.
    // 迁移执行结果：调用 signalComplete() 时存储，对象销毁时设置到执行者的 Future 上
    // 确保即使主执行者异常退出，等待者也能收到合理的错误状态
    boost::optional<Status> _completionOutcome;
};

/**
 * ScopedReceiveChunk：分片接收迁移的作用域管理类
 * 
 * 核心功能：
 * 1. 接收状态管理：自动管理 chunk 接收操作在注册表中的注册和注销
 * 2. RAII 资源管理：构造时注册接收状态，析构时自动清理，确保资源正确释放
 * 3. 并发控制：防止同一分片同时进行多个接收操作，避免资源冲突
 * 4. 移动语义支持：支持高效的对象传递，避免不必要的复制开销
 * 5. 异常安全保证：即使发生异常也能确保接收状态被正确清理
 * 
 * 工作原理：
 * - 构造时：在 ActiveMigrationsRegistry 中注册当前分片正在接收 chunk
 * - 运行时：阻止其他冲突的迁移操作（发送迁移、其他接收等）
 * - 析构时：从注册表中移除接收状态，允许后续操作继续
 * 
 * 使用场景：
 * - chunk 迁移的目标分片端，用于标记正在接收数据
 * - 防止分片同时作为多个迁移的接收方
 * - 确保接收操作的排他性和一致性
 * 
 * 该类通过简洁的 RAII 接口为 MongoDB 分片接收操作提供了可靠的状态管理机制。
 */
/**
 * Object of this class is returned from the registerReceiveChunk call of the active migrations
 * registry.
 */
class ScopedReceiveChunk {
    ScopedReceiveChunk(const ScopedReceiveChunk&) = delete;
    ScopedReceiveChunk& operator=(const ScopedReceiveChunk&) = delete;

public:
    // 构造函数：注册接收操作到活跃迁移注册表
    // 参数：
    // - registry：用于管理接收状态的注册表指针（不拥有所有权）
    // 
    // 功能：
    // - 在注册表中标记当前分片正在接收 chunk
    // - 阻止冲突的迁移操作（如同时发送其他 chunk）
    // - 为接收操作提供排他性保证
    ScopedReceiveChunk(ActiveMigrationsRegistry* registry);
    
    // 析构函数：自动注销接收操作
    // 功能：
    // - 从注册表中移除接收状态标记
    // - 允许后续的迁移或接收操作继续进行
    // - 通知等待的操作可以开始执行
    // - 确保即使发生异常也能正确清理资源
    ~ScopedReceiveChunk();

    // 移动构造函数：支持对象的高效传递
    // 功能：
    // - 转移接收操作的所有权和注册状态
    // - 原对象失效，不再负责清理操作
    // - 避免不必要的对象复制开销
    // - 保持注册表状态的一致性
    ScopedReceiveChunk(ScopedReceiveChunk&&);
    
    // 移动赋值操作符：支持移动语义的赋值操作
    // 功能：
    // - 正确处理资源转移，避免重复注销或资源泄露
    // - 如果当前对象已持有资源，先进行清理
    // - 转移新对象的资源所有权
    // - 确保异常安全的资源管理
    ScopedReceiveChunk& operator=(ScopedReceiveChunk&&);

private:
    // Registry from which to unregister the migration. Not owned.
    // 用于注销迁移的注册表指针（不拥有所有权）
    // 析构时调用注册表的 _clearReceiveChunk() 方法移除接收状态
    // 注册表本身由 ServiceContext 管理生命周期，无需本对象负责释放
    ActiveMigrationsRegistry* _registry;
};

/**
 * ScopedSplitMergeChunk：拆分/合并操作的作用域管理类
 * 
 * 核心功能：
 * 1. 命名空间级别的操作管理：管理特定命名空间上的拆分或合并操作
 * 2. RAII 资源管理：构造时注册操作，析构时自动清理状态
 * 3. 细粒度并发控制：只阻止同一命名空间的冲突操作，不影响其他命名空间
 * 4. 操作类型支持：同时支持 chunk 拆分和合并两种操作类型
 * 5. 移动语义优化：支持高效的对象传递和资源管理
 * 
 * 并发控制策略：
 * - 同一命名空间：阻止并发的拆分/合并和迁移操作
 * - 不同命名空间：允许并发执行，互不影响
 * - 操作隔离：确保拆分/合并操作的原子性和一致性
 * 
 * 使用场景：
 * - chunk 拆分操作：防止拆分期间的并发迁移
 * - chunk 合并操作：确保合并过程的数据一致性
 * - 负载均衡优化：配合迁移操作进行 chunk 大小调整
 * 
 * 该类通过命名空间级别的细粒度控制，为 MongoDB 拆分/合并操作提供了高效的并发管理。
 */
/**
 * Object of this class is returned from the registerSplitOrMergeChunk call of the active migrations
 * registry.
 */
class ScopedSplitMergeChunk {
public:
    // 构造函数：注册拆分/合并操作到活跃迁移注册表
    // 参数：
    // - registry：用于管理操作状态的注册表指针（不拥有所有权）
    // - nss：进行拆分/合并操作的命名空间
    // 
    // 功能：
    // - 在注册表的命名空间映射中添加操作记录
    // - 阻止同一命名空间的并发迁移或其他拆分/合并操作
    // - 允许不同命名空间的操作并发进行
    // - 为拆分/合并操作提供排他性保证
    ScopedSplitMergeChunk(ActiveMigrationsRegistry* registry, const NamespaceString& nss);
    
    // 析构函数：自动注销拆分/合并操作
    // 功能：
    // - 从注册表的命名空间映射中移除操作记录
    // - 允许该命名空间的后续操作继续进行
    // - 通知等待该命名空间的其他操作可以开始
    // - 确保异常安全的资源清理
    ~ScopedSplitMergeChunk();

    // 移动构造函数：支持对象的高效传递
    // 功能：
    // - 转移操作的所有权和注册状态
    // - 保持命名空间映射的正确性
    // - 原对象失效，避免重复清理
    // - 优化对象传递的性能
    ScopedSplitMergeChunk(ScopedSplitMergeChunk&&);
    
    // 移动赋值操作符：支持移动语义的赋值
    // 功能：
    // - 正确处理命名空间资源的转移
    // - 避免注册表状态的不一致
    // - 确保异常安全的资源管理
    // - 支持链式赋值和复杂的对象操作
    ScopedSplitMergeChunk& operator=(ScopedSplitMergeChunk&&);

private:
    // Registry from which to unregister the split/merge. Not owned.
    // 用于注销拆分/合并操作的注册表指针（不拥有所有权）
    // 析构时使用此指针调用 _clearSplitMergeChunk(nss) 清理特定命名空间的状态
    ActiveMigrationsRegistry* _registry;

    // 进行拆分/合并操作的命名空间
    // 用于在注册表中标识和管理特定命名空间的操作状态
    // 支持命名空间级别的细粒度并发控制
    NamespaceString _nss;
};

}  // namespace mongo
