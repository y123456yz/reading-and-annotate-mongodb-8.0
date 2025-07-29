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
#include <boost/optional.hpp>
#include <boost/optional/optional.hpp>
#include <memory>
#include <utility>

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/oid.h"
#include "mongo/client/connection_string.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/s/collection_metadata.h"
#include "mongo/db/s/collection_sharding_runtime.h"
#include "mongo/db/s/migration_chunk_cloner_source.h"
#include "mongo/db/s/migration_coordinator.h"
#include "mongo/db/s/move_timing_helper.h"
#include "mongo/db/write_concern_options.h"
#include "mongo/s/chunk_version.h"
#include "mongo/s/request_types/move_range_request_gen.h"
#include "mongo/util/future.h"
#include "mongo/util/future_impl.h"
#include "mongo/util/net/hostandport.h"
#include "mongo/util/timer.h"
#include "mongo/util/uuid.h"

namespace mongo {

struct ShardingStatistics;

/**
 * MigrationSourceManager 类的作用：
 * MongoDB 分片集群中 chunk 迁移的源端状态机管理器，负责协调和控制整个迁移过程。
 * 
 * 核心功能：
 * 1. 迁移状态机管理：控制迁移从创建到完成的完整生命周期
 * 2. 数据克隆协调：管理 chunk 数据从源分片到目标分片的传输过程
 * 3. 临界区控制：在迁移关键阶段阻止写操作，确保数据一致性
 * 4. 元数据同步：协调 Config Server 上的元数据更新
 * 5. 异常处理和清理：在各种失败场景下确保资源正确清理
 * 
 * 状态转换流程：
 * kCreated → kCloning → kCloneCaughtUp → kCriticalSection → kCloneCompleted → kCommittingOnConfig → kDone
 * 
 * 关键阶段：
 * - 创建阶段：初始化迁移参数，验证前置条件
 * - 克隆阶段：启动后台数据复制，响应接收方的数据请求
 * - 追赶阶段：等待增量变更同步完成
 * - 临界区阶段：阻止写操作，进行最终数据同步
 * - 提交阶段：更新 Config Server 元数据，完成迁移
 * 
 * 线程安全：
 * - 单线程设计：对象必须由单一线程创建和控制
 * - 内部同步：使用互斥锁保护并发访问的内部状态
 * - 不可传递：不应在线程间传递对象实例
 * 
 * 该类是 MongoDB 分片迁移的核心组件，确保了数据迁移的安全性、一致性和可靠性。
 */
/**
 * The donor-side migration state machine. This object must be created and owned by a single thread,
 * which controls its lifetime and should not be passed across threads. Unless explicitly indicated
 * its methods must not be called from more than one thread and must not be called while any locks
 * are held.
 *
 * The intended workflow is as follows:
 *  - Instantiate a MigrationSourceManager on the stack.
 *      This will perform preliminary checks and snapshot the latest collection
 *  - Call startClone to initiate background cloning of the chunk contents. This will perform the
 *      necessary registration of the cloner with the replication subsystem and will start listening
 *      for document changes, while at the same time responding to data fetch requests from the
 *      recipient.
 *  - Call awaitUntilCriticalSectionIsAppropriate to wait for the cloning process to catch up
 *      sufficiently so we don't keep the server in read-only state for too long.
 *  - Call enterCriticalSection to cause the shard to enter in 'read only' mode while the latest
 *      changes are drained by the recipient shard.
 *  - Call commitDonateChunk to commit the chunk move in the config server's metadata and leave the
 *      read only (critical section) mode.
 *
 * At any point in time it is safe to let the MigrationSourceManager object go out of scope in which
 * case the desctructor will take care of clean up based on how far we have advanced. One exception
 * is the commitDonateChunk and its comments explain the reasoning.
 * ShardsvrMoveRangeCommand::_runImpl中构造 MigrationSourceManager
 */
class MigrationSourceManager {
    MigrationSourceManager(const MigrationSourceManager&) = delete;
    MigrationSourceManager& operator=(const MigrationSourceManager&) = delete;

public:
    /**
     * Retrieves the MigrationSourceManager pointer that corresponds to the given collection under
     * a CollectionShardingRuntime that has its ResourceMutex locked.
     */
    /**
     * 静态访问方法：获取指定集合对应的迁移源管理器
     * 
     * 功能：
     * - 从 CollectionShardingRuntime 中获取当前活跃的迁移管理器
     * - 要求调用时已持有 ResourceMutex 锁，确保线程安全
     * - 用于在其他组件中访问正在进行的迁移状态
     * 
     * 使用场景：
     * - OpObserver 记录写操作变更
     * - 统计信息收集和状态报告
     * - 并发操作的冲突检测
     */
    static MigrationSourceManager* get(const CollectionShardingRuntime& csr);

    /**
     * If the currently installed migration has reached the cloning stage (i.e., after startClone),
     * returns the cloner currently in use.
     */
    /**
     * 获取当前克隆器：返回正在使用的数据克隆组件
     * 
     * 功能：
     * - 检查迁移是否已达到克隆阶段
     * - 返回活跃的 MigrationChunkClonerSource 实例
     * - 用于访问克隆过程的详细信息和控制
     * 
     * 前置条件：
     * - 迁移必须已调用 startClone() 方法
     * - 集合分片运行时必须处于锁定状态
     */
    static std::shared_ptr<MigrationChunkClonerSource> getCurrentCloner(
        const CollectionShardingRuntime& csr);

    /**
     * Instantiates a new migration source manager with the specified migration parameters.
     *
     * Loads the most up-to-date collection metadata and uses it as a starting point.
     *
     * May throw any exception. Known exceptions are:
     *  - InvalidOptions if the operation context is missing shard version
     *  - StaleConfig if the expected placement version does not match the one known by this shard.
     */
    /**
     * 构造函数：创建新的迁移源管理器实例
     * 
     * 参数：
     * - opCtx：操作上下文，必须在对象生命周期内保持有效
     * - request：迁移请求参数，包含源/目标分片、chunk范围等
     * - writeConcern：写关注点，控制迁移操作的持久性要求
     * - donorConnStr：源分片的连接字符串
     * - recipientHost：目标分片的主节点地址
     * 
     * 功能：
     * - 加载最新的集合元数据作为迁移起点
     * - 验证分片版本和迁移参数的有效性
     * - 初始化迁移状态和相关组件
     * - 注册到集合分片运行时中
     * 
     * 异常处理：
     * - InvalidOptions：操作上下文缺少分片版本信息
     * - StaleConfig：期望的放置版本与当前分片已知版本不匹配
     * - 其他验证失败的相关异常
     */
    MigrationSourceManager(OperationContext* opCtx,
                           ShardsvrMoveRange&& request,
                           WriteConcernOptions&& writeConcern,
                           ConnectionString donorConnStr,
                           HostAndPort recipientHost);
    
    /**
     * 析构函数：自动清理迁移资源
     * 
     * 功能：
     * - 根据当前迁移状态执行相应的清理操作
     * - 从集合分片运行时中注销管理器
     * - 释放临界区（如果已进入）
     * - 清理克隆器和协调器资源
     * - 记录迁移结束日志
     * 
     * 异常安全：
     * - 保证在任何状态下都能正确清理
     * - 不会抛出异常，确保析构过程的安全性
     */
    ~MigrationSourceManager();

    /**
     * Contacts the donor shard and tells it to start cloning the specified chunk. This method will
     * fail if for any reason the donor shard fails to initiate the cloning sequence.
     *
     * Expected state: kCreated
     * Resulting state: kCloning on success, kDone on failure
     */
    /**
     * 启动克隆阶段：开始 chunk 数据的后台复制过程
     * 
     * 功能：
     * - 创建和启动 MigrationChunkClonerSource
     * - 向接收方发送克隆开始通知
     * - 注册 OpObserver 监听器跟踪写操作变更
     * - 初始化迁移协调器管理整个过程
     * - 更新迁移统计信息
     * 
     * 状态转换：
     * - 期望状态：kCreated
     * - 成功状态：kCloning
     * - 失败状态：kDone
     * 
     * 核心操作：
     * - 获取集合元数据和分片键信息
     * - 设置适当的读关注点确保数据一致性
     * - 启动后台克隆线程
     * - 建立与接收方的通信连接
     */
    void startClone();

    /**
     * Waits for the cloning to catch up sufficiently so we won't have to stay in the critical
     * section for a long period of time. This method will fail if any error occurs while the
     * recipient is catching up.
     *
     * Expected state: kCloning
     * Resulting state: kCloneCaughtUp on success, kDone on failure
     */
    /**
     * 等待追赶完成：确保克隆过程充分追赶，减少临界区时间
     * 
     * 功能：
     * - 监控克隆进度，等待初始数据传输完成
     * - 确保增量变更队列在可控范围内
     * - 计算进入临界区的最佳时机
     * - 验证接收方的数据接收状态
     * 
     * 状态转换：
     * - 期望状态：kCloning
     * - 成功状态：kCloneCaughtUp
     * - 失败状态：kDone
     * 
     * 性能考虑：
     * - 平衡克隆完整性和临界区持续时间
     * - 监控网络传输性能和延迟
     * - 动态调整等待策略
     */
    void awaitToCatchUp();

    /**
     * Waits for the active clone operation to catch up and enters critical section. Once this call
     * returns successfully, no writes will be happening on this shard until the chunk donation is
     * committed. Therefore, commitChunkOnRecipient/commitChunkMetadata must be called as soon as
     * possible afterwards.
     *
     * Expected state: kCloneCaughtUp
     * Resulting state: kCriticalSection on success, kDone on failure
     */
    /**
     * 进入临界区：阻止写操作，进行最终数据同步
     * 
     * 功能：
     * - 创建集合临界区，阻止所有写操作
     * - 等待最后的增量变更传输完成
     * - 确保源端和接收端数据完全一致
     * - 为元数据更新做准备
     * 
     * 状态转换：
     * - 期望状态：kCloneCaughtUp
     * - 成功状态：kCriticalSection
     * - 失败状态：kDone
     * 
     * 关键约束：
     * - 一旦成功返回，必须尽快调用后续提交方法
     * - 临界区期间会影响应用程序的写入性能
     * - 需要确保接收方准备好接收最终数据
     * 
     * 监控指标：
     * - 临界区持续时间
     * - 阻塞的写操作数量
     * - 最终同步的数据量
     */
    void enterCriticalSection();

    /**
     * Tells the recipient of the chunk to commit the chunk contents, which it received.
     *
     * Expected state: kCriticalSection
     * Resulting state: kCloneCompleted on success, kDone on failure
     */
    /**
     * 接收方提交：通知接收方提交已接收的 chunk 数据
     * 
     * 功能：
     * - 向接收方发送提交信号
     * - 验证接收方数据完整性
     * - 确认接收方准备好接管 chunk 服务
     * - 收集克隆统计信息
     * 
     * 状态转换：
     * - 期望状态：kCriticalSection
     * - 成功状态：kCloneCompleted
     * - 失败状态：kDone
     * 
     * 通信协议：
     * - 发送 _recvChunkCommit 命令到接收方
     * - 等待接收方确认提交成功
     * - 处理网络超时和重试逻辑
     * 
     * 数据验证：
     * - 文档数量一致性检查
     * - 索引完整性验证
     * - 分片键范围确认
     */
    void commitChunkOnRecipient();

    /**
     * Tells the recipient shard to fetch the latest portion of data from the donor and to commit it
     * locally. After that it persists the changed metadata to the config servers and leaves the
     * critical section.
     *
     * NOTE: Since we cannot recover from failures to write chunk metadata to the config servers, if
     *       applying the committed chunk information fails and we cannot definitely verify that the
     *       write was definitely applied or not, this call may crash the server.
     *
     * Expected state: kCloneCompleted
     * Resulting state: kDone
     */
    /**
     * Config Server 元数据提交：更新分片集群元数据，完成迁移
     * 
     * 功能：
     * - 通知接收方获取最新数据片段并本地提交
     * - 向 Config Server 写入新的 chunk 分布元数据
     * - 退出临界区，恢复正常写操作
     * - 触发源端范围删除任务
     * 
     * 状态转换：
     * - 期望状态：kCloneCompleted
     * - 结果状态：kDone
     * 
     * 关键风险：
     * - Config Server 写入失败无法恢复
     * - 元数据不一致可能导致路由错误
     * - 严重错误时可能触发服务器崩溃
     * 
     * 事务性保证：
     * - 使用分布式事务确保元数据原子更新
     * - 实现重试和幂等性机制
     * - 记录详细的审计日志
     * 
     * 后续清理：
     * - 安排源端数据删除任务
     * - 更新本地路由缓存
     * - 通知负载均衡器更新状态
     */
    void commitChunkMetadataOnConfig();

    /**
     * Aborts the migration after observing a concurrent index operation by marking its operation
     * context as killed.
     */
    /**
     * 中止迁移：因并发索引操作而终止迁移过程
     * 
     * 功能：
     * - 标记操作上下文为已终止状态
     * - 停止所有正在进行的克隆操作
     * - 清理已分配的资源和状态
     * - 返回异步完成的 Future 对象
     * 
     * 触发条件：
     * - 检测到并发的索引创建/删除操作
     * - 索引操作可能影响数据完整性
     * - 需要避免迁移和索引操作的竞争条件
     * 
     * 清理操作：
     * - 停止克隆器和数据传输
     * - 释放临界区（如果已进入）
     * - 回滚部分完成的状态变更
     * - 记录中止原因到变更日志
     * 
     * 返回值：
     * - SharedSemiFuture<void>：异步完成信号
     * - 可用于等待清理操作完全结束
     */
    SharedSemiFuture<void> abort();

    /**
     * Returns a report on the active migration.
     *
     * Must be called with some form of lock on the collection namespace.
     */
    /**
     * 获取迁移状态报告：返回当前迁移的详细状态信息
     * 
     * 功能：
     * - 收集迁移各阶段的进度信息
     * - 统计数据传输量和性能指标
     * - 报告当前状态和剩余工作量
     * - 提供诊断和监控数据
     * 
     * 前置条件：
     * - 调用时必须持有集合命名空间的某种形式锁
     * - 确保读取状态时的数据一致性
     * 
     * 报告内容：
     * - 当前迁移状态和阶段
     * - 已克隆的文档数量和字节数
     * - 剩余待传输的数据量
     * - 各阶段的耗时统计
     * - 错误信息和重试次数
     * 
     * 用途：
     * - serverStatus 命令的分片迁移部分
     * - 监控工具的实时状态展示
     * - 问题诊断和性能分析
     */
    BSONObj getMigrationStatusReport(
        const CollectionShardingRuntime::ScopedSharedCollectionShardingRuntime& scopedCsrLock)
        const;

    // 获取迁移的命名空间
    const NamespaceString& nss() {
        return _args.getCommandParameter();
    }

    // 获取迁移ID（如果协调器已创建）
    boost::optional<UUID> getMigrationId() {
        if (_coordinator) {
            return _coordinator->getMigrationId();
        }
        return boost::none;
    }

    // 获取整个操作的耗时（毫秒）
    long long getOpTimeMillis() {
        return _entireOpTimer.millis();
    }

private:
    // Used to track the current state of the source manager. See the methods above, which have
    // comments explaining the various state transitions.
    // 迁移状态枚举：跟踪源管理器的当前状态
    // 各方法的注释中解释了各种状态转换
    enum State {
        kCreated,           // 已创建：初始状态，完成基本初始化
        kCloning,           // 克隆中：数据传输进行中
        kCloneCaughtUp,     // 追赶完成：准备进入临界区
        kCriticalSection,   // 临界区：阻止写操作，最终同步
        kCloneCompleted,    // 克隆完成：接收方已确认数据接收
        kCommittingOnConfig,// Config提交中：更新元数据中
        kDone               // 已完成：迁移结束，清理完成
    };

    /**
     * 获取当前元数据并检查冲突错误：
     * 
     * 功能：
     * - 安全地获取集合当前的分片元数据
     * - 验证集合版本一致性（时间戳/纪元）
     * - 检测并发操作导致的状态变更
     * - 确保迁移基于正确的元数据进行
     * 
     * 检查项目：
     * - 集合分片状态是否被清除
     * - 版本时间戳是否发生变化
     * - 版本纪元是否匹配
     * - 集合是否仍然是分片的
     */
    CollectionMetadata _getCurrentMetadataAndCheckForConflictingErrors();

    /**
     * Called when any of the states fails. May only be called once and will put the migration
     * manager into the kDone state.
     */
    /**
     * 迁移清理：当任何状态失败时调用的清理方法
     * 
     * 参数：
     * - completeMigration：是否将迁移标记为完成
     * 
     * 功能：
     * - 只能调用一次，将管理器状态设为 kDone
     * - 根据参数决定是否完成或中止迁移
     * - 清理所有已分配的资源
     * - 记录最终状态到审计日志
     * 
     * 异常安全：
     * - 使用 noexcept 保证不抛出异常
     * - 确保析构过程的安全性
     */
    void _cleanup(bool completeMigration) noexcept;

    /**
     * May be called at any time. Unregisters the migration source manager from the collection,
     * restores the committed metadata (if in critical section) and logs error in the change log to
     * indicate that the migration has failed.
     *
     * Expected state: Any
     * Resulting state: kDone
     */
    /**
     * 错误清理：在发生错误时调用的清理方法
     * 
     * 功能：
     * - 可在任何状态下调用
     * - 从集合中注销迁移源管理器
     * - 恢复已提交的元数据（如果在临界区中）
     * - 在变更日志中记录失败信息
     * 
     * 状态转换：
     * - 期望状态：任何状态
     * - 结果状态：kDone
     * 
     * 清理操作：
     * - 释放集合临界区
     * - 停止克隆器操作
     * - 清理迁移协调器
     * - 更新统计计数器
     * 
     * 异常安全：
     * - 使用 noexcept 保证不抛出异常
     * - 即使在异常情况下也能正确清理
     */
    void _cleanupOnError() noexcept;

    // Mutex to protect concurrent reads and writes to internal attributes
    // 保护内部属性并发读写的互斥锁
    // 确保多线程环境下状态访问的线程安全性
    mutable Mutex _mutex = MONGO_MAKE_LATCH("MigrationSourceManager::_mutex");

    // This is the opCtx of the moveChunk request that constructed the MigrationSourceManager.
    // The caller must guarantee it outlives the MigrationSourceManager.
    // 构造 MigrationSourceManager 的 moveChunk 请求的操作上下文
    // 调用者必须保证其生命周期超过 MigrationSourceManager
    OperationContext* const _opCtx;

    // The parameters to the moveRange command
    // moveRange 命令的参数，包含迁移的所有配置信息
    ShardsvrMoveRange _args;

    // The write concern received for the moveRange command
    // moveRange 命令接收到的写关注点，控制持久性要求
    const WriteConcernOptions _writeConcern;

    // The resolved connection string of the donor shard
    // 源分片的解析连接字符串，用于建立网络连接
    const ConnectionString _donorConnStr;

    // The resolved primary of the recipient shard
    // 目标分片的解析主节点地址，数据传输的目标端点
    const HostAndPort _recipientHost;

    // Stores a reference to the process sharding statistics object which needs to be updated
    // 存储进程分片统计对象的引用，需要更新统计信息
    ShardingStatistics& _stats;

    // Information about the moveChunk to be used in the critical section.
    // 在临界区中使用的 moveChunk 信息，用于日志和监控
    const BSONObj _critSecReason;

    // Times the entire moveChunk operation
    // 计时整个 moveChunk 操作的耗时统计器
    const Timer _entireOpTimer;

    // Utility for constructing detailed logs for the steps of the chunk migration
    // 为 chunk 迁移步骤构造详细日志的工具类
    MoveTimingHelper _moveTimingHelper;

    // Promise which will be signaled when the migration source manager has finished running and is
    // ready to be destroyed
    // 当迁移源管理器完成运行并准备销毁时发出信号的 Promise
    SharedPromise<void> _completion;

    // Starts counting from creation time and is used to time various parts from the lifetime of the
    // move chunk sequence
    // 从创建时开始计时，用于计算 move chunk 序列生命周期中各部分的时间
    Timer _cloneAndCommitTimer;

    // The current state. Used only for diagnostics and validation.
    // 当前状态，仅用于诊断和验证
    State _state{kCreated};

    // Responsible for registering and unregistering the MigrationSourceManager from the collection
    // sharding runtime for the collection
    // 负责在集合分片运行时中注册和注销 MigrationSourceManager
    class ScopedRegisterer {
    public:
        // 构造时注册管理器到集合分片运行时
        ScopedRegisterer(MigrationSourceManager* msm, CollectionShardingRuntime& csr);
        // 析构时自动注销管理器
        ~ScopedRegisterer();

    private:
        MigrationSourceManager* const _msm;
    };
    boost::optional<ScopedRegisterer> _scopedRegisterer;

    // The epoch of the collection being migrated and its UUID, as of the time the migration
    // started. Values are boost::optional only up until the constructor runs, because UUID doesn't
    // have a default constructor.
    // TODO SERVER-80188: remove _collectionEpoch once 8.0 becomes last-lts.
    // 迁移开始时被迁移集合的纪元和UUID
    // 值为 boost::optional 仅在构造函数运行之前，因为 UUID 没有默认构造函数
    boost::optional<OID> _collectionEpoch;           // 集合纪元，用于版本一致性检查
    boost::optional<UUID> _collectionUUID;          // 集合UUID，唯一标识集合
    // 用途：检测集合分片配置的变更
    // 生命周期：与集合分片配置绑定
    boost::optional<Timestamp> _collectionTimestamp; // 集合时间戳，精确版本控制

    // The version of the chunk at the time the migration started.
    // 迁移开始时 chunk 的版本信息，用于版本一致性验证
    boost::optional<ChunkVersion> _chunkVersion;

    // The chunk cloner source. Only available if there is an active migration going on. To set and
    // remove it, a collection lock and the CSRLock need to be acquired first in order to block all
    // logOp calls and then the mutex. To access it, only the mutex is necessary. Available after
    // cloning stage has completed.
    // chunk 克隆器源。仅在有活跃迁移时可用。
    // 设置和移除时需要先获取集合锁和CSRLock以阻止所有logOp调用，然后获取互斥锁。
    // 访问时只需要互斥锁。在克隆阶段完成后可用。
    std::shared_ptr<MigrationChunkClonerSource> _cloneDriver;

    // Contains logic for ensuring the donor's and recipient's config.rangeDeletions entries are
    // correctly updated based on whether the migration committed or aborted.
    // 包含确保源端和接收端的 config.rangeDeletions 条目根据迁移提交或中止
    // 正确更新的逻辑
    boost::optional<migrationutil::MigrationCoordinator> _coordinator;

    // Holds the in-memory critical section for the collection. Only set when migration has reached
    // the critical section phase.
    // 持有集合的内存临界区。仅在迁移达到临界区阶段时设置。
    boost::optional<CollectionCriticalSection> _critSec;

    // The statistics about a chunk migration to be included in moveChunk.commit
    // 要包含在 moveChunk.commit 中的 chunk 迁移统计信息
    boost::optional<BSONObj> _recipientCloneCounts;

    // Optional future that is populated if the migration succeeds and range deletion is scheduled
    // on this node. The future is set when the range deletion completes. Used if the moveChunk was
    // sent with waitForDelete.
    // 可选的 future，如果迁移成功并且在此节点上安排了范围删除，则会填充。
    // 当范围删除完成时设置 future。如果 moveChunk 发送时带有 waitForDelete 则使用。
    boost::optional<SharedSemiFuture<void>> _cleanupCompleteFuture;
};

}  // namespace mongo
