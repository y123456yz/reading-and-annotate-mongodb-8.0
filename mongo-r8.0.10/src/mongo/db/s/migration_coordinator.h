/**
 *    Copyright (C) 2019-present MongoDB, Inc.
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

#include <utility>

#include <boost/move/utility_core.hpp>
#include <boost/optional/optional.hpp>

#include "mongo/db/keypattern.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/s/migration_coordinator_document_gen.h"
#include "mongo/db/s/migration_session_id.h"
#include "mongo/db/session/logical_session_id.h"
#include "mongo/db/session/logical_session_id_gen.h"
#include "mongo/db/shard_id.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/chunk_version.h"
#include "mongo/util/future.h"
#include "mongo/util/uuid.h"

namespace mongo {
namespace migrationutil {
/**
 * MigrationCoordinator 类的作用：
 * MongoDB 分片集群中迁移协调器，负责管理迁移的提交/中止过程和相关的持久化状态。
 * 
 * 核心功能：
 * 1. 迁移状态管理：管理迁移从开始到完成的完整生命周期状态
 * 2. 持久化协调：确保迁移状态在节点故障后能够正确恢复
 * 3. 范围删除协调：管理源端和接收端的 config.rangeDeletions 条目更新
 * 4. 路由表更新：协调 Config Server 上的路由表元数据更新
 * 5. 异常恢复：在主从切换或崩溃后恢复未完成的迁移
 * 
 * 持久化组件：
 * - config.migrationCoordinators：存储迁移协调器状态
 * - config.rangeDeletions：管理源端和接收端的范围删除任务
 * - Config Server 路由表：更新分片集群的元数据
 * 
 * 决策流程：
 * - 启动：初始化持久化状态，创建范围删除任务
 * - 决策：设置提交或中止决策
 * - 完成：根据决策更新两端的 rangeDeletions，处理路由表
 * - 清理：删除协调器状态，释放临界区
 * 
 * 故障恢复：
 * - 通过持久化的协调器文档在故障后恢复状态
 * - 支持从任意阶段继续未完成的迁移
 * - 确保孤儿文档清理的最终一致性
 * 
 * 该类是 MongoDB 分片迁移容错和一致性保证的核心组件。
 */
/**
 * Manages the migration commit/abort process, including updates to config.rangeDeletions on the
 * donor and the recipient, and updates to the routing table on the config server.
 */
class MigrationCoordinator {
public:
    /**
     * 构造函数：创建新的迁移协调器实例
     * 
     * 参数：
     * - sessionId：迁移会话标识符，用于去重和幂等性
     * - donorShard：源分片标识符
     * - recipientShard：目标分片标识符
     * - collectionNamespace：集合命名空间
     * - collectionUuid：集合唯一标识符
     * - range：迁移的 chunk 范围
     * - preMigrationChunkVersion：迁移前的 chunk 版本
     * - shardKeyPattern：分片键模式
     * - waitForDelete：是否等待删除完成
     * 
     * 功能：
     * - 生成唯一的迁移ID（UUID）
     * - 初始化迁移协调器文档
     * - 设置逻辑会话ID和事务号
     * - 存储所有迁移元数据参数
     */
    MigrationCoordinator(MigrationSessionId sessionId,
                         ShardId donorShard,
                         ShardId recipientShard,
                         NamespaceString collectionNamespace,
                         UUID collectionUuid,
                         ChunkRange range,
                         ChunkVersion preMigrationChunkVersion,
                         const KeyPattern& shardKeyPattern,
                         bool waitForDelete);

    /**
     * 从文档构造：从持久化的协调器文档恢复迁移协调器
     * 
     * 参数：
     * - doc：从 config.migrationCoordinators 集合读取的协调器文档
     * 
     * 功能：
     * - 用于故障恢复场景
     * - 从持久化状态重建协调器对象
     * - 恢复迁移ID、会话信息和决策状态
     * - 支持迁移过程的中断恢复
     */
    MigrationCoordinator(const MigrationCoordinatorDocument& doc);
    
    // 禁用拷贝和移动语义，确保协调器的唯一性
    MigrationCoordinator(const MigrationCoordinator&) = delete;
    MigrationCoordinator& operator=(const MigrationCoordinator&) = delete;
    MigrationCoordinator(MigrationCoordinator&&) = delete;
    MigrationCoordinator& operator=(MigrationCoordinator&&) = delete;

    /**
     * 析构函数：自动清理协调器资源
     * 
     * 功能：
     * - 等待异步操作完成（如释放接收方临界区）
     * - 确保所有清理工作正确执行
     * - 处理未完成的 Future 对象
     * - 记录清理完成状态
     */
    ~MigrationCoordinator();

    // 获取迁移的唯一标识符
    const UUID& getMigrationId() const;
    
    // 获取逻辑会话ID，用于事务和重试控制
    const LogicalSessionId& getLsid() const;
    
    // 获取事务号，确保操作的幂等性
    TxnNumber getTxnNumber() const;

    /**
     * Sets the shard key pattern on the coordinator. Needs to be called by migration recovery to
     * allow the range deletion task to access the shard key pattern.
     */
    /**
     * 设置分片键模式：在协调器上设置分片键模式
     * 
     * 参数：
     * - shardKeyPattern：分片键模式（可选）
     * 
     * 功能：
     * - 为范围删除任务提供分片键模式访问
     * - 主要用于迁移恢复场景
     * - 确保范围删除能够正确识别文档范围
     * - 支持动态设置和更新分片键信息
     * 
     * 使用场景：
     * - 故障恢复时重建协调器状态
     * - 范围删除任务需要分片键进行文档过滤
     * - 支持分片键模式的延迟加载
     */
    void setShardKeyPattern(const boost::optional<KeyPattern>& shardKeyPattern);

    /**
     * Initializes persistent state required to ensure that orphaned ranges are properly handled,
     * even after failover, by doing the following:
     *
     * 1) Inserts a document into the local config.migrationCoordinators with the lsid and
     * recipientId and waits for majority writeConcern. 2) Inserts a document into the local
     * config.rangeDeletions with the collectionUUID, range to delete, and "pending: true" and waits
     * for majority writeConcern.
     */
    /**
     * 启动迁移：初始化确保孤儿范围正确处理所需的持久化状态
     * 
     * 参数：
     * - opCtx：操作上下文
     * 
     * 执行步骤：
     * 1. 向本地 config.migrationCoordinators 插入文档，包含 lsid 和 recipientId，
     *    等待 majority 写关注点确认
     * 2. 向本地 config.rangeDeletions 插入文档，包含 collectionUUID、删除范围
     *    和 "pending: true" 状态，等待 majority 写关注点确认
     * 
     * 持久化保证：
     * - 使用 majority 写关注点确保数据持久化
     * - 即使在故障切换后也能正确处理孤儿范围
     * - 创建恢复所需的完整状态信息
     * 
     * 异常处理：
     * - 任何步骤失败都会回滚已完成的操作
     * - 确保持久化状态的一致性
     * - 支持重试和幂等操作
     */
    void startMigration(OperationContext* opCtx);

    /**
     * Saves the decision.
     *
     * This method is non-blocking and does not perform any I/O.
     */
    /**
     * 设置迁移决策：保存迁移的最终决策（提交或中止）
     * 
     * 参数：
     * - decision：迁移决策枚举（kCommitted 或 kAborted）
     * 
     * 功能：
     * - 在内存中设置迁移决策
     * - 非阻塞操作，不执行任何 I/O
     * - 为后续的 completeMigration 调用准备决策信息
     * - 支持决策的延迟持久化
     * 
     * 决策类型：
     * - kCommitted：迁移成功，需要提交变更
     * - kAborted：迁移失败，需要回滚状态
     * 
     * 设计原理：
     * - 分离决策制定和决策执行
     * - 允许在关键时刻快速设置决策
     * - 减少临界区中的 I/O 操作时间
     */
    void setMigrationDecision(DecisionEnum decision);

    /**
     * If a decision has been set, makes the decision durable, then communicates the decision by
     * updating the local (donor's) and remote (recipient's) config.rangeDeletions entries.
     *
     * If the decision was to commit, returns a future that is set when range deletion for
     * the donated range completes.
     */
    /**
     * 完成迁移：如果已设置决策，则使决策持久化，然后通过更新本地和远程 rangeDeletions 条目传达决策
     * 
     * 参数：
     * - opCtx：操作上下文
     * 
     * 功能：
     * - 持久化迁移决策到 config.migrationCoordinators
     * - 更新源端（本地）的 config.rangeDeletions 条目
     * - 更新接收端（远程）的 config.rangeDeletions 条目
     * - 协调两端的范围删除任务状态
     * 
     * 返回值：
     * - 如果决策是提交：返回范围删除完成时设置的 Future
     * - 如果决策是中止：返回 boost::none
     * 
     * 执行逻辑：
     * - 提交决策：删除接收端任务，启动源端删除
     * - 中止决策：删除源端任务，启动接收端删除
     * 
     * 异步处理：
     * - 范围删除在后台异步执行
     * - 返回的 Future 允许调用者等待删除完成
     * - 支持与其他操作的并发执行
     */
    boost::optional<SharedSemiFuture<void>> completeMigration(OperationContext* opCtx);

    /**
     * Deletes the persistent state for this migration from config.migrationCoordinators.
     */
    /**
     * 忘记迁移：从 config.migrationCoordinators 删除此迁移的持久化状态
     * 
     * 参数：
     * - opCtx：操作上下文
     * 
     * 功能：
     * - 删除协调器文档，标记迁移完全结束
     * - 清理持久化状态，释放存储空间
     * - 防止故障恢复时重复处理已完成的迁移
     * - 使用 majority 写关注点确保删除的持久性
     * 
     * 调用时机：
     * - 迁移完全完成后调用
     * - 范围删除完成后的最终清理
     * - 确保不会留下孤儿协调器文档
     * 
     * 幂等性：
     * - 可以安全地多次调用
     * - 处理文档已删除的情况
     * - 忽略不存在的文档错误
     */
    void forgetMigration(OperationContext* opCtx);

    /**
     * Asynchronously releases the recipient critical section without waiting for it to finish. Sets
     * the _releaseRecipientCriticalSectionFuture future that will be readied once the recipient
     * critical section has been released.
     */
    /**
     * 启动释放接收方临界区：异步释放接收方临界区而不等待其完成
     * 
     * 参数：
     * - opCtx：操作上下文
     * 
     * 功能：
     * - 向接收方发送释放临界区的异步请求
     * - 设置 _releaseRecipientCriticalSectionFuture，在释放完成时就绪
     * - 不阻塞当前线程等待释放完成
     * - 允许并发处理其他迁移任务
     * 
     * 异步设计：
     * - 立即返回，不等待网络操作完成
     * - 使用 Future 机制跟踪异步操作状态
     * - 支持超时和错误处理
     * 
     * 临界区释放：
     * - 恢复接收方的正常写操作
     * - 完成迁移的最后步骤
     * - 确保接收方能够服务新的请求
     * 
     * 错误处理：
     * - 处理网络错误和分片不可达
     * - 支持重试机制
     * - 记录释放状态和错误信息
     */
    void launchReleaseRecipientCriticalSection(OperationContext* opCtx);

private:
    /**
     * Deletes the range deletion task from the recipient node and marks the range deletion task on
     * the donor as ready to be processed. Returns a future that is set when range deletion for
     * the donated range completes.
     */
    /**
     * 在源端和接收端提交迁移：删除接收端的范围删除任务，标记源端的任务为可处理状态
     * 
     * 参数：
     * - opCtx：操作上下文
     * 
     * 功能：
     * - 删除接收端的 config.rangeDeletions 条目
     * - 将源端的范围删除任务标记为就绪状态
     * - 启动源端的范围删除处理
     * - 更新 Config Server 的路由表元数据
     * 
     * 返回值：
     * - SharedSemiFuture<void>：源端范围删除完成时设置的 Future
     * 
     * 执行顺序：
     * 1. 向接收端发送删除 rangeDeletions 条目的请求
     * 2. 更新源端的 rangeDeletions 条目状态
     * 3. 启动源端的异步范围删除任务
     * 4. 返回删除完成的 Future
     * 
     * 一致性保证：
     * - 确保只有一端执行范围删除
     * - 防止数据丢失和重复删除
     * - 处理网络分区和节点故障
     */
    SharedSemiFuture<void> _commitMigrationOnDonorAndRecipient(OperationContext* opCtx);

    /**
     * Deletes the range deletion task from the donor node and marks the range deletion task on the
     * recipient node as ready to be processed.
     */
    /**
     * 在源端和接收端中止迁移：删除源端的范围删除任务，标记接收端的任务为可处理状态
     * 
     * 参数：
     * - opCtx：操作上下文
     * 
     * 功能：
     * - 删除源端的 config.rangeDeletions 条目
     * - 将接收端的范围删除任务标记为就绪状态
     * - 启动接收端的范围删除处理
     * - 回滚路由表到迁移前状态
     * 
     * 中止逻辑：
     * - 源端保留原始数据，删除迁移标记
     * - 接收端清理已接收的数据
     * - 恢复原始的 chunk 分布状态
     * 
     * 清理操作：
     * 1. 向源端发送删除 rangeDeletions 条目的请求
     * 2. 更新接收端的 rangeDeletions 条目状态
     * 3. 启动接收端的异步范围删除任务
     * 4. 确保路由表状态回滚
     * 
     * 错误恢复：
     * - 处理部分完成的中止操作
     * - 确保清理操作的最终完成
     * - 支持重试和幂等处理
     */
    void _abortMigrationOnDonorAndRecipient(OperationContext* opCtx);

    /**
     * Waits for the completion of _releaseRecipientCriticalSectionFuture and ignores ShardNotFound
     * exceptions.
     */
    /**
     * 等待释放接收方临界区完成：等待 _releaseRecipientCriticalSectionFuture 完成并忽略 ShardNotFound 异常
     * 
     * 参数：
     * - opCtx：操作上下文
     * 
     * 功能：
     * - 阻塞等待异步临界区释放操作完成
     * - 忽略 ShardNotFound 异常（接收方可能已下线）
     * - 确保协调器清理前临界区已释放
     * - 处理网络超时和连接错误
     * 
     * 异常处理：
     * - ShardNotFound：接收方分片不存在或已下线，忽略
     * - 网络错误：记录但不影响整体迁移完成
     * - 超时错误：根据配置决定是否重试
     * 
     * 设计考虑：
     * - 临界区释放失败不应阻止迁移完成
     * - 接收方下线时仍应完成迁移
     * - 提供最大努力的清理保证
     * 
     * 调用场景：
     * - 析构函数中确保清理完成
     * - 迁移完成后的最终清理
     * - 异常路径中的资源释放
     */
    void _waitForReleaseRecipientCriticalSectionFutureIgnoreShardNotFound(OperationContext* opCtx);

    // 迁移协调器文档，包含所有迁移元数据和状态
    MigrationCoordinatorDocument _migrationInfo;
    
    // 分片键模式，用于范围删除任务的文档过滤
    boost::optional<KeyPattern> _shardKeyPattern;
    
    // 是否等待删除完成的标志
    bool _waitForDelete = false;
    
    // 异步释放接收方临界区的 Future 对象
    boost::optional<ExecutorFuture<void>> _releaseRecipientCriticalSectionFuture;
};

}  // namespace migrationutil
}  // namespace mongo
