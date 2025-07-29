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

#include "mongo/platform/atomic_word.h"

namespace mongo {

class BSONObjBuilder;
class OperationContext;
class ServiceContext;


/**
 * ShardingStatistics 结构体的作用：
 * MongoDB 分片子系统的进程级别统计信息收集器，用于监控和分析分片操作的性能指标。
 * 
 * 核心功能：
 * 1. 性能监控：收集各种分片操作的计数、耗时和数据量统计
 * 2. 错误追踪：记录配置过时、超时、冲突等异常情况的发生次数
 * 3. 资源使用分析：监控迁移过程中的资源消耗和并发度
 * 4. 运维支持：为 serverStatus 命令提供详细的分片子系统状态报告
 * 5. 调优依据：为系统调优和容量规划提供数据支撑
 * 
 * 统计类别：
 * - 迁移生命周期：启动、提交、中止等各阶段的计数和耗时
 * - 数据传输：克隆文档数、字节数、删除量等数据移动指标
 * - 临界区性能：写阻塞时间、提交耗时等关键性能指标
 * - 错误统计：各类异常和超时情况的记录
 * - 配置变更：Config Server 转换操作的统计
 * 
 * 该结构体通过原子操作确保了多线程环境下统计数据的准确性，是 MongoDB 分片集群监控的核心组件。
 */
/**
 * Encapsulates per-process statistics for the sharding subsystem.
 */
struct ShardingStatistics {
    // Counts how many times threads hit stale config exception (which is what triggers metadata
    // refreshes).
    // 统计线程遇到过时配置异常的次数（这是触发元数据刷新的原因）
    // 用途：监控分片路由信息的新鲜度，高数值表示配置更新频繁或网络延迟问题
    AtomicWord<long long> countStaleConfigErrors{0};

    // Cumulative, always-increasing counter of how many chunks this node has started to donate
    // (whether they succeeded or not).
    // 累积计数器：此节点开始发送的 chunk 数量（无论成功与否）
    // 用途：监控作为源分片的迁移活动总量，评估分片负载均衡的活跃度
    AtomicWord<long long> countDonorMoveChunkStarted{0};

    // Cumulative, always-increasing counter of how many chunks this node successfully committed.
    // 累积计数器：此节点成功提交的 chunk 数量
    // 用途：监控迁移成功率，与 countDonorMoveChunkStarted 对比可计算成功率
    AtomicWord<long long> countDonorMoveChunkCommitted{0};

    // Cumulative, always-increasing counter of how many move chunks this node aborted.
    // 累积计数器：此节点中止的 chunk 迁移数量
    // 用途：监控迁移失败情况，高数值可能表示系统资源不足或网络问题
    AtomicWord<long long> countDonorMoveChunkAborted{0};

    // Cumulative, always-increasing counter of how much time the entire move chunk operation took
    // (excluding range deletion).
    // 累积计数器：整个 chunk 迁移操作的总耗时（不包括范围删除）
    // 用途：评估迁移性能，计算平均迁移时间，识别性能瓶颈
    AtomicWord<long long> totalDonorMoveChunkTimeMillis{0};

    // Cumulative, always-increasing counter of how much time the clone phase took on the donor
    // node, before it was appropriate to enter the critical section.
    // 累积计数器：发送方节点克隆阶段的耗时，在进入临界区之前
    // 用途：分析迁移过程中数据复制阶段的性能，优化克隆策略
    AtomicWord<long long> totalDonorChunkCloneTimeMillis{0};

    // Cumulative, always-increasing counter of how many documents have been cloned on the
    // recipient node.
    // 累积计数器：接收方节点克隆的文档数量
    // 用途：监控接收端的数据处理能力，评估网络传输效率
    AtomicWord<long long> countDocsClonedOnRecipient{0};

    // Cumulative, always-increasing counter of how many documents have been cloned on the catch up
    // phase on the recipient node.
    // 累积计数器：接收方节点在追赶阶段克隆的文档数量
    // 用途：监控增量同步的数据量，评估追赶阶段的效率
    AtomicWord<long long> countDocsClonedOnCatchUpOnRecipient{0};

    // Cumulative, always-increasing counter of how many bytes have been cloned on the catch up
    // phase on the recipient node.
    // 累积计数器：接收方节点在追赶阶段克隆的字节数
    // 用途：监控追赶阶段的网络带宽使用，评估数据传输效率
    AtomicWord<long long> countBytesClonedOnCatchUpOnRecipient{0};

    // Cumulative, always-increasing counter of how many bytes have been cloned on the
    // recipient node.
    // 累积计数器：接收方节点克隆的总字节数
    // 用途：监控数据传输总量，计算网络带宽利用率和传输速度
    AtomicWord<long long> countBytesClonedOnRecipient{0};

    // Cumulative, always-increasing counter of how many documents have been cloned on the donor
    // node.
    // 累积计数器：发送方节点克隆的文档数量
    // 用途：监控发送端的数据处理能力，与接收端数据对比验证一致性
    AtomicWord<long long> countDocsClonedOnDonor{0};

    // Cumulative, always-increasing counter of how many bytes have been cloned on the donor
    // node.
    // 累积计数器：发送方节点克隆的字节数
    // 用途：监控发送端的数据传输量，评估发送端的网络和存储性能
    AtomicWord<long long> countBytesClonedOnDonor{0};

    // Cumulative, always-increasing counter of how many documents have been deleted by the
    // rangeDeleter.
    // 累积计数器：范围删除器删除的文档数量
    // 用途：监控迁移后清理工作的进度，确保源分片空间得到释放
    AtomicWord<long long> countDocsDeletedByRangeDeleter{0};

    // Cumulative, always-increasing counter of how many bytes have been deleted by the
    // rangeDeleter.
    // 累积计数器：范围删除器删除的字节数
    // 用途：监控存储空间的回收情况，评估删除操作的效率
    AtomicWord<long long> countBytesDeletedByRangeDeleter{0};

    // Cumulative, always-increasing counter of how many chunks this node started to receive
    // (whether the receiving succeeded or not)
    // 累积计数器：此节点开始接收的 chunk 数量（无论接收成功与否）
    // 用途：监控作为目标分片的迁移活动，评估接收端的负载情况
    AtomicWord<long long> countRecipientMoveChunkStarted{0};

    // Cumulative, always-increasing counter of how much time the critical section's commit phase
    // took (this is the period of time when all operations on the collection are blocked, not just
    // the reads)
    // 累积计数器：临界区提交阶段的耗时（此期间集合上的所有操作都被阻塞，不仅仅是读操作）
    // 用途：监控写阻塞时间，这是影响应用性能的关键指标
    AtomicWord<long long> totalCriticalSectionCommitTimeMillis{0};

    // Cumulative, always-increasing counter of how much time the entire critical section took. It
    // includes the time the recipient took to fetch the latest modifications from the donor and
    // persist them plus the critical section commit time.
    //
    // The value of totalCriticalSectionTimeMillis - totalCriticalSectionCommitTimeMillis gives the
    // duration of the catch-up phase of the critical section (where the last mods are transferred
    // from the donor to the recipient).
    // 累积计数器：整个临界区的总耗时。包括接收方从发送方获取最新修改并持久化的时间，以及临界区提交时间。
    //
    // totalCriticalSectionTimeMillis - totalCriticalSectionCommitTimeMillis 的值给出了
    // 临界区追赶阶段的持续时间（最后的修改从发送方传输到接收方的阶段）
    // 用途：全面评估临界区性能，区分追赶阶段和提交阶段的耗时
    AtomicWord<long long> totalCriticalSectionTimeMillis{0};

    // Cumulative, always-increasing counter of the number of migrations aborted on this node
    // after timing out waiting to acquire a lock.
    // 累积计数器：因等待获取锁超时而在此节点上中止的迁移数量
    // 用途：监控锁竞争问题，高数值表示系统并发压力大或存在长时间锁持有
    AtomicWord<long long> countDonorMoveChunkLockTimeout{0};

    // Cumulative, always-increasing counter of how much time the migration recipient critical
    // section took (this is the period of time when write operations on the collection on the
    // recipient are blocked).
    // 累积计数器：迁移接收方临界区的耗时（此期间接收方集合上的写操作被阻塞）
    // 用途：监控接收端的写阻塞时间，评估对接收分片应用的影响
    AtomicWord<long long> totalRecipientCriticalSectionTimeMillis{0};

    // Cumulative, always-increasing counter of the number of migrations aborted on this node
    // due to concurrent index operations.
    // 累积计数器：因并发索引操作而在此节点上中止的迁移数量
    // 用途：监控索引操作与迁移的冲突情况，指导索引维护时机的选择
    AtomicWord<long long> countDonorMoveChunkAbortConflictingIndexOperation{0};

    // Total number of migrations leftover from previous primaries that needs to be run to
    // completion. Valid only when this process is the repl set primary.
    // 前一个主节点遗留的需要运行完成的迁移总数。仅在此进程是复制集主节点时有效。
    // 用途：监控主从切换后的迁移恢复情况，确保数据一致性
    AtomicWord<long long> unfinishedMigrationFromPreviousPrimary{0};

    // Current number for chunkMigrationConcurrency that defines concurrent fetchers and inserters
    // used for _migrateClone(step 4) of chunk migration
    // 当前 chunkMigrationConcurrency 的数值，定义了 chunk 迁移的 _migrateClone（步骤4）中
    // 使用的并发获取器和插入器数量
    // 用途：动态调整迁移并发度，平衡性能和资源使用
    AtomicWord<int> chunkMigrationConcurrencyCnt{1};

    // Total number of commands run directly against this shard without the directShardOperations
    // role.
    // 在没有 directShardOperations 角色的情况下直接对此分片运行的命令总数
    // 用途：监控安全违规操作，确保分片访问的合规性
    AtomicWord<long long> unauthorizedDirectShardOperations{0};

    // Total number of times the _configsvrTransitionToDedicatedConfigServer command has started.
    // _configsvrTransitionToDedicatedConfigServer 命令启动的总次数
    // 用途：监控 Config Server 专用化转换操作的活动
    AtomicWord<long long> countTransitionToDedicatedConfigServerStarted{0};

    // Total number of times the _configsvrTransitionToDedicatedConfigServer command has completed.
    // _configsvrTransitionToDedicatedConfigServer 命令完成的总次数
    // 用途：监控专用化转换的成功率和完成情况
    AtomicWord<long long> countTransitionToDedicatedConfigServerCompleted{0};

    // Total number of times the _configsvrTransitionFromDedicatedConfigServer command has
    // completed.
    // _configsvrTransitionFromDedicatedConfigServer 命令完成的总次数
    // 用途：监控从专用 Config Server 的转换操作
    AtomicWord<long long> countTransitionFromDedicatedConfigServerCompleted{0};

    /**
     * Obtains the per-process instance of the sharding statistics object.
     */
    /**
     * 获取分片统计对象的进程级实例：
     * 
     * 功能：
     * - 提供全局访问点，确保整个进程中统计数据的一致性
     * - 支持从 ServiceContext 和 OperationContext 两种上下文获取
     * - 实现单例模式，避免重复创建统计对象
     * 
     * 使用场景：
     * - 在各种分片操作中记录统计数据
     * - 在 serverStatus 命令中获取统计信息
     * - 在监控和调试工具中访问性能指标
     */
    static ShardingStatistics& get(ServiceContext* serviceContext);
    static ShardingStatistics& get(OperationContext* opCtx);

    /**
     * Reports the accumulated statistics for serverStatus.
     */
    /**
     * 为 serverStatus 命令报告累积的统计信息：
     * 
     * 功能：
     * - 将所有统计指标格式化为 BSON 对象
     * - 提供结构化的性能数据输出
     * - 支持监控工具和管理脚本的数据获取
     * 
     * 输出内容：
     * - 迁移操作的计数和耗时统计
     * - 数据传输量和删除量统计
     * - 错误和异常情况统计
     * - 配置变更操作统计
     * 
     * 参数：
     * @param builder BSON 对象构建器，用于组装统计数据
     */
    void report(BSONObjBuilder* builder) const;
};

}  // namespace mongo
