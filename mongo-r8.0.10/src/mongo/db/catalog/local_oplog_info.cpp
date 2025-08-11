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


#include "mongo/db/catalog/local_oplog_info.h"

#include <boost/move/utility_core.hpp>
#include <boost/optional/optional.hpp>
// IWYU pragma: no_include "ext/alloc_traits.h"
#include <mutex>
#include <utility>

#include "mongo/db/curop.h"
#include "mongo/db/logical_time.h"
#include "mongo/db/repl/oplog.h"
#include "mongo/db/repl/optime.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/db/storage/flow_control.h"
#include "mongo/db/storage/record_store.h"
#include "mongo/db/storage/recovery_unit.h"
#include "mongo/db/transaction_resources.h"
#include "mongo/db/vector_clock_mutable.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/decorable.h"
#include "mongo/util/duration.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/timer.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kCatalog


namespace mongo {
namespace {

const auto localOplogInfo = ServiceContext::declareDecoration<LocalOplogInfo>();

}  // namespace

// static
LocalOplogInfo* LocalOplogInfo::get(ServiceContext& service) {
    return get(&service);
}

// static
LocalOplogInfo* LocalOplogInfo::get(ServiceContext* service) {
    return &localOplogInfo(service);
}

// static
LocalOplogInfo* LocalOplogInfo::get(OperationContext* opCtx) {
    return get(opCtx->getServiceContext());
}

RecordStore* LocalOplogInfo::getRecordStore() const {
    return _rs;
}

void LocalOplogInfo::setRecordStore(RecordStore* rs) {
    _rs = rs;
}

void LocalOplogInfo::resetRecordStore() {
    _rs = nullptr;
}

void LocalOplogInfo::setNewTimestamp(ServiceContext* service, const Timestamp& newTime) {
    VectorClockMutable::get(service)->tickClusterTimeTo(LogicalTime(newTime));
}

/**
 * 获取下一批OplogSlot时间戳槽位 - MongoDB oplog时间戳分配的核心机制
 * 
 * 核心功能：
 * 1. 为批量操作分配连续的、全局唯一的oplog时间戳槽位
 * 2. 更新集群逻辑时钟，确保时间戳的全局单调递增性
 * 3. 向存储引擎注册预留的时间戳，支持doc-locking引擎的乱序写入
 * 4. 集成流量控制机制，为复制延迟管理提供采样数据
 * 5. 管理异常情况下的稳定时间戳推进和资源清理
 * 
 * 设计原理：
 * - 原子性分配：通过互斥锁确保时间戳分配的原子性和一致性
 * - 存储引擎协调：提前通知存储引擎预留时间戳，优化写入性能
 * - 流量控制集成：为复制流量控制提供实时的写入负载信息
 * - 异常安全保证：通过回滚和提交钩子管理时间戳生命周期
 * 
 * 关键特性：
 * - 支持批量分配，提升高并发场景下的性能
 * - 与VectorClock集成，维护分布式时钟一致性
 * - 处理副本集term变更，确保选举后的时间戳正确性
 * - 提供调试信息收集，支持性能分析和问题排查
 * 
 * @param opCtx 操作上下文，包含事务状态、复制设置等
 * @param count 请求的时间戳槽位数量，对应批次大小
 * @return std::vector<OplogSlot> 分配的时间戳槽位列表，保证连续且唯一
 */

 /*
 具体应用场景举例
场景1：批量插入操作
// 客户端代码：批量插入3个文档
db.users.insertMany([
    {_id: 1, name: "Alice", age: 25},
    {_id: 2, name: "Bob", age: 30},
    {_id: 3, name: "Charlie", age: 35}
]);

// 1. acquireOplogSlotsForInserts调用getNextOpTimes
void acquireOplogSlotsForInserts(...) {
    auto batchSize = 3;  // 三个文档
    
    // 2. getNextOpTimes分配时间戳槽位
    auto oplogSlots = repl::getNextOpTimes(opCtx, batchSize);
    
    // 结果示例：
    // oplogSlots[0] = {ts: 1000, term: 5}
    // oplogSlots[1] = {ts: 1001, term: 5}  
    // oplogSlots[2] = {ts: 1002, term: 5}
    
    // 3. 分配给具体的插入语句
    insertStatements[0].oplogSlot = oplogSlots[0];  // Alice
    insertStatements[1].oplogSlot = oplogSlots[1];  // Bob
    insertStatements[2].oplogSlot = oplogSlots[2];  // Charlie
}

// 4. 后续的oplog写入使用预分配的时间戳
void writeOplogEntries(...) {
    // Alice的oplog条目将使用ts: 1000
    // Bob的oplog条目将使用ts: 1001
    // Charlie的oplog条目将使用ts: 1002
    // 即使物理写入顺序可能是: Bob -> Alice -> Charlie
    // 逻辑顺序仍然是: Alice(1000) -> Bob(1001) -> Charlie(1002)
}
 */
std::vector<OplogSlot> LocalOplogInfo::getNextOpTimes(OperationContext* opCtx, std::size_t count) {
    // 获取复制协调器：负责副本集状态管理和配置
    auto replCoord = repl::ReplicationCoordinator::get(opCtx);
    
    // 初始化term为未初始化状态
    // term代表副本集选举周期，用于区分不同主节点产生的oplog条目
    long long term = repl::OpTime::kUninitializedTerm;

    // Fetch term out of the newOpMutex.
    // 获取当前副本集term：在互斥锁外获取，避免锁嵌套
    // 重要性：term确保oplog条目能正确关联到产生它的主节点选举周期
    if (replCoord->getSettings().isReplSet()) {
        // Current term. If we're not a replset of pv=1, it remains kOldProtocolVersionTerm.
        // 获取当前term：如果不是pv=1的副本集，保持旧协议版本的term
        term = replCoord->getTerm();
    }

    // 时间戳变量：将被设置为分配的基础时间戳
    Timestamp ts;
    
    // Provide a sample to FlowControl after the `oplogInfo.newOpMutex` is released.
    // 流量控制采样：在互斥锁释放后向FlowControl提供采样数据
    // 作用：帮助流量控制机制监控写入负载，调节复制速度
    // 时机：使用ON_BLOCK_EXIT确保无论正常返回还是异常都会执行采样
    ON_BLOCK_EXIT([opCtx, &ts, count] {
        auto flowControl = FlowControl::get(opCtx);
        if (flowControl) {
            // 采样参数：最新分配的时间戳和操作数量
            // 用途：流量控制算法据此调整写入速率，防止复制延迟过大
            flowControl->sample(ts, count);
        }
    });

    // Allow the storage engine to start the transaction outside the critical section.
    // 存储引擎事务预准备：在临界区外启动事务，优化性能
    // 目的：减少临界区内的操作时间，提升并发度
    // 机制：存储引擎可以在锁外进行事务初始化和资源分配
    shard_role_details::getRecoveryUnit(opCtx)->preallocateSnapshot();
    
    {
        // 临界区开始：保护时间戳分配的原子性
        // _newOpMutex确保同时只有一个线程在分配时间戳
        // 这是oplog时间戳全局唯一性和单调性的核心保证
        stdx::lock_guard<Latch> lk(_newOpMutex);

        // 核心时间戳分配：推进集群逻辑时钟
        // tickClusterTime执行以下关键操作：
        // 1. 将集群时钟推进count个单位
        // 2. 确保时间戳的全局单调递增性
        // 3. 返回分配的基础时间戳
        // 4. 更新集群的逻辑时间视图
        ts = VectorClockMutable::get(opCtx)->tickClusterTime(count).asTimestamp();
        
        // 有序提交标志：设置为false表示允许乱序提交
        // 对于doc-locking存储引擎，oplog条目可以按物理位置乱序写入
        // 但逻辑顺序仍然由时间戳确定
        const bool orderedCommit = false;

        // The local oplog record store pointer must already be established by this point.
        // We can't establish it here because that would require locking the local database, which
        // would be a lock order violation.
        // 存储引擎时间戳注册：确保oplog记录存储已初始化
        // 断言检查：_rs必须已经建立，避免锁顺序违反
        // 锁顺序：如果在此处建立_rs，需要锁定local数据库，会造成锁顺序违反
        invariant(_rs);
        
        // 向存储引擎注册时间戳：
        // 关键功能：通知存储引擎预留指定的时间戳位置
        // 参数说明：
        // - opCtx: 操作上下文
        // - ts: 预留的基础时间戳
        // - orderedCommit: 是否要求有序提交（doc-locking为false）
        // 返回值检查：确保注册成功，失败则触发致命错误
        fassert(28560, _rs->oplogDiskLocRegister(opCtx, ts, orderedCommit));
    }
    // 临界区结束：时间戳分配完成，可以开始后续处理

    // 性能计时器：跟踪oplog槽位的持有时间
    // 用途：为性能分析提供槽位使用时长的统计数据
    // 重要性：帮助识别長时间持有槽位的操作，优化系统性能
    Timer oplogSlotDurationTimer;
    
    // 槽位分配：为每个操作创建对应的OplogSlot
    // 容量预分配：提前分配vector容量，避免动态扩容的开销
    std::vector<OplogSlot> oplogSlots(count);
    for (std::size_t i = 0; i < count; i++) {
        // 连续时间戳分配：每个槽位的时间戳递增1
        // 时间戳计算：基础时间戳 + 偏移量，确保严格递增
        // OplogSlot构造：{时间戳, term}，完整标识一个oplog位置
        oplogSlots[i] = {Timestamp(ts.asULL() + i), term};
    }

    // If we abort a transaction that has reserved an optime, we should make sure to update the
    // stable timestamp if necessary, since this oplog hole may have been holding back the stable
    // timestamp.
    // 异常回滚处理：注册事务回滚时的清理逻辑
    // 核心问题：预留的oplog槽位可能会阻碍stable timestamp的推进
    // 解决方案：在回滚时尝试推进stable timestamp，填补oplog空洞
    shard_role_details::getRecoveryUnit(opCtx)->onRollback(
        [replCoord, oplogSlotDurationTimer](OperationContext* opCtx) {
            // 推进稳定时间戳：尝试跳过因回滚产生的oplog空洞
            // 重要性：避免未使用的oplog槽位持续阻碍stable timestamp
            // 机制：重新计算可以安全推进到的最新时间戳位置
            replCoord->attemptToAdvanceStableTimestamp();
            
            // Sum the oplog slot durations. An operation may participate in multiple transactions.
            // 性能统计：累加oplog槽位持有时间到调试信息
            // 多事务支持：一个操作可能参与多个事务，需要累加时间
            // 用途：帮助分析oplog槽位的使用效率和潜在的性能瓶颈
            CurOp::get(opCtx)->debug().totalOplogSlotDurationMicros +=
                Microseconds(oplogSlotDurationTimer.elapsed());
        });

    // 正常提交处理：注册事务成功提交时的统计逻辑
    // 目的：无论成功还是失败，都记录槽位使用时间用于性能分析
    shard_role_details::getRecoveryUnit(opCtx)->onCommit(
        [oplogSlotDurationTimer](OperationContext* opCtx, boost::optional<Timestamp>) {
            // Sum the oplog slot durations. An operation may participate in multiple transactions.
            // 成功提交的性能统计：与回滚处理类似，累加槽位持有时间
            // 数据收集：为MongoDB的性能监控和优化提供关键指标
            CurOp::get(opCtx)->debug().totalOplogSlotDurationMicros +=
                Microseconds(oplogSlotDurationTimer.elapsed());
        });

    // 返回分配的槽位：每个槽位包含唯一的{时间戳, term}对
    // 保证：返回的槽位严格按时间戳递增排序
    // 用途：调用者将这些槽位分配给具体的写入操作
    return oplogSlots;
}

}  // namespace mongo
