/**
 *    Copyright (C) 2023-present MongoDB, Inc.
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

#include "mongo/db/ops/write_ops_exec_util.h"

#include "mongo/base/error_codes.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/db/s/collection_sharding_state.h"
#include "mongo/logv2/log.h"
#include "mongo/logv2/log_attr.h"
#include "mongo/logv2/log_component.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/str.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kWrite

namespace mongo::write_ops_exec {

LastOpFixer::LastOpFixer(OperationContext* opCtx) : _opCtx(opCtx) {}

/**
 * LastOpFixer析构函数 - RAII模式的LastOp自动修正执行器
 * 
 * 核心功能：
 * 1. 在对象销毁时自动执行LastOp修正逻辑，确保不遗漏任何需要修正的操作
 * 2. 检测并处理no-op写入操作，将客户端LastOp更新为系统最新OpTime
 * 3. 排除多文档事务场景，避免与事务自身的LastOp管理机制冲突
 * 4. 提供异常安全的LastOp修正，即使操作过程中发生异常也能正确处理
 * 5. 记录调试日志，便于监控和问题排查
 * 
 * 设计原理：
 * - RAII保证：利用C++析构函数的确定性执行，确保修正逻辑总是被调用
 * - 条件修正：只对真正需要修正的no-op操作进行处理，避免性能损失
 * - 事务兼容：智能跳过多文档事务，因为事务有专门的LastOp管理机制
 * - 容错处理：使用IgnoringCtxInterrupted版本，优雅处理操作上下文中断情况
 * 
 * 修正时机的重要性：
 * - 操作完成时修正：确保获取的是操作完成时刻的系统最新OpTime
 * - 异常安全性：即使writeConflictRetry或其他异常发生，也能正确修正
 * - 写关注支持：为后续的写关注等待提供正确的OpTime基准
 * 
 * 特殊处理场景：
 * - No-op更新：条件更新但文档已满足条件，需要修正LastOp
 * - No-op删除：删除操作但没有匹配文档，需要修正LastOp  
 * - 批量混合：批量操作中最后一个是no-op的情况
 * - 异常恢复：操作失败但仍需要正确的LastOp视图
 */
LastOpFixer::~LastOpFixer() {
    // We don't need to do this if we are in a multi-document transaction as read-only/noop
    // transactions will always write another noop entry at transaction commit time which we can
    // use to wait for writeConcern.
    
    // 多文档事务排除检查：事务有专门的LastOp管理机制
    // 原理：多文档事务在提交时会写入一个专门的事务提交oplog条目
    // 即使事务中包含no-op操作，事务提交条目也能为写关注等待提供正确的OpTime参考
    // 因此事务环境下无需通过LastOpFixer进行额外的LastOp修正
    if (!_opCtx->inMultiDocumentTransaction() && _needToFixLastOp) {
        // If this operation has already generated a new lastOp, don't bother setting it
        // here. No-op updates will not generate a new lastOp, so we still need the
        // guard to fire in that case.
        
        // 核心修正逻辑：将客户端LastOp设置为系统最新OpTime
        // 关键作用：
        // 1. 获取当前系统的最新写入OpTime，代表数据库的当前状态
        // 2. 更新客户端的LastOp视图，确保写关注等待基于正确的时间戳
        // 3. 维护副本集一致性，保证客户端看到操作完成时的复制进度
        // 4. 使用IgnoringCtxInterrupted版本，优雅处理操作上下文中断的情况
        replClientInfo().setLastOpToSystemLastOpTimeIgnoringCtxInterrupted(_opCtx);
        
        // 调试日志记录：记录修正操作的执行和结果时间戳
        // 重要性：
        // 1. 提供修正操作的可观测性，便于性能分析和问题排查
        // 2. 帮助开发者理解no-op操作的处理流程
        // 3. 在调试副本集一致性问题时提供关键信息
        // 4. 日志级别5确保只在详细调试时输出，避免影响性能
        LOGV2_DEBUG(20888,
                    5,
                    "Set last op to system time",
                    "timestamp"_attr = replClientInfo().getLastOp().getTimestamp());
    }
    
    // 析构完成后的状态：
    // 1. 如果是no-op操作，客户端LastOp已被修正为系统最新时间
    // 2. 如果是正常操作，LastOp保持操作自身更新的值不变
    // 3. 如果是事务操作，LastOp由事务机制管理，不受影响
    // 4. 写关注等待现在可以基于正确的OpTime进行
}

void LastOpFixer::startingOp(const NamespaceString& ns) {
    // Operations on the local DB aren't replicated, so they don't need to bump the lastOp.
    _needToFixLastOp = !ns.isLocalDB();
    _opTimeAtLastOpStart = replClientInfo().getLastOp();
}

void LastOpFixer::finishedOpSuccessfully() {
    // If we intended to fix the LastOp for this operation when it started, fix it now
    // if it was a no-op write. If the op was successful and already bumped LastOp itself,
    // we don't need to do it again.
    _needToFixLastOp = _needToFixLastOp && (replClientInfo().getLastOp() == _opTimeAtLastOpStart);
}

void assertCanWrite_inlock(OperationContext* opCtx, const NamespaceString& nss) {
    uassert(ErrorCodes::PrimarySteppedDown,
            str::stream() << "Not primary while writing to " << nss.toStringForErrorMsg(),
            repl::ReplicationCoordinator::get(opCtx->getServiceContext())
                ->canAcceptWritesFor(opCtx, nss));

    CollectionShardingState::assertCollectionLockedAndAcquire(opCtx, nss)
        ->checkShardVersionOrThrow(opCtx);
}

}  // namespace mongo::write_ops_exec
