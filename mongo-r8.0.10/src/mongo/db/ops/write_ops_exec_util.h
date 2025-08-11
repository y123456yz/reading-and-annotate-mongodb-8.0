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

#pragma once

#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/repl/optime.h"
#include "mongo/db/repl/repl_client_info.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/logv2/log.h"
#include "mongo/util/decorable.h"

namespace mongo::write_ops_exec {

/**
 * Sets the Client's LastOp to the system OpTime if needed. This is especially helpful for
 * adjusting the client opTime for cases when batched write performed multiple writes, but
 * when the last write was a no-op (which will not advance the client opTime).
 */

/** // 最终目的是确保客户端的LastOp总是反映最新的操作状态，即使空操作不会生成oplog，也会推进ReplClientInfo._lastOp。
 * LastOpFixer - 副本集写入操作的LastOp修正器类
 * 
 * 核心作用：
 * 1. 自动检测和修正no-op写入操作的客户端LastOp时间戳
 * 2. 确保写关注(WriteConcern)等待机制在no-op操作下的正确性
 * 3. 维护副本集环境下客户端复制进度的一致性和单调性
 * 4. 为批量写入操作提供统一的LastOp管理策略
 * 5. 优化性能，避免对不需要复制的本地数据库操作进行处理
 * 
 * 设计原理：
 * - RAII模式：通过构造和析构函数自动管理LastOp修正生命周期
 * - No-op检测：比较操作前后LastOp变化，识别未产生oplog条目的操作
 * - 按需修正：只对确实需要修正的操作进行LastOp调整，避免性能损失
 * - 副本集感知：区分本地数据库和需要复制的数据库操作
 * 
 * 典型使用场景：
 * - 条件更新但文档已满足条件（nModified=0）
 * - 删除操作但没有匹配的文档（nDeleted=0）
 * - 插入操作遇到重复键但继续处理其他文档
 * - 批量写入中部分操作为no-op的混合场景
 * 
 * 工作流程：
 * 1. 构造时初始化操作上下文引用
 * 2. startingOp()记录操作开始时的LastOp基线
 * 3. finishedOpSuccessfully()检测操作是否为no-op
 * 4. 析构时根据需要自动修正客户端LastOp到系统最新时间
 * 
 * 重要性：
 * - 确保写关注等待不会因no-op操作而失效或超时
 * - 保证客户端看到的复制进度准确反映数据库状态
 * - 支持MongoDB副本集的强一致性语义
 */
/**
 * Sets the Client's LastOp to the system OpTime if needed. This is especially helpful for
 * adjusting the client opTime for cases when batched write performed multiple writes, but
 * when the last write was a no-op (which will not advance the client opTime).
 */
class LastOpFixer {
public:
    /**
     * 构造函数 - 初始化LastOp修正器
     * 
     * 功能：
     * 1. 保存操作上下文引用，用于后续的LastOp访问和修正
     * 2. 初始化默认状态，准备进行LastOp跟踪
     * 3. 建立与当前客户端会话的关联，确保修正操作的正确性
     * 
     * @param opCtx 操作上下文，包含客户端会话、事务状态等信息
     */
    LastOpFixer(OperationContext* opCtx);

    /**
     * 析构函数 - 自动执行LastOp修正逻辑
     * 
     * 核心功能：
     * 1. 检查是否需要修正LastOp（基于_needToFixLastOp标志）
     * 2. 排除多文档事务场景（事务有自己的LastOp管理机制）
     * 3. 将客户端LastOp设置为系统最新OpTime，确保写关注等待的正确性
     * 4. 记录调试日志，便于问题排查和性能分析
     * 
     * 设计考虑：
     * - RAII保证：即使发生异常也会执行修正逻辑
     * - 事务兼容：避免干扰多文档事务的LastOp管理
     * - 性能友好：只在真正需要时才执行修正操作
     * - 可观测性：提供充分的日志记录用于监控
     */
    ~LastOpFixer();

    /**
     * 写入操作开始通知函数 - 设置LastOp跟踪基线
     * 
     * 核心功能：
     * 1. 根据命名空间特性决定是否需要LastOp修正（本地数据库除外）
     * 2. 记录操作开始时的客户端LastOp作为比较基线
     * 3. 为后续的no-op检测提供参考时间戳
     * 4. 优化性能，避免对不参与复制的操作进行跟踪
     * 
     * 命名空间处理逻辑：
     * - 本地数据库(local)：不参与副本集复制，无需LastOp修正
     * - 普通数据库：参与复制，需要LastOp跟踪和可能的修正
     * - 系统集合：根据复制策略决定是否需要处理
     * 
     * @param ns 目标命名空间，用于判断是否需要复制和LastOp修正
     */
    // Called when we are starting an operation on the given namespace. The namespace is
    // needed so we can check if it is local, since we do not need to fix lastOp for local
    // writes.
    void startingOp(const NamespaceString& ns);

    /**
     * 写入操作成功完成通知函数 - 检测no-op操作并设置修正标志
     * 
     * 核心功能：
     * 1. 比较操作前后的客户端LastOp，检测是否产生了新的oplog条目
     * 2. 识别no-op操作：如果LastOp未变化，说明操作未生成oplog
     * 3. 设置修正标志：为析构函数的LastOp修正提供判断依据
     * 4. 避免重复修正：如果操作已经更新了LastOp，则无需再次修正
     * 
     * No-op检测逻辑：
     * - LastOp未变化 = 没有新oplog条目 = no-op操作 = 需要修正
     * - LastOp已更新 = 有新oplog条目 = 正常操作 = 无需修正
     * 
     * 重要性：
     * - 确保写关注等待有正确的OpTime参考点
     * - 避免客户端因no-op操作而出现写关注等待异常
     * - 维护副本集复制进度的准确性和一致性
     */
    // Called when we finish the operation that we last called startingOp() for.
    void finishedOpSuccessfully();

private:
    /**
     * 客户端复制信息访问器 - 获取当前客户端的复制状态信息
     * 
     * 功能：
     * 1. 提供对客户端LastOp的读取和设置接口
     * 2. 封装底层的ReplClientInfo访问逻辑
     * 3. 确保操作的线程安全性和会话一致性
     * 4. 简化LastOp相关操作的代码复杂度
     * 
     * @return ReplClientInfo& 当前客户端的复制信息引用
     */
    repl::ReplClientInfo& replClientInfo() {
        return repl::ReplClientInfo::forClient(_opCtx->getClient());
    }

    /**
     * 成员变量说明：
     * 
     * _opCtx: 操作上下文常量指针
     * - 作用：提供对客户端会话、事务状态、锁状态等的访问
     * - 生命周期：与LastOpFixer对象相同，确保访问安全性
     * - 用途：获取客户端信息、检查事务状态、执行LastOp修正
     * 
     * _needToFixLastOp: LastOp修正需求标志
     * - 作用：标识当前操作是否需要在析构时进行LastOp修正
     * - 初始值：true（默认需要修正）
     * - 更新时机：startingOp()中根据命名空间设置，finishedOpSuccessfully()中根据no-op检测结果更新
     * - 用途：控制析构函数中的修正逻辑执行
     * 
     * _opTimeAtLastOpStart: 操作开始时的LastOp基线
     * - 作用：记录写入操作开始时客户端的LastOp时间戳
     * - 设置时机：startingOp()函数中设置
     * - 比较用途：在finishedOpSuccessfully()中与当前LastOp比较，检测no-op操作
     * - 重要性：是no-op检测算法的核心数据，确保修正逻辑的准确性
     */
    OperationContext* const _opCtx;
    bool _needToFixLastOp = true;
    repl::OpTime _opTimeAtLastOpStart;
};

/**
 * 写入权限验证函数 - 检查当前操作是否有权写入指定命名空间
 * 
 * 功能：
 * 1. 验证用户是否具有对目标集合的写入权限
 * 2. 检查命名空间的写入限制和约束条件
 * 3. 确保操作符合MongoDB的安全策略
 * 4. 在权限验证失败时抛出相应的异常
 * 
 * 注意：此函数假设调用者已经持有适当的锁
 * 
 * @param opCtx 操作上下文，包含用户身份和权限信息
 * @param nss 目标命名空间，指定要写入的集合
 * @throws UnauthorizedException 当用户没有写入权限时
 * @throws NamespaceNotSupportedException 当命名空间不支持写入时
 */
void assertCanWrite_inlock(OperationContext* opCtx, const NamespaceString& nss);

}  // namespace mongo::write_ops_exec
