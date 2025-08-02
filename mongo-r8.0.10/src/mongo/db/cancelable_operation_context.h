/**
 *    Copyright (C) 2021-present MongoDB, Inc.
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
#include <memory>
#include <utility>

#include "mongo/db/client.h"
#include "mongo/db/service_context.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/util/cancellation.h"
#include "mongo/util/future.h"
#include "mongo/util/out_of_line_executor.h"

namespace mongo {

class OperationContext;

/**
 * Wrapper class around an OperationContext that calls markKilled(ErrorCodes::Interrupted) when the
 * supplied CancellationToken is canceled.
 *
 * This class is useful for having an OperationContext be interrupted when a CancellationToken is
 * canceled. Note that OperationContext::getCancellationToken() is instead useful for having a
 * CancellationToken be canceled when an OperationContext is interrupted. The combination of the two
 * enables bridging between OperationContext interruption and CancellationToken cancellation
 * arbitrarily.
 *
 * IMPORTANT: Executors are allowed to refuse work. markKilled(ErrorCodes::Interrupted) won't be
 * called when the supplied CancellationToken is canceled if the task executor has already been shut
 * down, for example. Use a task executor bound to the process lifetime if you must guarantee that
 * the OperationContext is interrupted when the CancellationToken is canceled.
 */
/**
 * CancelableOperationContext 类的作用：
 * MongoDB中用于桥接OperationContext中断机制和CancellationToken取消机制的包装器类。
 * 
 * 核心功能：
 * 1. 中断机制桥接：当CancellationToken被取消时，自动调用OperationContext的markKilled()
 * 2. 双向取消支持：支持OperationContext → CancellationToken 和 CancellationToken → OperationContext 的取消传播
 * 3. 异步中断处理：通过执行器异步处理中断操作，避免阻塞调用线程
 * 4. 资源生命周期管理：确保OperationContext在适当时机被安全中断和清理
 * 5. 线程安全保护：通过原子操作和共享状态确保多线程环境下的安全性
 * 
 * 应用场景：
 * - 分片迁移：在迁移过程中需要响应外部取消信号
 * - 异步任务：长时间运行的任务需要支持取消操作
 * - 批处理操作：大批量数据处理过程中的中断控制
 * - 网络操作：可取消的网络请求和数据传输
 * 
 * 设计模式：
 * - RAII模式：自动管理资源生命周期和清理
 * - 代理模式：作为OperationContext的智能代理
 * - 观察者模式：监听CancellationToken的状态变化
 * 
 * 注意事项：
 * - 执行器可能拒绝工作：如果执行器已关闭，可能无法保证中断操作
 * - 建议使用进程生命周期绑定的执行器以确保可靠性
 * - 不支持拷贝和移动操作，确保唯一所有权
 * 
 * 该类是MongoDB异步操作和取消机制的核心组件，确保操作的可控性和响应性。
 */
class CancelableOperationContext {
public:
    /**
     * 构造函数：创建可取消的操作上下文包装器
     * 
     * @param opCtx 要包装的操作上下文的唯一指针
     * @param cancelToken 用于监听取消信号的取消令牌
     * @param executor 用于异步执行中断操作的执行器
     * 
     * 工作原理：
     * 1. 创建共享状态块用于线程间通信
     * 2. 设置取消令牌的回调函数
     * 3. 当取消令牌被触发时，异步调用 markKilled()
     */
    CancelableOperationContext(ServiceContext::UniqueOperationContext opCtx,
                               const CancellationToken& cancelToken,
                               ExecutorPtr executor);

    // 禁用拷贝构造和拷贝赋值：确保唯一所有权，防止资源管理混乱
    CancelableOperationContext(const CancelableOperationContext&) = delete;
    CancelableOperationContext& operator=(const CancelableOperationContext&) = delete;

    // 禁用移动构造和移动赋值：简化生命周期管理，避免悬空引用
    CancelableOperationContext(CancelableOperationContext&&) = delete;
    CancelableOperationContext& operator=(CancelableOperationContext&&) = delete;

    /**
     * 析构函数：清理资源并确保中断操作完成
     * 
     * 功能：
     * 1. 设置完成标志，防止后续的中断操作
     * 2. 等待正在进行的中断操作完成
     * 3. 确保OperationContext被正确清理
     * 
     * 线程安全：通过原子标志和期货机制确保安全清理
     */
    ~CancelableOperationContext();

    /**
     * 获取底层OperationContext指针
     * 
     * @return 指向包装的OperationContext的指针
     * 
     * 用法：允许直接访问OperationContext的所有功能
     * 设计：noexcept 确保不会抛出异常
     */
    OperationContext* get() const noexcept {
        return _opCtx.get();
    }

    /**
     * 箭头操作符重载：提供透明的OperationContext访问
     * 
     * @return 指向包装的OperationContext的指针
     * 
     * 用法：支持 cancelableOpCtx->method() 语法
     * 设计：使包装器表现得像普通指针
     */
    OperationContext* operator->() const noexcept {
        return get();
    }

    /**
     * 解引用操作符重载：提供直接的OperationContext引用访问
     * 
     * @return 包装的OperationContext的引用
     * 
     * 用法：支持 (*cancelableOpCtx).method() 语法
     * 设计：提供完整的OperationContext接口访问
     */
    OperationContext& operator*() const noexcept {
        return *get();
    }

private:
    /**
     * 共享状态块：用于线程间通信和同步
     * 
     * 功能：
     * - done：原子布尔标志，指示操作是否已完成
     * - 防止析构后的中断操作继续执行
     * - 确保中断操作的线程安全性
     */
    struct SharedBlock {
        AtomicWord<bool> done{false};  // 原子操作，线程安全的完成标志
    };

    // 成员变量：按照依赖关系顺序声明，确保正确的构造和析构顺序
    
    const std::shared_ptr<SharedBlock> _sharedBlock;        // 共享状态块，支持多线程访问
    const ServiceContext::UniqueOperationContext _opCtx;   // 被包装的操作上下文
    const SemiFuture<void> _markKilledFinished;            // 中断操作完成的期货对象
};

/**
 * A factory to create CancelableOperationContext objects that use the same CancelationToken and
 * executor.
 */
/**
 * CancelableOperationContextFactory 工厂类的作用：
 * 创建使用相同CancellationToken和执行器的CancelableOperationContext对象的工厂。
 * 
 * 核心功能：
 * 1. 批量创建：为多个客户端创建具有相同取消语义的操作上下文
 * 2. 配置复用：避免重复传递相同的取消令牌和执行器参数
 * 3. 一致性保证：确保所有创建的操作上下文具有相同的取消行为
 * 4. 资源共享：多个操作上下文共享同一个执行器和取消令牌
 * 
 * 应用场景：
 * - 批处理任务：需要为多个子任务创建可取消的操作上下文
 * - 连接池：为每个连接创建统一管理的操作上下文
 * - 分片操作：在多个分片上执行可统一取消的操作
 * 
 * 设计模式：
 * - 工厂模式：封装对象创建逻辑
 * - 配置对象模式：预设创建参数
 */
class CancelableOperationContextFactory {
public:
    /**
     * 构造函数：初始化工厂配置
     * 
     * @param cancelToken 所有创建的操作上下文将使用的取消令牌
     * @param executor 所有创建的操作上下文将使用的执行器
     * 
     * 设计：使用移动语义避免不必要的拷贝开销
     */
    CancelableOperationContextFactory(CancellationToken cancelToken, ExecutorPtr executor)
        : _cancelToken{std::move(cancelToken)}, _executor{std::move(executor)} {}

    /**
     * 创建可取消操作上下文
     * 
     * @param client 用于创建操作上下文的客户端
     * @return 新创建的CancelableOperationContext对象
     * 
     * 功能：
     * 1. 使用客户端创建新的操作上下文
     * 2. 应用工厂预设的取消令牌和执行器
     * 3. 返回完全配置的可取消操作上下文
     */
    CancelableOperationContext makeOperationContext(Client* client) const {
        return CancelableOperationContext{client->makeOperationContext(), _cancelToken, _executor};
    }

private:
    // 工厂配置：所有创建的对象将共享这些配置
    const CancellationToken _cancelToken;  // 共享的取消令牌
    const ExecutorPtr _executor;           // 共享的执行器
};

}  // namespace mongo
