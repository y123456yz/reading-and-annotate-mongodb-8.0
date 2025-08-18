/**
 *    Copyright (C) 2022-present MongoDB, Inc.
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

#include "mongo/db/s/migration_batch_inserter.h"

#include <mutex>
#include <type_traits>
#include <vector>

#include <boost/move/utility_core.hpp>
#include <boost/none.hpp>
#include <boost/optional/optional.hpp>

#include "mongo/base/error_codes.h"
#include "mongo/bson/bsonelement.h"
#include "mongo/db/admission/execution_admission_context.h"
#include "mongo/db/cancelable_operation_context.h"
#include "mongo/db/catalog/collection_operation_source.h"
#include "mongo/db/catalog/document_validation.h"
#include "mongo/db/client.h"
#include "mongo/db/ops/single_write_result_gen.h"
#include "mongo/db/ops/write_ops_exec.h"
#include "mongo/db/ops/write_ops_gen.h"
#include "mongo/db/repl/repl_client_info.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/db/s/range_deletion_util.h"
#include "mongo/db/s/sharding_runtime_d_params_gen.h"
#include "mongo/db/s/sharding_statistics.h"
#include "mongo/db/service_context.h"
#include "mongo/db/session/logical_session_id.h"
#include "mongo/db/session/session_catalog.h"
#include "mongo/db/session/session_catalog_mongod.h"
#include "mongo/db/transaction/transaction_participant.h"
#include "mongo/executor/task_executor_pool.h"
#include "mongo/logv2/log.h"
#include "mongo/logv2/log_attr.h"
#include "mongo/logv2/log_component.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/s/grid.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/decorable.h"
#include "mongo/util/out_of_line_executor.h"
#include "mongo/util/str.h"
#include "mongo/util/time_support.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kShardingMigration

namespace mongo {

namespace {

void checkOutSessionAndVerifyTxnState(OperationContext* opCtx) {
    auto mongoDSessionCatalog = MongoDSessionCatalog::get(opCtx);
    mongoDSessionCatalog->checkOutUnscopedSession(opCtx);
    TransactionParticipant::get(opCtx).beginOrContinue(
        opCtx,
        {*opCtx->getTxnNumber()},
        boost::none /* autocommit */,
        TransactionParticipant::TransactionActions::kNone);
}

template <typename Callable>
constexpr bool returnsVoid() {
    return std::is_void_v<std::invoke_result_t<Callable>>;
}

// Yields the checked out session before running the given function. If the function runs without
// throwing, will reacquire the session and verify it is still valid to proceed with the migration.
template <typename Callable, std::enable_if_t<!returnsVoid<Callable>(), int> = 0>
auto runWithoutSession(OperationContext* opCtx, Callable callable) {
    auto mongoDSessionCatalog = MongoDSessionCatalog::get(opCtx);
    mongoDSessionCatalog->checkInUnscopedSession(opCtx,
                                                 OperationContextSession::CheckInReason::kYield);

    auto retVal = callable();

    // The below code can throw, so it cannot run in a scope guard.
    opCtx->checkForInterrupt();
    checkOutSessionAndVerifyTxnState(opCtx);

    return retVal;
}

// Same as runWithoutSession above but takes a void function.
template <typename Callable, std::enable_if_t<returnsVoid<Callable>(), int> = 0>
void runWithoutSession(OperationContext* opCtx, Callable callable) {
    auto mongoDSessionCatalog = MongoDSessionCatalog::get(opCtx);
    mongoDSessionCatalog->checkInUnscopedSession(opCtx,
                                                 OperationContextSession::CheckInReason::kYield);

    callable();

    // The below code can throw, so it cannot run in a scope guard.
    opCtx->checkForInterrupt();
    checkOutSessionAndVerifyTxnState(opCtx);
}
}  // namespace


void MigrationBatchInserter::onCreateThread(const std::string& threadName) {
    Client::initThread(threadName, getGlobalServiceContext()->getService(ClusterRole::ShardServer));
}

/**
 * MigrationBatchInserter::run 函数的作用：
 * chunk迁移中的批量插入器执行函数，负责将从源分片获取的数据批次插入到目标分片的集合中。
 * 
 * 核心功能：
 * 1. 数据批次处理：解析并处理从源分片传输过来的BSON文档批次
 * 2. 批量插入执行：将文档分批插入到目标集合，优化插入性能
 * 3. 会话状态管理：维护迁移会话的一致性，支持事务上下文
 * 4. 写关注处理：根据配置等待副本集确认，确保数据持久性
 * 5. 进度跟踪更新：更新迁移进度统计，包括文档数量和字节数
 * 6. 孤儿文档管理：更新孤儿文档计数，用于后续清理
 * 7. 性能统计记录：记录克隆的文档数和字节数用于监控
 * 8. 中断响应处理：支持迁移过程中的优雅中断和错误处理
 * 
 * 执行流程：
 * - 创建可取消的操作上下文以支持中断
 * - 按配置的批次大小分批处理文档
 * - 禁用文档验证以提高插入性能
 * - 执行批量插入操作并检查结果
 * - 更新孤儿文档计数和迁移进度
 * - 处理写关注和副本集同步
 * 
 * 性能优化：
 * - 批量插入：减少网络往返和锁竞争
 * - 文档验证禁用：跳过不必要的验证提高性能
 * - 可配置批次大小：平衡内存使用和插入效率
 * - 二级限流：控制副本集同步压力
 * 
 * 容错机制：
 * - 双重中断检查：检查外层和内层操作上下文
 * - 异常捕获处理：捕获并记录插入异常
 * - 会话状态验证：确保迁移会话的有效性
 * - 写关注超时处理：处理副本集同步超时场景
 * 
 * 参数说明：
 * @param status 任务调度状态，必须为OK才能执行插入
 * 
 * 该函数是chunk迁移数据写入端的核心实现，确保数据的可靠传输和插入。
 * MigrationBatchFetcher<Inserter>::_runFetcher()
 */
void MigrationBatchInserter::run(Status status) const try {
    // Run is passed in a non-ok status if this function runs inline.
    // That happens if we schedule this task on a ThreadPool that is
    // already shutdown.  If we were to schedule a task on a shutdown ThreadPool,
    // then there is a logic error in our code.  Therefore, we assert that here.
    // 任务状态验证：确保任务调度状态正常
    // 如果ThreadPool已关闭但仍调度任务，说明代码逻辑错误
    invariant(status.isOK());
    
    // 提取数据批次：从BSON对象中获取要插入的文档数组
    // _batch 包含从源分片传输过来的文档批次数据
    auto arr = _batch["objects"].Obj();
    if (arr.isEmpty())
        return;  // 空批次直接返回，无需处理

    // 获取执行器：用于创建可取消的操作上下文
    // 执行器提供异步任务调度能力，支持迁移过程中的取消操作
    auto executor =
        Grid::get(_innerOpCtx->getServiceContext())->getExecutorPool()->getFixedExecutor();

    // 创建应用层操作上下文：支持取消操作，继承内层上下文的取消令牌
    // 这允许在插入过程中响应迁移中断信号，确保优雅停止
    auto applicationOpCtx = CancelableOperationContext(
        cc().makeOperationContext(), _innerOpCtx->getCancellationToken(), executor);

    auto opCtx = applicationOpCtx.get();

    // 定义中断检查函数：检查外层和内层操作上下文是否被中断
    // 双重检查确保在各个层级都能及时响应中断请求
    auto assertNotAborted = [&]() {
        {
            // 锁定外层客户端并检查中断状态
            // 外层上下文通常对应迁移管理命令或用户请求
            stdx::lock_guard<Client> lk(*_outerOpCtx->getClient());
            _outerOpCtx->checkForInterrupt();
        }
        // 检查当前操作上下文的中断状态
        // 当前上下文专门用于数据插入操作
        opCtx->checkForInterrupt();
    };

    // 文档迭代器：遍历批次中的所有文档进行插入
    auto it = arr.begin();
    while (it != arr.end()) {
        // 批次统计变量：跟踪当前子批次的插入情况
        int batchNumCloned = 0;     // 当前子批次克隆的文档数量
        int batchClonedBytes = 0;   // 当前子批次克隆的字节数
        // 获取配置的最大批次大小：动态加载配置参数
        // 允许运行时调整批次大小以优化性能
        const int batchMaxCloned = migrateCloneInsertionBatchSize.load();

        // 中断检查：在开始新的子批次插入前检查是否被中断
        assertNotAborted();

        // 构建插入命令：创建MongoDB写操作命令对象
        write_ops::InsertCommandRequest insertOp(_nss);
        insertOp.getWriteCommandRequestBase().setOrdered(true);  // 设置有序插入
        
        // 准备文档批次：收集要插入的文档直到达到批次大小限制
        insertOp.setDocuments([&] {
            std::vector<BSONObj> toInsert;
            // 循环收集文档直到达到批次限制或遍历完所有文档
            while (it != arr.end() && (batchMaxCloned <= 0 || batchNumCloned < batchMaxCloned)) {
                const auto& doc = *it;
                BSONObj docToClone = doc.Obj();
                toInsert.push_back(docToClone);      // 添加到插入列表
                batchNumCloned++;                    // 增加文档计数
                batchClonedBytes += docToClone.objsize();  // 累加字节数
                ++it;  // 移动到下一个文档
            }
            return toInsert;
        }());

        {
            // Disable the schema validation (during document inserts and updates)
            // and any internal validation for opCtx for performInserts()
            // 文档验证禁用：提高插入性能
            // 在迁移过程中，源分片的数据已经验证过，目标分片可以跳过验证
            // 禁用模式验证和内部验证，显著提高插入吞吐量
            DisableDocumentValidation documentValidationDisabler(
                opCtx,
                DocumentValidationSettings::kDisableSchemaValidation |
                    DocumentValidationSettings::kDisableInternalValidation);
            
            // 执行批量插入：调用MongoDB核心插入引擎
            // OperationSource::kFromMigrate 标记这是来自迁移的操作
            // 这个标记会影响oplog记录和触发器行为
            const auto reply =
                write_ops_exec::performInserts(opCtx, insertOp, OperationSource::kFromMigrate);
            
            // 插入结果验证：检查每个文档的插入状态
            // 如果任何文档插入失败，抛出异常终止迁移
            for (unsigned long i = 0; i < reply.results.size(); ++i) {
                uassertStatusOKWithContext(
                    reply.results[i],
                    str::stream() << "Insert of " << insertOp.getDocuments()[i] << " failed.");
            }
            // Revert to the original DocumentValidationSettings for opCtx
            // 文档验证设置自动恢复：作用域结束时自动恢复原始验证设置
        }

        // 孤儿文档计数更新：持久化更新孤儿文档数量
        // 孤儿文档是指在迁移过程中暂时存在于目标分片但尚未完成迁移的文档
        // 这个计数用于后续的清理和回滚操作
        rangedeletionutil::persistUpdatedNumOrphans(opCtx, _collectionUuid, _range, batchNumCloned);
        
        // 迁移进度更新：更新最大操作时间戳
        // 记录最新的复制操作时间，用于跟踪迁移进度和恢复点
        _migrationProgress->updateMaxOptime(
            repl::ReplClientInfo::forClient(opCtx->getClient()).getLastOp());

        // 分片统计更新：更新全局分片统计计数器
        // 这些统计信息用于监控和性能分析
        ShardingStatistics::get(opCtx).countDocsClonedOnRecipient.addAndFetch(batchNumCloned);
        ShardingStatistics::get(opCtx).countBytesClonedOnRecipient.addAndFetch(batchClonedBytes);
        
        // 调试日志：记录批次克隆统计信息
        LOGV2(6718408,
              "Incrementing cloned count by  ",
              "batchNumCloned"_attr = batchNumCloned,
              "batchClonedBytes"_attr = batchClonedBytes);
        
        // 迁移进度统计：更新迁移进度对象的统计信息
        _migrationProgress->incNumCloned(batchNumCloned);    // 增加克隆文档数
        _migrationProgress->incNumBytes(batchClonedBytes);   // 增加克隆字节数

        // 写关注和二级限流处理：确保副本集同步和控制同步压力
        // 只有当需要等待其他节点且线程数为1时才执行
        if (_writeConcern.needToWaitForOtherNodes() && _threadCount == 1) {
            // 获取二级限流票据：控制副本集同步的并发度
            // 防止过多的写关注等待操作压垮副本集
            if (auto ticket =
                    _secondaryThrottleTicket->tryAcquire(&ExecutionAdmissionContext::get(opCtx))) {
                // 在无会话状态下等待写关注：避免持有会话时的死锁
                // runWithoutSession 确保在等待期间释放迁移会话
                runWithoutSession(_outerOpCtx, [&] {
                    // 等待副本集确认：确保数据已复制到指定数量的副本节点
                    repl::ReplicationCoordinator::StatusAndDuration replStatus =
                        repl::ReplicationCoordinator::get(opCtx)->awaitReplication(
                            opCtx,
                            repl::ReplClientInfo::forClient(opCtx->getClient()).getLastOp(),
                            _writeConcern);
                    
                    // 写关注超时处理：处理副本集同步超时情况
                    if (replStatus.status.code() == ErrorCodes::WriteConcernFailed) {
                        // 记录警告但继续迁移：避免因为副本集延迟导致迁移失败
                        LOGV2_WARNING(22011,
                                      "secondaryThrottle on, but doc insert timed out; continuing",
                                      "migrationId"_attr = _migrationId.toBSON());
                    } else {
                        // 其他错误直接抛出异常：严重错误需要终止迁移
                        uassertStatusOK(replStatus.status);
                    }
                });
            } else {
                // Ticket should always be available unless thread pool max size 1 setting is not
                // being respected.
                // 票据获取失败断言：正常情况下票据应该总是可用的
                // 除非线程池最大大小为1的设置没有被遵守
                dassert(false);
            }
        }

        // 批次间延迟：在处理下一个子批次前暂停指定时间
        // 这个延迟可以减少对系统资源的压力，允许其他操作执行
        sleepmillis(migrateCloneInsertionBatchDelayMS.load());
    }
} catch (const DBException& e) {
    // 异常处理：捕获插入过程中的数据库异常
    // 
    // 终止内层操作：在异常情况下杀死内层操作上下文
    // 防止异常操作继续影响系统状态
    ClientLock lk(_innerOpCtx->getClient());
    _innerOpCtx->getServiceContext()->killOperation(lk, _innerOpCtx, ErrorCodes::Error(6718402));
    
    // 错误日志：记录批次应用失败的详细信息
    // 包含错误状态，便于问题诊断和调试
    LOGV2(6718407, "Batch application failed", "error"_attr = e.toStatus());
}
}  // namespace mongo
