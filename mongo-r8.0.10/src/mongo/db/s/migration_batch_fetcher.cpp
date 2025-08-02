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

#include "mongo/db/s/migration_batch_fetcher.h"

#include <functional>
#include <mutex>
#include <utility>

#include <absl/container/node_hash_map.h>
#include <boost/move/utility_core.hpp>

#include "mongo/base/error_codes.h"
#include "mongo/base/status.h"
#include "mongo/client/read_preference.h"
#include "mongo/db/cancelable_operation_context.h"
#include "mongo/db/feature_flag.h"
#include "mongo/db/s/migration_batch_mock_inserter.h"
#include "mongo/db/s/sharding_runtime_d_params_gen.h"
#include "mongo/executor/task_executor_pool.h"
#include "mongo/logv2/log.h"
#include "mongo/logv2/log_attr.h"
#include "mongo/logv2/log_component.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/grid.h"
#include "mongo/s/sharding_feature_flags_gen.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/duration.h"
#include "mongo/util/out_of_line_executor.h"
#include "mongo/util/timer.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kSharding

namespace mongo {

template <typename Inserter>
void MigrationBatchFetcher<Inserter>::BufferSizeTracker::waitUntilSpaceAvailableAndAdd(
    OperationContext* opCtx, int sizeBytes) {
    if (_maxSizeBytes == MigrationBatchFetcher<Inserter>::BufferSizeTracker::kUnlimited) {
        return;
    }

    uassert(8120100,
            str::stream() << "chunkMigrationFetcherMaxBufferedSizeBytesPerThread setting of "
                          << _maxSizeBytes << " is too small for received batch size of "
                          << sizeBytes,
            sizeBytes <= _maxSizeBytes);

    stdx::unique_lock lk(_mutex);
    opCtx->waitForConditionOrInterrupt(_hasAvailableSpace, lk, [this, sizeBytes] {
        return (_currentSize + sizeBytes) <= _maxSizeBytes;
    });
    _currentSize += sizeBytes;
}

template <typename Inserter>
void MigrationBatchFetcher<Inserter>::BufferSizeTracker::remove(int sizeBytes) {
    if (_maxSizeBytes == MigrationBatchFetcher<Inserter>::BufferSizeTracker::kUnlimited) {
        return;
    }

    stdx::unique_lock lk(_mutex);
    invariant(_currentSize >= sizeBytes);

    _currentSize -= sizeBytes;
    _hasAvailableSpace.notify_one();
}

template <typename Inserter>
MigrationBatchFetcher<Inserter>::MigrationBatchFetcher(
    OperationContext* outerOpCtx,
    OperationContext* innerOpCtx,
    NamespaceString nss,
    MigrationSessionId sessionId,
    const WriteConcernOptions& writeConcern,
    const ShardId& fromShardId,
    const ChunkRange& range,
    const UUID& migrationId,
    const UUID& collectionId,
    std::shared_ptr<MigrationCloningProgressSharedState> migrationProgress,
    bool parallelFetchingSupported,
    int maxBufferedSizeBytesPerThread)
    : _nss{std::move(nss)},
      _chunkMigrationConcurrency{1},
      _sessionId{std::move(sessionId)},
      _inserterWorkers{[&]() {
          ThreadPool::Options options;
          options.poolName = "ChunkMigrationInserters";
          options.minThreads = _chunkMigrationConcurrency;
          options.maxThreads = _chunkMigrationConcurrency;
          options.onCreateThread = Inserter::onCreateThread;
          return std::make_unique<ThreadPool>(options);
      }()},
      _migrateCloneRequest{_createMigrateCloneRequest()},
      _outerOpCtx{outerOpCtx},
      _innerOpCtx{innerOpCtx},
      _fromShard{uassertStatusOK(
          Grid::get(_outerOpCtx)->shardRegistry()->getShard(_outerOpCtx, fromShardId))},
      _migrationProgress{migrationProgress},
      _range{range},
      _collectionUuid(collectionId),
      _migrationId{migrationId},
      _writeConcern{writeConcern},
      _isParallelFetchingSupported{parallelFetchingSupported},
      _secondaryThrottleTicket(outerOpCtx->getServiceContext(), 1, false /* trackPeakUsed */),
      _bufferSizeTracker(maxBufferedSizeBytesPerThread) {
    // (Ignore FCV check): This feature flag doesn't have any upgrade/downgrade concerns.
    if (mongo::feature_flags::gConcurrencyInChunkMigration.isEnabledAndIgnoreFCVUnsafe() &&
        chunkMigrationConcurrency.load() > 1) {
        LOGV2_INFO(9532401,
                   "The ChunkMigrationConcurrency setting has been deprecated and is now fixed at "
                   "a value of 1");
    }

    _inserterWorkers->startup();
}

template <typename Inserter>
BSONObj MigrationBatchFetcher<Inserter>::_fetchBatch(OperationContext* opCtx) {
    auto commandResponse = uassertStatusOKWithContext(
        _fromShard->runCommand(opCtx,
                               ReadPreferenceSetting(ReadPreference::PrimaryOnly),
                               DatabaseName::kAdmin,
                               _migrateCloneRequest,
                               Shard::RetryPolicy::kNoRetry),
        "_migrateClone failed: ");

    uassertStatusOKWithContext(Shard::CommandResponse::getEffectiveStatus(commandResponse),
                               "_migrateClone failed: ");

    return commandResponse.response;
}

/**
 * MigrationBatchFetcher<Inserter>::fetchAndScheduleInsertion 函数的作用：
 * 负责协调chunk迁移过程中的数据获取和插入操作的核心调度函数。
 * 
 * 核心职责：
 * 1. 并发管理：创建并管理数据获取线程池，支持并行获取数据
 * 2. 数据获取协调：调度多个获取器线程从源分片并发拉取数据批次
 * 3. 插入任务调度：将获取的数据批次异步调度给插入器线程池处理
 * 4. 流水线优化：实现获取-插入的流水线并行处理，提高迁移效率
 * 5. 线程生命周期管理：管理获取器线程池的启动、执行和优雅关闭
 * 
 * 工作原理：
 * - 根据并行支持配置创建获取器线程池
 * - 每个获取器线程独立执行 _runFetcher() 循环获取数据
 * - 获取的数据通过插入器线程池异步插入到目标分片
 * - 使用缓冲区大小跟踪器控制内存使用
 * 
 * 并发策略：
 * - 支持单线程和多线程获取模式
 * - 获取器和插入器独立运行，形成生产者-消费者模式
 * - 通过信号量和条件变量协调线程间同步
 * 
 * 性能优化：
 * - 流水线处理：获取下一批次时同时处理当前批次
 * - 内存控制：通过缓冲区限制防止内存过度使用
 * - 批次大小优化：动态调整批次大小以平衡网络和处理效率
 * 
 * 错误处理：
 * - 支持优雅的线程池关闭和资源清理
 * - 传播获取和插入过程中的异常
 * - 支持迁移中断和取消操作
 * 
 * 该函数是chunk迁移数据传输的核心调度器，确保高效、可靠的数据迁移。
 */
template <typename Inserter>
void MigrationBatchFetcher<Inserter>::fetchAndScheduleInsertion() {
    // 根据并行获取支持情况确定获取器线程数量
    // 如果支持并行获取，使用配置的并发数；否则使用单线程
    auto numFetchers = _isParallelFetchingSupported ? _chunkMigrationConcurrency : 1;
    
    // 创建数据获取器线程池：专门负责从源分片获取数据批次
    // MongoDB chunk迁移采用生产者-消费者双线程池架构：生产者线程池: ChunkMigrationFetchers  消费者线程池: ChunkMigrationInserters
    auto fetchersThreadPool = [&]() { 
        ThreadPool::Options options;
        options.poolName = "ChunkMigrationFetchers";  // 线程池名称，便于调试和监控
        options.minThreads = numFetchers;             // 最小线程数等于获取器数量
        options.maxThreads = numFetchers;             // 最大线程数等于获取器数量，固定大小
        options.onCreateThread = onCreateThread;     // 线程创建时的回调函数
        return std::make_unique<ThreadPool>(options);
    }();
    
    // 启动获取器线程池：准备开始数据获取工作
    fetchersThreadPool->startup();
    
    // 为每个获取器线程调度数据获取任务
    // 每个线程将独立执行 _runFetcher() 函数
    for (int i = 0; i < numFetchers; ++i) {
        fetchersThreadPool->schedule([this](Status status) { 
            this->_runFetcher();  // 执行实际的数据获取逻辑
        });
    }

    // 优雅关闭获取器线程池：
    // 1. shutdown() - 停止接受新任务，但允许现有任务完成
    fetchersThreadPool->shutdown();
    
    // 2. join() - 等待所有获取器线程完成其工作
    // 确保所有数据都已获取并调度给插入器
    fetchersThreadPool->join();
    
    // 当此函数返回时，意味着：
    // - 所有数据批次都已从源分片获取完毕
    // - 所有批次都已调度给插入器线程池处理
    // - 获取器线程池已完全关闭和清理
    // 注意：插入器线程池可能仍在处理最后的批次
}

/**
 * MigrationBatchFetcher<Inserter>::_runFetcher 函数的作用：
 * 单个获取器线程的核心执行函数，负责从源分片循环获取数据批次并调度插入操作。
 * 
 * 核心职责：
 * 1. 数据批次获取：循环向源分片发送_migrateClone命令获取数据批次
 * 2. 获取完成检测：检测空批次以确定数据获取是否完成
 * 3. 内存流控管理：通过缓冲区跟踪器控制内存使用，防止内存溢出
 * 4. 插入任务调度：将获取的数据批次异步调度给插入器线程池处理
 * 5. 性能监控统计：记录获取时间、批次大小、吞吐量等性能指标
 * 6. 中断检查处理：定期检查操作是否被中断或取消
 * 7. 异常处理传播：捕获并处理获取过程中的异常情况
 * 
 * 工作流程：
 * - 创建取消操作上下文以支持中断
 * - 循环执行数据获取直到收到空批次
 * - 每次获取后检查缓冲区空间并等待可用空间
 * - 创建插入器对象并调度到插入线程池
 * - 记录详细的性能统计信息
 * 
 * 性能优化：
 * - 流水线处理：获取和插入并行执行
 * - 内存控制：通过缓冲区限制防止内存过度使用
 * - 批次处理：减少网络往返次数
 * - 异步插入：插入操作不阻塞获取操作
 * 
 * 错误处理：
 * - 支持优雅的中断处理
 * - 异常时终止内层操作上下文
 * - 详细的错误日志记录
 * 
 * 该函数是每个获取器线程的主要工作循环，确保高效可靠的数据获取。
 */
template <typename Inserter>
void MigrationBatchFetcher<Inserter>::_runFetcher() try {
    // 获取执行器用于创建可取消的操作上下文
    // 执行器提供异步任务调度能力，支持取消操作
    auto executor =
        Grid::get(_innerOpCtx->getServiceContext())->getExecutorPool()->getFixedExecutor();

    // 创建应用层操作上下文：支持取消操作，继承内层上下文的取消令牌
    // 这允许在迁移过程中响应中断信号，确保优雅停止
    auto applicationOpCtx = CancelableOperationContext(
        cc().makeOperationContext(), _innerOpCtx->getCancellationToken(), executor);

    auto opCtx = applicationOpCtx.get();
    
    // 定义中断检查函数：检查外层和内层操作上下文是否被中断
    // 双重检查确保在各个层级都能及时响应中断请求
    auto assertNotAborted = [&]() {
        {
            // 锁定外层客户端并检查中断状态
            // 外层上下文通常对应用户请求或管理命令
            stdx::lock_guard<Client> lk(*_outerOpCtx->getClient());
            _outerOpCtx->checkForInterrupt();
        }
        // 检查当前操作上下文的中断状态
        // 当前上下文专门用于数据获取操作
        opCtx->checkForInterrupt();
    };

    // 记录数据获取开始日志，便于迁移过程跟踪和调试
    LOGV2_DEBUG(6718405, 0, "Chunk migration data fetch start", "migrationId"_attr = _migrationId);
    
    // 主要的数据获取循环：持续获取直到收到空批次
    // 这是获取器线程的核心工作循环
    while (true) {
        // 开始计时：用于性能统计和监控
        // totalTimer 记录整个批次处理的总时间（获取+插入）
        Timer totalTimer;
        
        // 从源分片获取下一个数据批次
        // 向源分片发送_migrateClone命令并获取响应
        // 这是关键的网络通信操作
        BSONObj nextBatch = _fetchBatch(opCtx);
        
        // 检查操作是否被中断
        // 在网络操作后立即检查，确保及时响应中断
        assertNotAborted();
        
        // 检查是否为空批次：空批次表示数据获取完成
        // 源分片通过返回空的objects数组来表示没有更多数据
        if (_isEmptyBatch(nextBatch)) {
            LOGV2_DEBUG(6718404,
                        0,
                        "Chunk migration initial clone complete",
                        "migrationId"_attr = _migrationId,
                        "duration"_attr = totalTimer.elapsed());
            break;  // 退出获取循环，数据获取完成
        }

        // 计算批次大小：用于内存管理和性能统计
        // objsize() 返回BSON对象的字节大小
        const auto batchSize = nextBatch.objsize();
        // 记录获取时间：用于性能分析和网络延迟监控
        const auto fetchTime = totalTimer.elapsed();
        
        // 记录批次获取完成日志，包含关键性能指标
        LOGV2_DEBUG(6718416,
                    0,
                    "Chunk migration initial clone fetch end",
                    "migrationId"_attr = _migrationId,
                    "batchSize"_attr = batchSize,
                    "fetch"_attr = duration_cast<Milliseconds>(fetchTime));

        // 内存流控：等待缓冲区有足够空间容纳当前批次
        // 这防止了过多的批次同时在内存中等待处理，避免内存溢出
        // 如果缓冲区满，线程会阻塞直到有足够空间
        _bufferSizeTracker.waitUntilSpaceAvailableAndAdd(opCtx, batchSize);

        // 创建插入器对象：负责将当前批次插入到目标分片
        // 插入器封装了批次数据和所有必要的上下文信息
        Inserter inserter{_outerOpCtx,                    // 外层操作上下文
                          _innerOpCtx,                    // 内层操作上下文
                          nextBatch.getOwned(),           // 获取批次数据的所有权
                          _nss,                           // 目标命名空间
                          _range,                         // 迁移范围
                          _writeConcern,                  // 写关注设置
                          _collectionUuid,                // 集合UUID
                          _migrationProgress,             // 迁移进度跟踪
                          _migrationId,                   // 迁移ID
                          _chunkMigrationConcurrency,     // 并发设置
                          &_secondaryThrottleTicket};     // 辅助节点限流票据

        // 将插入任务调度到插入器线程池中异步执行
        // 这实现了获取和插入的流水线并行处理，提高整体效率
        // 使用lambda捕获所有必要的变量，确保异步执行时数据有效
        _inserterWorkers->schedule([this,
                                    batchSize,                     // 批次大小（用于缓冲区管理）
                                    fetchTime,                     // 获取时间（用于性能统计）
                                    totalTimer = std::move(totalTimer),  // 总计时器
                                    insertTimer = Timer(),         // 插入计时器
                                    migrationId = _migrationId,    // 迁移ID
                                    inserter = std::move(inserter)](Status status) {
            // 确保在插入完成后释放缓冲区空间
            // ON_BLOCK_EXIT 确保无论正常完成还是异常都会执行清理
            ON_BLOCK_EXIT([&] { _bufferSizeTracker.remove(batchSize); });
            
            // 执行实际的插入操作
            // inserter.run() 将批次中的文档插入到目标分片的集合中
            inserter.run(status);

            // 性能统计计算：定义辅助函数避免除零错误
            // 当时间为0时返回-1表示无效值，避免程序崩溃
            const auto checkDivByZero = [](auto divisor, auto expression) {
                return divisor == 0 ? -1 : expression();
            };
            
            // 计算吞吐量的辅助函数
            // 吞吐量以MB/s为单位，用于性能监控和调优
            const auto calcThroughput = [&](auto bytes, auto duration) {
                return checkDivByZero(durationCount<Microseconds>(duration), [&]() {
                    return static_cast<double>(bytes) / durationCount<Microseconds>(duration);
                });
            };

            // 计算各种性能指标
            const auto insertTime = insertTimer.elapsed();           // 插入时间
            const auto totalTime = totalTimer.elapsed();             // 总时间
            const auto batchThroughputMBps = calcThroughput(batchSize, totalTime);    // 总吞吐量
            const auto insertThroughputMBps = calcThroughput(batchSize, insertTime);  // 插入吞吐量
            const auto fetchThroughputMBps = calcThroughput(batchSize, fetchTime);    // 获取吞吐量

            // 记录详细的性能统计日志
            // 这些指标对于性能调优和问题诊断非常重要
            LOGV2_DEBUG(6718417,
                        1,
                        "Chunk migration initial clone apply batch",
                        "migrationId"_attr = migrationId,
                        "batchSize"_attr = batchSize,
                        "total"_attr = duration_cast<Milliseconds>(totalTime),
                        "totalThroughputMBps"_attr = batchThroughputMBps,
                        "fetch"_attr = duration_cast<Milliseconds>(fetchTime),
                        "fetchThroughputMBps"_attr = fetchThroughputMBps,
                        "insert"_attr = duration_cast<Milliseconds>(insertTime),
                        "insertThroughputMBps"_attr = insertThroughputMBps);
        });
    }
} catch (const DBException& e) {
    // 异常处理：当获取过程中发生异常时
    
    // 锁定内层客户端并终止操作
    // 这确保了在异常情况下能够及时停止相关操作
    ClientLock lk(_innerOpCtx->getClient());
    _innerOpCtx->getServiceContext()->killOperation(lk, _innerOpCtx, ErrorCodes::Error(6718400));
    
    // 记录获取数据失败的错误日志
    // 包含详细的错误信息，便于问题诊断和排查
    LOGV2_ERROR(6718413,
                "Chunk migration failure fetching data",
                "migrationId"_attr = _migrationId,
                "failure"_attr = e.toStatus());
}

template <typename Inserter>
MigrationBatchFetcher<Inserter>::~MigrationBatchFetcher() {
    LOGV2(6718401,
          "Shutting down and joining inserter threads for migration {migrationId}",
          "migrationId"_attr = _migrationId);

    // Call waitForIdle first since join can spawn another thread while ignoring the maxPoolSize
    // to finish the pending task. This is safe as long as ThreadPool::shutdown can't be
    // interleaved with this call.
    _inserterWorkers->waitForIdle();
    _inserterWorkers->shutdown();
    _inserterWorkers->join();

    LOGV2(6718415,
          "Inserter threads for migration {migrationId} joined",
          "migrationId"_attr = _migrationId);
}

template class MigrationBatchFetcher<MigrationBatchInserter>;

template class MigrationBatchFetcher<MigrationBatchMockInserter>;

}  // namespace mongo
