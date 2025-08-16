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

#include <memory>
#include <string>

#include "mongo/base/error_extra_info.h"
#include "mongo/bson/bsonelement.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/client.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/repl/optime.h"
#include "mongo/db/s/migration_batch_inserter.h"
#include "mongo/db/s/migration_batch_mock_inserter.h"
#include "mongo/db/s/migration_session_id.h"
#include "mongo/db/service_context.h"
#include "mongo/db/shard_id.h"
#include "mongo/db/write_concern_options.h"
#include "mongo/logv2/log.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/client/shard.h"
#include "mongo/s/grid.h"
#include "mongo/util/cancellation.h"
#include "mongo/util/concurrency/semaphore_ticketholder.h"
#include "mongo/util/concurrency/thread_pool.h"
#include "mongo/util/namespace_string_util.h"
#include "mongo/util/producer_consumer_queue.h"
#include "mongo/util/uuid.h"

#pragma once

namespace mongo {


// This class is only instantiated on the destination of a chunk migration and
// has a single purpose: to manage two thread pools, one
// on which threads perform inserters, and one on which
// threads run _migrateClone requests (to fetch batches of documents to insert).
//
// The constructor creates and starts the inserter thread pool.  The destructor shuts down
// and joins the inserter thread pool.
//
// The main work of the class is in method fetchAndScheduleInsertion.  That method
// starts a thread pool for fetchers.  Each thread in that thread pool sits in a loop
// sending out _migrateClone requests, blocking on the response, and scheduling an
// inserter on the inserter thread pool.  This function joins and shuts down the
// fetcher thread pool once all batches have been fetched.
//
// Inserter is templated only to allow a mock inserter to exist.
// There is only one implementation of inserter currently, which is MigrationBatchInserter.
//
// A few things to note:
//  - After fetchAndScheduleInsertion returns, insertions are still being executed (although fetches
//    are not).
//  - Sending out _migrateClone requests in parallel implies the need for synchronization on the
//    source.  See the comments in migration_chunk_cloner_source.h for details around
//    that.
//  - The requirement on source side synchronization implies that care must be taken on upgrade.
//    In particular, if the source is running an earlier binary that doesn't have code for
//    source side synchronization, it is unsafe to send _migrateClone requests in parallel.
//    To handle that case, when the source is prepared to service _migrateClone requests in
//    parallel, the field "parallelMigrateCloneSupported" is included in the "_recvChunkStart"
//    command.  The inclusion of that field indicates to the destination that it is safe
//    to send _migrateClone requests in parallel.  Its exclusion indicates that it is unsafe.
template <typename Inserter>
class MigrationBatchFetcher {
public:
    MigrationBatchFetcher(OperationContext* outerOpCtx,
                          OperationContext* innerOpCtx,
                          NamespaceString nss,
                          MigrationSessionId sessionId,
                          const WriteConcernOptions& writeConcern,
                          const ShardId& fromShardId,
                          const ChunkRange& range,
                          const UUID& migrationId,
                          const UUID& collectionId,
                          std::shared_ptr<MigrationCloningProgressSharedState> migrationInfo,
                          bool parallelFetchingSupported,
                          int maxBufferedSizeBytesPerThread);

    ~MigrationBatchFetcher();

    // Repeatedly fetch batches (using _migrateClone request) and schedule inserter jobs
    // on thread pool.
    void fetchAndScheduleInsertion();

    // Get inserter thread pool stats.
    ThreadPool::Stats getThreadPoolStats() const {
        return _inserterWorkers->getStats();
    }

    int getChunkMigrationConcurrency() const {
        return _chunkMigrationConcurrency;
    }

private:
    /**
     * Keeps track of memory usage and makes sure it won't exceed the limit.
     */
    class BufferSizeTracker {
    public:
        const static int kUnlimited{0};

        BufferSizeTracker(int maxSizeBytes) : _maxSizeBytes(maxSizeBytes) {}

        /**
         * If adding the given amount of bytes will go over the limit, wait until there's
         * enough space then add.
         */
        void waitUntilSpaceAvailableAndAdd(OperationContext* opCtx, int sizeBytes);

        /**
         * Subtracts the tracked bytes by the given amount.
         */
        void remove(int sizeBytes);

    private:
        Mutex _mutex = MONGO_MAKE_LATCH("MigrationBatchFetcher::BufferSizeTracker::_mutex");
        stdx::condition_variable _hasAvailableSpace;
       
        // 默认值 chunkMigrationFetcherMaxBufferedSizeBytesPerThread = 4 * BSONObjMaxInternalSize
        const int _maxSizeBytes;
        int _currentSize{0};
    };

    NamespaceString _nss;

    // Size of thread pools.
    int _chunkMigrationConcurrency;

    MigrationSessionId _sessionId;

    // Inserter thread pool.
    std::unique_ptr<ThreadPool> _inserterWorkers;

    BSONObj _migrateCloneRequest;

    OperationContext* _outerOpCtx;

    OperationContext* _innerOpCtx;

    std::shared_ptr<Shard> _fromShard;

    // Shared state, by which the progress of migration is communicated
    // to MigrationDestinationManager.
    std::shared_ptr<MigrationCloningProgressSharedState> _migrationProgress;

    ChunkRange _range;

    UUID _collectionUuid;

    UUID _migrationId;

    WriteConcernOptions _writeConcern;

    // Indicates if source is prepared to service _migrateClone requests in parallel.
    bool _isParallelFetchingSupported;

    SemaphoreTicketHolder _secondaryThrottleTicket;

    BufferSizeTracker _bufferSizeTracker;

    // Given session id and namespace, create migrateCloneRequest.
    // Only should be created once for the lifetime of the object.
    /**
     * _createMigrateCloneRequest 函数的作用：
     * 构建用于chunk迁移数据获取的_migrateClone命令请求对象。
     * 
     * 核心功能：
     * 1. 命令构建：创建标准的_migrateClone内部命令BSON对象
     * 2. 命名空间序列化：将目标集合的命名空间信息序列化到请求中
     * 3. 会话信息附加：添加迁移会话ID以确保请求的唯一性和可追踪性
     * 4. 请求标准化：确保请求格式符合MongoDB内部迁移协议规范
     * 
     * 设计特点：
     * - 轻量级构建：只包含最核心的必要字段，减少网络开销
     * - 会话关联：通过sessionId确保请求与特定迁移操作的关联
     * - 格式标准：使用标准的BSON构建器确保数据格式正确性
     * - 一次创建：在对象生命周期内只创建一次，避免重复构建开销
     * 
     * 请求内容：
     * - _migrateClone：命令标识符，指示这是一个迁移克隆请求
     * - 命名空间：序列化的目标集合命名空间信息
     * - 会话ID：唯一标识当前迁移会话的标识符
     * 
     * 使用场景：
     * - 在MigrationBatchFetcher构造时被调用一次
     * - 生成的请求对象在整个迁移过程中被重复使用
     * - 每次_fetchBatch调用都使用这个预构建的请求对象
     * 
     * 网络协议：
     * - 该请求会被发送到源分片的admin数据库
     * - 源分片接收后根据会话状态返回相应的数据批次
     * - 支持流式数据传输，每次返回一个批次直到完成
     * 
     * 性能考虑：
     * - 预构建避免了每次fetch时的重复构建开销
     * - 最小化请求大小以减少网络传输成本
     * - 使用高效的BSON构建器进行对象创建
     * 
     * 该函数是chunk迁移网络协议的基础，确保请求格式的正确性和一致性。
     * 
     * @return BSONObj 构建完成的_migrateClone命令请求对象
     */
    // Given session id and namespace, create migrateCloneRequest.
    // Only should be created once for the lifetime of the object.
    BSONObj _createMigrateCloneRequest() const {
        // 创建BSON对象构建器，用于逐步构建命令请求
        // BSONObjBuilder提供了类型安全的BSON文档构建接口
        BSONObjBuilder builder;
        
        // 添加_migrateClone命令标识符字段
        // 字段说明：
        // - "_migrateClone": MongoDB内部迁移克隆命令的标准名称
        // - 值为序列化的命名空间字符串：指定要迁移数据的目标集合
        // 
        // NamespaceStringUtil::serialize的作用：
        // - 将NamespaceString对象转换为标准的字符串格式
        // - 使用默认序列化上下文确保格式一致性
        // - 处理特殊字符和命名空间验证
        // 
        // 序列化格式通常为："database.collection"
        // 例如："myapp.users", "inventory.products"
        builder.append("_migrateClone",
                    NamespaceStringUtil::serialize(_nss, SerializationContext::stateDefault()));
        
        // 附加迁移会话ID到请求中
        // 会话ID的重要作用：
        // 1. 唯一性标识：确保每个迁移操作有唯一的会话标识
        // 2. 状态追踪：源分片通过会话ID追踪迁移进度和状态
        // 3. 幂等性保证：重复请求可以通过会话ID进行去重处理
        // 4. 错误恢复：异常情况下可以通过会话ID恢复迁移状态
        // 5. 并发控制：多个并发迁移通过不同会话ID进行隔离
        // 
        // append方法会将MigrationSessionId对象的内容序列化到BSON中
        // 通常包含会话UUID、事务号等关键信息
        // "sessionId":"shard2ReplSet_shard3ReplSet_689c943e517105ded4a54eee"
        _sessionId.append(&builder);
        
        // 构建并返回最终的BSON对象
        // obj()方法完成BSON文档的构建并返回不可变的BSONObj实例
        // 
        // 最终生成的请求格式示例：
        // {
        //   "_migrateClone": "mydb.mycollection",
        //   "sessionId": "sessionId":"shard2ReplSet_shard3ReplSet_689c943e517105ded4a54eee"
        // }
        // 
        // 注意：这里只包含基础字段，其他如迁移范围(min/max)、
        // 集合UUID、迁移ID等信息在实际使用时可能会在
        // _fetchBatch方法中动态添加或通过其他方式传递
        return builder.obj();
    }

    void _runFetcher();

    // Fetches next batch using _migrateClone request and return it.  May return an empty batch.
    BSONObj _fetchBatch(OperationContext* opCtx);

    static bool _isEmptyBatch(const BSONObj& batch) {
        return batch.getField("objects").Obj().isEmpty();
    }

    static void onCreateThread(const std::string& threadName) {
        Client::initThread(threadName,
                           getGlobalServiceContext()->getService(ClusterRole::ShardServer));
    }

};  // namespace mongo

}  // namespace mongo
