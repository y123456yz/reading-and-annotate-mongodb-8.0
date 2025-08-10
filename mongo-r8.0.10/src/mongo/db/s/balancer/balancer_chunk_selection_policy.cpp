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

#include <absl/container/node_hash_map.h>
#include <absl/container/node_hash_set.h>
#include <algorithm>
#include <boost/cstdint.hpp>
#include <boost/move/utility_core.hpp>
#include <boost/none.hpp>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <fmt/format.h>
#include <iterator>
#include <map>
#include <set>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>

#include "mongo/base/data_range.h"
#include "mongo/base/error_codes.h"
#include "mongo/base/status_with.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonelement.h"
#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobj_comparator_interface.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/simple_bsonobj_comparator.h"
#include "mongo/db/client.h"
#include "mongo/db/database_name.h"
#include "mongo/db/keypattern.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/repl/read_concern_level.h"
#include "mongo/db/s/balancer/balancer_chunk_selection_policy.h"
#include "mongo/db/s/config/sharding_catalog_manager.h"
#include "mongo/db/s/sharding_config_server_parameters_gen.h"
#include "mongo/db/s/sharding_util.h"
#include "mongo/executor/remote_command_response.h"
#include "mongo/executor/task_executor_pool.h"
#include "mongo/idl/idl_parser.h"
#include "mongo/logv2/log.h"
#include "mongo/logv2/log_attr.h"
#include "mongo/logv2/log_component.h"
#include "mongo/logv2/redaction.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/platform/random.h"
#include "mongo/s/async_requests_sender.h"
#include "mongo/s/balancer_configuration.h"
#include "mongo/s/catalog/sharding_catalog_client.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/catalog/type_collection.h"
#include "mongo/s/catalog/type_collection_gen.h"
#include "mongo/s/catalog_cache.h"
#include "mongo/s/chunk.h"
#include "mongo/s/chunk_manager.h"
#include "mongo/s/chunk_version.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/grid.h"
#include "mongo/s/request_types/get_stats_for_balancing_gen.h"
#include "mongo/s/routing_information_cache.h"
#include "mongo/s/shard_key_pattern.h"
#include "mongo/stdx/unordered_map.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/duration.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/str.h"
#include "mongo/util/timer.h"
#include "mongo/util/uuid.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kSharding

MONGO_FAIL_POINT_DEFINE(overrideStatsForBalancingBatchSize);

namespace mongo {

namespace {

/**
 * Does a linear pass over the information cached in the specified chunk manager and extracts chunk
 * distribution and chunk placement information which is needed by the balancer policy.
 */
StatusWith<DistributionStatus> createCollectionDistributionStatus(
    OperationContext* opCtx,
    const NamespaceString& nss,
    const ShardStatisticsVector& allShards,
    const ChunkManager& chunkMgr) {

    auto swZoneInfo =
        ZoneInfo::getZonesForCollection(opCtx, nss, chunkMgr.getShardKeyPattern().getKeyPattern());
    if (!swZoneInfo.isOK()) {
        return swZoneInfo.getStatus();
    }

    return {DistributionStatus{nss, std::move(swZoneInfo.getValue()), chunkMgr}};
}

/**
 * getDataSizeInfoForCollections 函数的作用：
 * 批量获取多个分片集合的数据大小统计信息，用于负载均衡决策。
 * 
 * 核心功能：
 * 1. 数据大小收集：从所有分片收集指定集合的数据大小统计信息
 * 2. 批量处理优化：通过单次网络请求获取多个集合的统计信息，提高效率
 * 3. 配置参数处理：为每个集合确定最大chunk大小限制
 * 4. 统计信息映射：构建集合命名空间到分片数据大小映射的完整结构
 * 5. 负载均衡支持：为均衡器策略提供基础数据支撑
 * 
 * 处理流程：
 * - 获取均衡器配置，确定全局和集合级别的chunk大小限制
 * - 构建批量统计请求，包含所有目标集合的命名空间和UUID
 * - 向所有分片发送统计请求，收集每个分片上各集合的数据大小
 * - 整合统计结果，构建完整的数据大小映射结构
 * 
 * 性能优化：
 * - 批量网络请求：避免为每个集合单独发送请求
 * - 并行收集：同时从多个分片收集统计信息
 * - 结构化缓存：提供易于查询的数据结构给上层调用者
 * 
 * 参数说明：
 * @param opCtx 操作上下文，提供事务和中断支持
 * @param collections 待统计的集合元数据列表
 * 
 * 返回值：
 * @return 集合命名空间到CollectionDataSizeInfoForBalancing的映射
 *         包含每个集合在各分片的数据大小和最大chunk大小配置
 * 
 * 该函数是负载均衡数据收集的核心组件，为后续的chunk迁移决策提供准确的数据基础。
 */
stdx::unordered_map<NamespaceString, CollectionDataSizeInfoForBalancing>
getDataSizeInfoForCollections(OperationContext* opCtx,
                              const std::vector<CollectionType>& collections) {
    // 获取负载均衡器配置：用于确定全局chunk大小限制和其他均衡参数
    // 这个配置包含了集群级别的均衡策略设置
    const auto balancerConfig = Grid::get(opCtx)->getBalancerConfiguration();
    uassertStatusOK(balancerConfig->refreshAndCheck(opCtx));

    // 获取分片注册表：用于获取集群中所有活跃分片的ID列表
    // 统计请求需要发送到所有分片以获得完整的数据大小信息
    const auto shardRegistry = Grid::get(opCtx)->shardRegistry();
    const auto shardIds = shardRegistry->getAllShardIds(opCtx);

    // Map to be returned, incrementally populated with the collected statistics
    // 初始化返回映射：逐步填充收集到的统计信息
    // Key: NamespaceString（集合命名空间）
    // Value: CollectionDataSizeInfoForBalancing（包含分片数据大小和chunk配置）
    stdx::unordered_map<NamespaceString, CollectionDataSizeInfoForBalancing> dataSizeInfoMap;

    // 构建统计请求的命名空间列表：为批量统计请求准备数据
    // 包含每个集合的命名空间和UUID，确保请求的准确性
    std::vector<NamespaceWithOptionalUUID> namespacesWithUUIDsForStatsRequest;
    for (const auto& coll : collections) {
        const auto& nss = coll.getNss();
        
        // 确定集合的最大chunk大小：优先使用集合级配置，否则使用全局默认值
        // 这个大小限制会影响chunk拆分和迁移的决策
        const auto maxChunkSizeBytes =
            coll.getMaxChunkSizeBytes().value_or(balancerConfig->getMaxChunkSizeBytes());

        // 初始化集合的数据大小信息结构：
        // - 空的分片到数据大小映射（后续填充）
        // - 确定的最大chunk大小限制
        dataSizeInfoMap.emplace(
            nss,
            CollectionDataSizeInfoForBalancing(std::map<ShardId, int64_t>(), maxChunkSizeBytes));

        // 构建带UUID的命名空间：确保统计请求的唯一性和准确性
        // UUID避免了因集合删除重建导致的统计数据混淆
        NamespaceWithOptionalUUID nssWithUUID(nss);
        nssWithUUID.setUUID(coll.getUuid());
        namespacesWithUUIDsForStatsRequest.push_back(nssWithUUID);
    }

    // 批量获取统计信息：向所有分片发送统计请求，收集各集合的数据大小
    // 这是一个网络密集型操作，通过批量请求优化性能
    // 返回结果格式：NamespaceString -> (ShardId -> 数据大小)
    auto namespaceToShardDataSize =
        getStatsForBalancing(opCtx, shardIds, namespacesWithUUIDsForStatsRequest);
    
    // 整合统计结果：将收集到的分片数据大小填充到返回映射中
    for (auto& [ns, shardDataSizeMap] : namespaceToShardDataSize) {
        // 验证集合存在性：确保统计结果对应的集合在我们的处理列表中
        // 这是一个安全性检查，防止意外的数据不一致
        tassert(8245201, "Namespace not found", dataSizeInfoMap.contains(ns));
        
        // 填充分片数据大小映射：将从各分片收集到的数据大小信息
        // 移动到最终的返回结构中，避免不必要的数据复制
        dataSizeInfoMap.at(ns).shardToDataSizeMap = std::move(shardDataSizeMap);
    }
    
    // 返回完整的集合数据大小信息映射：
    // 包含每个集合在各分片的数据大小统计和chunk大小配置
    // 供负载均衡器用于制定迁移决策
    return dataSizeInfoMap;
}

CollectionDataSizeInfoForBalancing getDataSizeInfoForCollection(OperationContext* opCtx,
                                                                const NamespaceString& nss) {
    const auto catalogClient = ShardingCatalogManager::get(opCtx)->localCatalogClient();
    const auto coll = catalogClient->getCollection(opCtx, nss);
    std::vector<CollectionType> vec{coll};
    return std::move(getDataSizeInfoForCollections(opCtx, vec).at(nss));
}

/**
 * Helper class used to accumulate the split points for the same chunk together so they can be
 * submitted to the shard as a single call versus multiple. This is necessary in order to avoid
 * refreshing the chunk metadata after every single split point (if done one by one), because
 * splitting a chunk does not yield the same chunk anymore.
 */
class SplitCandidatesBuffer {
    SplitCandidatesBuffer(const SplitCandidatesBuffer&) = delete;
    SplitCandidatesBuffer& operator=(const SplitCandidatesBuffer&) = delete;

public:
    SplitCandidatesBuffer(NamespaceString nss, ChunkVersion collectionPlacementVersion)
        : _nss(std::move(nss)),
          _collectionPlacementVersion(collectionPlacementVersion),
          _chunkSplitPoints(SimpleBSONObjComparator::kInstance.makeBSONObjIndexedMap<SplitInfo>()) {
    }

    /**
     * Adds the specified split point to the chunk. The split points must always be within the
     * boundaries of the chunk and must come in increasing order.
     */
    void addSplitPoint(const Chunk& chunk, const BSONObj& splitPoint) {
        auto it = _chunkSplitPoints.find(chunk.getMin());
        if (it == _chunkSplitPoints.end()) {
            _chunkSplitPoints.emplace(chunk.getMin(),
                                      SplitInfo(chunk.getShardId(),
                                                _nss,
                                                _collectionPlacementVersion,
                                                chunk.getLastmod(),
                                                chunk.getMin(),
                                                chunk.getMax(),
                                                {splitPoint}));
        } else if (splitPoint.woCompare(it->second.splitKeys.back()) > 0) {
            it->second.splitKeys.push_back(splitPoint);
        } else {
            // Split points must come in order
            tassert(8245202,
                    "Split points are out of order",
                    splitPoint.woCompare(it->second.splitKeys.back()) == 0);
        }
    }

    /**
     * May be called only once for the lifetime of the buffer. Moves the contents of the buffer into
     * a vector of split infos to be passed to the split call.
     */
    SplitInfoVector done() {
        SplitInfoVector splitPoints;
        for (auto& entry : _chunkSplitPoints) {
            splitPoints.push_back(std::move(entry.second));
        }

        return splitPoints;
    }

private:
    // Namespace and expected collection placement version
    const NamespaceString _nss;
    const ChunkVersion _collectionPlacementVersion;

    // Chunk min key and split vector associated with that chunk
    BSONObjIndexedMap<SplitInfo> _chunkSplitPoints;
};

/**
 * Populates splitCandidates with chunk and splitPoint pairs for chunks that violate zone
 * range boundaries.
 */
void getSplitCandidatesToEnforceZoneRanges(const ChunkManager& cm,
                                           const ZoneInfo& zoneInfo,
                                           SplitCandidatesBuffer* splitCandidates) {
    const auto& globalMax = cm.getShardKeyPattern().getKeyPattern().globalMax();

    // For each zone range, find chunks that need to be split.
    for (const auto& zoneRangeEntry : zoneInfo.zoneRanges()) {
        const auto& zoneRange = zoneRangeEntry.second;

        const auto chunkAtZoneMin = cm.findIntersectingChunkWithSimpleCollation(zoneRange.min);
        tassert(8245203,
                "Chunk's max is smaller than zone's min",
                chunkAtZoneMin.getMax().woCompare(zoneRange.min) > 0);

        if (chunkAtZoneMin.getMin().woCompare(zoneRange.min)) {
            splitCandidates->addSplitPoint(chunkAtZoneMin, zoneRange.min);
        }

        // The global max key can never fall in the middle of a chunk.
        if (!zoneRange.max.woCompare(globalMax))
            continue;

        const auto chunkAtZoneMax = cm.findIntersectingChunkWithSimpleCollation(zoneRange.max);

        // We need to check that both the chunk's minKey does not match the zone's max and also that
        // the max is not equal, which would only happen in the case of the zone ending in MaxKey.
        if (chunkAtZoneMax.getMin().woCompare(zoneRange.max) &&
            chunkAtZoneMax.getMax().woCompare(zoneRange.max)) {
            splitCandidates->addSplitPoint(chunkAtZoneMax, zoneRange.max);
        }
    }
}

}  // namespace

bool balancer_policy_utils::canBalanceCollection(const CollectionType& coll) {
    if (!coll.getAllowBalance() || !coll.getAllowMigrations() || !coll.getPermitMigrations() ||
        coll.getDefragmentCollection()) {
        LOGV2_DEBUG(5966401,
                    1,
                    "Not balancing explicitly disabled collection",
                    logAttrs(coll.getNss()),
                    "allowBalance"_attr = coll.getAllowBalance(),
                    "allowMigrations"_attr = coll.getAllowMigrations(),
                    "permitMigrations"_attr = coll.getPermitMigrations(),
                    "defragmentCollection"_attr = coll.getDefragmentCollection(),
                    "unsplittable"_attr = coll.getUnsplittable());
        return false;
    }
    return true;
}

BalancerChunkSelectionPolicy::BalancerChunkSelectionPolicy(ClusterStatistics* clusterStats)
    : _clusterStats(clusterStats) {}

/**
 * BalancerChunkSelectionPolicy::selectChunksToSplit 的作用：
 * 扫描集群中所有分片集合，识别并选择需要拆分的 chunk，主要解决 zone（分区）边界违规问题。
 * 这是 Balancer 数据均衡前的关键预处理步骤，确保所有 chunk 边界与 zone 配置严格对齐。
 * 
 * 核心功能：
 * 1. 全集合扫描：遍历集群中所有分片集合，进行全面的 zone 合规性检查
 * 2. Zone 边界检测：识别 chunk 边界与 zone 边界不对齐的违规情况
 * 3. 拆分点计算：为违规 chunk 计算精确的拆分点，确保拆分后符合 zone 约束
 * 4. 错误隔离处理：单个集合处理失败不影响其他集合的拆分候选选择
 * 5. 随机化处理：随机遍历集合顺序，避免总是优先处理相同集合
 * 6. 批量优化：将同一 chunk 的多个拆分点合并，减少元数据刷新开销
 * 
 * 应用场景：
 * - Zone sharding：确保数据严格按地理区域或硬件分区分布
 * - Tag-aware balancing：保证带标签的 chunk 分布符合标签约束规则
 * - 动态 zone 调整：当管理员修改 zone 配置后，自动调整 chunk 边界
 * - 合规性维护：持续监控并修复因各种操作导致的 zone 边界违规
 * 
 * 执行时机：
 * - 在每轮 balancing round 开始前执行，作为数据迁移的前置条件
 * - 当检测到 zone 配置变更时触发，确保新配置得到执行
 * - 管理员手动触发集群重平衡时的第一步操作
 * 
 * @param opCtx 操作上下文，提供事务控制、中断处理和资源访问
 * @return StatusWith<SplitInfoVector> 拆分候选信息向量，包含所有需要拆分的 chunk 及其拆分点
 * 
 * 该函数是 MongoDB 分片集群维护 zone 合规性的核心入口，为后续的 chunk 均衡迁移提供合规的基础。
 * 
* // 对于普通数据不均衡表，candidatesStatus.getValue() 通常为空向量， 
  // 因此对于普通不均衡表，splitCandidates返回空
 */
StatusWith<SplitInfoVector> BalancerChunkSelectionPolicy::selectChunksToSplit(
    OperationContext* opCtx) {
    
    // ========== 第一阶段：获取集群统计信息 ==========
    // 获取集群中所有分片的统计信息，包括分片状态、可用性等基础数据
    // 这些信息用于后续的 chunk 分布分析和拆分决策
    auto shardStatsStatus = _clusterStats->getStats(opCtx);
    if (!shardStatsStatus.isOK()) {
        // 如果无法获取分片统计信息（如网络问题、分片不可达等），直接返回错误
        // 因为没有准确的集群状态信息，无法进行可靠的拆分决策
        return shardStatsStatus.getStatus();
    }

    // 提取分片统计信息，供后续处理使用
    const auto& shardStats = shardStatsStatus.getValue();

    // ========== 第二阶段：获取所有分片集合元数据 ==========
    // 获取 ShardingCatalogManager 的本地目录客户端，用于访问配置数据库
    const auto catalogClient = ShardingCatalogManager::get(opCtx)->localCatalogClient();

    // 从 config.collections 获取所有分片集合的元数据信息
    // 参数说明：
    // - DatabaseName::kEmpty: 获取所有数据库的集合
    // - kMajorityReadConcern: 使用多数读关注，确保读取到已提交的数据
    // - {}: 空的查询过滤器，获取所有分片集合
    auto collections = catalogClient->getShardedCollections(
        opCtx, DatabaseName::kEmpty, repl::ReadConcernLevel::kMajorityReadConcern, {});
    if (collections.empty()) {
        // 如果集群中没有分片集合，则不需要进行任何拆分操作
        // 返回空的拆分候选向量
        return SplitInfoVector{};
    }

    // ========== 第三阶段：初始化拆分候选收集器 ==========
    // 初始化拆分候选信息向量，用于收集所有需要拆分的 chunk 信息
    SplitInfoVector splitCandidates;

    // ========== 第四阶段：随机化集合处理顺序 ==========
    // 获取当前操作的客户端对象，用于访问伪随机数生成器
    auto client = opCtx->getClient();
    // 随机打乱集合处理顺序，避免总是优先处理字典序靠前的集合
    // 这种随机化有助于：
    // 1. 均匀分布处理负载，避免某些集合总是优先处理
    // 2. 减少因处理顺序固定导致的性能热点
    // 3. 提高系统的整体公平性和均衡性
    std::shuffle(collections.begin(), collections.end(), client->getPrng().urbg());

    // ========== 第五阶段：逐个处理分片集合 ==========
    // 遍历所有分片集合，为每个集合检查并生成拆分候选
    for (const auto& coll : collections) {
        // 提取集合的命名空间（数据库名.集合名）
        const NamespaceString& nss(coll.getNss());

        // ========== 子步骤 5.1：获取集合的拆分候选 ==========
        // 调用内部函数，分析单个集合的 zone 合规性并生成拆分候选
        // 该函数会：
        // 1. 获取集合的最新路由信息和 zone 配置
        // 2. 检查 chunk 边界与 zone 边界的对齐情况
        // 3. 计算违规 chunk 的拆分点
        // 4. 返回该集合的所有拆分候选信息
        auto candidatesStatus = _getSplitCandidatesForCollection(opCtx, nss, shardStats);
        
        // ========== 子步骤 5.2：处理集合不存在的情况 ==========
        if (candidatesStatus == ErrorCodes::NamespaceNotFound) {
            // 集合在处理过程中被删除（并发删除操作）
            // 这是正常情况，直接跳过该集合继续处理下一个
            // 不需要记录错误日志，因为集合删除是合法的管理操作
            continue;
        } else if (!candidatesStatus.isOK()) {
            // ========== 子步骤 5.3：处理其他错误情况 ==========
            // 处理集合时发生其他错误（如网络问题、权限问题、数据损坏等）
            // 记录警告日志，但不终止整个拆分候选选择流程
            // 这种错误隔离策略确保部分集合的问题不会影响其他集合的处理
            LOGV2_WARNING(21852,
                          "Unable to enforce zone range policy for collection",
                          logAttrs(nss),                           // 集合命名空间信息
                          "error"_attr = candidatesStatus.getStatus());  // 具体错误详情

            // 继续处理下一个集合，不因单个集合失败而终止
            continue;
        }

        // 对于普通数据不均衡表，candidatesStatus.getValue() 通常为空向量， 
        // 因此对于普通不均衡表，splitCandidates返回空

        // ========== 子步骤 5.4：收集成功的拆分候选 ==========
        // 将当前集合的拆分候选信息追加到总的拆分候选向量中
        // 使用 move iterator 优化性能，避免不必要的数据复制
        // insert + make_move_iterator 的组合实现了高效的批量移动插入
        splitCandidates.insert(splitCandidates.end(),
                               std::make_move_iterator(candidatesStatus.getValue().begin()),
                               std::make_move_iterator(candidatesStatus.getValue().end()));
    }

    // ========== 第六阶段：返回所有拆分候选 ==========
    // 返回收集到的所有拆分候选信息
    // 每个 SplitInfo 包含：
    // - 目标分片 ID
    // - 集合命名空间
    // - 集合版本信息
    // - chunk 的当前边界（min, max）
    // - 计算出的拆分点数组
    // 调用方（通常是 Balancer::_splitChunksIfNeeded）将使用这些信息执行实际的 chunk 拆分操作
    return splitCandidates;
}

StatusWith<SplitInfoVector> BalancerChunkSelectionPolicy::selectChunksToSplit(
    OperationContext* opCtx, const NamespaceString& nss) {

    auto shardStatsStatus = _clusterStats->getStats(opCtx);
    if (!shardStatsStatus.isOK()) {
        return shardStatsStatus.getStatus();
    }

    const auto& shardStats = shardStatsStatus.getValue();

    return _getSplitCandidatesForCollection(opCtx, nss, shardStats);
}

/**
 * 选择本轮需要迁移的 chunk，生成对应的迁移任务（MigrateInfoVector），以实现分片集群的负载均衡。
 * 
 * 主要流程：
 * 1. 检查可用分片数，若不足2则无需迁移，直接返回空结果。
 * 2. 获取所有分片集合元数据（config.collections），如无集合则直接返回。
 * 3. 批量处理集合，提升统计和迁移候选选择效率：
 *    - 优先处理缓存中已知不均衡的集合（imbalancedCollectionsCachePtr）。
 *    - 随机遍历剩余集合，批量收集需要迁移的 chunk。
 *    - 每批集合统计数据后，调用 _getMigrateCandidatesForCollection 选出迁移候选 chunk。
 *    - 若集合无迁移候选，则从缓存移除；否则缓存集合名以便后续快速处理。
 * 4. 若超时或可用分片数不足，则提前返回已选迁移候选。
 * 5. 返回所有本轮选出的 chunk 迁移任务。
 */
StatusWith<MigrateInfoVector> BalancerChunkSelectionPolicy::selectChunksToMove(
    OperationContext* opCtx,
    const std::vector<ClusterStatistics::ShardStatistics>& shardStats,
    stdx::unordered_set<ShardId>* availableShards,
    stdx::unordered_set<NamespaceString>* imbalancedCollectionsCachePtr) {

    // 可用分片数不足2，无需迁移
    if (availableShards->size() < 2) {
        return MigrateInfoVector{};
    }

    Timer chunksSelectionTimer;

    // 获取所有分片集合元数据
    const auto catalogClient = ShardingCatalogManager::get(opCtx)->localCatalogClient();
    auto collections =
        catalogClient->getShardedCollections(opCtx,
                                             DatabaseName::kEmpty,
                                             repl::ReadConcernLevel::kMajorityReadConcern,
                                             BSON(CollectionType::kNssFieldName << 1));
    if (collections.empty()) {
        return MigrateInfoVector{};
    }

    MigrateInfoVector candidateChunks;

    // 批次大小，可通过 failpoint 动态调整
    const uint32_t kStatsForBalancingBatchSize = [&]() {
        auto batchSize = 100U;
        overrideStatsForBalancingBatchSize.execute([&batchSize](const BSONObj& data) {
            batchSize = data["size"].numberInt();
            LOGV2(7617200, "Overriding collections batch size", "size"_attr = batchSize);
        });
        return batchSize;
    }();

    // 缓存集合最大数量，提升后续轮次效率
    const uint32_t kMaxCachedCollectionsSize = 0.75 * kStatsForBalancingBatchSize;

    // Lambda：根据集合命名空间查找集合元数据
    auto getCollectionTypeByNss = [&collections](const NamespaceString& nss)
        -> std::pair<boost::optional<CollectionType>, std::vector<CollectionType>::iterator> {
        const auto collIt =
            std::lower_bound(collections.begin(),
                             collections.end(),
                             nss,
                             [](const CollectionType& coll, const NamespaceString& ns) {
                                 return coll.getNss() < ns;
                             });

        if (collIt == collections.end() || collIt->getNss() != nss) {
            return std::make_pair(boost::none, collections.end());
        }
        return std::make_pair(*collIt, collIt);
    };

    // Lambda：批量处理集合，收集迁移候选 chunk
    const auto processBatch = [&](std::vector<CollectionType>& collBatch) {
        // 批量获取集合数据大小信息
        const auto collsDataSizeInfo = getDataSizeInfoForCollections(opCtx, collBatch);

        auto client = opCtx->getClient();
        // std::shuffle 是 C++ 标准库中用于随机重排容器元素的函数，需 C++11 或更高版本支持。它通过随机数生成器打乱序列顺序，相比早期 std::random_shuffle 提供了更可控的随机机制
        std::shuffle(collBatch.begin(), collBatch.end(), client->getPrng().urbg());
        for (const auto& coll : collBatch) {

            if (availableShards->size() < 2) {
                break;
            }

            const auto& nss = coll.getNss();

            // 选出集合的迁移候选 chunk
            auto swMigrateCandidates = _getMigrateCandidatesForCollection(
                opCtx, nss, shardStats, collsDataSizeInfo.at(nss), availableShards);
            if (swMigrateCandidates == ErrorCodes::NamespaceNotFound) {
                // 集合已被删除，移除缓存
                imbalancedCollectionsCachePtr->erase(nss);
                continue;
            } else if (!swMigrateCandidates.isOK()) {
                LOGV2_WARNING(21853,
                              "Unable to balance collection",
                              logAttrs(nss),
                              "error"_attr = swMigrateCandidates.getStatus());
                continue;
            }

            // 收集迁移候选 chunk
            candidateChunks.insert(
                candidateChunks.end(),
                std::make_move_iterator(swMigrateCandidates.getValue().first.begin()),
                std::make_move_iterator(swMigrateCandidates.getValue().first.end()));

            const auto& migrateCandidates = swMigrateCandidates.getValue().first;
            // 若无迁移候选，则移除缓存；否则缓存集合名
            // 把需要balance的表名加入缓存
            if (migrateCandidates.empty()) {
                // 说明该表没有不均衡的chunk了，可以从缓存中移除，避免下次继续处理该表
                imbalancedCollectionsCachePtr->erase(nss);
            } else if (imbalancedCollectionsCachePtr->size() < kMaxCachedCollectionsSize) {
                // 把该表缓存起来，下一次优先处理
                imbalancedCollectionsCachePtr->insert(nss);
            }
        }
    };

    // 批量处理集合
    std::vector<CollectionType> collBatch;

    // 优先处理缓存中已知不均衡的集合
    for (auto imbalancedNssIt = imbalancedCollectionsCachePtr->begin();
         imbalancedNssIt != imbalancedCollectionsCachePtr->end();) {

        const auto& [imbalancedColl, collIt] = getCollectionTypeByNss(*imbalancedNssIt);

        if (!imbalancedColl.has_value() ||
            !balancer_policy_utils::canBalanceCollection(imbalancedColl.value())) {
            // 集合已被删除或不允许均衡，移除缓存
            imbalancedCollectionsCachePtr->erase(imbalancedNssIt++);
            continue;
        }
        
        // 之前做过balance的表可以优先加入本次的处理批次
        collBatch.push_back(imbalancedColl.value());
        ++imbalancedNssIt;

        // 从集合列表中移除，避免重复处理
        collections.erase(collIt);
    }

    // 随机遍历剩余集合，批量收集迁移候选 chunk
    // 选出100个可 balance 的集合，这100个集合执行 processBatch 选出候选chunk, 每一批集合中挑选候选 chunk 的时间不超过 balancerChunksSelectionTimeoutMs 毫秒，避免挑选chunk耗时太长
    // 每一轮选100个表的候选chunk， 直到遍历完所有的表获取到所有的候选chunk
    auto client = opCtx->getClient();
    std::shuffle(collections.begin(), collections.end(), client->getPrng().urbg());
    for (const auto& coll : collections) {
        // 选出可 balance 的集合存入 collBatch 这个vector
        if (balancer_policy_utils::canBalanceCollection(coll)) {
            collBatch.push_back(coll);
        }

        // 达到批次大小则处理，默认 100 个表
        if (collBatch.size() == kStatsForBalancingBatchSize) {
            // 调用 Lambda 处理这批可balance的表
            processBatch(collBatch);
            if (availableShards->size() < 2) {
                return candidateChunks;
            }
            collBatch.clear();
        }

        // 超时提前返回已选迁移候选
        const auto maxTimeMs = balancerChunksSelectionTimeoutMs.load();
        if (candidateChunks.size() > 0 && chunksSelectionTimer.millis() > maxTimeMs) {
            LOGV2_DEBUG(
                7100900,
                1,
                "Exceeded max time while searching for candidate chunks to migrate in this round.",
                "maxTime"_attr = Milliseconds(maxTimeMs),
                "chunksSelectionTime"_attr = chunksSelectionTimer.elapsed(),
                "numCandidateChunks"_attr = candidateChunks.size());

            return candidateChunks;
        }
    }

    // 处理最后一批集合
    if (collBatch.size() > 0) {
        processBatch(collBatch);
    }

    // 返回所有本轮选出的 chunk 迁移任务
    return candidateChunks;
}

StatusWith<MigrateInfosWithReason> BalancerChunkSelectionPolicy::selectChunksToMove(
    OperationContext* opCtx, const NamespaceString& nss) {
    auto shardStatsStatus = _clusterStats->getStats(opCtx);
    if (!shardStatsStatus.isOK()) {
        return shardStatsStatus.getStatus();
    }

    const auto& shardStats = shardStatsStatus.getValue();

    // Used to check locally if the collection exists, it should trow NamespaceNotFound if it
    // doesn't.
    ShardingCatalogManager::get(opCtx)->localCatalogClient()->getCollection(opCtx, nss);

    stdx::unordered_set<ShardId> availableShards;
    std::transform(shardStats.begin(),
                   shardStats.end(),
                   std::inserter(availableShards, availableShards.end()),
                   [](const ClusterStatistics::ShardStatistics& shardStatistics) -> ShardId {
                       return shardStatistics.shardId;
                   });


    const auto dataSizeInfo = getDataSizeInfoForCollection(opCtx, nss);

    auto candidatesStatus =
        _getMigrateCandidatesForCollection(opCtx, nss, shardStats, dataSizeInfo, &availableShards);
    if (!candidatesStatus.isOK()) {
        return candidatesStatus.getStatus();
    }

    return candidatesStatus;
}

/**
 * 检查指定集合（nss）是否存在违反 zone（分区）边界的 chunk，并为这些 chunk 生成拆分候选点。
 * 主要流程如下：
 * 1. 获取集合的最新分片路由信息（RoutingInformationCache），确保操作基于最新元数据。
 * 2. 获取集合的 zone 信息（ZoneInfo），用于判断 chunk 是否跨 zone 边界。
 * 3. 遍历所有 zone 范围，查找 chunk 是否有边界不对齐的情况：
 *    - 如果 chunk 的 minKey 或 maxKey 与 zone 的边界不一致，则需要在该位置拆分 chunk。
 * 4. 对每个需要拆分的 chunk，收集拆分点，并将同一个 chunk 的所有拆分点合并，避免多次刷新元数据。
 * 5. 返回所有待拆分 chunk 及其对应的拆分点（SplitInfoVector），供后续分裂操作使用。
 * 6. 对于特殊集合（如 internal sessions collection），忽略 zone 配置并跳过处理。
 *
 * 该函数是分片均衡前 zone 边界强制对齐的核心入口，确保所有 chunk 的分布严格满足 zone 约束。
 * 
 *  *
 * 违反zone场景举例：
 * 假设原有 zone 配置为：
 *   zoneA: shard key 范围 [A, M)
 *   zoneB: shard key 范围 [M, Z)
 * 某 chunk 范围为 [L, N)，原本属于 zoneA。
 * 管理员将 zoneB 的起始范围从 M 改为 L，即 zoneB: [L, Z)。
 *   sh.updateZoneKeyRange("test.coll", { shardKey: "L" }, { shardKey: "Z" }, "zoneB")
 * 此时 chunk [L, N) 跨越了 zoneA 和 zoneB 的边界，属于 zone违规。
 * 
 * 
 * // 普通的数据不均衡表，不会触发zone违规，最终会返回空的拆分候选
 */
StatusWith<SplitInfoVector> BalancerChunkSelectionPolicy::_getSplitCandidatesForCollection(
    OperationContext* opCtx, const NamespaceString& nss, const ShardStatisticsVector& shardStats) {
    auto routingInfoStatus =
        RoutingInformationCache::get(opCtx)->getShardedCollectionRoutingInfoWithPlacementRefresh(
            opCtx, nss);
    if (!routingInfoStatus.isOK()) {
        return routingInfoStatus.getStatus();
    }

    const auto& [cm, _] = routingInfoStatus.getValue();

    auto swZoneInfo =
        ZoneInfo::getZonesForCollection(opCtx, nss, cm.getShardKeyPattern().getKeyPattern());
    if (!swZoneInfo.isOK()) {
        return swZoneInfo.getStatus();
    }

    const auto& zoneInfo = swZoneInfo.getValue();

    // Accumulate split points for the same chunk together
    SplitCandidatesBuffer splitCandidates(nss, cm.getVersion());

    if (nss == NamespaceString::kLogicalSessionsNamespace && !zoneInfo.allZones().empty()) {
        // 普通的数据不均衡表，不会触发zone违规，因此走这里，最终会返回空的拆分候选
        LOGV2_WARNING(4562401,
                      "Ignoring zones for the internal sessions collection.",
                      "nss"_attr = NamespaceString::kLogicalSessionsNamespace,
                      "zones"_attr = zoneInfo.allZones());
    } else {
        getSplitCandidatesToEnforceZoneRanges(cm, zoneInfo, &splitCandidates);
    }

    return splitCandidates.done();
}

/*
Balancer::_mainThread()
    ↓
BalancerChunkSelectionPolicy::selectChunksToMove()  ← 这里是实际的类名
    ↓
BalancerChunkSelectionPolicy::_getMigrateCandidatesForCollection()
    ↓
BalancerPolicy::balance()
*/
/**
 * 计算指定集合（nss）本轮需要迁移的 chunk，生成迁移候选任务（MigrateInfosWithReason）。
 * 主要流程：
 * 1. 获取集合最新路由信息，确保元数据一致。
 * 2. 构造集合分布状态（DistributionStatus），包含 zone 信息和 chunk 分布。
 * 3. 检查 zone 边界是否落在 chunk 中间，若有则返回错误并推迟均衡，需先拆分 chunk。
 * 4. 若所有 zone 边界合法，则调用 BalancerPolicy::balance 计算迁移候选 chunk。
 * 5. 返回本轮所有可迁移 chunk 及原因。
 */
StatusWith<MigrateInfosWithReason> BalancerChunkSelectionPolicy::_getMigrateCandidatesForCollection(
    OperationContext* opCtx,
    const NamespaceString& nss,
    const ShardStatisticsVector& shardStats,
    const CollectionDataSizeInfoForBalancing& collDataSizeInfo,
    stdx::unordered_set<ShardId>* availableShards) {
    // 获取集合最新路由信息，确保元数据一致
    auto routingInfoStatus =
        RoutingInformationCache::get(opCtx)->getShardedCollectionRoutingInfoWithPlacementRefresh(
            opCtx, nss);
    if (!routingInfoStatus.isOK()) {
        return routingInfoStatus.getStatus();
    }

    const auto& [cm, _] = routingInfoStatus.getValue();

    const auto& shardKeyPattern = cm.getShardKeyPattern().getKeyPattern();

    // 构造集合分布状态，包含 zone 信息和 chunk 分布
    const auto collInfoStatus = createCollectionDistributionStatus(opCtx, nss, shardStats, cm);
    if (!collInfoStatus.isOK()) {
        return collInfoStatus.getStatus();
    }
å
    const DistributionStatus& distribution = collInfoStatus.getValue();

    // 检查 zone 边界是否落在 chunk 中间，若有则返回错误并推迟均衡
    for (const auto& zoneRangeEntry : distribution.getZoneInfo().zoneRanges()) {
        const auto& zoneRange = zoneRangeEntry.second;

        const auto chunkAtZoneMin = cm.findIntersectingChunkWithSimpleCollation(zoneRange.min);

        if (chunkAtZoneMin.getMin().woCompare(zoneRange.min)) {
            return {ErrorCodes::IllegalOperation,
                    str::stream()
                        << "Zone boundaries " << zoneRange.toString()
                        << " fall in the middle of an existing chunk "
                        << ChunkRange(chunkAtZoneMin.getMin(), chunkAtZoneMin.getMax()).toString()
                        << ". Balancing for collection " << nss.toStringForErrorMsg()
                        << " will be postponed until the chunk is split appropriately."};
        }

        // The global max key can never fall in the middle of a chunk
        if (!zoneRange.max.woCompare(shardKeyPattern.globalMax()))
            continue;

        const auto chunkAtZoneMax = cm.findIntersectingChunkWithSimpleCollation(zoneRange.max);

        // We need to check that both the chunk's minKey does not match the zone's max and also that
        // the max is not equal, which would only happen in the case of the zone ending in MaxKey.
        if (chunkAtZoneMax.getMin().woCompare(zoneRange.max) &&
            chunkAtZoneMax.getMax().woCompare(zoneRange.max)) {
            return {ErrorCodes::IllegalOperation,
                    str::stream()
                        << "Zone boundaries " << zoneRange.toString()
                        << " fall in the middle of an existing chunk "
                        << ChunkRange(chunkAtZoneMax.getMin(), chunkAtZoneMax.getMax()).toString()
                        << ". Balancing for collection " << nss.toStringForErrorMsg()
                        << " will be postponed until the chunk is split appropriately."};
        }
    }

    // 调用 BalancerPolicy::balance 计算迁移候选 chunk
    return BalancerPolicy::balance(
        shardStats,
        distribution,
        collDataSizeInfo,
        availableShards,
        Grid::get(opCtx)->getBalancerConfiguration()->attemptToBalanceJumboChunks());
}

}  // namespace mongo
