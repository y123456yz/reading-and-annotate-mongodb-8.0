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

stdx::unordered_map<NamespaceString, CollectionDataSizeInfoForBalancing>
getDataSizeInfoForCollections(OperationContext* opCtx,
                              const std::vector<CollectionType>& collections) {
    const auto balancerConfig = Grid::get(opCtx)->getBalancerConfiguration();
    uassertStatusOK(balancerConfig->refreshAndCheck(opCtx));

    const auto shardRegistry = Grid::get(opCtx)->shardRegistry();
    const auto shardIds = shardRegistry->getAllShardIds(opCtx);

    // Map to be returned, incrementally populated with the collected statistics
    stdx::unordered_map<NamespaceString, CollectionDataSizeInfoForBalancing> dataSizeInfoMap;

    std::vector<NamespaceWithOptionalUUID> namespacesWithUUIDsForStatsRequest;
    for (const auto& coll : collections) {
        const auto& nss = coll.getNss();
        const auto maxChunkSizeBytes =
            coll.getMaxChunkSizeBytes().value_or(balancerConfig->getMaxChunkSizeBytes());

        dataSizeInfoMap.emplace(
            nss,
            CollectionDataSizeInfoForBalancing(std::map<ShardId, int64_t>(), maxChunkSizeBytes));

        NamespaceWithOptionalUUID nssWithUUID(nss);
        nssWithUUID.setUUID(coll.getUuid());
        namespacesWithUUIDsForStatsRequest.push_back(nssWithUUID);
    }

    auto namespaceToShardDataSize =
        getStatsForBalancing(opCtx, shardIds, namespacesWithUUIDsForStatsRequest);
    for (auto& [ns, shardDataSizeMap] : namespaceToShardDataSize) {
        tassert(8245201, "Namespace not found", dataSizeInfoMap.contains(ns));
        dataSizeInfoMap.at(ns).shardToDataSizeMap = std::move(shardDataSizeMap);
    }
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

StatusWith<SplitInfoVector> BalancerChunkSelectionPolicy::selectChunksToSplit(
    OperationContext* opCtx) {
    auto shardStatsStatus = _clusterStats->getStats(opCtx);
    if (!shardStatsStatus.isOK()) {
        return shardStatsStatus.getStatus();
    }

    const auto& shardStats = shardStatsStatus.getValue();

    const auto catalogClient = ShardingCatalogManager::get(opCtx)->localCatalogClient();

    auto collections = catalogClient->getShardedCollections(
        opCtx, DatabaseName::kEmpty, repl::ReadConcernLevel::kMajorityReadConcern, {});
    if (collections.empty()) {
        return SplitInfoVector{};
    }

    SplitInfoVector splitCandidates;

    auto client = opCtx->getClient();
    std::shuffle(collections.begin(), collections.end(), client->getPrng().urbg());

    for (const auto& coll : collections) {
        const NamespaceString& nss(coll.getNss());

        auto candidatesStatus = _getSplitCandidatesForCollection(opCtx, nss, shardStats);
        if (candidatesStatus == ErrorCodes::NamespaceNotFound) {
            // Namespace got dropped before we managed to get to it, so just skip it
            continue;
        } else if (!candidatesStatus.isOK()) {
            LOGV2_WARNING(21852,
                          "Unable to enforce zone range policy for collection",
                          logAttrs(nss),
                          "error"_attr = candidatesStatus.getStatus());

            continue;
        }

        splitCandidates.insert(splitCandidates.end(),
                               std::make_move_iterator(candidatesStatus.getValue().begin()),
                               std::make_move_iterator(candidatesStatus.getValue().end()));
    }

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
            if (migrateCandidates.empty()) {
                imbalancedCollectionsCachePtr->erase(nss);
            } else if (imbalancedCollectionsCachePtr->size() < kMaxCachedCollectionsSize) {
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

        collBatch.push_back(imbalancedColl.value());
        ++imbalancedNssIt;

        // 从集合列表中移除，避免重复处理
        collections.erase(collIt);
    }

    // 随机遍历剩余集合，批量收集迁移候选 chunk
    auto client = opCtx->getClient();
    std::shuffle(collections.begin(), collections.end(), client->getPrng().urbg());
    for (const auto& coll : collections) {

        if (balancer_policy_utils::canBalanceCollection(coll)) {
            collBatch.push_back(coll);
        }

        // 达到批次大小则处理
        if (collBatch.size() == kStatsForBalancingBatchSize) {
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
        LOGV2_WARNING(4562401,
                      "Ignoring zones for the internal sessions collection.",
                      "nss"_attr = NamespaceString::kLogicalSessionsNamespace,
                      "zones"_attr = zoneInfo.allZones());
    } else {
        getSplitCandidatesToEnforceZoneRanges(cm, zoneInfo, &splitCandidates);
    }

    return splitCandidates.done();
}

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
