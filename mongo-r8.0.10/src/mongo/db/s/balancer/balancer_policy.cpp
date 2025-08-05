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
#include <algorithm>
#include <cstdint>
#include <ctime>
#include <fmt/format.h>
#include <limits>
#include <memory>
#include <random>

#include <boost/move/utility_core.hpp>
#include <boost/none.hpp>
#include <boost/optional/optional.hpp>

#include "mongo/base/error_codes.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonelement.h"
#include "mongo/bson/simple_bsonobj_comparator.h"
#include "mongo/bson/util/builder.h"
#include "mongo/bson/util/builder_fwd.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/s/balancer/balancer_policy.h"
#include "mongo/db/s/config/sharding_catalog_manager.h"
#include "mongo/db/s/sharding_util.h"
#include "mongo/logv2/log.h"
#include "mongo/logv2/log_attr.h"
#include "mongo/logv2/log_component.h"
#include "mongo/logv2/redaction.h"
#include "mongo/platform/compiler.h"
#include "mongo/s/catalog/sharding_catalog_client.h"
#include "mongo/s/catalog/type_tags.h"
#include "mongo/s/grid.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/namespace_string_util.h"
#include "mongo/util/str.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kSharding


namespace mongo {

MONGO_FAIL_POINT_DEFINE(balancerShouldReturnRandomMigrations);

using std::numeric_limits;
using std::string;
using std::vector;
using namespace fmt::literals;

namespace {

ChunkType makeChunkType(const UUID& collUUID, const Chunk& chunk) {
    ChunkType ct{collUUID, chunk.getRange(), chunk.getLastmod(), chunk.getShardId()};
    ct.setJumbo(chunk.isJumbo());
    return ct;
}

/**
 * Return a vector of zones after they have been normalized according to the given chunk
 * configuration.
 *
 * If a zone covers only partially a chunk, boundaries of that zone will be shrank so that the
 * normalized zone won't overlap with that chunk. The boundaries of a normalized zone will never
 * fall in the middle of a chunk.
 *
 * Additionally the vector will contain also zones for the "NoZone",
 */
std::vector<ZoneRange> normalizeZones(const ChunkManager& cm, const ZoneInfo& zoneInfo) {
    std::vector<ZoneRange> normalizedRanges;

    auto lastMax = cm.getShardKeyPattern().getKeyPattern().globalMin();

    for (const auto& [max, zoneRange] : zoneInfo.zoneRanges()) {
        const auto& minChunk = cm.findIntersectingChunkWithSimpleCollation(zoneRange.min);
        const auto gtMin =
            SimpleBSONObjComparator::kInstance.evaluate(zoneRange.min > minChunk.getMin());
        const auto& normalizedMin = gtMin ? minChunk.getMax() : zoneRange.min;


        const auto& maxChunk = cm.findIntersectingChunkWithSimpleCollation(zoneRange.max);
        const auto gtMax =
            SimpleBSONObjComparator::kInstance.evaluate(zoneRange.max > maxChunk.getMin()) &&
            SimpleBSONObjComparator::kInstance.evaluate(
                zoneRange.max != cm.getShardKeyPattern().getKeyPattern().globalMax());
        const auto& normalizedMax = gtMax ? maxChunk.getMin() : zoneRange.max;


        if (SimpleBSONObjComparator::kInstance.evaluate(normalizedMin == normalizedMax)) {
            // This normalised zone has a length of zero, therefore can't contain any chunks so we
            // can ignore it
            continue;
        }

        if (SimpleBSONObjComparator::kInstance.evaluate(normalizedMin != lastMax)) {
            // The zone is not contiguous with the previous one so we add a kNoZoneRange
            // does not fully contain any chunk so we will ignore it
            normalizedRanges.emplace_back(lastMax, normalizedMin, ZoneInfo::kNoZoneName);
        }

        normalizedRanges.emplace_back(normalizedMin, normalizedMax, zoneRange.zone);
        lastMax = normalizedMax;
    }

    const auto& globalMaxKey = cm.getShardKeyPattern().getKeyPattern().globalMax();
    if (SimpleBSONObjComparator::kInstance.evaluate(lastMax != globalMaxKey)) {
        normalizedRanges.emplace_back(lastMax, globalMaxKey, ZoneInfo::kNoZoneName);
    }
    return normalizedRanges;
}

}  // namespace

DistributionStatus::DistributionStatus(NamespaceString nss,
                                       ZoneInfo zoneInfo,
                                       const ChunkManager& chunkMngr)
    : _nss(std::move(nss)), _zoneInfo(std::move(zoneInfo)), _chunkMngr(chunkMngr) {

    _normalizedZones = normalizeZones(_chunkMngr, _zoneInfo);

    for (size_t zoneRangeIdx = 0; zoneRangeIdx < _normalizedZones.size(); zoneRangeIdx++) {
        const auto& zoneRange = _normalizedZones[zoneRangeIdx];
        chunkMngr.forEachOverlappingChunk(
            zoneRange.min, zoneRange.max, false /* isMaxInclusive */, [&](const auto& chunkInfo) {
                auto [zoneIt, created] =
                    _shardZoneInfoMap[chunkInfo.getShardId().toString()].try_emplace(
                        zoneRange.zone, 1 /* numChunks */, zoneRangeIdx, chunkInfo.getMin());

                if (!created) {
                    ++(zoneIt->second.numChunks);
                }
                return true;
            });
    }
}

size_t DistributionStatus::numberOfChunksInShard(const ShardId& shardId) const {
    const auto shardZonesIt = _shardZoneInfoMap.find(shardId.toString());
    if (shardZonesIt == _shardZoneInfoMap.end()) {
        return 0;
    }
    size_t total = 0;
    for (const auto& [_, shardZoneInfo] : shardZonesIt->second) {
        total += shardZoneInfo.numChunks;
    }
    return total;
}

const StringMap<ShardZoneInfo>& DistributionStatus::getZoneInfoForShard(
    const ShardId& shardId) const {
    static const StringMap<ShardZoneInfo> emptyMap;
    const auto shardZonesIt = _shardZoneInfoMap.find(shardId.toString());
    if (shardZonesIt == _shardZoneInfoMap.end()) {
        return emptyMap;
    }
    return shardZonesIt->second;
}

const string ZoneInfo::kNoZoneName = "";

ZoneInfo::ZoneInfo()
    : _zoneRanges(SimpleBSONObjComparator::kInstance.makeBSONObjIndexedMap<ZoneRange>()) {}

Status ZoneInfo::addRangeToZone(const ZoneRange& range) {
    const auto minIntersect = _zoneRanges.upper_bound(range.min);
    const auto maxIntersect = _zoneRanges.upper_bound(range.max);

    // Check for partial overlap
    if (minIntersect != maxIntersect) {
        tassert(8245219, "minIntersect not found", minIntersect != _zoneRanges.end());
        const auto& intersectingRange =
            (SimpleBSONObjComparator::kInstance.evaluate(minIntersect->second.min < range.max))
            ? minIntersect->second
            : maxIntersect->second;

        if (SimpleBSONObjComparator::kInstance.evaluate(intersectingRange.min == range.min) &&
            SimpleBSONObjComparator::kInstance.evaluate(intersectingRange.max == range.max) &&
            intersectingRange.zone == range.zone) {
            return Status::OK();
        }

        return {ErrorCodes::RangeOverlapConflict,
                str::stream() << "Zone range: " << range.toString()
                              << " is overlapping with existing: " << intersectingRange.toString()};
    }

    // Check for containment
    if (minIntersect != _zoneRanges.end()) {
        const ZoneRange& nextRange = minIntersect->second;
        if (SimpleBSONObjComparator::kInstance.evaluate(range.max > nextRange.min)) {
            tassert(8245220,
                    "Range max is greater than or equal to next range's max",
                    SimpleBSONObjComparator::kInstance.evaluate(range.max < nextRange.max));
            return {ErrorCodes::RangeOverlapConflict,
                    str::stream() << "Zone range: " << range.toString()
                                  << " is overlapping with existing: " << nextRange.toString()};
        }
    }

    // This must be a new entry
    _zoneRanges.emplace(range.max.getOwned(), range);
    _allZones.insert(range.zone);
    return Status::OK();
}

string ZoneInfo::getZoneForRange(const ChunkRange& chunk) const {
    const auto minIntersect = _zoneRanges.upper_bound(chunk.getMin());
    const auto maxIntersect = _zoneRanges.lower_bound(chunk.getMax());

    // We should never have a partial overlap with a chunk range. If it happens, treat it as if this
    // chunk doesn't belong to a zone
    if (minIntersect != maxIntersect) {
        return ZoneInfo::kNoZoneName;
    }

    if (minIntersect == _zoneRanges.end()) {
        return ZoneInfo::kNoZoneName;
    }

    const ZoneRange& intersectRange = minIntersect->second;

    // Check for containment
    if (SimpleBSONObjComparator::kInstance.evaluate(intersectRange.min <= chunk.getMin()) &&
        SimpleBSONObjComparator::kInstance.evaluate(chunk.getMax() <= intersectRange.max)) {
        return intersectRange.zone;
    }

    return ZoneInfo::kNoZoneName;
}


StatusWith<ZoneInfo> ZoneInfo::getZonesForCollection(OperationContext* opCtx,
                                                     const NamespaceString& nss,
                                                     const KeyPattern& keyPattern) {
    const auto swCollectionZones =
        ShardingCatalogManager::get(opCtx)->localCatalogClient()->getTagsForCollection(opCtx, nss);
    if (!swCollectionZones.isOK()) {
        return swCollectionZones.getStatus().withContext(
            str::stream() << "Unable to load zones for collection " << nss.toStringForErrorMsg());
    }
    const auto& collectionZones = swCollectionZones.getValue();

    ZoneInfo zoneInfo;

    for (const auto& zone : collectionZones) {
        auto status =
            zoneInfo.addRangeToZone(ZoneRange(keyPattern.extendRangeBound(zone.getMinKey(), false),
                                              keyPattern.extendRangeBound(zone.getMaxKey(), false),
                                              zone.getTag()));

        if (!status.isOK()) {
            return status;
        }
    }

    return {std::move(zoneInfo)};
}

Status BalancerPolicy::isShardSuitableReceiver(const ClusterStatistics::ShardStatistics& stat,
                                               const string& chunkZone) {
    if (stat.isDraining) {
        return {ErrorCodes::IllegalOperation,
                str::stream() << stat.shardId << " is currently draining."};
    }

    if (chunkZone != ZoneInfo::kNoZoneName && !stat.shardZones.count(chunkZone)) {
        return {ErrorCodes::IllegalOperation,
                str::stream() << stat.shardId << " is not in the correct zone " << chunkZone};
    }

    return Status::OK();
}

/**
 * BalancerPolicy::_getLeastLoadedReceiverShard 的作用：
 * 在指定 zone 内查找数据量最小且适合作为迁移目标的分片，用于负载均衡中选择最佳的 chunk 接收方。
 * 
 * 核心逻辑：
 * 1. 遍历所有可用分片，检查分片是否适合作为接收方（非 draining 状态，符合 zone 约束）。
 * 2. 获取各分片在指定集合的数据大小统计信息。
 * 3. 比较各分片的数据量，选择数据量最小的分片作为最佳接收候选。
 * 4. 返回负载最轻的分片ID及其数据大小，供后续迁移决策使用。
 * 
 * 注意：该函数会进行 zone 约束和 draining 状态检查，确保选出的分片能安全接收数据。
 */
std::tuple<ShardId, int64_t> BalancerPolicy::_getLeastLoadedReceiverShard(
    const ShardStatisticsVector& shardStats,
    const CollectionDataSizeInfoForBalancing& collDataSizeInfo,
    const string& zone,
    const stdx::unordered_set<ShardId>& availableShards) {
    ShardId best;
    int64_t currentMin = numeric_limits<int64_t>::max();

    // 遍历所有分片统计信息，寻找数据量最小且适合的接收分片
    for (const auto& stat : shardStats) {
        // 跳过不在可用分片集合中的分片
        if (!availableShards.count(stat.shardId))
            continue;

        // 检查分片是否适合作为接收方（非 draining 状态，符合 zone 约束）
        auto status = isShardSuitableReceiver(stat, zone);
        if (!status.isOK()) {
            continue;
        }

        // 从集合数据大小信息中查找该分片的数据统计
        const auto& shardSizeIt = collDataSizeInfo.shardToDataSizeMap.find(stat.shardId);
        if (shardSizeIt == collDataSizeInfo.shardToDataSizeMap.end()) {
            // Skip if stats not available (may happen if add|remove shard during a round)
            continue;
        }

        // 获取该分片的数据大小，并与当前最小值比较
        const auto shardSize = shardSizeIt->second;
        if (shardSize < currentMin) {
            best = stat.shardId;        // 更新负载最轻的分片ID
            currentMin = shardSize;     // 更新当前最小数据量
        }
    }

    // 返回负载最轻的分片ID及其数据大小
    return {best, currentMin};
}

/**
 * BalancerPolicy::_getMostOverloadedShard 的作用：
 * 在指定 zone 内查找数据量最大（过载最严重）的分片，用于负载均衡中选择需要减负的源分片。
 * 
 * 核心逻辑：
 * 1. 遍历所有可用分片，获取各分片在指定集合的数据大小统计信息。
 * 2. 比较各分片的数据量，选择数据量最大的分片作为候选。
 * 3. 返回过载最严重的分片ID及其数据大小，供后续迁移决策使用。
 * 
 * 注意：该函数不考虑 zone 约束检查，只是单纯基于数据量大小选择分片。
 */
std::tuple<ShardId, int64_t> BalancerPolicy::_getMostOverloadedShard(
    const ShardStatisticsVector& shardStats,
    const CollectionDataSizeInfoForBalancing& collDataSizeInfo,
    const string& chunkZone,
    const stdx::unordered_set<ShardId>& availableShards) {
    ShardId worst;
    long long currentMax = numeric_limits<long long>::min();

    // 遍历所有分片统计信息，寻找数据量最大的分片
    for (const auto& stat : shardStats) {
        // 跳过不在可用分片集合中的分片
        if (!availableShards.count(stat.shardId))
            continue;

        // 从集合数据大小信息中查找该分片的数据统计
        const auto& shardSizeIt = collDataSizeInfo.shardToDataSizeMap.find(stat.shardId);
        if (shardSizeIt == collDataSizeInfo.shardToDataSizeMap.end()) {
            // Skip if stats not available (may happen if add|remove shard during a round)
            continue;
        }

        // 获取该分片的数据大小，并与当前最大值比较
        const auto shardSize = shardSizeIt->second;
        if (shardSize > currentMax) {
            worst = stat.shardId;        // 更新过载最严重的分片ID
            currentMax = shardSize;      // 更新当前最大数据量
        }
    }

    // 返回过载最严重的分片ID及其数据大小
    return {worst, currentMax};
}

// Returns a random integer in [0, max) using a uniform random distribution.
int getRandomIndex(int max) {
    std::default_random_engine gen(time(nullptr));
    std::uniform_int_distribution<int> dist(0, max - 1);

    return dist(gen);
}

// Returns a randomly chosen pair of source -> destination shards for testing.
boost::optional<MigrateInfo> chooseRandomMigration(
    const ShardStatisticsVector& shardStats,
    const stdx::unordered_set<ShardId>& availableShards,
    const DistributionStatus& distribution) {

    if (availableShards.size() < 2) {
        return boost::none;
    }

    // Do not perform random migrations if there is a shard that is draining to avoid starving
    // them of eligible shards to migrate to.
    auto drainingShardIter = std::find_if(
        shardStats.begin(), shardStats.end(), [](const auto& stat) { return stat.isDraining; });

    if (drainingShardIter != shardStats.end()) {
        return boost::none;
    }

    std::vector<ShardId> shards;
    std::copy(availableShards.begin(), availableShards.end(), std::back_inserter(shards));
    std::default_random_engine rng(time(nullptr));
    std::shuffle(shards.begin(), shards.end(), rng);

    // Get a random shard with chunks as the donor shard and another random shard as the recipient
    boost::optional<ShardId> donorShard;
    boost::optional<ShardId> recipientShard;
    for (auto i = 0U; i < shards.size(); ++i) {
        if (distribution.numberOfChunksInShard(shards[i]) != 0) {
            donorShard = shards[i];

            if (i == shards.size() - 1) {
                recipientShard = shards[0];
            } else {
                recipientShard = shards[i + 1];
            }
            break;
        }
    }

    if (!donorShard) {
        return boost::none;
    }
    tassert(8245221, "Recipient is invalid", recipientShard);

    LOGV2_DEBUG(21880,
                1,
                "balancerShouldReturnRandomMigrations",
                "fromShardId"_attr = donorShard.get(),
                "toShardId"_attr = recipientShard.get());

    const auto& randomChunk = [&] {
        const auto numChunksOnDonorShard = distribution.numberOfChunksInShard(donorShard.get());
        const auto rndChunkIdx = getRandomIndex(numChunksOnDonorShard);
        ChunkType rndChunk;

        int idx{0};
        distribution.getChunkManager().forEachChunk([&](const auto& chunk) {
            if (chunk.getShardId() == donorShard.get() && idx++ == rndChunkIdx) {
                rndChunk = makeChunkType(distribution.getChunkManager().getUUID(), chunk);
                return false;
            }
            return true;
        });

        invariant(rndChunk.getShard().isValid());
        return rndChunk;
    }();

    tassert(8245222, "randomChunk's shard is invalid", randomChunk.getShard().isValid());

    return MigrateInfo{
        recipientShard.get(), distribution.nss(), randomChunk, ForceJumbo::kDoNotForce};
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
// BalancerChunkSelectionPolicyImpl::selectChunksToMove 调用
// 返回需要迁移的 chunk 列表和迁移原因
MigrateInfosWithReason BalancerPolicy::balance(
    const ShardStatisticsVector& shardStats,
    const DistributionStatus& distribution,
    const CollectionDataSizeInfoForBalancing& collDataSizeInfo,
    stdx::unordered_set<ShardId>* availableShards,
    bool forceJumbo) {
    vector<MigrateInfo> migrations;
    MigrationReason firstReason = MigrationReason::none;

    // 1) Check for shards, which are in draining mode
    {
        // 遍历所有分片，优先处理 draining 状态分片上的 chunk
        for (const auto& stat : shardStats) {
            if (!stat.isDraining)
                continue;

            if (!availableShards->count(stat.shardId))
                continue;

            // Now we know we need to move chunks off this shard, but only if permitted by the
            // tags policy
            unsigned numJumboChunks = 0;
            
            /*
            例如没有启用addShardTag和addTagRange,对应shardId zoneinfo如下:
            "shard0001" => {
                "" => ShardZoneInfo{
                    numChunks: 5,           // 该分片上的chunk数量
                    firstNormalizedZoneIdx: 0,  // 指向_normalizedZones[0],说明没有指定shardtag tagRange
                    firstChunkMinKey: {userId: MinKey}  // 第一个chunk的最小键
                }
            },
            */
            const auto& shardZones = distribution.getZoneInfoForShard(stat.shardId);
            for (const auto& shardZone : shardZones) {
                const auto& zoneName = shardZone.first;

                // 遍历该分片该zone下的所有chunk，优先迁移非jumbo chunk
                const auto chunkFoundForShard = !distribution.forEachChunkOnShardInZone(
                    stat.shardId, zoneName, [&](const auto& chunk) {
                        if (chunk.isJumbo()) {
                            numJumboChunks++;
                            return true;  // continue
                        }

                        // 选择负载最轻的目标分片，也就是数据最少的分片
                        const auto [to, _] = _getLeastLoadedReceiverShard(
                            shardStats, collDataSizeInfo, zoneName, *availableShards);
                        if (!to.isValid()) {
                            if (migrations.empty()) {
                                LOGV2_DEBUG(21889,
                                            3,
                                            "Chunk is on a draining shard, but no appropriate "
                                            "recipient found",
                                            "chunk"_attr = redact(
                                                makeChunkType(
                                                    distribution.getChunkManager().getUUID(), chunk)
                                                    .toString()));
                            }
                            return true;  // continue
                        }
                        tassert(
                            8245225, "Destination shard is a draining shard", to != stat.shardId);

                        // 生成迁移任务，强制迁移jumbo chunk
                        migrations.emplace_back(
                            to,
                            chunk.getShardId(),
                            distribution.nss(),
                            distribution.getChunkManager().getUUID(),
                            chunk.getMin(),
                            boost::none /* max */,
                            chunk.getLastmod(),
                            // Always force jumbo chunks to be migrated off draining shards
                            ForceJumbo::kForceBalancer,
                            collDataSizeInfo.maxChunkSizeBytes);

                        if (firstReason == MigrationReason::none) {
                            firstReason = MigrationReason::drain;
                        }

                        // 从可用分片集合中移除已迁移的分片，避免重复迁移
                        tassert(8245226,
                                "Migration's source shard does not exist in available shards",
                                availableShards->erase(stat.shardId));
                        tassert(8245227,
                                "Migration's target shard does not exist in available shards",
                                availableShards->erase(to));
                        return false;  // break
                    });

                if (chunkFoundForShard) {
                    break;
                }
            }

            if (migrations.empty()) {
                LOGV2_WARNING(21890,
                              "Unable to find any chunk to move from draining shard",
                              "shardId"_attr = stat.shardId,
                              "numJumboChunks"_attr = numJumboChunks);
            }

            // 可用分片数不足2时提前返回
            if (availableShards->size() < 2) {
                return std::make_pair(std::move(migrations), firstReason);
            }
        }
    }

    // Select random migrations after checking for draining shards so tests with removeShard or
    // transitionToDedicatedConfigServer can eventually drain shards.
    // NOTE: randomly chosen migrations do not respect zones.
    // 2) 支持 failpoint 测试场景下的随机迁移（不考虑 zone）
    if (MONGO_unlikely(balancerShouldReturnRandomMigrations.shouldFail()) &&
        !distribution.nss().isConfigDB()) {
        LOGV2_DEBUG(21881, 1, "balancerShouldReturnRandomMigrations failpoint is set");

        // 随机选择迁移任务用于测试
        auto migration = chooseRandomMigration(shardStats, *availableShards, distribution);

        if (migration) {
            migrations.push_back(migration.get());
            firstReason = MigrationReason::chunksImbalance;

            // 从可用分片集合中移除已迁移的分片，避免重复迁移
            tassert(8245223,
                    "Migration's from shard does not exist in available shards",
                    availableShards->erase(migration.get().from));
            tassert(8245224,
                    "Migration's to shard does not exist in available shards",
                    availableShards->erase(migration.get().to));

            return std::make_pair(std::move(migrations), firstReason);
        }
    }

    // 3) 检查 zone 约束，迁移落在错误分片上的 chunk，使其分布满足 zone 规则
    if (!distribution.zones().empty()) {
        for (const auto& stat : shardStats) {

            if (!availableShards->count(stat.shardId))
                continue;

            const auto& shardZones = distribution.getZoneInfoForShard(stat.shardId);
            for (const auto& shardZone : shardZones) {
                const auto& zoneName = shardZone.first;

                if (zoneName == ZoneInfo::kNoZoneName)
                    continue;

                if (stat.shardZones.count(zoneName))
                    continue;

                const auto chunkFoundForShard = !distribution.forEachChunkOnShardInZone(
                    stat.shardId, zoneName, [&](const auto& chunk) {
                        if (chunk.isJumbo()) {
                            LOGV2_WARNING(
                                21891,
                                "Chunk violates zone, but it is jumbo and cannot be moved",
                                "chunk"_attr = redact(
                                    makeChunkType(distribution.getChunkManager().getUUID(), chunk)
                                        .toString()),
                                "zone"_attr = redact(zoneName));
                            return true;  // continue
                        }

                        const auto [to, _] = _getLeastLoadedReceiverShard(
                            shardStats, collDataSizeInfo, zoneName, *availableShards);
                        if (!to.isValid()) {
                            if (migrations.empty()) {
                                LOGV2_DEBUG(
                                    21892,
                                    3,
                                    "Chunk violates zone, but no appropriate recipient found",
                                    "chunk"_attr =
                                        redact(makeChunkType(
                                                   distribution.getChunkManager().getUUID(), chunk)
                                                   .toString()),
                                    "zone"_attr = redact(zoneName));
                            }
                            return true;  // continue
                        }
                        tassert(8245228, "Destination is the starting shard", to != stat.shardId);

                        migrations.emplace_back(to,
                                                chunk.getShardId(),
                                                distribution.nss(),
                                                distribution.getChunkManager().getUUID(),
                                                chunk.getMin(),
                                                boost::none /* max */,
                                                chunk.getLastmod(),
                                                forceJumbo ? ForceJumbo::kForceBalancer
                                                           : ForceJumbo::kDoNotForce,
                                                collDataSizeInfo.maxChunkSizeBytes);

                        if (firstReason == MigrationReason::none) {
                            firstReason = MigrationReason::zoneViolation;
                        }

                        tassert(8245229,
                                "Migration's from shard does not exist in available shards",
                                availableShards->erase(stat.shardId));
                        tassert(8245230,
                                "Migration's to shard does not exist in available shards",
                                availableShards->erase(to));
                        return false;  // break
                    });

                if (chunkFoundForShard) {
                    break;
                }
            }
            if (availableShards->size() < 2) {
                return std::make_pair(std::move(migrations), firstReason);
            }
        }
    }

    // 4) 按 zone 逐步均衡数据量，迁移 chunk 以实现 zone 内负载均衡
    vector<string> zonesPlusEmpty(distribution.zones().begin(), distribution.zones().end());
    zonesPlusEmpty.push_back(ZoneInfo::kNoZoneName);
    
    for (const auto& zone : zonesPlusEmpty) {
        size_t numShardsInZone = 0;
        int64_t totalDataSizeOfShardsWithZone = 0;
    
        // 统计当前 zone 内分片数量和总数据量
        for (const auto& stat : shardStats) {
            // 检查分片是否属于当前 zone
            // 如果是 kNoZoneName（无zone），则所有分片都算在内
            // 如果是具体 zone，则只统计属于该 zone 的分片
            if (zone == ZoneInfo::kNoZoneName || stat.shardZones.count(zone)) {
                // 从集合数据大小信息中查找该分片的数据统计
                const auto& shardSizeIt = collDataSizeInfo.shardToDataSizeMap.find(stat.shardId);
                if (shardSizeIt == collDataSizeInfo.shardToDataSizeMap.end()) {
                    // Skip if stats not available (may happen if add|remove shard during a round)
                    // 跳过统计信息不可用的分片（可能在均衡轮次中添加或移除分片）
                    continue;
                }
                // 累加该 zone 内所有分片的数据量
                totalDataSizeOfShardsWithZone += shardSizeIt->second;
                // 累计该 zone 内的分片数量
                numShardsInZone++;
            }
        }
    
        // Skip zones which have no shards assigned to them. This situation is not harmful, but
        // should not be possible so warn the operator to correct it.
        // 跳过没有分配分片的 zone。这种情况无害，但不应该发生，因此警告操作员纠正它
        if (numShardsInZone == 0) {
            if (zone != ZoneInfo::kNoZoneName) {
                LOGV2_WARNING(21893,
                              "Zone in collection has no assigned shards and chunks which fall "
                              "into it cannot be balanced. This should be corrected by either "
                              "assigning shards to the zone or by deleting it.",
                              "zone"_attr = redact(zone),
                              logAttrs(distribution.nss()));
            }
            continue; // 跳过这个 zone，处理下一个
        }
    
        // 断言：确保 zone 内的总数据量不为负数
        tassert(ErrorCodes::BadValue,
                str::stream() << "Total data size for shards in zone " << zone << " and collection "
                              << distribution.nss().toStringForErrorMsg()
                              << " must be greater or equal than zero but is "
                              << totalDataSizeOfShardsWithZone,
                totalDataSizeOfShardsWithZone >= 0);
    
        if (totalDataSizeOfShardsWithZone == 0) {
            // No data to balance within this zone
            // 该 zone 内没有数据需要均衡，跳过
            continue;
        }
    
        // 计算该 zone 内每个分片的理想数据量
        // 理想数据量 = zone 内总数据量 / zone 内分片数量
        const int64_t idealDataSizePerShardForZone =
            totalDataSizeOfShardsWithZone / numShardsInZone;
    
        // 在该 zone 内重复执行单轮均衡，直到无法找到更多迁移候选
        // _singleZoneBalanceBasedOnDataSize 返回 true 表示找到了一个迁移任务
        // 返回 false 表示该 zone 已经均衡或无法找到合适的迁移候选
        while (_singleZoneBalanceBasedOnDataSize(shardStats,
                                                 distribution,
                                                 collDataSizeInfo,
                                                 zone,
                                                 idealDataSizePerShardForZone,
                                                 &migrations,
                                                 availableShards,
                                                 forceJumbo ? ForceJumbo::kForceBalancer
                                                            : ForceJumbo::kDoNotForce)) {
            // 如果这是第一个迁移任务，设置迁移原因为 chunksImbalance
            if (firstReason == MigrationReason::none) {
                firstReason = MigrationReason::chunksImbalance;
            }
            // 继续下一轮均衡，直到该 zone 内无法找到更多迁移候选
        }
    }

    return std::make_pair(std::move(migrations), firstReason);
}

/**
 * BalancerPolicy::_singleZoneBalanceBasedOnDataSize 的作用：
 * 在指定 zone 内基于数据大小进行单轮负载均衡，通过迁移 chunk 使分片间数据分布更加均衡。
 * 
 * 核心逻辑：
 * 1. 找出该 zone 内数据量最大的分片（donor）和数据量最小的分片（recipient）。
 * 2. 判断是否满足迁移条件：donor 数据量超过理想值，recipient 数据量低于理想值，且两者差距足够大。
 * 3. 从 donor 分片选择一个非 jumbo chunk 迁移到 recipient 分片。
 * 4. 生成迁移任务并更新可用分片集合，避免重复迁移。
 * 5. 返回是否成功找到并生成了迁移任务。
 */
bool BalancerPolicy::_singleZoneBalanceBasedOnDataSize(
    const ShardStatisticsVector& shardStats,
    const DistributionStatus& distribution,
    const CollectionDataSizeInfoForBalancing& collDataSizeInfo,
    const string& zone,
    const int64_t idealDataSizePerShardForZone,
    vector<MigrateInfo>* migrations,
    stdx::unordered_set<ShardId>* availableShards,
    ForceJumbo forceJumbo) {
    
    // 找出该 zone 内数据量最大的分片作为 donor
    const auto [from, fromSize] =
        _getMostOverloadedShard(shardStats, collDataSizeInfo, zone, *availableShards);
    if (!from.isValid())
        return false;

    // 找出该 zone 内数据量最小且可接收数据的分片作为 recipient
    const auto [to, toSize] =
        _getLeastLoadedReceiverShard(shardStats, collDataSizeInfo, zone, *availableShards);
    if (!to.isValid()) {
        if (migrations->empty()) {
            LOGV2(6581600, "No available shards to take chunks for zone", "zone"_attr = zone);
        }
        return false;
    }

    // 源分片和目标分片相同，无需迁移
    if (from == to) {
        return false;
    }

    //{"t":{"$date":"2025-07-29T19:42:13.844+08:00"},"s":"D1", "c":"SHARDING", "id":7548100, "svc":"S", "ctx":"Balancer","msg":"Balancing single zone","attr":{"namespace":"config.system.sessions","zone":"","idealDataSizePerShardForZone":330,"fromShardId":"shard1ReplSet","fromShardDataSize":990,"toShardId":"shard2ReplSet","toShardDataSize":0,"maxChunkSizeBytes":200000}}
    LOGV2_DEBUG(7548100,
                1,
                "Balancing single zone",
                logAttrs(distribution.nss()),
                "zone"_attr = zone,
                "idealDataSizePerShardForZone"_attr = idealDataSizePerShardForZone,
                "fromShardId"_attr = from,
                "fromShardDataSize"_attr = fromSize,
                "toShardId"_attr = to,
                "toShardDataSize"_attr = toSize,
                "maxChunkSizeBytes"_attr = collDataSizeInfo.maxChunkSizeBytes);

    // 源分片数据量未超过理想值，无需迁移
    if (fromSize <= idealDataSizePerShardForZone) {
        return false;
    }

    // 目标分片数据量已达到或超过理想值，不适合作为接收方
    if (toSize >= idealDataSizePerShardForZone) {
        // Do not use a shard if it already has more data than the ideal per-shard size
        return false;
    }

    // 两个分片间数据量差距不够大（小于3个chunk大小），不值得迁移
    if (fromSize - toSize < 3 * collDataSizeInfo.maxChunkSizeBytes) {
        // Do not balance if the collection's size differs too few between the chosen shards
        return false;
    }


    const auto& fromShardId = from;
    const auto& toShardId = to;

    unsigned numJumboChunks = 0;

    // 遍历源分片在该 zone 下的所有 chunk，寻找可迁移的非 jumbo chunk
    //  找到一个可迁移的 chunk 即可，跳出循环
    const auto chunkFound =
        !distribution.forEachChunkOnShardInZone(fromShardId, zone, [&](const auto& chunk) {
            if (chunk.isJumbo()) {
                numJumboChunks++;
                return true;  // continue - jumbo chunk 无法迁移，继续寻找下一个
            }

            // 生成迁移任务：将该 chunk 从源分片迁移到目标分片
            migrations->emplace_back(toShardId,
                                     chunk.getShardId(),
                                     distribution.nss(),
                                     distribution.getChunkManager().getUUID(),
                                     chunk.getMin(),
                                     boost::none /* max */,
                                     chunk.getLastmod(),
                                     forceJumbo,
                                     collDataSizeInfo.maxChunkSizeBytes);
            
            // 从可用分片集合中移除源分片和目标分片，避免本轮重复迁移
            tassert(8245231,
                    "Source shard does not exist in available shards",
                    availableShards->erase(chunk.getShardId()));
            tassert(8245232,
                    "Target shard does not exist in available shards",
                    availableShards->erase(toShardId));
            return false;  // break - 找到一个可迁移的 chunk 即可，跳出循环
        });

    // 如果源分片只有 jumbo chunk 且无法找到可迁移的 chunk，记录警告
    if (!chunkFound && numJumboChunks) {
        LOGV2_WARNING(6581602,
                      "Shard has only jumbo chunks for this collection and cannot be balanced",
                      logAttrs(distribution.nss()),
                      "shardId"_attr = from,
                      "zone"_attr = zone,
                      "numJumboChunks"_attr = numJumboChunks);
    }

    // 返回是否成功找到并生成了迁移任务
    return chunkFound;
}

ZoneRange::ZoneRange(const BSONObj& a_min, const BSONObj& a_max, const std::string& _zone)
    : min(a_min.getOwned()), max(a_max.getOwned()), zone(_zone) {}

string ZoneRange::toString() const {
    return str::stream() << min << " -->> " << max << "  on  " << zone;
}

MigrateInfo::MigrateInfo(const ShardId& a_to,
                         const NamespaceString& a_nss,
                         const ChunkType& a_chunk,
                         const ForceJumbo a_forceJumbo,
                         boost::optional<int64_t> maxChunkSizeBytes)
    : nss(a_nss), uuid(a_chunk.getCollectionUUID()) {
    tassert(8245233, "Target shard is invalid", a_to.isValid());

    to = a_to;

    from = a_chunk.getShard();
    minKey = a_chunk.getMin();
    maxKey = a_chunk.getMax();
    version = a_chunk.getVersion();
    forceJumbo = a_forceJumbo;
    optMaxChunkSizeBytes = maxChunkSizeBytes;
}

MigrateInfo::MigrateInfo(const ShardId& a_to,
                         const ShardId& a_from,
                         const NamespaceString& a_nss,
                         const UUID& a_uuid,
                         const BSONObj& a_min,
                         const boost::optional<BSONObj>& a_max,
                         const ChunkVersion& a_version,
                         const ForceJumbo a_forceJumbo,
                         boost::optional<int64_t> maxChunkSizeBytes)
    : nss(a_nss),
      uuid(a_uuid),
      minKey(a_min),
      maxKey(a_max),
      version(a_version),
      forceJumbo(a_forceJumbo),
      optMaxChunkSizeBytes(maxChunkSizeBytes) {
    tassert(8245234, "Target shard is invalid", a_to.isValid());
    tassert(8245235, "Source shard is invalid", a_from.isValid());

    to = a_to;
    from = a_from;
}

std::string MigrateInfo::getName() const {
    // Generates a unique name for a MigrateInfo based on the namespace and the lower bound of the
    // chunk being moved.
    StringBuilder buf;
    buf << uuid << "-";

    BSONObjIterator i(minKey);
    while (i.more()) {
        BSONElement e = i.next();
        buf << e.fieldName() << "_" << e.toString(false, true);
    }

    return buf.str();
}

string MigrateInfo::toString() const {
    return str::stream() << uuid << ": [" << minKey << ", " << maxKey << "), from " << from
                         << ", to " << to;
}

boost::optional<int64_t> MigrateInfo::getMaxChunkSizeBytes() const {
    return optMaxChunkSizeBytes;
}

SplitInfo::SplitInfo(const ShardId& inShardId,
                     const NamespaceString& inNss,
                     const ChunkVersion& inCollectionPlacementVersion,
                     const ChunkVersion& inChunkVersion,
                     const BSONObj& inMinKey,
                     const BSONObj& inMaxKey,
                     std::vector<BSONObj> inSplitKeys)
    : shardId(inShardId),
      nss(inNss),
      collectionPlacementVersion(inCollectionPlacementVersion),
      chunkVersion(inChunkVersion),
      minKey(inMinKey),
      maxKey(inMaxKey),
      splitKeys(std::move(inSplitKeys)) {}

std::string SplitInfo::toString() const {
    StringBuilder splitKeysBuilder;
    for (const auto& splitKey : splitKeys) {
        splitKeysBuilder << splitKey.toString() << ", ";
    }

    return "Splitting chunk in {} [ {}, {} ), residing on {} at [ {} ] with version {} and collection placement version {}"_format(
        toStringForLogging(nss),
        minKey.toString(),
        maxKey.toString(),
        shardId.toString(),
        splitKeysBuilder.str(),
        chunkVersion.toString(),
        collectionPlacementVersion.toString());
}

MergeInfo::MergeInfo(const ShardId& shardId,
                     const NamespaceString& nss,
                     const UUID& uuid,
                     const ChunkVersion& collectionPlacementVersion,
                     const ChunkRange& chunkRange)
    : shardId(shardId),
      nss(nss),
      uuid(uuid),
      collectionPlacementVersion(collectionPlacementVersion),
      chunkRange(chunkRange) {}

std::string MergeInfo::toString() const {
    return "Merging chunk range {} in {} residing on {} with collection placement version {}"_format(
        chunkRange.toString(),
        NamespaceStringUtil::serialize(nss, SerializationContext::stateDefault()),
        shardId.toString(),
        collectionPlacementVersion.toString());
}

MergeAllChunksOnShardInfo::MergeAllChunksOnShardInfo(const ShardId& shardId,
                                                     const NamespaceString& nss)
    : shardId(shardId), nss(nss) {}

std::string MergeAllChunksOnShardInfo::toString() const {
    return "Merging all contiguous chunks residing on shard {} for collection {}"_format(
        shardId.toString(),
        NamespaceStringUtil::serialize(nss, SerializationContext::stateDefault()));
}

DataSizeInfo::DataSizeInfo(const ShardId& shardId,
                           const NamespaceString& nss,
                           const UUID& uuid,
                           const ChunkRange& chunkRange,
                           const ShardVersion& version,
                           const KeyPattern& keyPattern,
                           bool estimatedValue,
                           int64_t maxSize)
    : shardId(shardId),
      nss(nss),
      uuid(uuid),
      chunkRange(chunkRange),
      version(version),
      keyPattern(keyPattern),
      estimatedValue(estimatedValue),
      maxSize(maxSize) {}


/*
// 完整的调用链：
BalancerChunkSelectionPolicy::selectChunksToMove()
    ↓
getDataSizeInfoForCollections()  // 批量获取多个集合的统计信息
    ↓
getStatsForBalancing()  // ← 当前函数，从分片收集统计数据
    ↓
sharding_util::sendCommandToShards()  // 并发网络通信

// 函数返回的嵌套映射结构示例：
NamespaceStringToShardDataSizeMap result = {
    "myapp.users" => {
        "shard0001" => 1073741824,  // 1GB
        "shard0002" => 536870912,   // 512MB  
        "shard0003" => 2147483648   // 2GB
    },
    "myapp.orders" => {
        "shard0001" => 268435456,   // 256MB
        "shard0002" => 805306368,   // 768MB
        "shard0003" => 134217728    // 128MB
    },
    "myapp.products" => {
        "shard0001" => 134217728,   // 128MB
        "shard0002" => 402653184,   // 384MB
        "shard0003" => 67108864     // 64MB
    }
};
*/

/**
 * getStatsForBalancing 函数的作用：
 * 从分片集群中的所有指定分片收集多个集合的数据大小统计信息，用于负载均衡决策。
 * 
 * 核心功能：
 * 1. 批量统计收集：通过单次网络请求从每个分片获取多个集合的数据统计信息
 * 2. 并行网络通信：同时向所有目标分片发送统计请求，提高收集效率
 * 3. 数据结构构建：将收集到的统计信息组织成嵌套映射结构便于查询
 * 4. 异常处理机制：处理分片不可用、网络错误等异常情况
 * 5. 负载均衡支持：为BalancerPolicy提供准确的数据分布信息
 * 
 * 处理流程：
 * - 构建ShardsvrGetStatsForBalancing请求，包含所有目标集合的命名空间和UUID
 * - 并行向所有指定分片发送统计请求
 * - 收集和解析各分片的响应数据
 * - 构建集合命名空间到分片数据大小的完整映射
 * - 处理分片移除等异常情况，确保系统稳定性
 * 
 * 性能优化：
 * - 批量请求：避免为每个集合单独发送网络请求
 * - 并行通信：同时从多个分片收集统计信息
 * - 结构化存储：提供高效的嵌套映射查询接口
 * 
 * 容错机制：
 * - 跳过已移除的分片，支持动态分片管理
 * - 处理网络超时和连接错误
 * - 验证响应数据的完整性和一致性
 * 
 * 参数说明：
 * @param opCtx 操作上下文，提供事务和中断支持
 * @param shardIds 目标分片ID列表，指定从哪些分片收集统计信息
 * @param namespacesWithUUIDsForStatsRequest 待统计的集合列表，包含命名空间和UUID
 * 
 * 返回值：
 * @return NamespaceStringToShardDataSizeMap 嵌套映射结构
 *         外层Key: NamespaceString（集合命名空间）
 *         内层Key: ShardId（分片标识）
 *         Value: int64_t（该分片上该集合的数据大小，单位字节）
 * 
 * 该函数是MongoDB负载均衡器数据收集的核心组件，为chunk迁移决策提供数据基础。
 */
NamespaceStringToShardDataSizeMap getStatsForBalancing(
    OperationContext* opCtx,
    const std::vector<ShardId>& shardIds,
    const std::vector<NamespaceWithOptionalUUID>& namespacesWithUUIDsForStatsRequest) {

    // 构建统计请求命令：创建 _shardsvrGetStatsForBalancing 请求对象
    // 该请求包含所有需要统计的集合命名空间和UUID信息
    ShardsvrGetStatsForBalancing req{namespacesWithUUIDsForStatsRequest};
    req.setScaleFactor(1);  // 设置比例因子为1，获取实际数据大小
    const auto reqObj = req.toBSON({});  // 序列化为BSON格式用于网络传输

    // 获取执行器：用于并行网络通信的线程池执行器
    // FixedExecutor提供固定大小的线程池，适合并发网络操作
    const auto executor = Grid::get(opCtx)->getExecutorPool()->getFixedExecutor();
    
    // 并行发送统计请求：同时向所有指定分片发送统计命令
    // 关键性能优化：避免串行请求导致的延迟累积
    // throwOnError = false：不在网络错误时立即抛异常，而是在响应中标记错误状态
    auto responsesFromShards = sharding_util::sendCommandToShards(
        opCtx, DatabaseName::kAdmin, reqObj, shardIds, executor, false /* throwOnError */);

    // 使用fmt库的字面量语法，支持Python风格的字符串格式化
    using namespace fmt::literals;
    
    // 初始化返回映射：嵌套映射结构用于存储最终的统计结果
    // 外层Key: NamespaceString, 内层Key: ShardId, Value: 数据大小(字节)
    NamespaceStringToShardDataSizeMap namespaceToShardDataSize;
    
    // 处理分片响应：遍历所有分片的响应，解析统计数据
    for (auto&& response : responsesFromShards) {
        try {
            const auto& shardId = response.shardId;
            
            // 构建错误上下文信息：用于异常处理时的错误描述
            auto errorContext =
                "Failed to get stats for balancing from shard '{}'"_format(shardId.toString());
                
            // 验证响应状态：检查网络请求是否成功
            // 如果请求失败，uassertStatusOKWithContext会抛出包含上下文的异常
            const auto responseValue =
                uassertStatusOKWithContext(std::move(response.swResponse), errorContext);

            // 解析响应数据：将BSON响应反序列化为结构化对象
            // IDLParserContext提供解析上下文，用于错误定位和调试
            const ShardsvrGetStatsForBalancingReply reply =
                ShardsvrGetStatsForBalancingReply::parse(
                    IDLParserContext("ShardsvrGetStatsForBalancingReply"), responseValue.data);
                    
            // 提取集合统计信息：从响应中获取该分片上所有集合的统计数据
            const auto collStatsFromShard = reply.getStats();

            // 数据完整性验证：确保响应中的集合数量与请求数量一致
            // 这是一个重要的安全检查，防止数据不一致导致的逻辑错误
            tassert(8245200,
                    "Collection count mismatch",
                    collStatsFromShard.size() == namespacesWithUUIDsForStatsRequest.size());

            // 填充统计映射：将该分片的统计数据添加到全局映射中
            for (const auto& stats : collStatsFromShard) {
                // 构建嵌套映射：namespace -> shardId -> collectionSize
                // stats.getNs(): 集合命名空间
                // shardId: 当前分片ID  
                // stats.getCollSize(): 该集合在当前分片的数据大小（字节）
                namespaceToShardDataSize[stats.getNs()][shardId] = stats.getCollSize();
            }
        } catch (const ExceptionFor<ErrorCodes::ShardNotFound>& ex) {
            // Handle `removeShard`: skip shards removed during a balancing round
            // 处理分片移除场景：跳过在均衡轮次中被移除的分片
            // 
            // 这种情况在以下场景中会发生：
            // 1. 管理员执行removeShard命令移除分片
            // 2. 分片因网络问题暂时不可达
            // 3. 分片正在进行维护或重启
            // 
            // 跳过这些分片是安全的，因为：
            // - 被移除的分片上的chunk会被迁移到其他分片
            // - 暂时不可用的分片在下一轮均衡中会重新包含
            // - 不影响整体的负载均衡决策
            LOGV2_DEBUG(6581603,
                        1,
                        "Skipping shard for the current balancing round",
                        "error"_attr = redact(ex));
        }
    }
    
    // 返回完整的统计映射：包含所有可用分片上所有集合的数据大小信息
    // 调用者可以通过namespaceToShardDataSize[nss][shardId]快速查询特定集合在特定分片的数据大小
    return namespaceToShardDataSize;
}

ShardDataSizeMap getStatsForBalancing(
    OperationContext* opCtx,
    const std::vector<ShardId>& shardIds,
    const NamespaceWithOptionalUUID& namespaceWithUUIDsForStatsRequest) {
    return getStatsForBalancing(
               opCtx,
               shardIds,
               std::vector<NamespaceWithOptionalUUID>{namespaceWithUUIDsForStatsRequest})
        .at(namespaceWithUUIDsForStatsRequest.getNs());
}

}  // namespace mongo
