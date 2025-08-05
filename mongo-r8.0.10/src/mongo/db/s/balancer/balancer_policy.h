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

#pragma once

#include <boost/move/utility_core.hpp>
#include <boost/none.hpp>
#include <boost/optional/optional.hpp>
#include <cstddef>
#include <cstdint>
#include <fmt/format.h>
#include <map>
#include <set>
#include <string>
#include <tuple>
#include <utility>
#include <variant>
#include <vector>

#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobj_comparator_interface.h"
#include "mongo/db/auth/validated_tenancy_scope.h"
#include "mongo/db/keypattern.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/s/balancer/cluster_statistics.h"
#include "mongo/db/shard_id.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/chunk_manager.h"
#include "mongo/s/chunk_version.h"
#include "mongo/s/request_types/get_stats_for_balancing_gen.h"
#include "mongo/s/request_types/move_range_request_gen.h"
#include "mongo/s/shard_version.h"
#include "mongo/stdx/unordered_set.h"
#include "mongo/util/uuid.h"

namespace mongo {

using namespace fmt::literals;

struct ZoneRange {
    ZoneRange(const BSONObj& a_min, const BSONObj& a_max, const std::string& _zone);

    std::string toString() const;

    BSONObj min;
    BSONObj max;
    std::string zone;
};

/**
 * MigrateInfo 结构体的作用：
 * 描述一个完整的 chunk 迁移任务，包含迁移操作所需的所有元数据信息。
 * 
 * 核心功能：
 * 1. 封装单个 chunk 迁移的完整信息（源分片、目标分片、chunk范围等）
 * 2. 支持不同类型的迁移操作（普通迁移、moveRange操作等）
 * 3. 提供迁移任务的标识和描述信息
 * 4. 控制是否强制迁移 jumbo chunk
 * 5. 支持数据大小感知的迁移策略
 * 
 * 主要用途：
 * - 作为负载均衡器生成的迁移计划的基本单元
 * - 传递给迁移执行器进行实际的 chunk 迁移操作
 * - 记录和追踪迁移任务的状态和进度
 */
struct MigrateInfo {
    /**
     * 构造函数1：基于 ChunkType 创建迁移信息
     * @param a_to 目标分片ID
     * @param a_nss 集合命名空间
     * @param a_chunk 要迁移的 chunk 对象
     * @param a_forceJumbo 是否强制迁移 jumbo chunk
     * @param maxChunkSizeBytes 可选的最大 chunk 大小限制
     */
    MigrateInfo(const ShardId& a_to,
                const NamespaceString& a_nss,
                const ChunkType& a_chunk,
                ForceJumbo a_forceJumbo,
                boost::optional<int64_t> maxChunkSizeBytes = boost::none);

    /**
     * 构造函数2：基于详细参数创建迁移信息
     * 用于更灵活的迁移任务创建，特别是 moveRange 操作
     * @param a_to 目标分片ID
     * @param a_from 源分片ID
     * @param a_nss 集合命名空间
     * @param a_uuid 集合UUID
     * @param a_min chunk的最小键值
     * @param a_max chunk的最大键值（可选，用于moveRange）
     * @param a_version chunk版本信息
     * @param a_forceJumbo 是否强制迁移 jumbo chunk
     * @param maxChunkSizeBytes 可选的最大 chunk 大小限制
     */
    MigrateInfo(const ShardId& a_to,
                const ShardId& a_from,
                const NamespaceString& a_nss,
                const UUID& a_uuid,
                const BSONObj& a_min,
                const boost::optional<BSONObj>& a_max,
                const ChunkVersion& a_version,
                ForceJumbo a_forceJumbo,
                boost::optional<int64_t> maxChunkSizeBytes = boost::none);

    /**
     * 获取迁移任务的唯一标识名称
     * @return 迁移任务的字符串标识
     */
    std::string getName() const;

    /**
     * 获取迁移任务的详细描述
     * @return 包含所有迁移信息的字符串表示
     */
    std::string toString() const;

    /**
     * 获取最大 chunk 大小限制
     * @return 可选的最大 chunk 大小（字节）
     */
    boost::optional<int64_t> getMaxChunkSizeBytes() const;

    // ========== 成员变量 ==========

    /**
     * 集合命名空间
     * 作用：标识要迁移的 chunk 属于哪个数据库和集合
     */
    NamespaceString nss;

    /**
     * 集合的唯一标识符
     * 作用：确保迁移操作针对正确的集合版本，避免并发 DDL 操作的冲突
     */
    UUID uuid;

    /**
     * 目标分片ID
     * 作用：指定 chunk 迁移的目标分片
     */
    ShardId to;

    /**
     * 源分片ID  
     * 作用：指定 chunk 当前所在的源分片
     */
    ShardId from;

    /**
     * chunk 的最小键值
     * 作用：定义要迁移的 chunk 范围的下边界（包含）
     */
    BSONObj minKey;

    // May be optional in case of moveRange
    /**
     * chunk 的最大键值（可选）
     * 作用：
     * - 对于普通 chunk 迁移：定义 chunk 范围的上边界（不包含）
     * - 对于 moveRange 操作：可能为空，表示迁移到集合末尾
     * - boost::optional 允许处理不同类型的迁移场景
     */
    boost::optional<BSONObj> maxKey;

    /**
     * chunk 版本信息
     * 作用：
     * - 确保迁移操作基于最新的 chunk 版本
     * - 防止并发操作导致的版本冲突
     * - 支持分片集群的一致性控制
     */
    ChunkVersion version;

    /**
     * 强制迁移 jumbo chunk 的标志
     * 作用：
     * - ForceJumbo::kDoNotForce：不强制迁移，跳过 jumbo chunk
     * - ForceJumbo::kForceBalancer：强制迁移，用于特殊场景（如分片下线）
     * - 控制是否迁移超过大小限制的 chunk
     */
    ForceJumbo forceJumbo;

    // Set only in case of data-size aware balancing
    /**
     * 可选的最大 chunk 大小限制（字节）
     * 作用：
     * - 仅在数据大小感知均衡时设置
     * - 用于验证 chunk 是否超过大小限制
     * - 影响迁移策略和性能预期
     * - 支持细粒度的大小控制
     */
    boost::optional<int64_t> optMaxChunkSizeBytes;
};

/**
 * MigrationReason 枚举的作用：
 * 定义触发 chunk 迁移的原因类型，用于分类和优先级排序不同的迁移场景。
 */
enum MigrationReason { 
    none,               // 无迁移原因
    drain,              // 分片下线，需要迁移数据
    zoneViolation,      // zone 约束违规，需要重新分布
    chunksImbalance     // chunk 负载不均衡
};

/**
 * MigrateInfoVector 类型别名的作用：
 * 表示一组迁移任务的集合，通常为同一轮均衡操作生成的所有迁移计划。
 */
typedef std::vector<MigrateInfo> MigrateInfoVector;

/**
 * MigrateInfosWithReason 类型别名的作用：
 * 将迁移任务列表与触发原因打包，提供完整的迁移计划上下文信息。
 * 第一个元素：迁移任务列表
 * 第二个元素：触发这批迁移的主要原因
 */
typedef std::pair<MigrateInfoVector, MigrationReason> MigrateInfosWithReason;

/**
 * SplitPoints 类型别名的作用：
 * 定义 chunk 拆分的分割点集合，每个 BSONObj 代表一个拆分边界。
 */
typedef std::vector<BSONObj> SplitPoints;

/**
 * Describes a chunk which needs to be split, because it violates the balancer policy.
 */
struct SplitInfo {
    SplitInfo(const ShardId& shardId,
              const NamespaceString& nss,
              const ChunkVersion& collectionPlacementVersion,
              const ChunkVersion& chunkVersion,
              const BSONObj& minKey,
              const BSONObj& maxKey,
              SplitPoints splitKeys);

    std::string toString() const;

    ShardId shardId;
    NamespaceString nss;
    ChunkVersion collectionPlacementVersion;
    ChunkVersion chunkVersion;
    BSONObj minKey;
    BSONObj maxKey;
    SplitPoints splitKeys;
};

typedef std::vector<SplitInfo> SplitInfoVector;

struct MergeInfo {
    MergeInfo(const ShardId& shardId,
              const NamespaceString& nss,
              const UUID& uuid,
              const ChunkVersion& collectionPlacementVersion,
              const ChunkRange& chunkRange);

    std::string toString() const;

    ShardId shardId;
    NamespaceString nss;
    UUID uuid;
    ChunkVersion collectionPlacementVersion;
    ChunkRange chunkRange;
};

struct MergeAllChunksOnShardInfo {
    MergeAllChunksOnShardInfo(const ShardId& shardId, const NamespaceString& nss);

    std::string toString() const;

    ShardId shardId;
    NamespaceString nss;

    bool applyThrottling{false};
};

struct DataSizeInfo {
    DataSizeInfo(const ShardId& shardId,
                 const NamespaceString& nss,
                 const UUID& uuid,
                 const ChunkRange& chunkRange,
                 const ShardVersion& version,
                 const KeyPattern& keyPattern,
                 bool estimatedValue,
                 int64_t maxSize);

    ShardId shardId;
    NamespaceString nss;
    UUID uuid;
    ChunkRange chunkRange;
    // Use ShardVersion for CRUD targeting since datasize is considered a CRUD operation, not a DDL
    // operation.
    ShardVersion version;
    KeyPattern keyPattern;
    bool estimatedValue;
    int64_t maxSize;
};

struct DataSizeResponse {
    DataSizeResponse(long long sizeBytes, long long numObjects, bool maxSizeReached)
        : sizeBytes(sizeBytes), numObjects(numObjects), maxSizeReached(maxSizeReached) {}

    long long sizeBytes;
    long long numObjects;
    bool maxSizeReached;
};

struct ShardZoneInfo {
    ShardZoneInfo(size_t numChunks, size_t firstNormalizedZoneIdx, const BSONObj& firstChunkMinKey)
        : numChunks(numChunks),
          firstNormalizedZoneIdx(firstNormalizedZoneIdx),
          firstChunkMinKey(firstChunkMinKey) {}

    // Total number of chunks this shard has for this zone
    size_t numChunks;
    // Index in the vector of normalised zones of the first zone range that contains the first chunk
    // for this shard in this zone
    size_t firstNormalizedZoneIdx;
    // minKey of the first chunk this shard has in this zone
    BSONObj firstChunkMinKey;
};

typedef int NumMergedChunks;

typedef std::variant<MergeInfo, DataSizeInfo, MigrateInfo, MergeAllChunksOnShardInfo>
    BalancerStreamAction;

typedef std::variant<Status, StatusWith<DataSizeResponse>, StatusWith<NumMergedChunks>>
    BalancerStreamActionResponse;

typedef std::vector<ClusterStatistics::ShardStatistics> ShardStatisticsVector;
typedef StringMap<StringMap<ShardZoneInfo>> ShardZoneInfoMap;

/*
// 完整的数据结构表示：
NamespaceStringToShardDataSizeMap = {
    集合命名空间1 => {
        分片ID1 => 数据大小（字节）,
        分片ID2 => 数据大小（字节）,
        ...
    },
    集合命名空间2 => {
        分片ID1 => 数据大小（字节）,
        分片ID2 => 数据大小（字节）,
        ...
    },
    ...
}
*/
using ShardDataSizeMap = std::map<ShardId, int64_t>;
using NamespaceStringToShardDataSizeMap = stdx::unordered_map<NamespaceString, ShardDataSizeMap>;
/*
 * Keeps track of info needed for data size aware balancing.
 */
/**
 * CollectionDataSizeInfoForBalancing 结构体的作用：
 * 存储用于数据大小感知负载均衡的集合统计信息，包含各分片的数据大小映射和最大chunk大小限制。
 * 
 * 主要用途：
 * 1. 提供负载均衡算法所需的数据大小统计信息
 * 2. 支持基于数据量而非chunk数量的均衡策略
 * 3. 确保迁移决策考虑实际数据分布情况
 * 4. 为chunk拆分和合并提供大小约束
 */
struct CollectionDataSizeInfoForBalancing {
    /**
     * 构造函数：初始化集合的数据大小信息
     * @param shardToDataSizeMap 分片到数据大小的映射，移动语义避免拷贝
     * @param maxChunkSizeBytes 最大chunk大小限制（字节）
     */
    CollectionDataSizeInfoForBalancing(ShardDataSizeMap&& shardToDataSizeMap,
                                       long maxChunkSizeBytes)
        : shardToDataSizeMap(std::move(shardToDataSizeMap)), maxChunkSizeBytes(maxChunkSizeBytes) {}

    /**
     * 分片到数据大小的映射表
     * 类型：std::map<ShardId, int64_t>
     * 作用：
     * - 记录每个分片上该集合的实际数据大小（字节）
     * - 用于计算负载均衡时的数据量差异
     * - 帮助选择过载和轻载的分片
     * - 支持基于数据大小而非chunk数量的均衡策略
     */
    ShardDataSizeMap shardToDataSizeMap;
    
    /**
     * 最大chunk大小限制（字节）
     * 作用：
     * - 定义单个chunk允许的最大数据量
     * - 用于判断chunk是否为jumbo chunk
     * - 影响chunk拆分和迁移决策
     * - 确保迁移操作不会因chunk过大而失败
     * - 在均衡算法中作为数据量差异的比较基准
     */
    const int64_t maxChunkSizeBytes;
};

NamespaceStringToShardDataSizeMap getStatsForBalancing(
    OperationContext* opCtx,
    const std::vector<ShardId>& shardIds,
    const std::vector<NamespaceWithOptionalUUID>& namespacesWithUUIDsForStatsRequest);

ShardDataSizeMap getStatsForBalancing(
    OperationContext* opCtx,
    const std::vector<ShardId>& shardIds,
    const NamespaceWithOptionalUUID& namespaceWithUUIDsForStatsRequest);

/**
 * Keeps track of zones for a collection.
 */
class ZoneInfo {
public:
    static const std::string kNoZoneName;

    ZoneInfo();
    ZoneInfo(ZoneInfo&&) = default;

    /**
     * Appends the specified range to the set of ranges tracked for this collection and checks if
     * it overlaps with existing ranges.
     */
    Status addRangeToZone(const ZoneRange& range);

    /**
     * Returns all zones added so far.
     */
    const std::set<std::string>& allZones() const {
        return _allZones;
    }

    /**
     * Using the set of zones added so far, returns what zone corresponds to the specified range.
     * Returns an empty string if the chunk doesn't fall into any zone.
     */
    std::string getZoneForRange(const ChunkRange& chunkRange) const;

    /**
     * Returns all zone ranges defined.
     */
    const BSONObjIndexedMap<ZoneRange>& zoneRanges() const {
        return _zoneRanges;
    }

    /**
     * Retrieves the collection zones from the catalog client
     */
    static StatusWith<ZoneInfo> getZonesForCollection(OperationContext* opCtx,
                                                      const NamespaceString& nss,
                                                      const KeyPattern& keyPattern);

private:
    // Map of zone max key to the zone description
    BSONObjIndexedMap<ZoneRange> _zoneRanges;

    // Set of all zones defined for this collection
    std::set<std::string> _allZones;
};

class ChunkManager;

/**
 * This class constitutes a cache of the chunk distribution across the entire cluster along with the
 * zone boundaries imposed on it. This information is stored in format, which makes it efficient to
 * query utilization statististics and to decide what to balance.
 */
class DistributionStatus final {
    DistributionStatus(const DistributionStatus&) = delete;
    DistributionStatus& operator=(const DistributionStatus&) = delete;

public:
    DistributionStatus(NamespaceString nss, ZoneInfo zoneInfo, const ChunkManager& chunkMngr);
    DistributionStatus(DistributionStatus&&) = default;
    ~DistributionStatus() {}

    /**
     * Returns the namespace for which this balance status applies.
     */
    const NamespaceString& nss() const {
        return _nss;
    }

    /**
     * Returns number of chunks in the specified shard.
     */
    size_t numberOfChunksInShard(const ShardId& shardId) const;

    /**
     * Returns all zones defined for the collection.
     */
    const std::set<std::string>& zones() const {
        return _zoneInfo.allZones();
    }

    const ChunkManager& getChunkManager() const {
        return _chunkMngr;
    }

    const ZoneInfo& getZoneInfo() const {
        return _zoneInfo;
    }

    const StringMap<ShardZoneInfo>& getZoneInfoForShard(const ShardId& shardId) const;

    /**
     * Loop through each chunk on this shard within the given zone invoking 'handler' for each one
     * of them.
     *
     * The iteration stops either when all the chunks have been visited (the method
     * will return 'true') or the first time 'handler' returns 'false' (in which case the method
     * will return 'false').
     *
     * Effectively, return of 'true' means all chunks were visited and none matched, and
     * 'false' means the hanlder return 'false' before visiting all chunks.
     */

    /**
     * forEachChunkOnShardInZone 的作用：
     * 遍历指定分片在指定 zone 内的所有 chunk，对每个 chunk 执行用户提供的处理函数。
     * 
     * 核心功能：
     * 1. 在指定分片和 zone 的交集范围内查找所有 chunk
     * 2. 对找到的每个 chunk 调用用户提供的处理函数（handler）
     * 3. 支持提前终止遍历：当 handler 返回 false 时停止遍历
     * 4. 利用预缓存的索引信息优化遍历性能，避免全集合扫描
     * 
     * 返回值语义：
     * - true：所有 chunk 都被访问完毕，或该分片在该 zone 内没有 chunk
     * - false：handler 在某个 chunk 上返回了 false，提前终止了遍历
     * 
     * 主要用于负载均衡算法中快速定位和处理特定分片特定 zone 的 chunk。
     */
    template <typename Callable>
    bool forEachChunkOnShardInZone(const ShardId& shardId,
                                   const std::string& zoneName,
                                   Callable&& handler) const {
    
        bool shouldContinue = true;
    
        // 获取指定分片的所有 zone 信息映射
        const auto& shardZoneInfoMap = getZoneInfoForShard(shardId);
        // 查找指定 zone 在该分片上的信息
        auto shardZoneInfoIt = shardZoneInfoMap.find(zoneName);
        if (shardZoneInfoIt == shardZoneInfoMap.end()) {
            // 该分片在指定 zone 内没有 chunk，直接返回 true
            return shouldContinue;
        }
        const auto& shardZoneInfo = shardZoneInfoIt->second;
    
        // Start from the first normalized zone that contains chunks for this shard
        // 从包含该分片 chunk 的第一个规范化 zone 开始
        // 利用预缓存的索引 firstNormalizedZoneIdx 直接定位，避免从头扫描
        const auto initialZoneIt = _normalizedZones.cbegin() + shardZoneInfo.firstNormalizedZoneIdx;
    
        // 遍历从初始 zone 开始的所有规范化 zone 范围
        for (auto normalizedZoneIt = initialZoneIt; normalizedZoneIt < _normalizedZones.cend();
             normalizedZoneIt++) {
            const auto& zoneRange = *normalizedZoneIt;
    
            // 判断是否为该分片在目标 zone 的第一个范围
            const auto isFirstRange = (normalizedZoneIt == initialZoneIt);
    
            if (isFirstRange) {
                // 第一个范围必须与目标 zone 匹配（数据一致性检查）
                tassert(
                    8236530,
                    "Unexpected first normalized zone for shard '{}'. Expected '{}' but found '{}'"_format(
                        shardId.toString(), zoneName, zoneRange.zone),
                    zoneRange.zone == zoneName);
            } else if (zoneRange.zone != zoneName) {
                // 跳过不匹配的 zone 范围，继续查找目标 zone 的下一个范围
                continue;
            }
    
            // For the first range in zone we have pre-cached the minKey of the first chunk,
            // thus we can start iterating from that one.
            // For the subsequent ranges in this zone we start iterating from the minKey of
            // the range itself.
            // 
            // 优化遍历起点：
            // - 对于第一个范围：使用预缓存的 firstChunkMinKey，直接定位到该分片的第一个 chunk
            // - 对于后续范围：从 zone 范围的 minKey 开始遍历
            const auto firstKey = isFirstRange ? shardZoneInfo.firstChunkMinKey : zoneRange.min;
    
            // 在当前 zone 范围内查找与指定分片相关的所有 chunk
            getChunkManager().forEachOverlappingChunk(
                firstKey, zoneRange.max, false /* isMaxInclusive */, [&](const auto& chunk) {
                    // 过滤：只处理属于目标分片的 chunk
                    if (chunk.getShardId() != shardId) {
                        return true;  // continue - 跳过不属于目标分片的 chunk
                    }
                    
                    // 调用用户提供的处理函数
                    if (!handler(chunk)) {
                        // handler 返回 false，标记需要停止遍历
                        shouldContinue = false;
                    };
                    return shouldContinue; // 返回是否继续遍历的标志
                });
    
            // 如果 handler 要求停止遍历，则跳出外层循环
            if (!shouldContinue) {
                break;
            }
        }
        
        // 返回遍历结果：
        // true  - 所有 chunk 都被访问，没有提前终止
        // false - handler 在某个 chunk 上返回 false，提前终止
        return shouldContinue;
    }


private:
    // Namespace for which this distribution applies
    NamespaceString _nss;

    // Map that tracks how many chunks every shard is owning in each zone
    // shardId -> zoneName -> shardZoneInfo
    /*
        假设有一个分片集群：

        分片：shard0001, shard0002, shard0003
        集合：myapp.users，分片键：{userId: 1}
        没有设置任何zone配置，那么_shardZoneInfoMap的内容将是：
        _shardZoneInfoMap = {
            "shard0001" => {
                "" => ShardZoneInfo{
                    numChunks: 5,           // 该分片上的chunk数量
                    firstNormalizedZoneIdx: 0,  // 指向_normalizedZones[0]
                    firstChunkMinKey: {userId: MinKey}  // 第一个chunk的最小键
                }
            },
            "shard0002" => {
                "" => ShardZoneInfo{
                    numChunks: 3,
                    firstNormalizedZoneIdx: 0,
                    firstChunkMinKey: {userId: 100}
                }
            },
            "shard0003" => {
                "" => ShardZoneInfo{
                    numChunks: 4,
                    firstNormalizedZoneIdx: 0,
                    firstChunkMinKey: {userId: 500}
                }
            }
        }
    */
    ShardZoneInfoMap _shardZoneInfoMap;

    // Info for zones.
    ZoneInfo _zoneInfo;

    // Normalized zone are calculated starting from the currently configured zone in `config.tags`
    // and the chunks provided by @this._chunkManager.
    //
    // The normalization process is performed to guarantee the following properties:
    //  - **All zone ranges are contiguous.** If there was a gap between two zones ranges we fill it
    //  with a range associated to the special kNoZone.
    //
    //  - **Range boundaries always align with chunk boundaries.** If a zone range covers only
    //  partially a chunk, boundaries of that zone will be shrunk so that the normalized zone won't
    //  overlap with that chunk. Boundaries of a normalized zone will never fall in the middle of a
    //  chunk.
    /*
    // 如果是没有任何zone配置
    // _normalizedZones内容：
    _normalizedZones = [
        ZoneRange{min: {userId: MinKey}, max: {userId: MaxKey}, zone: ""}
    ]
    */
    std::vector<ZoneRange> _normalizedZones;

    ChunkManager _chunkMngr;
};

class BalancerPolicy {
public:
    /**
     * Determines whether a shard with the specified utilization statistics would be able to accept
     * a chunk with the specified zone. According to the policy a shard cannot accept chunks if its
     * size is maxed out and if the chunk's zone conflicts with the zone of the shard.
     */
    static Status isShardSuitableReceiver(const ClusterStatistics::ShardStatistics& stat,
                                          const std::string& chunkZone);

    /**
     * Returns a suggested set of chunks or ranges to move within a collection's shards, given the
     * specified state of the shards (draining, max size reached, etc) and the number of chunks or
     * data size for that collection. If the policy doesn't recommend anything to move, it returns
     * an empty vector. The entries in the vector do are all for separate source/destination shards
     * and as such do not need to be done serially and can be scheduled in parallel.
     *
     * The balancing logic calculates the optimum number of chunks per shard for each zone and if
     * any of the shards have chunks, which are sufficiently higher than this number, suggests
     * moving chunks to shards, which are under this number.
     *
     * The availableShards parameter is in/out and it contains the set of shards, which haven't
     * been used for migrations yet. Used so we don't return multiple conflicting migrations for the
     * same shard.
     */
    static MigrateInfosWithReason balance(
        const ShardStatisticsVector& shardStats,
        const DistributionStatus& distribution,
        const CollectionDataSizeInfoForBalancing& collDataSizeInfo,
        stdx::unordered_set<ShardId>* availableShards,
        bool forceJumbo);

private:
    /*
     * Only considers shards with the specified zone, all shards in case the zone is empty.
     * Returns a tuple <ShardID, amount of data in bytes> referring the shard with less data.
     */
    static std::tuple<ShardId, int64_t> _getLeastLoadedReceiverShard(
        const ShardStatisticsVector& shardStats,
        const CollectionDataSizeInfoForBalancing& collDataSizeInfo,
        const std::string& zone,
        const stdx::unordered_set<ShardId>& availableShards);

    /**
     * Only considers shards with the specified zone, all shards in case the zone is empty.
     * Returns a tuple <ShardID, amount of data in bytes> referring the shard with more data.
     */
    static std::tuple<ShardId, int64_t> _getMostOverloadedShard(
        const ShardStatisticsVector& shardStats,
        const CollectionDataSizeInfoForBalancing& collDataSizeInfo,
        const std::string& zone,
        const stdx::unordered_set<ShardId>& availableShards);

    /**
     * Selects one range for the specified zone (if appropriate) to be moved in order to bring the
     * deviation of the collection data size closer to even across all shards in the specified
     * zone. Takes into account and updates the shards, which haven't been used for migrations yet.
     *
     * Returns true if a migration was suggested, false otherwise. This method is intented to be
     * called multiple times until all posible migrations for a zone have been selected.
     */
    static bool _singleZoneBalanceBasedOnDataSize(
        const ShardStatisticsVector& shardStats,
        const DistributionStatus& distribution,
        const CollectionDataSizeInfoForBalancing& collDataSizeInfo,
        const std::string& zone,
        int64_t idealDataSizePerShardForZone,
        std::vector<MigrateInfo>* migrations,
        stdx::unordered_set<ShardId>* availableShards,
        ForceJumbo forceJumbo);
};

}  // namespace mongo
