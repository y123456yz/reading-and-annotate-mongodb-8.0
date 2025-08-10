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

#include "mongo/db/s/auto_split_vector.h"

#include <cstddef>
#include <memory>
#include <set>
#include <string>
#include <tuple>

#include <boost/optional/optional.hpp>

#include "mongo/bson/simple_bsonobj_comparator.h"
#include "mongo/bson/util/builder.h"
#include "mongo/db/bson/dotted_path_support.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog_raii.h"
#include "mongo/db/concurrency/lock_manager_defs.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/keypattern.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/query/index_bounds.h"
#include "mongo/db/query/internal_plans.h"
#include "mongo/db/query/plan_executor.h"
#include "mongo/db/query/plan_yield_policy.h"
#include "mongo/db/s/shard_key_index_util.h"
#include "mongo/db/server_options.h"
#include "mongo/logv2/log.h"
#include "mongo/logv2/log_attr.h"
#include "mongo/logv2/log_component.h"
#include "mongo/logv2/redaction.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/util/duration.h"
#include "mongo/util/str.h"
#include "mongo/util/timer.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kSharding

namespace mongo {
namespace {

/*
 * BSON arrays are serialized as BSON objects with the index of each element as a string key: for
 * example, the array ["a","b","c"] is going to be serialized as {"0":"a","1":"b","2":"c"}. The
 * minimum size for a BSON object is `BSONObj::kMinBSONLength`.
 *
 * Given that the `vector<BSONObj>` returned by `autoSplitVector` can't be greater than 16MB when
 * serialized, pessimistically assume that each key occupies the highest possible number of bytes.
 */
/**
 * BSON数组序列化规则：
 * BSON数组在序列化时会转换为BSON对象，数组索引作为字符串键
 * 例如：["a","b","c"] → {"0":"a","1":"b","2":"c"}
 * 
 * estimatedAdditionalBytesPerItemInBSONArray的作用：
 * 预估每个数组元素在序列化时额外需要的字节数（主要是索引键的开销）
 */
const int estimatedAdditionalBytesPerItemInBSONArray{
    (int)std::to_string(BSONObjMaxUserSize / BSONObj::kMinBSONLength).length()};

constexpr int kMaxSplitPointsToReposition{3};

BSONObj prettyKey(const BSONObj& keyPattern, const BSONObj& key) {
    return key.replaceFieldNames(keyPattern).clientReadable();
}

/*
 * Takes the given min/max BSON objects that are a prefix of the shardKey and return two new BSON
 * object extended to cover the entire shardKey. See KeyPattern::extendRangeBound documentation for
 * some examples.
 */
std::tuple<BSONObj, BSONObj> getMinMaxExtendedBounds(const ShardKeyIndex& shardKeyIdx,
                                                     const BSONObj& min,
                                                     const BSONObj& max) {
    KeyPattern kp(shardKeyIdx.keyPattern());

    // Extend min to get (min, MinKey, MinKey, ....)
    BSONObj minKey = Helpers::toKeyFormat(kp.extendRangeBound(min, false /* upperInclusive */));
    BSONObj maxKey;
    if (max.isEmpty()) {
        // if max not specified, make it (MaxKey, Maxkey, MaxKey...)
        maxKey = Helpers::toKeyFormat(kp.extendRangeBound(max, true /* upperInclusive */));
    } else {
        // otherwise make it (max,MinKey,MinKey...) so that bound is non-inclusive
        maxKey = Helpers::toKeyFormat(kp.extendRangeBound(max, false /* upperInclusive*/));
    }

    return {minKey, maxKey};
}

/*
 * Reshuffle fields according to the shard key pattern.
 */
auto orderShardKeyFields(const BSONObj& keyPattern, const BSONObj& key) {
    // Note: It is correct to hydrate the indexKey 'key' with 'keyPattern', because the index key
    // pattern is a prefix of 'keyPattern'.
    return dotted_path_support::extractElementsBasedOnTemplate(key.replaceFieldNames(keyPattern),
                                                               keyPattern);
}

}  // namespace


/**
 * autoSplitVector 函数的作用：
 * 自动计算分片集合 chunk 的分裂点，用于将大 chunk 分割成多个较小的 chunk。
 * 
 * 核心功能：
 * 1. 分裂点计算：基于集合数据分布和 chunk 大小限制计算最优的分裂点
 * 2. 索引扫描：使用分片键索引扫描数据，确保分裂点的有效性和性能
 * 3. 大小控制：确保生成的分裂点向量不超过 BSON 最大尺寸限制
 * 4. 均匀分布：通过重新计算分裂点确保生成的 chunk 大小相对均匀
 * 5. 基数检测：识别和处理低基数键，避免无效分裂
 * 
 * 算法策略：
 * - 基于平均文档大小估算每个 chunk 应包含的文档数量
 * - 正向或反向扫描索引，按文档数量间隔选择分裂点
 * - 避免重复分裂点，处理高频键的特殊情况
 * - 动态调整最后几个分裂点，确保最后一个 chunk 不会太小
 * 
 * 性能优化：
 * - 使用分片键前缀索引进行高效扫描
 * - 限制分裂点数量和响应大小，避免内存溢出
 * - 支持增量扫描和中断处理
 * 
 * 返回值：
 * - first：分裂点的 BSON 对象向量，每个对象表示一个分裂位置
 * - second：是否因为 BSON 大小限制而提前终止计算
 * 
 * 该函数是分片集群自动平衡和 chunk 分裂的核心算法，确保数据在分片间的均匀分布。
 */
// AutoSplitVectorCommand::typedRun
// MigrationSourceManager::MigrationSourceManager->computeOtherBound 调用
std::pair<std::vector<BSONObj>, bool> autoSplitVector(OperationContext* opCtx,
                                                      const NamespaceString& nss,
                                                      const BSONObj& keyPattern,
                                                      const BSONObj& min,
                                                      const BSONObj& max,
                                                      long long maxChunkSizeBytes,
                                                      boost::optional<int> limit,
                                                      bool forward) {
    // 参数验证：确保 limit 参数为正数
    if (limit) {
        uassert(ErrorCodes::InvalidOptions, "autoSplitVector expects a positive limit", *limit > 0);
    }

    // 结果变量初始化：
    // splitKeys：存储计算出的分裂点
    // reachedMaxBSONSize：标记是否因为响应大小限制而提前终止
    std::vector<BSONObj> splitKeys;
    bool reachedMaxBSONSize = false;  // True if the split points vector becomes too big

    // 性能统计变量：记录查找分裂点的耗时
    int elapsedMillisToFindSplitPoints;

    // Contains each key appearing multiple times and estimated to be able to fill-in a chunk alone
    // 高频键集合：存储出现频率过高的键，这些键可能导致无法有效分裂
    auto tooFrequentKeys = SimpleBSONObjComparator::kInstance.makeBSONObjSet();

    {
        // 获取集合的共享锁：确保在分析期间集合结构稳定
        AutoGetCollection collection(opCtx, nss, MODE_IS);

        // 集合存在性检查：确保目标集合存在
        uassert(ErrorCodes::NamespaceNotFound,
                str::stream() << "namespace " << nss.toStringForErrorMsg() << " does not exists",
                collection);

        // Get the size estimate for this namespace
        // 获取集合统计信息：用于估算分裂策略
        const long long totalLocalCollDocuments = collection->numRecords(opCtx);
        const long long dataSize = collection->dataSize(opCtx);

        // Return empty vector if current estimated data size is less than max chunk size
        // 提前退出检查：如果数据量小于 chunk 大小限制，无需分裂
        if (dataSize < maxChunkSizeBytes || totalLocalCollDocuments == 0) {
            return {};
        }

        // Allow multiKey based on the invariant that shard keys must be single-valued. Therefore,
        // any multi-key index prefixed by shard key cannot be multikey over the shard key fields.
        // 查找分片键索引：
        // 功能：找到以分片键为前缀的索引，用于高效扫描
        // 多键支持：允许多键索引，因为分片键必须是单值的
        const auto shardKeyIdx = findShardKeyPrefixedIndex(opCtx,
                                                           *collection,
                                                           keyPattern,
                                                           /*requireSingleKey=*/false);
        uassert(ErrorCodes::IndexNotFound,
                str::stream() << "couldn't find index over splitting key "
                              << keyPattern.clientReadable().toString(),
                shardKeyIdx);

        // 扩展边界计算：将输入的 min/max 扩展为完整的分片键边界
        const auto [minKey, maxKey] = getMinMaxExtendedBounds(*shardKeyIdx, min, max);

        // 索引扫描器工厂函数：
        // 功能：创建配置好的索引扫描器，支持不同方向和边界包含策略
        // 参数：最小键、最大键、边界包含策略、让步策略、扫描方向
        auto getIdxScanner = [&](const BSONObj& minKey,
                                 const BSONObj& maxKey,
                                 BoundInclusion inclusion,
                                 PlanYieldPolicy::YieldPolicy yieldPolicy,
                                 InternalPlanner::Direction direction) {
            return InternalPlanner::shardKeyIndexScan(opCtx,
                                                      &(*collection),
                                                      *shardKeyIdx,
                                                      minKey,
                                                      maxKey,
                                                      inclusion,
                                                      yieldPolicy,
                                                      direction);
        };

        // Setup the index scanner that will be used to find the split points
        // 主扫描器设置：根据扫描方向创建主要的索引扫描器
        // 正向扫描：从 minKey 到 maxKey，包含起始键
        // 反向扫描：从 maxKey 到 minKey，包含结束键
        auto idxScanner = forward ? getIdxScanner(minKey,
                                                  maxKey,
                                                  BoundInclusion::kIncludeStartKeyOnly,
                                                  PlanYieldPolicy::YieldPolicy::YIELD_AUTO,
                                                  InternalPlanner::FORWARD)
                                  : getIdxScanner(maxKey,
                                                  minKey,
                                                  BoundInclusion::kIncludeEndKeyOnly,
                                                  PlanYieldPolicy::YieldPolicy::YIELD_AUTO,
                                                  InternalPlanner::BACKWARD);
        // Get first key belonging to the chunk
        // 获取范围内的第一个键：确定扫描的起始点
        BSONObj firstKeyInOriginalChunk;
        {
            PlanExecutor::ExecState state = idxScanner->getNext(&firstKeyInOriginalChunk, nullptr);
            if (state == PlanExecutor::IS_EOF) {
                // Range is empty
                // 空范围检查：如果范围内没有数据，返回空结果
                return {};
            }
        }

        // Compare the first and last document belonging to the range; if they have the same shard
        // key value, no split point can be found.
        // 可分裂性检查：比较范围内第一个和最后一个文档的分片键
        // 如果相同，说明整个范围只有一个分片键值，无法分裂
        bool chunkCanBeSplit = true;
        {
            BSONObj lastKeyInChunk;
            // Use INTERRUPT_ONLY since it just fetches the last key. Using YIELD_AUTO could
            // invalidate idxScanner below.
            // 创建范围结束扫描器：获取范围内的最后一个键
            // 使用 INTERRUPT_ONLY 策略避免影响主扫描器的有效性
            auto rangeEndIdxScanner = forward
                ? getIdxScanner(maxKey,
                                minKey,
                                BoundInclusion::kIncludeEndKeyOnly,
                                PlanYieldPolicy::YieldPolicy::INTERRUPT_ONLY,
                                InternalPlanner::BACKWARD)
                : getIdxScanner(minKey,
                                maxKey,
                                BoundInclusion::kIncludeStartKeyOnly,
                                PlanYieldPolicy::YieldPolicy::INTERRUPT_ONLY,
                                InternalPlanner::FORWARD);

            PlanExecutor::ExecState state = rangeEndIdxScanner->getNext(&lastKeyInChunk, nullptr);
            if (state == PlanExecutor::IS_EOF) {
                // Range is empty
                return {};
            }

            // 比较第一个和最后一个键：判断是否可以分裂
            chunkCanBeSplit = firstKeyInOriginalChunk.woCompare(lastKeyInChunk) != 0;
        }

        // 低基数警告：如果整个范围只有一个键值，记录警告并返回
        if (!chunkCanBeSplit) {
            LOGV2_WARNING(
                5865001,
                "Possible low cardinality key detected in range. Range contains only a single key.",
                logAttrs(collection.getNss()),
                "minKey"_attr = redact(prettyKey(keyPattern, minKey)),
                "maxKey"_attr = redact(prettyKey(keyPattern, maxKey)),
                "key"_attr = redact(prettyKey(shardKeyIdx->keyPattern(), firstKeyInOriginalChunk)));
            return {};
        }

        // 记录分裂查找开始：用于调试和性能监控
        LOGV2(6492600,
              "Requested split points lookup for range",
              logAttrs(nss),
              "minKey"_attr = redact(prettyKey(keyPattern, minKey)),
              "maxKey"_attr = redact(prettyKey(keyPattern, maxKey)),
              "direction"_attr = forward ? "forwards" : "backwards");

        // Use the average document size and number of documents to find the approximate number of
        // keys each chunk should contain
        // 每 chunk 文档数计算：
        // 基于平均文档大小估算每个 chunk 应该包含的文档数量
        const long long avgDocSize = dataSize / totalLocalCollDocuments;

        // Split at max chunk size
        // 计算每个 chunk 的最大文档数
        long long maxDocsPerChunk = maxChunkSizeBytes / avgDocSize;

        // 扫描状态变量：
        BSONObj currentKey;               // Last key seen during the index scan 当前扫描到的键
        long long numScannedKeys = 1;     // firstKeyInOriginalChunk has already been scanned 已扫描的键数量
        std::size_t resultArraySize = 0;  // Approximate size in bytes of the split points array 结果数组大小估算

        // Lambda to check whether the split points vector would exceed BSONObjMaxUserSize in case
        // of additional split key of the specified size.
        // BSON 大小检查函数：
        // 功能：检查添加新的分裂点是否会超过 BSON 最大尺寸限制
        // 目的：防止响应过大导致传输失败
        auto checkMaxBSONSize = [&resultArraySize](const int additionalKeySize) {
            return resultArraySize + additionalKeySize > BSONObjMaxUserSize;
        };

        // Reference to last split point that needs to be checked in order to avoid adding duplicate
        // split points. Initialized to the min of the first chunk being split.
        // 重复分裂点检查：
        // 维护对上一个分裂点的引用，避免添加重复的分裂点
        // 初始化为第一个 chunk 的最小值
        auto firstKeyElement = orderShardKeyFields(keyPattern, firstKeyInOriginalChunk);
        auto lastSplitPoint = firstKeyElement;

        Timer timer;  // To measure time elapsed while searching split points 计时器：测量查找分裂点的耗时

        // Traverse the index and add the maxDocsPerChunk-th key to the result vector
        // 主扫描循环：遍历索引并按间隔添加分裂点
        // 策略：每扫描 maxDocsPerChunk 个文档就添加一个分裂点
        while (idxScanner->getNext(&currentKey, nullptr) == PlanExecutor::ADVANCED) {
            if (++numScannedKeys >= maxDocsPerChunk) {
                // 重新排序字段：确保分裂点与分片键模式一致
                currentKey = orderShardKeyFields(keyPattern, currentKey);

                // 分裂点方向性检查：确保分裂点的顺序正确
                const auto compareWithPreviousSplitPoint = currentKey.woCompare(lastSplitPoint);
                if (forward) {
                    dassert(compareWithPreviousSplitPoint >= 0,
                            str::stream()
                                << "Found split key smaller than the last one in forwards lookup: "
                                << currentKey << " < " << lastSplitPoint);
                } else {
                    dassert(compareWithPreviousSplitPoint <= 0,
                            str::stream()
                                << "Found split key larger than the last one in backwards lookup: "
                                << currentKey << " > " << lastSplitPoint);
                }
                if (compareWithPreviousSplitPoint == 0) {
                    // Do not add again the same split point in case of frequent shard key.
                    // 重复分裂点处理：将高频键记录到特殊集合中
                    tooFrequentKeys.insert(currentKey.getOwned());
                    continue;
                }

                // BSON 大小检查：确保添加新分裂点不会超过大小限制
                const auto additionalKeySize =
                    currentKey.objsize() + estimatedAdditionalBytesPerItemInBSONArray;
                if (checkMaxBSONSize(additionalKeySize)) {
                    if (splitKeys.empty()) {
                        // Keep trying until finding at least one split point that isn't above
                        // the max object user size. Very improbable corner case: the shard key
                        // size for the chosen split point is exactly 16MB.
                        // 至少一个分裂点保证：继续尝试直到找到至少一个不超过大小限制的分裂点
                        continue;
                    }
                    reachedMaxBSONSize = true;
                    break;
                }

                // 添加有效的分裂点：
                resultArraySize += additionalKeySize;
                splitKeys.push_back(currentKey.getOwned());
                lastSplitPoint = splitKeys.back();
                numScannedKeys = 0;

                // 用户限制检查：如果指定了分裂点数量限制，在达到限制时停止
                if (limit && splitKeys.size() == static_cast<size_t>(*limit) + 1) {
                    // If the user has specified a limit, calculate the first `limit + 1` split
                    // points (avoid creating small chunks)
                    break;
                }

                LOGV2_DEBUG(5865003, 4, "Picked a split key", "key"_attr = redact(currentKey));
            }
        }

        // Avoid creating small chunks by fairly recalculating the last split points if the last
        // chunk would be too small (containing less than `80% maxDocsPerChunk` documents).
        // 小 chunk 避免算法：
        // 如果最后一个 chunk 太小（少于 80% 的标准大小），重新计算最后几个分裂点
        // 目的：确保生成的 chunk 大小相对均匀，避免出现过小的 chunk
        bool lastChunk80PercentFull = numScannedKeys >= maxDocsPerChunk * 0.8;
        if (!lastChunk80PercentFull && !splitKeys.empty() && !reachedMaxBSONSize) {
            // Eventually recalculate the last split points (at most `kMaxSplitPointsToReposition`).
            // 确定需要重新定位的分裂点数量：最多重新定位 3 个分裂点
            int nSplitPointsToReposition = splitKeys.size() > kMaxSplitPointsToReposition
                ? kMaxSplitPointsToReposition
                : splitKeys.size();

            // Equivalent to: (nSplitPointsToReposition * maxDocsPerChunk + numScannedKeys) divided
            // by the number of reshuffled chunks (nSplitPointsToReposition + 1).
            // 新 chunk 大小计算：
            // 重新分配文档，使最后几个 chunk 的大小更加均匀
            const auto maxDocsPerNewChunk = maxDocsPerChunk -
                ((maxDocsPerChunk - numScannedKeys) / (nSplitPointsToReposition + 1));

            if (numScannedKeys < maxDocsPerChunk - maxDocsPerNewChunk) {
                // If the surplus is not too much, simply keep a bigger last chunk.
                // The surplus is considered enough if repositioning the split points would imply
                // generating chunks with a number of documents lower than `67% maxDocsPerChunk`.
                // 简单策略：如果剩余文档不多，直接保持一个较大的最后 chunk
                splitKeys.pop_back();
            } else {
                // Fairly recalculate the last `nSplitPointsToReposition` split points.
                // 公平重算策略：重新计算最后几个分裂点的位置
                splitKeys.erase(splitKeys.end() - nSplitPointsToReposition, splitKeys.end());

                // Since the previous idxScanner is finished, we are not violating the requirement
                // that only one yieldable plan is active.
                // 创建新的扫描器：从最后保留的分裂点开始重新扫描
                idxScanner = forward
                    ? getIdxScanner(splitKeys.empty() ? firstKeyElement : splitKeys.back(),
                                    maxKey,
                                    BoundInclusion::kIncludeStartKeyOnly,
                                    PlanYieldPolicy::YieldPolicy::YIELD_AUTO,
                                    InternalPlanner::FORWARD)
                    : getIdxScanner(splitKeys.empty() ? firstKeyElement : splitKeys.back(),
                                    minKey,
                                    BoundInclusion::kIncludeBothStartAndEndKeys,
                                    PlanYieldPolicy::YieldPolicy::YIELD_AUTO,
                                    InternalPlanner::BACKWARD);

                numScannedKeys = 0;

                // 重新扫描和计算分裂点：
                // 使用新的文档间隔重新确定分裂点位置
                auto previousSplitPoint = splitKeys.empty() ? firstKeyElement : splitKeys.back();
                while (idxScanner->getNext(&currentKey, nullptr) == PlanExecutor::ADVANCED) {
                    if (++numScannedKeys >= maxDocsPerNewChunk) {
                        currentKey = orderShardKeyFields(keyPattern, currentKey);

                        const auto compareWithPreviousSplitPoint =
                            currentKey.woCompare(previousSplitPoint);

                        // 方向性验证：确保重新计算的分裂点方向正确
                        if (forward) {
                            dassert(compareWithPreviousSplitPoint >= 0,
                                    str::stream() << "Found split key smaller than the last one in "
                                                     "forwards lookup: "
                                                  << currentKey << " < " << previousSplitPoint);
                        } else {
                            dassert(compareWithPreviousSplitPoint <= 0,
                                    str::stream() << "Found split key larger than the previous one "
                                                     "in backwards lookup: "
                                                  << currentKey << " > " << previousSplitPoint);
                        }
                        if ((forward && compareWithPreviousSplitPoint > 0) ||
                            (!forward && compareWithPreviousSplitPoint < 0)) {
                            // 添加新的重新计算的分裂点：
                            const auto additionalKeySize =
                                currentKey.objsize() + estimatedAdditionalBytesPerItemInBSONArray;
                            if (checkMaxBSONSize(additionalKeySize)) {
                                reachedMaxBSONSize = true;
                                break;
                            }

                            splitKeys.push_back(currentKey.getOwned());
                            previousSplitPoint = splitKeys.back();
                            numScannedKeys = 0;

                            if (--nSplitPointsToReposition == 0) {
                                break;
                            }
                        } else if (compareWithPreviousSplitPoint == 0) {
                            // Don't add again the same split point in case of frequent shard key.
                            // 高频键处理：在重新计算过程中也要避免重复分裂点
                            tooFrequentKeys.insert(currentKey.getOwned());
                        }
                    }
                }
            }
        }

        // 记录查找耗时：用于性能监控和优化
        elapsedMillisToFindSplitPoints = timer.millis();

        // 大小限制达到警告：记录因为响应大小限制而提前终止的情况
        if (reachedMaxBSONSize) {
            LOGV2(5865002,
                  "Max BSON response size reached for split vector before the end of chunk",
                  logAttrs(nss),
                  "minKey"_attr = redact(prettyKey(shardKeyIdx->keyPattern(), minKey)),
                  "maxKey"_attr = redact(prettyKey(shardKeyIdx->keyPattern(), maxKey)));
        }
    }

    // Emit a warning for each frequent key
    // 高频键警告：为每个检测到的高频键输出警告
    // 这些键可能导致热点问题或分裂困难
    for (const auto& frequentKey : tooFrequentKeys) {
        LOGV2_WARNING(5865004,
                      "Possible low cardinality key detected",
                      logAttrs(nss),
                      "key"_attr = redact(prettyKey(keyPattern, frequentKey)));
    }

    // 性能警告：如果查找分裂点耗时过长，输出性能警告
    if (elapsedMillisToFindSplitPoints > serverGlobalParams.slowMS.load()) {
        LOGV2_WARNING(5865005,
                      "Finding the auto split vector completed",
                      logAttrs(nss),
                      "keyPattern"_attr = redact(keyPattern),
                      "numSplits"_attr = splitKeys.size(),
                      "duration"_attr = Milliseconds(elapsedMillisToFindSplitPoints));
    }

    // 用户限制应用：如果用户指定了分裂点数量限制，截断结果
    if (limit && splitKeys.size() > static_cast<size_t>(*limit)) {
        splitKeys.resize(*limit);
    }

    // 返回结果：分裂点向量和是否达到大小限制的标记
    return std::make_pair(std::move(splitKeys), reachedMaxBSONSize);
}

}  // namespace mongo
