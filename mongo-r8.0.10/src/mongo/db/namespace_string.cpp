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

#include "mongo/db/namespace_string.h"

#include <boost/move/utility_core.hpp>
#include <boost/optional/optional.hpp>

#include "mongo/base/parse_number.h"
#include "mongo/base/status.h"
#include "mongo/bson/timestamp.h"
#include "mongo/bson/util/builder.h"
#include "mongo/bson/util/builder_fwd.h"
#include "mongo/db/server_options.h"
// IWYU pragma: no_include "mongo/db/namespace_string_reserved.def.h"
#include "mongo/util/duration.h"

namespace mongo {
namespace {

constexpr auto listCollectionsCursorCol = "$cmd.listCollections"_sd;
constexpr auto bulkWriteCursorCol = "$cmd.bulkWrite"_sd;
constexpr auto dropPendingNSPrefix = "system.drop."_sd;

constexpr auto fle2Prefix = "enxcol_."_sd;
constexpr auto fle2EscSuffix = ".esc"_sd;
constexpr auto fle2EccSuffix = ".ecc"_sd;
constexpr auto fle2EcocSuffix = ".ecoc"_sd;
constexpr auto fle2EcocCompactSuffix = ".ecoc.compact"_sd;

// The following are namespaces in the form of config.xxx for which only one instance exist globally
// within the cluster.
static const absl::flat_hash_set<NamespaceString> globallyUniqueConfigDbCollections = {
    NamespaceString::kConfigsvrCollectionsNamespace,
    NamespaceString::kConfigsvrChunksNamespace,
    NamespaceString::kConfigDatabasesNamespace,
    NamespaceString::kConfigsvrShardsNamespace,
    NamespaceString::kConfigsvrIndexCatalogNamespace,
    NamespaceString::kConfigsvrPlacementHistoryNamespace,
    NamespaceString::kConfigChangelogNamespace,
    NamespaceString::kConfigsvrTagsNamespace,
    NamespaceString::kConfigVersionNamespace,
    NamespaceString::kConfigMongosNamespace,
    NamespaceString::kLogicalSessionsNamespace};

}  // namespace

bool NamespaceString::isListCollectionsCursorNS() const {
    return coll() == listCollectionsCursorCol;
}

bool NamespaceString::isCollectionlessAggregateNS() const {
    return coll() == kCollectionlessAggregateCollection;
}

bool NamespaceString::isLegalClientSystemNS() const {
    auto collectionName = coll();
    if (isAdminDB()) {
        if (collectionName == "system.roles")
            return true;
        if (collectionName == kServerConfigurationNamespace.coll())
            return true;
        if (collectionName == kKeysCollectionNamespace.coll())
            return true;
        if (collectionName == "system.backup_users")
            return true;
        if (collectionName == "system.new_users")
            return true;
    } else if (isConfigDB()) {
        if (collectionName == "system.sessions")
            return true;
        if (collectionName == kIndexBuildEntryNamespace.coll())
            return true;
        if (collectionName.find(".system.resharding.") != std::string::npos)
            return true;
        if (collectionName == kShardingDDLCoordinatorsNamespace.coll())
            return true;
        if (collectionName == kConfigsvrCoordinatorsNamespace.coll())
            return true;
    } else if (isLocalDB()) {
        if (collectionName == kSystemReplSetNamespace.coll())
            return true;
        if (collectionName == kLocalHealthLogNamespace.coll())
            return true;
        if (collectionName == kConfigsvrRestoreNamespace.coll())
            return true;
    }

    if (collectionName == "system.users")
        return true;
    if (collectionName == "system.js")
        return true;
    if (collectionName == kSystemDotViewsCollectionName)
        return true;
    if (isTemporaryReshardingCollection()) {
        return true;
    }
    if (isTimeseriesBucketsCollection() &&
        validCollectionName(collectionName.substr(kTimeseriesBucketsCollectionPrefix.size()))) {
        return true;
    }
    if (isChangeStreamPreImagesCollection()) {
        return true;
    }

    if (isChangeCollection()) {
        return true;
    }

    if (isSystemStatsCollection()) {
        return true;
    }

    return false;
}

/**
 * Oplog entries on 'system.views' should also be processed one at a time. View catalog immediately
 * reflects changes for each oplog entry so we can see inconsistent view catalog if multiple oplog
 * entries on 'system.views' are being applied out of the original order.
 *
 * Process updates to 'admin.system.version' individually as well so the secondary's FCV when
 * processing each operation matches the primary's when committing that operation.
 *
 * Process updates to 'config.shardMergeRecipients' individually so they serialize after
 * inserts into 'config.donatedFiles.<migrationId>'.
 *
 * Oplog entries on 'config.shards' should be processed one at a time, otherwise the in-memory state
 * that its kept on the TopologyTimeTicker might be wrong.
 *
 * Serialize updates to 'config.tenantMigrationDonors' and 'config.shardSplitDonors' to avoid races
 * with creating tenant access blockers on secondaries.
 */
bool NamespaceString::mustBeAppliedInOwnOplogBatch() const {
    auto ns = this->ns();
    return isSystemDotViews() || isServerConfigurationCollection() || isPrivilegeCollection() ||
        ns == kDonorReshardingOperationsNamespace.ns() ||
        ns == kForceOplogBatchBoundaryNamespace.ns() ||
        ns == kTenantMigrationDonorsNamespace.ns() || ns == kShardMergeRecipientsNamespace.ns() ||
        ns == kTenantMigrationRecipientsNamespace.ns() || ns == kShardSplitDonorsNamespace.ns() ||
        ns == kConfigsvrShardsNamespace.ns();
}

NamespaceString NamespaceString::makeBulkWriteNSS(const boost::optional<TenantId>& tenantId) {
    return NamespaceString(tenantId, DatabaseName::kAdmin.db(omitTenant), bulkWriteCursorCol);
}

NamespaceString NamespaceString::makeClusterParametersNSS(
    const boost::optional<TenantId>& tenantId) {
    return tenantId
        ? NamespaceString(tenantId, DatabaseName::kConfig.db(omitTenant), "clusterParameters")
        : kClusterParametersNamespace;
}

NamespaceString NamespaceString::makeSystemDotViewsNamespace(const DatabaseName& dbName) {
    return NamespaceString(dbName, kSystemDotViewsCollectionName);
}

NamespaceString NamespaceString::makeSystemDotProfileNamespace(const DatabaseName& dbName) {
    return NamespaceString(dbName, kSystemDotProfileCollectionName);
}

NamespaceString NamespaceString::makeListCollectionsNSS(const DatabaseName& dbName) {
    NamespaceString nss(dbName, listCollectionsCursorCol);
    dassert(nss.isValid());
    dassert(nss.isListCollectionsCursorNS());
    return nss;
}

NamespaceString NamespaceString::makeGlobalConfigCollection(StringData collName) {
    return NamespaceString(DatabaseName::kConfig, collName);
}

NamespaceString NamespaceString::makeLocalCollection(StringData collName) {
    return NamespaceString(DatabaseName::kLocal, collName);
}

NamespaceString NamespaceString::makeCollectionlessAggregateNSS(const DatabaseName& dbName) {
    NamespaceString nss(dbName, kCollectionlessAggregateCollection);
    dassert(nss.isValid());
    dassert(nss.isCollectionlessAggregateNS());
    return nss;
}

NamespaceString NamespaceString::makeChangeCollectionNSS(
    const boost::optional<TenantId>& tenantId) {
    return NamespaceString{tenantId, DatabaseName::kConfig.db(omitTenant), kChangeCollectionName};
}

NamespaceString NamespaceString::makeGlobalIndexNSS(const UUID& id) {
    return NamespaceString(DatabaseName::kSystem,
                           NamespaceString::kGlobalIndexCollectionPrefix + id.toString());
}

NamespaceString NamespaceString::makePreImageCollectionNSS(
    const boost::optional<TenantId>& tenantId) {
    return NamespaceString{
        tenantId, DatabaseName::kConfig.db(omitTenant), kPreImagesCollectionName};
}

NamespaceString NamespaceString::makeReshardingLocalOplogBufferNSS(
    const UUID& existingUUID, const std::string& donorShardId) {
    return NamespaceString(DatabaseName::kConfig,
                           "localReshardingOplogBuffer." + existingUUID.toString() + "." +
                               donorShardId);
}

NamespaceString NamespaceString::makeReshardingLocalConflictStashNSS(
    const UUID& existingUUID, const std::string& donorShardId) {
    return NamespaceString(DatabaseName::kConfig,
                           "localReshardingConflictStash." + existingUUID.toString() + "." +
                               donorShardId);
}

NamespaceString NamespaceString::makeTenantUsersCollection(
    const boost::optional<TenantId>& tenantId) {
    return NamespaceString(
        tenantId, DatabaseName::kAdmin.db(omitTenant), NamespaceString::kSystemUsers);
}

NamespaceString NamespaceString::makeTenantRolesCollection(
    const boost::optional<TenantId>& tenantId) {
    return NamespaceString(
        tenantId, DatabaseName::kAdmin.db(omitTenant), NamespaceString::kSystemRoles);
}

NamespaceString NamespaceString::makeCommandNamespace(const DatabaseName& dbName) {
    return NamespaceString(dbName, "$cmd");
}

std::string NamespaceString::getSisterNS(StringData local) const {
    MONGO_verify(local.size() && local[0] != '.');
    return db_deprecated().toString() + "." + local.toString();
}

void NamespaceString::serializeCollectionName(BSONObjBuilder* builder, StringData fieldName) const {
    if (isCollectionlessAggregateNS()) {
        builder->append(fieldName, 1);
    } else {
        builder->append(fieldName, coll());
    }
}

bool NamespaceString::isDropPendingNamespace() const {
    return coll().startsWith(dropPendingNSPrefix);
}

NamespaceString NamespaceString::makeDropPendingNamespace(const repl::OpTime& opTime) const {
    StringBuilder ss;
    ss << db_deprecated() << "." << dropPendingNSPrefix;
    ss << opTime.getSecs() << "i" << opTime.getTimestamp().getInc() << "t" << opTime.getTerm();
    ss << "." << coll();
    return NamespaceString(tenantId(), ss.stringData());
}

StatusWith<repl::OpTime> NamespaceString::getDropPendingNamespaceOpTime() const {
    if (!isDropPendingNamespace()) {
        return Status(ErrorCodes::BadValue, fmt::format("Not a drop-pending namespace: {}", ns()));
    }

    auto collectionName = coll();
    auto opTimeBeginIndex = dropPendingNSPrefix.size();
    auto opTimeEndIndex = collectionName.find('.', opTimeBeginIndex);
    auto opTimeStr = std::string::npos == opTimeEndIndex
        ? collectionName.substr(opTimeBeginIndex)
        : collectionName.substr(opTimeBeginIndex, opTimeEndIndex - opTimeBeginIndex);

    auto incrementSeparatorIndex = opTimeStr.find('i');
    if (std::string::npos == incrementSeparatorIndex) {
        return Status(ErrorCodes::FailedToParse,
                      fmt::format("Missing 'i' separator in drop-pending namespace: {}", ns()));
    }

    auto termSeparatorIndex = opTimeStr.find('t', incrementSeparatorIndex);
    if (std::string::npos == termSeparatorIndex) {
        return Status(ErrorCodes::FailedToParse,
                      fmt::format("Missing 't' separator in drop-pending namespace: {}", ns()));
    }

    long long seconds;
    auto status = NumberParser{}(opTimeStr.substr(0, incrementSeparatorIndex), &seconds);
    if (!status.isOK()) {
        return status.withContext(
            fmt::format("Invalid timestamp seconds in drop-pending namespace: {}", ns()));
    }

    unsigned int increment;
    status = NumberParser{}(opTimeStr.substr(incrementSeparatorIndex + 1,
                                             termSeparatorIndex - (incrementSeparatorIndex + 1)),
                            &increment);
    if (!status.isOK()) {
        return status.withContext(
            fmt::format("Invalid timestamp increment in drop-pending namespace: {}", ns()));
    }

    long long term;
    status = mongo::NumberParser{}(opTimeStr.substr(termSeparatorIndex + 1), &term);
    if (!status.isOK()) {
        return status.withContext(fmt::format("Invalid term in drop-pending namespace: {}", ns()));
    }

    return repl::OpTime(Timestamp(Seconds(seconds), increment), term);
}

bool NamespaceString::isNamespaceAlwaysUntracked() const {
    // Local and admin never have sharded collections
    if (isLocalDB() || isAdminDB())
        return true;

    // Config can only have the system.sessions as sharded
    if (isConfigDB())
        return *this != NamespaceString::kLogicalSessionsNamespace;

    if (isSystem()) {
        // Only some system collections (<DB>.system.<COLL>) can be sharded,
        // all the others are always unsharded.
        // This list does not contain 'config.system.sessions' because we already check it above
        return !isTemporaryReshardingCollection() && !isTimeseriesBucketsCollection();
    }

    return false;
}

bool NamespaceString::isShardLocalNamespace() const {
    if (isLocalDB() || isAdminDB()) {
        return true;
    }

    if (isConfigDB()) {
        return !globallyUniqueConfigDbCollections.contains(*this);
    }

    if (isSystem()) {
        // Only some db.system.xxx collections are cluster global.
        const bool isUniqueInstanceSystemCollection =
            isTemporaryReshardingCollection() || isTimeseriesBucketsCollection();
        return !isUniqueInstanceSystemCollection;
    }

    return false;
}

bool NamespaceString::isConfigDotCacheDotChunks() const {
    return db_deprecated() == "config" && coll().startsWith("cache.chunks.");
}

bool NamespaceString::isReshardingLocalOplogBufferCollection() const {
    return db_deprecated() == "config" && coll().startsWith(kReshardingLocalOplogBufferPrefix);
}

bool NamespaceString::isReshardingConflictStashCollection() const {
    return db_deprecated() == "config" && coll().startsWith(kReshardingConflictStashPrefix);
}

bool NamespaceString::isTemporaryReshardingCollection() const {
    return coll().startsWith(kTemporaryTimeseriesReshardingCollectionPrefix) ||
        coll().startsWith(kTemporaryReshardingCollectionPrefix);
}

bool NamespaceString::isTimeseriesBucketsCollection() const {
    return coll().startsWith(kTimeseriesBucketsCollectionPrefix);
}

bool NamespaceString::isChangeStreamPreImagesCollection() const {
    return isConfigDB() && coll() == kPreImagesCollectionName;
}

bool NamespaceString::isChangeCollection() const {
    return isConfigDB() && coll() == kChangeCollectionName;
}

bool NamespaceString::isConfigImagesCollection() const {
    return ns() == kConfigImagesNamespace.ns();
}

bool NamespaceString::isConfigTransactionsCollection() const {
    return ns() == kSessionTransactionsTableNamespace.ns();
}

bool NamespaceString::isFLE2StateCollection() const {
    return coll().startsWith(fle2Prefix) &&
        (coll().endsWith(fle2EscSuffix) || coll().endsWith(fle2EccSuffix) ||
         coll().endsWith(fle2EcocSuffix) || coll().endsWith(fle2EcocCompactSuffix));
}

bool NamespaceString::isFLE2StateCollection(StringData coll) {
    return coll.startsWith(fle2Prefix) &&
        (coll.endsWith(fle2EscSuffix) || coll.endsWith(fle2EccSuffix) ||
         coll.endsWith(fle2EcocSuffix));
}

bool NamespaceString::isOplogOrChangeCollection() const {
    return isOplog() || isChangeCollection();
}

bool NamespaceString::isSystemStatsCollection() const {
    return coll().startsWith(kStatisticsCollectionPrefix);
}

bool NamespaceString::isOutTmpBucketsCollection() const {
    return isTimeseriesBucketsCollection() &&
        getTimeseriesViewNamespace().coll().startsWith(kOutTmpCollectionPrefix);
}

NamespaceString NamespaceString::makeTimeseriesBucketsNamespace() const {
    return {dbName(), kTimeseriesBucketsCollectionPrefix.toString() + coll()};
}

NamespaceString NamespaceString::getTimeseriesViewNamespace() const {
    invariant(isTimeseriesBucketsCollection(), ns());
    return {dbName(), coll().substr(kTimeseriesBucketsCollectionPrefix.size())};
}

/**
 * 隐式复制命名空间检测函数 - MongoDB复制系统的特殊集合识别机制
 * 
 * 核心功能：
 * 1. 识别需要隐式复制但不生成完整oplog条目的特殊系统集合
 * 2. 区分隐式复制与显式复制的集合类型，支持复制行为差异化处理
 * 3. 为变更流、OpObserver和复制协调器提供集合类型判断依据
 * 4. 确保特殊系统集合的复制一致性和数据完整性保证
 * 5. 支持分片环境下的元数据集合复制策略决策
 * 
 * 设计原理：
 * - 选择性复制：仅复制操作的子集，减少oplog开销和网络传输
 * - 系统集合特化：针对特殊用途的集合采用定制化的复制策略
 * - 复制层次化：区分用户数据复制与系统元数据复制的不同需求
 * - 一致性保证：确保即使是隐式复制也满足副本集的一致性要求
 * 
 * 隐式复制特点：
 * - 不生成完整的oplog条目，但保证数据复制
 * - 通过特殊的复制机制同步到从节点
 * - 主要用于系统内部元数据和状态信息
 * - 对用户透明，不影响业务逻辑
 * 
 * 适用场景：
 * - 变更流预镜像集合的部分写入复制
 * - 配置数据库中镜像集合的状态同步
 * - 变更集合的增量复制和压缩操作
 * - 系统集合的选择性元数据复制
 * 
 * 重要约束：
 * - 隐式复制集合必须同时是可复制集合（isReplicated() == true）
 * - 仅限于config数据库中的特定系统集合
 * - 不支持用户自定义集合的隐式复制
 * - 隐式复制行为由MongoDB内核控制，不可配置
 * 
 * @return bool 如果命名空间需要隐式复制返回true，否则返回false
 */
bool NamespaceString::isImplicitlyReplicated() const {
    // 配置数据库检查：隐式复制仅适用于config数据库中的特殊集合
    // 原因：config数据库包含MongoDB集群的关键元数据和系统状态信息
    // 这些信息需要在副本集间保持同步，但不需要完整的oplog记录
    if (db_deprecated() == DatabaseName::kConfig.db(omitTenant)) {
        // 变更流预镜像集合检查：用于存储变更流操作的文档镜像
        // 功能：在文档被修改或删除前保存其完整镜像
        // 隐式复制原因：
        // 1. 预镜像数据量大，完整oplog复制成本高
        // 2. 主要用于变更流功能，不是核心业务数据
        // 3. 可以通过特殊机制实现选择性同步
        if (isChangeStreamPreImagesCollection() || 
            // 配置镜像集合检查：存储配置变更的镜像信息
            // 用途：跟踪分片配置、索引状态等关键配置的历史版本
            // 隐式复制原因：配置镜像主要用于审计和回滚，不需要实时oplog同步
            isConfigImagesCollection() ||
            // 变更集合检查：存储变更流事件和压缩后的变更记录
            // 特点：包含增量变更、事件聚合和数据压缩信息
            // 隐式复制原因：
            // 1. 变更记录具有时效性，过期数据可以选择性清理
            // 2. 压缩操作产生的中间状态不需要完整复制
            // 3. 通过批量同步机制更高效
            isChangeCollection()) {
            
            // Implicitly replicated namespaces are replicated, although they only replicate a
            // subset of writes.
            // 一致性断言：确保隐式复制的集合同时也是可复制的集合
            // 重要性：
            // 1. 维护复制系统的逻辑一致性
            // 2. 确保隐式复制集合仍然参与副本集的数据同步
            // 3. 防止出现既不复制又不隐式复制的系统集合
            // 4. 为复制协调器提供正确的集合分类信息
            invariant(isReplicated());
            return true;
        }
    }

    // 默认情况：大多数集合不需要隐式复制
    // 包括：
    // 1. 所有用户数据集合 - 使用标准oplog复制
    // 2. 其他数据库中的系统集合 - 根据具体情况决定复制策略
    // 3. local数据库集合 - 通常不复制或有特殊处理
    // 4. admin数据库集合 - 使用标准复制机制
    return false;
}

bool NamespaceString::isReplicated() const {
    if (isLocalDB()) {
        return false;
    }

    // Of collections not in the `local` database, only `system` collections might not be
    // replicated.
    if (!isSystem()) {
        return true;
    }

    if (isSystemDotProfile()) {
        return false;
    }

    // E.g: `system.version` is replicated.
    return true;
}

std::string NamespaceStringOrUUID::toStringForErrorMsg() const {
    if (isNamespaceString()) {
        return nss().toStringForErrorMsg();
    }

    return uuid().toString();
}

std::string toStringForLogging(const NamespaceStringOrUUID& nssOrUUID) {
    if (nssOrUUID.isNamespaceString()) {
        return toStringForLogging(nssOrUUID.nss());
    }

    return nssOrUUID.uuid().toString();
}

void NamespaceStringOrUUID::serialize(BSONObjBuilder* builder, StringData fieldName) const {
    if (const NamespaceString* nss = get_if<NamespaceString>(&_nssOrUUID)) {
        builder->append(fieldName, nss->coll());
    } else {
        get<1>(get<UUIDWithDbName>(_nssOrUUID)).appendToBuilder(builder, fieldName);
    }
}

}  // namespace mongo
