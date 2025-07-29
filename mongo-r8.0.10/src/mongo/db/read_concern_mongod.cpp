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


#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <tuple>
#include <utility>

#include <boost/move/utility_core.hpp>
#include <boost/optional/optional.hpp>

#include "mongo/base/error_codes.h"
#include "mongo/base/shim.h"
#include "mongo/base/status.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/timestamp.h"
#include "mongo/db/catalog_raii.h"
#include "mongo/db/client.h"
#include "mongo/db/cluster_role.h"
#include "mongo/db/concurrency/d_concurrency.h"
#include "mongo/db/concurrency/exception_util.h"
#include "mongo/db/concurrency/lock_manager_defs.h"
#include "mongo/db/curop.h"
#include "mongo/db/curop_failpoint_helpers.h"
#include "mongo/db/database_name.h"
#include "mongo/db/logical_time.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/op_observer/op_observer.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/read_concern_mongod_gen.h"
#include "mongo/db/repl/member_state.h"
#include "mongo/db/repl/optime.h"
#include "mongo/db/repl/read_concern_args.h"
#include "mongo/db/repl/read_concern_level.h"
#include "mongo/db/repl/repl_client_info.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/db/repl/speculative_majority_read_info.h"
#include "mongo/db/repl/tenant_migration_access_blocker_util.h"
#include "mongo/db/server_options.h"
#include "mongo/db/service_context.h"
#include "mongo/db/storage/recovery_unit.h"
#include "mongo/db/storage/write_unit_of_work.h"
#include "mongo/db/transaction_resources.h"
#include "mongo/db/vector_clock.h"
#include "mongo/db/write_concern_options.h"
#include "mongo/executor/task_executor.h"
#include "mongo/logv2/log.h"
#include "mongo/logv2/log_attr.h"
#include "mongo/logv2/log_component.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/platform/mutex.h"
#include "mongo/rpc/get_status_from_command_result.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/concurrency/notification.h"
#include "mongo/util/decorable.h"
#include "mongo/util/duration.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/str.h"
#include "mongo/util/time_support.h"
#include "mongo/util/timer.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kCommand


namespace mongo {

namespace {

MONGO_FAIL_POINT_DEFINE(hangBeforeLinearizableReadConcern);

/**
 *  Synchronize writeRequests
 */

class WriteRequestSynchronizer;
const auto getWriteRequestsSynchronizer =
    ServiceContext::declareDecoration<WriteRequestSynchronizer>();

class WriteRequestSynchronizer {
public:
    WriteRequestSynchronizer() = default;

    /**
     * Returns a tuple <false, existingWriteRequest> if it can  find the one that happened after or
     * at clusterTime.
     * Returns a tuple <true, newWriteRequest> otherwise.
     */
    std::tuple<bool, std::shared_ptr<Notification<Status>>> getOrCreateWriteRequest(
        LogicalTime clusterTime) {
        stdx::unique_lock<Latch> lock(_mutex);
        auto lastEl = _writeRequests.rbegin();
        if (lastEl != _writeRequests.rend() && lastEl->first >= clusterTime.asTimestamp()) {
            return std::make_tuple(false, lastEl->second);
        } else {
            auto newWriteRequest = std::make_shared<Notification<Status>>();
            _writeRequests[clusterTime.asTimestamp()] = newWriteRequest;
            return std::make_tuple(true, newWriteRequest);
        }
    }

    /**
     * Erases writeRequest that happened at clusterTime
     */
    void deleteWriteRequest(LogicalTime clusterTime) {
        stdx::unique_lock<Latch> lock(_mutex);
        auto el = _writeRequests.find(clusterTime.asTimestamp());
        invariant(el != _writeRequests.end());
        invariant(el->second);
        el->second.reset();
        _writeRequests.erase(el);
    }

private:
    Mutex _mutex = MONGO_MAKE_LATCH("WriteRequestSynchronizer::_mutex");
    std::map<Timestamp, std::shared_ptr<Notification<Status>>> _writeRequests;
};

/**
 *  Schedule a write via appendOplogNote command to the primary of this replica set.
 */
Status makeNoopWriteIfNeeded(OperationContext* opCtx,
                             LogicalTime clusterTime,
                             const DatabaseName& dbName) {
    repl::ReplicationCoordinator* const replCoord = repl::ReplicationCoordinator::get(opCtx);
    invariant(replCoord->getSettings().isReplSet());

    auto& writeRequests = getWriteRequestsSynchronizer(opCtx->getClient()->getServiceContext());

    auto lastWrittenOpTime = LogicalTime(replCoord->getMyLastWrittenOpTime().getTimestamp());

    // secondaries may lag primary so wait first to avoid unnecessary noop writes.
    if (clusterTime > lastWrittenOpTime && replCoord->getMemberState().secondary()) {
        auto deadline = Date_t::now() + Milliseconds(waitForSecondaryBeforeNoopWriteMS.load());
        auto waitStatus = replCoord->waitUntilOpTimeWrittenUntil(opCtx, clusterTime, deadline);
        lastWrittenOpTime = LogicalTime(replCoord->getMyLastWrittenOpTime().getTimestamp());
        if (!waitStatus.isOK()) {
            LOGV2_DEBUG(20986,
                        1,
                        "Wait for clusterTime: {clusterTime} until deadline: {deadline} failed "
                        "with {waitStatus}",
                        "clusterTime"_attr = clusterTime.toString(),
                        "deadline"_attr = deadline,
                        "waitStatus"_attr = waitStatus.toString());
        }
    }

    auto status = Status::OK();
    int remainingAttempts = 3;
    // this loop addresses the case when two or more threads need to advance the opLog time but the
    // one that waits for the notification gets the later clusterTime, so when the request finishes
    // it needs to be repeated with the later time.
    while (clusterTime > lastWrittenOpTime) {
        // Standalone replica set, so there is no need to advance the OpLog on the primary. The only
        // exception is after a tenant migration because the target time may be from the other
        // replica set and is not guaranteed to be in the oplog of this node's set.
        if (serverGlobalParams.clusterRole.has(ClusterRole::None) &&
            !tenant_migration_access_blocker::hasActiveTenantMigration(opCtx, dbName)) {
            return Status::OK();
        }

        if (!remainingAttempts--) {
            std::stringstream ss;
            ss << "Requested clusterTime " << clusterTime.toString()
               << " is greater than the last primary OpTime: " << lastWrittenOpTime.toString()
               << " no retries left";
            return Status(ErrorCodes::InternalError, ss.str());
        }

        auto myWriteRequest = writeRequests.getOrCreateWriteRequest(clusterTime);
        if (std::get<0>(myWriteRequest)) {  // Its a new request
            try {
                LOGV2_DEBUG(20987,
                            2,
                            "New appendOplogNote request on clusterTime: {clusterTime} remaining "
                            "attempts: {remainingAttempts}",
                            "clusterTime"_attr = clusterTime.toString(),
                            "remainingAttempts"_attr = remainingAttempts);

                auto onRemoteCmdScheduled = [](executor::TaskExecutor::CallbackHandle handle) {
                };
                auto onRemoteCmdComplete = [](executor::TaskExecutor::CallbackHandle handle) {
                };
                auto appendOplogNoteResponse = replCoord->runCmdOnPrimaryAndAwaitResponse(
                    opCtx,
                    DatabaseName::kAdmin,
                    BSON("appendOplogNote"
                         << 1 << "maxClusterTime" << clusterTime.asTimestamp() << "data"
                         << BSON("noop write for afterClusterTime read concern" << 1)
                         << WriteConcernOptions::kWriteConcernField
                         << WriteConcernOptions::Acknowledged),
                    onRemoteCmdScheduled,
                    onRemoteCmdComplete);

                status = getStatusFromCommandResult(appendOplogNoteResponse);
                std::get<1>(myWriteRequest)->set(status);
                writeRequests.deleteWriteRequest(clusterTime);
            } catch (const DBException& ex) {
                status = ex.toStatus();
                // signal the writeRequest to unblock waiters
                std::get<1>(myWriteRequest)->set(status);
                writeRequests.deleteWriteRequest(clusterTime);
            }
        } else {
            LOGV2_DEBUG(20988,
                        2,
                        "Join appendOplogNote request on clusterTime: {clusterTime} remaining "
                        "attempts: {remainingAttempts}",
                        "clusterTime"_attr = clusterTime.toString(),
                        "remainingAttempts"_attr = remainingAttempts);
            try {
                status = std::get<1>(myWriteRequest)->get(opCtx);
            } catch (const DBException& ex) {
                return ex.toStatus();
            }
        }
        // If the write status is ok need to wait for the oplog to replicate.
        if (status.isOK()) {
            return status;
        }

        // If the write failed with StaleClusterTime it means that the noop write to the primary was
        // not necessary to bump the clusterTime. It could be a race where the secondary decides to
        // issue the noop write while some writes have already happened on the primary that have
        // bumped the clusterTime beyond the 'clusterTime' the noop write requested.
        if (status == ErrorCodes::StaleClusterTime) {
            LOGV2_DEBUG(54102,
                        2,
                        "appendOplogNote request on clusterTime {clusterTime} failed with "
                        "StaleClusterTime",
                        "clusterTime"_attr = clusterTime.asTimestamp());
            return Status::OK();
        }

        lastWrittenOpTime = LogicalTime(replCoord->getMyLastWrittenOpTime().getTimestamp());
    }
    // This is when the noop write failed but the opLog caught up to clusterTime by replicating.
    if (!status.isOK()) {
        LOGV2_DEBUG(20989,
                    1,
                    "Reached clusterTime {lastWrittenOpTime} but failed noop write due to {error}",
                    "lastWrittenOpTime"_attr = lastWrittenOpTime.toString(),
                    "error"_attr = status.toString());
    }
    return Status::OK();
}

/**
 * Evaluates if it's safe for the command to ignore prepare conflicts.
 */
bool canIgnorePrepareConflicts(OperationContext* opCtx,
                               const repl::ReadConcernArgs& readConcernArgs) {
    if (opCtx->inMultiDocumentTransaction()) {
        return false;
    }

    auto readConcernLevel = readConcernArgs.getLevel();

    // Only these read concern levels are eligible for ignoring prepare conflicts.
    if (readConcernLevel != repl::ReadConcernLevel::kLocalReadConcern &&
        readConcernLevel != repl::ReadConcernLevel::kAvailableReadConcern &&
        readConcernLevel != repl::ReadConcernLevel::kMajorityReadConcern) {
        return false;
    }

    auto afterClusterTime = readConcernArgs.getArgsAfterClusterTime();
    auto atClusterTime = readConcernArgs.getArgsAtClusterTime();

    if (afterClusterTime || atClusterTime) {
        return false;
    }

    return true;
}

void setPrepareConflictBehaviorForReadConcernImpl(OperationContext* opCtx,
                                                  const repl::ReadConcernArgs& readConcernArgs,
                                                  PrepareConflictBehavior prepareConflictBehavior) {
    // DBDirectClient should inherit whether or not to ignore prepare conflicts from its parent.
    if (opCtx->getClient()->isInDirectClient()) {
        return;
    }

    // Enforce prepare conflict behavior if the command is not eligible to ignore prepare conflicts.
    if (!(prepareConflictBehavior == PrepareConflictBehavior::kEnforce ||
          canIgnorePrepareConflicts(opCtx, readConcernArgs))) {
        prepareConflictBehavior = PrepareConflictBehavior::kEnforce;
    }

    shard_role_details::getRecoveryUnit(opCtx)->setPrepareConflictBehavior(prepareConflictBehavior);
}

/**
 * waitForReadConcernImpl 函数的作用：
 * MongoDB mongod 环境中读关注点的具体实现，确保读操作满足指定的一致性级别要求。
 * 
 * 核心功能：
 * 1. 读关注点级别验证：验证并处理不同级别的读关注点（local、majority、linearizable、snapshot）
 * 2. 集群时间协调：处理 afterClusterTime 和 atClusterTime 参数，确保因果一致性
 * 3. 复制集状态等待：与复制协调器交互，等待指定的 OpTime 或快照可用
 * 4. 存储引擎集成：设置适当的时间戳读取源，确保读取一致性
 * 5. 线性化读取支持：为 linearizable 读关注点提供特殊处理
 * 6. 推测性读取优化：支持推测性 majority 读取以提高性能
 * 
 * 一致性保证：
 * - local：读取本地最新数据，无需等待复制
 * - available：读取可用数据，适用于分片环境
 * - majority：等待数据被大多数节点确认
 * - linearizable：提供线性化一致性保证
 * - snapshot：在指定时间戳提供快照隔离
 * 
 * 时间戳处理：
 * - afterClusterTime：等待集群时间达到指定值后读取
 * - atClusterTime：在精确的集群时间点读取
 * - 自动生成 noop 写入以推进集群时间（如需要）
 * 
 * 特殊场景处理：
 * - 独立节点的限制检查
 * - 复制集成员状态验证
 * - 事务中的读关注点处理
 * - 租户迁移期间的特殊逻辑
 * 
 * 该函数是 mongod 读一致性的核心实现，通过弱符号注册机制被统一接口调用。
 */
Status waitForReadConcernImpl(OperationContext* opCtx,
                              const repl::ReadConcernArgs& readConcernArgs,
                              const DatabaseName& dbName,
                              bool allowAfterClusterTime) {
    // If we are in a direct client that's holding a global lock, then this means it is illegal to
    // wait for read concern. This is fine, since the outer operation should have handled waiting
    // for read concern. We don't want to ignore prepare conflicts because reads in transactions
    // should block on prepared transactions.
    //
    // 直接客户端锁定检查：
    // 功能：检测是否在持有全局锁的直接客户端中执行
    // 原理：直接客户端表示内部操作，外层操作应已处理读关注点等待
    // 目的：避免嵌套等待导致的死锁，确保事务中读取正确阻塞预备事务
    if (opCtx->getClient()->isInDirectClient() &&
        shard_role_details::getLocker(opCtx)->isLocked()) {
        return Status::OK();
    }

    // 获取复制协调器：
    // 功能：获取当前节点的复制集协调器实例
    // 用途：后续的读关注点处理需要与复制集状态交互
    // 要求：在 mongod 环境中，复制协调器必须存在
    repl::ReplicationCoordinator* const replCoord = repl::ReplicationCoordinator::get(opCtx);
    invariant(replCoord);

    // 线性化读关注点特殊处理：
    // 功能：对 linearizable 读关注点进行预检查和限制
    // 要求：必须在复制集环境中，且必须在主节点上执行
    // 限制：不支持 afterOpTime 参数，因为线性化读取有特殊的时序要求
    if (readConcernArgs.getLevel() == repl::ReadConcernLevel::kLinearizableReadConcern) {
        if (!replCoord->getSettings().isReplSet()) {
            // For standalone nodes, Linearizable Read is not supported.
            return {ErrorCodes::NotAReplicaSet,
                    "node needs to be a replica set member to use read concern"};
        }

        if (readConcernArgs.getArgsOpTime()) {
            return {ErrorCodes::FailedToParse,
                    "afterOpTime not compatible with linearizable read concern"};
        }

        if (!replCoord->getMemberState().primary()) {
            return {ErrorCodes::NotWritablePrimary,
                    "cannot satisfy linearizable read concern on non-primary node"};
        }
    }

    // 快照读关注点特殊处理：
    // 功能：对 snapshot 读关注点进行环境检查
    // 要求：必须在复制集环境中，且需要启用 majority 读关注点支持
    // 例外：多文档事务中可以使用，即使 enableMajorityReadConcern=false
    if (readConcernArgs.getLevel() == repl::ReadConcernLevel::kSnapshotReadConcern) {
        if (!replCoord->getSettings().isReplSet()) {
            return {ErrorCodes::NotAReplicaSet,
                    "node needs to be a replica set member to use readConcern: snapshot"};
        }
        if (!opCtx->inMultiDocumentTransaction() && !serverGlobalParams.enableMajorityReadConcern) {
            return {ErrorCodes::ReadConcernMajorityNotEnabled,
                    "read concern level snapshot is not supported when "
                    "enableMajorityReadConcern=false"};
        }
    }

    // 集群时间参数提取：
    // 功能：从读关注点参数中提取时间戳相关设置
    // afterClusterTime：指定读取的最小集群时间（因果一致性）
    // atClusterTime：指定读取的精确集群时间（快照读取）
    auto afterClusterTime = readConcernArgs.getArgsAfterClusterTime();
    auto atClusterTime = readConcernArgs.getArgsAtClusterTime();

    // afterClusterTime 权限检查：
    // 功能：检查当前命令是否允许使用 afterClusterTime 参数
    // 目的：某些命令不支持因果一致性，需要在此处阻止
    if (afterClusterTime) {
        if (!allowAfterClusterTime) {
            return {ErrorCodes::InvalidOptions, "afterClusterTime is not allowed for this command"};
        }
    }

    // 主要的读关注点处理逻辑：
    // 功能：处理非空的读关注点参数
    // 包括：集群时间验证、noop 写入、OpTime 等待等
    if (!readConcernArgs.isEmpty()) {
        // 时间戳互斥性检查：
        // 功能：确保 afterClusterTime 和 atClusterTime 不能同时指定
        // 原理：这两个参数有不同的语义，不能混合使用
        invariant(!afterClusterTime || !atClusterTime);
        auto targetClusterTime = afterClusterTime ? afterClusterTime : atClusterTime;

        // 集群时间相关处理：
        // 功能：验证和处理指定的集群时间参数
        if (targetClusterTime) {
            std::string readConcernName = afterClusterTime ? "afterClusterTime" : "atClusterTime";

            // 复制集环境检查：
            // 功能：确保只有在复制集环境中才能使用集群时间
            // 原理：独立节点没有集群时间概念
            if (!replCoord->getSettings().isReplSet()) {
                return {ErrorCodes::IllegalOperation,
                        str::stream() << "Cannot specify " << readConcernName
                                      << " readConcern without replication enabled"};
            }

            // We must read the member state before obtaining the cluster time. Otherwise, we can
            // run into a race where the cluster time is read as uninitialized, but the member state
            // is set to RECOVERING by another thread before we invariant that the node is in
            // STARTUP or STARTUP2.
            //
            // 成员状态和集群时间获取：
            // 功能：按正确顺序获取成员状态和集群时间，避免竞争条件
            // 原理：必须先读取成员状态，再获取集群时间，防止状态不一致
            // 目的：确保在启动阶段正确处理未初始化的集群时间
            const auto memberState = replCoord->getMemberState();

            const auto currentTime = VectorClock::get(opCtx)->getTime();
            const auto clusterTime = currentTime.clusterTime();
            
            // 集群时间有效性检查：
            // 功能：检查当前集群时间是否已初始化
            // 条件：只有在 STARTUP 或 STARTUP2 状态下，集群时间才允许未初始化
            if (!VectorClock::isValidComponentTime(clusterTime)) {
                // currentTime should only be uninitialized if we are in startup recovery or initial
                // sync.
                invariant(memberState.startup() || memberState.startup2());
                return {ErrorCodes::NotPrimaryOrSecondary,
                        str::stream() << "Current clusterTime is uninitialized, cannot service the "
                                         "requested clusterTime. Requested clusterTime: "
                                      << targetClusterTime->toString()
                                      << "; current clusterTime: " << clusterTime.toString()};
            }
            
            // 集群时间范围检查：
            // 功能：确保请求的集群时间不超过当前集群时间
            // 原理：不能读取"未来"的数据，只能读取已经发生的数据
            if (clusterTime < *targetClusterTime) {
                return {ErrorCodes::InvalidOptions,
                        str::stream() << "readConcern " << readConcernName
                                      << " value must not be greater than the current clusterTime. "
                                         "Requested clusterTime: "
                                      << targetClusterTime->toString()
                                      << "; current clusterTime: " << clusterTime.toString()};
            }

            // Noop 写入生成（如需要）：
            // 功能：如果目标集群时间超过本地最后写入时间，生成 noop 写入
            // 目的：确保本地 oplog 时间戳满足读关注点要求
            // 机制：向主节点发送 appendOplogNote 命令
            auto status = makeNoopWriteIfNeeded(opCtx, *targetClusterTime, dbName);
            if (!status.isOK()) {
                LOGV2(20990,
                      "Failed noop write",
                      "targetClusterTime"_attr = targetClusterTime,
                      "error"_attr = status);
            }
        }

        // OpTime 等待处理：
        // 功能：等待复制集达到指定的 OpTime 或读关注点条件
        // 适用：复制集环境，或非 afterClusterTime 的情况
        // 原理：通过复制协调器等待数据复制到足够多的节点
        if (replCoord->getSettings().isReplSet() || !afterClusterTime) {
            auto status = replCoord->waitUntilOpTimeForRead(opCtx, readConcernArgs);
            if (!status.isOK()) {
                return status;
            }
        }
    }

    // 存储引擎读取源设置：
    // 功能：根据读关注点类型设置合适的时间戳读取源
    // 目的：确保存储引擎在正确的时间点读取数据
    auto ru = shard_role_details::getRecoveryUnit(opCtx);
    
    // atClusterTime 精确时间戳读取：
    // 功能：设置存储引擎在指定的精确时间戳读取
    // 用途：支持快照隔离和时间点恢复
    if (atClusterTime) {
        ru->setTimestampReadSource(RecoveryUnit::ReadSource::kProvided,
                                   atClusterTime->asTimestamp());
    } 
    // 快照读关注点的自动时间戳选择：
    // 功能：为 snapshot 读关注点自动选择合适的快照时间戳
    // 条件：复制集环境且不在多文档事务中
    // 原理：使用当前提交的快照 OpTime
    else if (readConcernArgs.getLevel() == repl::ReadConcernLevel::kSnapshotReadConcern &&
               replCoord->getSettings().isReplSet() && !opCtx->inMultiDocumentTransaction()) {
        auto opTime = replCoord->getCurrentCommittedSnapshotOpTime();
        uassert(ErrorCodes::SnapshotUnavailable,
                "No committed OpTime for snapshot read",
                !opTime.isNull());
        ru->setTimestampReadSource(RecoveryUnit::ReadSource::kProvided, opTime.getTimestamp());
        repl::ReadConcernArgs::get(opCtx).setArgsAtClusterTimeForSnapshot(opTime.getTimestamp());
    } 
    // Majority 读关注点的特殊处理：
    // 功能：为 majority 读关注点设置合适的读取源和等待逻辑
    // 排除：snapshot 读关注点（有自己的处理逻辑）和 atClusterTime（已在上面处理）
    else if (readConcernArgs.getLevel() == repl::ReadConcernLevel::kMajorityReadConcern &&
               replCoord->getSettings().isReplSet()) {
        // This block is not used for kSnapshotReadConcern because snapshots are always speculative;
        // we wait for majority when the transaction commits.
        // It is not used for atClusterTime because waitUntilOpTimeForRead handles waiting for
        // the majority snapshot in that case.

        // Handle speculative majority reads.
        // 推测性 majority 读取处理：
        // 功能：优化 majority 读取性能，在确认前开始读取
        // 原理：使用 "no overlap" 读取源，读取 all-committed 和 lastApplied 的最小值
        // 优势：在主节点和从节点上都能安全工作
        if (readConcernArgs.getMajorityReadMechanism() ==
            repl::ReadConcernArgs::MajorityReadMechanism::kSpeculative) {
            // For speculative majority reads, we utilize the "no overlap" read source as a means of
            // always reading at the minimum of the all-committed and lastApplied timestamps. This
            // allows for safe behavior on both primaries and secondaries, where the behavior of the
            // all-committed and lastApplied timestamps differ significantly.
            ru->setTimestampReadSource(RecoveryUnit::ReadSource::kNoOverlap);
            auto& speculativeReadInfo = repl::SpeculativeMajorityReadInfo::get(opCtx);
            speculativeReadInfo.setIsSpeculativeRead();
            return Status::OK();
        }

        // 标准 majority 读取处理：
        // 功能：等待 majority 提交的快照可用
        // 日志级别：配置服务器使用级别1，其他使用级别2
        const int debugLevel =
            serverGlobalParams.clusterRole.has(ClusterRole::ConfigServer) ? 1 : 2;

        LOGV2_DEBUG(
            20991,
            debugLevel,
            "Waiting for 'committed' snapshot to be available for reading: {readConcernArgs}",
            "readConcernArgs"_attr = readConcernArgs);

        // 设置 majority 提交读取源：
        // 功能：指示存储引擎使用 majority 提交的快照
        // 等待：如果快照不可用，会循环等待直到可用
        ru->setTimestampReadSource(RecoveryUnit::ReadSource::kMajorityCommitted);
        Status status = ru->majorityCommittedSnapshotAvailable();

        // Wait until a snapshot is available.
        // 快照可用性等待循环：
        // 功能：循环等待直到 majority 提交的快照可用
        // 机制：通过复制协调器等待快照提交
        // 退出：快照可用或发生错误
        while (status == ErrorCodes::ReadConcernMajorityNotAvailableYet) {
            LOGV2_DEBUG(20992, debugLevel, "Snapshot not available yet.");
            replCoord->waitUntilSnapshotCommitted(opCtx, Timestamp());
            status = ru->majorityCommittedSnapshotAvailable();
        }

        if (!status.isOK()) {
            return status;
        }

        LOGV2_DEBUG(20993,
                    debugLevel,
                    "Using 'committed' snapshot",
                    "operation_description"_attr = CurOp::get(opCtx)->opDescription());
    }

    // 最后稳定恢复时间戳等待（特殊功能）：
    // 功能：等待存储引擎的最后稳定恢复时间戳达到指定值
    // 用途：主要用于内部操作和数据一致性检查
    // 要求：需要 snapshot 读关注点、atClusterTime 参数和内部权限
    if (readConcernArgs.waitLastStableRecoveryTimestamp()) {
        uassert(8138101,
                "readConcern level 'snapshot' is required when specifying "
                "$_waitLastStableRecoveryTimestamp",
                readConcernArgs.getLevel() == repl::ReadConcernLevel::kSnapshotReadConcern);
        uassert(ErrorCodes::InvalidOptions,
                "atClusterTime is required for $_waitLastStableRecoveryTimestamp",
                atClusterTime);
        uassert(
            ErrorCodes::Unauthorized,
            "Unauthorized",
            AuthorizationSession::get(opCtx->getClient())
                ->isAuthorizedForActionsOnResource(
                    ResourcePattern::forClusterResource(dbName.tenantId()), ActionType::internal));
        
        // 存储引擎最后稳定时间戳检查和等待：
        // 功能：确保存储引擎的稳定恢复时间戳达到要求
        // 机制：如果不满足，触发 flushAllFiles 并循环等待
        // 目的：确保数据的持久化和恢复一致性
        auto* const storageEngine = opCtx->getServiceContext()->getStorageEngine();
        Lock::GlobalLock global(opCtx,
                                MODE_IS,
                                Date_t::max(),
                                Lock::InterruptBehavior::kThrow,
                                Lock::GlobalLockSkipOptions{.skipRSTLLock = true});
        auto lastStableRecoveryTimestamp = storageEngine->getLastStableRecoveryTimestamp();
        if (!lastStableRecoveryTimestamp ||
            *lastStableRecoveryTimestamp < atClusterTime->asTimestamp()) {
            // If the lastStableRecoveryTimestamp hasn't passed atClusterTime, we invoke
            // flushAllFiles explicitly here to push it. By default, fsync will run every minute to
            // call flushAllFiles. The lastStableRecoveryTimestamp should already be updated after
            // flushAllFiles return but we add a retry to make sure we wait until the timestamp gets
            // advanced.
            storageEngine->flushAllFiles(opCtx, /*callerHoldsReadLock*/ true);
            while (true) {
                lastStableRecoveryTimestamp = storageEngine->getLastStableRecoveryTimestamp();
                if (lastStableRecoveryTimestamp &&
                    *lastStableRecoveryTimestamp >= atClusterTime->asTimestamp()) {
                    break;
                }

                opCtx->sleepFor(Milliseconds(100));
            }
        }
    }
    return Status::OK();
}

Status waitForLinearizableReadConcernImpl(OperationContext* opCtx,
                                          const Milliseconds readConcernTimeout) {
    // If we are in a direct client that's holding a global lock, then this means this is a
    // sub-operation of the parent. In this case we delegate the wait to the parent.
    if (opCtx->getClient()->isInDirectClient() &&
        shard_role_details::getLocker(opCtx)->isLocked()) {
        return Status::OK();
    }
    CurOpFailpointHelpers::waitWhileFailPointEnabled(
        &hangBeforeLinearizableReadConcern, opCtx, "hangBeforeLinearizableReadConcern", [opCtx]() {
            LOGV2(20994,
                  "batch update - hangBeforeLinearizableReadConcern fail point enabled. "
                  "Blocking until fail point is disabled.");
        });

    repl::ReplicationCoordinator* replCoord =
        repl::ReplicationCoordinator::get(opCtx->getClient()->getServiceContext());

    {
        AutoGetOplogFastPath oplogWrite(opCtx, OplogAccessMode::kWrite);
        if (!replCoord->canAcceptWritesForDatabase(opCtx, DatabaseName::kAdmin)) {
            return {ErrorCodes::NotWritablePrimary,
                    "No longer primary when waiting for linearizable read concern"};
        }

        // With linearizable readConcern, read commands may write to the oplog, which is an
        // exception to the rule that writes are not allowed while ignoring prepare conflicts. If we
        // are ignoring prepare conflicts (during a read command), force the prepare conflict
        // behavior to permit writes.
        auto originalBehavior =
            shard_role_details::getRecoveryUnit(opCtx)->getPrepareConflictBehavior();
        if (originalBehavior == PrepareConflictBehavior::kIgnoreConflicts) {
            shard_role_details::getRecoveryUnit(opCtx)->setPrepareConflictBehavior(
                PrepareConflictBehavior::kIgnoreConflictsAllowWrites);
        }

        writeConflictRetry(
            opCtx, "waitForLinearizableReadConcern", NamespaceString::kRsOplogNamespace, [&opCtx] {
                WriteUnitOfWork uow(opCtx);
                opCtx->getClient()->getServiceContext()->getOpObserver()->onOpMessage(
                    opCtx,
                    BSON("msg"
                         << "linearizable read"));
                uow.commit();
            });
    }
    WriteConcernOptions wc = WriteConcernOptions{
        WriteConcernOptions::kMajority, WriteConcernOptions::SyncMode::UNSET, readConcernTimeout};
    repl::OpTime lastOpApplied = repl::ReplClientInfo::forClient(opCtx->getClient()).getLastOp();
    auto awaitReplResult = replCoord->awaitReplication(opCtx, lastOpApplied, wc);

    if (awaitReplResult.status == ErrorCodes::WriteConcernFailed) {
        return Status(ErrorCodes::LinearizableReadConcernError,
                      "Failed to confirm that read was linearizable.");
    }
    return awaitReplResult.status;
}

Status waitForSpeculativeMajorityReadConcernImpl(
    OperationContext* opCtx, repl::SpeculativeMajorityReadInfo speculativeReadInfo) {
    invariant(speculativeReadInfo.isSpeculativeRead());

    // If we are in a direct client that's holding a global lock, then this means this is a
    // sub-operation of the parent. In this case we delegate the wait to the parent.
    if (opCtx->getClient()->isInDirectClient() &&
        shard_role_details::getLocker(opCtx)->isLocked()) {
        return Status::OK();
    }

    // Select the timestamp to wait on. A command may have selected a specific timestamp to wait on.
    // If not, then we use the timestamp selected by the read source.
    auto replCoord = repl::ReplicationCoordinator::get(opCtx);
    Timestamp waitTs;
    auto speculativeReadTimestamp = speculativeReadInfo.getSpeculativeReadTimestamp();
    if (speculativeReadTimestamp) {
        waitTs = *speculativeReadTimestamp;
    } else {
        // Speculative majority reads are required to use the 'kNoOverlap' read source.
        invariant(shard_role_details::getRecoveryUnit(opCtx)->getTimestampReadSource() ==
                  RecoveryUnit::ReadSource::kNoOverlap);

        // Storage engine operations require at least Global IS.
        Lock::GlobalLock lk(opCtx, MODE_IS);
        boost::optional<Timestamp> readTs =
            shard_role_details::getRecoveryUnit(opCtx)->getPointInTimeReadTimestamp(opCtx);
        invariant(readTs);
        waitTs = *readTs;
    }

    // Block to make sure returned data is majority committed.
    LOGV2_DEBUG(20995,
                1,
                "Servicing speculative majority read, waiting for timestamp {waitTs} to become "
                "committed, current commit point: {replCoord_getLastCommittedOpTime}",
                "waitTs"_attr = waitTs,
                "replCoord_getLastCommittedOpTime"_attr = replCoord->getLastCommittedOpTime());

    if (!opCtx->hasDeadline()) {
        // This hard-coded value represents the maximum time we are willing to wait for a timestamp
        // to majority commit when doing a speculative majority read if no maxTimeMS value has been
        // set for the command. We make this value rather conservative. This exists primarily to
        // address the fact that getMore commands do not respect maxTimeMS properly. In this case,
        // we still want speculative majority reads to time out after some period if a timestamp
        // cannot majority commit.
        auto timeout = Seconds(15);
        opCtx->setDeadlineAfterNowBy(timeout, ErrorCodes::MaxTimeMSExpired);
    }
    Timer t;
    auto waitStatus = replCoord->awaitTimestampCommitted(opCtx, waitTs);
    if (waitStatus.isOK()) {
        LOGV2_DEBUG(20996,
                    1,
                    "Timestamp {waitTs} became majority committed, waited {t_millis}ms for "
                    "speculative majority read to be satisfied.",
                    "waitTs"_attr = waitTs,
                    "t_millis"_attr = t.millis());
    }
    return waitStatus;
}

auto setPrepareConflictBehaviorForReadConcernRegistration = MONGO_WEAK_FUNCTION_REGISTRATION(
    setPrepareConflictBehaviorForReadConcern, setPrepareConflictBehaviorForReadConcernImpl);
auto waitForReadConcernRegistration =
    MONGO_WEAK_FUNCTION_REGISTRATION(waitForReadConcern, waitForReadConcernImpl);
auto waitForLinearizableReadConcernRegistration = MONGO_WEAK_FUNCTION_REGISTRATION(
    waitForLinearizableReadConcern, waitForLinearizableReadConcernImpl);
auto waitForSpeculativeMajorityReadConcernRegistration = MONGO_WEAK_FUNCTION_REGISTRATION(
    waitForSpeculativeMajorityReadConcern, waitForSpeculativeMajorityReadConcernImpl);
}  // namespace

}  // namespace mongo
