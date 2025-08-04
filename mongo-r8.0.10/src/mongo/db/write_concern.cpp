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


#include "mongo/db/write_concern.h"

#include <boost/optional.hpp>
#include <cstddef>
#include <cstdint>
#include <fmt/format.h>
#include <memory>
#include <variant>

#include <absl/container/node_hash_map.h>
#include <boost/move/utility_core.hpp>
#include <boost/optional/optional.hpp>

#include "mongo/base/error_codes.h"
#include "mongo/db/client.h"
#include "mongo/db/cluster_role.h"
#include "mongo/db/commands/server_status_metric.h"
#include "mongo/db/curop.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/read_write_concern_defaults.h"
#include "mongo/db/read_write_concern_defaults_gen.h"
#include "mongo/db/read_write_concern_provenance.h"
#include "mongo/db/repl/optime.h"
#include "mongo/db/repl/repl_settings.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/db/repl/storage_interface.h"
#include "mongo/db/server_options.h"
#include "mongo/db/service_context.h"
#include "mongo/db/stats/timer_stats.h"
#include "mongo/db/storage/control/journal_flusher.h"
#include "mongo/db/storage/storage_engine.h"
#include "mongo/db/transaction_validation.h"
#include "mongo/db/write_concern_options.h"
#include "mongo/logv2/log.h"
#include "mongo/logv2/log_attr.h"
#include "mongo/logv2/log_component.h"
#include "mongo/platform/compiler.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/duration.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/timer.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kReplication


namespace mongo {

using repl::OpTime;
using std::string;

namespace {
auto& gleWtimeStats = *MetricBuilder<TimerStats>{"getLastError.wtime"};
auto& gleWtimeouts = *MetricBuilder<Counter64>{"getLastError.wtimeouts"};
auto& gleDefaultWtimeouts = *MetricBuilder<Counter64>{"getLastError.default.wtimeouts"};
auto& gleDefaultUnsatisfiable = *MetricBuilder<Counter64>{"getLastError.default.unsatisfiable"};
}  // namespace

MONGO_FAIL_POINT_DEFINE(hangBeforeWaitingForWriteConcern);

bool commandSpecifiesWriteConcern(const CommonRequestArgs& requestArgs) {
    return !!requestArgs.getWriteConcern();
}

StatusWith<WriteConcernOptions> extractWriteConcern(OperationContext* opCtx,
                                                    const BSONObj& cmdObj,
                                                    bool isInternalClient) {
    auto wcResult = WriteConcernOptions::extractWCFromCommand(cmdObj);
    if (!wcResult.isOK()) {
        return wcResult.getStatus();
    }

    WriteConcernOptions writeConcern = wcResult.getValue();

    // This is the WC extracted from the command object, so the CWWC or implicit default hasn't been
    // applied yet, which is why "usedDefaultConstructedWC" flag can be used an indicator of whether
    // the client supplied a WC or not.
    // If the user supplied write concern from the command is empty (writeConcern: {}),
    // usedDefaultConstructedWC will be true so we will then use the CWWC or implicit default.
    // Note that specifying writeConcern: {w:0} is not the same as empty. {w:0} differs from {w:1}
    // in that the client will not expect a command reply/acknowledgement at all, even in the case
    // of errors.
    bool clientSuppliedWriteConcern = !writeConcern.usedDefaultConstructedWC;
    bool customDefaultWasApplied = false;

    // WriteConcern defaults can only be applied on regular replica set members.
    // Operations received by shard and config servers should always have WC explicitly specified.
    bool canApplyDefaultWC = serverGlobalParams.clusterRole.has(ClusterRole::None) &&
        repl::ReplicationCoordinator::get(opCtx)->getSettings().isReplSet() &&
        (!opCtx->inMultiDocumentTransaction() ||
         isTransactionCommand(opCtx->getService(), cmdObj.firstElementFieldName())) &&
        !opCtx->getClient()->isInDirectClient() && !isInternalClient;


    // If no write concern is specified in the command, then use the cluster-wide default WC (if
    // there is one), or else the default implicit WC:
    // (if [(#arbiters > 0) AND (#arbiters >= ½(#voting nodes) - 1)] then {w:1} else {w:majority}).
    if (canApplyDefaultWC) {
        auto getDefaultWC = ([&]() {
            auto rwcDefaults =
                ReadWriteConcernDefaults::get(opCtx->getServiceContext()).getDefault(opCtx);
            auto wcDefault = rwcDefaults.getDefaultWriteConcern();
            const auto defaultWriteConcernSource = rwcDefaults.getDefaultWriteConcernSource();
            customDefaultWasApplied = defaultWriteConcernSource &&
                defaultWriteConcernSource == DefaultWriteConcernSourceEnum::kGlobal;
            return wcDefault;
        });


        if (!clientSuppliedWriteConcern) {
            writeConcern = ([&]() {
                auto wcDefault = getDefaultWC();
                // Default WC can be 'boost::none' if the implicit default is used and set to 'w:1'.
                if (wcDefault) {
                    LOGV2_DEBUG(22548,
                                2,
                                "Applying default writeConcern on {cmdObj_firstElementFieldName} "
                                "of {wcDefault}",
                                "cmdObj_firstElementFieldName"_attr =
                                    cmdObj.firstElementFieldName(),
                                "wcDefault"_attr = wcDefault->toBSON());
                    return *wcDefault;
                }
                return writeConcern;
            })();
            writeConcern.notExplicitWValue = true;
        }
        // Client supplied a write concern object without 'w' field.
        else if (writeConcern.isExplicitWithoutWField()) {
            auto wcDefault = getDefaultWC();
            // Default WC can be 'boost::none' if the implicit default is used and set to 'w:1'.
            if (wcDefault) {
                clientSuppliedWriteConcern = false;
                writeConcern.w = wcDefault->w;
                if (writeConcern.syncMode == WriteConcernOptions::SyncMode::UNSET) {
                    writeConcern.syncMode = wcDefault->syncMode;
                }
            }
        }
    }

    // It's fine for clients to provide any provenance value to mongod. But if they haven't, then an
    // appropriate provenance needs to be determined.
    auto& provenance = writeConcern.getProvenance();
    if (!provenance.hasSource()) {
        if (clientSuppliedWriteConcern) {
            provenance.setSource(ReadWriteConcernProvenance::Source::clientSupplied);
        } else if (customDefaultWasApplied) {
            provenance.setSource(ReadWriteConcernProvenance::Source::customDefault);
        } else if (opCtx->getClient()->isInDirectClient() || isInternalClient) {
            provenance.setSource(ReadWriteConcernProvenance::Source::internalWriteDefault);
        } else {
            provenance.setSource(ReadWriteConcernProvenance::Source::implicitDefault);
        }
    }

    if (writeConcern.syncMode == WriteConcernOptions::SyncMode::NONE && writeConcern.isMajority() &&
        !opCtx->getServiceContext()->getStorageEngine()->isEphemeral()) {
        auto* const replCoord = repl::ReplicationCoordinator::get(opCtx);
        if (replCoord && replCoord->getSettings().isReplSet() &&
            replCoord->getWriteConcernMajorityShouldJournal()) {
            LOGV2_DEBUG(8668500,
                        1,
                        "Overriding write concern majority j:false to j:true",
                        "writeConcern"_attr = writeConcern);
            writeConcern.majorityJFalseOverridden = true;
            writeConcern.syncMode = WriteConcernOptions::SyncMode::JOURNAL;
        }
    }

    Status wcStatus = validateWriteConcern(opCtx, writeConcern);
    if (!wcStatus.isOK()) {
        return wcStatus;
    }

    return writeConcern;
}

Status validateWriteConcern(OperationContext* opCtx, const WriteConcernOptions& writeConcern) {
    if (writeConcern.syncMode == WriteConcernOptions::SyncMode::JOURNAL &&
        opCtx->getServiceContext()->getStorageEngine()->isEphemeral()) {
        return Status(ErrorCodes::BadValue,
                      "cannot use 'j' option when a host does not have journaling enabled");
    }

    if (!repl::ReplicationCoordinator::get(opCtx)->getSettings().isReplSet()) {
        if (holds_alternative<int64_t>(writeConcern.w) && get<int64_t>(writeConcern.w) > 1) {
            return Status(ErrorCodes::BadValue, "cannot use 'w' > 1 when a host is not replicated");
        }

        if (writeConcern.hasCustomWriteMode()) {
            return Status(
                ErrorCodes::BadValue,
                fmt::format("cannot use non-majority 'w' mode \"{}\" when a host is not a "
                            "member of a replica set",
                            get<std::string>(writeConcern.w)));
        }
    }

    return Status::OK();
}

void WriteConcernResult::appendTo(BSONObjBuilder* result) const {
    if (syncMillis >= 0)
        result->appendNumber("syncMillis", syncMillis);

    if (fsyncFiles >= 0)
        result->appendNumber("fsyncFiles", fsyncFiles);

    if (wTime >= 0) {
        if (wTimedOut)
            result->appendNumber("waited", wTime);
        else
            result->appendNumber("wtime", wTime);
    }

    if (wTimedOut)
        result->appendBool("wtimeout", true);

    if (writtenTo.size()) {
        BSONArrayBuilder hosts(result->subarrayStart("writtenTo"));
        for (size_t i = 0; i < writtenTo.size(); ++i) {
            hosts.append(writtenTo[i].toString());
        }
    } else {
        result->appendNull("writtenTo");
    }

    result->append("writeConcern", wcUsed.toBSON());

    if (err.empty())
        result->appendNull("err");
    else
        result->append("err", err);
}

/**
 * Write concern with {j: true} on single voter replica set primaries must wait for no oplog holes
 * behind a write, before flushing to disk (not done in this function), in order to guarantee that
 * a write will remain after unclean shutdown and server restart recovery.
 *
 * Multi-voter replica sets will likely roll back writes if the primary crashes and restarts.
 * However, single voter sets never roll back writes, so we must maintain that behavior. Multi-node
 * single-voter primaries must truncate the oplog to ensure cross-replica set data consistency; and
 * single-node single-voter sets must never lose confirmed writes.
 *
 * The oplogTruncateAfterPoint is updated with the no holes point prior to journal flushing (write
 * persistence). Ensuring the no holes point is past (or equal to) our write, ensures the flush to
 * disk will save a truncate point that will not truncate the new write we wish to guarantee.
 *
 * Can throw on opCtx interruption.
 */
void waitForNoOplogHolesIfNeeded(OperationContext* opCtx) {
    auto const replCoord = repl::ReplicationCoordinator::get(opCtx);
    if (replCoord->getNumConfigVotingMembers() == 1) {
        // It is safe for secondaries in multi-node single voter replica sets to truncate writes if
        // there are oplog holes. They can catch up again.
        repl::StorageInterface::get(opCtx)->waitForAllEarlierOplogWritesToBeVisible(
            opCtx, /*primaryOnly*/ true);
    }
}

/**
 * waitForWriteConcern 函数的作用：
 * 等待写关注（Write Concern）条件得到满足，确保写操作达到指定的持久性和复制要求。
 * 
 * 核心功能：
 * 1. 持久性保证：根据写关注的同步模式等待数据刷盘或日志提交
 * 2. 复制确认：等待指定数量的副本节点确认写操作
 * 3. 超时处理：在指定时间内无法满足写关注条件时返回超时错误
 * 4. 性能统计：记录等待时间和超时次数用于监控和调优
 * 5. 异常处理：处理等待过程中的中断和故障场景
 * 
 * 持久性级别：
 * - NONE：不等待任何持久性保证
 * - FSYNC：等待数据完全刷写到磁盘文件
 * - JOURNAL：等待写操作记录到日志文件
 * 
 * 复制级别：
 * - w:1：只需主节点确认（默认）
 * - w:majority：需要大多数节点确认
 * - w:n：需要指定数量的节点确认
 * - w:"tagName"：需要指定标签集合的节点确认
 * 
 * 参数说明：
 * @param opCtx 操作上下文，提供执行环境和中断控制
 * @param replOpTime 写操作的复制时间戳，用于复制确认
 * @param writeConcern 写关注选项，指定持久性和复制要求
 * @param result 输出参数，返回等待结果和统计信息
 * 
 * 返回值：
 * - Status::OK()：写关注条件成功满足
 * - ErrorCodes::WriteConcernFailed：等待超时
 * - ErrorCodes::UnsatisfiableWriteConcern：写关注条件无法满足
 * - 其他错误码：等待过程中发生的异常
 * 
 * 性能考虑：
 * - 写关注级别越高，等待时间越长
 * - 网络延迟和节点状态影响复制确认时间
 * - 日志刷盘和文件同步影响持久性等待时间
 * 
 * 该函数是MongoDB写操作可靠性保证的核心实现，平衡性能和数据安全性。
 */
Status waitForWriteConcern(OperationContext* opCtx,
                           const OpTime& replOpTime,
                           const WriteConcernOptions& writeConcern,
                           WriteConcernResult* result) {
    // If we are in a direct client that's holding a global lock, then this means it is illegal to
    // wait for write concern. This is fine, since the outer operation should have handled waiting
    // for write concern.
    // 直接客户端全局锁检查：
    // 如果处于持有全局锁的直接客户端中，则等待写关注是非法的
    // 这是可以接受的，因为外层操作应该已经处理了写关注等待
    if (opCtx->getClient()->isInDirectClient() &&
        shard_role_details::getLocker(opCtx)->isLocked()) {
        return Status::OK();
    }

    // 调试日志：记录开始等待写关注的详细信息
    // 包含复制操作时间戳和写关注配置，便于问题诊断
    LOGV2_DEBUG(22549,
                2,
                "Waiting for write concern. OpTime: {replOpTime}, write concern: {writeConcern}",
                "replOpTime"_attr = replOpTime,
                "writeConcern"_attr = writeConcern.toBSON());

    // Add time waiting for write concern to CurOp.
    // 性能统计：将等待写关注的时间添加到当前操作统计中
    // 用于监控写操作的总体性能和写关注等待开销
    CurOp::get(opCtx)->beginWaitForWriteConcernTimer();
    ScopeGuard finishTiming([&] { CurOp::get(opCtx)->stopWaitForWriteConcernTimer(); });

    // 系统组件获取：获取存储引擎和复制协调器的引用
    // 存储引擎：用于判断是否支持持久化和执行刷盘操作
    // 复制协调器：用于等待复制确认和填充写关注选项
    auto* const storageEngine = opCtx->getServiceContext()->getStorageEngine();
    auto const replCoord = repl::ReplicationCoordinator::get(opCtx);

    // 故障点测试：在等待写关注前暂停，用于测试和调试
    // 注意：不对内部客户端生效，以免影响副本集步进等关键操作
    if (MONGO_unlikely(hangBeforeWaitingForWriteConcern.shouldFail()) &&
        !opCtx->getClient()->isInDirectClient()) {
        // Respecting this failpoint for internal clients prevents stepup from working properly.
        // This fail point pauses with an open snapshot on the oplog. Some tests pause on this fail
        // point prior to running replication rollback. This prevents the operation from being
        // killed and the snapshot being released. Hence, we release the snapshot here.
        // 快照释放：释放当前快照以避免阻塞副本集操作
        // 某些测试在复制回滚前在此故障点暂停，需要释放快照防止死锁
        shard_role_details::replaceRecoveryUnit(opCtx);

        hangBeforeWaitingForWriteConcern.pauseWhileSet();
    }

    // Next handle blocking on disk
    // 磁盘持久性处理阶段：处理数据刷盘和日志同步
    Timer syncTimer;  // 同步操作计时器，用于统计持久性等待时间
    
    // 写关注选项填充：填充未设置的同步模式选项
    // 复制协调器根据当前配置确定默认的同步行为
    WriteConcernOptions writeConcernWithPopulatedSyncMode =
        replCoord->populateUnsetWriteConcernOptionsSyncMode(writeConcern);

    // Waiting for durability (flushing the journal or all files to disk) can throw on interruption.
    // 持久性等待处理：刷盘和日志同步可能因中断而抛出异常
    try {
        // 根据同步模式执行相应的持久性操作
        switch (writeConcernWithPopulatedSyncMode.syncMode) {
            case WriteConcernOptions::SyncMode::UNSET:
                // 致命错误：同步模式不应该未设置到这里
                LOGV2_FATAL(34410,
                            "Attempting to wait on a WriteConcern with an unset sync option");
            case WriteConcernOptions::SyncMode::NONE:
                // 无同步要求：直接跳过持久性等待
                break;
            case WriteConcernOptions::SyncMode::FSYNC: {
                // 文件系统同步模式：等待所有数据刷写到磁盘
                // 
                // oplog洞等待：确保在刷盘前没有oplog空洞
                // 对于单投票者副本集，这确保写操作在非正常关机后能够保持
                waitForNoOplogHolesIfNeeded(opCtx);
                
                if (!storageEngine->isEphemeral()) {
                    // 持久化存储引擎：设置刷盘文件数统计
                    // This field has had a dummy value since MMAP went away. It is undocumented.
                    // Maintaining it so as not to cause unnecessary user pain across upgrades.
                    // 兼容性字段：自MMAP存储引擎移除后的虚拟值，保持向后兼容
                    result->fsyncFiles = 1;
                } else {
                    // 临时存储引擎：只需要提交日志
                    // We only need to commit the journal if we're durable
                    JournalFlusher::get(opCtx)->waitForJournalFlush(opCtx);
                }
                break;
            }
            case WriteConcernOptions::SyncMode::JOURNAL:
                // 日志同步模式：等待写操作记录到日志文件
                //
                // oplog洞等待：确保日志刷盘前没有oplog空洞
                waitForNoOplogHolesIfNeeded(opCtx);
                
                // In most cases we only need to trigger a journal flush without waiting for it
                // to complete because waiting for replication with j:true already tracks the
                // durable point for all data-bearing nodes and thus is sufficient to guarantee
                // durability.
                //
                // One exception is for w:1 writes where we need to wait for the journal flush
                // to complete because we skip waiting for replication for w:1 writes. In fact
                // for multi-voter replica sets, durability of w:1 writes could be meaningless
                // because they may still be rolled back if the primary crashes. Single-voter
                // replica sets, however, can never rollback confirmed writes, thus durability
                // does matter in this case.
                // 日志刷盘策略：
                // 大多数情况下只需触发日志刷盘而无需等待完成，因为复制等待已经跟踪持久点
                // 
                // 例外情况：w:1写操作需要等待日志刷盘完成
                // - 多投票者副本集：w:1写操作的持久性可能无意义（可能被回滚）
                // - 单投票者副本集：永不回滚已确认写操作，持久性很重要
                if (!writeConcernWithPopulatedSyncMode.needToWaitForOtherNodes()) {
                    // w:1写操作：等待日志刷盘完成
                    JournalFlusher::get(opCtx)->waitForJournalFlush(opCtx);
                } else {
                    // 需要等待其他节点：只触发日志刷盘，复制等待会处理持久性
                    JournalFlusher::get(opCtx)->triggerJournalFlush();
                }
                break;
        }
    } catch (const DBException& ex) {
        // 持久性等待异常处理：转换为状态码返回
        return ex.toStatus();
    }

    // 记录同步操作耗时：用于性能分析和监控
    result->syncMillis = syncTimer.millis();

    // Now wait for replication
    // 复制等待阶段：等待副本节点确认写操作

    if (replOpTime.isNull()) {
        // no write happened for this client yet
        // 空操作时间：表示此客户端尚未发生写操作，无需等待复制
        return Status::OK();
    }

    // needed to avoid incrementing gleWtimeStats SERVER-9005
    // 复制需求检查：避免不必要的统计计数增加
    if (!writeConcernWithPopulatedSyncMode.needToWaitForOtherNodes()) {
        // no desired replication check
        // 无复制要求：w:1或类似情况，不需要等待其他节点
        return Status::OK();
    }

    // Replica set stepdowns and gle mode changes are thrown as errors
    // 复制等待执行：等待指定数量的副本节点确认写操作
    // 副本集步进和gle模式变化会作为错误抛出
    repl::ReplicationCoordinator::StatusAndDuration replStatus =
        replCoord->awaitReplication(opCtx, replOpTime, writeConcernWithPopulatedSyncMode);
    
    // 写关注失败处理：处理超时和无法满足的情况
    if (replStatus.status == ErrorCodes::WriteConcernFailed) {
        // 超时统计：增加全局超时计数器
        gleWtimeouts.increment();
        if (!writeConcern.getProvenance().isClientSupplied()) {
            // 默认写关注超时：增加默认写关注超时计数
            gleDefaultWtimeouts.increment();
        }
        result->err = "timeout";      // 设置错误信息
        result->wTimedOut = true;     // 标记为超时
    }
    if (replStatus.status == ErrorCodes::UnsatisfiableWriteConcern) {
        // 无法满足的写关注：记录统计信息
        if (!writeConcern.getProvenance().isClientSupplied()) {
            gleDefaultUnsatisfiable.increment();
        }
    }

    // 等待时间统计：记录复制等待的详细统计信息
    gleWtimeStats.recordMillis(durationCount<Milliseconds>(replStatus.duration));
    result->wTime = durationCount<Milliseconds>(replStatus.duration);

    // 使用的写关注记录：保存实际使用的写关注配置
    result->wcUsed = writeConcern;

    // 返回复制等待的最终状态
    return replStatus.status;
}

}  // namespace mongo
