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


#include <boost/optional.hpp>
#include <cstdint>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include <boost/cstdint.hpp>
#include <boost/move/utility_core.hpp>
#include <boost/none.hpp>
#include <boost/optional/optional.hpp>

#include "mongo/base/error_codes.h"
#include "mongo/base/status_with.h"
#include "mongo/bson/bsonelement.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/bsontypes.h"
#include "mongo/bson/timestamp.h"
#include "mongo/client/read_preference.h"
#include "mongo/db/cancelable_operation_context.h"
#include "mongo/db/client.h"
#include "mongo/db/concurrency/d_concurrency.h"
#include "mongo/db/concurrency/exception_util.h"
#include "mongo/db/concurrency/lock_manager_defs.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/ops/write_ops_retryability.h"
#include "mongo/db/repl/oplog.h"
#include "mongo/db/repl/oplog_entry.h"
#include "mongo/db/repl/oplog_entry_gen.h"
#include "mongo/db/s/migration_session_id.h"
#include "mongo/db/s/session_catalog_migration.h"
#include "mongo/db/s/session_catalog_migration_destination.h"
#include "mongo/db/service_context.h"
#include "mongo/db/session/logical_session_id.h"
#include "mongo/db/session/logical_session_id_helpers.h"
#include "mongo/db/session/session_catalog_mongod.h"
#include "mongo/db/session/session_txn_record_gen.h"
#include "mongo/db/shard_id.h"
#include "mongo/db/storage/write_unit_of_work.h"
#include "mongo/db/transaction/transaction_participant.h"
#include "mongo/db/write_concern.h"
#include "mongo/db/write_concern_options.h"
#include "mongo/executor/task_executor_pool.h"
#include "mongo/logv2/log.h"
#include "mongo/logv2/log_attr.h"
#include "mongo/logv2/log_component.h"
#include "mongo/logv2/redaction.h"
#include "mongo/s/client/shard.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/grid.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/duration.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/out_of_line_executor.h"
#include "mongo/util/str.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kSharding

namespace mongo {
namespace {

MONGO_FAIL_POINT_DEFINE(interruptBeforeProcessingPrePostImageOriginatingOp);

const auto kOplogField = "oplog";
const WriteConcernOptions kMajorityWC(WriteConcernOptions::kMajority,
                                      WriteConcernOptions::SyncMode::UNSET,
                                      Milliseconds(0));

/**
 * Returns the command request to extract session information from the source shard.
 */
BSONObj buildMigrateSessionCmd(const MigrationSessionId& migrationSessionId) {
    BSONObjBuilder builder;
    builder.append("_getNextSessionMods", 1);
    migrationSessionId.append(&builder);
    return builder.obj();
}

/**
 * Determines whether the oplog entry has a link to either preImage/postImage and sets a new link
 * to lastResult.oplogTime. For example, if entry has link to preImageTs, this sets preImageTs to
 * lastResult.oplogTime.
 *
 * It is an error to have both preImage and postImage as well as not having them at all.
 */
void setPrePostImageTs(const SessionCatalogMigrationDestination::ProcessOplogResult& lastResult,
                       repl::MutableOplogEntry* entry) {
    if (!lastResult.isPrePostImage) {
        uassert(40628,
                str::stream() << "expected oplog with ts: " << entry->getTimestamp().toString()
                              << " to not have " << repl::OplogEntryBase::kPreImageOpTimeFieldName
                              << " or " << repl::OplogEntryBase::kPostImageOpTimeFieldName,
                !entry->getPreImageOpTime() && !entry->getPostImageOpTime());
        return;
    }

    invariant(!lastResult.oplogTime.isNull());

    uassert(40629,
            str::stream() << "expected oplog with ts: " << entry->getTimestamp().toString() << ": "
                          << redact(entry->toBSON())
                          << " to have session: " << lastResult.sessionId,
            lastResult.sessionId == entry->getSessionId());
    uassert(40630,
            str::stream() << "expected oplog with ts: " << entry->getTimestamp().toString() << ": "
                          << redact(entry->toBSON()) << " to have txnNumber: " << lastResult.txnNum,
            lastResult.txnNum == entry->getTxnNumber());

    // PM-2213 introduces oplog entries that link to pre/post images in the
    // `config.image_collection` table. For chunk migration, we downconvert to the classic format
    // where the image is stored as a no-op in the oplog. A chunk migration source will always send
    // the appropriate no-op. This code on the destination patches up the CRUD operation oplog entry
    // to look like the classic format.
    if (entry->getNeedsRetryImage()) {
        switch (entry->getNeedsRetryImage().value()) {
            case repl::RetryImageEnum::kPreImage:
                entry->setPreImageOpTime({repl::OpTime()});
                break;
            case repl::RetryImageEnum::kPostImage:
                entry->setPostImageOpTime({repl::OpTime()});
                break;
        }
        entry->setNeedsRetryImage(boost::none);
    }

    if (entry->getPreImageOpTime()) {
        entry->setPreImageOpTime(lastResult.oplogTime);
    } else if (entry->getPostImageOpTime()) {
        entry->setPostImageOpTime(lastResult.oplogTime);
    } else {
        uasserted(40631,
                  str::stream() << "expected oplog with opTime: " << entry->getOpTime().toString()
                                << ": " << redact(entry->toBSON()) << " to have either "
                                << repl::OplogEntryBase::kPreImageOpTimeFieldName << " or "
                                << repl::OplogEntryBase::kPostImageOpTimeFieldName);
    }
}

/**
 * Parses the oplog into an oplog entry and makes sure that it contains the expected fields.
 */
repl::MutableOplogEntry parseOplog(const BSONObj& oplogBSON) {
    auto oplogEntry = uassertStatusOK(repl::MutableOplogEntry::parse(oplogBSON));

    const auto& sessionInfo = oplogEntry.getOperationSessionInfo();

    uassert(ErrorCodes::UnsupportedFormat,
            str::stream() << "oplog with opTime " << oplogEntry.getTimestamp().toString()
                          << " does not have sessionId: " << redact(oplogBSON),
            sessionInfo.getSessionId());

    uassert(ErrorCodes::UnsupportedFormat,
            str::stream() << "oplog with opTime " << oplogEntry.getTimestamp().toString()
                          << " does not have txnNumber: " << redact(oplogBSON),
            sessionInfo.getTxnNumber());

    uassert(ErrorCodes::UnsupportedFormat,
            str::stream() << "oplog with opTime " << oplogEntry.getTimestamp().toString()
                          << " does not have stmtId: " << redact(oplogBSON),
            !oplogEntry.getStatementIds().empty());

    return oplogEntry;
}

/**
 * Gets the next batch of oplog entries from the source shard.
 */
BSONObj getNextSessionOplogBatch(OperationContext* opCtx,
                                 const ShardId& fromShard,
                                 const MigrationSessionId& migrationSessionId) {
    auto shardStatus = Grid::get(opCtx)->shardRegistry()->getShard(opCtx, fromShard);
    uassertStatusOK(shardStatus.getStatus());

    auto shard = shardStatus.getValue();
    auto responseStatus = shard->runCommand(opCtx,
                                            ReadPreferenceSetting(ReadPreference::PrimaryOnly),
                                            DatabaseName::kAdmin,
                                            buildMigrateSessionCmd(migrationSessionId),
                                            Shard::RetryPolicy::kNoRetry);

    uassertStatusOK(responseStatus.getStatus());
    uassertStatusOK(responseStatus.getValue().commandStatus);

    auto result = responseStatus.getValue().response;

    auto oplogElement = result[kOplogField];
    uassert(ErrorCodes::FailedToParse,
            "_getNextSessionMods response does not have the 'oplog' field as array",
            oplogElement.type() == Array);

    return result;
}
}  // namespace

SessionCatalogMigrationDestination::SessionCatalogMigrationDestination(
    NamespaceString nss,
    ShardId fromShard,
    MigrationSessionId migrationSessionId,
    CancellationToken cancellationToken)
    : _nss(std::move(nss)),
      _fromShard(std::move(fromShard)),
      _migrationSessionId(std::move(migrationSessionId)),
      _cancellationToken(std::move(cancellationToken)) {}

SessionCatalogMigrationDestination::~SessionCatalogMigrationDestination() {
    if (_thread.joinable()) {
        _errorOccurred("Destructor cleaning up thread");
        _thread.join();
    }
}

/**
 * 启动会话迁移的后台线程
 * 
 * 核心功能：
 * 1. 将状态从 NotStarted 切换到 Migrating
 * 2. 创建独立的后台线程执行会话数据迁移任务
 * 3. 在后台线程中调用 _retrieveSessionStateFromSource 持续拉取源分片的会话数据
 * 4. 处理异常情况，包括兼容旧版本源分片（CommandNotFound错误）
 * 
 * 调用时机：
 * - 在迁移流程的第4阶段被调用（与初始数据克隆并行进行）
 * - 与数据迁移同时进行，确保会话相关的oplog也能被及时传输
 * 
 * 线程安全：
 * - 使用互斥锁保护状态变更
 * - 后台线程独立运行，不阻塞主迁移流程
 * 
 * 异常处理：
 * - 捕获 CommandNotFound 错误（兼容v3.7之前版本的源分片）
 * - 其他异常会调用 _errorOccurred 设置错误状态
 * MigrationDestinationManager::_migrateDriver
 */
void SessionCatalogMigrationDestination::start(ServiceContext* service) {
    {
        // 加锁保护状态变更，确保线程安全
        stdx::lock_guard<Latch> lk(_mutex);
        // 验证当前状态必须是未开始状态
        invariant(_state == State::NotStarted);
        // 将状态切换到迁移中
        _state = State::Migrating;
    }

    // 创建后台工作线程，执行会话数据迁移任务
    _thread = stdx::thread([=, this] {
        try {
            // 调用核心函数从源分片检索会话状态
            // 这个函数会持续运行直到迁移完成或发生错误
            _retrieveSessionStateFromSource(service);
        } catch (const DBException& ex) {
            // 处理命令未找到异常，这通常发生在源分片版本较老的情况下
            if (ex.code() == ErrorCodes::CommandNotFound) {
                // TODO: remove this after v3.7
                //
                // This means that the donor shard is running at an older version so it is safe to
                // just end this because there is no session information to transfer.
                // 
                // 这意味着源分片运行在较老版本，没有会话信息需要传输，可以安全结束
                return;
            }

            // 其他类型的异常，设置错误状态并记录错误信息
            _errorOccurred(ex.toString());
        }
    });
}

void SessionCatalogMigrationDestination::finish() {
    stdx::lock_guard<Latch> lk(_mutex);
    if (_state != State::ErrorOccurred) {
        _state = State::Committing;
    }
}

bool SessionCatalogMigrationDestination::joinable() const {
    return _thread.joinable();
}

void SessionCatalogMigrationDestination::join() {
    invariant(_thread.joinable());
    _thread.join();
}

/**
 * 后台线程主函数：从源分片持续检索会话状态和oplog条目
 * 
 * 核心功能：
 * 1. 建立与源分片的连接，循环发送 _getNextSessionMods 命令获取会话相关oplog
 * 2. 解析每批oplog条目，调用 _processSessionOplog 处理retryable writes和事务记录
 * 3. 管理迁移状态转换：Migrating -> ReadyToCommit -> Committing -> Done
 * 4. 确保在finish()被调用后彻底排空源分片的oplog缓冲区
 * 5. 等待所有写操作达到多数派写关注，保证数据持久性
 * 
 * 状态转换逻辑：
 * - 首次排空缓冲区：Migrating -> ReadyToCommit （等待提交信号）
 * - 收到finish()调用：ReadyToCommit -> Committing （开始最终排空）
 * - 再次排空缓冲区：Committing -> Done （迁移完成）
 * 
 * 容错机制：
 * - 检查错误状态并及时退出
 * - 处理TransactionTooOld异常（跳过过期事务）
 * - 响应CancellationToken中断请求
 * - 在每次批处理后等待写关注确认
 * 
 * 线程安全：
 * - 通过互斥锁保护状态访问
 * - 使用CancelableOperationContext响应外部中断
 * - 独立于主迁移线程运行，不阻塞chunk数据传输
 * 
 * 调用上下文：
 * - 由start()在后台线程中启动
 * - 与第4阶段的数据克隆并行执行
 * - 为第6-7阶段的会话完成提供数据支撑
 */
/**
 * Outline:
 *
 * 1. Get oplog with session info from the source shard.
 * 2. For each oplog entry, convert to type 'n' if not yet type 'n' while preserving all info
 *    needed for retryable writes.
 * 3. Also update the sessionCatalog for every oplog entry.
 * 4. Once the source shard returned an empty oplog buffer, it means that this should enter
 *    ReadyToCommit state and wait for the commit signal (by calling finish()).
 * 5. Once finish() is called, keep on trying to get more oplog from the source shard until it
 *    returns an empty result again.
 * 6. Wait for writes to be committed to majority of the replica set.
 */
void SessionCatalogMigrationDestination::_retrieveSessionStateFromSource(ServiceContext* service) {
    // 初始化专用的客户端线程，用于会话目录迁移
    Client::initThread("sessionCatalogMigrationProducer-" + _migrationSessionId.toString(),
                       service->getService(ClusterRole::ShardServer),
                       Client::noSession());
    
    // 标记是否在提交后排空过oplog缓冲区
    bool oplogDrainedAfterCommiting = false;
    // 记录上次处理的oplog结果，用于状态链接
    ProcessOplogResult lastResult;
    // 记录上次等待的操作时间
    repl::OpTime lastOpTimeWaited;

    // 主循环：持续从源分片拉取会话oplog直到完成
    while (true) {
        {
            // 检查错误状态，如果发生错误则立即退出
            stdx::lock_guard<Latch> lk(_mutex);
            if (_state == State::ErrorOccurred) {
                return;
            }
        }

        BSONObj nextBatch;
        BSONArray oplogArray;
        {
            // 创建可取消的操作上下文，支持外部中断
            auto executor = Grid::get(service)->getExecutorPool()->getFixedExecutor();
            auto uniqueCtx = CancelableOperationContext(
                cc().makeOperationContext(), _cancellationToken, executor);
            auto opCtx = uniqueCtx.get();
            // 设置在stepDown或stepUp时总是中断
            opCtx->setAlwaysInterruptAtStepDownOrUp_UNSAFE();

            // 从源分片获取下一批会话oplog条目
            nextBatch = getNextSessionOplogBatch(opCtx, _fromShard, _migrationSessionId);
            oplogArray = BSONArray{nextBatch[kOplogField].Obj()};

            // 如果oplog数组为空，说明当前缓冲区已排空
            if (oplogArray.isEmpty()) {
                {
                    stdx::lock_guard<Latch> lk(_mutex);
                    if (_state == State::Committing) {
                        // The migration is considered done only when it gets an empty result from
                        // the source shard while this is in state committing. This is to make sure
                        // that it doesn't miss any new oplog created between the time window where
                        // this depleted the buffer from the source shard and receiving the commit
                        // command.
                        // 只有在Committing状态下收到空结果才认为迁移完成
                        // 这确保不会错过在排空缓冲区和收到提交命令之间产生的新oplog
                        if (oplogDrainedAfterCommiting) {
                            LOGV2(5087100,
                                  "Recipient finished draining oplog entries for retryable writes "
                                  "and transactions from donor again after receiving "
                                  "_recvChunkCommit",
                                  logAttrs(_nss),
                                  "migrationSessionId"_attr = _migrationSessionId,
                                  "fromShard"_attr = _fromShard);
                            break; // 迁移完成，退出主循环
                        }

                        // 标记在提交后已排空一次
                        oplogDrainedAfterCommiting = true;
                    }
                }

                // 等待上次处理的oplog达到多数派写关注
                WriteConcernResult unusedWCResult;
                uassertStatusOK(
                    waitForWriteConcern(opCtx, lastResult.oplogTime, kMajorityWC, &unusedWCResult));

                {
                    stdx::lock_guard<Latch> lk(_mutex);
                    // Note: only transition to "ready to commit" if state is not error/force stop.
                    // 仅在非错误/强制停止状态下转换到"准备提交"
                    if (_state == State::Migrating) {
                        // We depleted the buffer at least once, transition to ready for commit.
                        // 第一次排空缓冲区，转换到准备提交状态
                        LOGV2(5087101,
                              "Recipient finished draining oplog entries for retryable writes and "
                              "transactions from donor for the first time, before receiving "
                              "_recvChunkCommit",
                              logAttrs(_nss),
                              "migrationSessionId"_attr = _migrationSessionId,
                              "fromShard"_attr = _fromShard);
                        _state = State::ReadyToCommit;
                    }
                }

                // 记录上次等待的操作时间
                lastOpTimeWaited = lastResult.oplogTime;
            }
        }

        // 处理当前批次中的每个oplog条目
        for (BSONArrayIteratorSorted oplogIter(oplogArray); oplogIter.more();) {
            auto oplogEntry = oplogIter.next().Obj();
            
            // failpoint：在处理pre/post image相关oplog前故意失败
            interruptBeforeProcessingPrePostImageOriginatingOp.executeIf(
                [&](const auto&) {
                    uasserted(6749200,
                              "Intentionally failing session migration before processing post/pre "
                              "image originating update oplog entry");
                },
                [&](const auto&) {
                    return !oplogEntry["needsRetryImage"].eoo() ||
                        !oplogEntry["preImageOpTime"].eoo() || !oplogEntry["postImageOpTime"].eoo();
                });
            
            try {
                // 处理单条会话oplog，更新本地会话目录
                lastResult =
                    _processSessionOplog(oplogEntry, lastResult, service, _cancellationToken);
            } catch (const ExceptionFor<ErrorCodes::TransactionTooOld>&) {
                // This means that the server has a newer txnNumber than the oplog being
                // migrated, so just skip it
                // 服务器有更新的事务号，跳过这个过期的oplog条目
                continue;
            }
        }
    }

    // 最终确保所有写操作达到多数派写关注
    WriteConcernResult unusedWCResult;

    auto executor = Grid::get(service)->getExecutorPool()->getFixedExecutor();
    auto uniqueOpCtx =
        CancelableOperationContext(cc().makeOperationContext(), _cancellationToken, executor);
    uniqueOpCtx->setAlwaysInterruptAtStepDownOrUp_UNSAFE();

    uassertStatusOK(
        waitForWriteConcern(uniqueOpCtx.get(), lastResult.oplogTime, kMajorityWC, &unusedWCResult));

    {
        // 设置最终状态为完成
        stdx::lock_guard<Latch> lk(_mutex);
        _state = State::Done;
    }
}

/**
 * Insert a new oplog entry by converting the oplogBSON into type 'n' oplog with the session
 * information. The new oplogEntry will also link to prePostImageTs if not null.
 */
SessionCatalogMigrationDestination::ProcessOplogResult
SessionCatalogMigrationDestination::_processSessionOplog(const BSONObj& oplogBSON,
                                                         const ProcessOplogResult& lastResult,
                                                         ServiceContext* serviceContext,
                                                         CancellationToken cancellationToken) {

    auto oplogEntry = parseOplog(oplogBSON);

    ProcessOplogResult result;
    result.sessionId = *oplogEntry.getSessionId();
    result.txnNum = *oplogEntry.getTxnNumber();

    if (oplogEntry.getOpType() == repl::OpTypeEnum::kNoop) {
        // Note: Oplog is already no-op type, no need to nest.
        // There are three types of type 'n' oplog format expected here:
        // (1) Oplog entries that has been transformed by a previous migration into a
        //     nested oplog. In this case, o field contains {$sessionMigrateInfo: 1}
        //     and o2 field contains the details of the original oplog.
        // (2) Oplog entries that contains the pre/post-image information of a
        //     findAndModify operation. In this case, o field contains the relevant info
        //     and o2 will be empty.
        // (3) Oplog entries that are a dead sentinel, which the donor sent over as the replacement
        //     for a prepare oplog entry or unprepared transaction commit oplog entry.
        // (4) Oplog entries that are a WouldChangeOwningShard sentinel entry, used for making
        //     retries of a WouldChangeOwningShard update or findAndModify fail with
        //     IncompleteTransactionHistory. In this case, the o field is non-empty and the o2
        //     field is an empty BSONObj.

        BSONObj object2;
        if (oplogEntry.getObject2()) {
            object2 = *oplogEntry.getObject2();
        } else {
            oplogEntry.setObject2(object2);
        }

        if (object2.isEmpty() && !isWouldChangeOwningShardSentinelOplogEntry(oplogEntry)) {
            result.isPrePostImage = true;

            uassert(40632,
                    str::stream() << "Can't handle 2 pre/post image oplog in a row. Prevoius oplog "
                                  << lastResult.oplogTime.getTimestamp().toString()
                                  << ", oplog ts: " << oplogEntry.getTimestamp().toString() << ": "
                                  << oplogBSON,
                    !lastResult.isPrePostImage);
        }
    } else {
        oplogEntry.setObject2(oplogBSON);  // TODO: strip redundant info?
    }

    const auto stmtIds = oplogEntry.getStatementIds();

    auto executor = Grid::get(serviceContext)->getExecutorPool()->getFixedExecutor();
    auto uniqueOpCtx =
        CancelableOperationContext(cc().makeOperationContext(), cancellationToken, executor);
    auto opCtx = uniqueOpCtx.get();
    opCtx->setAlwaysInterruptAtStepDownOrUp_UNSAFE();

    {
        auto lk = stdx::lock_guard(*opCtx->getClient());
        opCtx->setLogicalSessionId(result.sessionId);
        opCtx->setTxnNumber(result.txnNum);
    }

    // Irrespective of whether or not the oplog gets logged, we want to update the
    // entriesMigrated counter to signal that we have succesfully recieved the oplog
    // from the source and have processed it.
    _sessionOplogEntriesMigrated.addAndFetch(1);

    auto mongoDSessionCatalog = MongoDSessionCatalog::get(opCtx);
    auto ocs = mongoDSessionCatalog->checkOutSession(opCtx);

    auto txnParticipant = TransactionParticipant::get(opCtx);

    try {
        txnParticipant.beginOrContinue(opCtx,
                                       {result.txnNum},
                                       boost::none /* autocommit */,
                                       TransactionParticipant::TransactionActions::kNone);
        if (txnParticipant.checkStatementExecutedNoOplogEntryFetch(opCtx, stmtIds.front())) {
            // Skip the incoming statement because it has already been logged locally
            return lastResult;
        }
    } catch (const DBException& ex) {
        // If the transaction chain is incomplete because oplog was truncated, just ignore the
        // incoming oplog and don't attempt to 'patch up' the missing pieces.
        if (ex.code() == ErrorCodes::IncompleteTransactionHistory) {
            return lastResult;
        }

        if (stmtIds.front() == kIncompleteHistoryStmtId) {
            // No need to log entries for transactions whose history has been truncated
            invariant(stmtIds.size() == 1);
            return lastResult;
        }

        throw;
    }

    if (!result.isPrePostImage && !isWouldChangeOwningShardSentinelOplogEntry(oplogEntry)) {
        // Do not overwrite the "o" field if this is a pre/post image oplog entry. Also do not
        // overwrite it if this is a WouldChangeOwningShard sentinel oplog entry since it contains
        // a special BSONObj used for making retries fail with an IncompleteTransactionHistory
        // error.
        oplogEntry.setObject(SessionCatalogMigration::kSessionOplogTag);
    }
    setPrePostImageTs(lastResult, &oplogEntry);
    oplogEntry.setPrevWriteOpTimeInTransaction(txnParticipant.getLastWriteOpTime());

    oplogEntry.setOpType(repl::OpTypeEnum::kNoop);
    oplogEntry.setFromMigrate(true);
    // Reset OpTime so logOp() can assign a new one.
    oplogEntry.setOpTime(OplogSlot());

    writeConflictRetry(
        opCtx, "SessionOplogMigration", NamespaceString::kSessionTransactionsTableNamespace, [&] {
            // Need to take global lock here so repl::logOp will not unlock it and trigger the
            // invariant that disallows unlocking global lock while inside a WUOW. Take the
            // transaction table db lock to ensure the same lock ordering with normal replicated
            // updates to the table.
            Lock::DBLock lk(
                opCtx, NamespaceString::kSessionTransactionsTableNamespace.dbName(), MODE_IX);
            WriteUnitOfWork wunit(opCtx);

            result.oplogTime = repl::logOp(opCtx, &oplogEntry);

            const auto& oplogOpTime = result.oplogTime;
            uassert(40633,
                    str::stream() << "Failed to create new oplog entry for oplog with opTime: "
                                  << oplogEntry.getOpTime().toString() << ": " << redact(oplogBSON),
                    !oplogOpTime.isNull());

            // Do not call onWriteOpCompletedO nPrimary if we inserted a pre/post image, because the
            // next oplog will contain the real operation
            if (!result.isPrePostImage) {
                SessionTxnRecord sessionTxnRecord;
                sessionTxnRecord.setSessionId(result.sessionId);
                sessionTxnRecord.setTxnNum(result.txnNum);
                sessionTxnRecord.setLastWriteOpTime(oplogOpTime);

                // Use the same wallTime as oplog since SessionUpdateTracker looks at the oplog
                // entry wallTime when replicating.
                sessionTxnRecord.setLastWriteDate(oplogEntry.getWallClockTime());

                if (isInternalSessionForRetryableWrite(result.sessionId)) {
                    sessionTxnRecord.setParentSessionId(*getParentSessionId(result.sessionId));
                }

                // We do not migrate transaction oplog entries so don't set the txn state.
                txnParticipant.onRetryableWriteCloningCompleted(opCtx, stmtIds, sessionTxnRecord);
            }

            wunit.commit();
        });

    return result;
}

std::string SessionCatalogMigrationDestination::getErrMsg() {
    stdx::lock_guard<Latch> lk(_mutex);
    return _errMsg;
}

void SessionCatalogMigrationDestination::_errorOccurred(StringData errMsg) {
    LOGV2(5087102,
          "Recipient failed to copy oplog entries for retryable writes and transactions from donor",
          logAttrs(_nss),
          "migrationSessionId"_attr = _migrationSessionId,
          "fromShard"_attr = _fromShard,
          "error"_attr = errMsg);

    stdx::lock_guard<Latch> lk(_mutex);
    _state = State::ErrorOccurred;
    _errMsg = errMsg.toString();
}

MigrationSessionId SessionCatalogMigrationDestination::getMigrationSessionId() const {
    return _migrationSessionId;
}

SessionCatalogMigrationDestination::State SessionCatalogMigrationDestination::getState() {
    stdx::lock_guard<Latch> lk(_mutex);
    return _state;
}

void SessionCatalogMigrationDestination::forceFail(StringData errMsg) {
    _errorOccurred(errMsg);
}

long long SessionCatalogMigrationDestination::getSessionOplogEntriesMigrated() {
    return _sessionOplogEntriesMigrated.load();
}
}  // namespace mongo
