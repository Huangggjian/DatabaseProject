/**
 * transaction_manager.cpp
 *
 */
#include "concurrency/transaction_manager.h"
#include "table/table_heap.h"

#include <cassert>
namespace cmudb
{

    Transaction *TransactionManager::Begin()
    {
        Transaction *txn = new Transaction(next_txn_id_++);

        if (ENABLE_LOGGING)
        {
            assert(txn->GetPrevLSN() == INVALID_LSN);
            //           参数：txn_id_t txn_id, lsn_t prev_lsn, LogRecordType log_record_type
            LogRecord log{txn->GetTransactionId(), txn->GetPrevLSN(), LogRecordType::BEGIN};
            txn->SetPrevLSN(log_manager_->AppendLogRecord(log)); //写日志，当前这一条日志信息设置为下一条日志的prev lsn
        }

        return txn;
    }
    //innodb通过force log at commit机制实现事务的持久性，即在事务提交的时候，必须先将该事务的所有事务日志写入到磁盘上的redo log file和undo log file中进行持久化。
    void TransactionManager::Commit(Transaction *txn)
    {
        txn->SetState(TransactionState::COMMITTED);
        // truly delete before commit
        auto write_set = txn->GetWriteSet();
        while (!write_set->empty())         //markdelete的（假删），要提交了，就真正删除吧！
        {
            auto &item = write_set->back();
            auto table = item.table_;
            if (item.wtype_ == WType::DELETE)
            {
                // this also release the lock when holding the page latch
                table->ApplyDelete(item.rid_, txn);
            }
            write_set->pop_back();
        }
        write_set->clear();

        //commit结束前，要落盘日志, logtype = commit
        if (ENABLE_LOGGING)
        { // you need to make sure your log records are permanently stored on disk file before release the
            // locks. But instead of forcing flush, you need to wait for LOG_TIMEOUT or other operations to implicitly trigger
            // the flush operations. write log and update transaction's prev_lsn here
            LogRecord log{txn->GetTransactionId(), txn->GetPrevLSN(), LogRecordType::COMMIT};
            txn->SetPrevLSN(log_manager_->AppendLogRecord(log));
            log_manager_->Flush(false); //需要FLUSH FALSE，来阻塞当前线程，直到LOG MANAGER 把LOG刷到磁盘，唤醒之后，TXN才能释放。
        }
        //释放所有的锁
        // release all the lock
        std::unordered_set<RID> lock_set;
        for (auto item : *txn->GetSharedLockSet())
            lock_set.emplace(item);
        for (auto item : *txn->GetExclusiveLockSet())
            lock_set.emplace(item);
        // release all the lock
        for (auto locked_rid : lock_set)
        {
            lock_manager_->Unlock(txn, locked_rid);
        }
    }

    void TransactionManager::Abort(Transaction *txn)
    {
        txn->SetState(TransactionState::ABORTED);
        // rollback before releasing lock
        auto write_set = txn->GetWriteSet();
        //回滚：假删，插入，更新
        while (!write_set->empty())
        {
            auto &item = write_set->back();
            auto table = item.table_;
            if (item.wtype_ == WType::DELETE)           //是markdelete的操作，则用对应的rollbackdelete回滚
            {
                LOG_DEBUG("rollback delete");
                table->RollbackDelete(item.rid_, txn);
            }
            else if (item.wtype_ == WType::INSERT)  //插入-->删除
            {
                LOG_DEBUG("rollback insert");
                table->ApplyDelete(item.rid_, txn);
            }
            else if (item.wtype_ == WType::UPDATE)      //更新-->反向更新回滚
            {
                LOG_DEBUG("rollback update");
                table->UpdateTuple(item.tuple_, item.rid_, txn);
            }
            write_set->pop_back();
        }
        write_set->clear();

        if (ENABLE_LOGGING)
        {
            // write log and update transaction's prev_lsn here
            LogRecord log{txn->GetTransactionId(), txn->GetPrevLSN(), LogRecordType::ABORT};
            txn->SetPrevLSN(log_manager_->AppendLogRecord(log));
            log_manager_->Flush(false);
        }

        // release all the lock
        std::unordered_set<RID> lock_set;
        for (auto item : *txn->GetSharedLockSet())
            lock_set.emplace(item);
        for (auto item : *txn->GetExclusiveLockSet())
            lock_set.emplace(item);
        // release all the lock
        for (auto locked_rid : lock_set)
        {
            lock_manager_->Unlock(txn, locked_rid);
        }
    }
} // namespace cmudb
