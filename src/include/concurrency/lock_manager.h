/**
 * lock_manager.h
 *
 * Tuple level lock manager, use wait-die to prevent deadlocks
 */

#pragma once

#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <algorithm>
#include <cassert>

#include "common/rid.h"
#include "concurrency/transaction.h"
using namespace std;
namespace cmudb
{
    //从数据库系统角度分为三种：排他锁、共享锁、更新锁。
    enum class LockMode
    {
        SHARED = 0,
        EXCLUSIVE,
        UPGRADING //用来预定要对此页施加X锁，它允许其他事务读，但不允许再施加U锁或X锁；当被读取的页将要被更新时，则升级为X锁；U锁一直到事务结束时才能被释放。
    };

    class LockManager
    {

        struct TxItem
        {
            TxItem(txn_id_t tid, LockMode mode, bool granted) : tid_(tid), mode_(mode), granted_(granted) {}

            void Wait()
            {
                unique_lock<mutex> ul(mutex_);
                cv_.wait(ul, [this]
                         { return this->granted_; }); //等待可以上锁
            }

            void Grant()
            {
                lock_guard<mutex> lg(mutex_);
                granted_ = true;
                cv_.notify_one();
            }

            mutex mutex_;
            condition_variable cv_;
            txn_id_t tid_;
            LockMode mode_;
            bool granted_;
        };

        struct TxList
        {
            mutex mutex_;
            list<TxItem> locks_;
            bool hasUpgrading_; //一个链表只能持有一次更新锁
            bool checkCanGrant(LockMode mode)
            { //protect by mutex outside
                if (locks_.empty())
                    return true;
                const auto last = &locks_.back(); //链表最后一个TxItem
                if (mode == LockMode::SHARED)
                {
                    return last->granted_ && last->mode_ == LockMode::SHARED;
                }
                return false;
            }
            void insert(Transaction *txn, const RID &rid, LockMode mode, bool granted, unique_lock<mutex> *lock)
            {
                bool upgradingMode = (mode == LockMode::UPGRADING); //是不是更新锁
                if (upgradingMode && granted)                       //是更新锁，且是granted。则mode变为X （这种情况只发生在链表原来只有一个granted的S锁，因为要它要升级锁，被删掉后链表变为空）
                    mode = LockMode::EXCLUSIVE;
                locks_.emplace_back(txn->GetTransactionId(), mode, granted); //链表插入
                auto &last = locks_.back();
                if (!granted)
                {
                    hasUpgrading_ |= upgradingMode;
                    lock->unlock(); //解锁，其他线程可以继续往这链表插入。 unique_lock支持自己unlock.lock_guard则不支持，只能超出作用范围程序自己unlock
                    last.Wait();    //等待刚刚插入的这个的granted变为true
                }
                if (mode == LockMode::SHARED)
                {
                    txn->GetSharedLockSet()->insert(rid);
                }
                else //mode == LockMode::EXCLUSIVE || mode == LockMode::UPGRADING
                {
                    txn->GetExclusiveLockSet()->insert(rid);
                }
            }
        };

    public:
        LockManager(bool strict_2PL) : strict_2PL_(strict_2PL){};

        /*** below are APIs need to implement ***/
        // lock:
        // return false if transaction is aborted
        // it should be blocked on waiting and should return true when granted
        // note the behavior of trying to lock locked rids by same txn is undefined
        // it is transaction's job to keep track of its current locks
        bool LockShared(Transaction *txn, const RID &rid);
        bool LockExclusive(Transaction *txn, const RID &rid);
        bool LockUpgrade(Transaction *txn, const RID &rid);

        // unlock:
        // release the lock hold by the txn
        bool Unlock(Transaction *txn, const RID &rid);
        /*** END OF APIs ***/
    private:
        bool lockTemplate(Transaction *txn, const RID &rid, LockMode mode);

        bool strict_2PL_;
        mutex mutex_;
        unordered_map<RID, TxList> lockTable_; //hash table + 链表   key值是RID（行记录：page id + slot num）
    };

} // namespace cmudb
