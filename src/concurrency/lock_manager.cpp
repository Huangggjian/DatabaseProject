/**
 * lock_manager.cpp
 */

#include "concurrency/lock_manager.h"
using namespace std;

namespace cmudb
{

    bool LockManager::LockShared(Transaction *txn, const RID &rid)
    {
        return lockTemplate(txn, rid, LockMode::SHARED);
    }

    bool LockManager::LockExclusive(Transaction *txn, const RID &rid)
    {
        return lockTemplate(txn, rid, LockMode::EXCLUSIVE);
    }

    bool LockManager::LockUpgrade(Transaction *txn, const RID &rid)
    {
        return lockTemplate(txn, rid, LockMode::UPGRADING);
    }
    // lock:
    // return false if transaction is aborted
    // it should be blocked on waiting and should return true when granted
    // note the behavior of trying to lock locked rids by same txn is undefined
    // it is transaction's job to keep track of its current locks
    bool LockManager::lockTemplate(Transaction *txn, const RID &rid, LockMode mode)
    {
        // step 1
        if (txn->GetState() != TransactionState::GROWING) //不在上锁阶段。（初始为GROWING）
        {
            txn->SetState(TransactionState::ABORTED);
            return false;
        }
        unique_lock<mutex> tableLatch(mutex_); //锁hash table
        TxList &txList = lockTable_[rid];
        unique_lock<mutex> txListLatch(txList.mutex_); //锁其中一个链表
        tableLatch.unlock();

        if (mode == LockMode::UPGRADING)
        {                             //step 2
            if (txList.hasUpgrading_) //只能有一次更新锁
            {
                txn->SetState(TransactionState::ABORTED);
                return false;
            }
            auto it = find_if(txList.locks_.begin(), txList.locks_.end(),
                              [txn](const TxItem &item)
                              { return item.tid_ == txn->GetTransactionId(); });
            if (it == txList.locks_.end() || it->mode_ != LockMode::SHARED || !it->granted_) //不存在||存在但他不是S锁||存在但他不grant。
            {
                txn->SetState(TransactionState::ABORTED);
                return false;
            }
            txList.locks_.erase(it);                          //从链表中删
            assert(txn->GetSharedLockSet()->erase(rid) == 1); //从S锁set删
        }
        //step 3
        bool canGrant = txList.checkCanGrant(mode); //只有链表为空 或者 mode == S且链表最后一个锁为granted的S锁才返回true

        //如果canGrant==false且当前事务id（新）大于链表最后一个事件id(旧)，直接abort
        //(wait-die机制只允许时间戳小的等待时间戳大的事务，也就是说在wait-for graph中任意一条边Ti->Tj，Ti的时间戳都小于Tj，显然不可能出现环。所以不会出现环，也就不可能出现死锁。)
        if (!canGrant && txList.locks_.back().tid_ < txn->GetTransactionId())
        {
            txn->SetState(TransactionState::ABORTED);
            return false;
        }
        //链表插入锁
        txList.insert(txn, rid, mode, canGrant, &txListLatch);
        return true;
    }

    bool LockManager::Unlock(Transaction *txn, const RID &rid)
    {
        /*
            2PL（2 Phase Locking）, 锁分两阶段，一阶段申请，一阶段释放
            S2PL（Strict 2PL），在2PL的基础上，写锁保持到事务结束
            SS2PL（ Strong 2PL），在2PL的基础上，读写锁都保持到事务结束
        
        */
        if (strict_2PL_) //s2pl必须处于COMMITED 或者 ABORT才可以unlock
        {                //step1
            if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED)
            {
                txn->SetState(TransactionState::ABORTED);
                return false;
            }
        }
        else if (txn->GetState() == TransactionState::GROWING) //如果还在上锁阶段（growing） 。转成解锁阶段（shrinking）
        {
            txn->SetState(TransactionState::SHRINKING);
        }
        unique_lock<mutex> tableLatch(mutex_); //锁hash table
        TxList &txList = lockTable_[rid];
        unique_lock<mutex> txListLatch(txList.mutex_); //锁链表
        //step 2 remove txList and txn->lockset
        auto it = find_if(txList.locks_.begin(), txList.locks_.end(),
                          [txn](const TxItem &item)
                          { return item.tid_ == txn->GetTransactionId(); });
        assert(it != txList.locks_.end()); //肯定是存在的！（lock阶段加进去了）
        auto lockSet = it->mode_ == LockMode::SHARED ? txn->GetSharedLockSet() : txn->GetExclusiveLockSet();
        assert(lockSet->erase(rid) == 1); //先从集合删掉
        txList.locks_.erase(it);          //链表删！
        if (txList.locks_.empty())        //链表为空了。从unordered_map里删掉key:rid.直接返回
        {
            lockTable_.erase(rid);
            return true;
        }
        tableLatch.unlock();
        //step 3 check can grant other
        for (auto &tx : txList.locks_) //链表里其他的
        {
            if (tx.granted_) //已经是granted。ok，那就就是你了。退出
                break;
            tx.Grant();                       //grant blocking one。原来阻塞着等待granted的，那现在轮到你了
            if (tx.mode_ == LockMode::SHARED) //是S锁
            {
                continue;
            }
            if (tx.mode_ == LockMode::UPGRADING) //是更新锁
            {
                txList.hasUpgrading_ = false;   //置位
                tx.mode_ = LockMode::EXCLUSIVE; //升级
            }
            break;
        }
        return true;
    }

} // namespace cmudb
