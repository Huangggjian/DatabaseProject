/**
 * log_recovey.cpp
 */

#include "logging/log_recovery.h"
#include "page/table_page.h"

namespace cmudb
{
    /*
 * deserialize a log record from log buffer
 * @return: true means deserialize succeed, otherwise can't deserialize cause
 * incomplete log record
 */
    //根据不同的操作类型，根据log_record.h展示的日志结构，解析出来
    bool LogRecovery::DeserializeLogRecord(const char *data,
                                           LogRecord &log_record)
    {
        //直到DESERIALIZE 失败，什么时候失败呢？
        //就是要解析的LOG的位置，超过了log_buffer_+LOG_BUFFER_SIZE
        if (data + LogRecord::HEADER_SIZE > log_buffer_ + LOG_BUFFER_SIZE)
            return false;
        memcpy(&log_record, data, LogRecord::HEADER_SIZE); //COPY HEADER.  把data的前20字节当成header
        if (log_record.size_ <= 0 || data + log_record.size_ > log_buffer_ + LOG_BUFFER_SIZE)//size=0或者长度不够的
            return false;
        data += LogRecord::HEADER_SIZE;     //data指针移动
        switch (log_record.log_record_type_)
        {
        case LogRecordType::INSERT:
            log_record.insert_rid_ = *reinterpret_cast<const RID *>(data);
            log_record.insert_tuple_.DeserializeFrom(data + sizeof(RID));
            break;
        case LogRecordType::MARKDELETE:
        case LogRecordType::APPLYDELETE:
        case LogRecordType::ROLLBACKDELETE:
            log_record.delete_rid_ = *reinterpret_cast<const RID *>(data);
            log_record.delete_tuple_.DeserializeFrom(data + sizeof(RID));
            break;
        case LogRecordType::UPDATE:
            log_record.update_rid_ = *reinterpret_cast<const RID *>(data);
            log_record.old_tuple_.DeserializeFrom(data + sizeof(RID));
            log_record.new_tuple_.DeserializeFrom(data + sizeof(RID) + 4 + log_record.old_tuple_.GetLength());
            break;
        case LogRecordType::BEGIN:
        case LogRecordType::COMMIT:
        case LogRecordType::ABORT:
            break;
        case LogRecordType::NEWPAGE:
            log_record.prev_page_id_ = *reinterpret_cast<const page_id_t *>(data);
            log_record.page_id_ = *reinterpret_cast<const page_id_t *>(data + sizeof(page_id_t));
            break;
        default:
            assert(false);
        }
        return true;
    }

    /*
 *redo phase on TABLE PAGE level(table/table_page.h)
 *read log file from the beginning to end (you must prefetch log records into
 *log buffer to reduce unnecessary I/O operations), remember to compare page's
 *LSN with log_record's sequence number, and also build active_txn_ table &
 *lsn_mapping_ table
 */
    void LogRecovery::Redo()
    {
        //lock_guard<mutex> lock(mu_); no thread safe
        // ENABLE_LOGGING must be false when recovery
        assert(ENABLE_LOGGING == false);
        // always replay history from start without checkpoint
        offset_ = 0;
        int bufferOffset = 0;//bufferoffset是前面可能有没有处理完整的日志信息还留在开头，所有需要加上不完整残留长度，在它的后面接着写
                //LOG_BUFFER_SIZE - bufferOffset  ==> 意思是还能继续写的字节数。最多LOG_BUFFER_SIZE个减去残留的bufferOffset个
                //offset ==> logfile的偏移 代表着当前从log file读取了多少过来了，下次继续读取的时候要从offset的地方开始读取
        //先把磁盘上的读进来。log file从偏移offset处，读取size字节到logbuffer去
        //                               char *log_data             int size                     int offset
        while (disk_manager_->ReadLog(log_buffer_ + bufferOffset, LOG_BUFFER_SIZE - bufferOffset, offset_)) //读完为止. offset超过了文件所能容纳的大小
        {                                                                                                   // false means log eof
            int temp = offset_;     //最开始的日志文件偏移
            offset_ += LOG_BUFFER_SIZE - bufferOffset;//偏移 增加int size
            bufferOffset = 0;       //记录在log file偏移。每处理一条日志 他就加log.size 实际存放了当前处理的log大小
            LogRecord log;
            while (DeserializeLogRecord(log_buffer_ + bufferOffset, log)) //log_buffer_ + bufferOffset是新的日志起始的位置
            {
                lsn_mapping_[log.GetLSN()] = temp + bufferOffset;//这条日志在log file中的偏移
                active_txn_[log.txn_id_] = log.lsn_; //活跃的事务id
                bufferOffset += log.size_;           //处理完一条, 偏移+logsize
                if (log.log_record_type_ == LogRecordType::BEGIN)
                    continue;
                if (log.log_record_type_ == LogRecordType::COMMIT ||
                    log.log_record_type_ == LogRecordType::ABORT)
                {
                    assert(active_txn_.erase(log.GetTxnId()) > 0);//已经commit/abort了，那么后面不需要undo 了
                    continue;
                }
                if (log.log_record_type_ == LogRecordType::NEWPAGE)
                {
                    auto page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(log.page_id_));
                    assert(page != nullptr);
                    bool needRedo = log.lsn_ > page->GetLSN(); //需要redo
                    if (needRedo)
                    {
                        page->Init(log.page_id_, PAGE_SIZE, log.prev_page_id_, nullptr, nullptr);
                        page->SetLSN(log.lsn_);
                        if (log.prev_page_id_ != INVALID_PAGE_ID)
                        {
                            auto prevPage = static_cast<TablePage *>(
                                buffer_pool_manager_->FetchPage(log.prev_page_id_));
                            assert(prevPage != nullptr);
                            bool needChange = prevPage->GetNextPageId() == log.page_id_;
                            prevPage->SetNextPageId(log.page_id_);
                            buffer_pool_manager_->UnpinPage(prevPage->GetPageId(), needChange);
                        }
                    }
                    buffer_pool_manager_->UnpinPage(page->GetPageId(), needRedo);

                    continue;
                }
                RID rid = log.log_record_type_ == LogRecordType::INSERT ? log.insert_rid_ : log.log_record_type_ == LogRecordType::UPDATE ? log.update_rid_
                                                                                                                                          : log.delete_rid_;
                auto page = static_cast<TablePage *>(
                    buffer_pool_manager_->FetchPage(rid.GetPageId()));  
                    //FetchPage: 如果不在hashtable（落盘了或者宕机后数据丢失了）从磁盘中读取进来旧的内容到page这一页（当然）
                    //宕机后，可能并没有把所有的TablePage的操作落盘，就是会有缺漏
                   
                assert(page != nullptr);
                //是否需要进行redo log. 就是日志编号大于TablePage最后一次操作的日志号的时候，说明信息缺漏了
                bool needRedo = log.lsn_ > page->GetLSN();
                if (needRedo)
                {
                    if (log.log_record_type_ == LogRecordType::INSERT)
                    {
                        page->InsertTuple(log.insert_tuple_, rid, nullptr, nullptr, nullptr);
                    }
                    else if (log.log_record_type_ == LogRecordType::UPDATE)
                    {
                        page->UpdateTuple(log.new_tuple_, log.old_tuple_, rid, nullptr, nullptr, nullptr);
                    }
                    else if (log.log_record_type_ == LogRecordType::MARKDELETE)
                    {
                        page->MarkDelete(rid, nullptr, nullptr, nullptr);
                    }
                    else if (log.log_record_type_ == LogRecordType::APPLYDELETE)
                    {
                        page->ApplyDelete(rid, nullptr, nullptr);
                    }
                    else if (log.log_record_type_ == LogRecordType::ROLLBACKDELETE)
                    {
                        page->RollbackDelete(rid, nullptr, nullptr);
                    }
                    else
                    {
                        assert(false); //invalid area
                    }
                    page->SetLSN(log.lsn_); //更新最新一次page的日志信息
                }
                buffer_pool_manager_->UnpinPage(page->GetPageId(), needRedo);
            }
            //把最后一段的前半部分的LOG，给存好。（说明log file太大 log buffer只存了最后一条日志的前半部分。需要把最后那条不完整的提到起始位置，
            //继续读取新的）
            memmove(log_buffer_, log_buffer_ + bufferOffset, LOG_BUFFER_SIZE - bufferOffset); //没有处理的移动到前面来
            bufferOffset = LOG_BUFFER_SIZE - bufferOffset;                                    //rest partial log
        }
    }

    /*
 *undo phase on TABLE PAGE level(table/table_page.h)
 *iterate through active txn map and undo each operation
 */
    void LogRecovery::Undo()
    {
        //lock_guard<mutex> lock(mu_); no thread safe
        // ENABLE_LOGGING must be false when recovery
        assert(ENABLE_LOGGING == false);
        for (auto &txn : active_txn_)
        {
            lsn_t lsn = txn.second;
            while (lsn != INVALID_LSN)
            {
                LogRecord log;
                //
                disk_manager_->ReadLog(log_buffer_, PAGE_SIZE, lsn_mapping_[lsn]);
                assert(DeserializeLogRecord(log_buffer_, log));
                assert(log.lsn_ == lsn);
                lsn = log.prev_lsn_;
                if (log.log_record_type_ == LogRecordType::BEGIN)
                {
                    assert(log.prev_lsn_ == INVALID_LSN);
                    continue;
                }
                if (log.log_record_type_ == LogRecordType::COMMIT ||
                    log.log_record_type_ == LogRecordType::ABORT)
                    assert(false);
                if (log.log_record_type_ == LogRecordType::NEWPAGE)
                {
                    if (!buffer_pool_manager_->DeletePage(log.page_id_))
                        disk_manager_->DeallocatePage(log.page_id_);
                    if (log.prev_page_id_ != INVALID_PAGE_ID)
                    {
                        auto prevPage = static_cast<TablePage *>(
                            buffer_pool_manager_->FetchPage(log.prev_page_id_));
                        assert(prevPage != nullptr);
                        assert(prevPage->GetNextPageId() == log.page_id_);
                        prevPage->SetNextPageId(INVALID_PAGE_ID);
                        buffer_pool_manager_->UnpinPage(prevPage->GetPageId(), true);
                    }
                    continue;
                }
                RID rid = log.log_record_type_ == LogRecordType::INSERT ? log.insert_rid_ : log.log_record_type_ == LogRecordType::UPDATE ? log.update_rid_
                                                                                                                                          : log.delete_rid_;
                auto page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
                assert(page != nullptr);
                assert(page->GetLSN() >= log.lsn_);
                if (log.log_record_type_ == LogRecordType::INSERT)
                {
                    page->ApplyDelete(log.insert_rid_, nullptr, nullptr);
                }
                else if (log.log_record_type_ == LogRecordType::UPDATE)
                {
                    Tuple tuple;
                    page->UpdateTuple(log.old_tuple_, tuple, log.update_rid_, nullptr, nullptr, nullptr);
                    assert(tuple.GetLength() == log.new_tuple_.GetLength() &&
                           memcmp(tuple.GetData(), log.new_tuple_.GetData(), tuple.GetLength()) == 0);
                }
                else if (log.log_record_type_ == LogRecordType::MARKDELETE)
                {
                    page->RollbackDelete(log.delete_rid_, nullptr, nullptr);
                }
                else if (log.log_record_type_ == LogRecordType::APPLYDELETE)
                {
                    page->InsertTuple(log.delete_tuple_, log.delete_rid_, nullptr, nullptr, nullptr);
                }
                else if (log.log_record_type_ == LogRecordType::ROLLBACKDELETE)
                {
                    page->MarkDelete(log.delete_rid_, nullptr, nullptr, nullptr);
                }
                else
                    assert(false);
                buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
            }
        }
        active_txn_.clear();
        lsn_mapping_.clear();
    }

} // namespace cmudb
