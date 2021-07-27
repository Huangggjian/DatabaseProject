/**
 * rwmutex.h
 *
 * Reader-Writer lock
 */

#pragma once

#include <climits>
#include <condition_variable>
#include <mutex>

namespace cmudb
{
    class RWMutex
    {

        typedef std::mutex mutex_t;
        typedef std::condition_variable cond_t;
        static const uint32_t max_readers_ = UINT_MAX;

    public:
        RWMutex() : reader_count_(0), writer_entered_(false) {}

        ~RWMutex() { std::lock_guard<mutex_t> guard(mutex_); }

        RWMutex(const RWMutex &) = delete;
        RWMutex &operator=(const RWMutex &) = delete;

        void WLock()
        {
            std::unique_lock<mutex_t> lock(mutex_);
            while (writer_entered_)
                reader_.wait(lock);
            writer_entered_ = true;//先加上标记位置，只要读全部释放了（read_count == 0），就可以解除阻塞了.保证写优先
            while (reader_count_ > 0)
                writer_.wait(lock);
        }

        void WUnlock()
        {
            std::lock_guard<mutex_t> guard(mutex_);
            writer_entered_ = false;
            reader_.notify_all();
        }

        void RLock()
        {
            std::unique_lock<mutex_t> lock(mutex_);
            while (writer_entered_ || reader_count_ == max_readers_)//判断写标记位。
                reader_.wait(lock);
            reader_count_++;
        }

        void RUnlock()
        {
            std::lock_guard<mutex_t> guard(mutex_);
            reader_count_--;
            if (writer_entered_)
            {
                if (reader_count_ == 0)
                    writer_.notify_one();
            }
            else
            {
                if (reader_count_ == max_readers_ - 1)
                    reader_.notify_one();
            }
        }

    private:
        mutex_t mutex_;
        cond_t writer_;
        cond_t reader_;
        uint32_t reader_count_;
        bool writer_entered_;
    };
} // namespace cmudb
