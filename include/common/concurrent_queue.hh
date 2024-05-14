#pragma once

#include <queue>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>
#include <memory>
#include <type_traits>

namespace springtail {
    /**
     * @brief Concurrent (threadsafe) queue with optional limit (or unbounded)
     * @tparam T Base entry type; actual entry should be std::shared_ptr<T>
     */
    template<typename T>
    class ConcurrentQueue {

        using Tptr = std::shared_ptr<T>;

    public:
        /** constructor, limit = number of elements in queue; -1 unbounded */
        ConcurrentQueue(int limit) : _limit(limit) {}

        ConcurrentQueue() : _limit(-1) {}

        /** virtual destructor */
        virtual ~ConcurrentQueue() {}

        /**
         * @brief Push entry onto queue, try to merge with entry on back of queue if possible
         * @param entry std::shared_ptr<T> entry to push onto queue
         */
        void push(Tptr entry)
        {
            std::unique_lock<std::mutex> write_lock{_mutex};
            _internal_push(entry, write_lock);
        }

        /**
         * @brief Push multiple entries onto queue at once, get the write lock once, may block
         * @param entries list of entries to push on queue
         */
        void push(const std::vector<Tptr> &entries)
        {
            if (entries.empty()) {
                return;
            }

            std::unique_lock<std::mutex> write_lock{_mutex};
            for (auto entry: entries) {
                _internal_push(entry, write_lock);
            }
        }

        /**
         * @brief Push multiple entries onto queue at once, get the write lock once, may block
         * @param entries list of entries to push on queue
         */
        void push(const std::shared_ptr<std::vector<Tptr>> entries)
        {
            push(*entries);
        }

        /**
         * @brief Pop entry from queue, optionally waiting for entry
         * @return std::shared_ptr<T> log queue entry
         */
        Tptr pop()
        {
            std::unique_lock<std::mutex> write_lock{_mutex};
            while (_queue.empty() && !_shutdown) {
                // wait on cv until not empty
                _cv_pop.wait(write_lock);
            }

            if (_queue.empty()) {
                return nullptr;
            }

            Tptr entry = _queue.front();
            _queue.pop();

            write_lock.unlock();

            if (_queue.size() >= (_limit-1)) {
                _cv_push.notify_one();
            }

            return entry;
        }

        /**
         * @brief Try and pop an entry from the queue, return nullptr if queue is empty
         * @return Tptr entry or nullptr if queue is empty
         */
        Tptr try_pop()
        {
            std::unique_lock<std::mutex> write_lock{_mutex};
            if (_queue.empty()) {
                return nullptr;
            }

            Tptr entry = _queue.front();
            _queue.pop();

            write_lock.unlock();

            if (_queue.size() >= (_limit-1)) {
                _cv_push.notify_one();
            }

            return entry;
        }

        /**
         * @brief Peek at the front of the queue without unlocking
         * Note: another thread may pop this item between front() and pop(); so use carefully
         * @return std::shared_ptr<T> element at front of queue
         */
        Tptr front() {
            std::unique_lock<std::mutex> write_lock{_mutex};
            if (_queue.empty()) {
                return nullptr;
            }

            Tptr &entry = _queue.front();
            return entry;
        }

        /** Shutdown queue */
        void shutdown()
        {
            _shutdown = true;
            _cv_pop.notify_all();
        }

        /** is queue empty */
        bool empty() {
            std::unique_lock<std::mutex> write_lock{_mutex};
            return _queue.empty();
        }

        /** get size of queue */
        int size() {
            std::unique_lock<std::mutex> write_lock{_mutex};
            return _queue.size();
        }

        /** clear the queue */
        void clear() {
            std::unique_lock<std::mutex> write_lock{_mutex};
            while (!_queue.empty()) {
                _queue.pop();
            }
        }

    protected:
        /** max number of elements in queue */
        int _limit=-1;
        /** mutex to protect queue */
        std::mutex _mutex;
        /** condition variable for queue to wait on */
        std::condition_variable _cv_pop;
        /** condition variable to wait on if queue is full */
        std::condition_variable _cv_push;
        /** internal queue */
        std::queue<Tptr> _queue;
        /** shutdown flag */
        std::atomic<bool> _shutdown = false;

        /** push variant that accepts an already locked write lock */
        void _internal_push(Tptr entry, std::unique_lock<std::mutex> &write_lock)
        {
            // if this is a bounded queue wait until it decreases in size
            while (_queue.size() >= _limit) {
                _cv_push.wait(write_lock);
            }

            // push entry
            _queue.push(entry);

            // notify condition variable
            _cv_pop.notify_one();
        }
    };
}
