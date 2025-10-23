#pragma once

#include <deque>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <memory>
#include <vector>

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
         * @param seconds timeout in seconds
         * @return std::shared_ptr<T> log queue entry, nullptr if no entry found either due to
         *         timeout or shutdown
         */
        Tptr pop(uint32_t seconds = 0)
        {
            std::unique_lock<std::mutex> write_lock{_mutex};
            while (_queue.empty() && !_shutdown) {
                // wait on cv until not empty
                if (seconds) {
                    // wait for the requested number of seconds
                    _cv_pop.wait_for(write_lock, std::chrono::seconds(seconds));
                    if (_queue.empty() && !_shutdown) {
                        // timeout
                        return nullptr;
                    }
                } else {
                    // wait indefinitely
                    _cv_pop.wait(write_lock);
                }
            }

            if (_queue.empty()) {
                write_lock.unlock();
                if (_shutdown) {
                    _cv_shutdown.notify_all();
                }
                return nullptr;
            }

            Tptr entry = _queue.front();
            _queue.pop_front();

            write_lock.unlock();

            _cv_push.notify_one();

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
            _queue.pop_front();

            write_lock.unlock();

            _cv_push.notify_one();

            return entry;
        }

        /**
         * @brief Pop all entries from queue in one operation, optionally waiting for entries
         * @param seconds timeout in seconds
         * @return std::deque<Tptr> all entries currently in queue, empty deque if no entries
         *         found either due to timeout or shutdown
         */
        std::deque<Tptr> pop_all(uint32_t seconds = 0)
        {
            std::unique_lock<std::mutex> write_lock{_mutex};
            while (_queue.empty() && !_shutdown) {
                // wait on cv until not empty
                if (seconds) {
                    // wait for the requested number of seconds
                    _cv_pop.wait_for(write_lock, std::chrono::seconds(seconds));
                    if (_queue.empty() && !_shutdown) {
                        // timeout
                        return std::deque<Tptr>();
                    }
                } else {
                    // wait indefinitely
                    _cv_pop.wait(write_lock);
                }
            }

            if (_queue.empty()) {
                write_lock.unlock();
                if (_shutdown) {
                    _cv_shutdown.notify_all();
                }
                return std::deque<Tptr>();
            }

            // Efficiently extract all items by swapping with an empty deque
            std::deque<Tptr> result;
            std::swap(_queue, result);

            write_lock.unlock();

            // Notify any threads waiting to push (in case we had a bounded queue)
            _cv_push.notify_all();

            return result;
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
        void shutdown(bool wait=false)
        {
            if (!_shutdown) {
                _shutdown = true;
                _cv_pop.notify_all();
            }

            if (wait) {
                std::unique_lock<std::mutex> write_lock{_mutex};
                while (!_queue.empty()) {
                    _cv_shutdown.wait(write_lock);
                }
            }
        }

        /** is queue empty */
        bool empty() const {
            std::unique_lock<std::mutex> write_lock{_mutex};
            return _queue.empty();
        }

        /** get size of queue */
        int size() const {
            std::unique_lock<std::mutex> write_lock{_mutex};
            return _queue.size();
        }

        /** clear the queue */
        void clear() {
            std::unique_lock<std::mutex> write_lock{_mutex};
            _queue.clear();
        }

        /** is queue shutdown */
        bool is_shutdown() const {
            return _shutdown;
        }

    protected:
        /** max number of elements in queue */
        std::size_t _limit=-1;
        /** mutex to protect queue */
        mutable std::mutex _mutex;
        /** condition variable for queue to wait on */
        std::condition_variable _cv_pop;
        /** condition variable to wait on if queue is full */
        std::condition_variable _cv_push;
        /** condition variable to wait on empty for shutdown */
        std::condition_variable _cv_shutdown;
        /** internal queue */
        std::deque<Tptr> _queue;
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
            _queue.push_back(entry);

            // notify condition variable
            _cv_pop.notify_one();
        }
    };
}
