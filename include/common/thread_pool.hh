#pragma once

#include <mutex>
#include <queue>
#include <thread>
#include <memory>
#include <vector>
#include <functional>
#include <condition_variable>

#include <common/exception.hh>

namespace springtail {
    
    /**
     * @brief Thread queue
     * @tparam ThreadRequest class that implements the request to be queued 
     */
    template <class ThreadRequest>
    class ThreadQueue {
    private:
        /** underlying request queue */
        std::queue<std::shared_ptr<ThreadRequest>> _queue;
        /** condition variable to block access */
        std::condition_variable _cv;
        /** mutex protecting condition variable */
        std::mutex _mutex;
        /** shutdown flag */
        bool _shutdown = false;

    public:
        /**
         * @brief Push an IO request onto the worker pool queue
         * @param request IO request to enqueue
         */
        void push(std::shared_ptr<ThreadRequest> request);

        /**
         * @brief Pop an IO request off the worker pool queue; or block if empty
         * @return std::shared_ptr<IORequest> ptr to the IORequest
         */        
        std::shared_ptr<ThreadRequest> pop();

        /**
         * @brief Set shutdown flag; wake all blocked workers
         */
        inline void shutdown() { 
            _shutdown = true;
            _cv.notify_all();
        }
    };

    /**
     * @brief Worker thread
     * @tparam ThreadRequest class that implements the request to be queued 
     */
    template<class ThreadRequest>
    class ThreadWorker {   
    private:
        bool _shutdown = false;   ///< shutdown flag for worker
        
        /**
         * @brief Worker function; main worker thread entry
         * @param worker Worker thread
         * @param queue  Thread queue
         */
        static void worker_fn(std::shared_ptr<ThreadWorker<ThreadRequest>> worker,
                              ThreadQueue<ThreadRequest> &queue);

    public:

        /**
         * @brief Construct a new Thread Worker object
         * @param process_fn function ptr for processing ThreadRequest
         */
        ThreadWorker() {}

        /**
         * @brief Destroy the Thread Worker object
         */
        ~ThreadWorker() {}

        /**
         * @brief Set shutdown flag to true
         */
        inline void shutdown() { 
            _shutdown = true;
        }

        /**
         * @brief Is shutdown set on this worker
         * @return true if set
         * @return false if not set
         */
        bool is_shutdown() { return _shutdown; }

        /**
         * @brief Get the worker process function ptr
         * @return std::function<void(std::shared_ptr<ThreadWorker<ThreadRequest>> worker,
         * ThreadQueue<ThreadRequest> queue)> (static function)
         */
        inline
        std::function<void(std::shared_ptr<ThreadWorker<ThreadRequest>> worker,
                           ThreadQueue<ThreadRequest> &queue)> get_worker_fn() {
            return this->worker_fn;
        }
    };

    /**
     * @brief Thread pool
     * @tparam ThreadRequest class implements the request to be queued 
     * @tparam ThreadContext class context passed into the worker (constructed per worker)
     */
    template <class ThreadRequest>
    class ThreadPool {
    public:
        /**
         * @brief Construct a new Thread Pool object
         * @param max_threads number of threads to create
         */
        ThreadPool(int max_threads);

        /**
         * @brief Destroy the Thread Pool object; shuts down all threads if not already
         */
        ~ThreadPool() { shutdown(); };

        /**
         * @brief Shutdown thread pool; pool can not be used after this call
         */
        void shutdown();

        /**
         * @brief Queue the thread request
         * @param request request to queue
         */
        void queue(std::shared_ptr<ThreadRequest> request);

    private:
        /** thread queue */
        ThreadQueue<ThreadRequest> _queue;
        /** list of thread */
        std::vector<std::thread> _threads;
        /** list of worker threads */
        std::vector<std::shared_ptr<ThreadWorker<ThreadRequest>>> _workers;
        /** shutdown flag */ 
        bool _shutdown = false;
    };
}