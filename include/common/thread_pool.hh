#pragma once

#include <mutex>
#include <queue>
#include <thread>
#include <memory>
#include <vector>
#include <functional>
#include <condition_variable>
#include <iostream>
#include <atomic>

#include <common/exception.hh>
#include <common/logging.hh>

namespace springtail {
    /**
     * @brief Thread pool
     * @tparam ThreadRequest class implements the request to be queued
     * @tparam ThreadContext class context passed into the worker (constructed per worker)
     */
    template <class ThreadRequest>
    class ThreadPool {
        using ThreadRequestPtr = std::shared_ptr<ThreadRequest>;
    private:
        /**
         * @brief Thread queue -- helper class
         * @tparam ThreadRequest class that implements the request to be queued
         */
        class ThreadQueue {
        private:
            /** underlying request queue */
            std::queue<ThreadRequestPtr> _queue;
            /** condition variable to block access */
            std::condition_variable _cv;
            /** mutex protecting condition variable */
            std::mutex _mutex;
            /** shutdown flag */
            std::atomic<bool> _shutdown = false;

        public:
            /**
             * @brief Push a request onto the worker pool queue
             * @param request request to enqueue
             */
            void push(ThreadRequestPtr request)
            {
                // lock queue lock
                std::scoped_lock<std::mutex> queue_lock(_mutex);

                // push request onto queue and notify a single thread
                _queue.push(request);
                _cv.notify_one();
            }

            /**
             * @brief Push many requests onto queue at once and notify all threads
             * @param requests requests to enqueue
             */
            void push(const std::vector<ThreadRequestPtr> &requests)
            {
                // lock queue lock
                std::scoped_lock<std::mutex> queue_lock(_mutex);

                // push requests onto queue and notify all threads
                for (auto request: requests) {
                    _queue.push(request);
                }
                _cv.notify_all();
            }

            /**
             * @brief Pop an IO request off the worker pool queue; or block if empty
             *        If shutdown is set, queue will be allowed to drain.
             * @return std::shared_ptr<ThreadRequest> ptr to the ThreadRequest
             */
            ThreadRequestPtr pop()
            {
                // lock queue lock
                std::unique_lock<std::mutex> queue_lock(_mutex);

                // block until queue is not empty
                // or until queue is signalled to wake all
                while (_queue.empty() && !_shutdown) {
                    _cv.wait(queue_lock);
                }

                // extract first element from queue and return it
                if (_queue.empty()) {
                    return nullptr;
                }

                ThreadRequestPtr val = _queue.front();
                _queue.pop();

                return val;
            }

            /**
             * @brief Set shutdown flag; wake all blocked workers
             */
            inline void shutdown() {
                _shutdown = true;
                _cv.notify_all();
            }
        };  // Class ThreadQueue

        // forward decl for ptr typedef
        class ThreadWorker;
        using ThreadWorkerPtr = std::shared_ptr<ThreadWorker>;

        /**
         * @brief Worker thread helper class
         */
        class ThreadWorker {
        private:
            bool _shutdown = false;   ///< shutdown flag for worker

            /**
             * @brief Worker function; main worker thread entry
             * @param worker Worker thread
             * @param queue  Thread queue
             */
            static void worker_fn(ThreadWorkerPtr worker,
                                  ThreadQueue &queue)
            {
                SPDLOG_DEBUG("Thread starting: {}\n", std::this_thread::get_id());
                while (true) {
                    ThreadRequestPtr request = queue.pop();
                    // only get a nullptr if queue is empty, if so check for shutdown
                    if (request == nullptr) {
                        if (worker->is_shutdown()) {
                            SPDLOG_DEBUG("Thread exiting: {}\n", std::this_thread::get_id());
                            return;
                        }
                        continue;
                    }

                    // process request
                    (*request)();
                }
            }

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
            std::function<void(ThreadWorkerPtr worker,
                            ThreadQueue &queue)> get_worker_fn() {
                return this->worker_fn;
            }
        }; // Class ThreadWorker

    public:
        /**
         * @brief Construct a new Thread Pool object
         * @param max_threads number of threads to create
         */
        ThreadPool(int max_threads)
        {
            for (int i = 0; i < max_threads; i++) {
                ThreadWorkerPtr worker = std::make_shared<ThreadWorker>();
                _workers.push_back(worker);
                _threads.push_back(std::thread(worker->get_worker_fn(), worker, std::ref(_queue)));
            }
        }

        /**
         * @brief Destroy the Thread Pool object; shuts down all threads if not already
         */
        ~ThreadPool() { shutdown(); };

        /**
         * @brief Shutdown thread pool; pool can not be used after this call
         * Queue will first drain and then will shutdown
         */
        void shutdown()
        {
            if (_shutdown) {
                return;
            }

            // set shutdown flag
            _shutdown = true;

            // issue shutdown to workers
            for (auto worker: _workers) {
                worker->shutdown();
            }

            // issue shutdown to queue (this will drain the queue)
            _queue.shutdown();

            // join the threads and remove them from the vector
            while (!_threads.empty()) {
                std::thread &t = _threads.back();
                std::cout << "Thread joining: " << t.get_id() << std::endl;
                t.join();
                _threads.pop_back();
            }
        }

        /**
         * @brief Queue the thread request; exception if shutdown
         * @param request request to queue
         */
        void queue(ThreadRequestPtr request)
        {
            if (_shutdown) {
                throw Error("Pool has shutdown\n");
            }
            _queue.push(request);
        }

        /**
         * @brief Queue multiple thread requests at once; exception if shutdown
         * @param requests requests to queue
         */
        void queue(const std::vector<ThreadRequestPtr> &requests)
        {
            if (_shutdown) {
                throw Error("Pool has shutdown\n");
            }
            _queue.push(requests);
        }

    private:
        /** thread queue */
        ThreadQueue _queue;
        /** list of thread */
        std::vector<std::thread> _threads;
        /** list of worker threads */
        std::vector<ThreadWorkerPtr> _workers;
        /** shutdown flag */
        bool _shutdown = false;
    };
}