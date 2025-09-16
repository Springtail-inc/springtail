#pragma once

#include <thread>
#include <memory>
#include <vector>
#include <functional>
#include <iostream>

#include <common/exception.hh>
#include <common/logging.hh>
#include <common/concurrent_queue.hh>

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
                                  ConcurrentQueue<ThreadRequest> &queue)
            {
                LOG_DEBUG(LOG_COMMON, LOG_LEVEL_DEBUG1, "Thread starting: {}\n", std::this_thread::get_id());
                while (true) {
                    ThreadRequestPtr request = queue.pop();
                    // only get a nullptr if queue is empty, if so check for shutdown
                    if (request == nullptr) {
                        if (worker->is_shutdown()) {
                            LOG_DEBUG(LOG_COMMON, LOG_LEVEL_DEBUG1, "Thread exiting: {}\n", std::this_thread::get_id());
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
                               ConcurrentQueue<ThreadRequest> &queue)> get_worker_fn() {
                return this->worker_fn;
            }
        }; // Class ThreadWorker

    public:
        /**
         * @brief Construct a new Thread Pool object
         * @param max_threads number of threads to create
         */
        ThreadPool(int max_threads, std::optional<std::string> thread_name = std::nullopt)
        {
            std::string thread_name_base;
            if (thread_name.has_value()) {
                thread_name_base = thread_name.value();
                if (thread_name_base.length() > 12) {
                    thread_name_base = thread_name_base.substr(0, 12);
                }
            } else {
                thread_name_base = "TPoolWorker";
            }
            for (int i = 0; i < max_threads; i++) {
                ThreadWorkerPtr worker = std::make_shared<ThreadWorker>();
                _workers.push_back(worker);

                std::string thread_name = fmt::format("{}_{}", thread_name_base, i);
                _threads.emplace_back(worker->get_worker_fn(), worker, std::ref(_queue));
                pthread_setname_np(_threads.back().native_handle(), thread_name.c_str());
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
        ConcurrentQueue<ThreadRequest> _queue;
        /** list of thread */
        std::vector<std::thread> _threads;
        /** list of worker threads */
        std::vector<ThreadWorkerPtr> _workers;
        /** shutdown flag */
        bool _shutdown = false;
    };
}