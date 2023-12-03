#pragma once

#include <mutex>
#include <queue>
#include <thread>
#include <memory>
#include <vector>
#include <functional>
#include <condition_variable>
#include <iostream>

#include <common/exception.hh>

namespace springtail {
    /**
     * @brief Thread pool
     * @tparam ThreadRequest class implements the request to be queued 
     * @tparam ThreadContext class context passed into the worker (constructed per worker)
     */
    template <class ThreadRequest>
    class ThreadPool {
    
    private:
        /**
         * @brief Thread queue -- helper class
         * @tparam ThreadRequest class that implements the request to be queued 
         */
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
            void push(std::shared_ptr<ThreadRequest> request)
            {
                // lock queue lock
                std::scoped_lock<std::mutex> queue_lock(_mutex);

                // push request onto queue and notify a single thread
                _queue.push(request);
                _cv.notify_one();
            }

            /**
             * @brief Pop an IO request off the worker pool queue; or block if empty
             *        If shutdown is set, queue will be allowed to drain.
             * @return std::shared_ptr<IORequest> ptr to the IORequest
             */        
            std::shared_ptr<ThreadRequest> pop()
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

                std::shared_ptr<ThreadRequest> val = _queue.front();
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

        /**
         * @brief Worker thread helper class
         * @tparam ThreadRequest class that implements the request to be queued 
         */
        //template<class ThreadRequest>
        class ThreadWorker {   
        private:
            bool _shutdown = false;   ///< shutdown flag for worker
            
            /**
             * @brief Worker function; main worker thread entry
             * @param worker Worker thread
             * @param queue  Thread queue
             */
            static void worker_fn(std::shared_ptr<ThreadWorker> worker,
                                ThreadQueue &queue)
            {
                std::cout << "Thread starting: " << std::this_thread::get_id() << std::endl;
                while (true) {
                    std::shared_ptr<ThreadRequest> request = queue.pop();
                    // only get a nullptr if queue is empty, if so check for shutdown
                    if (request == nullptr) {
                        if (worker->is_shutdown()) {
                            return;
                        }
                        continue;
                    }

                    // process request
                    (*request)();
                }
                std::cout << "Thread exiting: " << std::this_thread::get_id() << std::endl;
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
            std::function<void(std::shared_ptr<ThreadWorker> worker,
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
                std::shared_ptr<ThreadWorker> worker = std::make_shared<ThreadWorker>();
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
        void queue(std::shared_ptr<ThreadRequest> request)
        {
            if (_shutdown) {
                throw Error("Pool has shutdown\n");
            }
            _queue.push(request);
        }

    private:
        /** thread queue */
        ThreadQueue _queue;
        /** list of thread */
        std::vector<std::thread> _threads;
        /** list of worker threads */
        std::vector<std::shared_ptr<ThreadWorker>> _workers;
        /** shutdown flag */ 
        bool _shutdown = false;
    };
}