#pragma once

#include <map>

#include <common/thread_pool.hh>

namespace springtail::common {
    /**
     * @brief Multiple Queue Request object definition. It contains the id of the queue that
     *      it belongs in.
     *
     */
    class MultiQueueRequest {
    public:
        /**
         * @brief Construct a new Multi Queue Request object
         *
         * @param queue_id - id of the queue
         * @param run_cb - callback to be executed by the thread worker
         */
        MultiQueueRequest(uint64_t queue_id, std::function<void ()> run_cb) :
            _queue_id(queue_id), _run_cb(run_cb) {}

        /**
         * @brief Destroy the Multi Queue Request object; default destructor
         *
         */
        ~MultiQueueRequest() = default;

        /**
         * @brief Get the queue id of the request object
         *
         * @return uint64_t
         */
        uint64_t get_queue_id() {
            return _queue_id;
        }

        /**
         * @brief Return queued status of the request
         *
         * @return true
         * @return false
         */
        bool queued() {
            return _queued;
        }

        /**
         * @brief Set the queued status of the request to true
         *
         */
        void set_queued() {
            _queued = true;
        }

        /**
         * @brief Set the notify callback for this request. This is set by the thread manager
         *      to wake its up when some work is done.
         *
         * @param notify_done_cb
         */
        void set_notify_done(std::function<void (uint64_t)> notify_done_cb) {
            _notify_done_cb = notify_done_cb;
        }

        /**
         * @brief This operator is called by the thread worker to execute the request.
         *
         */
        void operator()() {
            _run_cb();
            _notify_done_cb(_queue_id);
        }
    private:
        uint64_t _queue_id;                                 ///> queue id
        bool _queued{false};                                ///> queued flag
        std::function<void (uint64_t)> _notify_done_cb;     ///> notify task completion callback
        std::function<void ()> _run_cb;                     ///> work completion callback
    };

    using MultiQueueRequestPtr = std::shared_ptr<MultiQueueRequest>;

    /**
     * @brief This multi-queue thread manager allows the requests that belong in the same queue
     *      to be processed sequentially in the order they were received, while not interfering
     *      with the requests that belong in other queues and allowing them to be processed
     *      concurrently.
     *
     *      This thread manager has an incoming requests queue. From the incoming requests queue
     *      it distributes all request among the appropriate internal queues identified by the queue id
     *      in the request.
     *
     *      From each internal queue, only the top request will be given to the thread pool for processing.
     *      Upon request processing, each request will add its queue id to the completed queue, which
     *      will signal to the thread manager to pop the front element from the appropriate internal queue
     *      and schedule the next one if present.
     *
     */
    class MultiQueueThreadManager {
    public:
        /**
         * @brief Multi Queue Thread Manager constructor with the number of thread pool threads.
         *
         * @param max_threads - number of threads to use for thread pool
         */
        explicit MultiQueueThreadManager(int max_threads) : _thread_pool(max_threads) {}

        /**
         * @brief Multi Queue Thread Manager object destructor.
         *
         */
        ~MultiQueueThreadManager()
        {
            if (!_shutdown_done) {
                shutdown();
            }
        }

        /**
         * @brief Notify thread manager that some work is ready.
         *
         */
        void
        notify_ready()
        {
            _work_ready = true;
            _work_ready.notify_one();
        }

        /**
         * @brief Notify thread manager about shutdown.
         *
         */
        void
        notify_shutdown()
        {
            _shutdown = true;
            notify_ready();
        }

        /**
         * @brief Shutdown thread manager
         *
         */
        void
        shutdown()
        {
            _shutdown = true;
            _manager_thread.join();
            _thread_pool.shutdown();
            _shutdown_done = true;
        }

        /**
         * @brief Start thread manager.
         *
         */
        void
        start()
        {
            _manager_thread = std::thread(&MultiQueueThreadManager::_run, this);
        }

        /**
         * @brief Queue request for the thread manager.
         *
         * @param request - incoming request object
         */
        void
        queue_request(MultiQueueRequestPtr request)
        {
            // add request to the incoming queue
            std::unique_lock<std::mutex> lock(_incoming_queue_mutex);
            _incoming_requests.push(request);
            lock.unlock();
            notify_ready();
        }
    private:
        std::thread _manager_thread;                ///< thread manager thread
        ThreadPool<MultiQueueRequest> _thread_pool; ///< thread pool that processes requests
        std::map<uint64_t, std::queue<MultiQueueRequestPtr>> _request_queues;   ///> map of request queues
        std::queue<uint64_t> _completed_queue;      ///< queue of queue ids of the completed requests
        std::queue<MultiQueueRequestPtr> _incoming_requests;    ///> queue of incoming requests
        std::mutex _completed_queue_mutex;          ///< mutex for completed queue
        std::mutex _incoming_queue_mutex;           ///< mutex for incoming requests queue
        std::atomic<bool> _work_ready{false};     ///< atomic work ready flag
        std::atomic<bool> _shutdown{false};       ///< atomic shutdown flag
        std::atomic<bool> _shutdown_done{false};  ///< atomic shutdown done flag

        /**
         * @brief This function takes all the objects in the incoming requests queue and distributes them
         *      among the appropriate internal queues based on the queue id specified in the request.
         *
         */
        void _drain_incoming_queue();

        /**
         * @brief This function processes completed queue.
         *
         */
        void _process_completed_queue();

        /**
         * @brief Schedule requests that have not been scheduled yet and
         *        return true if at least one request was still present in any of the queues
         *        and false if it found all the queues empty.
         *
         * @return bool - true if scheduled requests, false if all the queues were empty
         */
        bool _schedule_requests();

        /**
         * @brief Main run function that is executed by the manager thread.
         *
         */
        void _run();
    };
};
