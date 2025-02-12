#include <pg_fdw/multi-queue-thread-manager.hh>

namespace springtail::pg_fdw {

    void
    MultiQueueThreadManager::_drain_incoming_queue()
    {
        std::unique_lock<std::mutex> lock(_incoming_queue_mutex);
        while (!_incoming_requests.empty()) {
            // take the next request from the front
            MultiQueueRequestPtr next_request = _incoming_requests.front();
            // remove request from the incoming queue
            _incoming_requests.pop();
            lock.unlock();

            // set notification callback for this request
            next_request->set_notify_done([this](uint64_t queue_id) {
                std::unique_lock<std::mutex> lock(_completed_queue_mutex);
                _completed_queue.push(queue_id);
                lock.unlock();
                this->notify_ready();
            });

            // get queue id of the request
            uint64_t queue_id = next_request->get_queue_id();

            // create a new queue for this request if it does not exists yet
            if (!_request_queues.contains(queue_id)) {
                _request_queues.insert(std::make_pair(queue_id, std::queue<MultiQueueRequestPtr>()));
            }

            // queue this request into the appropriate queue
            _request_queues[queue_id].push(next_request);
            lock.lock();
        }
    }

    void
    MultiQueueThreadManager::_process_completed_queue()
    {
        std::unique_lock<std::mutex> lock(_completed_queue_mutex);
        while (!_completed_queue.empty()) {
            // remove queue id from the completed queue
            uint64_t queue_id = _completed_queue.front();
            _completed_queue.pop();
            lock.unlock();

            // when request is scheduled to run by the thread pool, it remains
            // at the top of the its queue till _completed_queue indicates
            // that this request is now done
            // remove request from the appropriate queue
            _request_queues[queue_id].pop();

            lock.lock();
        }
    }

    void
    MultiQueueThreadManager::_schedule_requests(bool &queues_empty)
    {
        queues_empty = true;
        for (auto [queue_id, request_queue]: _request_queues) {
            if (request_queue.empty()) {
                continue;
            }
            queues_empty = false;
            MultiQueueRequestPtr next_request = request_queue.front();

            // if this request is already queued, skip it, otherwise schedule it
            if (!next_request->queued()) {
                next_request->set_queued();
                _thread_pool.queue(next_request);
            }
        }
    }

    void
    MultiQueueThreadManager::_run()
    {
        while (!_shutdown) {
            _work_ready = false;
            // 1. check incoming queue
            _drain_incoming_queue();
            // 2. check completed queues
            _process_completed_queue();
            // 3. find non-queued requests at the top of all queues and queue them
            bool queues_empty = true;
            _schedule_requests(queues_empty);

            // wait to be notified of the change
            _work_ready.wait(false);
        }

        // after shutdown is requested, drain the incoming queue once
        _drain_incoming_queue();

        while (true) {
            _work_ready = false;
            // continue to process completed requests and schedule the new ones
            // until all the queues are empty
            _process_completed_queue();
            bool queues_empty = true;
            _schedule_requests(queues_empty);
            if (queues_empty) {
                break;
            }

            // wait to be notified of the change
            _work_ready.wait(false);
        }
    }


}