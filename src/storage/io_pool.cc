#include <string>
#include <memory>
#include <vector>
#include <functional>
#include <filesystem>
#include <variant>
#include <cstdio>
#include <thread>
#include <mutex>
#include <condition_variable>

#include <storage/compressors.hh>
#include <storage/io_pool.hh>


namespace springtail {
    void
    IORequestQueue::push(const IORequest &request)
    {
        std::lock_guard<std::mutex> queue_lock {_mutex};
        _queue.push(request);
        _cv.notify_one();
    }

    IORequest IORequestQueue::pop()
    {
        std::unique_lock<std::mutex> queue_lock {_mutex};
        _cv.wait(queue_lock, [&]{ return !_queue.empty();});
        IORequestQueue val = _queue.front();
        _queue.pop();
        return val;
    }

    void
    worker_fn(IOWorker &worker, IORequestQueue &queue)
    {
        while (true) {
            const IORequest req = queue.pop();
            if (req.type == IORequest::OpType::SHUTDOWN) {
                return;
            }
        }
    }

    IOPool::IOPool(int threads)
    {
        for (int i = 0; i < threads; i++) {
            IOWorker worker();
            _workers.push_back(worker);
            _threads.push_back(std::thread(worker_fn, worker, _queue));
        }
    }

    ~IOPool::IOPool()
    {
        for (int i = 0; i < threads; i++) {
            _queue.push({{IORequest::OpType::SHUTDOWN}, {}, {}});
        }

        for (auto &&t : _threads) {
            t.join();
        }
    }

    void
    IOPool::queue(const IORequest &request)
    {

    }
}