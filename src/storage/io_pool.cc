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
#include <storage/exception.hh>


namespace springtail {

    static void
    worker_fn(IOWorker *worker, IORequestQueue &queue)
    {
        while (true) {
            const IORequest request = queue.pop();
            if (request.type == IORequest::OpType::SHUTDOWN) {
                return;
            }
            worker->process_request(request);
        }
    }

    IOPool::IOPool(int threads)
    {
        for (int i = 0; i < threads; i++) {
            IOWorker *worker = new IOWorker();
            _workers.push_back(worker);
            _threads.push_back(std::thread(worker_fn, worker, std::ref(_queue)));
        }
    }

    IOPool::~IOPool()
    {
        for (int i = 0; i < _threads.size(); i++) {
            IORequest req = {};
            _queue.push(req);
        }

        for (auto &&t : _threads) {
            t.join();
        }

        for (IOWorker *w: _workers) {
            delete w;
        }
    }

    /* static member initialization must happen outside of class */
    IOMgr* IOMgr::_instance {nullptr};
    std::mutex IOMgr::_mutex;

    /**
     * @brief getInstance() of singleton IOMgr; create if it doesn't exist.
     * @return instance of IOMgr
     */
    IOMgr *
    IOMgr::getInstance()
    {
        std::scoped_lock<std::mutex> lock(_mutex);

        if (_instance == nullptr) {
            _instance = new IOMgr(NUM_THREADS, MAX_FILEHANDLES);
        }

        return _instance;
    }


    std::shared_ptr<IOHandle>
    IOMgr::open(const std::filesystem::path &path, int mode, bool compressed)
    {
        if (mode != IO_MODE_READ && mode != IO_MODE_WRITE && mode != IO_MODE_APPEND) {
            throw StorageError();
        }

        // XXX need to figure out how we know a file is compressed
        return std::make_shared<IOHandle>(path, mode, compressed);
    }


    void
    IOMgr::remove(const std::filesystem::path &path)
    {
        // TBD
    }


    bool
    IOMgr::_evict_callback(std::shared_ptr<IOFile> filehandle)
    {
        return true;
    }


    void
    IORequestQueue::push(const IORequest &request)
    {
        std::scoped_lock<std::mutex> queue_lock(_mutex);
        _queue.push(request);
        _cv.notify_one();
    }


    const IORequest &
    IORequestQueue::pop()
    {
        std::unique_lock<std::mutex> queue_lock(_mutex);
        _cv.wait(queue_lock, [&]{ return !_queue.empty();});
        const IORequest &val = _queue.front();
        _queue.pop();
        return val;
    }


    void
    IOWorker::process_request(const IORequest &request)
    {
        switch (request.type) {
            case IORequest::OpType::READ:
                break;
            case IORequest::OpType::APPEND:
                break;
            case IORequest::OpType::WRITE:
                break;
            case IORequest::OpType::SYNC:
                break;
            case IORequest::OpType::SHUTDOWN:
                return;
        }
    }


    void
    IOWorker::read(const IORequest &request)
    {

    }


    void
    IOWorker::write(const IORequest &request)
    {

    }


    void
    IOWorker::append(const IORequest &request)
    {

    }


    void
    IOWorker::sync(const IORequest &request)
    {

    }

}

int main(void)
{
    return 0;
}