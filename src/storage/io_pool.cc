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
#include <storage/io_file.hh>
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
    std::mutex IOMgr::_instance_mutex;

    /**
     * @brief getInstance() of singleton IOMgr; create if it doesn't exist.
     * @return instance of IOMgr
     */
    IOMgr *
    IOMgr::getInstance()
    {
        std::scoped_lock<std::mutex> lock(_instance_mutex);

        if (_instance == nullptr) {
            _instance = new IOMgr(NUM_THREADS, MAX_FILE_OBJECTS);
        }

        return _instance;
    }


    std::shared_ptr<IOHandle>
    IOMgr::open(const std::filesystem::path &path, IO_MODE &mode, bool compressed)
    {
        if (mode != IO_MODE::READ && mode != IO_MODE::WRITE && mode != IO_MODE::APPEND) {
            throw StorageError();
        }

        // XXX need to figure out how we know a file is compressed
        return std::make_shared<IOHandle>(path, mode, compressed);
    }


    /**
     * @brief Lookup file object in LRU cache based on path name
     * @details Lookup file object in LRU cache;
     *          if not found a new file object is created and added to the cache.
     *          May trigger eviction of another object.
     *
     * @param path ID for LRU cache lookup
     * @param is_compressed is file compressed, used in new file object creation
     *
     * @return file object ptr
     */
    std::shared_ptr<IOFile>
    IOMgr::lookup(const std::filesystem::path &path,
                  bool is_compressed)
    {
        // lock cache
        std::scoped_lock<std::mutex> lock(_cache_mutex);

        std::shared_ptr<IOFile> file = _file_cache.get(path);
        if (file == nullptr) {
            // allocate a file object and insert into cache
            // while mutex is held
            file = std::make_shared<IOFile>(path, is_compressed);

            // this may trigger an eviction and eviction callback
            _file_cache.insert(path, file);
        }

        // mark file object as in use
        file->incr_in_use();

        return file;
    }


    void
    IOMgr::remove(const std::filesystem::path &path)
    {
        // TBD
    }


    /**
     * @brief Callback from LRU cache, called in the context of a get/lookup with
     *        _cache_mutex locked.  Callback should be non-blocking
     *
     * @param file object for eviction
     * @return true if evictable, false otherwise
     */
    bool
    IOMgr::_evict_callback(std::shared_ptr<IOFile> file)
    {
        // if file object in use fail
        if (file->in_use()) {
            return false;
        }

        // try to close all open file handles
        if (file->try_close_all()) {
            return true;
        }

        return false;
    }


    void
    IORequestQueue::push(const IORequest &request)
    {
        // lock queue lock
        std::scoped_lock<std::mutex> queue_lock(_mutex);

        // push request onto queue and notify a single thread
        _queue.push(request);
        _cv.notify_one();
    }


    const IORequest &
    IORequestQueue::pop()
    {
        // lock queue lock
        std::unique_lock<std::mutex> queue_lock(_mutex);
        // block until queue is not empty
        _cv.wait(queue_lock, [&]{ return !_queue.empty();});

        // extract first element from queue and return it
        const IORequest &val = _queue.front();
        _queue.pop();

        return val;
    }


    void
    IOWorker::_issue_request(const IORequest &request, std::shared_ptr<IOSysFH> fh)
    {
        // handle request
        switch (request.type) {
            case IORequest::OpType::READ: {
                IORequestRead req = std::get<IORequestRead>(request.args);
                fh->read(req.offset, _decompressor, req.callback);
                break;
            }

            case IORequest::OpType::APPEND: {
                IORequestAppend req = std::get<IORequestAppend>(request.args);
                if (std::holds_alternative<std::shared_ptr<std::vector<char>>>(req.data)) {
                    fh->append(std::get<std::shared_ptr<std::vector<char>>>(req.data), _compressor, req.callback);
                } else {
                    fh->append(std::get<std::vector<std::shared_ptr<std::vector<char>>>>(req.data), _compressor, req.callback);
                }
                break;
            }

            case IORequest::OpType::WRITE: {
                IORequestWrite req = std::get<IORequestWrite>(request.args);
                if (std::holds_alternative<std::shared_ptr<std::vector<char>>>(req.data)) {
                    fh->write(req.offset, std::get<std::shared_ptr<std::vector<char>>>(req.data), req.callback);
                } else {
                    fh->write(req.offset, std::get<std::vector<std::shared_ptr<std::vector<char>>>>(req.data), req.callback);
                }
                break;
            }

            case IORequest::OpType::SYNC: {
                IORequestSync req = std::get<IORequestSync>(request.args);
                fh->sync(req.callback);
                break;
            }

            default:
                break;
        }
    }


    void
    IOWorker::process_request(const IORequest &request)
    {
        // on shutdown return immediately
        if (IORequest::OpType::SHUTDOWN == request.type) {
            return;
        }

        // get IOFile
        IOMgr *mgr = IOMgr::getInstance();

        // lookup path for file object; creates new one if not present
        // marks file object as in use
        std::shared_ptr<IOFile> io_file = mgr->lookup(request.path, request.compressed);

        // get a free handle based on IO mode; may block
        // marks file handle as in use
        std::shared_ptr<IOSysFH> fh = io_file->get_fh((IORequest::OpType::READ == request.type) ?
                                                          IOMgr::IO_MODE::READ : IOMgr::IO_MODE::WRITE);

        try {
            _issue_request(request, fh);
        } catch (...) {
            // log exception
        }

        // work is complete, release fh
        io_file->put_fh(fh);

        // release file object
        io_file->decr_in_use();

    }
}


int main(void)
{
    return 0;
}