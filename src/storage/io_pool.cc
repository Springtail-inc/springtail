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

#include <fmt/core.h>

#include <storage/compressors.hh>
#include <storage/io_pool.hh>
#include <storage/io_file.hh>
#include <storage/io.hh>
#include <storage/exception.hh>

#include <common/logging.hh>


namespace springtail {

    /**
     * @brief Worker thread main loop; wait on queue, process request
     * @param worker Worker pointer
     * @param queue IOQueue reference
     */
    static void
    worker_fn(std::shared_ptr<IOWorker> worker, IORequestQueue &queue)
    {
        while (true) {
            // before blocking check if worker should shutdown
            if (worker->is_shutdown()) {
                return;
            }

            std::shared_ptr<IORequest> request = queue.pop();
            if (request == nullptr) {
                // just a wakeup so we can check if we should shutdown
                continue;
            }

            // shutdown message, shutdown
            if (request->type == IORequest::IOType::SHUTDOWN) {
                return;
            }

            // process request
            worker->process_request(request);
        }
    }


    IOPool::IOPool(int threads)
    {
        resize(threads);
    }


    IOPool::~IOPool()
    {
        for (int i = 0; i < _threads.size(); i++) {
            std::shared_ptr<IORequest> req = std::make_shared<IORequest>();
            _queue.push(req);
        }

        for (auto &&t : _threads) {
            t.join();
        }
    }


    void
    IOPool::resize(int size)
    {
        std::scoped_lock<std::mutex> lock(_resize_mutex);

        int old_size = _threads.size();
        
        if (old_size == size) {
            return;
        }

        if (old_size > size) {
            // shutdown some workers
            std::vector<std::thread> shutdown_threads;

            // pop workers off pool vectors
            std::move(_threads.begin(), _threads.begin() + old_size-size,
                      std::back_inserter(shutdown_threads));

            for (int i = 0; i < old_size - size; i++) {
                std::shared_ptr<IOWorker> w = _workers.back();
                _workers.pop_back();

                w->set_shutdown(); // set shutdown flag
            }

            // send an interrupt message to all workers
            // can't interrupt just a single targetted worker
            _queue.signal_all();

            // wait for specific threads to exit
            for (auto &&t: shutdown_threads) {
                t.join();
            }

            return;
        }

        if (size > old_size) {
            // startup some workers
            for (int i = old_size; i < size; i++) {
                std::shared_ptr<IOWorker> worker = std::make_shared<IOWorker>();
                _workers.push_back(worker);
                _threads.push_back(std::thread(worker_fn, worker, std::ref(_queue)));
            }
            
            return;
        }
    }

    /* static member initialization must happen outside of class */
    IOMgr* IOMgr::_instance {nullptr};
    std::mutex IOMgr::_instance_mutex;


    IOMgr *
    IOMgr::get_instance()
    {
        std::scoped_lock<std::mutex> lock(_instance_mutex);

        if (_instance == nullptr) {
            _instance = new IOMgr(NUM_THREADS, MAX_FILE_OBJECTS);
        }

        return _instance;
    }


    void
    IOMgr::shutdown()
    {
        std::scoped_lock<std::mutex> lock(_instance_mutex);

        if (_instance != nullptr) {
            delete _instance;
            _instance = nullptr;
        }
    }


     std::shared_ptr<IOHandle> 
     IOMgr::open(const char *path, IO_MODE mode, bool compressed)
     {
        std::filesystem::path fspath(path);
        return open(fspath, mode, compressed);
     }


    std::shared_ptr<IOHandle>
    IOMgr::open(const std::filesystem::path &path, IO_MODE mode, bool compressed)
    {
        if (mode != IO_MODE::READ && mode != IO_MODE::WRITE && mode != IO_MODE::APPEND) {
            throw StorageError();
        }

        // XXX need to figure out how we know a file is compressable
        // right now it is based on caller passing flag in.
        return std::make_shared<IOHandle>(path, mode, compressed);
    }



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
        std::filesystem::remove(path);
    }


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
    IORequestQueue::push(std::shared_ptr<IORequest> request)
    {
        // lock queue lock
        std::scoped_lock<std::mutex> queue_lock(_mutex);

        // push request onto queue and notify a single thread
        _queue.push(request);
        _cv.notify_one();
    }


    std::shared_ptr<IORequest>
    IORequestQueue::pop()
    {
        // lock queue lock
        std::unique_lock<std::mutex> queue_lock(_mutex);
        
        // block until queue is not empty
        // or until queue is signalled to wake all
        if (_queue.empty()) {
            _cv.wait(queue_lock);
        }

        // extract first element from queue and return it
        if (_queue.empty()) {
            return nullptr;
        }

        std::shared_ptr<IORequest> val = _queue.front();
        _queue.pop();

        return val;
    }


    void
    IOWorker::_issue_request(std::shared_ptr<IORequest> request,
                             std::shared_ptr<IOSysFH> fh)
    {
        //std::cout << "IOWorker: _issue_request: type=" << request->type << std::endl;;

        // handle request
        switch (request->type) {
            case IORequest::IOType::READ: {
                std::shared_ptr<IORequestRead> req = std::dynamic_pointer_cast<IORequestRead>(request);
                fh->read(req, _decompressor);
                break;
            }

            case IORequest::IOType::APPEND: {
                std::shared_ptr<IORequestAppend> req = std::dynamic_pointer_cast<IORequestAppend>(request);
                fh->append(req, _compressor);
                break;
            }

            case IORequest::IOType::WRITE: {
                std::shared_ptr<IORequestWrite> req = std::dynamic_pointer_cast<IORequestWrite>(request);
                fh->write(req);
                break;
            }

            case IORequest::IOType::SYNC: {
                std::shared_ptr<IORequestSync> req = std::dynamic_pointer_cast<IORequestSync>(request);
                fh->sync(req);
                break;
            }

            case IORequest::IOType::SHUTDOWN:
                break;

            default:
                // log error
                SPDLOG_ERROR("IOWorker::_issue_request unknown request type: {}", request->get_type());
                break;
        }
    }


    void
    IOWorker::process_request(std::shared_ptr<IORequest> request)
    {
        // on shutdown return immediately
        if (IORequest::IOType::SHUTDOWN == request->type) {
            return;
        }

        // get IOFile
        IOMgr *mgr = IOMgr::get_instance();

        // lookup path for file object; creates new one if not present
        // marks file object as in use
        SPDLOG_INFO("IOWorker got request for path: {}", request->path.c_str());

        std::shared_ptr<IOFile> io_file = mgr->lookup(request->path, request->compressed);

        // get a free handle based on IO mode; may block
        // marks file handle as in use
        std::shared_ptr<IOSysFH> fh = io_file->get_fh((IORequest::IOType::READ == request->type) ?
                                                           IOMgr::IO_MODE::READ : 
                                                           IOMgr::IO_MODE::WRITE);
        try {
            _issue_request(request, fh);
        } catch (const std::exception &exc) {
            // log exception
            SPDLOG_ERROR("Caught exception for IO type={}", request->get_type());
            SPDLOG_ERROR("Exception: {}", exc.what());
            
            _handle_error(request, exc);
        }

        // work is complete, release fh
        io_file->put_fh(fh);

        // release file object
        io_file->decr_in_use();
        
        return;
    }


    void
    IOWorker::_handle_error(std::shared_ptr<IORequest> request, const std::exception &exc)
    {
         switch (request->type) {
            case IORequest::IOType::READ: {
                std::shared_ptr<IORequestRead> req = std::dynamic_pointer_cast<IORequestRead>(request);
                std::shared_ptr<IOResponseRead> res = std::make_shared<IOResponseRead>(req, IOStatus::ERROR);
                req->complete(res);
                break;
            }

            case IORequest::IOType::APPEND: {
                std::shared_ptr<IORequestAppend> req = std::dynamic_pointer_cast<IORequestAppend>(request);
                std::shared_ptr<IOResponseAppend> res = std::make_shared<IOResponseAppend>(req, IOStatus::ERROR);
                req->complete(res);
                break;
            }

            case IORequest::IOType::WRITE: {
                std::shared_ptr<IORequestWrite> req = std::dynamic_pointer_cast<IORequestWrite>(request);
                std::shared_ptr<IOResponseWrite> res = std::make_shared<IOResponseWrite>(req, IOStatus::ERROR);
                req->complete(res);
                break;
            }

            case IORequest::IOType::SYNC: {
                std::shared_ptr<IORequestSync> req = std::dynamic_pointer_cast<IORequestSync>(request);
                std::shared_ptr<IOResponse> res = std::make_shared<IOResponse>(req, IOStatus::ERROR);
                req->complete(res);
                break;
            }

            default:
                break;
        }
    }
}