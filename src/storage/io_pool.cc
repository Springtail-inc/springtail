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

    /**
     * @brief Construct a new IOPool::IOPool object; initialize the workers
     * @param threads Number of initial workers
     */
    IOPool::IOPool(int threads)
    {
        resize(threads);
    }

    /**
     * @brief Destroy the IOPool::IOPool object; queue a SHUTDOWN IO request for each worker.
     */
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

    /**
     * @brief Grow or shrink the pool; will block if downsizing until
     * threads are done
     */
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

    /**
     * @brief getInstance() of singleton IOMgr; create if it doesn't exist.
     * @return instance of IOMgr
     */
    IOMgr *
    IOMgr::get_instance()
    {
        std::scoped_lock<std::mutex> lock(_instance_mutex);

        if (_instance == nullptr) {
            _instance = new IOMgr(NUM_THREADS, MAX_FILE_OBJECTS);
        }

        return _instance;
    }

    /**
     * @brief Shutdown IOMgr instance.  Delete internal instance.
     *        Shuts down worker pool and FH cache (by calling their destructors).
     *        Worker pool will drain before shutting down each thread.
     */
    void
    IOMgr::shutdown()
    {
        std::scoped_lock<std::mutex> lock(_instance_mutex);

        if (_instance != nullptr) {
            delete _instance;
            _instance = nullptr;
        }
    }

     /**
     * @brief Open a file, retrieve virtual FH from IOMgr singleton instance
     * 
     * @param path        Path of file to open
     * @param mode        Mode of file (read, write, append)
     * @param compressed  Is this a compressed file (boolean)
     * @return std::shared_ptr<IOHandle> Ptr to IOHandle representing file
     */
     std::shared_ptr<IOHandle> 
     IOMgr::open(const char *path, IO_MODE mode, bool compressed)
     {
        std::filesystem::path fspath(path);
        return open(fspath, mode, compressed);
     }


    /**
     * @brief Open a file, retrieve virtual FH from IOMgr singleton instance
     * 
     * @param path        Path of file to open
     * @param mode        Mode of file (read, write, append)
     * @param compressed  Is this a compressed file (boolean)
     * @return std::shared_ptr<IOHandle> Ptr to IOHandle representing file
     */
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


    /**
     * @brief Remove file at path; 
     * NOTE: currently no checking for ongoing IO or locking
     * @param path Path to remove
     */
    void
    IOMgr::remove(const std::filesystem::path &path)
    {
        std::filesystem::remove(path);
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

    /**
     * @brief Push an IO request onto the worker pool queue
     * @param request IO request to enqueue
     */
    void
    IORequestQueue::push(std::shared_ptr<IORequest> request)
    {
        // lock queue lock
        std::scoped_lock<std::mutex> queue_lock(_mutex);

        // push request onto queue and notify a single thread
        _queue.push(request);
        _cv.notify_one();
    }

    /**
     * @brief Pop an IO request off the worker pool queue; or block if empty
     * 
     * @return std::shared_ptr<IORequest> ptr to the IORequest
     */
    std::shared_ptr<IORequest>
    IORequestQueue::pop()
    {
        // lock queue lock
        std::unique_lock<std::mutex> queue_lock(_mutex);
        
        // block until queue is not empty
        // or until queue is signalled to wake all
        _cv.wait(queue_lock);

        // extract first element from queue and return it
        if (_queue.empty()) {
            return nullptr;
        }

        std::shared_ptr<IORequest> val = _queue.front();
        _queue.pop();

        return val;
    }

    /**
     * @brief Worker thread internal issue_request call
     * 
     * @param request IORequest to process
     * @param fh      File handle for request
     */
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
                std::cerr << "IOWorker::_issue_request unknown request type: " << request->type << std::endl;
                break;
        }
    }

    /**
     * @brief Process the IO request.  Lookup file in cache, get FH, issue the request
     * 
     * @param request IO request to process
     */
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
        //std::cout << "IOWorker::process_request lookup path: " << request->path << std::endl;

        std::shared_ptr<IOFile> io_file = mgr->lookup(request->path, request->compressed);

        // get a free handle based on IO mode; may block
        // marks file handle as in use
        //std::cout << "IOWorker::process_request get fh\n";

        std::shared_ptr<IOSysFH> fh = io_file->get_fh((IORequest::IOType::READ == request->type) ?
                                                           IOMgr::IO_MODE::READ : 
                                                           IOMgr::IO_MODE::WRITE);

        //std::cout << "IOWorker::process_request got fh\n";

        try {
            _issue_request(request, fh);
        } catch (...) {
            // log exception
        }

        // work is complete, release fh
        io_file->put_fh(fh);

        // release file object
        io_file->decr_in_use();
        
        return;
    }
}