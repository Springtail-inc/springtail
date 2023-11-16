#pragma once

#include <string>
#include <memory>
#include <vector>
#include <functional>
#include <filesystem>
#include <variant>
#include <cstdio>
#include <thread>
#include <atomic>
#include <queue>
#include <lz4.h>

#include <storage/compressors.hh>
#include <storage/io_request.hh>
#include <storage/io.hh>

#include <common/object_cache.hh>

namespace springtail {

    // forward references to avoid include loops
    class IOFile;
    class IOSysFH;


    class IOWorker {
    private:
        std::shared_ptr<Compressor> _compressor;
        std::shared_ptr<Decompressor> _decompressor;

        bool _shutdown = false;

        void _issue_request(std::shared_ptr<IORequest> request, 
                            std::shared_ptr<IOSysFH> fh);

    public:
        IOWorker()
            : _compressor(new Lz4Compressor()),
              _decompressor(new Lz4Decompressor())
        { }

        ~IOWorker() { }

        /**
         * @brief Set shutdown flag to true
         */
        void set_shutdown() { _shutdown = true; }

        /**
         * @brief Is shutdown set on this worker
         * 
         * @return true if set
         * @return false if not set
         */
        bool is_shutdown() { return _shutdown; }

        /**
         * @brief Process IO request
         * 
         * @param request IO request to process
         */
        void process_request(std::shared_ptr<IORequest> request);
    };

    class IORequestQueue {
    private:
        std::queue<std::shared_ptr<IORequest>> _queue;
        std::condition_variable _cv;
        std::mutex _mutex;
    public:
        void push(std::shared_ptr<IORequest> request);
        std::shared_ptr<IORequest> pop();
        void signal_all() { _cv.notify_all(); };
    };


    class IOPool {
    private:
        IORequestQueue _queue;
        std::vector<std::thread> _threads;
        std::vector<std::shared_ptr<IOWorker>> _workers;
    public:
        IOPool(int threads);
        ~IOPool();

        std::mutex _resize_mutex;

        /**
         * @brief Resize the worker pool
         * 
         * @param size new size
         */
        void resize(int size);

        inline void queue(std::shared_ptr<IORequest> request) {
            _queue.push(request);
        }
    };

    /**
     * @brief Singleton IOMgr; used to retrieve IOSysFHs
     */
    class IOMgr {
    public:
        enum IO_MODE { READ, APPEND, WRITE };

        static const int NUM_THREADS = 1; // XXX need way to set dynamically
        static const int MAX_FILE_OBJECTS = 32;
        static const int MAX_FILE_HANDLES_PER_FILE=4;

        static IOMgr *get_instance();

        // no create call, first write, after open for write, will do the create
        std::shared_ptr<IOHandle> open(const std::filesystem::path &path, IO_MODE mode, bool compressed);
        std::shared_ptr<IOHandle> open(const char *path, IO_MODE mode, bool compressed);        

        void remove(const std::filesystem::path &path);

        inline void queue_request(std::shared_ptr<IORequest> request) {
            _thread_pool.queue(request);
        }

        std::shared_ptr<IOFile> lookup(const std::filesystem::path &path,
                                       bool compressed);

        /**
         * @brief Shuts down the IOMgr instance by deleting _instance.  It causes the thread pool and LRU cache
         * destructors to run.  The thread pool will cleanly shutdown all worker threads by enqueing a shutdown
         * message and waiting for it to complete in all threads.
         */
        void shutdown();

        /**
         * @brief Grow or shrink the pool
         * @param size new size
         */
        void resize_pool(int size) { _thread_pool.resize(size); }

        /**
         * @brief Resize filehandle cache
         * @param size new size
         */
        void resize_cache(int size) { _file_cache.resize(size); };

    protected:
        static IOMgr *_instance;
        static std::mutex _instance_mutex;

        IOMgr(int num_threads, int max_filehandles)
            : _thread_pool(num_threads),
              _file_cache(max_filehandles, _evict_callback)
        { };

        ~IOMgr(){};

    private:
        IOPool _thread_pool;

        LruObjectCache<std::filesystem::path, IOFile> _file_cache;

        std::mutex _cache_mutex;

        // delete copy constructor
        IOMgr(const IOMgr &)          = delete;
        void operator=(const IOMgr &) = delete;

        static bool _evict_callback(std::shared_ptr<IOFile> filehandle);
    };

}