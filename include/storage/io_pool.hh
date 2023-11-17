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

#include <common/object_cache.hh>

namespace springtail {

    // forward references to avoid include loops
    class IOFile;
    class IOSysFH;
    class IOHandle;

    /**
     * @brief Worker thread
     */
    class IOWorker {
    private:
        std::shared_ptr<Compressor> _compressor;       ///< compressor object
        std::shared_ptr<Decompressor> _decompressor;   ///< decompressor object

        bool _shutdown = false;   ///< shutdown flag for worker

        /**
         * @brief Worker thread internal issue_request call
         * @param request IORequest to process
         * @param fh      File handle for request
         */
        void _issue_request(std::shared_ptr<IORequest> request, 
                            std::shared_ptr<IOSysFH> fh);

        /**
         * @brief Handle caught exception
         * @param request IORequest ptr
         * @param exc     Exception caught
         */
        void _handle_error(std::shared_ptr<IORequest> request, const std::exception &exc);

    public:
        /**
         * @brief Construct a new IOWorker object
         */
        IOWorker()
            : _compressor(new Lz4Compressor()),
              _decompressor(new Lz4Decompressor())
        { }

        /**
         * @brief Destroy the IOWorker object
         */
        ~IOWorker() { }

        /**
         * @brief Set shutdown flag to true
         */
        void set_shutdown() { _shutdown = true; }

        /**
         * @brief Is shutdown set on this worker
         * @return true if set
         * @return false if not set
         */
        bool is_shutdown() { return _shutdown; }

        /**
         * @brief Process IO request
         * @param request IO request to process
         */
        void process_request(std::shared_ptr<IORequest> request);
    };

    /**
     * @brief Request queue; worker blocks on queue pop; IOMgr enqueues IORequest
     */
    class IORequestQueue {
    private:
        std::queue<std::shared_ptr<IORequest>> _queue;  ///< underlying request queue
        std::condition_variable _cv;                    ///< condition variable workers block on
        std::mutex _mutex;                              ///< mutex protecting condition variable & queue
    public:
        /**
         * @brief Push an IO request onto the worker pool queue
         * @param request IO request to enqueue
         */
        void push(std::shared_ptr<IORequest> request);

        /**
         * @brief Pop an IO request off the worker pool queue; or block if empty
         * @return std::shared_ptr<IORequest> ptr to the IORequest
         */        
        std::shared_ptr<IORequest> pop();

        /**
         * @brief Do a notify_all on queue condition variable; wake all blocked workers
         */
        void signal_all() { _cv.notify_all(); };
    };

    /**
     * @brief IO Pool; pool of workers and threads; contains request queue
     */
    class IOPool {
    private:
        IORequestQueue _queue;               ///< Request queue
        std::vector<std::thread> _threads;   ///< List of worker threads
        std::vector<std::shared_ptr<IOWorker>> _workers;  ///< List of workers
    public:

        /**
         * @brief Construct a new IOPool::IOPool object; initialize the workers
         * @param threads Number of initial workers
         */
        IOPool(int threads);

        /**
         * @brief Destroy the IOPool::IOPool object; queue a SHUTDOWN IO request for each worker.
         */
        ~IOPool();

        std::mutex _resize_mutex;  // mutex to protect resizing worker pool

        /**
         * @brief Grow or shrink the pool; will block if downsizing until
         * threads are done
         */
        void resize(int size);

        /**
         * @brief Queue IO request, notifies worker threads blocked on queue
         * @param request IORequest to queue
         */
        inline void queue(std::shared_ptr<IORequest> request) {
            _queue.push(request);
        }
    };

    /**
     * @brief Singleton IOMgr; used to retrieve IOSysFHs
     */
    class IOMgr {
    public:
        /** IO Mode for opening a file; APPEND appends to end of file; WRITE allows overwrite */
        enum IO_MODE { READ, APPEND, WRITE };

        static const int NUM_THREADS = 1;             ///< initial thread count, use resize to change
        static const int MAX_FILE_OBJECTS = 32;       ///< initial file object in file cache, use resize to change
        static const int MAX_FILE_HANDLES_PER_FILE=4; ///< number of read file handles per file object

        /**
         * @brief getInstance() of singleton IOMgr; create if it doesn't exist.
         * @return instance of IOMgr
         */
        static IOMgr *get_instance();

        /**
         * @brief Open a file, retrieve virtual FH from IOMgr singleton instance
         *        No create call, first write/append after open will do create if not exists
         * @param path        Path of file to open
         * @param mode        Mode of file (read, write, append)
         * @param compressed  Is this a compressed file (boolean)
         * @return std::shared_ptr<IOHandle> Ptr to IOHandle representing file
         */        
        std::shared_ptr<IOHandle> open(const std::filesystem::path &path, IO_MODE mode, bool compressed);

        /**
         * @brief Open a file, retrieve virtual FH from IOMgr singleton instance
         *        No create call, first write/append after open will do create if not exists
         * @param path        Path of file to open
         * @param mode        Mode of file (read, write, append)
         * @param compressed  Is this a compressed file (boolean)
         * @return std::shared_ptr<IOHandle> Ptr to IOHandle representing file
         */  
        std::shared_ptr<IOHandle> open(const char *path, IO_MODE mode, bool compressed);        

        /**
         * @brief Remove file at path; 
         * NOTE: currently no checking for ongoing IO or locking
         * @param path Path to remove
         */
        void remove(const std::filesystem::path &path);

        /**
         * @brief Queue IO request, notify thread blocked on queue
         * @param request IO request to queue
         */
        inline void queue_request(std::shared_ptr<IORequest> request) {
            _thread_pool.queue(request);
        }

        /**
         * @brief Lookup file object in LRU cache based on path name
         * @details Lookup file object in LRU cache;
         *          if not found a new file object is created and added to the cache.
         *          May trigger eviction of another object.
         * @param path ID for LRU cache lookup
         * @param is_compressed is file compressed, used in new file object creation
         *
         * @return file object ptr
         */
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
        static IOMgr *_instance;             ///< static instance (singleton)
        static std::mutex _instance_mutex;   ///< protects lookup/creation of singleton _instance

        /**
         * @brief Construct a new IOMgr object
         * @param num_threads     Initial number of threads for thread pool (NUM_THREADS)
         * @param max_filehandles Initial size of file handle cache (MAX_FILE_OBJECTS)
         */
        IOMgr(int num_threads, int max_filehandles)
            : _thread_pool(num_threads),
              _file_cache(max_filehandles, _evict_callback)
        { };

        /**
         * @brief Destroy the IOMgr object
         */
        ~IOMgr(){};

    private:
        IOPool _thread_pool;  ///< worker thread pool

        LruObjectCache<std::filesystem::path, IOFile> _file_cache; ///< file object cache

        std::mutex _cache_mutex;  // mutex to protect file object cache lookups/inserts

        // delete copy constructor
        IOMgr(const IOMgr &)          = delete;
        void operator=(const IOMgr &) = delete;

        /**
         * @brief Eviction callback for file object cache; must not block
         * @param filehandle File handle to evict
         * @return true File handle was successfully evicted
         * @return false File handle could not be evicted (busy with IO)
         */
        static bool _evict_callback(std::shared_ptr<IOFile> filehandle);
    };

}