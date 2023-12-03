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

#include <common/thread_pool.hh>
#include <common/object_cache.hh>
#include <storage/compressors.hh>
#include <storage/io_request.hh>

namespace springtail {

    // forward references to avoid include loops
    class IOFile;
    class IOSysFH;
    class IOHandle;

    /**
     * @brief Singleton IOMgr; used to retrieve IOSysFHs
     */
    class IOMgr {
    public:
        /** IO Mode for opening a file; APPEND appends to end of file; WRITE allows overwrite */
        enum IO_MODE { READ, APPEND, WRITE };

        static const int NUM_THREADS = 1;             ///< initial thread count
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
         * @brief Resize filehandle cache
         * @param size new size
         */
        void resize_cache(int size) { _file_cache.resize(size); };

        //

        /**
         * @brief Put compressor object back in pool
         * @param compressor
         */
         void put_compressor(std::shared_ptr<Compressor> compressor);

        /**
         * @brief Get the decompressor object from pool
         * @return std::shared_ptr<Decompressor>
         */
        std::shared_ptr<Decompressor> get_decompressor();

        /**
         * @brief Get the compressor object from pool
         * @return std::shared_ptr<Compressor>
         */
        std::shared_ptr<Compressor> get_compressor();

        /**
         * @brief Put decompressor object back in pool
         * @param decompressor
         */
        void put_decompressor(std::shared_ptr<Decompressor> decompressor);

    protected:

        static IOMgr *_instance;             ///< static instance (singleton)
        static std::mutex _instance_mutex;   ///< protects lookup/creation of singleton _instance

        /**
         * @brief Construct a new IOMgr object
         * @param num_threads     Initial number of threads for thread pool (NUM_THREADS)
         * @param max_filehandles Initial size of file handle cache (MAX_FILE_OBJECTS)
         */
        IOMgr(int num_threads, int max_filehandles);

        /**
         * @brief Destroy the IOMgr object
         */
        ~IOMgr(){};

    private:
        ThreadPool<IORequest> _thread_pool;  ///< worker thread pool

        LruObjectCache<std::filesystem::path, IOFile> _file_cache; ///< file object cache

        std::mutex _cache_mutex;  ///< mutex to protect file object cache lookups/inserts

        std::mutex _compressor_mutex; ///< mutex for compressor pool

        std::mutex _decompressor_mutex; ///< mutex for decompressor pool

        // delete copy constructor
        IOMgr(const IOMgr &)          = delete;
        void operator=(const IOMgr &) = delete;

        /** pool of compressor objects -- size matches thread pool size */
        std::queue<std::shared_ptr<Compressor>> _compressors;

        /** pool of decompressor objects -- size matches thread pool size */
        std::queue<std::shared_ptr<Decompressor>> _decompressors;

        /**
         * @brief Eviction callback for file object cache; must not block
         * @param filehandle File handle to evict
         * @return true File handle was successfully evicted
         * @return false File handle could not be evicted (busy with IO)
         */
        static bool _evict_callback(std::shared_ptr<IOFile> filehandle);
    };
}