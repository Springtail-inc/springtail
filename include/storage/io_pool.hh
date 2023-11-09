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

        void _issue_request(const IORequest &request, std::shared_ptr<IOSysFH> fh);

    public:
        IOWorker()
            : _compressor(new Lz4Compressor()),
              _decompressor(new Lz4Decompressor())
        { }

        ~IOWorker() { }

        void process_request(const IORequest &request);
    };

    class IORequestQueue {
    private:
        std::queue<IORequest> _queue;
        std::condition_variable _cv;
        std::mutex _mutex;
    public:
        void push(const IORequest &request);
        const IORequest &pop();
    };


    class IOPool {
    private:
        IORequestQueue _queue;
        std::vector<std::thread> _threads;
        std::vector<IOWorker *> _workers;
    public:
        IOPool(int threads);
        ~IOPool();

        inline void queue(const IORequest &request) {
            _queue.push(request);
        }
    };

    /**
     * @brief Singleton IOMgr; used to retrieve IOSysFHs
     */
    class IOMgr {
    public:
        enum IO_MODE { READ, APPEND, WRITE };

        static const int NUM_THREADS = 16; // XXX need way to set dynamically
        static const int MAX_FILE_OBJECTS = 32;
        static const int MAX_FILE_HANDLES_PER_FILE=4;

        static IOMgr *getInstance();

        // no create call, first write, after open for write, will do the create
        std::shared_ptr<IOHandle> open(const std::filesystem::path &path, IO_MODE &mode, bool compressed);

        void remove(const std::filesystem::path &path);

        inline void queue_request(const IORequest &request) {
            _thread_pool.queue(request);
        }

        std::shared_ptr<IOFile> lookup(const std::filesystem::path &path,
                                       bool compressed);

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