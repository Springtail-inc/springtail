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


    struct IOSysFH {
        std::filesystem::path _path;
        std::FILE *_file;
        bool _is_dirty;
        bool _is_busy;
        bool _is_readonly;
    };

    class IOFile {
    public:
        IOFile(const std::filesystem::path &path)
            : _path(path), _in_use(false) {}

        inline bool in_use() { return _in_use; }

    private:
        std::filesystem::path _path;
        std::atomic<bool> _in_use;
        std::mutex _mutex;
        std::vector<IOSysFH> _in_use_fhs;
        std::vector<IOSysFH> _free_fhs;
    };


    class IOWorker {
    private:
        std::unique_ptr<Compressor> _compressor;
        std::unique_ptr<Decompressor> _decompressor;

        void read(const IORequest &request);
        void write(const IORequest &request);
        void append(const IORequest &request);
        void sync(const IORequest &request);

    public:
        IOWorker()
            : _compressor(new Lz4Compressor()),
              _decompressor(new Lz4Decompressor())
        { }

        ~IOWorker() { }

        void process_request(const IORequest &request);
    };

    class IORequestQueue {
    public:
        void push(const IORequest &request);
        const IORequest &pop();

    private:
        std::queue<IORequest> _queue;
        std::condition_variable _cv;
        std::mutex _mutex;
    };


    class IOPool {
    public:
        IOPool(int threads);
        ~IOPool();

        inline void queue(const IORequest &request) {
            _queue.push(request);
        }

    private:
        IORequestQueue _queue;
        std::vector<std::thread> _threads;
        std::vector<IOWorker *> _workers;
    };


    /**
     * @brief Singleton IOMgr; used to retrieve IOHandles
     */
    class IOMgr {
    public:
        static const int IO_MODE_READ = 1;
        static const int IO_MODE_APPEND = 2;
        static const int IO_MODE_WRITE = 4;

        static const int NUM_THREADS = 16; // XXX need way to set dynamically
        static const int MAX_FILEHANDLES = 32;

        static IOMgr *getInstance();

        // no create call, first write, after open for write, will do the create
        std::shared_ptr<IOHandle> open(const std::filesystem::path &path, int mode, bool compressed);

        void remove(const std::filesystem::path &path);

        inline void queue_request(const IORequest &request) {
            _thread_pool.queue(request);
        }

    protected:
        static IOMgr *_instance;
        static std::mutex _mutex;

        IOMgr(int num_threads, int max_filehandles)
            : _thread_pool(num_threads),
              _file_cache(max_filehandles, _evict_callback)
        { };

        ~IOMgr(){};

    private:
        IOPool _thread_pool;

        LruObjectCache<std::filesystem::path, IOFile> _file_cache;

        // delete copy constructor
        IOMgr(const IOMgr &)          = delete;
        void operator=(const IOMgr &) = delete;

        static bool _evict_callback(std::shared_ptr<IOFile> filehandle);
    };

}