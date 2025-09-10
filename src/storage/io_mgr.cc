#include <memory>
#include <filesystem>
#include <cstdio>
#include <mutex>

#include <fmt/core.h>

#include <storage/compressors.hh>
#include <storage/io_mgr.hh>
#include <storage/io_file.hh>
#include <storage/io.hh>
#include <storage/exception.hh>

#include <common/logging.hh>


namespace springtail {

    void
    IOMgr::_init(int num_threads, int max_filehandles, int io_request_queue_size)
    {
        _thread_pool = std::make_shared<ThreadPool<IORequest>>(num_threads, io_request_queue_size);
        _file_cache = std::make_shared<LruObjectCache<std::filesystem::path, IOFile>>(max_filehandles, _evict_callback);

        for (int i = 0; i < num_threads; i++) {
            std::shared_ptr<Compressor> c = std::make_shared<Lz4Compressor>();
            _compressors.push(c);
            std::shared_ptr<Decompressor> d = std::make_shared<Lz4Decompressor>();
            _decompressors.push(d);
        }
    };

    void
    IOMgr::_internal_shutdown()
    {
        _file_cache->clear(true);
        _thread_pool->shutdown();
    }

    std::shared_ptr<Compressor>
    IOMgr::get_compressor()
    {
        std::scoped_lock<std::mutex> lock(_compressor_mutex);
        assert(!_compressors.empty());
        std::shared_ptr<Compressor> compressor = _compressors.front();
        _compressors.pop();
        return compressor;
    }

    void
    IOMgr::put_compressor(std::shared_ptr<Compressor> compressor)
    {
        std::scoped_lock<std::mutex> lock(_compressor_mutex);
        _compressors.push(compressor);
    }

    std::shared_ptr<Decompressor>
    IOMgr::get_decompressor()
    {
        std::scoped_lock<std::mutex> lock(_decompressor_mutex);
        assert(!_decompressors.empty());
        std::shared_ptr<Decompressor> decompressor = _decompressors.front();
        _decompressors.pop();
        return decompressor;
    }

    void
    IOMgr::put_decompressor(std::shared_ptr<Decompressor> decompressor)
    {
        std::scoped_lock<std::mutex> lock(_decompressor_mutex);
        _decompressors.push(decompressor);
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

        std::shared_ptr<IOFile> file = _file_cache->get(path);
        if (file == nullptr) {
            // allocate a file object and insert into cache
            // while mutex is held
            file = std::make_shared<IOFile>(path, is_compressed);

            // this may trigger an eviction and eviction callback
            _file_cache->insert(path, file);
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

} // namespace springtail