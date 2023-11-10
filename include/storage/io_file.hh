#pragma once

#include <memory>
#include <functional>
#include <iostream>
#include <vector>
#include <cstdio>
#include <atomic>
#include <mutex>
#include <cassert>

#include <storage/io_request.hh>
#include <storage/io_pool.hh>
#include <storage/exception.hh>
#include <storage/compressors.hh>

namespace springtail {
    class IOSysFH {
    private:
        std::filesystem::path _path;
        std::FILE *_file;
        bool _is_compressed;
        bool _is_dirty;
        bool _is_readonly;

    public:
        std::atomic<bool> is_busy;

        IOSysFH(const std::filesystem::path &path, const IOMgr::IO_MODE &mode, bool is_compressed);
        ~IOSysFH();

        void read(uint64_t offset, std::shared_ptr<Decompressor> decompressor, io_read_callback_fn callback);

        void write(uint64_t offset, std::shared_ptr<std::vector<char>> data, io_write_callback_fn callback);

        void write(uint64_t offset, const std::vector<std::shared_ptr<std::vector<char>>> &data, io_write_callback_fn callback);

        void append(std::shared_ptr<std::vector<char>> data, std::shared_ptr<Compressor> compressor, io_write_callback_fn callback);

        void append(const std::vector<std::shared_ptr<std::vector<char>>> &data, std::shared_ptr<Compressor> compressor, io_write_callback_fn callback);

        void sync(io_status_callback_fn callback);

        void close();

        bool is_readonly() { return _is_readonly; }
    };


    class IOFile {
    private:
        std::filesystem::path _path;
        bool _is_compressed;

        /** count worker threads with object reference; protected by IOMgr::_cache_mutex */
        std::atomic<int> _in_use_count;

        // following protected by _mutex
        std::mutex _mutex;
        std::condition_variable _cv_read;
        std::condition_variable _cv_write;
        std::vector<std::shared_ptr<IOSysFH>> _read_fhs;
        std::shared_ptr<IOSysFH> _write_fh;

        std::shared_ptr<IOSysFH> _get_read_fh(const IOMgr::IO_MODE &mode);

        std::shared_ptr<IOSysFH> _get_write_fh(const IOMgr::IO_MODE &mode);

    public:
        IOFile(const std::filesystem::path &path, bool is_compressed)
            : _path(path), _is_compressed(is_compressed), _in_use_count(0)
        {}

        // in use count protected by IOMgr::_cache_mutex
        // incremented in IOMgr::lookup
        // decremented in IOMgr::release
        inline bool in_use() { return _in_use_count > 0; }
        inline void incr_in_use() { _in_use_count++; }
        inline void decr_in_use() { _in_use_count--; assert(_in_use_count >= 0); }

        std::shared_ptr<IOSysFH> get_fh(const IOMgr::IO_MODE &mode);
        void put_fh(std::shared_ptr<IOSysFH> fh);

        // called with IOMgr::_cache_mutex locked
        bool try_close_all();
    };
}