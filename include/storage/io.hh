#pragma once

#include <string>
#include <memory>
#include <vector>
#include <functional>
#include <filesystem>
#include <cstdio>

#include <storage/compressors.hh>

namespace springtail {

    class IOMgr;

    class IOHandle {
    private:
        std::unique_ptr<Compressor> compressor;
        std::unique_ptr<Decompressor> decompressor;

        std::filesystem::path _path;
        std::FILE *_file;
        int _mode;
        bool _is_dirty;

    public:
        enum IOStatus {SUCCESS, ERR_NOENT, ERR_BADFD, ERR_ACCESS, ERR_ARGS, ERR_DECODE, ERR_FATAL};

        IOHandle(std::FILE *file, std::filesystem::path path, int mode);

        IOHandle(std::filesystem::path path, int mode);

        // delete operator closes the underlying file handle
        ~IOHandle() {
            fclose(_file);
        };

        int get_mode() { return _mode; }

        std::string get_path() { return _path.string(); }

        // synchronous operations; blocks calling thread
        std::shared_ptr<std::vector<char>> read(uint64_t pos);

        uint64_t append(const char *buffer, int length);

        uint64_t append(const std::vector<char> &data);

        uint64_t append(std::vector<char> data[], uint8_t count);

        void sync();

        // async operations; calls callback

        // helper types for callbacks; to improve readability
        typedef std::function<void(const std::vector<char> data, const IOStatus &status)> read_callback_fn;
        typedef std::function<void(uint64_t offset, const IOHandle::IOStatus &status)> write_callback_fn;
        typedef std::function<void(const IOStatus &status)> status_callback_fn;

        void read(uint64_t pos, read_callback_fn callback);

        void append(const char *buffer, int length, write_callback_fn callback);

        void append(const std::vector<char> &data, write_callback_fn callback);

        void append(const std::vector<char> data[], uint8_t count, write_callback_fn callback);

        void sync(status_callback_fn callback);
    };


    class IOMgr {
    public:
        static const int IO_MODE_READ = 1;
        static const int IO_MODE_APPEND = 2;
        static const int IO_MODE_WRITE = 4;

        IOMgr() {};
        ~IOMgr(){};

        std::shared_ptr<IOHandle> open(std::string &path, int mode);
        std::shared_ptr<IOHandle> open(std::filesystem::path &path, int mode);

        std::shared_ptr<IOHandle> create(std::filesystem::path &path, int mode);
        std::shared_ptr<IOHandle> create(std::string &path, int mode);

        void remove(std::string &path);
    };
}