#pragma once

#include <vector>
#include <functional>
#include <filesystem>

namespace springtail {
    enum IOStatus {SUCCESS, ERR_NOENT, ERR_BADFD, ERR_ACCESS, ERR_ARGS, ERR_DECODE, ERR_FATAL};

    // helper types for callbacks; to improve readability
    typedef std::function<void(const std::vector<char> data, const IOStatus &status)> io_read_callback_fn;
    typedef std::function<void(uint64_t offset, const IOStatus &status)> io_write_callback_fn;
    typedef std::function<void(const IOStatus &status)> io_status_callback_fn;

    struct IORequestRead {
        uint64_t offset;
        io_read_callback_fn callback;

        IORequestRead(uint64_t boffset, io_read_callback_fn callbackfn)
            : offset(boffset), callback(callbackfn) {}
    };

    struct IORequestWrite {
        uint64_t offset;

        std::variant<
            std::vector<char>,
            std::vector<std::vector<char>>
        > data;

        io_write_callback_fn callback;

        IORequestWrite(uint64_t boffset,
                       const std::vector<char> &datavec,
                       io_write_callback_fn callbackfn)
            : offset(boffset), callback(callbackfn) {
            data.emplace<std::vector<char>>(datavec);
        }

        IORequestWrite(uint64_t boffset,
                       const std::vector<std::vector<char>> &datavecs,
                       io_write_callback_fn callbackfn)
            : offset(boffset), callback(callbackfn) {
            data.emplace<std::vector<std::vector<char>>>(datavecs);
        }
    };

    struct IORequestAppend {
        std::variant<
            std::vector<char>,
            std::vector<std::vector<char>>
        > data;

        io_write_callback_fn callback;

        IORequestAppend(const std::vector<char> &datavec, io_write_callback_fn callbackfn)
            : callback(callbackfn) {
            data.emplace<std::vector<char>>(datavec);
        }

        IORequestAppend(const std::vector<std::vector<char>> &datavecs,
                        io_write_callback_fn callbackfn)
            : callback(callbackfn) {
            data.emplace<std::vector<std::vector<char>>>(datavecs);
        }
    };

    struct IORequestSync {
        io_status_callback_fn callback;

        IORequestSync(io_status_callback_fn callbackfn) : callback(callbackfn) {}
    };

    class IORequest {
    public:
        enum OpType {READ, APPEND, WRITE, SYNC, SHUTDOWN};

        OpType  type;
        std::filesystem::path path;
        bool    compressed;

        std::variant<
            IORequestAppend,
            IORequestWrite,
            IORequestRead,
            IORequestSync,
            void *
        > args;

        IORequest() : type(SHUTDOWN), args(nullptr) {}

        IORequest(const IORequestAppend &request, const std::filesystem::path &fspath, bool is_compressed)
            : type(APPEND), path(fspath), compressed(is_compressed), args(request) {}

        IORequest(const IORequestWrite &request, const std::filesystem::path &fspath, bool is_compressed)
            : type(WRITE), path(fspath), compressed(is_compressed), args(request) {}

        IORequest(const IORequestRead &request, const std::filesystem::path &fspath, bool is_compressed)
            : type(READ), compressed(is_compressed), args(request) {}

        IORequest(const IORequestSync &request, const std::filesystem::path &fspath)
            : type(READ), path(fspath), args(request) {}
    };
}