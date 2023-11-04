#pragma once

#include <string>
#include <memory>
#include <vector>
#include <functional>
#include <filesystem>
#include <variant>
#include <cstdio>
#include <thread>

#include <storage/compressors.hh>

namespace springtail {

    enum IOStatus {SUCCESS, ERR_NOENT, ERR_BADFD, ERR_ACCESS, ERR_ARGS, ERR_DECODE, ERR_FATAL};

    struct IORequestRead {
        uint64_t offset;
        std::function<void(const std::vector<char> data, const IOStatus &status)> callback;
    };

    struct IORequestWrite {
        uint64_t offset;
        int count;

        std::variant<
            std::vector<char>,
            std::vector<std::vector<char>>
        > data;

        std::function<void(uint64_t offset, const IOStatus &status)> callback;
    };

    struct IORequestAppend {
        int count;

        std::variant<
            std::vector<char>,
            std::vector<std::vector<char>>
        > data;

        std::function<void(uint64_t offset, const IOStatus &status)> callback;
    };

    struct IORequestSync {
        std::function<void(const IOStatus &status)> callback;
    };

    class IOFile;

    struct IORequest {
        enum OpType {READ, APPEND, WRITE, SYNC, SHUTDOWN};

        OpType  type;
        bool    compressed;

        std::variant<
            IORequestAppend,
            IORequestWrite,
            IORequestRead,
            IORequestSync
        > args;
    };

    class IOFile {
    public:
        IOFile(std::filesystem::path path, std::FILE *file) :
            _path(path), _file(file), _is_dirty(false), _is_busy(false) {}

        ~IOFile();

    private:
        std::filesystem::path _path;
        std::FILE * _file;
        bool _is_dirty;
        bool _is_busy;
    };


    class IOWorker {
    private:
        std::unique_ptr<Compressor> _compressor;
        std::unique_ptr<Decompressor> _decompressor;

    public:
        inline std::unique_ptr<Compressor> get_compressor() { return _compressor; }
        inline std::unique_ptr<Decompressor> get_decompressor() { return _decompressor; }


    };


    class IORequestQueue {
    public:
        void push(const IORequest &request);
        IORequest pop();

    private:
        std::queue<IORequest> _queue;
        std::condition_variable _cv;
        std::mutex _mutex;
    };


    class IOPool {
    public:
        IOPool(int threads);
        void queue(const IORequest &request);

    private:
        IORequestQueue _queue;
        std::vector<std::thread> _threads;
        std::vector<IOWorker> _workers;
        LruObjectCache<std::filesystem::path, IOFile> _file_cache;
    };
}