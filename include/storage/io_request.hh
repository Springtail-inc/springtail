#pragma once

#include <vector>
#include <functional>
#include <filesystem>
#include <future>
#include <memory>

namespace springtail {

    enum IOStatus {SUCCESS, ERR_NOENT, ERR_BADFD, ERR_ACCESS, ERR_ARGS, ERR_DECODE, ERR_FATAL};

        // helper types for callbacks; to improve readability
    typedef std::function<void(const std::vector<std::shared_ptr<std::vector<char>>> data, const IOStatus &status)> io_read_callback_fn;
    typedef std::function<void(uint64_t offset, const IOStatus &status)> io_write_callback_fn;
    typedef std::function<void(const IOStatus &status)> io_status_callback_fn;

    class IORequest {
    public:
        enum IOType {READ, APPEND, WRITE, SYNC, SHUTDOWN};

        IOType                type;
        std::filesystem::path path;
        bool                  compressed;

        IORequest(IOType type, std::filesystem::path &path, bool is_compressed)
            : type(type), path(path), compressed(is_compressed) {}

        IORequest() : type(IOType::SHUTDOWN) {}

        virtual ~IORequest() = default;
    };

    class IOResponse {
    public:
        IORequest::IOType     type;
        std::filesystem::path path;
        IOStatus              status;

        IOResponse(IORequest::IOType &type, std::filesystem::path &path, IOStatus &status)
            : type(type), path(path), status(status) {}

        bool is_success() { return status == IOStatus::SUCCESS; }
    };

    class IOResponseWrite : public IOResponse {
    public: 
        uint64_t offset;
    };

    class IORequestWrite : public IORequest {
    public:
        uint64_t                      offset;

        std::vector<std::shared_ptr<std::vector<char>>> data;
        std::promise<std::shared_ptr<IOResponseWrite>> promise;
        io_write_callback_fn          callback;

        IORequestWrite(std::filesystem::path &path, bool is_compressed, 
                       uint64_t offset, std::shared_ptr<std::vector<char>> datavec, 
                       io_write_callback_fn cb) 
            : IORequest(IORequest::IOType::WRITE, path, is_compressed),
              offset(offset), callback(cb) { data.push_back(datavec); }

        IORequestWrite(std::filesystem::path &path, bool is_compressed, 
                uint64_t offset, std::shared_ptr<std::vector<char>> datavec) 
            : IORequest(IORequest::IOType::WRITE, path, is_compressed),
              offset(offset) { data.push_back(datavec); }

        IORequestWrite(std::filesystem::path &path, bool is_compressed, uint64_t offset, 
                       const std::vector<std::shared_ptr<std::vector<char>>> &data, 
                       io_write_callback_fn cb) 
            : IORequest(IORequest::IOType::WRITE, path, is_compressed),
              offset(offset), data(data), callback(cb) {}

        IORequestWrite(std::filesystem::path &path, bool is_compressed, uint64_t offset, 
                       const std::vector<std::shared_ptr<std::vector<char>>> &data) 
            : IORequest(IORequest::IOType::WRITE, path, is_compressed),
              offset(offset), data(data) {}
    };

    class IOResponseAppend : public IOResponse {
    public:
        uint64_t offset;
    };

    class IORequestAppend : public IORequest {
    public:
        std::vector<std::shared_ptr<std::vector<char>>> data;
        std::promise<std::shared_ptr<IOResponseAppend>> promise;
        io_write_callback_fn callback;

        IORequestAppend(std::filesystem::path &path, bool is_compressed, 
                        std::shared_ptr<std::vector<char>> datavec, 
                        io_write_callback_fn cb) 
            : IORequest(IORequest::IOType::APPEND, path, is_compressed),
              callback(cb) { data.push_back(datavec); }

        IORequestAppend(std::filesystem::path &path, bool is_compressed, 
                        std::shared_ptr<std::vector<char>> datavec) 
            : IORequest(IORequest::IOType::APPEND, path, is_compressed)
        { data.push_back(datavec); }

        IORequestAppend(std::filesystem::path &path, bool is_compressed, 
                       const std::vector<std::shared_ptr<std::vector<char>>> &data, 
                       io_write_callback_fn cb) 
            : IORequest(IORequest::IOType::APPEND, path, is_compressed),
              data(data), callback(cb) {}

        IORequestAppend(std::filesystem::path &path, bool is_compressed,
                        const std::vector<std::shared_ptr<std::vector<char>>> &data) 
            : IORequest(IORequest::IOType::APPEND, path, is_compressed), data(data) {}
    };

    class IOResponseRead : public IOResponse {
    public:
        uint64_t offset;
        std::vector<std::shared_ptr<std::vector<char>>> data;
    };

    class IORequestRead : public IORequest {
    public:
        uint64_t offset;
        std::promise<std::shared_ptr<IOResponseRead>> promise;
        io_read_callback_fn callback;

        IORequestRead(std::filesystem::path &path, bool is_compressed, 
                      uint64_t offset, io_read_callback_fn cb)
            : IORequest(IORequest::IOType::READ, path, is_compressed),
              offset(offset), callback(cb) {}

        IORequestRead(std::filesystem::path &path, bool is_compressed, 
                      uint64_t offset)
            : IORequest(IORequest::IOType::READ, path, is_compressed), offset(offset) {}
    };

    class IORequestSync : public IORequest {
    public:
        std::promise<std::shared_ptr<IOResponse>> promise;
        io_status_callback_fn callback;

        IORequestSync(std::filesystem::path &path, bool is_compressed, 
                      io_status_callback_fn cb)
            : IORequest(IORequest::IOType::SYNC, path, is_compressed),
              callback(cb) {}

        IORequestSync(std::filesystem::path &path, bool is_compressed)
            : IORequest(IORequest::IOType::SYNC, path, is_compressed) {}
    };
}