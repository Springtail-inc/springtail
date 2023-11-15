#pragma once

#include <vector>
#include <functional>
#include <filesystem>
#include <future>
#include <memory>
#include <cerrno>

namespace springtail {

    /** IO Status set in response; important keep success == 0 */
    enum IOStatus {SUCCESS=0, ERROR, ERR_SEEK, ERR_NOENT, ERR_BADFD, ERR_ACCESS, ERR_ARGS, ERR_DECODE};

    // helper types for callbacks; to improve readability
    class IOResponseWrite;
    class IOResponseAppend;
    class IOResponseRead;
    class IOResponse;
    
    typedef std::function<void(std::shared_ptr<IOResponseRead> response)> io_read_callback_fn;
    typedef std::function<void(std::shared_ptr<IOResponseWrite> response)> io_write_callback_fn;
    typedef std::function<void(std::shared_ptr<IOResponseAppend> response)> io_append_callback_fn;
    typedef std::function<void(std::shared_ptr<IOResponse> response)> io_status_callback_fn;

    class IORequest {
    public:
        enum IOType {READ, APPEND, WRITE, SYNC, SHUTDOWN};

        IOType                type;
        std::filesystem::path path;
        bool                  compressed;

        IORequest(IOType type, const std::filesystem::path &path, bool is_compressed)
            : type(type), path(path), compressed(is_compressed) {}

        IORequest() : type(IOType::SHUTDOWN) {}

        virtual ~IORequest() = default;
    };

    class IOResponse {
    public:
        IORequest::IOType     type;
        std::filesystem::path path;
        IOStatus              status;

        IOResponse(IORequest::IOType type, const std::filesystem::path &path, 
                   IOStatus status=IOStatus::SUCCESS)
            : type(type), path(path), status(status) {}

        inline void set_status(IOStatus iostatus) { status = iostatus; }

        inline void set_status(int fh_errno) 
        {
            if (fh_errno == -1) { return; }
            switch (fh_errno) {
                case 0:
                    status = IOStatus::SUCCESS;
                    break;
                default:
                    status = IOStatus::ERROR;
            }
        }

        inline bool is_success() { return status == IOStatus::SUCCESS; }
    };

    class IOResponseWrite : public IOResponse {
    public: 
        /** offset of this write */
        uint64_t offset;

        /** next offset after this write (length of write data + hdr) */
        uint64_t next_offset;

        IOResponseWrite(std::filesystem::path &path, uint64_t offset, uint64_t next_offset, IOStatus status=IOStatus::SUCCESS)
            : IOResponse(IORequest::IOType::WRITE, path, status), offset(offset), next_offset(next_offset) {}

        IOResponseWrite(std::filesystem::path &path, IOStatus status=IOStatus::ERROR)
            : IOResponse(IORequest::IOType::WRITE, path, status) {}
    };

    class IORequestWrite : public IORequest {
    public:
        uint64_t                                        offset;
        std::vector<std::shared_ptr<std::vector<char>>> data;
        std::promise<std::shared_ptr<IOResponseWrite>>  promise;
        io_write_callback_fn                            callback;

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

        inline void complete(std::shared_ptr<IOResponseWrite> res, int fh_errno=-1)
        {
            res->set_status(fh_errno);
            promise.set_value(res);
            if (callback) {
                callback(res);
            }
        }
    };

    class IOResponseAppend : public IOResponse {
    public:
        /** offset of the append */
        uint64_t offset;

        /** next offset -- also EOF marker */
        uint64_t next_offset;

        IOResponseAppend(std::filesystem::path &path, uint64_t offset, uint64_t next_offset, IOStatus status=IOStatus::SUCCESS)
            : IOResponse(IORequest::IOType::APPEND, path, status), offset(offset), next_offset(next_offset) {}

        IOResponseAppend(std::filesystem::path &path, IOStatus status=IOStatus::ERROR)
            : IOResponse(IORequest::IOType::APPEND, path, status) {}
    };

    class IORequestAppend : public IORequest {
    public:
        std::vector<std::shared_ptr<std::vector<char>>> data;
        std::promise<std::shared_ptr<IOResponseAppend>> promise;
        io_append_callback_fn callback;

        IORequestAppend(std::filesystem::path &path, bool is_compressed, 
                        std::shared_ptr<std::vector<char>> datavec, 
                        io_append_callback_fn cb) 
            : IORequest(IORequest::IOType::APPEND, path, is_compressed),
              callback(cb) { data.push_back(datavec); }

        IORequestAppend(std::filesystem::path &path, bool is_compressed, 
                        std::shared_ptr<std::vector<char>> datavec) 
            : IORequest(IORequest::IOType::APPEND, path, is_compressed)
        { data.push_back(datavec); }

        IORequestAppend(std::filesystem::path &path, bool is_compressed, 
                       const std::vector<std::shared_ptr<std::vector<char>>> &data, 
                       io_append_callback_fn cb) 
            : IORequest(IORequest::IOType::APPEND, path, is_compressed),
              data(data), callback(cb) {}

        IORequestAppend(std::filesystem::path &path, bool is_compressed,
                        const std::vector<std::shared_ptr<std::vector<char>>> &data) 
            : IORequest(IORequest::IOType::APPEND, path, is_compressed), data(data) {}

        inline void complete(std::shared_ptr<IOResponseAppend> res, int fh_errno=-1) 
        {
            res->set_status(fh_errno);
            promise.set_value(res);
            if (callback) {
                callback(res);
            }
        }
    };

    class IOResponseRead : public IOResponse {
    public:
        /** current offset for read */
        uint64_t offset;
        
        /** next valid offset after this read */
        uint64_t next_offset;

        std::vector<std::shared_ptr<std::vector<char>>> data;

        IOResponseRead(std::filesystem::path &path, IOStatus status, uint64_t offset,
                       std::vector<std::shared_ptr<std::vector<char>>> &data)
            : IOResponse(IORequest::IOType::READ, path, status),
              offset(offset), data(data) {}

        IOResponseRead(std::filesystem::path &path, IOStatus status=IOStatus::ERROR)
            : IOResponse(IORequest::IOType::READ, path, status) {}
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
    
        inline void complete(std::shared_ptr<IOResponseRead> res, int fh_errno=-1) 
        {
            res->set_status(fh_errno);
            promise.set_value(res);
            if (callback) {
                callback(res);
            }
        }
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

        inline void complete(std::shared_ptr<IOResponse> res, int fh_errno=-1) 
        {
            res->set_status(fh_errno);
            promise.set_value(res);
            if (callback) {
                callback(res);
            }
        }
    };
}