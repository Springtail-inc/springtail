#include <string>
#include <vector>
#include <functional>
#include <filesystem>

#include <storage/io_request.hh>
#include <storage/io.hh>
#include <storage/io_pool.hh>
#include <storage/exception.hh>

namespace springtail {
    void
    IOHandle::read(uint64_t pos, io_read_callback_fn callback)
    {
        IORequestRead req(pos, callback);
        IOMgr *mgr = IOMgr::getInstance();
        mgr->queue_request(IORequest(req, _path, _is_compressed));
    }
    

    void 
    IOHandle::append(const char *buffer, int length, io_write_callback_fn callback)
    {
        std::vector<char> data(buffer, buffer + length);
        append(data, callback);
    }
    

    void 
    IOHandle::append(const std::vector<char> &data, io_write_callback_fn callback)
    {
        IORequestAppend req(data, callback);
        IOMgr *mgr = IOMgr::getInstance();
        mgr->queue_request(IORequest(req, _path, _is_compressed));
    }


    void 
    IOHandle::append(const std::vector<char> data[], uint8_t count, io_write_callback_fn callback)
    {
        std::vector<std::vector<char>> vec(data, data + count);
        append(vec, callback);
    }
    

    void
    IOHandle::append(const std::vector<std::vector<char>> data, io_write_callback_fn callback)
    {
        IORequestAppend req(data, callback);
        IOMgr *mgr = IOMgr::getInstance();
        mgr->queue_request(IORequest(req, _path, _is_compressed));
    }


    void 
    IOHandle::write(uint64_t offset, const std::vector<char> &data, io_write_callback_fn callback)
    {
        if (_is_compressed == false) {
            throw StorageError();
        }

        IORequestWrite req(offset, data, callback);
        IOMgr *mgr = IOMgr::getInstance();
        mgr->queue_request(IORequest(req, _path, _is_compressed));
    }
    
    
    void 
    IOHandle::sync(io_status_callback_fn callback)
    {
        IORequestSync req(callback);
        IOMgr *mgr = IOMgr::getInstance();
        mgr->queue_request(IORequest(req, _path));
    }

}