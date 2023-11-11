#include <string>
#include <vector>
#include <functional>
#include <filesystem>
#include <memory>
#include <future>

#include <storage/io_request.hh>
#include <storage/io.hh>
#include <storage/io_pool.hh>
#include <storage/exception.hh>

namespace springtail {

    std::future<std::shared_ptr<IOResponseRead>>
    IOHandle::read(uint64_t pos, io_read_callback_fn callback)
    {
        std::shared_ptr<IORequestRead> req = std::make_shared<IORequestRead>(_path, _is_compressed, pos, callback);
        std::future<std::shared_ptr<IOResponseRead>> future = req->promise.get_future();
        IOMgr *mgr = IOMgr::getInstance();
        mgr->queue_request(req);
        return future;
    }
    

    std::future<std::shared_ptr<IOResponseAppend>>
    IOHandle::append(const char *buffer, int length, io_write_callback_fn callback)
    {
        std::shared_ptr<std::vector<char>> data = std::make_shared<std::vector<char>>(buffer, buffer + length);
        return append(data, callback);
    }
    

    std::future<std::shared_ptr<IOResponseAppend>>
    IOHandle::append(std::shared_ptr<std::vector<char>> data, io_write_callback_fn callback)
    {
        std::shared_ptr<IORequestAppend> req = std::make_shared<IORequestAppend>(_path, _is_compressed, data, callback);
        std::future<std::shared_ptr<IOResponseAppend>> future = req->promise.get_future();
        IOMgr *mgr = IOMgr::getInstance();
        mgr->queue_request(req);
        return future;
    }


    std::future<std::shared_ptr<IOResponseAppend>>
    IOHandle::append(std::shared_ptr<std::vector<char>> data[], uint8_t count, io_write_callback_fn callback)
    {
        std::vector<std::shared_ptr<std::vector<char>>> vec(data, data + count);
        return append(vec, callback);
    }


    std::future<std::shared_ptr<IOResponseAppend>>
    IOHandle::append(const std::vector<std::shared_ptr<std::vector<char>>> &data, io_write_callback_fn callback)
    {
        std::shared_ptr<IORequestAppend> req = std::make_shared<IORequestAppend>(_path, _is_compressed, data, callback);
        std::future<std::shared_ptr<IOResponseAppend>> future = req->promise.get_future();
        IOMgr *mgr = IOMgr::getInstance();
        mgr->queue_request(req);
        return future;
    }


    std::future<std::shared_ptr<IOResponseWrite>>
    IOHandle::write(uint64_t offset, std::shared_ptr<std::vector<char>> data, io_write_callback_fn callback)
    {
        if (_is_compressed == false) {
            throw StorageError();
        }
        std::shared_ptr<IORequestWrite> req = std::make_shared<IORequestWrite>(_path, _is_compressed, offset, data, callback);
        std::future<std::shared_ptr<IOResponseWrite>> future = req->promise.get_future();        
        IOMgr *mgr = IOMgr::getInstance();
        mgr->queue_request(req);
        return future;
    }


    std::future<std::shared_ptr<IOResponseWrite>>
    IOHandle::write(uint64_t offset, std::vector<std::shared_ptr<std::vector<char>>> data, io_write_callback_fn callback)
    {
        if (_is_compressed == false) {
            throw StorageError();
        }

        std::shared_ptr<IORequestWrite> req = std::make_shared<IORequestWrite>(_path, _is_compressed, offset, data, callback);
        std::future<std::shared_ptr<IOResponseWrite>> future = req->promise.get_future();        
        IOMgr *mgr = IOMgr::getInstance();
        mgr->queue_request(req);
        return future;
    }
    
    
    std::future<std::shared_ptr<IOResponse>>
    IOHandle::sync(io_status_callback_fn callback)
    {
        std::shared_ptr<IORequestSync> req = std::make_shared<IORequestSync>(_path, _is_compressed, callback);
        std::future<std::shared_ptr<IOResponse>> future = req->promise.get_future();
        IOMgr *mgr = IOMgr::getInstance();
        mgr->queue_request(req);
        return future;
    }

    // synchronous methods

    std::shared_ptr<IOResponseRead>
    IOHandle::read(uint64_t pos)
    {
        std::shared_ptr<IORequestRead> req = std::make_shared<IORequestRead>(_path, _is_compressed, pos);
        std::future<std::shared_ptr<IOResponseRead>> future = req->promise.get_future();
        IOMgr *mgr = IOMgr::getInstance();
        mgr->queue_request(req);
        future.wait();
        return future.get();
    }


    std::shared_ptr<IOResponseAppend>
    IOHandle::append(const char *buffer, int length)
    {
        std::shared_ptr<std::vector<char>> data = std::make_shared<std::vector<char>>(buffer, buffer + length);
        return append(data);
    }
    

    std::shared_ptr<IOResponseAppend>   
    IOHandle::append(std::shared_ptr<std::vector<char>> data)
    {
        std::shared_ptr<IORequestAppend> req = std::make_shared<IORequestAppend>(_path, _is_compressed, data);
        std::future<std::shared_ptr<IOResponseAppend>> future = req->promise.get_future();
        IOMgr *mgr = IOMgr::getInstance();
        mgr->queue_request(req);
        future.wait();
        return future.get();
    }


    std::shared_ptr<IOResponseAppend>
    IOHandle::append(std::shared_ptr<std::vector<char>> data[], uint8_t count)
    {
        std::vector<std::shared_ptr<std::vector<char>>> vec(data, data + count);
        return append(vec);
    }


    std::shared_ptr<IOResponseAppend>
    IOHandle::append(const std::vector<std::shared_ptr<std::vector<char>>> &data)
    {
        std::shared_ptr<IORequestAppend> req = std::make_shared<IORequestAppend>(_path, _is_compressed, data);
        std::future<std::shared_ptr<IOResponseAppend>> future = req->promise.get_future();
        IOMgr *mgr = IOMgr::getInstance();
        mgr->queue_request(req);
        future.wait();
        return future.get();   
    }


    std::shared_ptr<IOResponseWrite>
    IOHandle::write(uint64_t offset, std::shared_ptr<std::vector<char>> data)
    {
        if (_is_compressed == false) {
            throw StorageError();
        }
        std::shared_ptr<IORequestWrite> req = std::make_shared<IORequestWrite>(_path, _is_compressed, offset, data);
        std::future<std::shared_ptr<IOResponseWrite>> future = req->promise.get_future();        
        IOMgr *mgr = IOMgr::getInstance();
        mgr->queue_request(req);
        future.wait();
        return future.get();
    }


    std::shared_ptr<IOResponseWrite>
    IOHandle::write(uint64_t offset, std::vector<std::shared_ptr<std::vector<char>>> data)
    {
        if (_is_compressed == false) {
            throw StorageError();
        }
        std::shared_ptr<IORequestWrite> req = std::make_shared<IORequestWrite>(_path, _is_compressed, offset, data);
        std::future<std::shared_ptr<IOResponseWrite>> future = req->promise.get_future();        
        IOMgr *mgr = IOMgr::getInstance();
        mgr->queue_request(req);
        future.wait();
        return future.get();
    }


    std::shared_ptr<IOResponse>
    IOHandle::sync()
    {
        std::shared_ptr<IORequestSync> req = std::make_shared<IORequestSync>(_path, _is_compressed);
        std::future<std::shared_ptr<IOResponse>> future = req->promise.get_future();
        IOMgr *mgr = IOMgr::getInstance();
        mgr->queue_request(req);
        future.wait();
        return future.get();
    }
}