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

    /**
     * @brief Asynchronous read
     * 
     * @param pos       Read offset; must be at a block boundary
     * @param callback  Optional callback; pass {} for empty callback
     * @return std::future<std::shared_ptr<IOResponseRead>> Future containing a IOResponseRead ptr
     */
    std::future<std::shared_ptr<IOResponseRead>>
    IOHandle::async_read(uint64_t pos, io_read_callback_fn callback)
    {
        std::shared_ptr<IORequestRead> req = std::make_shared<IORequestRead>(_path, _is_compressed, pos, callback);
        std::future<std::shared_ptr<IOResponseRead>> future = req->promise.get_future();
        IOMgr *mgr = IOMgr::get_instance();
        mgr->queue_request(req);
        return future;
    }
    

    /**
     * @brief Asynchronous append; append data to end of file
     * 
     * @param buffer    Buffer to writeout
     * @param length    Length of buffer
     * @param callback  Optional callback; pass {} for empty callback
     * @return std::future<std::shared_ptr<IOResponseAppend>> Future containing a IOResponseAppend ptr
     */
    std::future<std::shared_ptr<IOResponseAppend>>
    IOHandle::async_append(const char *buffer, int length, io_append_callback_fn callback)
    {
        std::shared_ptr<std::vector<char>> data = std::make_shared<std::vector<char>>(buffer, buffer + length);
        return async_append(data, callback);
    }
    
    
    /**
     * @brief Asynchronous append; append data to end of file
     * 
     * @param data      Data to be written out
     * @param callback  Optional callback; pass {} for empty callback
     * @return std::future<std::shared_ptr<IOResponseAppend>> Future containing a IOResponseAppend ptr
     */
    std::future<std::shared_ptr<IOResponseAppend>>
    IOHandle::async_append(std::shared_ptr<std::vector<char>> data, io_append_callback_fn callback)
    {
        std::shared_ptr<IORequestAppend> req = std::make_shared<IORequestAppend>(_path, _is_compressed, data, callback);
        std::future<std::shared_ptr<IOResponseAppend>> future = req->promise.get_future();
        IOMgr *mgr = IOMgr::get_instance();
        mgr->queue_request(req);
        return future;
    }


    /**
     * @brief Asynchronous append; append data to end of file
     * 
     * @param data      Data to be written out; array of vectors
     * @param count     Number of vectors in data array
     * @param callback  Optional callback; pass {} for empty callback
     * @return std::future<std::shared_ptr<IOResponseAppend>> Future containing a IOResponseAppend ptr
     */
    std::future<std::shared_ptr<IOResponseAppend>>
    IOHandle::async_append(std::shared_ptr<std::vector<char>> data[], uint8_t count, io_append_callback_fn callback)
    {
        std::vector<std::shared_ptr<std::vector<char>>> vec(data, data + count);
        return async_append(vec, callback);
    }


    /**
     * @brief Asynchronous append; append data to end of file
     * 
     * @param data      Data to be written out; vector of char vectors
     * @param callback  Optional callback; pass {} for empty callback
     * @return std::future<std::shared_ptr<IOResponseAppend>> Future containing a IOResponseAppend ptr
     */
    std::future<std::shared_ptr<IOResponseAppend>>
    IOHandle::async_append(const std::vector<std::shared_ptr<std::vector<char>>> &data, io_append_callback_fn callback)
    {
        std::shared_ptr<IORequestAppend> req = std::make_shared<IORequestAppend>(_path, _is_compressed, data, callback);
        std::future<std::shared_ptr<IOResponseAppend>> future = req->promise.get_future();
        IOMgr *mgr = IOMgr::get_instance();
        mgr->queue_request(req);
        return future;
    }


    /**
     * @brief Asynchronous write; overwrite data at offset
     * 
     * @param offset    Offset at which to write data
     * @param data      Data to be written out; char vector ptr
     * @param callback  Optional callback; pass {} for empty callback
     * @return std::future<std::shared_ptr<IOResponseWrite>> Future containing a IOResponseWrite ptr
     */
    std::future<std::shared_ptr<IOResponseWrite>>
    IOHandle::async_write(uint64_t offset, std::shared_ptr<std::vector<char>> data, io_write_callback_fn callback)
    {
        if (_is_compressed == true) {
            throw StorageError();
        }
        std::shared_ptr<IORequestWrite> req = std::make_shared<IORequestWrite>(_path, _is_compressed, offset, data, callback);
        std::future<std::shared_ptr<IOResponseWrite>> future = req->promise.get_future();        
        IOMgr *mgr = IOMgr::get_instance();
        mgr->queue_request(req);
        return future;
    }


    /**
     * @brief Asynchronous write; overwrite data at offset
     * 
     * @param offset    Offset at which to write data
     * @param data      Data to be written out; vector of char vector ptrs
     * @param callback  Optional callback; pass {} for empty callback
     * @return std::future<std::shared_ptr<IOResponseWrite>> Future containing a IOResponseWrite ptr
     */
    std::future<std::shared_ptr<IOResponseWrite>>
    IOHandle::async_write(uint64_t offset, std::vector<std::shared_ptr<std::vector<char>>> data, io_write_callback_fn callback)
    {
        if (_is_compressed == true) {
            throw StorageError();
        }

        std::shared_ptr<IORequestWrite> req = std::make_shared<IORequestWrite>(_path, _is_compressed, offset, data, callback);
        std::future<std::shared_ptr<IOResponseWrite>> future = req->promise.get_future();        
        IOMgr *mgr = IOMgr::get_instance();
        mgr->queue_request(req);
        return future;
    }
    

    /**
     * @brief Asynchronous sync; sync data to disk
     * 
     * @param callback  Optional callback; pass {} for empty callback
     * @return std::future<std::shared_ptr<IOResponse>> Future containing a IOResponse ptr
     */
    std::future<std::shared_ptr<IOResponse>>
    IOHandle::async_sync(io_status_callback_fn callback)
    {
        std::shared_ptr<IORequestSync> req = std::make_shared<IORequestSync>(_path, _is_compressed, callback);
        std::future<std::shared_ptr<IOResponse>> future = req->promise.get_future();
        IOMgr *mgr = IOMgr::get_instance();
        mgr->queue_request(req);
        return future;
    }

    // synchronous methods

    /**
     * @brief Synchronous read; calls async read; blocks on future
     * 
     * @param pos       Read offset; must be at a block boundary
     * @return std::shared_ptr<IOResponseRead> IOResponseRead ptr
     */
    std::shared_ptr<IOResponseRead>
    IOHandle::read(uint64_t pos)
    {
        auto &&future = async_read(pos, {});
        future.wait();
        return future.get();
    }


    /**
     * @brief Synchronous append; calls async append; blocks on future
     * 
     * @param buffer  Buffer to append to end of file
     * @param length  Length of buffer
     * @return std::shared_ptr<IOResponseAppend> IOResponseAppend ptr
     */
    std::shared_ptr<IOResponseAppend>
    IOHandle::append(const char *buffer, int length)
    {
        std::shared_ptr<std::vector<char>> data = std::make_shared<std::vector<char>>(buffer, buffer + length);
        return append(data);
    }
    

    /**
     * @brief Synchronous append; calls async append; blocks on future
     * 
     * @param data    Data vector; char vector ptr
     * @return std::shared_ptr<IOResponseAppend> IOResponseAppend ptr
     */
    std::shared_ptr<IOResponseAppend>   
    IOHandle::append(std::shared_ptr<std::vector<char>> data)
    {
        auto &&future = async_append(data, {});
        future.wait();
        return future.get();
    }


    /**
     * @brief Synchronous append; calls async append; blocks on future
     * 
     * @param data    Array of data vectors
     * @param count   Number of vectors in array
     * @return std::shared_ptr<IOResponseAppend> IOResponseAppend ptr
     */
    std::shared_ptr<IOResponseAppend>
    IOHandle::append(std::shared_ptr<std::vector<char>> data[], uint8_t count)
    {
        std::vector<std::shared_ptr<std::vector<char>>> vec(data, data + count);
        return append(vec);
    }


    /**
     * @brief Synchronous append; calls async append; blocks on future
     * 
     * @param data    Vector of data vectors; vector of char vector ptrs
     * @return std::shared_ptr<IOResponseAppend> IOResponseAppend ptr
     */
    std::shared_ptr<IOResponseAppend>
    IOHandle::append(const std::vector<std::shared_ptr<std::vector<char>>> &data)
    {
        auto &&future = async_append(data, {});
        future.wait();
        return future.get();   
    }


    /**
     * @brief Synchronous write (overwrite); calls async read; blocks on future
     * 
     * @param offset   Offset to write at
     * @param data     Data to write; char vector
     * @return std::shared_ptr<IOResponseWrite> IOResopnseWrite ptr
     */
    std::shared_ptr<IOResponseWrite>
    IOHandle::write(uint64_t offset, std::shared_ptr<std::vector<char>> data)
    {
        auto &&future = async_write(offset, data, {});
        future.wait();
        return future.get();
    }


    /**
     * @brief Synchronous write (overwrite); calls async read; blocks on future
     * 
     * @param offset   Offset to write at
     * @param data     Data to write; vector of char vectors
     * @return std::shared_ptr<IOResponseWrite> IOResopnseWrite ptr
     */
    std::shared_ptr<IOResponseWrite>
    IOHandle::write(uint64_t offset, std::vector<std::shared_ptr<std::vector<char>>> data)
    {
        auto &&future = async_write(offset, data, {});
        future.wait();
        return future.get();
    }


    /**
     * @brief Synchronous sync; sync data to disk; calls async sync; waits on future
     * 
     * @return std::shared_ptr<IOResponse> IOResponse ptr
     */
    std::shared_ptr<IOResponse>
    IOHandle::sync()
    {
        auto &&future = async_sync({});
        future.wait();
        return future.get();
    }
}