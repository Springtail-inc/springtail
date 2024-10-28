#pragma once

#include <string>
#include <vector>
#include <filesystem>
#include <memory>
#include <future>

#include <storage/io_request.hh>
#include <storage/io_mgr.hh>

namespace springtail {

    /**
     * @brief External IO interface.  
     * @details IOHandle is a virtual FH, it may have no
     * real (system FH) backing it.  Late binding to system FH done when IO call 
     * is made.  Retrieved via IOMgr::open()
     */
    class IOHandle {
    private:
        std::filesystem::path _path;  //!< underlying file path
        IOMgr::IO_MODE _mode;         //!< open mode (read/write/append)
        bool _is_compressed;          //!< is the file compressable (append)

    public:
        /**
         * @brief Construct a new IOHandle object
         * @param path file path
         * @param mode open mode (read/write/append)
         * @param is_compressed is file compressable
         */
        IOHandle(std::filesystem::path path, IOMgr::IO_MODE mode, bool is_compressed) :
            _path(path), _mode(mode), _is_compressed(is_compressed) {};

        /**
         * @brief Destroy the IOHandle object
         */
        ~IOHandle() {};

        /** maximum number of vectors per call */
        static constexpr int MAX_VECTORS=8;

        /**
         * @brief Get the open mode 
         * @return IOMgr::IO_MODE mode ()
         */
        IOMgr::IO_MODE get_mode() { return _mode; }

        /**
         * @brief Get the path
         * @return std::string path
         */
        std::string get_path() { return _path.string(); }

        // async operations; calls callback

        /**
         * @brief Asynchronous read
         * @param pos       Read offset; must be at a block boundary
         * @param callback  Optional callback; pass {} for empty callback
         * @return std::future<std::shared_ptr<IOResponseRead>> Future containing a IOResponseRead ptr
         */        
        std::future<std::shared_ptr<IOResponseRead>>
        async_read(uint64_t pos, io_read_callback_fn callback) const;

        /**
         * @brief Asynchronous append; append data to end of file
         * @param buffer    Buffer to writeout
         * @param length    Length of buffer
         * @param callback  Optional callback; pass {} for empty callback
         * @return std::future<std::shared_ptr<IOResponseAppend>> Future containing a IOResponseAppend ptr
         */
        std::future<std::shared_ptr<IOResponseAppend>>
        async_append(const char *buffer, int length, io_append_callback_fn callback) const;

        /**
         * @brief Asynchronous append; append data to end of file
         * @param data      Data to be written out
         * @param callback  Optional callback; pass {} for empty callback
         * @return std::future<std::shared_ptr<IOResponseAppend>> Future containing a IOResponseAppend ptr
         */        
        std::future<std::shared_ptr<IOResponseAppend>>
        async_append(std::shared_ptr<std::vector<char>> data, io_append_callback_fn callback) const;

        /**
         * @brief Asynchronous append; append data to end of file
         * @param data      Data to be written out; array of vectors
         * @param count     Number of vectors in data array
         * @param callback  Optional callback; pass {} for empty callback
         * @return std::future<std::shared_ptr<IOResponseAppend>> Future containing a IOResponseAppend ptr
         */
        std::future<std::shared_ptr<IOResponseAppend>>
        async_append(std::shared_ptr<std::vector<char>> data[], uint8_t count, io_append_callback_fn callback) const;

        /**
         * @brief Asynchronous append; append data to end of file
         * @param data      Data to be written out; vector of char vectors
         * @param callback  Optional callback; pass {} for empty callback
         * @return std::future<std::shared_ptr<IOResponseAppend>> Future containing a IOResponseAppend ptr
         */
        std::future<std::shared_ptr<IOResponseAppend>>
        async_append(const std::vector<std::shared_ptr<std::vector<char>>> &data, io_append_callback_fn callback) const;

        /**
         * @brief Asynchronous write; overwrite data at offset
         * @param offset    Offset at which to write data
         * @param data      Data to be written out; char vector ptr
         * @param callback  Optional callback; pass {} for empty callback
         * @return std::future<std::shared_ptr<IOResponseWrite>> Future containing a IOResponseWrite ptr
         */
        std::future<std::shared_ptr<IOResponseWrite>>
        async_write(uint64_t offset, std::shared_ptr<std::vector<char>> data, io_write_callback_fn callback) const;

        /**
         * @brief Asynchronous write; overwrite data at offset
         * @param offset    Offset at which to write data
         * @param data      Data to be written out; vector of char vector ptrs
         * @param callback  Optional callback; pass {} for empty callback
         * @return std::future<std::shared_ptr<IOResponseWrite>> Future containing a IOResponseWrite ptr
         */
        std::future<std::shared_ptr<IOResponseWrite>>
        async_write(uint64_t offset, std::vector<std::shared_ptr<std::vector<char>>> data, io_write_callback_fn callback) const;

        /**
         * @brief Asynchronous sync; sync data to disk
         * @param callback  Optional callback; pass {} for empty callback
         * @return std::future<std::shared_ptr<IOResponse>> Future containing a IOResponse ptr
         */
        std::future<std::shared_ptr<IOResponse>>
        async_sync(io_status_callback_fn callback) const;

        // sync operations, no callback blocks until completion

        /**
         * @brief Synchronous read; calls async read; blocks on future
         * @param pos       Read offset; must be at a block boundary
         * @return std::shared_ptr<IOResponseRead> IOResponseRead ptr
         */        
        std::shared_ptr<IOResponseRead>
        read(uint64_t pos) const;

        /**
         * @brief Synchronous append; calls async append; blocks on future
         * @param buffer  Buffer to append to end of file
         * @param length  Length of buffer
         * @return std::shared_ptr<IOResponseAppend> IOResponseAppend ptr
         */
        std::shared_ptr<IOResponseAppend>
        append(const char *buffer, int length) const;
        
        /**
         * @brief Synchronous append; calls async append; blocks on future
         * @param data    Data vector; char vector ptr
         * @return std::shared_ptr<IOResponseAppend> IOResponseAppend ptr
         */
        std::shared_ptr<IOResponseAppend>
        append(std::shared_ptr<std::vector<char>> data) const;

        /**
         * @brief Synchronous append; calls async append; blocks on future
         * @param data    Array of data vectors
         * @param count   Number of vectors in array
         * @return std::shared_ptr<IOResponseAppend> IOResponseAppend ptr
         */
        std::shared_ptr<IOResponseAppend>
        append(std::shared_ptr<std::vector<char>> data[], uint8_t count) const;

        /**
         * @brief Synchronous append; calls async append; blocks on future
         * @param data    Vector of data vectors; vector of char vector ptrs
         * @return std::shared_ptr<IOResponseAppend> IOResponseAppend ptr
         */
        std::shared_ptr<IOResponseAppend>
        append(const std::vector<std::shared_ptr<std::vector<char>>> &data) const;

        /**
         * @brief Synchronous write (overwrite); calls async read; blocks on future
         * @param offset   Offset to write at
         * @param data     Data to write; char vector
         * @return std::shared_ptr<IOResponseWrite> IOResopnseWrite ptr
         */
        std::shared_ptr<IOResponseWrite>
        write(uint64_t offset, std::shared_ptr<std::vector<char>> data) const;

        /**
         * @brief Synchronous write (overwrite); calls async read; blocks on future
         * @param offset   Offset to write at
         * @param data     Data to write; vector of char vectors
         * @return std::shared_ptr<IOResponseWrite> IOResopnseWrite ptr
         */
        std::shared_ptr<IOResponseWrite>
        write(uint64_t offset, std::vector<std::shared_ptr<std::vector<char>>> data) const;

        /**
         * @brief Synchronous sync; sync data to disk; calls async sync; waits on future
         * @return std::shared_ptr<IOResponse> IOResponse ptr
         */
        std::shared_ptr<IOResponse>
        sync() const;

        // no close, FH cache closes handles as necessary
    };
}
