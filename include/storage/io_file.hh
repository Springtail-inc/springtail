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
#include <storage/io_mgr.hh>
#include <storage/exception.hh>
#include <storage/compressors.hh>

namespace springtail {

    /**
     * @brief System file handle; holds underlying file descriptor
     */
    class IOSysFH {
    private:
        std::filesystem::path _path;  ///< underlying filename
        int  _fd;                     ///< underlying file descriptor
        bool _is_compressed;          ///< is underlying file compressable (append)
        bool _is_dirty;               ///< is fh dirty (unsynced writes)
        bool _is_readonly;            ///< is fh opened read-only

        /**
         * @brief Internal write used by write (overwrite) and append
         * @param data           Vector containing pointers to data vectors
         * @param compressor     Compressor if compression is going to be used
         * @param offset         Offset to write at (either EOF for append or other for write)
         * @param is_compressed  Should compression be attempted
         * @return int           Number of bytes written (hdr + data);
         *                       > 0 success, -1 io error with errno set, -2 decode error
         */
        int _internal_write(std::vector<std::shared_ptr<std::vector<char>>> &data,
                            std::shared_ptr<Compressor> compressor,
                            uint64_t offset, bool is_compressed);

        /**
         * @brief Compute hash over vector of vector (hash of hashes)
         * @param data vector of data vectors
         * @return uint64_t hash (computed as hash of hashes)
         */
        uint64_t _compute_hash(std::vector<std::shared_ptr<std::vector<char>>> data);

    public:
        std::atomic<bool> is_busy;  ///< indicates fh is busy with io for a worker

        /**
         * @brief Construct a new IOSysFH::IOSysFH object based on path, mode and whether
         *        file is compressable
         * @param path  Path to open
         * @param mode  Open mode, read/write/append
         * @param is_compressed Is file compressable
         */
        IOSysFH(const std::filesystem::path &path, const IOMgr::IO_MODE &mode, bool is_compressed);

        /**
         * @brief Destroy the IOSysFH::IOSysFH object; close underlying fd
         */
        ~IOSysFH();

        /**
         * @brief Read data from offset pos
         * @param pos          offset to read from
         * @param decompressor Decompressor class for decompression
         * @param callback     callback for completion
         */
        void read(IORequestRead * const request, std::shared_ptr<Decompressor> decompressor);

        /**
         * @brief Overwrite data within a file, file MUST NOT be compressed
         * @param offset     offset at which to write data
         * @param data       vector of data vectors to write out (written out as one block)
         * @param callback   callback for completion
         */
        void write(IORequestWrite * const request);

        /**
         * @brief Append data to end of file, file may be compressed or not
         * @param data       vector of data vectors to write out
         * @param compressor Compressor class to compress file
         * @param callback   callback for completion
         */
        void append(IORequestAppend *const request, std::shared_ptr<Compressor> compressor);

        /**
         * @brief Sync data to disk
         * @param callback callback for completion
         */
        void sync(IORequestSync * const request);

        /**
         * @brief Close underlying system file handle
         */
        void close();

        /**
         * @brief Is FH opened read-only
         * @return true if FH is readonly
         * @return false if FH is writeable
         */
        bool is_readonly() { return _is_readonly; }
    };


    /**
     * @brief Cacheable file object, references a single file.  Holds multiple system
     *        file handles for read, and a single fh for write.  Provides access to the
     *        underlying file handles.
     */
    class IOFile {
    private:
        /** file path for underlying file */
        std::filesystem::path _path;

        /** is file compressable (append files) */
        bool _is_compressed;

        /** count worker threads with reference to file object; protected by IOMgr::_cache_mutex */
        std::atomic<int> _in_use_count;

        /** Mutex protects _cv_read, _cv_write condition variables */
        std::mutex _mutex;

        /** Read condition variable; used to block when waiting for a read fh */
        std::condition_variable _cv_read;

        /** Write condition variable; used to block when waiting for a write fh */
        std::condition_variable _cv_write;

        /** List of read filehandles */
        std::vector<std::shared_ptr<IOSysFH>> _read_fhs;

        /** Single write filehande */
        std::shared_ptr<IOSysFH> _write_fh;

        /**
         * @brief Get read fh, creates file handle if it doesn't exist
         * @param mode Mode read (used for FH creation)
         * @return std::shared_ptr<IOSysFH> System filehandle or nullptr if none available
         */
        std::shared_ptr<IOSysFH> _get_read_fh(const IOMgr::IO_MODE &mode);

        /**
         * @brief Get write filehandle, creates it if it doesn't exist
         * @param mode Mode write or append (used for FH creation)
         * @return std::shared_ptr<IOSysFH> System filehandle or nullptr if busy
         */
        std::shared_ptr<IOSysFH> _get_write_fh(const IOMgr::IO_MODE &mode);

    public:
        /**
         * @brief Construct a new IOFile object
         * @param path          Path for underlying file
         * @param is_compressed Is file compressable
         */
        IOFile(const std::filesystem::path &path, bool is_compressed)
            : _path(path), _is_compressed(is_compressed), _in_use_count(0)
        {}

        /**
         * @brief Indicates file object is in use by at least one worker
         * @return true  file object is in use (at least one FH in use)
         * @return false file object is not in use
         */
        inline bool in_use() { return _in_use_count > 0; }

        /**
         * @brief Atomically increment in use counter indicating a FH is in use
         *        Incremented by IOMgr::lookup
         */
        inline void incr_in_use() { _in_use_count++; }
        
        /**
         * @brief Atomically decrement in use counter; Decremented by IOMgr::release
         * 
         */
        inline void decr_in_use() { _in_use_count--; assert(_in_use_count >= 0); }

        /**
         * @brief Get FH based on mode; blocks if none available
         * @param mode Open mode for FH (read/write/append)
         * @return std::shared_ptr<IOSysFH> System filehandle (either read or write)
         */
        std::shared_ptr<IOSysFH> get_fh(const IOMgr::IO_MODE &mode);

        /**
         * @brief Release system FH; notifies anyone blocked waiting
         * @param fh System FH to release
         */
        void put_fh(std::shared_ptr<IOSysFH> fh);

        /**
         * @brief Try to close all open fhs; called from IOMgr evict callback
         *        with IOMgr::_cache_mutex locked.
         * @return true on success, false otherwise
         */
        bool try_close_all();
    };
}