#include <string>
#include <memory>
#include <functional>
#include <iostream>
#include <vector>
#include <cstdio>
#include <cassert>

#include <storage/io_file.hh>
#include <storage/exception.hh>
#include <storage/compressors.hh>

namespace springtail {

    static const char *MODE_WRITE = "w+b";
    static const char *MODE_READ = "rb";

    static const char HDR_MAGIC_COMPRESSED[3] = { 'C', 'X', 'T'};
    static const char HDR_MAGIC_UNCOMPRESSED[3] = { 'U', 'X', 'T'};

    std::shared_ptr<IOSysFH>
    IOFile::_get_read_fh(const IOMgr::IO_MODE &mode)
    {
        // get a read filehandle
        for (auto fh: _read_fhs) {
            if (!fh->is_busy) {
                fh->is_busy = true;
                return fh;
            }
        }

        // nothing free, see if we can allocate new FH
        if (_read_fhs.size() < IOMgr::MAX_FILE_HANDLES_PER_FILE) {
            std::shared_ptr<IOSysFH> file = std::make_shared<IOSysFH>(_path, mode, _is_compressed);
            _read_fhs.push_back(file);
            return file;
        }

        return nullptr;
    }


    std::shared_ptr<IOSysFH>
    IOFile::_get_write_fh(const IOMgr::IO_MODE &mode)
    {
        // get a write filehandle
        if (_write_fh != nullptr && !_write_fh->is_busy) {
            _write_fh->is_busy = true;
            return _write_fh;
        }

        if (_write_fh == nullptr) {
            // allocate write fh
            _write_fh = std::make_shared<IOSysFH>(_path, mode, _is_compressed);
            return _write_fh;
        }

        return nullptr;
    }


    std::shared_ptr<IOSysFH>
    IOFile::get_fh(const IOMgr::IO_MODE &mode)
    {
        // lock file object
        std::unique_lock<std::mutex> lock(_mutex);

        std::shared_ptr<IOSysFH> file = nullptr;

        while (true) {
            if (mode == IOMgr::IO_MODE::READ) {
                file = _get_read_fh(mode);
            } else {
                file = _get_write_fh(mode);
            }

            // wasn't able to get the file handle, block
            if (file != nullptr) {
                _in_use_count++;
                return file;
            }

            // block on cv
            if (mode == IOMgr::IO_MODE::READ) {
                _cv_read.wait(lock);
            } else {
                _cv_write.wait(lock);
            }
        }
    }


    void
    IOFile::put_fh(std::shared_ptr<IOSysFH> fh)
    {
        // lock file object
        std::unique_lock<std::mutex> lock(_mutex);

        assert(fh->is_busy == true);

        fh->is_busy = false;
        _in_use_count--;

        assert(_in_use_count >= 0);

        if (fh->is_readonly()) {
            _cv_read.notify_one();
        } else {
            _cv_write.notify_one();
        }
    }


    /**
     * @brief Try to close all open fhs; called from IOMgr evict callback
     *        with IOMgr::_cache_mutex locked.
     * @return true on success, false otherwise
     */
    bool
    IOFile::try_close_all()
    {
        if (in_use()) {
            return false;
        }

        // try lock -- don't lock immediately
        std::unique_lock<std::mutex> lock(_mutex, std::defer_lock);
        if (!lock.try_lock()) {
            // not locked
            assert(0); // should not happen
            return false;
        }

        // locked file object

        // close all open FHs
        while (!_read_fhs.empty()) {
            std::shared_ptr<IOSysFH> fh = _read_fhs.back();
            assert(fh->is_busy == false);
            _read_fhs.pop_back();
            fh->close();
        }

        // close write fh
        if (_write_fh != nullptr) {
            _write_fh->close();
            _write_fh = nullptr;
        }

        return true;
    }


    IOSysFH::IOSysFH(const std::filesystem::path &path, const IOMgr::IO_MODE &mode, bool is_compressed) :
        _path(path),
        _file(nullptr),
        _is_compressed(is_compressed),
        _is_dirty(false),
        is_busy(false)
    {
        const char *fmode;
        if (mode == IOMgr::IO_MODE::READ) {
            _is_readonly = true;
            fmode = MODE_READ;
        } else {
            _is_readonly = false;
            fmode = MODE_WRITE;
        }

        _file = fopen(path.c_str(), fmode);
        if (_file == nullptr) {
            throw StorageError("Error opening file");
        }
    }


    IOSysFH::~IOSysFH()
    {
        if (_file != nullptr) {
            fclose(_file);
        }
    }


    void
    IOSysFH::close()
    {
        if (_file != nullptr) {
            fclose(_file);
            _file = nullptr;
        }
    }


    /**
     * @brief Read data from offset pos
     * @details Data stored in following format:
     *     Header 8 Bytes:
     *       0-2  (3B) Magic number (CXT for compressed, or UXT for uncompressed)
     *       3    (1B) Vector count (number of vectors that make up this block)
     *     Data Vector:
     *       0-3  (4B) Size of vector uncompressed
     *       4-8  (4B) Size of vector (compressed size if compressed)
     *       ...       Data of size mentioned above
     *
     * @param pos          offset to read from
     * @param decompressor Decompressor class for decompression
     * @param callback     callback for completion
     */
    void
    IOSysFH::read(uint64_t pos,
                  std::shared_ptr<Decompressor> decompressor,
                  io_read_callback_fn callback)
    {
        char hdr[8];
        uint8_t count;
        uint32_t size;

        if (std::fseek(_file, pos, SEEK_SET) != 0) {
            throw StorageError();
        }

        if (std::fread(hdr, 1, 8, _file) != 8) {
            throw StorageError();
        }

        bool is_compressed = false;
        if (hdr[0] == HDR_MAGIC_COMPRESSED[0]) {
            if (hdr[1] != HDR_MAGIC_COMPRESSED[1] || hdr[2] != HDR_MAGIC_COMPRESSED[2]) {
                throw StorageError();
            }
            is_compressed = true;
        } else if (hdr[0] == HDR_MAGIC_UNCOMPRESSED[0]) {
            if (hdr[1] != HDR_MAGIC_UNCOMPRESSED[1] || hdr[2] != HDR_MAGIC_UNCOMPRESSED[2]) {
                throw StorageError();
            }
        } else {
            throw StorageError();
        }

        assert(_is_compressed == is_compressed);

        // vector count
        count = hdr[3];

        // output vector
        std::vector<std::shared_ptr<std::vector<char>>> data(count);

        // temp buffer
        std::vector<char> compressed_data;

        uint32_t csize;
        for (int i = 0; i < count; i++) {
            // read uncompressed size
            std::fread(reinterpret_cast<char *>(&size), 1, sizeof(uint32_t), _file);

            // read vector size (compressed size if compressed)
            std::fread(reinterpret_cast<char *>(&csize), 1, sizeof(uint32_t), _file);

            // output buffer
            std::shared_ptr<std::vector<char>> data_ptr = std::make_shared<std::vector<char>>(size);
            
            if (is_compressed) {
                // resize temp buffer
                compressed_data.resize(csize);

                // read compressed data and decompress
                std::fread(compressed_data.data(), 1, csize, _file);
                decompressor->decompress_raw(compressed_data, data_ptr, 0);
            } else {
                std::fread(data_ptr->data(), 1, size, _file);
            }

            data.push_back(data_ptr);
        }

        return;
    }


    /**
     * @brief Append data to end of file, file may be compressed or not
     *
     * @param data       vector of data vectors to write out
     * @param compressor Compressor class to compress file
     * @param callback   callback for completion
     */
    void
    IOSysFH::append(const std::vector<std::shared_ptr<std::vector<char>>> &data,
                    std::shared_ptr<Compressor> compressor,
                    io_write_callback_fn callback)
    {
        if (std::fseek(_file, 0, SEEK_END) != 0) {
            throw StorageError();
        }

        //uint64_t offset = std::ftell(_file);
        if (data.size() == 0) {
            return;
        }

        // do it in two passes to avoid partial writes
        // first, compress data and compute total uncompressed size
        
        std::vector<char> compressed_data[data.size()];
        for (int i = 0; i < data.size(); i++) {
            compressor->compress_raw(data[i], compressed_data[i]);
        }

        if (_is_compressed) {
            compressor->reset_stream();
        }

        // write out data
        char hdr[8];

        // header and number of vectors
        std::copy_n((_is_compressed) ? HDR_MAGIC_COMPRESSED : HDR_MAGIC_UNCOMPRESSED, 3, &hdr[0]);
        hdr[3] = data.size();
        std::fwrite(hdr, 1, 4, _file);

        // write out compressed data
        for (int i = 0; i < data.size(); i++) {
            // size of uncompressed data 4B            
            uint32_t size = data[i]->size();
            std::copy_n(reinterpret_cast<char *>(&size), sizeof(int32_t), &hdr[0]);

            if (_is_compressed) {
                uint32_t csize = compressed_data[i].size();
                std::copy_n(reinterpret_cast<char *>(&csize), sizeof(int32_t), &hdr[4]);
                std::fwrite(&hdr[0], 1, 8, _file);
                std::fwrite(compressed_data[i].data(), 1, csize, _file);
            } else {
                // uncompressed
                std::copy_n(reinterpret_cast<char *>(&size), sizeof(int32_t), &hdr[4]);
                std::fwrite(&hdr[0], 1, 8, _file);                
                std::fwrite(data[i]->data(), 1, size, _file);
            }
        }

        // flush to kernel (this does not do a sync)
        std::fflush(_file);

        _is_dirty = true;

        return;
    }


    /**
     * @brief Overwrite data within a file, file MUST NOT be compressed
     *
     * @param offset     offset at which to write data
     * @param data       vector of data vectors to write out (written out as one block)
     * @param callback   callback for completion
     */
    void
    IOSysFH::write(uint64_t offset,
                   const std::vector<std::shared_ptr<std::vector<char>>> &data,
                   io_write_callback_fn callback)
    {
        assert(_is_compressed == false);

        if (std::fseek(_file, offset, SEEK_SET) != 0) {
            throw StorageError();
        }

        if (data.size() == 0) {
            return;
        }

        // write out header
        // 3B Magic + 1B vector count + Per vector: 4B total size + 4B vector size + data
        char hdr[8];

        // header and number of vectors; map to a single vector
        std::copy_n(HDR_MAGIC_UNCOMPRESSED, 3, &hdr[0]);
        hdr[3] = data.size();
        std::fwrite(hdr, 1, 4, _file);

        for (int i = 0; i < data.size(); i++) {
            uint32_t size = data[i]->size();
            std::copy_n(reinterpret_cast<char *>(&size), sizeof(int32_t), &hdr[0]);
            std::copy_n(reinterpret_cast<char *>(&size), sizeof(int32_t), &hdr[4]);
            std::fwrite(hdr, 1, 8, _file);
            std::fwrite(data[i]->data(), 1, data[i]->size(), _file);
        }

        std::fflush(_file);

        _is_dirty = true;

        return;
    }


    /**
     * @brief Sync data to disk
     *
     * @param callback callback for completion
     */
    void IOSysFH::sync(io_status_callback_fn callback)
    {
        if (!_is_dirty) {
            return;
        }

        if (std::fflush(_file) != 0) {
            throw StorageError();
        }

        int fd = ::fileno(_file);
        if (fd < 0) {
            throw StorageError();
        }

        ::fsync(fd);
        callback(IOStatus::SUCCESS);

        return;
    }
}
