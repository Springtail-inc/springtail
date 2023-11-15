#include <string>
#include <memory>
#include <functional>
#include <iostream>
#include <vector>
#include <cassert>
#include <cerrno>
#include <fcntl.h>
#include <unistd.h>
#include <sys/uio.h>

#include <storage/io_file.hh>
#include <storage/exception.hh>
#include <storage/compressors.hh>

namespace springtail {

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
            std::shared_ptr<IOSysFH> fh = std::make_shared<IOSysFH>(_path, mode, _is_compressed);
            _read_fhs.push_back(fh);
            fh->is_busy = true;
            return fh;
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
            _write_fh->is_busy = true;
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
            // get fh based on mode
            if (mode == IOMgr::IO_MODE::READ) {
                file = _get_read_fh(mode);
            } else {
                file = _get_write_fh(mode);
            }

            // got the file, incr use count and return
            if (file != nullptr) {
                _in_use_count++;
                return file;
            }

            // wasn't able to get the file handle, block on cv
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
        _fd(-1),
        _is_compressed(is_compressed),
        _is_dirty(false),
        is_busy(false)
    {
        int fmode;
        if (mode == IOMgr::IO_MODE::READ) {
            _is_readonly = true;
            fmode = O_RDONLY | O_CREAT;
        } else if (mode == IOMgr::IO_MODE::APPEND) {
            _is_readonly = false;
            fmode = O_RDWR | O_APPEND | O_CREAT;
        } else {
            _is_readonly = false;
            fmode = O_RDWR | O_CREAT;
        }

        _fd = ::open(path.c_str(), fmode);
        if (_fd == -1) {
            throw StorageError("Error opening file");
        }
    }


    IOSysFH::~IOSysFH()
    {
        close();
    }


    void
    IOSysFH::close()
    {
        if (_fd != -1) {
            ::close(_fd);
            _fd = -1;
        }
    }

    /**
     * Debugging dump header in readable format
     */
    static void 
    dump_hdr(char *hdr, int len)
    {
        std::cout << "Header\n";
        std::cout << "  Magic: " << hdr[0] << hdr[1] << hdr[2] << (hdr[0] == 'C' ? " (compressed)" : " (uncompressed)") << std::endl;
        std::cout << "  Count: " << std::hex << (0xFF & (uint8_t)hdr[3]) << std::dec << std::endl;
        assert(len == hdr[3]*8 + 4);

        uint32_t size, csize;
        int hdr_off = 4;
        for (int i=0; i < hdr[3]; i++) {
            std::copy_n(&hdr[hdr_off], sizeof(uint32_t), reinterpret_cast<char *>(&size));
            std::copy_n(&hdr[hdr_off+4], sizeof(uint32_t), reinterpret_cast<char *>(&csize));
            std::cout << "  Size: " << size << " CSize: " << csize << std::endl;
            hdr_off += 8;
        }
    }


    /**
     * @brief Read data from offset pos
     * @details Data stored in following format:
     *     Header 8 Bytes:
     *       0-2  (3B) Magic number (CXT for compressed, or UXT for uncompressed)
     *       3    (1B) Vector count (number of vectors that make up this block)
     *     Per data vector:
     *       0-3  (4B) Size of vector uncompressed
     *       4-8  (4B) Size of vector (compressed size if compressed)
     *     Data:
     *       ...       Data of size mentioned above
     *
     * @param pos          offset to read from
     * @param decompressor Decompressor class for decompression
     * @param callback     callback for completion
     */
    void
    IOSysFH::read(std::shared_ptr<IORequestRead> request,
                  std::shared_ptr<Decompressor> decompressor)
    {    
        char hdr[4 + 8 * IOHandle::MAX_VECTORS];  // 4B HDR + 8B per vector * 8 (prefetch)

        // default error response
        std::shared_ptr<IOResponseRead> response = std::make_shared<IOResponseRead>(request->path);

        std::cout << "IOSysFH::read offset=" << request->offset << std::endl;

        response->offset = request->offset;

        // prefetch 36 bytes to try to avoid multiple reads
        // this may read past the end of file, so must handle that case
        int hdr_read = ::pread(_fd, hdr, sizeof(hdr), request->offset);
        if (hdr_read <= 0) {
            request->complete(response, errno);
            return;
        }
        assert(hdr_read >= 12);

        // verify header and determine if block is compressed
        bool is_compressed = false;
        if (hdr[0] == HDR_MAGIC_COMPRESSED[0]) {
            if (hdr[1] != HDR_MAGIC_COMPRESSED[1] || hdr[2] != HDR_MAGIC_COMPRESSED[2]) {
                response->set_status(IOStatus::ERR_DECODE);
                request->complete(response);
                return;
            }
            is_compressed = true;
        } else if (hdr[0] == HDR_MAGIC_UNCOMPRESSED[0]) {
            if (hdr[1] != HDR_MAGIC_UNCOMPRESSED[1] || hdr[2] != HDR_MAGIC_UNCOMPRESSED[2]) {
                response->set_status(IOStatus::ERR_DECODE);
                request->complete(response);
                return;
            }
        } else {
            response->set_status(IOStatus::ERR_DECODE);
            request->complete(response);
            return;
        }

        // compressed files can have uncompressed blocks,
        // but uncompressed files should never have compressed blocks
        assert (!(_is_compressed == false && is_compressed == true));

        // vector count
        uint8_t count = hdr[3];
        assert(count <= IOHandle::MAX_VECTORS);

        dump_hdr(hdr, 4 + 8 * count);

        int hdr_off = 4;

        std::cout << "IOSysFH::read vector count=" << (0xFF & count) << std::endl;

        // output vector
        response->data.resize(count);

        // temp buffer for compressed data
        std::vector<std::vector<char>> compressed_data;

        if (is_compressed) {
            compressed_data.resize(count);
        }

        // loop through setting up buffers for read
        uint32_t size;
        uint32_t csize;
        uint32_t total_size=0;
        struct iovec iov[count];

        // construct the iovec for the read
        for (int i = 0; i < count; i++) {
            // decode the sizes from the header previously read in
            std::copy_n(&hdr[hdr_off], sizeof(uint32_t), reinterpret_cast<char *>(&size));
            std::copy_n(&hdr[hdr_off+4], sizeof(uint32_t), reinterpret_cast<char *>(&csize));

            hdr_off += 8;
            assert(hdr_off <= (4 + 8 * count));

            // generate the output vector of the correct size
            std::shared_ptr<std::vector<char>> data_ptr = std::make_shared<std::vector<char>>(size);
            response->data[i] = data_ptr;

            if (is_compressed) {
                // add vector to compressed data list
                compressed_data[i].resize(csize);

                iov[i].iov_base = compressed_data[i].data();
                iov[i].iov_len = csize;
            } else {
                iov[i].iov_base = data_ptr->data();
                iov[i].iov_len = size;
            }

            std::cout << "IOSysFH::read (" << (is_compressed ? "compressed" : "uncompressed") << ") Vector: " << i << " size=" << size << " csize=" << csize << std::endl;

            total_size += iov[i].iov_len;
        }

        // issue the read
        int bytes_read = preadv(_fd, iov, count, request->offset + hdr_off);
        if (bytes_read == -1) {
            request->complete(response, errno);
            return;
        }
        assert(bytes_read == total_size);

        // if data was compressed we need to uncompress it into final location, otherwise we are done
        if (is_compressed) {
            for (int i = 0; i < count; i++) {
                decompressor->decompress_raw(compressed_data[i], response->data[i], 0);
            }
        }

        std::cout << "Read " << response->data.size() << " vectors\n";

        response->next_offset = request->offset + hdr_off + total_size;
        request->complete(response, 0);

        return;
    }

    int
    IOSysFH::internal_write(std::vector<std::shared_ptr<std::vector<char>>> &data,
                            std::shared_ptr<Compressor> compressor,
                            uint64_t offset,
                            bool is_compressed)
    {
        uint8_t count = data.size();
        assert(count <= IOHandle::MAX_VECTORS);

        // do it in two passes to avoid partial writes
        // first, compress data and compute total uncompressed size
        std::vector<char> compressed_data[count];
        
        if (is_compressed) {
            uint32_t compressed_size = 0;
            uint32_t size = 0;

            compressor->reset_stream();
            for (int i = 0; i < count; i++) {
                compressor->compress_raw(data[i], compressed_data[i]);
                compressed_size += compressed_data[i].size();
                size += data[i]->size();
            }

            // if compression isn't helping then don't compress
            if (size <= compressed_size) {
                // don't compress
                is_compressed = false;

                std::cout << "IOSys::internal_write: Not compressing data, compressed size too big: " 
                          << compressed_size << " vs " << size << std::endl;
            }
        }

        // write out header data
        char hdr[4 + 8 * count];

        // header and number of vectors
        std::copy_n((is_compressed) ? HDR_MAGIC_COMPRESSED : HDR_MAGIC_UNCOMPRESSED, 3, &hdr[0]);
        hdr[3] = count;
        
        // fill in header and construct iovec for write
        int hdr_off = 4;
        uint32_t total_size = 4 + 8 * count;
        struct iovec iov[count+1];
        iov[0].iov_base = hdr;
        iov[0].iov_len = 4 + 8 * count;

        for (int i = 0; i < count; i++) {
            uint32_t size = data[i]->size();
            std::copy_n(reinterpret_cast<char *>(&size), sizeof(int32_t), &hdr[hdr_off]);

            if (is_compressed) {
                uint32_t csize = compressed_data[i].size();
                std::copy_n(reinterpret_cast<char *>(&csize), sizeof(int32_t), &hdr[hdr_off + 4]);                
                iov[i+1].iov_base = compressed_data[i].data();
                iov[i+1].iov_len = csize;

                std::cout << "IOSysFH::internal_write (compressed); idx=" << i << ", size=" << size << ", csize=" << csize << std::endl;
            } else {
                std::copy_n(reinterpret_cast<char *>(&size), sizeof(int32_t), &hdr[hdr_off + 4]);
                iov[i+1].iov_base = data[i]->data();
                iov[i+1].iov_len = size;
            
                std::cout << "IOSysFH::internal_write (uncompressed); idx=" << i << " size=" << size << std::endl;
            }

            total_size += iov[i+1].iov_len;
            hdr_off += 8;
            assert(hdr_off <= (4 + 8 * count));
        }

        dump_hdr(hdr, hdr_off);

        // do the write
        int bytes_written = ::pwritev(_fd, iov, count+1, offset);
        if (bytes_written > 0) {
            assert(bytes_written == total_size);
        }
        
        return bytes_written;
    }


    /**
     * @brief Append data to end of file, file may be compressed or not
     *
     * @param data       vector of data vectors to write out
     * @param compressor Compressor class to compress file
     * @param callback   callback for completion
     */
    void
    IOSysFH::append(std::shared_ptr<IORequestAppend> request, 
                    std::shared_ptr<Compressor> compressor) 
    {
        // default error response
        std::shared_ptr<IOResponseAppend> response = std::make_shared<IOResponseAppend>(request->path);
        
        // file should have been opened for append only so this shouldn't be strictly necessary
        uint64_t offset = ::lseek(_fd, 0, SEEK_END);
        if ((off_t)offset == -1) {
            request->complete(response, errno);
            return;
        }

        response->offset = offset;
        if (request->data.size() == 0) {
            response->next_offset = offset;
            request->complete(response, IOStatus::SUCCESS);
        }

        // issue write
        int bytes_written = internal_write(request->data, compressor, offset, _is_compressed);
        if (bytes_written == -1) {
            request->complete(response, errno);
            return;
        }

        _is_dirty = true;

        response->next_offset = offset + bytes_written;
        request->complete(response, IOStatus::SUCCESS);
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
    IOSysFH::write(std::shared_ptr<IORequestWrite> request)
    {
        assert(_is_compressed == false);
        
        // default error response
        std::shared_ptr<IOResponseWrite> response = std::make_shared<IOResponseWrite>(request->path);

        response->offset = request->offset;
        if (request->data.size() == 0) {
            response->next_offset = request->offset;            
            request->complete(response, IOStatus::SUCCESS);
            return;
        }

        // issue write
        int bytes_written = internal_write(request->data, nullptr, request->offset, false);
        if (bytes_written == -1) {
            request->complete(response, errno);
            return;
        }

        _is_dirty = true;

        response->next_offset = request->offset + bytes_written;
        request->complete(response, IOStatus::SUCCESS);
        return;
    }


    /**
     * @brief Sync data to disk
     *
     * @param callback callback for completion
     */
    void IOSysFH::sync(std::shared_ptr<IORequestSync> request)
    {
        // default success response
        std::shared_ptr<IOResponse> response = 
            std::make_shared<IOResponse>(IORequest::IOType::SYNC, request->path);
        
        if (!_is_dirty) {
            request->complete(response, IOStatus::SUCCESS);
            return;
        }

        if (_fd < 0) {
            request->complete(response, errno);
            return;
        }

        ::fsync(_fd);

        request->complete(response, IOStatus::SUCCESS);

        return;
    }
}
