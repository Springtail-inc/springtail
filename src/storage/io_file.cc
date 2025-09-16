#include <string>
#include <memory>
#include <vector>
#include <cerrno>
#include <fcntl.h>
#include <unistd.h>
#include <sys/uio.h>

#include <xxhash.h>

#include <absl/log/check.h>

#include <storage/io_file.hh>
#include <storage/io.hh>
#include <storage/exception.hh>
#include <storage/compressors.hh>

#include <common/logging.hh>

namespace springtail {

    std::shared_ptr<IOSysFH>
    IOFile::_get_read_fh()
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
            std::shared_ptr<IOSysFH> fh = std::make_shared<IOSysFH>(shared_from_this(), IOMgr::IO_MODE::READ, _is_compressed);
            _read_fhs.push_back(fh);
            fh->is_busy = true;
            return fh;
        }

        return nullptr;
    }

    std::shared_ptr<IOSysFH>
    IOFile::_get_append_fh()
    {
        // get a append filehandle
        for (auto fh: _append_fhs) {
            if (!fh->is_busy) {
                fh->is_busy = true;
                return fh;
            }
        }

        // nothing free, see if we can allocate new FH
        if (_append_fhs.size() < IOMgr::MAX_FILE_HANDLES_PER_FILE) {
            std::shared_ptr<IOSysFH> fh = std::make_shared<IOSysFH>(shared_from_this(), IOMgr::IO_MODE::APPEND, _is_compressed);
            _append_fhs.push_back(fh);
            fh->is_busy = true;
            return fh;
        }

        return nullptr;
    }


    std::shared_ptr<IOSysFH>
    IOFile::_get_write_fh()
    {
        // get a write filehandle
        if (_write_fh != nullptr && !_write_fh->is_busy) {
            _write_fh->is_busy = true;
            return _write_fh;
        }

        if (_write_fh == nullptr) {
            // allocate write fh
            _write_fh = std::make_shared<IOSysFH>(shared_from_this(), IOMgr::IO_MODE::WRITE, _is_compressed);
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
                file = _get_read_fh();
            } else if (mode == IOMgr::IO_MODE::APPEND) {
                file = _get_append_fh();
            } else {
                file = _get_write_fh();
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

        CHECK_EQ(fh->is_busy, true);

        fh->is_busy = false;
        _in_use_count--;

        DCHECK_GE(_in_use_count, 0);

        if (fh->is_readonly()) {
            _cv_read.notify_one();
        } else {
            _cv_write.notify_one();
        }
    }

    /*
     * Called with IOMgr::_cache_mutex locked
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
            DCHECK(false); // should not happen
            return false;
        }

        // locked file object

        // close all open FHs
        while (!_read_fhs.empty()) {
            std::shared_ptr<IOSysFH> fh = _read_fhs.back();
            CHECK_EQ(fh->is_busy, false);
            _read_fhs.pop_back();
            fh->close();
        }

        // close all open append FHs
        while (!_append_fhs.empty()) {
            std::shared_ptr<IOSysFH> fh = _append_fhs.back();
            CHECK_EQ(fh->is_busy, false);
            _append_fhs.pop_back();
            fh->close();
        }

        // close write fh
        if (_write_fh != nullptr) {
            _write_fh->close();
            _write_fh = nullptr;
        }

        return true;
    }


    IOSysFH::IOSysFH(std::shared_ptr<IOFile> io_file, const IOMgr::IO_MODE &mode, bool is_compressed) :
        _io_file(io_file),
        _fd(-1),
        _is_dirty(false),
        _mode(mode),
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

        // set ownership (user r/w; group r; other read)
        mode_t owner = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;

        auto path = _io_file->get_path();
        _fd = ::open(path.c_str(), fmode, owner);
        if (_fd == -1) {
            LOG_ERROR("Error opening file: path={}, errno={}", path.c_str(), errno);
            throw StorageError("Error opening file");
        }
    }


    IOSysFH::~IOSysFH()
    {
        close();
    }

    bool
    IOSysFH::is_compressed() const
    {
        return _io_file->is_compressed();
    }

    void
    IOSysFH::close()
    {
        if (_fd != -1) {
            ::close(_fd);
            _fd = -1;
        }
    }

#if 1
    /**
     * Debugging dump header in readable format
     */
    static void
    dump_hdr(char *hdr, int len)
    {
        std::cout << "Header\n";
        std::cout << "  Magic: " << hdr[0] << hdr[1] << hdr[2] << (hdr[0] == 'C' ? " (compressed)" : " (uncompressed)") << std::endl;
        std::cout << "  Count: " << std::hex << (0xFF & (uint8_t)hdr[3]) << std::dec << std::endl;
        std::cout << "  Hash : " << std::hex << *(uint64_t *)&hdr[4] << std::dec << std::endl;
        DCHECK_EQ(len, (hdr[3] * IOFile::VEC_HDR_SIZE + IOFile::EXT_HDR_SIZE));

        uint32_t size, csize;
        int hdr_off = IOFile::EXT_HDR_SIZE;
        for (int i=0; i < hdr[3]; i++) {
            std::copy_n(&hdr[hdr_off], sizeof(uint32_t), reinterpret_cast<char *>(&size));
            std::copy_n(&hdr[hdr_off+4], sizeof(uint32_t), reinterpret_cast<char *>(&csize));
            std::cout << "  Size : " << size << " CSize: " << csize << std::endl;
            hdr_off += IOFile::VEC_HDR_SIZE;
        }
    }
#endif


    uint64_t
    IOSysFH::_compute_hash(const std::vector<std::shared_ptr<std::vector<char>>> &data) const
    {
        int count = data.size();

        // fast path for 1
        if (count == 1) {
            return XXH64(data[0]->data(), data[0]->size(), 0);
        }

        DCHECK_LE(count, IOHandle::MAX_VECTORS);

        // otherwise generate hash of hashes
        char hashes[8 * IOHandle::MAX_VECTORS];

        for (int i=0; i < count; i++) {
            uint64_t hash = XXH64(data[i]->data(), data[i]->size(), 0);
            std::copy_n(reinterpret_cast<char *>(&hash), sizeof(uint64_t), &hashes[i*8]);
        }

        return XXH64(hashes, 8 * count, 0);
    }


    /*
     * Data stored in following format:
     *     Header 12 Bytes:
     *       0-2  (3B) Magic number (CXT for compressed, or UXT for uncompressed)
     *       3    (1B) Vector count (number of vectors that make up this block)
     *       4    (8B) xxHash (hash of hashes)
     *     Per data vector:
     *       0-3  (4B) Size of vector uncompressed
     *       4-8  (4B) Size of vector (compressed size if compressed)
     *     Data:
     *       ...       Data of size mentioned above
     */
    void
    IOSysFH::read(IORequestRead * const request,
                  std::shared_ptr<Decompressor> decompressor)
    {
        // 4B HDR + 8B hash +  8B per vector * 8 (prefetch)
        char hdr[IOFile::EXT_HDR_SIZE + IOFile::VEC_HDR_SIZE * IOHandle::MAX_VECTORS];

        // default error response
        std::shared_ptr<IOResponseRead> response = std::make_shared<IOResponseRead>(request);

        LOG_DEBUG(LOG_STORAGE, LOG_LEVEL_DEBUG1, "IOSysFH::read offset={}", request->offset);

        // prefetch 36 bytes to try to avoid multiple reads
        // this may read past the end of file, so must handle that case
        int hdr_read = ::pread(_fd, hdr, sizeof(hdr), request->offset);
        if (hdr_read <= 0) {
            request->complete(response, errno);
            return;
        }
        DCHECK_GE(hdr_read, 12);

        // verify header and determine if block is compressed
        bool is_compressed = false;
        bool decode_error = true;
        if (hdr[0] == IOFile::HDR_MAGIC_COMPRESSED[0] &&
            hdr[1] == IOFile::HDR_MAGIC_COMPRESSED[1] && hdr[2] == IOFile::HDR_MAGIC_COMPRESSED[2]) {
            decode_error = false;
            is_compressed = true;
        } else if (hdr[0] == IOFile::HDR_MAGIC_UNCOMPRESSED[0] &&
                   hdr[1] == IOFile::HDR_MAGIC_UNCOMPRESSED[1] && hdr[2] == IOFile::HDR_MAGIC_UNCOMPRESSED[2]) {
            decode_error = false;
            is_compressed = false;
        }

        if (decode_error) {
            // unable to decode header, return error response
            request->complete(response, IOStatus::ERR_DECODE);
            return;
        }

        // compressed files can have uncompressed blocks,
        // but uncompressed files should never have compressed blocks
        DCHECK (!(this->is_compressed() == false && is_compressed == true));

        // vector count
        uint8_t count = hdr[3];
        DCHECK_LE(count, IOHandle::MAX_VECTORS);

        // read the stored hash
        uint64_t hash;
        std::copy_n(&hdr[4], sizeof(uint64_t), reinterpret_cast<char *>(&hash));

        dump_hdr(hdr, IOFile::EXT_HDR_SIZE + IOFile::VEC_HDR_SIZE * count);

        int hdr_off = IOFile::EXT_HDR_SIZE;

        LOG_DEBUG(LOG_STORAGE, LOG_LEVEL_DEBUG1, "IOSysFH::read vector count={}", (0xFF & count));

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
        struct iovec iov[IOHandle::MAX_VECTORS];

        // construct the iovec for the read
        for (int i = 0; i < count; i++) {
            // decode the sizes from the header previously read in
            std::copy_n(&hdr[hdr_off], sizeof(uint32_t), reinterpret_cast<char *>(&size));
            std::copy_n(&hdr[hdr_off+4], sizeof(uint32_t), reinterpret_cast<char *>(&csize));

            hdr_off += IOFile::VEC_HDR_SIZE;
            DCHECK_LE(hdr_off, (IOFile::EXT_HDR_SIZE + IOFile::VEC_HDR_SIZE * count));

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

            LOG_DEBUG(LOG_STORAGE, LOG_LEVEL_DEBUG1, "IOSysFH::read ({}) Vector {}: size={} csize={}",
                         (is_compressed ? "compressed" : "uncompressed") , i, size, csize);

            total_size += iov[i].iov_len;
        }

        // issue the read
        int bytes_read = preadv(_fd, iov, count, request->offset + hdr_off);
        if (bytes_read == -1) {
            request->complete(response, errno);
            return;
        }
        CHECK_EQ(bytes_read, total_size);

        LOG_DEBUG(LOG_STORAGE, LOG_LEVEL_DEBUG1, "IOSysFH::read bytes read={}, hdr_off={}", bytes_read, hdr_off);

        // if data was compressed we need to decompress it into final location,
        // otherwise we are done
        if (is_compressed) {
            try {
                // iterate over vectors and decompress them
                for (int i = 0; i < count; i++) {
                    // decompress data
                    decompressor->decompress_raw(compressed_data[i], response->data[i], 0);
                }
            } catch (ValidationError &exc) {
                LOG_ERROR("Exception while decompressing data");
                request->complete(response, IOStatus::ERR_DECODE);
                return;
            }
        }

        // verify data hash; compute hash over response compare to hash in header
        uint64_t computed_hash = _compute_hash(response->data);
        if (computed_hash != hash) {
            LOG_ERROR("Checksum hash computation mismatch");
            request->complete(response, IOStatus::ERR_CKSUM);
            return;
        }

        response->next_offset = request->offset + hdr_off + total_size;
        LOG_DEBUG(LOG_STORAGE, LOG_LEVEL_DEBUG1, "Read {} vectors, total_size={}, next_offset={}",
                  response->data.size(), total_size, response->next_offset);

        request->complete(response, IOStatus::SUCCESS);

        return;
    }

    int
    IOSysFH::_internal_write(const std::vector<std::shared_ptr<std::vector<char>>> &data,
                             const struct iovec *data_ptrs,
                             uint64_t offset,
                             bool is_compressed)
    {
        uint8_t count = data.size();
        DCHECK_LE(count, IOHandle::MAX_VECTORS);

        // write out header data
        char hdr[IOFile::EXT_HDR_SIZE + IOFile::VEC_HDR_SIZE * IOHandle::MAX_VECTORS];

        // header and number of vectors
        std::copy_n((is_compressed) ? IOFile::HDR_MAGIC_COMPRESSED : IOFile::HDR_MAGIC_UNCOMPRESSED, 3, &hdr[0]);
        hdr[3] = count;

        // hash of data
        uint64_t hash = _compute_hash(data);
        std::copy_n(reinterpret_cast<char *>(&hash), sizeof(uint64_t), &hdr[4]);

        // fill in header and construct iovec for write
        int hdr_off = IOFile::EXT_HDR_SIZE;
        uint32_t total_size = IOFile::EXT_HDR_SIZE + IOFile::VEC_HDR_SIZE * count;
        struct iovec iov[IOHandle::MAX_VECTORS+1];
        iov[0].iov_base = hdr;
        iov[0].iov_len = IOFile::EXT_HDR_SIZE + IOFile::VEC_HDR_SIZE * count;

        for (int i = 0; i < count; i++) {
            // original data size
            uint32_t size = data[i]->size();
            std::copy_n(reinterpret_cast<char *>(&size), sizeof(int32_t), &hdr[hdr_off]);

            // stored data size (compressed size if compressed)
            int32_t dsize = data_ptrs[i].iov_len;
            std::copy_n(reinterpret_cast<char *>(&dsize), sizeof(int32_t), &hdr[hdr_off + 4]);
            iov[i+1].iov_base = data_ptrs[i].iov_base;
            iov[i+1].iov_len = data_ptrs[i].iov_len;

            LOG_DEBUG(LOG_STORAGE, LOG_LEVEL_DEBUG3, "IOSysFH::internal_write (uncompressed); idx={}, size={}", i, size);

            total_size += iov[i+1].iov_len;
            hdr_off += IOFile::VEC_HDR_SIZE;
            DCHECK_LE(hdr_off, (IOFile::EXT_HDR_SIZE + IOFile::VEC_HDR_SIZE * count));
        }

        dump_hdr(hdr, hdr_off);

        // do the write
        int bytes_written = ::pwritev(_fd, iov, count+1, offset);
        if (bytes_written > 0) {
            CHECK_EQ(bytes_written, total_size);
        } else if (bytes_written < 0) {
            LOG_ERROR("Received write error: errno={}", errno);
        }

        return bytes_written;  // either > 0 on success, or -1 on error with errno set
    }

    void
    IOSysFH::append(IORequestAppend * const request,
                    const struct iovec *data_ptrs,
                    bool is_compressed)
    {
        // default error response
        std::shared_ptr<IOResponseAppend> response = std::make_shared<IOResponseAppend>(request);

        // no data to be written
        auto data_count = request->data.size();
        if (data_count == 0) {
            response->offset = _io_file->reserve_offset(0);
            response->next_offset = response->offset;
            request->complete(response, IOStatus::SUCCESS);
            return;
        }

        // reserve space for append; iterate through iovec to get total size
        uint64_t total_size = IOFile::EXT_HDR_SIZE + IOFile::VEC_HDR_SIZE * data_count;
        for (size_t i = 0; i < data_count; i++) {
            total_size += data_ptrs[i].iov_len;
        }
        auto offset = _io_file->reserve_offset(total_size);
        response->offset = offset;

        // issue write
        int bytes_written = _internal_write(request->data, data_ptrs, offset, is_compressed);
        if (bytes_written < 0) {
            if (bytes_written == -1) {
                request->complete(response, errno);
            } else if (bytes_written == -2) {
                request->complete(response, IOStatus::ERR_DECODE);
            }
            return;
        }

        _is_dirty = true;

        response->next_offset = offset + bytes_written;
        LOG_DEBUG(LOG_STORAGE, LOG_LEVEL_DEBUG1, "Append at offset={}, next_offset={}, written={}", offset, response->next_offset, bytes_written);

        DCHECK_EQ(bytes_written, total_size);

        request->complete(response, IOStatus::SUCCESS);

        return;
    }

    void
    IOSysFH::write(IORequestWrite * const request)
    {
        CHECK_EQ(is_compressed(), false);

        // default error response
        std::shared_ptr<IOResponseWrite> response = std::make_shared<IOResponseWrite>(request);

        // no data to be written
        if (request->data.size() == 0) {
            response->next_offset = request->offset;
            request->complete(response, IOStatus::SUCCESS);
            return;
        }

        // construct iovec for write
        struct iovec data_ptrs[IOHandle::MAX_VECTORS];
        size_t count = request->data.size();
        DCHECK_LE(count, IOHandle::MAX_VECTORS);
        for (size_t i = 0; i < count; i++) {
            data_ptrs[i].iov_base = request->data[i].get();
            data_ptrs[i].iov_len = request->data[i]->size();
        }

        // issue write
        int bytes_written = _internal_write(request->data, data_ptrs, request->offset, false);
        if (bytes_written == -1) {
            request->complete(response, errno);
            return;
        }

        _is_dirty = true;

        response->next_offset = request->offset + bytes_written;
        request->complete(response, IOStatus::SUCCESS);
        return;
    }


    void IOSysFH::sync(IORequestSync * const request)
    {
        // default success response
        std::shared_ptr<IOResponse> response = std::make_shared<IOResponse>(request);

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
