#include <common/logging.hh>

#include <storage/compressors.hh>
#include <storage/io_mgr.hh>
#include <storage/io_file.hh>
#include <storage/io_request.hh>
#include <storage/io.hh>

namespace springtail {

    std::string
    IORequest::get_type() const noexcept
    {
        switch (type) {
            case IOType::READ:
                return "read";
            case IOType::APPEND:
                return "append";
            case IOType::SYNC:
                return "sync";
            case IOType::WRITE:
                return "write";
            case IOType::SHUTDOWN:
                return "shutdown";
        }
        return "";
    }

    void
    IORequestWrite::_issue_request(IOMgr * const io_mgr, std::shared_ptr<IOSysFH> fh) noexcept
    {
        try {
            fh->write(this);
        } catch (const std::exception &exc) {
            // log exception
            LOG_ERROR("Caught exception for IO type={}", get_type());
            LOG_ERROR("Exception: {}", exc.what());

            complete(std::make_shared<IOResponseWrite>(this, IOStatus::ERROR));
        }
    }

    int32_t
    IORequest::_compress_data(const std::vector<std::shared_ptr<std::vector<char>>> &data,
                              std::shared_ptr<Compressor> compressor,
                              std::vector<char>* compressed_data,
                              bool &is_compressed)
    {
        uint8_t count = data.size();
        DCHECK(count <= IOHandle::MAX_VECTORS);

        // compress data and compute total uncompressed size
        uint32_t compressed_size = 0;
        uint32_t size = 0;

        try {
            // iterate through vectors and compress them
            for (int i = 0; i < count; i++) {
                compressor->compress_raw(data[i], compressed_data[i]);

                LOG_DEBUG(LOG_STORAGE, LOG_LEVEL_DEBUG3, "IOSysFH::_internal_write: compressing vector: {}", i);

                compressed_size += compressed_data[i].size();
                size += data[i]->size();
            }

        } catch (ValidationError &exc) {
            LOG_ERROR("Exception while compressing data");
            LOG_ERROR("Exception: {}", exc.what());

            return -2; // decode error
        }

        // if compression isn't helping then don't compress
        if (size <= compressed_size) {
            // don't compress
            is_compressed = false;

            LOG_DEBUG(LOG_STORAGE, LOG_LEVEL_DEBUG1, "Not compressing data, compressed size too big: {} vs {}",
                            compressed_size, size);

            return size;
        }

        is_compressed = true;
        return compressed_size;
    }

    void
    IORequestAppend::_issue_request(IOMgr * const io_mgr, std::shared_ptr<IOSysFH> fh) noexcept
    {
        std::shared_ptr<Compressor> compressor = nullptr;

        try {
            bool is_compressed = fh->is_compressed();
            std::vector<char> compressed_data[IOHandle::MAX_VECTORS];

            // compress data if file is compressable
            if (is_compressed) {
                compressor = io_mgr->get_compressor();

                // compress the data, this may change is_compressed flag to false if it
                // isn't worth compressing
                // data is compressed into a std::vector<char> array which will deallocate
                // automatically at the end of this try block
                if (_compress_data(data, compressor, compressed_data, is_compressed) < 0) {
                    // compression error
                    io_mgr->put_compressor(compressor);
                    complete(std::make_shared<IOResponseAppend>(this, IOStatus::ERR_DECODE));
                    return;
                }
            }

            // unfortunately the uncompressed data and compressed data are in different vector
            // formats, so we need to normalize them to an array of raw pointers
            struct iovec data_ptrs[IOHandle::MAX_VECTORS];
            if (is_compressed) {
                for (size_t i = 0; i < data.size(); i++) {
                    data_ptrs[i].iov_base = compressed_data[i].data();
                    data_ptrs[i].iov_len = compressed_data[i].size();
                }
            } else {
                for (size_t i = 0; i < data.size(); i++) {
                    data_ptrs[i].iov_base = data[i].get();
                    data_ptrs[i].iov_len = data[i]->size();
                }
            }

            // issue append
            fh->append(this, data_ptrs, is_compressed);

        } catch (const std::exception &exc) {
            // log exception
            LOG_ERROR("Caught exception for IO type={}", get_type());
            LOG_ERROR("Exception: {}", exc.what());

            complete(std::make_shared<IOResponseAppend>(this, IOStatus::ERROR));
        }

        if (compressor) {
            io_mgr->put_compressor(compressor);
        }
    }

    void
    IORequestRead::_issue_request(IOMgr * const io_mgr, std::shared_ptr<IOSysFH> fh) noexcept
    {
        std::shared_ptr<Decompressor> decompressor;
        try {
             decompressor = io_mgr->get_decompressor();
            fh->read(this, decompressor);
        } catch (const std::exception &exc) {
            // log exception
            LOG_ERROR("Caught exception for IO type={}", get_type());
            LOG_ERROR("Exception: {}", exc.what());

            complete(std::make_shared<IOResponseRead>(this, IOStatus::ERROR));
        }

        if (decompressor) {
            io_mgr->put_decompressor(decompressor);
        }
    }

    void
    IORequestSync::_issue_request(IOMgr * const io_mgr, std::shared_ptr<IOSysFH> fh) noexcept
    {
        try {
            fh->sync(this);
        } catch (const std::exception &exc) {
            // log exception
            LOG_ERROR("Caught exception for IO type={}", get_type());
            LOG_ERROR("Exception: {}", exc.what());

            complete(std::make_shared<IOResponse>(this, IOStatus::ERROR));
        }
    }

    void
    IORequest::_process_request()
    {
        // on shutdown return immediately
        if (IORequest::IOType::SHUTDOWN == type) {
            return;
        }

        // get IOFile
        IOMgr *mgr = IOMgr::get_instance();

        // lookup path for file object; creates new one if not present
        // marks file object as in use
        LOG_DEBUG(LOG_STORAGE, LOG_LEVEL_DEBUG1, "IOWorker got request for path: {}", path.c_str());

        std::shared_ptr<IOFile> io_file = mgr->lookup(path, compressed);

        // get a free handle based on IO mode; may block
        // marks file handle as in use
        std::shared_ptr<IOSysFH> fh = io_file->get_fh((IOType::READ == type) ?
                                                           IOMgr::IO_MODE::READ :
                                                           IOMgr::IO_MODE::WRITE);

        // issue request -- calls derived _issue_request method
        _issue_request(mgr, fh);

        // work is complete, release fh
        io_file->put_fh(fh);

        // release file object
        io_file->decr_in_use();

        return;
    }
} // namespace springtail
