#include <common/logging.hh>

#include <storage/compressors.hh>
#include <storage/io_mgr.hh>
#include <storage/io_file.hh>
#include <storage/io_request.hh>

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

    void
    IORequestAppend::_issue_request(IOMgr * const io_mgr, std::shared_ptr<IOSysFH> fh) noexcept
    {
        std::shared_ptr<Compressor> compressor;
        try {
            compressor = io_mgr->get_compressor();
            fh->append(this, compressor);
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
