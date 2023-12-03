#include <common/logging.hh>

#include <storage/compressors.hh>
#include <storage/io_mgr.hh>
#include <storage/io_file.hh>
#include <storage/io_request.hh>

namespace springtail {

    std::string 
    IORequest::get_type()
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
    }

    void
    IORequest::_issue_request(std::shared_ptr<IOSysFH> fh,
                              std::shared_ptr<Compressor> compressor,
                              std::shared_ptr<Decompressor> decompressor)
    {
        IORequest *request = this;

        // handle request
        switch (type) {
            case IOType::READ: {
                IORequestRead* req = dynamic_cast<IORequestRead *>(request);
                fh->read(req, decompressor);
                break;
            }

            case IOType::APPEND: {
                IORequestAppend *req = dynamic_cast<IORequestAppend *>(request);
                fh->append(req, compressor);
                break;
            }

            case IOType::WRITE: {
                IORequestWrite *req = dynamic_cast<IORequestWrite *>(request);
                fh->write(req);
                break;
            }

            case IOType::SYNC: {
                IORequestSync *req = dynamic_cast<IORequestSync *>(request);
                fh->sync(req);
                break;
            }

            case IOType::SHUTDOWN:
                break;

            default:
                // log error
                SPDLOG_ERROR("IOWorker::_issue_request unknown request type: {}", get_type());
                break;
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
        SPDLOG_INFO("IOWorker got request for path: {}", path.c_str());

        std::shared_ptr<IOFile> io_file = mgr->lookup(path, compressed);

        // get a free handle based on IO mode; may block
        // marks file handle as in use
        std::shared_ptr<IOSysFH> fh = io_file->get_fh((IOType::READ == type) ?
                                                           IOMgr::IO_MODE::READ : 
                                                           IOMgr::IO_MODE::WRITE);

        // get compress/decompressor based on operation type
        std::shared_ptr<Compressor> compressor = nullptr;
        std::shared_ptr<Decompressor> decompressor;

        if (type == IOType::READ) {
            decompressor = mgr->get_decompressor();
        } else {
            compressor = mgr->get_compressor();
        }

        try {
            _issue_request(fh, compressor, decompressor);
        } catch (const std::exception &exc) {
            // log exception
            SPDLOG_ERROR("Caught exception for IO type={}", get_type());
            SPDLOG_ERROR("Exception: {}", exc.what());
            
            _handle_error(exc);
        }

        // work is complete, release fh
        io_file->put_fh(fh);

        // release file object
        io_file->decr_in_use();

        // release compressor/decompressor
        if (type == IOType::READ) {
            mgr->put_decompressor(decompressor);
        } else {
            mgr->put_compressor(compressor);
        }
        
        return;
    }


    void
    IORequest::_handle_error(const std::exception &exc)
    {
        IORequest *request = this;

        switch (type) {
            case IOType::READ: {
                IORequestRead *req = dynamic_cast<IORequestRead *>(request);
                std::shared_ptr<IOResponseRead> res = std::make_shared<IOResponseRead>(req, IOStatus::ERROR);
                req->complete(res);
                break;
            }

            case IOType::APPEND: {
                IORequestAppend *req = dynamic_cast<IORequestAppend *>(request);
                std::shared_ptr<IOResponseAppend> res = std::make_shared<IOResponseAppend>(req, IOStatus::ERROR);
                req->complete(res);
                break;
            }

            case IOType::WRITE: {
                IORequestWrite *req = dynamic_cast<IORequestWrite *>(request);
                std::shared_ptr<IOResponseWrite> res = std::make_shared<IOResponseWrite>(req, IOStatus::ERROR);
                req->complete(res);
                break;
            }

            case IOType::SYNC: {
                IORequestSync *req = dynamic_cast<IORequestSync *>(request);
                std::shared_ptr<IOResponse> res = std::make_shared<IOResponse>(req, IOStatus::ERROR);
                req->complete(res);
                break;
            }

            default:
                break;
        }
    }
} // namespace springtail