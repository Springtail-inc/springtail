#include <fcntl.h>
#include <sys/mman.h>

#include <pg_log_mgr/pg_xact_log_writer_mmap.hh>

#include <common/exception.hh>
#include <pg_log_mgr/pg_log_mgr.hh>

namespace springtail::pg_log_mgr {

PgXactLogWriterMmap::PgXactLogWriterMmap(const std::filesystem::path &base_dir, uint64_t committed_xid) :
            _base_dir(base_dir), _committed_xid(committed_xid)
{
    // create the base directory for the file if it doesn't exist
    std::filesystem::create_directories(_base_dir);
}

PgXactLogWriterMmap::~PgXactLogWriterMmap()
{
    if (_fd != -1) {
        ::munmap(_file_mem, PG_XLOG_PAGE_SIZE);
        ::close(_fd);
    }
}

void
PgXactLogWriterMmap::_recover()
{
    // read buffer
    XidElement xid_elements[PG_XLOG_PAGE_SIZE/sizeof(XidElement)];
    // number of elements in the read buffer
    size_t xid_elements_size = PG_XLOG_PAGE_SIZE/sizeof(XidElement);
    // void pointer to read buffer
    void *buffer = reinterpret_cast<void *>(xid_elements);
    int ret;

    // read the page
    while ((ret = ::read(_fd, buffer, PG_XLOG_PAGE_SIZE)) != 0) {
        if (ret == -1) {
            throw Error(fmt::format("Error reading from file {}; error {}: {}", _file.string(), errno, strerror(errno)));
        }
        // raise exception if somehow we did not read enough data
        if (ret != PG_XLOG_PAGE_SIZE) {
            throw Error(fmt::format("Error: read incomplete page from file {}", _file.string()));
        }

        // find position of the last committed xid
        size_t i = 0;
        while (i < xid_elements_size) {
            uint64_t current_xid = xid_elements[i].xid;
            // check if we reached the end of xid records
            if (current_xid == 0) {
                throw Error(fmt::format("Error: file {}, end of xid records reached before committed xid is found", _file.string()));
            }
            // move file offset
            if (current_xid <= _committed_xid) {
                _last_offset += sizeof(XidElement);
            }
            // same or greater xid found
            if (current_xid >= _committed_xid) {
                SPDLOG_INFO("Found current_xid: {}; _committed_xid: {}", current_xid, _committed_xid);

                // truncate file up to the committed xid entry
                if (::ftruncate(_fd, _last_offset) == -1) {
                    throw Error(fmt::format("Failed to resize file {}; error {}: {}", _file.string(), errno, strerror(errno)));
                }

                // set up variables for memmory mapping
                _mmap_offset = (_last_offset & ~(PG_XLOG_PAGE_SIZE - 1));
                _file_size = _mmap_offset + PG_XLOG_PAGE_SIZE;
                _last_offset = _last_offset & (PG_XLOG_PAGE_SIZE - 1);

                return;
            }
            i++;
        }
    }
    throw Error(fmt::format("Error: file {}, end of file reached before committed xid is found", _file.string()));
}

void
PgXactLogWriterMmap::rotate(uint64_t timestamp)
{
    // if we already have an open file, clean it up
    if (_fd != -1) {
        if (::munmap(_file_mem, PG_XLOG_PAGE_SIZE) == -1) {
            throw Error(fmt::format("Failed to unmap memory for file {}; error {}: {}", _file.string(), errno, strerror(errno)));
        }
        ::close(_fd);
        _fd = -1;
        _file_size = 0;
        _mmap_offset = 0;
        _last_offset = 0;
        _file_mem = nullptr;
    }

    // create log path
    _file = fs::create_log_file_with_timestamp(_base_dir, PgLogMgr::LOG_PREFIX_XACT, PgLogMgr::LOG_SUFFIX, timestamp);
    SPDLOG_INFO("Next Xact file: {}", _file.string());

    // if the file already exists, we are in recovery mode
    if (std::filesystem::exists(_file) && _first_file) {
        // open file
        _fd = ::open(_file.c_str(), O_RDWR, 0666);
        if (_fd == -1) {
            throw Error(fmt::format("Failed to open file {}; error {}: {}", _file.string(), errno, strerror(errno)));
        }
        _recover();
    } else {
        // open new file
        _fd = ::open(_file.c_str(), O_CREAT | O_TRUNC | O_RDWR, 0666);
        if (_fd == -1) {
            throw Error(fmt::format("Failed to open file {}; error {}: {}", _file.string(), errno, strerror(errno)));
        }

        // set file size
        _file_size = PG_XLOG_PAGE_SIZE;
    }
    _first_file = false;

    _resize_and_map();
}

void
PgXactLogWriterMmap::log(uint32_t pg_xid, uint64_t xid)
{
    DCHECK(_file_mem != nullptr);

    // set xid data in the mapped memory
    XidElement *mem_xid_element = reinterpret_cast<XidElement *>(_file_mem) + (_last_offset / sizeof(XidElement));
    mem_xid_element->pg_xid = pg_xid;
    mem_xid_element->xid = xid;

    // flush the memory
    _flush();

    _last_offset += sizeof(XidElement);
    // check if we reached the end of the page
    if (_last_offset == PG_XLOG_PAGE_SIZE) {

        // unmap the memory to prepare for file size change
        if (::munmap(_file_mem, PG_XLOG_PAGE_SIZE) == -1) {
            throw Error(fmt::format("Failed to unmap memory for file {}; error {}: {}", _file.string(), errno, strerror(errno)));
        }
        _mmap_offset += PG_XLOG_PAGE_SIZE;
        _file_size += PG_XLOG_PAGE_SIZE;
        _last_offset = 0;
        _file_mem = nullptr;

        // resize file to a bigger size and remap the newly added segment to memory
        _resize_and_map();
    }
}

void
PgXactLogWriterMmap::_resize_and_map()
{
    // resize
    if (::ftruncate(_fd, _file_size) == -1) {
        throw Error(fmt::format("Failed to resize file {}; error {}: {}", _file.string(), errno, strerror(errno)));
    }

    // map the last memory segment
    void *mem_ptr = ::mmap(nullptr, PG_XLOG_PAGE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, _fd, _mmap_offset);
    if (mem_ptr == MAP_FAILED) {
        throw Error(fmt::format("Failed to map memory for file {}; error {}: {}", _file.string(), errno, strerror(errno)));
    }
    _file_mem = reinterpret_cast<XidElement *>(mem_ptr);
}

void
PgXactLogWriterMmap::_flush()
{
    if (_file_mem != nullptr) {
        return;
    }
    if (::msync(_file_mem, PG_XLOG_PAGE_SIZE, MS_SYNC) == -1) {
        throw Error(fmt::format("Failed to sync memory for file {}; error {}: {}", _file.string(), errno, strerror(errno)));
    }
}

} // namespace springtail::pg_log_mgr