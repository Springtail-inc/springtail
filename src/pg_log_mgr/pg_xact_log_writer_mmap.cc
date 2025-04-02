#include <fcntl.h>
#include <sys/mman.h>

#include <pg_log_mgr/pg_xact_log_writer_mmap.hh>

#include <common/exception.hh>
#include <pg_log_mgr/pg_log_mgr.hh>

namespace springtail::pg_log_mgr {

PgXactLogWriterMmap::PgXactLogWriterMmap(const std::filesystem::path &base_dir) :
            _base_dir(base_dir)
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
        _fd = ::open(_file.c_str(), O_RDWR, 0660);
        if (_fd == -1) {
            throw Error(fmt::format("Failed to open file {}; error {}: {}", _file.string(), errno, strerror(errno)));
        }
        _file_size = std::filesystem::file_size(_file);
        _last_offset = _file_size & (PG_XLOG_PAGE_SIZE - 1);
        _mmap_offset = (_file_size & ~(PG_XLOG_PAGE_SIZE - 1));
        _file_size = _mmap_offset + PG_XLOG_PAGE_SIZE;
    } else {
        // open new file
        _fd = ::open(_file.c_str(), O_CREAT | O_TRUNC | O_RDWR, 0660);
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