#include <fcntl.h>
#include <sys/mman.h>
#include <functional>

#include <common/exception.hh>
#include <xid_mgr/pg_xact_log_writer.hh>

namespace springtail::xid_mgr {

PgXactLogWriter::PgXactLogWriter(const std::filesystem::path &base_dir) :
            _base_dir(base_dir)
{
    // create the base directory for the file if it doesn't exist
    std::filesystem::create_directories(_base_dir);
    LOG_INFO("Creating directory {}", base_dir);
    auto last_file = fs::find_latest_modified_file(_base_dir, LOG_PREFIX_XACT, LOG_SUFFIX);
    if (!last_file.has_value()) {
        return;
    }
    auto timestamp = fs::extract_timestamp_from_file(last_file.value(), LOG_PREFIX_XACT, LOG_SUFFIX);
    if (!timestamp.has_value()) {
        return;
    }
    rotate(timestamp.value());
}

PgXactLogWriter::~PgXactLogWriter()
{
    if (_fd != -1) {
        flush();
        ::munmap(_file_mem, PG_XLOG_PAGE_SIZE);
        ::close(_fd);
    }
}

void
PgXactLogWriter::rotate(uint64_t timestamp)
{
    if (timestamp == _current_log_timestamp) {
        return;
    }

    // create log path
    std::filesystem::path file = fs::create_log_file_with_timestamp(_base_dir, LOG_PREFIX_XACT, LOG_SUFFIX, timestamp);

    // if we already have an open file, clean it up
    if (_fd != -1) {
        if (file == _file) {
            return;
        }
        flush();
        if (::munmap(_file_mem, PG_XLOG_PAGE_SIZE) == -1) {
            throw Error(fmt::format("Failed to unmap memory for file {}; error {}: {}", _file.string(), errno, strerror(errno)));
        }
        ::close(_fd);
        _fd = -1;
        _file_size = 0;
        _mmap_offset = 0;
        _last_offset = 0;
        _file_mem = nullptr;
        _can_flush = false;
    }
    _file = file;

    LOG_INFO("Next Xact file: {}", _file.string());

    // if the file already exists, we are in recovery mode
    if (std::filesystem::exists(_file) && _first_file) {
        // open file
        _fd = ::open(_file.c_str(), O_RDWR, 0660);
        if (_fd == -1) {
            throw Error(fmt::format("Failed to open file {}; error {}: {}", _file.string(), errno, strerror(errno)));
        }
        _extract_last_xid();
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

    _current_log_timestamp = timestamp;
}

void
PgXactLogWriter::log(uint32_t pg_xid, uint64_t xid, bool real_commit)
{
    DCHECK(_file_mem != nullptr);

    // set xid data in the mapped memory
    XidElement *mem_xid_element = reinterpret_cast<XidElement *>(_file_mem) + (_last_offset / sizeof(XidElement));
    mem_xid_element->pg_xid = pg_xid;
    mem_xid_element->real_commit = real_commit;
    mem_xid_element->xid = xid;

    if (pg_xid != 0) {
        _last_stored_xid = xid;
    }

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
    _can_flush = true;
}

void
PgXactLogWriter::cleanup(uint64_t min_timestamp, bool archive_logs)
{
    fs::cleanup_files_from_dir(_base_dir, LOG_PREFIX_XACT, LOG_SUFFIX, min_timestamp, archive_logs);
}

void
PgXactLogWriter::_resize_and_map()
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
PgXactLogWriter::_extract_last_xid()
{
    int ret;
    char read_buffer[PG_XLOG_PAGE_SIZE];
    size_t total_offset = 0;
    size_t current_offset = 0;
    XidElement *current_xid = nullptr;

    while ((ret = ::read(_fd, read_buffer, PG_XLOG_PAGE_SIZE)) != 0) {
        total_offset += current_offset;
        if (ret == -1) {
            throw Error(fmt::format("Error reading from file {}; error {}: {}", _file.string(), errno, strerror(errno)));
        }
        if (ret != xid_mgr::PgXactLogWriter::PG_XLOG_PAGE_SIZE) {
            throw Error(fmt::format("Error: read incomplete page from file {}", _file.string()));
        }
        current_offset = 0;
        while (current_offset < PG_XLOG_PAGE_SIZE) {
            current_xid = reinterpret_cast<xid_mgr::PgXactLogWriter::XidElement *>(&read_buffer[current_offset]);
            if (current_xid->xid == 0) {
                break;
            }
            current_offset += sizeof(xid_mgr::PgXactLogWriter::XidElement);
            if (current_xid->pg_xid != 0) {
                _last_stored_xid = current_xid->xid;
            }
        }
        if (current_offset < PG_XLOG_PAGE_SIZE) {
            break;
        }
    }
    total_offset += current_offset;
    _last_offset = total_offset & (PG_XLOG_PAGE_SIZE - 1);
    _mmap_offset = (total_offset & ~(PG_XLOG_PAGE_SIZE - 1));
    _file_size = _mmap_offset + PG_XLOG_PAGE_SIZE;
}

void
PgXactLogWriter::flush()
{
    if (_file_mem != nullptr || !_can_flush) {
        return;
    }
    if (::msync(_file_mem, PG_XLOG_PAGE_SIZE, MS_SYNC) == -1) {
        throw Error(fmt::format("Failed to sync memory for file {}; error {}: {}", _file.string(), errno, strerror(errno)));
    }
    _can_flush = false;
}

bool
PgXactLogWriter::set_last_xid_in_storage(std::filesystem::path base_dir, uint64_t last_xid, bool archive)
{
    auto current_file = fs::find_earliest_modified_file(base_dir, LOG_PREFIX_XACT, LOG_SUFFIX);
    char read_buffer[PG_XLOG_PAGE_SIZE];
    while (current_file.has_value()) {
        int fd = ::open(current_file.value().c_str(), O_RDWR, 0660);
        if (fd == -1) {
            throw Error(fmt::format("Failed to open file {}; error {}: {}", current_file.value().string(), errno, strerror(errno)));
        }
        uint32_t page_count = 0;
        int ret;
        while ((ret = ::read(fd, read_buffer, PG_XLOG_PAGE_SIZE)) != 0) {
            ++page_count;
            if (ret == -1) {
                throw Error(fmt::format("Error reading from file {}; error {}: {}", current_file.value().string(), errno, strerror(errno)));
            }
            if (ret != PG_XLOG_PAGE_SIZE) {
                throw Error(fmt::format("Error: read incomplete page from file {}", current_file.value().string()));
            }
            size_t current_offset = 0;
            while (current_offset != PG_XLOG_PAGE_SIZE) {
                XidElement * current_xid = reinterpret_cast<XidElement *>(&read_buffer[current_offset]);
                if (current_xid->xid == 0) {
                    goto next_file;
                }
                if (current_xid->xid > last_xid) {
                    size_t file_size = PG_XLOG_PAGE_SIZE * (page_count - 1) + current_offset;
                    ::ftruncate(fd, file_size);
                    ::ftruncate(fd, PG_XLOG_PAGE_SIZE * page_count);
                    ::close(fd);
                    auto current_timestamp = fs::extract_timestamp_from_file(current_file.value(), LOG_PREFIX_XACT, LOG_SUFFIX);
                    if (!current_timestamp.has_value()) {
                        throw Error(fmt::format("Error: file name {} does not include timestamp", current_file.value().string()));
                    }
                    fs::cleanup_files_from_dir<std::greater<uint64_t>>(base_dir, LOG_PREFIX_XACT, LOG_SUFFIX, current_timestamp.value(), archive);
                    return true;
                }
                current_offset += sizeof(PgXactLogWriter::XidElement);
            }
        }
        next_file:
            ::close(fd);
            current_file = fs::get_next_log_file(current_file.value(), LOG_PREFIX_XACT, LOG_SUFFIX);
    }
    return false;
}


} // namespace springtail::pg_log_mgr
