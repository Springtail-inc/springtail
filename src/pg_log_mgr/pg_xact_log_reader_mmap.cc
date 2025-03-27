#include <fcntl.h>

#include <pg_log_mgr/pg_log_mgr.hh>
#include <pg_log_mgr/pg_xact_log_reader_mmap.hh>

namespace springtail::pg_log_mgr {
PgXactLogReaderMmap::PgXactLogReaderMmap(const std::filesystem::path &base_dir)
    : _base_dir(base_dir) {}

PgXactLogReaderMmap::~PgXactLogReaderMmap()
{
    if (_fd != -1) {
        ::close(_fd);
    }
}

bool
PgXactLogReaderMmap::begin()
{
    if (_fd != -1) {
        ::close(_fd);
        _fd = -1;
    }
    _current_file.reset();
    _current_xid = nullptr;
    return _open_next_file();
}

bool
PgXactLogReaderMmap::next()
{
    if (_current_xid == nullptr || _current_xid->pg_xid == 0) {
        return false;
    }

    // If we reached the end of the page
    if (_current_offset == PgXactLogWriterMmap::PG_XLOG_PAGE_SIZE) {
        // Try to load the next page from the current file
        _load_next_page();
    } else {
        _current_xid = reinterpret_cast<PgXactLogWriterMmap::XidElement *>(&_read_buffer[_current_offset]);
        _current_offset += sizeof(PgXactLogWriterMmap::XidElement);
    }

    return ((_current_xid == nullptr || _current_xid->pg_xid == 0)? _open_next_file() : true);
}

bool
PgXactLogReaderMmap::_open_next_file()
{
    while (_current_xid == nullptr || _current_xid->pg_xid == 0) {
        if (_fd != -1) {
            ::close(_fd);
            _fd = -1;
        }

        // attempt to find the first or the next file
        if (!_current_file.has_value()) {
            _current_file = fs::find_earliest_modified_file(_base_dir, PgLogMgr::LOG_PREFIX_XACT, PgLogMgr::LOG_SUFFIX);
        } else {
            _current_file = fs::get_next_log_file(*_current_file, PgLogMgr::LOG_PREFIX_XACT, PgLogMgr::LOG_SUFFIX);
        }
        if (!_current_file.has_value()) {
            return false;
        }
        SPDLOG_INFO("Current file: {}", _current_file->string());

        // open file
        _fd = ::open(_current_file.value().c_str(), O_RDWR, 0666);
        if (_fd == -1) {
            throw Error(fmt::format("Failed to open file {}; error {}: {}", _current_file.value().string(), errno, strerror(errno)));
        }

        _load_next_page();
    }

    return true;
}

void
PgXactLogReaderMmap::_load_next_page()
{
    int ret = ::read(_fd, _read_buffer, PgXactLogWriterMmap::PG_XLOG_PAGE_SIZE);
    if (ret == 0) {
        _current_xid = nullptr;
        return;
    }
    if (ret == -1) {
        throw Error(fmt::format("Error reading from file {}; error {}: {}", _current_file.value().string(), errno, strerror(errno)));
    }
    if (ret != PgXactLogWriterMmap::PG_XLOG_PAGE_SIZE) {
        throw Error(fmt::format("Error: read incomplete page from file {}", _current_file.value().string()));
    }
    _current_offset = 0;

    _current_xid = reinterpret_cast<PgXactLogWriterMmap::XidElement *>(&_read_buffer[_current_offset]);
    _current_offset += sizeof(PgXactLogWriterMmap::XidElement);
    return;
}

// Field accessors
uint32_t
PgXactLogReaderMmap::get_pg_xid() const
{
    return ((_current_xid == nullptr)? 0 : _current_xid->pg_xid);
}

uint64_t
PgXactLogReaderMmap::get_xid() const
{
    return ((_current_xid == nullptr)? 0 : _current_xid->xid);
}

}  // namespace springtail::pg_log_mgr