#include <fcntl.h>

#include <pg_log_mgr/pg_log_mgr.hh>
#include <xid_mgr/pg_xact_log_reader.hh>

namespace springtail::xid_mgr {

PgXactLogReader::~PgXactLogReader()
{
    if (_fd != -1) {
        ::close(_fd);
    }
}

bool
PgXactLogReader::begin()
{
    _cleanup();
    return _open_next_file() && _get_next_xid();
}

bool
PgXactLogReader::next()
{
    return _get_next_xid();
}

bool
PgXactLogReader::_get_next_xid()
{
    if (_fd == -1) {
        return false;
    }
    _current_xid = nullptr;
    while (_current_xid == nullptr) {
        // If we reached the end of the page
        if (_current_offset == PgXactLogWriter::PG_XLOG_PAGE_SIZE) {
            ++_page_count;
            // Try to load the next page from the current file
            if (!_load_next_page() && !_open_next_file()) {
                return false;
            }
        }
        _current_xid = reinterpret_cast<PgXactLogWriter::XidElement *>(&_read_buffer[_current_offset]);
        if (_current_xid->xid != 0) {
            _current_offset += sizeof(PgXactLogWriter::XidElement);
            if (_current_xid->pg_xid == 0) {
                _current_xid = nullptr;
            }
        } else {
            _current_xid = nullptr;
            if (!_open_next_file()) {
                return false;
            }
        }
    }
    return true;
}

bool
PgXactLogReader::_open_next_file()
{
    do {
        if (_fd != -1) {
            ::close(_fd);
            _fd = -1;
        }

        // attempt to find the first or the next file
        if (!_current_file.has_value()) {
            _current_file = fs::find_earliest_modified_file(_base_dir, PgXactLogWriter::LOG_PREFIX_XACT, PgXactLogWriter::LOG_SUFFIX);
        } else {
            _current_file = fs::get_next_log_file(*_current_file, PgXactLogWriter::LOG_PREFIX_XACT, PgXactLogWriter::LOG_SUFFIX);
        }
        if (!_current_file.has_value()) {
            return false;
        }
        LOG_INFO("Current file: {}", _current_file->string());

        // open file
        _fd = ::open(_current_file.value().c_str(), O_RDWR, 0660);
        if (_fd == -1) {
            throw Error(fmt::format("Failed to open file {}; error {}: {}", _current_file.value().string(), errno, strerror(errno)));
        }
        _page_count = 0;
    } while(!_load_next_page());

    return true;
}

bool
PgXactLogReader::_load_next_page()
{
    int ret = ::read(_fd, _read_buffer, PgXactLogWriter::PG_XLOG_PAGE_SIZE);
    if (ret == 0) {
        return false;
    }
    if (ret == -1) {
        throw Error(fmt::format("Error reading from file {}; error {}: {}", _current_file.value().string(), errno, strerror(errno)));
    }
    if (ret != PgXactLogWriter::PG_XLOG_PAGE_SIZE) {
        throw Error(fmt::format("Error: read incomplete page from file {}", _current_file.value().string()));
    }
    _current_offset = 0;

    return true;
}

void
PgXactLogReader::_cleanup()
{
    if (_fd != -1) {
        ::close(_fd);
        _fd = -1;
    }
    _current_file.reset();
    _current_xid = nullptr;
}

}  // namespace springtail::pg_log_mgr
