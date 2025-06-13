#include <fcntl.h>

#include <common/logging.hh>
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
    return _open_next_file() && next();
}

bool
PgXactLogReader::next()
{
    if (_fd == -1) {
        return false;
    }

    while (true) {
        // If we reached the end of the page
        if (_current_offset + PgXactLogWriter::XidElement::PACKED_SIZE > _end_offset) {
            // Try to load the next page from the current file
            if (!_load_next_page() && !_open_next_file()) {
                return false;
            }
        }

        _current_offset += PgXactLogWriter::XidElement::unpack(&_read_buffer[_current_offset], _current_xid);
        if (_current_xid.xid != 0) {
            return true;
        }

        _current_xid.xid = 0;
        if (!_open_next_file()) {
            return false;
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

        // open file
        LOG_DEBUG(LOG_PG_LOG_MGR, "Opening xact log file: {}", _current_file->string());
        _fd = ::open(_current_file.value().c_str(), O_RDWR, 0660);
        if (_fd == -1) {
            throw Error(fmt::format("Failed to open file {}; error {}: {}", _current_file.value().string(), errno, strerror(errno)));
        }

    } while(!_load_next_page()); // make sure we can read the first page or go to the next file

    return true;
}

bool
PgXactLogReader::_load_next_page()
{
    int ret = ::read(_fd, _read_buffer, PgXactLogWriter::PG_XLOG_PAGE_SIZE);
    if (ret == 0) {
        return false;          // EOF
    }
    if (ret == -1) {
        throw Error(fmt::format("Error reading from file {}; error {}: {}", _current_file.value().string(), errno, strerror(errno)));
    }

    DCHECK_EQ(ret % PgXactLogWriter::XidElement::PACKED_SIZE, 0)
        << "Read size must be a multiple of XidElement size";

    _end_offset = ret;
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
    _current_offset = 0;
}

}  // namespace springtail::pg_log_mgr
