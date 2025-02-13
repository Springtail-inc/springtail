#include <unistd.h>
#include <errno.h>
#include <vector>

#include <absl/log/check.h>

#include <common/common.hh>
#include <common/logging.hh>
#include <common/exception.hh>
#include <common/filesystem.hh>

#include <pg_repl/pg_repl_msg.hh>

#include <pg_log_mgr/pg_xact_log_reader.hh>
#include <pg_log_mgr/pg_xact_log_writer.hh>

namespace springtail::pg_log_mgr {

bool
PgXactLogReader::next()
{
    // Move to next row
    ++_current_row;

    // If we've reached the end of the current extent
    if (_current_row == _current_extent->end()) {
        // Try to load next extent in current file
        if (_load_next_extent()) {
            return true;
        }
            
        // If no more extents, try next file
        if (!_open_next_file()) {
            return false;
        }
    }

    return true;
}

bool
PgXactLogReader::begin()
{
    // Find first file
    _current_file = fs::find_earliest_modified_file(_base_dir, _file_prefix, _file_suffix);
    if (_current_file.empty()) {
        return false;
    }

    // Open the file and load first extent
    if (!_open_next_file()) {
        return false;
    }

    return true;
}

bool
PgXactLogReader::_load_next_extent()
{
    // Read the extent data from the file at the current offset
    auto response = _current_handle->read(_current_offset);
    if (!response) {
        return false;
    }
        
    // Create the extent from the response data
    _current_extent = std::make_shared<Extent>(response->data);

    // Update the offset to point after this extent for next read
    _current_offset = response->next_offset;

    // update the row iterator
    _current_row = _current_extent->begin();
    return true;
}

bool 
PgXactLogReader::_open_next_file()
{
    if (_current_file.empty()) {
        return false;
    }

    // If this isn't our first file, get the next one
    if (_current_extent) {
        _current_file = fs::get_next_file(_current_file, _file_prefix, _file_suffix);
        if (_current_file.empty() || !std::filesystem::exists(_current_file)) {
            _current_extent = nullptr;
            _current_handle = nullptr;
            return false;
        }
    }

    // Open the file and read the first extent
    _current_handle = IOMgr::get_instance()->open(_current_file, IOMgr::IO_MODE::READ, false);
    _current_offset = 0;
        
    // Load the first extent
    if (!_load_next_extent()) {
        return false;
    }
        
    return true;
}

// Field accessors
uint32_t 
PgXactLogReader::get_pg_xid() const 
{
    return _pg_xid_f->get_uint32(*_current_row);
}

uint64_t 
PgXactLogReader::get_xid() const 
{
    return _xid_f->get_uint64(*_current_row);
}

} // namespace springtail::pg_log_mgr
