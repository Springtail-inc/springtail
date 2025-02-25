#include <unistd.h>
#include <errno.h>
#include <vector>

#include <absl/log/check.h>

#include <common/common.hh>
#include <common/logging.hh>
#include <common/exception.hh>
#include <common/filesystem.hh>

#include <pg_repl/pg_repl_msg.hh>

#include <pg_log_mgr/pg_log_mgr.hh>
#include <pg_log_mgr/pg_xact_log_reader.hh>
#include <pg_log_mgr/pg_xact_log_writer.hh>

namespace springtail::pg_log_mgr {

// note: these are hard-coded from the postgres type OIDs to avoid having to include all of the
//       postgres headers here -- also duplicated in system_tables.cc
constexpr int32_t INT8OID = 20;
constexpr int32_t INT4OID = 23;

PgXactLogReader::PgXactLogReader(const std::filesystem::path &base_dir)
        : _base_dir(base_dir)
{
    // construct the schema of the log file
    std::vector<SchemaColumn> columns = {
        { "pgxid", 1, SchemaType::UINT32, INT4OID, false },
        { "xid", 2, SchemaType::UINT64, INT8OID, false }
    };
    _schema = std::make_shared<ExtentSchema>(columns);
    _pg_xid_f = _schema->get_field("pgxid");
    _xid_f = _schema->get_field("xid");
}

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
    _current_file = fs::find_earliest_modified_file(_base_dir, PgLogMgr::LOG_PREFIX_XACT, PgLogMgr::LOG_SUFFIX);
    if (!_current_file) {
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
    if (response->data.empty()) {
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
    if (!_current_file) {
        return false;
    }

    // If this isn't our first file, get the next one
    if (_current_extent) {
        _current_file = fs::get_next_log_file(*_current_file, PgLogMgr::LOG_PREFIX_XACT, PgLogMgr::LOG_SUFFIX);
        if (!_current_file) {
            _current_extent = nullptr;
            _current_handle = nullptr;
            return false;
        }
    }

    // Open the file and read the first extent
    _current_handle = IOMgr::get_instance()->open(*_current_file, IOMgr::IO_MODE::READ, true);
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
