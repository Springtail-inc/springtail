#include <absl/log/check.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <chrono>
#include <vector>

#include <common/common.hh>
#include <common/exception.hh>
#include <common/filesystem.hh>
#include <common/logging.hh>

#include <pg_log_mgr/pg_log_mgr.hh>
#include <pg_log_mgr/pg_xact_log_writer.hh>

namespace springtail::pg_log_mgr {

// note: these are hard-coded from the postgres type OIDs to avoid having to include all of the
//       postgres headers here -- also duplicated in system_tables.cc
constexpr int32_t INT8OID = 20;
constexpr int32_t INT4OID = 23;

PgXactLogWriter::PgXactLogWriter(const std::filesystem::path &base_dir)
{
    // create the base directory for the file if it doesn't exist
    std::filesystem::create_directories(base_dir);

    // construct the log file name
    _file = fs::create_log_file(base_dir, PgLogMgr::LOG_PREFIX_XACT, PgLogMgr::LOG_SUFFIX);

    // construct the schema of the log file
    std::vector<SchemaColumn> columns = {
        { "pgxid", 1, SchemaType::UINT32, INT4OID, false },
        { "xid", 2, SchemaType::UINT64, INT8OID, false }
    };
    _schema = std::make_shared<ExtentSchema>(columns);
    _pg_xid_f = _schema->get_mutable_field("pgxid");
    _xid_f = _schema->get_mutable_field("xid");

    // prepare an empty extent for buffering
    _extent = std::make_shared<Extent>(ExtentType(), 0, _schema->row_size());

    // start the background syncing thread
    _fsync_thread = std::thread(&PgXactLogWriter::_fsync_worker, this);
}

void
PgXactLogWriter::close()
{
    // atomic set of shutdown flag
    if (_shutdown.exchange(true)) {
        return;
    }

    // wait for the fsync worker to finish
    SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Xact log writer closing file: {}", _file.c_str());
    _fsync_thread.join();

    // once the sync worker finishes, check if we need to do a final flush
    if (!_extent->empty()) {
        _flush_extent(_extent);
    }
    _extent = nullptr;
}

void
PgXactLogWriter::rotate(uint64_t timestamp)
{
    std::unique_lock<std::shared_mutex> lock(_file_mutex);
    std::error_code ec;
    if (std::filesystem::exists(_file, ec)) {
        auto handle = IOMgr::get_instance()->open(_file, IOMgr::IO_MODE::APPEND, true);
        auto response = handle->sync();
        DCHECK(response->is_success());
    }
    _file = fs::create_log_file_with_timestamp(_file.parent_path(), PgLogMgr::LOG_PREFIX_XACT, PgLogMgr::LOG_SUFFIX, timestamp);
    SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "_file = ", _file.c_str());
}

void
PgXactLogWriter::_flush_extent(ExtentPtr extent)
{
    std::shared_lock<std::shared_mutex> lock(_file_mutex);
    auto handle = IOMgr::get_instance()->open(_file, IOMgr::IO_MODE::APPEND, true);
    auto response = extent->async_flush(handle);
}

void
PgXactLogWriter::_fsync_worker()
{
    while (!_shutdown) {
        // sleep for at least PG_LOG_MIN_FSYNC_MS
        std::this_thread::sleep_for(std::chrono::milliseconds(PG_XLOG_MIN_FSYNC_MS));

        // check for shutdown request
        if (_shutdown) {
            break;
        }

        // check if the extent needs to be flushed
        std::unique_lock lock(_mutex);
        if (_extent->empty()) {
            continue;
        }

        // swap the extent object
        // note: we aren't going through the StorageCache here
        auto sync_extent = _extent;
        _extent = std::make_shared<Extent>(ExtentType(), 0, _schema->row_size());
        lock.unlock();

        // flush the data to disk
        _flush_extent(sync_extent);
    }
}

void
PgXactLogWriter::log(uint32_t pg_xid,
                     uint64_t xid)
{
    std::unique_lock lock(_mutex);

    // get a new row in the current extent buffer
    auto row = _extent->append();

    // write the data to the row
    _pg_xid_f->set_uint32(row, pg_xid);
    _xid_f->set_uint64(row, xid);
}

}  // namespace springtail::pg_log_mgr
