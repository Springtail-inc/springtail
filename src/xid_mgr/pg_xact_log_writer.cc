#include <fcntl.h>

#include <common/exception.hh>
#include <common/logging.hh>
#include <xid_mgr/pg_xact_log_writer.hh>

namespace springtail::xid_mgr {

PgXactLogWriter::PgXactLogWriter(const std::filesystem::path &base_dir, uint64_t recovered_xid) :
    _base_dir(base_dir),
    _last_stored_xid(recovered_xid)
{
    // create the base directory for the file if it doesn't exist
    LOG_INFO("Creating directory {}", base_dir);
    CHECK_EQ(sizeof(XidElement), 16); // assumes that XidElement divides PG_XLOG_PAGE_SIZE

    std::filesystem::create_directories(_base_dir);

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
        try {
            flush();
        } catch (...) {
            LOG_ERROR("Faild to flush xact log {}", _base_dir.string());
        }
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

        ::close(_fd);
        _fd = -1;
        _can_flush = false;
    }
    _file = file;

    LOG_DEBUG(LOG_XID_MGR, LOG_LEVEL_DEBUG1, "Next Xact file: {}", _file.string());

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
    }

    _offset = 0;
    _first_file = false;
    _current_log_timestamp = timestamp;
}

void
PgXactLogWriter::log(uint32_t pg_xid, uint64_t xid, bool real_commit)
{
    LOG_DEBUG(LOG_XID_MGR, LOG_LEVEL_DEBUG1, "Recording xid in log: file: {}, pg_xid: {}, xid: {}, real_commit: {}, offset: {}",
              _file.string(), pg_xid, xid, real_commit, _offset);

    DCHECK(_offset + XidElement::PACKED_SIZE <= PG_XLOG_PAGE_SIZE)
        << "Offset must be less than page size";

    // set xid data in the mapped memory
    // write to the _write_buffer the XidElement data
    XidElement xid_element = {xid, pg_xid, real_commit};
    _offset += XidElement::pack(&_write_buffer[_offset], xid_element);

    if (real_commit) {
        CHECK_GT(xid, _last_stored_xid) << "XID must be greater than the last stored XID";
        _last_stored_xid = xid;
    }

    // check if we reached the end of the write buffer, if so write it out
    if (_offset + XidElement::PACKED_SIZE >= PG_XLOG_PAGE_SIZE) {
        // write the buffer to the file
        auto ret = ::write(_fd, _write_buffer, _offset);
        if (ret == -1) {
            throw Error(fmt::format("Failed to write to file {}; error {}: {}", _file.string(), errno, strerror(errno)));
        }
        _offset = 0;
    }
    _can_flush = true;
}

void
PgXactLogWriter::cleanup(uint64_t min_timestamp, bool archive_logs)
{
    fs::cleanup_files_from_dir(_base_dir, LOG_PREFIX_XACT, LOG_SUFFIX, min_timestamp, archive_logs);
}

void
PgXactLogWriter::_extract_last_xid()
{
    char read_buffer[PG_XLOG_PAGE_SIZE];
    size_t current_offset = 0;
    bool done = false;

    // iterate through the file and extract the last committed xid
    // this also sets the fd offset to the end of the file
    while (!done)
    {
        auto ret = ::read(_fd, read_buffer, PG_XLOG_PAGE_SIZE);
        if (ret == 0) {
            // end of file reached
            break;
        }

        if (ret == -1) {
            throw Error(fmt::format("Error reading from file {}; error {}: {}", _file.string(), errno, strerror(errno)));
        }

        current_offset = 0;
        while (current_offset + XidElement::PACKED_SIZE <= ret) {
            XidElement current_xid;
            current_offset += XidElement::unpack(&read_buffer[current_offset], current_xid);
            if (current_xid.xid == 0) {
                // this is legacy, we shouldn't see 0 xids
                done = true;
                break;
            }

            if (current_xid.real_commit) {
                _last_stored_xid = current_xid.xid;
            }
        }
    }

    LOG_DEBUG(LOG_XID_MGR, LOG_LEVEL_DEBUG1, "Extracted last XID, file: {}, last xid: {}",
              _file.string(), _last_stored_xid);
}

void
PgXactLogWriter::flush()
{
    if (_fd == -1 || !_can_flush) {
        return;
    }

    if (_offset > 0) {
        DCHECK_EQ(_offset % XidElement::PACKED_SIZE, 0)
            << "Offset must be a multiple of XidElement size";

        // write the buffer to the file
        auto ret = ::write(_fd, _write_buffer, _offset);
        if (ret == -1) {
            throw Error(fmt::format("Failed to write to file {}; error {}: {}", _file.string(), errno, strerror(errno)));
        }
        _offset = 0;
    }

    ::fsync(_fd);

    _can_flush = false;
}

// TODO: need unit tests for this function
uint64_t
PgXactLogWriter::set_last_xid_in_storage(std::filesystem::path base_dir,
                                         uint64_t last_xid,
                                         bool archive)
{
    char read_buffer[PG_XLOG_PAGE_SIZE];
    std::filesystem::path last_committed_xid_file;
    size_t last_committed_xid_offset = 0;
    uint32_t last_committed_xid_page_count = 0;
    uint64_t last_committed_xid = 0;

    // loop over files in the directory
    bool xid_found = false;
    auto current_file = fs::find_earliest_modified_file(base_dir, LOG_PREFIX_XACT, LOG_SUFFIX);
    while (!xid_found && current_file.has_value())
    {
        LOG_DEBUG(LOG_XID_MGR, LOG_LEVEL_DEBUG1, "Processing file: {}", current_file->string());
        bool done = false;

        int fd = ::open(current_file.value().c_str(), O_RDWR, 0660);
        if (fd == -1) {
            throw Error(fmt::format("Failed to open file {}; error {}: {}", current_file.value().string(), errno, strerror(errno)));
        }

        // loop over pages in the file; read the file in chunks of PG_XLOG_PAGE_SIZE
        uint32_t page_count = 0;
        while (!done && !xid_found)
        {
            auto ret = ::read(fd, read_buffer, PG_XLOG_PAGE_SIZE);
            if (ret == 0) {
                // end of file reached
                break;
            }

             if (ret == -1) {
                throw Error(fmt::format("Error reading from file {}; error {}: {}", current_file.value().string(), errno, strerror(errno)));
            }
            ++page_count;

            // loop over the read buffer and check for XidElement entries
            size_t current_offset = 0;
            while (current_offset + XidElement::PACKED_SIZE <= ret)
            {
                XidElement current_xid;
                current_offset += XidElement::unpack(&read_buffer[current_offset], current_xid);

                LOG_DEBUG(LOG_XID_MGR, LOG_LEVEL_DEBUG1, "Current entry: pg_xid = {}; xid = {}; real_commit = {}", current_xid.pg_xid, current_xid.xid, current_xid.real_commit);
                if (current_xid.xid == 0) {
                    done = true;
                    break;
                }

                DCHECK_GT(current_xid.xid, last_committed_xid)
                    << "Current xid must be greater than last committed xid";

                // check if the current xid is greater than the last xid, if so we can stop processing
                if (current_xid.xid > last_xid) {
                    LOG_DEBUG(LOG_XID_MGR, LOG_LEVEL_DEBUG1, "Current xid {} is greater than last_xid {}", current_xid.xid, last_xid);
                    xid_found = true;
                    break;
                }

                // record location of the last committed xid; this is used to truncate the file later
                if (current_xid.real_commit) {
                    last_committed_xid_file = current_file.value();
                    last_committed_xid_offset = current_offset;
                    last_committed_xid_page_count = page_count;
                    last_committed_xid = current_xid.xid;
                    LOG_INFO("Most recent real commit: xid = {}, file = {}, page = {}, offset = {}",
                        last_committed_xid, last_committed_xid_file.string(), last_committed_xid_page_count, last_committed_xid_offset);
                }
            }
        }
        ::close(fd);
        current_file = fs::get_next_log_file(current_file.value(), LOG_PREFIX_XACT, LOG_SUFFIX);
    }

    if (last_committed_xid == 0) {
        LOG_DEBUG(LOG_XID_MGR, LOG_LEVEL_DEBUG1, "No committed XID found in the log files");
        // no committed XID found, cleanup all files
        fs::cleanup_files_from_dir<std::greater<uint64_t>>(base_dir, LOG_PREFIX_XACT, LOG_SUFFIX, 0, archive);
        return 0;
    }

    // last_committed_xid != 0
    // truncate the file to the appropriate length
    int fd = ::open(last_committed_xid_file.c_str(), O_RDWR, 0660);
    if (fd == -1) {
        throw Error(fmt::format("Failed to open file {}; error {}: {}", last_committed_xid_file.string(), errno, strerror(errno)));
    }

    LOG_DEBUG(LOG_XID_MGR, LOG_LEVEL_DEBUG1, "Truncating file: {}, last committed xid: {}, offset: {}, page count: {}",
        last_committed_xid_file.string(), last_committed_xid, last_committed_xid_offset, last_committed_xid_page_count);

    size_t file_size = PG_XLOG_PAGE_SIZE * (last_committed_xid_page_count - 1) + last_committed_xid_offset;
    int err = ::ftruncate(fd, file_size); // truncate to the last committed xid offset
    CHECK_EQ(err, 0);
    ::close(fd);

    // cleanup leftover files
    auto file_timestamp = fs::extract_timestamp_from_file(last_committed_xid_file, LOG_PREFIX_XACT, LOG_SUFFIX);
    if (!file_timestamp.has_value()) {
        throw Error(fmt::format("Error: file name {} does not include timestamp", last_committed_xid_file.string()));
    }

    fs::cleanup_files_from_dir<std::greater<uint64_t>>(base_dir, LOG_PREFIX_XACT, LOG_SUFFIX, file_timestamp.value(), archive);

    return last_committed_xid;
}


} // namespace springtail::pg_log_mgr
