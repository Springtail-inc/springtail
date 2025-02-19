#include <xid_mgr/xid_partition.hh>

#include <absl/log/check.h>
#include <common/common.hh>
#include <common/logging.hh>

namespace springtail::xid_mgr {

    Partition::Partition(const std::filesystem::path &base_path, int id)
            : _id(id),
              _path(base_path / (std::string(PARTITION_FILE_PREFIX) + std::to_string(id))),
              _dirty(false),
              _shutdown(false)
    {
        _fd = ::open(_path.c_str(), O_RDWR | O_CREAT, 0644);
        if (_fd < 0) {
            throw Error("Failed to open xid_mgr file");
        }

        SPDLOG_DEBUG_MODULE(LOG_XID_MGR, "Partition: opened file: {}, dirty={}", _path, _dirty);

        _sync_thread = std::thread(&Partition::_sync_thread_func, this);
    }

    Partition::~Partition()
    {
        ::close(_fd);
    }

    void
    Partition::_sync_thread_func()
    {
        auto token = logging::set_context_variables({{"partition_id", std::to_string(_id)}});
        while (!_shutdown) {
            std::unique_lock<std::mutex> _shutdown_lock(_shutdown_mutex);
            _shutdown_cv.wait_for(_shutdown_lock, std::chrono::seconds(SYNC_SLEEP_TIME_SECS));

            if (_dirty) {
                _dirty = false; // reset dirty flag before we write them out
                _write_committed_xids();
            }
        }

        // shutdown, write out the last batch
        if (_dirty) {
            _write_committed_xids();
        }

        SPDLOG_DEBUG_MODULE(LOG_XID_MGR, "Partition: sync thread exiting");
    }

    void
    Partition::_write_committed_xids()
    {
        // first we dump the map into a buffer, then write it out to the file
        // so only need a shared lock here
        std::shared_lock lock(_map_mutex);

        uint64_t num_entries = _committed_xids.size();

        assert (num_entries * 16 <= BUFFER_SIZE);

        char buffer[16 * num_entries + 16];

        // write out header, magic followed by number of entries
        std::copy_n(reinterpret_cast<const char*>(&MAGIC_HDR), sizeof(MAGIC_HDR), buffer);
        std::copy_n(reinterpret_cast<const char*>(&num_entries), sizeof(num_entries), buffer + sizeof(MAGIC_HDR));

        int len = sizeof(MAGIC_HDR) + sizeof(num_entries);

        // iterate over committed xids and write them out
        for (auto &it : _committed_xids) {
            uint64_t db_id = it.first;
            uint64_t xid = it.second;
            char *buf = buffer + len;

            std::copy_n(reinterpret_cast<char*>(&db_id), sizeof(db_id), buf);
            std::copy_n(reinterpret_cast<char*>(&xid), sizeof(xid), buf + sizeof(db_id));
            len += sizeof(db_id) + sizeof(xid);
        }

        SPDLOG_DEBUG_MODULE(LOG_XID_MGR, "Partition: writing committed xids for {}", _path);

        lock.unlock();

        // lock the file lock and write it out, then fsync
        // shouldn't be any contention here since we are the only writer (from the sync thread)
        std::unique_lock file_lock(_file_mutex);

        ::lseek(_fd, 0, SEEK_SET);
        CHECK_EQ(::write(_fd, buffer, len), len);
        CHECK_EQ(::fsync(_fd), 0);
    }

    void
    Partition::_read_committed_xids()
    {
        char buffer[BUFFER_SIZE + 16];

        std::unique_lock<std::mutex> lock(_file_mutex);

        ::lseek(_fd, 0, SEEK_SET);

        int res = ::read(_fd, buffer, sizeof(buffer));
        if (res == 0) {
            return;
        }

        // decode header first
        uint64_t magic;
        uint64_t num_entries;

        std::copy_n(buffer, sizeof(magic), reinterpret_cast<char*>(&magic));
        std::copy_n(buffer + sizeof(magic), sizeof(num_entries), reinterpret_cast<char*>(&num_entries));

        assert (magic == MAGIC_HDR);
        assert (num_entries * 16 <= BUFFER_SIZE);

        int off = sizeof(magic) + sizeof(num_entries);
        for (int i = 0; i < num_entries; i++) {
            uint64_t db_id;
            uint64_t xid;
            std::copy_n(buffer + off, sizeof(db_id), reinterpret_cast<char*>(&db_id));
            off += sizeof(db_id);
            std::copy_n(buffer + off, sizeof(xid), reinterpret_cast<char*>(&xid));
            off += sizeof(xid);
            _committed_xids[db_id] = xid;
            SPDLOG_DEBUG_MODULE(LOG_XID_MGR, "Partition: read committed xids: db_id={}, xid={}", db_id, xid);
        }
    }

    void
    Partition::_add_history(uint64_t db_id,
                            uint64_t xid)
    {
        auto it = _history.find(db_id);
        if (it == _history.end()) {
            _history[db_id] = {xid};
        } else {
            it->second.push_back(xid);
        }
    }

    void
    Partition::commit_xid(uint64_t db_id, uint64_t xid, bool has_schema_changes)
    {
        std::unique_lock lock(_map_mutex);

        // lookup and see if it is latest
        auto it = _committed_xids.find(db_id);
        if (it == _committed_xids.end() || xid > it->second) {
            SPDLOG_DEBUG_MODULE(LOG_XID_MGR, "Partition: commit xid: db_id={}, xid={}", db_id, xid);

            _committed_xids[db_id] = xid;
            _dirty = true;
        }

        // if the XID contains schema changes, add it to the history
        if (has_schema_changes) {
            _add_history(db_id, xid);
        }

        lock.unlock();

        // XXX check if we can clean up history?  or do we need a background thread for that?
    }

    void
    Partition::record_ddl_change(uint64_t db_id,
                                 uint64_t xid)
    {
        std::unique_lock lock(_map_mutex);
        _add_history(db_id, xid);
    }

    uint64_t
    Partition::get_committed_xid(uint64_t db_id, uint64_t schema_xid)
    {
        std::shared_lock lock(_map_mutex);

        auto itx = _committed_xids.find(db_id);
        if (itx == _committed_xids.end()) {
            return 0;
        }

        uint64_t committed_xid = itx->second;

        // if schema XID is zero then we always return the most recent committed XID
        if (schema_xid == 0) {
            SPDLOG_DEBUG_MODULE(LOG_XID_MGR, "Partition: get committed xid: {}", committed_xid);
            return committed_xid;
        }

        // check the schema change history
        auto ith = _history.find(db_id);
        if (ith == _history.end()) {
            // if there is no history for this database, return the most recent commited XID
            SPDLOG_DEBUG_MODULE(LOG_XID_MGR, "Partition: get committed xid: {}", committed_xid);
            return committed_xid;
        }

        auto history = ith->second;
        auto pos_i = std::ranges::upper_bound(history, schema_xid);
        if (pos_i == history.end()) {
            // if the schema XID is ahead of the history, return the most recent commited XID
            SPDLOG_DEBUG_MODULE(LOG_XID_MGR, "Partition: get committed xid: {}", committed_xid);
            return committed_xid;
        }

        // if we found an entry in the history, return the XID directly before that
        SPDLOG_DEBUG_MODULE(LOG_XID_MGR, "Partition: xid limited by schema_xid: {}", (*pos_i) - 1);
        return (*pos_i) - 1;
    }

} // namespace springtail::xid_mgr
