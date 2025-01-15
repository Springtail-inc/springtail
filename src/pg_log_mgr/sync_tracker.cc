#include <pg_log_mgr/sync_tracker.hh>

namespace springtail::pg_log_mgr {

    bool
    SyncTracker::mark_resync(uint64_t db_id,
                             uint64_t table_id)
    {
        SPDLOG_DEBUG_MODULE(LOG_GC, "db {} table {}", db_id, table_id);
        boost::unique_lock lock(_mutex);

        bool first_table = _resync_map[db_id].empty() && !_sync_map.contains(db_id);

        // add the table to the resync map; will get removed when add_sync() is called
        _resync_map[db_id].insert(table_id);

        return first_table;
    }

    bool
    SyncTracker::add_sync(const pg_log_mgr::PgXactMsg::TableSyncMsg &sync_msg)
    {
        SPDLOG_DEBUG_MODULE(LOG_GC, "db {} xid {}", sync_msg.db_id, sync_msg.target_xid);
        boost::unique_lock lock(_mutex);

        // clear the resync map for the provided tables in the db
        auto resync_i = _resync_map.find(sync_msg.db_id);
        if (resync_i != _resync_map.end()) {
            for (int32_t table_id : sync_msg.tids) {
                resync_i->second.erase(table_id); // remove the table from the resync map
            }

            // remove the db from the map if the table set is empty
            if (resync_i->second.empty()) {
                _resync_map.erase(resync_i);
            }
        }

        // find the db in the _sync_map
        auto db_i = _sync_map.find(sync_msg.db_id);

        // check if this is the first table(s) to be added for syncing
        // note: we also check the resync map since adding a resync also forces commits to stop
        bool first_table = ((db_i == _sync_map.end()) && (resync_i == _resync_map.end()));

        // make a record of the table mapping(s)
        auto record = std::make_shared<XidRecord>(sync_msg);

        // make sure that the database has entries in the maps
        auto &db_map = (db_i == _sync_map.end()) ? _sync_map[sync_msg.db_id] : db_i->second;
        auto &table_map = _table_map[sync_msg.db_id];

        // check if we already have a record of a previous sync for this table
        for (auto &tid : record->tids()) {
            auto table_i = table_map.find(tid);
            if (table_i == table_map.end()) {
                continue;
            }

            // remove the existing entry to ensure we track only the latest un-swapped sync
            db_map.erase(table_i->second->pg_xid());
        }

        // store it against the pg_xid for this sync's snapshot
        db_map[sync_msg.pg_xid] = record;

        // also keep a map to the record for each table being copied
        for (int32_t table_id : sync_msg.tids) {
            table_map[table_id] = record; // add the record to the sync map
        }

        // record the target XID of the sync
        _target_xid_map[sync_msg.db_id] = sync_msg.target_xid;

        return first_table;
    }

    std::optional<gc::XidReady>
    SyncTracker::check_commit(uint64_t db_id,
                              uint32_t pg_xid)
    {
        SPDLOG_DEBUG_MODULE(LOG_GC, "db {} pg_xid {}", db_id, pg_xid);
        boost::unique_lock lock(_mutex);

        // get the map for this database
        auto db_i = _sync_map.find(db_id);
        if (db_i == _sync_map.end()) {
            SPDLOG_DEBUG_MODULE(LOG_GC, "didn't find db {} pg_xid {}", db_id, pg_xid);
            return {}; // no ongoing sync
        }

        // find any XidRecords that wouldn't be skipped by this pg_xid
        std::vector<std::shared_ptr<XidRecord>> completed;

        auto sync_i = db_i->second.begin();
        while (sync_i != db_i->second.end()) {
            auto current_i = sync_i++;

            // check each XidRecord
            if (!current_i->second->should_skip(pg_xid)) {
                completed.push_back(current_i->second);

                // clear from the _sync_map
                // note: we don't clear the _table_map here in case there are prior XIDs in-flight
                db_i->second.erase(current_i);
            }
        }

        // if nothing is completed, then we have to wait to swap/commit
        if (completed.empty()) {
            SPDLOG_DEBUG_MODULE(LOG_GC, "no completed db {} pg_xid {}", db_id, pg_xid);
            return {};
        }

        // We pass the target XID to the committer to be used if the completed XID is not ahead of the committed XID.
        uint64_t xid = _target_xid_map[db_id];

        auto type = gc::XidReady::Type::TABLE_SYNC_SWAP;
        if (db_i->second.empty()) {
            type = gc::XidReady::Type::TABLE_SYNC_COMMIT;
            _sync_map.erase(db_i);
            _target_xid_map.erase(db_id);
        }

        // construct an XidReady record from the XidRecord objects
        std::vector<uint64_t> tids;
        for (auto record : completed) {
            tids.insert(tids.end(), record->tids().begin(), record->tids().end());
        }
        SPDLOG_DEBUG_MODULE(LOG_GC, "Found {} tables", tids.size());

        return gc::XidReady(type, db_id, gc::XidReady::SwapMsg(xid, std::move(tids)));
    }

    void
    SyncTracker::clear_tables(uint64_t db_id,
                              const gc::XidReady &commit_msg)
    {
        SPDLOG_DEBUG_MODULE(LOG_GC, "db {}", db_id);
        boost::unique_lock lock(_mutex);

        // get the table map for this database
        auto db_i = _table_map.find(db_id);
        assert(db_i != _table_map.end());

        // remove all of the tables referenced in the commit message
        for (auto table_id : commit_msg.swap().tids()) {
            db_i->second.erase(table_id);
        }
    }

    bool
    SyncTracker::should_skip(uint64_t db_id,
                             uint64_t table_id,
                             uint32_t pg_xid) const
    {
        boost::shared_lock lock(_mutex);

        SPDLOG_DEBUG_MODULE(LOG_GC, "db {} table_id {} pg_xid {}", db_id, table_id, pg_xid);

        // first check the resync map
        auto resync_i = _resync_map.find(db_id);
        if (resync_i != _resync_map.end()) {
            if (resync_i->second.contains(table_id)) {
                return true; // if the table is present, skip
            }
        }

        // then check the table map
        auto db_i = _table_map.find(db_id);
        if (db_i == _table_map.end()) {
            return false;
        }

        auto table_i = db_i->second.find(table_id);
        if (table_i == db_i->second.end()) {
            return false;
        }

        return table_i->second->should_skip(pg_xid);
    }

}
