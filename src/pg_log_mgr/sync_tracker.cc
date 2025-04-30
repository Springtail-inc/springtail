#include <pg_log_mgr/sync_tracker.hh>

namespace springtail::pg_log_mgr {

    bool
    SyncTracker::mark_resync(uint64_t db_id,
                             uint64_t table_id,
                             const XidLsn &xid)
    {
        LOG_DEBUG(LOG_PG_LOG_MGR, "db {} table {} xid {}:{}",
                            db_id, table_id, xid.xid, xid.lsn);
        boost::unique_lock lock(_mutex);

        bool first_table = (_resync_map[db_id].empty() &&
                            !_inflight_map.contains(db_id) &&
                            !_sync_map.contains(db_id));

        // add the table to the resync map; will get removed when mark_inflight() is called
        _resync_map[db_id][table_id].insert(xid);

        return first_table;
    }

    void
    SyncTracker::mark_inflight(uint64_t db_id,
                               uint64_t table_id,
                               const XidLsn &xid)
    {
        LOG_DEBUG(LOG_PG_LOG_MGR, "db {} table {} xid {}:{}",
                            db_id, table_id, xid.xid, xid.lsn);
        boost::unique_lock lock(_mutex);

        // find the db map
        auto db_i = _resync_map.find(db_id);
        CHECK(db_i != _resync_map.end());
        if (db_i != _resync_map.end()) {
            auto table_i = db_i->second.find(table_id);
            CHECK(table_i != db_i->second.end());

            // clear from the resync map
            table_i->second.erase(xid);
            if (table_i->second.empty()) {
                db_i->second.erase(table_i);
                if (db_i->second.empty()) {
                    _resync_map.erase(db_i);
                }
            }
        }

        // add to the in-flight map; will get removed when add_sync() is called
        _inflight_map[db_id].insert(table_id);
    }

    bool
    SyncTracker::add_sync(const pg_log_mgr::PgXactMsg::TableSyncMsg &sync_msg)
    {
        LOG_DEBUG(LOG_PG_LOG_MGR, "db {} xid {}", sync_msg.db_id, sync_msg.target_xid);
        boost::unique_lock lock(_mutex);

        // clear the inflight map for the provided tables in the db
        auto inflight_i = _inflight_map.find(sync_msg.db_id);
        if (inflight_i != _inflight_map.end()) {
            for (auto &entry : sync_msg.tids) {
                inflight_i->second.erase(entry->table_id); // remove the table from the resync map
            }

            // remove the db from the map if the table set is empty
            if (inflight_i->second.empty()) {
                _inflight_map.erase(inflight_i);
            }
        }

        // find the db in the _sync_map
        auto db_i = _sync_map.find(sync_msg.db_id);

        // check if this is the first table(s) to be added for syncing
        // note: we also check the resync map since adding a resync also forces commits to stop
        bool first_table = ((db_i == _sync_map.end()) &&
                            (inflight_i == _inflight_map.end()) &&
                            (_resync_map.find(sync_msg.db_id) == _resync_map.end()));

        // make a record of the table mapping(s)
        auto record = std::make_shared<XidRecord>(sync_msg);
        LOG_DEBUG(LOG_PG_LOG_MGR, "XidRecord: pg_xid={} xmax={} xmin={}", sync_msg.pg_xid,
                  sync_msg.xmax, sync_msg.xmin);

        // make sure that the database has entries in the maps
        auto &db_map = (db_i == _sync_map.end()) ? _sync_map[sync_msg.db_id] : db_i->second;
        auto &table_map = _table_map[sync_msg.db_id];

        // check if we already have a record of a previous sync for this table
        for (const auto &entry : record->tids()) {
            auto table_i = table_map.find(entry->table_id);
            if (table_i == table_map.end()) {
                continue;
            }

            // remove the existing entry to ensure we track only the latest un-swapped sync
            db_map.erase(table_i->second->pg_xid());
        }

        // store it against the pg_xid for this sync's snapshot
        db_map[sync_msg.pg_xid] = record;

        // also keep a map to the record for each table being copied
        for (auto &entry : sync_msg.tids) {
            table_map[entry->table_id] = record; // add the record to the sync map
        }

        // record the target XID of the sync
        _target_xid_map[sync_msg.db_id] = sync_msg.target_xid;

        return first_table;
    }

    std::shared_ptr<SyncTracker::SwapRequest>
    SyncTracker::check_commit(uint64_t db_id,
                              uint32_t pg_xid)
    {
        LOG_DEBUG(LOG_PG_LOG_MGR, "db {} pg_xid {}", db_id, pg_xid);
        boost::unique_lock lock(_mutex);

        // get the map for this database
        auto db_i = _sync_map.find(db_id);
        if (db_i == _sync_map.end()) {
            LOG_DEBUG(LOG_PG_LOG_MGR, "didn't find db {} pg_xid {}", db_id, pg_xid);
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
            LOG_DEBUG(LOG_PG_LOG_MGR, "no completed db {} pg_xid {}", db_id, pg_xid);
            return {};
        }

        auto type = committer::XidReady::Type::TABLE_SYNC_SWAP;
        if (db_i->second.empty() &&
            _inflight_map.find(db_id) == _inflight_map.end() &&
            _resync_map.find(db_id) == _resync_map.end()) {
            // there should be no in-flight copies or resync requests in order to commit
            // note: we had to add the check to the resync map to ensure that a second resync seen
            //       between the original resync request and the sync completion was also processed
            //       before the commit
            type = committer::XidReady::Type::TABLE_SYNC_COMMIT;
            _sync_map.erase(db_i);
            _target_xid_map.erase(db_id);
        }

        // construct an XidReady record from the XidRecord objects
        std::vector<PgCopyResult::TableInfoPtr> tids;
        for (auto record : completed) {
            tids.insert(tids.end(), record->tids().begin(), record->tids().end());
        }
        LOG_DEBUG(LOG_PG_LOG_MGR, "Found {} tables", tids.size());

        LOG_DEBUG(LOG_PG_LOG_MGR, "Creating commit_queue message for committer queue db: {}, xid: {}, type: {}", db_id, _target_xid_map[db_id], std::string(1,type));
        return std::make_shared<SwapRequest>(type, db_id, std::move(tids));
    }

    void
    SyncTracker::clear_tables(std::shared_ptr<SwapRequest> swap)
    {
        LOG_DEBUG(LOG_PG_LOG_MGR, "db {}", swap->db());
        boost::unique_lock lock(_mutex);

        // get the table map for this database
        auto db_i = _table_map.find(swap->db());
        assert(db_i != _table_map.end());

        // remove all of the tables referenced in the commit message
        for (const auto &entry : swap->table_info()) {
            db_i->second.erase(entry->table_id);
        }
    }

    SyncTracker::SkipDetails
    SyncTracker::should_skip(uint64_t db_id,
                             uint64_t table_id,
                             uint32_t pg_xid) const
    {
        boost::shared_lock lock(_mutex);

        LOG_DEBUG(LOG_PG_LOG_MGR, "db {} table_id {} pg_xid {}", db_id, table_id, pg_xid);

        // first check the resync map
        auto resync_i = _resync_map.find(db_id);
        if (resync_i != _resync_map.end()) {
            if (resync_i->second.contains(table_id)) {
                return { true, true }; // if the table is present, skip
            }
        }

        // then check the inflight map
        auto inflight_i = _inflight_map.find(db_id);
        if (inflight_i != _inflight_map.end()) {
            if (inflight_i->second.contains(table_id)) {
                return { true, true }; // if the table is present, skip
            }
        }

        // then check the table map
        auto db_i = _table_map.find(db_id);
        if (db_i == _table_map.end()) {
            return { false, false };
        }

        auto table_i = db_i->second.find(table_id);
        if (table_i == db_i->second.end()) {
            return { false, false };
        }

        bool should_skip = table_i->second->should_skip(pg_xid);
        if (should_skip) {
            return { true, true };
        } else {
            // find the schema in the table entry
            auto it = std::find_if(table_i->second->tids().begin(), table_i->second->tids().end(),
                                   [table_id](const auto &entry) {
                                       return (entry->table_id == table_id);
                                   });
            CHECK(it != table_i->second->tids().end());
            return { true, false, (*it)->schema };
        }
    }
}
