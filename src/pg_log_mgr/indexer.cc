#include <mutex>
#include <stop_token>
#include <algorithm>
#include <pg_log_mgr/indexer.hh>
#include <common/logging.hh>
#include <common/properties.hh>
#include <sys_tbl_mgr/table_mgr.hh>
#include <sys_tbl_mgr/client.hh>

namespace springtail::committer {

    Indexer::Indexer(uint32_t worker_count, const ReconciliationQueuePtr& index_reconciliation_queue)
        : _index_reconciliation_queue(index_reconciliation_queue)
    {
        CHECK_GT(worker_count, 0);
        for (auto i = 0; i != worker_count; ++i) {
            _workers.emplace_back([this](std::stop_token st) { task(st); });
        }
        LOG_INFO("Indexer created: {}", worker_count);
    }

    void Indexer::process_ddls(uint64_t db_id, uint64_t xid, nlohmann::json const& ddls)
    {
        std::scoped_lock lock(_xid_ddl_counter_map_mtx);
        // Set counter for XID for ddls
        _xid_ddl_counter_map[xid].store(ddls.size());
        for (auto const& ddl: ddls) {
            auto action = ddl["action"];
            if (action == "create_index") {
                build({db_id, xid, ddl});
            } else if (action == "drop_index") {
                drop(db_id, ddl["id"], xid);
            } else if (action == "abort_index") {
                abort_indexes(db_id, ddl["table_id"], xid);
            } else {
                CHECK(false);
            }
        }
    }

    void Indexer::abort_indexes(uint64_t db_id, uint64_t table_id, uint64_t xid)
    {
        std::scoped_lock g(_m, _table_idx_map_mtx);
        if (--_xid_ddl_counter_map[xid] == 0) {
            _xid_ddl_counter_map.erase(xid);
        }
        auto db_it = _table_idx_map.find(db_id);
        if (db_it == _table_idx_map.end()) {
            return; // No entries for this db_id
        }

        auto table_it = db_it->second.find(table_id);
        if (table_it == db_it->second.end()) {
            return; // No entries for this table_id
        }

        // Iterate through all keys and set work_item as ABORTING
        for (const Key& key : table_it->second) {
            auto work_it = _work_set.find(key);
            if (work_it != _work_set.end()) {
                work_it->second._status = IndexStatus::ABORTING;
            }
        }
    }

    void Indexer::build(IndexParams idx)
    {
        std::scoped_lock g(_m, _table_idx_map_mtx);
        Key key(idx._db_id, idx._ddl["id"]);
        // I don't think PG will issue two creates with the same index ID.
        CHECK(_work_set.find(key) == _work_set.end());
        auto client = sys_tbl_mgr::Client::get_instance();
        proto::IndexInfo info = client->get_index_info(idx._db_id, idx._ddl["id"], {idx._xid, constant::MAX_LSN});
        if (info.id() != 0 && static_cast<sys_tbl::IndexNames::State>(info.state()) == sys_tbl::IndexNames::State::READY) {
            // Decrement the counter as we are not going to process the create request
            // as the index already exists
            if (--_xid_ddl_counter_map[idx._xid] == 0) {
                _xid_ddl_counter_map.erase(idx._xid);
            }
        } else {
            // Insert into table-indices map
            _table_idx_map.try_emplace(idx._db_id)
                .first->second
                .try_emplace(idx._ddl["table_id"])
                .first->second.push_back(key);
            _work_set[key] = std::move(idx);
            _queue.push(key);
            // notify workers about new items
            _cv.notify_one();
        }
    }

    void Indexer::drop(uint64_t db_id, uint64_t index_id, uint64_t xid)
    {
        std::scoped_lock g(_m);
        Key key(db_id, index_id);
        auto it = _work_set.find(key);
        if (it == _work_set.end()) {
            // note: the work item has _ddl.empty() == true
            // it means to drop the index right away
            _work_set[key] = {db_id, xid, {}, IndexStatus::DELETING};
            _queue.push(key);
            _cv.notify_one();
        } else {
            // mark the status as ABORTING, it will tell the worker to
            // cancel the index build / catchup
            // and proceed for dropping the index
            it->second._status = IndexStatus::ABORTING;

            // Decrement the counter as there is no separate processing
            // needed for this drop as it is only updating existing work item
            if (--_xid_ddl_counter_map[xid] == 0) {
                _xid_ddl_counter_map.erase(xid);
            }
        }
    }

    void Indexer::task(std::stop_token st)
    {
        while(!st.stop_requested()) {
            Key key;
            IndexParams params;

            // get the next work item
            {
                std::unique_lock g(_m);
                if (!_cv.wait(g, st, [this]{ return !_queue.empty(); })) {
                    break;
                }

                key = _queue.front();
                _queue.pop();
                params = _work_set.at(key);
            }
            if (params.is_status(IndexStatus::BUILDING)) {
                _add_to_pending_reconciliation(_build(st, key, params));
            } else {
                _add_to_pending_reconciliation(IndexState{nullptr, key, params, std::numeric_limits<uint64_t>::max()});
            }
        }
        LOG_INFO("Indexer thread joined");
    }

    void Indexer::_drop(const Key& key, const IndexParams& idx, uint64_t end_xid)
    {
        CHECK(idx._ddl.is_null());

        auto [db_id, index_id] = key;
        LOG_INFO("Drop index {}, {}, {}", db_id, index_id, end_xid);

        auto client = sys_tbl_mgr::Client::get_instance();

        IndexParams work_item;
        {
            std::unique_lock g(_m);

            // fetch the latest state of the work item before we erase it
            work_item = _work_set.at(key);
            CHECK(work_item._ddl.is_null());

            _work_set.erase(key);
        }

        XidLsn xid{end_xid, constant::INDEX_COMMIT_LSN};

        proto::IndexInfo info = client->get_index_info(db_id, index_id, xid);
        if (info.id() == 0) {
            //TODO: it seems like PG generates DROP INDEX with table ids, need
            //to investigate it more.
            LOG_INFO("The index is not valid: {}", index_id);
            return;
        }

        auto exists = TableMgr::get_instance()->exists(db_id, info.table_id(), xid.xid, xid.lsn);
        if (!exists) {
            // when dropping a table, PG generates DROP TABLE first
            // following by DROP INDEX. We ignore DROP INDEX after DROP TABLE.
            LOG_INFO("Table doesn't exists: {}, {}", info.table_id(), index_id);
            return;
        }

        // index column positions
        std::vector<uint32_t> idx_cols;
        for (auto const& col : info.columns()) {
            idx_cols.push_back(col.position());
        }

        auto meta = client->get_roots(db_id, info.table_id(), end_xid);
        auto it = std::ranges::find_if(meta->roots,
                [&](auto const& v) { return index_id == v.index_id; });
        CHECK(it != meta->roots.end());

        // XXX: Optimize roundtrips:
        // https://linear.app/springtail/issue/SPR-679/optimize-indexer-to-reduce-roundtrips-to-systblmgr
        auto table =
            TableMgr::get_instance()->get_mutable_table(db_id, info.table_id(), end_xid, end_xid);
        auto root = table->create_index_root(index_id, idx_cols);
        if (it->extent_id != constant::UNKNOWN_EXTENT) {
            root->init(it->extent_id);
        } else {
            root->init_empty();
        }
        root->truncate();
        root->finalize();

        meta->roots.erase(it);
        client->update_roots(db_id, info.table_id(), end_xid, *meta);
        client->set_index_state(db_id, xid, info.table_id(), index_id, sys_tbl::IndexNames::State::DELETED);

        // Cleanup table-index map
        _remove_index_key(db_id, info.table_id(), key);

        LOG_INFO("Index dropped: {}:{} @ {}", db_id, index_id, end_xid);
    }

    Indexer::IndexState
    Indexer::_build(std::stop_token st, const Key& key, const IndexParams& idx)
    {
        constexpr int DROP_CHECK_PERIOD = 1000;

        LOG_DEBUG(LOG_COMMITTER, "Build index: {}:{} - {}", key.first, key.second, idx._ddl.dump());

        auto [db_id, index_id] = key;
        auto tid = idx._ddl["table_id"];

        CHECK_EQ(idx._ddl["action"], "create_index");

        auto client = sys_tbl_mgr::Client::get_instance();
        client->invalidate_table(db_id, tid, XidLsn{idx._xid});

        // index column positions
        std::vector<uint32_t> idx_cols;
        for (auto const& col : idx._ddl["columns"]) {
            idx_cols.push_back(col["position"]);
        }

        std::shared_ptr<std::vector<FieldPtr>> key_fields;

        auto mutable_table = TableMgr::get_instance()->get_mutable_table(db_id, tid, idx._xid, idx._xid);
        MutableBTreePtr root = mutable_table->create_index_root(index_id, idx_cols);
        root->init_empty();
        key_fields = mutable_table->schema()->get_fields(mutable_table->schema()->get_column_names(idx_cols));

        // additional fields in the root schema to keep extent and row ids
        auto value_fields = std::make_shared<FieldArray>(2);
        uint64_t row_cnt = 0;
        uint64_t current_extent_id = 0;
        uint32_t current_row_id = 0;

        LOG_DEBUG(LOG_COMMITTER, "Indexing build in progress: {}:{}", db_id, index_id);
        auto table = TableMgr::get_instance()->get_table(db_id, tid, idx._xid);
        for (auto row_i = table->begin(); row_i != table->end(); ++row_i) {
            if (st.stop_requested()) {
                root->truncate();
                return {nullptr, key, idx, tid};
            }
            // check if the index was dropped
            if (row_cnt % DROP_CHECK_PERIOD == 0 && _was_dropped(key)) {
                return {root, key, idx, tid};
            }
            auto extent_id = row_i.extent_id();

            if (extent_id != current_extent_id) {
                // We are scanning in primary key order. It guarantees that
                // row IDs start from zero and be in ascending order for
                // each new extent. Note: The extent IDs (offsets) may
                // be at any order.
                current_extent_id = extent_id;
                current_row_id = 0;
            }
            (*value_fields)[0] = std::make_shared<ConstTypeField<uint64_t>>(extent_id);
            (*value_fields)[1] = std::make_shared<ConstTypeField<uint32_t>>(current_row_id);

            // insert key
            auto&& svalue = std::make_shared<KeyValueTuple>(key_fields, value_fields, *row_i);
            root->insert(svalue);

            ++current_row_id;
            ++row_cnt;
        }
        LOG_DEBUG(LOG_COMMITTER, "Index build finished: {}:{}, rows={}", db_id, index_id, row_cnt);
        return {root, key, idx, tid};
    }

    bool
    Indexer::_was_dropped(const Key& key)
    {
        std::unique_lock g(_m);
        auto const& params = _work_set.at(key);
        // index drop requested while we've been building it
        return params.is_status(IndexStatus::ABORTING);
    }

    void
    Indexer::_commit_build(MutableBTreePtr root, const Key& key, const IndexParams& idx, uint64_t end_xid)
    {
        auto [db_id, index_id] = key;
        auto tid = idx._ddl["table_id"];
        XidLsn xid{end_xid, constant::INDEX_COMMIT_LSN};

        IndexParams work_item;

        std::unique_lock g(_m);

        // fetch the latest state of the work item before we erase it
        work_item = _work_set.at(key);

        _work_set.erase(key);
        auto client = sys_tbl_mgr::Client::get_instance();
        proto::IndexInfo index_info = client->get_index_info(db_id, index_id, xid);
        auto index_deleted = static_cast<sys_tbl::IndexNames::State>(index_info.state()) == sys_tbl::IndexNames::State::DELETED;

        if (!root) {
            // if IndexStatus is BUILDING - stop could have got requested, so the index
            //                              will be rebuilt during restart
            // If IndexStatus is ABORTING - Drop came before build was even picked,
            //                              mark the state as DELETED

            if (work_item.is_status(IndexStatus::ABORTING)) {
                auto table_exists = TableMgr::get_instance()->exists(db_id, tid, xid.xid, xid.lsn);
                if (table_exists && !index_deleted) {
                    // when dropping a table, PG generates DROP TABLE first
                    // following by DROP INDEX. We ignore DROP INDEX after DROP TABLE, because
                    // indexes will be set as DELETED directly as part of sys_tbl_mgr DROP TABLE
                    client->set_index_state(db_id, xid, tid, index_id, sys_tbl::IndexNames::State::DELETED);
                }
            }
        } else {
            // Index building was attempted, finalize and process build/abort
            auto extent_id = root->finalize();
            if (work_item.is_status(IndexStatus::BUILDING)) {
                auto meta = client->get_roots(db_id, tid, end_xid);
                meta->roots.emplace_back(key.second, extent_id);
                client->update_roots(db_id, tid, end_xid, *meta);
                client->set_index_state(db_id, xid, tid, index_id, sys_tbl::IndexNames::State::READY);
            } else if (work_item.is_status(IndexStatus::ABORTING)) {
                // the index was deleted while we were building it
                // lets also finalize here as part of the tree
                // may have got finalized while we were building.
                root->truncate();
                root->finalize();
                if (!index_deleted) {
                    client->set_index_state(db_id, xid, tid, index_id, sys_tbl::IndexNames::State::DELETED);
                }
            }
        }

        // Cleanup table-index map
        _remove_index_key(db_id, tid, key);
    }

    // Index reconciliation flows
    void
    Indexer::_add_to_pending_reconciliation(IndexState&& idx_state)
    {
        std::scoped_lock lock(_pending_reconciliation_map_mtx, _xid_ddl_counter_map_mtx);
        auto [db_id, index_id] = idx_state._key;
        _pending_idx_reconciliation_map
            .try_emplace(db_id)                  // Ensure db_id entry exists
            .first->second
            .try_emplace(idx_state._idx._xid)     // Ensure xid entry exists
            .first->second.push_back(std::move(idx_state)); // Add IndexState to the list

        // Push to index reconciliation reader to notify committer
        // only after all the DDLs of XID are processed
        if (--_xid_ddl_counter_map[idx_state._idx._xid] == 0) {
            _xid_ddl_counter_map.erase(idx_state._idx._xid);
            _index_reconciliation_queue->push(std::make_shared<IndexReconcileRequest>(db_id, idx_state._idx._xid));
        }
    }

    void
    Indexer::process_index_reconciliation(uint64_t db_id, uint64_t reconcile_xid, uint64_t end_xid) {
        std::scoped_lock lock(_pending_reconciliation_map_mtx);

        auto db_it = _pending_idx_reconciliation_map.find(db_id);
        CHECK(db_it != _pending_idx_reconciliation_map.end());

        auto& xid_map = db_it->second;
        CHECK(!xid_map.empty());

        // Get the entry for the reconcile_xid
        auto xid_it = xid_map.find(reconcile_xid);
        CHECK((xid_it != xid_map.end()));

        auto& idx_list = xid_it->second;
        // Process each entry in the list
        for (auto& idx_state : idx_list) {
            _reconcile_index(idx_state, end_xid);
        }

        // Clean up if entries are empty
        xid_map.erase(xid_it); // Remove processed xid entry
        if (xid_map.empty()) {
            _pending_idx_reconciliation_map.erase(db_it); // Remove empty db_id entry
        }
    }

    void
    Indexer::_reconcile_index(IndexState& idx_state, uint64_t end_xid)
    {
        auto [db_id, index_id] = idx_state._key;
        auto is_fresh_drop = false;
        auto is_drop_while_processing = false;
        {
            std::unique_lock g(_m);

            // fetch the latest state of the work item before we proceed for catchup
            const auto& work_item = _work_set.at(idx_state._key);
            // When a fresh work item comes in for drop index
            // there wont be any DDL
            is_fresh_drop = work_item.is_status(IndexStatus::DELETING);

            // When drop index request comes in while
            // we are in the process of building/catching-up,
            // Proceed for commit phase to decide on abort
            is_drop_while_processing = work_item.is_status(IndexStatus::ABORTING);
        }

        if (is_fresh_drop) {
            // Do clear drop index
            _drop(idx_state._key, idx_state._idx, end_xid);
        } else if (is_drop_while_processing) {
            // since btree inserts have a possibility of partial flush,
            // we will do full flush of root once at whichever stage it is in,
            // truncate and flush again
            _commit_build(idx_state._root, idx_state._key, idx_state._idx, end_xid);
        } else {
            LOG_DEBUG(LOG_COMMITTER, "Index reconciliation in progress: {}:{}", db_id, index_id);

            // index column positions
            std::vector<uint32_t> idx_cols;
            for (auto const& col : idx_state._idx._ddl["columns"]) {
                idx_cols.push_back(col["position"]);
            }

            // Get the next_extent from disk using the stats last offset
            auto table = TableMgr::get_instance()->get_table(db_id, idx_state._tid, idx_state._idx._xid);
            auto next_eid = table->get_stats().end_offset;
            auto next_extent_result = table->read_extent_from_disk(next_eid);
            auto next_extent = next_extent_result.first.value_or(nullptr);

            // If next_extent is available, invalidate previous XID's extents first and then populate all the
            // extents of the next XID
            while (next_extent) {
                // Get the table at the next XID
                // and fetch the page for the extent
                auto next_xid = next_extent->header().xid;
                table = TableMgr::get_instance()->get_table(db_id, idx_state._tid, next_xid);
                auto next_page = StorageCache::get_instance()->get(table->get_table_dir() / constant::DATA_FILE, next_eid, next_xid);

                // If previous offset exists, lets invalidate that first
                if (auto prev_eid = next_page->header().prev_offset; prev_eid != constant::UNKNOWN_EXTENT) {

                    // Get the previous extent and its schema
                    auto [prev_extent, tmp_next_eid] = table->read_extent_from_disk(prev_eid);
                    auto prev_xid = prev_extent.value()->header().xid;
                    auto prev_schema = SchemaMgr::get_instance()->get_extent_schema(db_id, idx_state._tid, XidLsn(prev_xid));
                    auto prev_page = StorageCache::get_instance()->get(table->get_table_dir() / constant::DATA_FILE, prev_eid, prev_xid);

                    // and invalidate index for the rows in the prev page
                    indexer_helpers::invalidate_index_for_page(prev_eid, prev_page, idx_state._root, idx_cols, prev_schema);
                }

                // Populate index for the rows in the next page
                indexer_helpers::populate_index_for_page(next_eid, next_page, idx_state._root, idx_cols, table->schema());

                // Get the next extent if next_offset is present, else exit the reconciliation
                next_eid = next_extent_result.second.value_or(0);
                if (next_eid > 0) {
                    next_extent_result = table->read_extent_from_disk(next_eid);
                    next_extent = next_extent_result.first.value_or(nullptr);
                } else {
                    break;
                }
            }

            LOG_DEBUG(LOG_COMMITTER, "Initiating Index commit: {}:{}", db_id, index_id);
            _commit_build(idx_state._root, idx_state._key, idx_state._idx, end_xid);
        }
    }

    void Indexer::_remove_index_key(uint64_t db_id, uint64_t table_id, const Key& key)
    {
        std::scoped_lock lock(_table_idx_map_mtx);
        auto db_it = _table_idx_map.find(db_id);
        if (db_it != _table_idx_map.end()) {
            auto& table_map = db_it->second;
            auto table_it = table_map.find(table_id);
            if (table_it != table_map.end()) {
                auto& key_list = table_it->second;
                key_list.remove(key);  // Remove the key if it exists

                // Clean up empty entries
                if (key_list.empty()) {
                    table_map.erase(table_it);
                    if (table_map.empty()) {
                        _table_idx_map.erase(db_it);
                    }
                }
            }
        }
    }

}  // namespace springtail::gc
