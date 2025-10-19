#include <mutex>
#include <stop_token>
#include <algorithm>

#include <common/logging.hh>
#include <common/properties.hh>
#include <pg_log_mgr/indexer.hh>
#include <sys_tbl_mgr/server.hh>
#include <sys_tbl_mgr/table_mgr.hh>

namespace springtail::committer {

    Indexer::Indexer(uint32_t worker_count,
            std::shared_ptr<pg_log_mgr::IndexReconciliationQueueManager> index_reconciliation_queue_mgr)
        : _index_reconciliation_queue_mgr(index_reconciliation_queue_mgr)
    {
        CHECK_GT(worker_count, 0);
        for (auto i = 0; i != worker_count; ++i) {
            std::string thread_name = fmt::format("IndexWorker_{}", i);
            _workers.emplace_back([this](std::stop_token st) { task(st); });
            pthread_setname_np(_workers.back().native_handle(), thread_name.c_str());
        }
        LOG_INFO("Indexer created: {}", worker_count);
    }

    void Indexer::process_requests(uint64_t db_id, uint64_t xid, const std::list<proto::IndexProcessRequest> &index_requests)
    {
        std::scoped_lock lock(_xid_ddl_counter_map_mtx);
        // Set counter for XID for ddls
        _xid_ddl_counter_map[xid].store(index_requests.size());
        for (auto const& index_request: index_requests) {
            LOG_INFO("Process index request {} for XID: {}@{}, table_id: {}", index_request.action(), db_id, xid, index_request.index().table_id());
            auto &action = index_request.action();
            if (action == "create_index") {
                build({db_id, xid, index_request});
            } else if (action == "drop_index") {
                drop(db_id, index_request.index().id(), xid);
            } else if (action == "abort_index") {
                abort_indexes(db_id, index_request.index().table_id(), xid);
            } else {
                CHECK(false);
            }
        }
    }

    void Indexer::_cleanup_for_db(uint64_t db_id) {
        std::scoped_lock lock(_m, _pending_reconciliation_map_mtx, _table_idx_map_mtx);

        // Clear _work_set entries for the db, to start fresh recovery
        auto it = _work_set.begin();
        while (it != _work_set.end()) {
            if (it->second._db_id == db_id) {
                it = _work_set.erase(it);  // erase returns the next valid iterator
            } else {
                ++it;
            }
        }

        // Cleanup pending reconciliation map for the db
        _pending_idx_reconciliation_map.erase(db_id);

        // Cleanup table_idx_map for the db
        _table_idx_map.erase(db_id);
    }

    void Indexer::recover_indexes(uint64_t db_id) {
        // Cleanup for db from the previous run
        _cleanup_for_db(db_id);

        // Get indexes which were not completed during last shutdown/crash
        auto unfinished_indexes_info = sys_tbl_mgr::Server::get_instance()->get_unfinished_indexes_info(db_id);

        // Get each such XIDs and their indexes
        for (const auto& [index_xid, idx_list]: unfinished_indexes_info.xid_index_map()) {
            std::list<proto::IndexProcessRequest> index_requests;
            for (const auto& index_info: idx_list.indexes()) {
                // Build indexer-known ddl out of the indexes - to be created/dropped
                if (static_cast<sys_tbl::IndexNames::State>(index_info.state())
                        == sys_tbl::IndexNames::State::NOT_READY) {
                    proto::IndexProcessRequest create_idx_request;
                    create_idx_request.set_action("create_index");
                    *create_idx_request.mutable_index() = std::move(index_info);
                    index_requests.push_back(std::move(create_idx_request));
                } else if (static_cast<sys_tbl::IndexNames::State>(index_info.state())
                        == sys_tbl::IndexNames::State::BEING_DELETED) {
                    proto::IndexProcessRequest drop_idx_request;
                    drop_idx_request.set_action("drop_index");
                    *drop_idx_request.mutable_index() = std::move(index_info);
                    index_requests.push_back(std::move(drop_idx_request));
                } else {
                    // We shouldnt hit this point as we picked only the unfinished indexes
                    CHECK(false);
                }
            }

            // Schedule the index processing at its XID
            process_requests(db_id, index_xid, std::move(index_requests));
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
        Key key(idx._db_id, idx._index_request.index().id());
        // I don't think PG will issue two creates with the same index ID.
        CHECK(_work_set.find(key) == _work_set.end());
        auto server = sys_tbl_mgr::Server::get_instance();
        proto::IndexInfo info = server->get_index_info(idx._db_id, idx._index_request.index().id(), {idx._xid, constant::MAX_LSN});
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
                .try_emplace(idx._index_request.index().table_id())
                .first->second.push_back(key);
            _work_set[key] = std::move(idx);
            _queue.push(key);
            _redis_ddl.insert_index_xid(idx._db_id, idx._xid);
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
            _redis_ddl.insert_index_xid(db_id, xid);
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
        CHECK(idx._status == IndexStatus::DELETING);

        auto [db_id, index_id] = key;
        LOG_INFO("Drop index {}, {}, {}", db_id, index_id, end_xid);

        auto server = sys_tbl_mgr::Server::get_instance();

        IndexParams work_item;
        {
            std::unique_lock g(_m);

            // fetch the latest state of the work item before we erase it
            work_item = _work_set.at(key);
            CHECK(idx._status == IndexStatus::DELETING);

            _work_set.erase(key);
        }

        XidLsn xid{end_xid, constant::INDEX_COMMIT_LSN};

        proto::IndexInfo info = server->get_index_info(db_id, index_id, xid);
        if (info.id() == 0) {
            //TODO: it seems like PG generates DROP INDEX with table ids, need
            //to investigate it more.
            LOG_INFO("The index is not valid: {}", index_id);
            return;
        }

        // Index should be at BEING_DELETED to be dropped
        DCHECK(static_cast<sys_tbl::IndexNames::State>(info.state()) == sys_tbl::IndexNames::State::BEING_DELETED);

        auto exists = sys_tbl_mgr::Server::get_instance()->exists(db_id, info.table_id(), xid);
        if (!exists) {
            // when dropping a table, PG generates DROP TABLE first
            // following by DROP INDEX, We will mark the index as DELETED in this case.
            LOG_INFO("Table doesn't exists: {}, {}", info.table_id(), index_id);
            server->set_index_state(db_id, xid, info.table_id(), index_id, sys_tbl::IndexNames::State::DELETED);
            return;
        }

        auto meta = server->get_roots(db_id, info.table_id(), end_xid);
        DCHECK(meta != nullptr);
        auto it = std::ranges::find_if(meta->roots,
                [&](auto const& v) { return index_id == v.index_id; });
        // Erase roots if present, roots wont be there if index drop came in before processing build,
        // and in that case, proceed for making index DELETED
        if (it != meta->roots.end()) {
            // XXX: Optimize roundtrips:
            // https://linear.app/springtail/issue/SPR-679/optimize-indexer-to-reduce-roundtrips-to-systblmgr

            auto idx_cols = _get_index_cols(info);
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
        }
        server->set_index_state(db_id, xid, info.table_id(), index_id, sys_tbl::IndexNames::State::DELETED);

        // Cleanup table-index map
        _remove_index_key(db_id, info.table_id(), key);

        LOG_INFO("Index dropped: {}:{} @ {}", db_id, index_id, end_xid);
    }

    Indexer::IndexState
    Indexer::_build(std::stop_token st, const Key& key, const IndexParams& idx)
    {
        constexpr int DROP_CHECK_PERIOD = 1000;

        LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "Build index: {}:{} - {}", key.first, key.second, idx._index_request.index().id());

        auto [db_id, index_id] = key;
        auto tid = static_cast<uint64_t>(idx._index_request.index().table_id());

        CHECK_EQ(idx._index_request.action(), "create_index");

        auto server = sys_tbl_mgr::Server::get_instance();
        server->invalidate_table(db_id, tid, XidLsn{idx._xid});

        // index column positions
        auto idx_cols = _get_index_cols(idx._index_request.index());

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

        LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "Indexing build in progress: {}:{}", db_id, index_id);
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
            auto &&row = *row_i;
            auto &&svalue = std::make_shared<KeyValueTuple>(key_fields, value_fields, &row);
            root->insert(svalue);

            ++current_row_id;
            ++row_cnt;
        }
        LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "Index build finished: {}:{}, rows={}", db_id, index_id, row_cnt);
        return {root, key, idx, tid};
    }

    std::vector<uint32_t>
    Indexer::_get_index_cols(proto::IndexInfo index_info)
    {
        std::vector<std::pair<uint32_t, uint32_t>> temp;

        // Step 1: Collect (idx_position, position) pairs
        for (const auto& column : index_info.columns()) {
            temp.emplace_back(column.idx_position(), column.position());
        }

        // Step 2: Sort by idx_position
        std::sort(temp.begin(), temp.end());

        // Step 3: Extract position values into idx_cols
        std::vector<uint32_t> idx_cols;
        idx_cols.reserve(temp.size());
        for (const auto& [_, pos] : temp) {
            idx_cols.push_back(pos);
        }
        return idx_cols;
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
        auto tid = idx._index_request.index().table_id();
        XidLsn xid{end_xid, constant::INDEX_COMMIT_LSN};

        IndexParams work_item;

        std::unique_lock g(_m);

        // fetch the latest state of the work item before we erase it
        work_item = _work_set.at(key);

        _work_set.erase(key);
        auto server = sys_tbl_mgr::Server::get_instance();
        proto::IndexInfo index_info = server->get_index_info(db_id, index_id, xid);

        // Table resync will cause index to be marked as deleted
        if (static_cast<sys_tbl::IndexNames::State>(index_info.state()) == sys_tbl::IndexNames::State::DELETED) {
            // Index building was attempted, truncate and finalize
            if (root) {
                root->truncate();
                root->finalize();
            }
        } else {
            // Index should be at NOT_READY / BEING_DELETED to be processed
            DCHECK(static_cast<sys_tbl::IndexNames::State>(index_info.state()) == sys_tbl::IndexNames::State::BEING_DELETED ||
                    static_cast<sys_tbl::IndexNames::State>(index_info.state()) == sys_tbl::IndexNames::State::NOT_READY);

            if (!root) {
                // if IndexStatus is BUILDING - stop could have got requested, so the index
                //                              will be rebuilt during restart
                // If IndexStatus is ABORTING - Drop came before build was even picked,
                //                              mark the state as DELETED

                if (work_item.is_status(IndexStatus::ABORTING)) {
                    server->set_index_state(db_id, xid, tid, index_id, sys_tbl::IndexNames::State::DELETED);
                }
            } else {
                // Index building was attempted, finalize and process build/abort
                auto extent_id = root->finalize();
                if (work_item.is_status(IndexStatus::BUILDING)) {
                    auto meta = server->get_roots(db_id, tid, end_xid);
                    meta->roots.clear();
                    meta->roots.emplace_back(key.second, extent_id);
                    server->update_roots(db_id, tid, end_xid, *meta);
                    server->set_index_state(db_id, xid, tid, index_id, sys_tbl::IndexNames::State::READY);
                } else if (work_item.is_status(IndexStatus::ABORTING)) {
                    // the index was deleted while we were building it
                    // lets also finalize here as part of the tree
                    // may have got finalized while we were building.
                    root->truncate();
                    root->finalize();
                    server->set_index_state(db_id, xid, tid, index_id, sys_tbl::IndexNames::State::DELETED);
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
            auto push_success = _index_reconciliation_queue_mgr->push(db_id,
                    std::make_shared<IndexReconcileRequest>(db_id, idx_state._idx._xid));
            if (!push_success) {
                // Queue not available as respective pg_log_mgr is shutdown
                // Abort reconciliation as the same will be redone as part of recovery
                _pending_idx_reconciliation_map[db_id].erase(idx_state._idx._xid);
            }
        }
    }

    void
    Indexer::process_index_reconciliation(uint64_t db_id, uint64_t reconcile_xid, uint64_t end_xid) {
        std::scoped_lock lock(_pending_reconciliation_map_mtx);

        auto db_it = _pending_idx_reconciliation_map.find(db_id);
        if (db_it == _pending_idx_reconciliation_map.end()) {
            return; // we assume the work was completed earlier
        }
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
            LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "Index reconciliation in progress: {}:{}", db_id, index_id);

            // index column positions
            auto idx_cols = _get_index_cols(idx_state._idx._index_request.index());

            // Get the next_extent from disk using the stats last offset
            auto table = TableMgr::get_instance()->get_table(db_id, idx_state._tid, idx_state._idx._xid);
            auto next_eid = table->get_stats().end_offset;
            auto next_extent_result = table->read_extent_from_disk(next_eid);
            auto next_extent = next_extent_result.first;

            // To keep track of invalidated extent IDs, as multiple extents can have same previous extent
            std::unordered_set<uint64_t> invalidated_eids;

            // If next_extent is available, invalidate previous extent first and then populate using next_extent
            while (next_extent) {
                // Get the table at the next XID
                // and fetch the page for the extent
                auto next_xid = next_extent->header().xid;
                auto next_schema = TableMgr::get_instance()->get_extent_schema(db_id, idx_state._tid, XidLsn(next_xid));

                // If previous offset exists and not processed before, lets invalidate that first
                if (auto prev_eid = next_extent->header().prev_offset; prev_eid != constant::UNKNOWN_EXTENT
                        && invalidated_eids.find(prev_eid) == invalidated_eids.end()) {

                    // Get the previous extent and its schema
                    auto [prev_extent, tmp_next_eid] = table->read_extent_from_disk(prev_eid);
                    auto prev_xid = prev_extent->header().xid;
                    auto prev_schema = TableMgr::get_instance()->get_extent_schema(db_id, idx_state._tid, XidLsn(prev_xid));

                    // and invalidate index for the rows in the prev page
                    indexer_helpers::invalidate_index_for_extent(prev_eid, prev_extent, idx_state._root, idx_cols, prev_schema);

                    // Insert into a set to skip for other extents pointing
                    // to the same previous extent
                    invalidated_eids.insert(prev_eid);
                }

                // Populate index for the rows in the next page
                indexer_helpers::populate_index_for_extent(next_eid, next_extent, idx_state._root, idx_cols, next_schema);

                // Get the next extent if next_offset is present, else exit the reconciliation
                next_eid = next_extent_result.second;
                if (next_eid > 0) {
                    next_extent_result = table->read_extent_from_disk(next_eid);
                    next_extent = next_extent_result.first;
                } else {
                    break;
                }
            }

            // Clear invalidated extents list
            invalidated_eids.clear();

            LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "Initiating Index commit: {}:{}", db_id, index_id);
            _commit_build(idx_state._root, idx_state._key, idx_state._idx, end_xid);
        }

        // Remove XID from the tracker
        _redis_ddl.remove_index_xid(db_id, idx_state._idx._xid);
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
