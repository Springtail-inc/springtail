#include <mutex>
#include <stop_token>
#include <assert.h>
#include <algorithm>
#include <pg_log_mgr/indexer.hh>
#include <common/logging.hh>
#include <sys_tbl_mgr/table_mgr.hh>
#include <sys_tbl_mgr/client.hh>

namespace springtail::committer {

    Indexer::Indexer(uint32_t worker_count) 
    {
        assert(worker_count);
        for (auto i = 0; i != worker_count; ++i) {
            _workers.emplace_back([this](std::stop_token st) { task(st); });
        }
        SPDLOG_INFO("Indexer created: {}", worker_count);
    }

    void Indexer::process_ddls(uint64_t db_id, uint64_t xid, nlohmann::json const& ddls)
    {
        for (auto const& ddl: ddls) {
            auto action = ddl["action"];
            if (action == "create_index") {
                build({db_id, xid, ddl});
            } else if (action == "drop_index") {
                drop(db_id, ddl["id"], xid);
            } else {
                assert(false);
            }
        }
    }

    void Indexer::build(IndexParams idx)
    {
        {
            std::scoped_lock g(_m);
            Key key(idx._db_id, idx._ddl["id"]);
            // I don't think PG will issue two creates with the same index ID.
            assert(_work_set.find(key) == _work_set.end());
            _work_set[key] = std::move(idx);
            _queue.push(key);
        }
        // notify workers about new items
        _cv.notify_one();
    }

    void Indexer::drop(uint64_t db_id, uint64_t index_id, uint64_t xid)
    {
        std::scoped_lock g(_m);
        Key key(db_id, index_id);
        auto it = _work_set.find(key);
        if (it == _work_set.end()) {
            // note: the work item has _ddl.empty() == true
            // it means to drop the index right away
            _work_set[key] = {db_id, xid, {}, IndexStatus::Deleting};
            _queue.push(key);
            _cv.notify_one();
        } else {
            // mark the status as Aborting, it will tell the worker to
            // cancel the index build / catchup
            // and proceed for dropping the index
            it->second._xid = xid;
            it->second._status = IndexStatus::Aborting;
        }
    }

    void Indexer::wait_for_completion(uint64_t db_id)
    {
        auto find_work = [&]() {
            auto it = std::ranges::find_if(_work_set,
                    [&](auto const& v) { 
                        return v.first.first == db_id;
                    });
            return it != _work_set.end();
        };

        // wait for the key {db, tid} to be removed from the working set
        std::unique_lock g(_m);
        _cv_done.wait(g, [&find_work]{return !find_work();});
    }

    void Indexer::wait_for_completion(uint64_t db_id, uint64_t tid)
    {
        auto find_work = [&]() {
            auto it = std::ranges::find_if(_work_set,
                    [&](auto const& v) { 
                        if (v.first.first == db_id && !v.second._ddl.is_null()) {
                            return v.second._ddl["table_id"] == tid;
                        }
                        return false;
                    });
            return it != _work_set.end();
        };

        // wait for the key {db, tid} to be removed from the working set
        std::unique_lock g(_m);
        while( find_work()  ) {
            _cv_done.wait(g, [&find_work]{return !find_work();});
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
                params = _work_set[key];
            }
            if (params.is_status(IndexStatus::Building)) {
                _add_to_pending_reconciliation(_build(st, key, params));
            } else {
                _add_to_pending_reconciliation(IndexState(nullptr, key, params, std::numeric_limits<uint64_t>::max()));
            }
        }
        SPDLOG_INFO("Indexer thread joined");
    }

    void Indexer::_drop(const Key& key, const IndexParams& idx)
    {
        assert(idx._ddl.is_null());

        auto [db_id, index_id] = key;
        SPDLOG_INFO("Drop index {}, {}, {}", db_id, index_id, idx._xid);

        auto client = sys_tbl_mgr::Client::get_instance();

        IndexParams work_item;
        {
            std::unique_lock g(_m);

            // fetch the latest state of the work item before we erase it
            work_item = _work_set[key];
            assert(work_item._ddl.is_null());

            _work_set.erase(key);
            _cv_done.notify_one();
        }

        XidLsn xid{idx._xid};

        proto::IndexInfo info = client->get_index_info(db_id, index_id, xid);
        if (info.id() == 0) {
            //TODO: it seems like PG generates DROP INDEX with table ids, need
            //to investigate it more.
            SPDLOG_INFO("The index is not valid: {}", index_id);
            return;
        }

        auto exists = TableMgr::get_instance()->exists(db_id, info.table_id(), xid.xid, xid.lsn);
        if (!exists) {
            // when dropping a table, PG generates DROP TABLE first
            // following by DROP INDEX. We ignore DROP INDEX after DROP TABLE.
            SPDLOG_INFO("Table doesn't exists: {}, {}", info.table_id(), index_id);
            return;
        }

        // index column positions
        std::vector<uint32_t> idx_cols;
        for (auto const& col : info.columns()) {
            idx_cols.push_back(col.position());
        }

        auto meta = client->get_roots(db_id, info.table_id(), idx._xid);
        auto it = std::ranges::find_if(meta->roots,
                                       [&](auto const& v) { return index_id == v.index_id; });
        CHECK(it != meta->roots.end());

        auto table =
            TableMgr::get_instance()->get_mutable_table(db_id, info.table_id(), idx._xid, idx._xid);
        auto root = table->create_index_root(index_id, idx_cols);
        if (it->extent_id != constant::UNKNOWN_EXTENT) {
            root->init(it->extent_id);
        } else {
            root->init_empty();
        }
        root->truncate();
        root->finalize();

        meta->roots.erase(it);
        client->update_roots(db_id, info.table_id(), idx._xid, *meta);

        SPDLOG_INFO("Index dropped: {}:{}", db_id, index_id);
    }

    Indexer::IndexState
    Indexer::_build(std::stop_token st, const Key& key, const IndexParams& idx)
    {
        constexpr int DROP_CHECK_PERIOD = 1000;

        SPDLOG_DEBUG_MODULE(LOG_COMMITTER, "Build index: {}:{} - {}", key.first, key.second, idx._ddl.dump());

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

        SPDLOG_DEBUG_MODULE(LOG_COMMITTER, "Indexing build in progress: {}:{}", db_id, index_id);
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
        SPDLOG_DEBUG_MODULE(LOG_COMMITTER, "Index build finished: {}:{}, rows={}", db_id, index_id, row_cnt);
        return {root, key, idx, tid};
    }

    bool
    Indexer::_was_dropped(const Key& key)
    {
        std::unique_lock g(_m);
        auto const& params = _work_set[key];
        // index drop requested while we've been building it
        return params.is_status(IndexStatus::Aborting);
    }

    void
    Indexer::_commit_build(MutableBTreePtr root, const Key& key, const IndexParams& idx, uint64_t end_xid)
    {
        if (!root) {
            // The build was cancelled as due to a shutdown.
            // When the system restarts it will check the redis precommit hash
            // and restart the build process.
            return;
        }

        auto [db_id, index_id] = key;
        auto tid = idx._ddl["table_id"];
        XidLsn xid{end_xid};

        IndexParams work_item;

        std::unique_lock g(_m);

        // fetch the latest state of the work item before we erase it
        work_item = _work_set[key];

        _work_set.erase(key);
        _cv_done.notify_one();

        auto client = sys_tbl_mgr::Client::get_instance();
        auto extent_id = root->finalize();
        if (work_item.is_status(IndexStatus::Building)) {
            auto meta = client->get_roots(db_id, tid, end_xid);
            meta->roots.emplace_back(key.second, extent_id);
            client->update_roots(db_id, tid, end_xid, *meta);
            client->set_index_state(db_id, xid, tid, index_id, sys_tbl::IndexNames::State::READY);
        } else {
            // the index was deleted while we were building it
            root->truncate();
            root->finalize();
            // XXX: since the root was not added in the first place,
            // should we record the latest state of the root even if its dropped while building?
        }

        // TODO: revisit it when we support asynchronous index builds
        // so that the lock and _cv_done are not required while
        // root->finalize() and sys table updates.
    }

    // Index reconciliation flows
    void
    Indexer::_add_to_pending_reconciliation(IndexState&& idxState)
    {
        std::scoped_lock lock(_pending_recon_map_mtx);
        _pending_idx_reconciliation_map
            .try_emplace(idxState._idx._db_id)   // Ensure db_id entry exists
            .first->second
            .try_emplace(idxState._idx._xid)     // Ensure xid entry exists
            .first->second.push_back(std::move(idxState)); // Add IndexState to the list
    }

    std::optional<uint64_t>
    Indexer::process_first_pending_reconciliation(uint64_t db_id) {
        std::scoped_lock lock(_pending_recon_map_mtx);
        auto db_it = _pending_idx_reconciliation_map.find(db_id);
        if (db_it == _pending_idx_reconciliation_map.end()) {
            return std::nullopt; // No pending entries for this db_id
        }
        return _process_first_pending_reconciliation(db_it);
    }

    std::optional<std::pair<uint64_t, uint64_t>>
    Indexer::process_first_pending_reconciliation() {
        std::scoped_lock lock(_pending_recon_map_mtx);
        if (_pending_idx_reconciliation_map.empty()) {
            return std::nullopt;
        }
        auto db_it = _pending_idx_reconciliation_map.begin();
        auto xid_opt = _process_first_pending_reconciliation(db_it);
        if (xid_opt) {
            return std::pair(db_it->first, *xid_opt);
        } else {
            return std::nullopt;
        }
    }

    std::optional<uint64_t>
    Indexer::_process_first_pending_reconciliation(PendingReconMap::iterator db_it) {
        auto& xid_map = db_it->second;
        if (xid_map.empty()) {
            _pending_idx_reconciliation_map.erase(db_it); // Clean up empty db_id entry
            return std::nullopt;
        }

        // Get iterator to the first xid entry
        auto xid_it = xid_map.begin();
        auto& idx_list = xid_it->second;

        // Process each entry in the list
        for (auto& idxState : idx_list) {
            _reconcile_index(idxState);
        }

        // Clean up if entries are empty
        xid_map.erase(xid_it); // Remove processed xid entry
        if (xid_map.empty()) {
            _pending_idx_reconciliation_map.erase(db_it); // Remove empty db_id entry
        }
        return xid_it->first;
    }

    void
    Indexer::_reconcile_index(IndexState& idxState)
    {
        auto [db_id, index_id] = idxState._key;
        auto end_xid = idxState._idx._xid;
        auto is_fresh_drop = false;
        auto is_drop_while_processing = false;
        {
            std::unique_lock g(_m);

            // fetch the latest state of the work item before we proceed for catchup
            auto&& work_item = _work_set[idxState._key];
            end_xid = work_item._xid;
            // When a fresh work item comes in for drop index
            // there wont be any DDL
            is_fresh_drop = work_item.is_status(IndexStatus::Deleting);

            // When drop index request comes in when
            // we are in the process of building/catching-up
            // status will denote to proceed for drop
            is_drop_while_processing = work_item.is_status(IndexStatus::Aborting);
        }

        if (is_fresh_drop) {
            // Do clear drop index
            _drop(idxState._key, idxState._idx);
        } else if (is_drop_while_processing) {
            // since btree inserts have a possibility of partial flush,
            // we will do full flush of root once at whichever stage it is in,
            // truncate and flush again
            _commit_build(idxState._root, idxState._key, idxState._idx, end_xid);
        } else {
            SPDLOG_DEBUG_MODULE(LOG_COMMITTER, "Index reconciliation in progress: {}:{}", db_id, index_id);

            // index column positions
            std::vector<uint32_t> idx_cols;
            for (auto const& col : idxState._idx._ddl["columns"]) {
                idx_cols.push_back(col["position"]);
            }

            // Get the next_extent from disk using the stats last offset
            auto table = TableMgr::get_instance()->get_table(db_id, idxState._tid, idxState._idx._xid);
            auto next_eid = table->get_stats().end_offset;
            auto next_page = table->read_page_from_disk(next_eid);

            while (!next_page->empty()) {
                end_xid = next_page->header().xid;
                // Get the previous_extent_id from next_extent header
                // and fetch the extent from disk using the extent_id
                if (auto prev_eid = next_page->header().prev_offset; prev_eid != constant::UNKNOWN_EXTENT) {
                    // Retrieve the page for previous_extent_id
                    auto prev_page = table->read_page_from_disk(prev_eid);

                    // and invalidate index for the rows in the page
                    indexer_helpers::invalidate_index_for_page(db_id, index_id, idxState._tid, prev_eid,
                            prev_page, idxState._root, XidLsn(prev_page->header().xid), idx_cols);
                }

                // Populate index for the rows in the next page
                indexer_helpers::populate_index_for_page(db_id, index_id, idxState._tid, next_eid,
                        next_page, idxState._root, XidLsn(next_page->header().xid), idx_cols);

                // Get the next page using end offset of that XID
                table = TableMgr::get_instance()->get_table(db_id, idxState._tid, next_page->header().xid);
                next_eid = table->get_stats().end_offset;
                next_page = table->read_page_from_disk(next_eid);
            }
            // Commit the index
            SPDLOG_DEBUG_MODULE(LOG_COMMITTER, "Initiating Index commit: {}:{}", db_id, index_id);
            _commit_build(idxState._root, idxState._key, idxState._idx, end_xid);
        }
    }
}  // namespace springtail::gc
