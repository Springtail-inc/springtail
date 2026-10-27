#include <pg_log_mgr/indexer.hh>
#include <sys_tbl_mgr/server.hh>
#include <sys_tbl_mgr/table_mgr.hh>

#include <pg_ext/extn_registry.hh>

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
            RedisDDL::get_instance()->insert_index_xid(idx._db_id, idx._xid);
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
            RedisDDL::get_instance()->insert_index_xid(db_id, xid);
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
                _add_to_pending_reconciliation(IndexState{AnyIndexRoot{}, key, params, std::numeric_limits<uint64_t>::max(), nullptr});
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

        auto la_it = meta->roots.end();
        bool is_last_index = true;

        for (auto it = meta->roots.begin(); it != meta->roots.end(); ++it) {
            if (it->index_id == constant::INDEX_LOOK_ASIDE) {
                la_it = it;
            }

            if (!(it->index_id == constant::INDEX_PRIMARY ||
                        it->index_id == constant::INDEX_LOOK_ASIDE ||
                        it->index_id == index_id)) {
                is_last_index = false;
                break;
            }
        }

        // Erase roots if present, roots wont be there if index drop came in before processing build,
        // and in that case, proceed for making index DELETED
        if (it != meta->roots.end()) {
            // XXX: Optimize roundtrips:
            // https://linear.app/springtail/issue/SPR-679/optimize-indexer-to-reduce-roundtrips-to-systblmgr

            auto idx_cols = _get_index_cols(info);
            auto table =
                TableMgr::get_instance()->get_mutable_table(db_id, info.table_id(), end_xid, end_xid, {PgExtnRegistry::get_instance()->comparator_func});

            // Dispatch on access_method (btree redesign phase 10, the third call site
            // phase-9.md's handoff named explicitly): dropping a GIST index must construct its
            // root via create_gist_index_root(), never create_index_root(), or it would
            // misinterpret the GiST tree's content as a btree while truncating it.
            //
            // index_access_method_from_string() throws on any unrecognized index_type string by
            // design (phase 9, architecture.md Decisions log entry 10) -- "a throwing validator
            // is only as safe as its least-protected caller", so this call site needs its own
            // local catch, matching _build()'s dispatch four lines below in this same file (round-
            // 1 review of this phase, finding 1: this call site was new in this commit and was
            // the one sibling of that pattern left unprotected -- info.index_type() is read fresh
            // from the catalog here, and nothing upstream of _drop() re-validates it, so this
            // can't assume _upsert_index_name's write-side guard is the only thing ever able to
            // reach this line). Unlike _build(), there's no valid root to truncate if the access
            // method can't even be classified, so log and leave this index's `IndexNames` row as
            // whatever the caller already set it (BEING_DELETED) rather than inventing a partial
            // drop -- non-destructive, and this call site is unreachable today under ordinary DDL
            // for the same reason _build()'s dispatch is: the write-side guard already stops
            // this exact string from reaching the catalog.
            try {
                auto access_method = index_access_method_from_string(info.index_type());
                if (access_method == IndexAccessMethod::GIST) {
                    auto opclass_names = _get_index_opclasses(info);
                    auto gist_root = table->create_gist_index_root(index_id, idx_cols, opclass_names,
                            {PgExtnRegistry::get_instance()->comparator_func});
                    if (it->extent_id != constant::UNKNOWN_EXTENT) {
                        gist_root->init(it->extent_id);
                        gist_root->truncate();
                        gist_root->finalize();
                    }
                } else {
                    auto root = table->create_index_root(index_id, idx_cols,
                            {PgExtnRegistry::get_instance()->comparator_func}, access_method);
                    if (it->extent_id != constant::UNKNOWN_EXTENT) {
                        root->init(it->extent_id);
                        root->truncate();
                        root->finalize();
                    }
                }
            } catch (const Error& e) {
                LOG_ERROR("Index drop failed to resolve access method, index left at its current "
                        "state: {}:{} - {}", db_id, index_id, e.what());
                return;
            }

            // Truncate look-aside index if this is the last index in the table getting dropped
            if (is_last_index) {
                LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "Dropping look aside index as we are dropping final secondary index: {}", index_id);

                auto look_aside_root = table->create_look_aside_root({PgExtnRegistry::get_instance()->comparator_func});
                if (la_it->extent_id != constant::UNKNOWN_EXTENT) {
                    look_aside_root->init(la_it->extent_id);
                    look_aside_root->truncate();
                    look_aside_root->finalize();
                }
            }
        }

        server->set_index_state(db_id, xid, info.table_id(), index_id, sys_tbl::IndexNames::State::DELETED);
        if (is_last_index) {
            meta->roots.emplace_back(constant::INDEX_LOOK_ASIDE, constant::UNKNOWN_EXTENT);
            server->update_roots(db_id, info.table_id(), end_xid, *meta);
        }

        // Cleanup table-index map. _drop() operates on an index that was already READY (it must
        // have gone through a prior _commit_build() to get BEING_DELETED in the first place), so
        // it never owns the look-aside bootstrap tracker entry for this table -- never clear it
        // here.
        _remove_index_key(db_id, info.table_id(), key, false);

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

        auto mutable_table = TableMgr::get_instance()->get_mutable_table(db_id, tid, idx._xid, idx._xid, {PgExtnRegistry::get_instance()->comparator_func});

        // Dispatch point (btree redesign phase 9, architecture.md Decisions log entry 8):
        // resolve the index's access method from the catalog-carried index_type string. GIST
        // (phase 10) builds a GistWriter via create_gist_index_root(), wrapped in AnyIndexRoot so
        // the rest of this function (and _commit_build()/_reconcile_index()'s catch-up) can stay
        // kind-agnostic; GIN (phase 11) and any unrecognized/corrupt index_type string still fail
        // exactly like the original stub (never silently defaults to BTREE) -- see
        // index_access_method_from_string()'s own doc comment.
        AnyIndexRoot root;
        try {
            auto access_method =
                index_access_method_from_string(idx._index_request.index().index_type());
            if (access_method == IndexAccessMethod::GIST) {
                auto opclass_names = _get_index_opclasses(idx._index_request.index());
                root = mutable_table->create_gist_index_root(index_id, idx_cols, opclass_names,
                        {PgExtnRegistry::get_instance()->comparator_func});
            } else {
                root = mutable_table->create_index_root(index_id, idx_cols,
                        {PgExtnRegistry::get_instance()->comparator_func}, access_method);
            }
        } catch (const Error& e) {
            // Leave the index at NOT_READY -- the same outcome _commit_build() already gives a
            // build interrupted by st.stop_requested() below -- rather than inventing a new
            // "permanently failed" state: IndexNames::State has no such state, and once phase
            // 11 lands, the next recover_indexes() pass (daemon restart) retries this exact
            // still-NOT_READY row and builds it for real instead of reusing a stale failure.
            LOG_ERROR("Index build failed, index will remain not-ready: {}:{} - {}", db_id,
                    index_id, e.what());
            return {AnyIndexRoot{}, key, idx, tid, nullptr};
        }
        root.init_empty();

        auto look_aside_index = mutable_table->look_aside_index();
        bool build_look_aside = false;

        if (!look_aside_index) {
            std::unique_lock g(_look_aside_mutex);

            auto la_tracker_it = _look_aside_build_tracker.find(tid);
            if (la_tracker_it == _look_aside_build_tracker.end()) {
                // We are the first concurrent "first index on this table" build to arrive --
                // create and own the look-aside index's bootstrap, catch-up, and finalize for
                // the whole duration of this table's "no look-aside index yet" window
                // (architecture.md Sec 5.4 case 2).
                look_aside_index = mutable_table->create_look_aside_root({PgExtnRegistry::get_instance()->comparator_func});
                look_aside_index->init_empty();
                _look_aside_build_tracker[tid] = look_aside_index;
                build_look_aside = true;
            } else {
                // Another builder already owns this table's look-aside bootstrap this
                // generation. Discover its instance (architecture.md Sec 5.4 case 2: "others...
                // discover it already exists") rather than silently staying null regardless of
                // which MutableTable instance we got (TableMgr::get_mutable_table() constructs a
                // fresh MutableTable per call, so this builder's own mutable_table->
                // look_aside_index() can never reflect a sibling builder's create_look_aside_root()
                // call -- confirmed by reading table_mgr.cc). We are NOT the owner, though, so we
                // must never call a mutating method (insert/remove/truncate/finalize) on it:
                // IndexWriter has no internal locking (Sec 5.4), and the owner is already relying
                // on exclusive access for the rest of its build -- sharing a second live mutator
                // would violate that contract and risk a double finalize(). The owner's own
                // bootstrap scan (a complete snapshot as of its own start xid) plus its own
                // reconciliation catch-up (through its own commit) plus steady-state ordinary
                // ingest after that commit together cover the table's whole timeline with no gap
                // that this builder needs to help fill -- this builder's own secondary index is
                // built directly off the primary table, never off look-aside. See phases/
                // phase-3.md for the full audit, including a known, separate, pre-existing gap
                // this fix does not close: if the *owner's* build is itself aborted/dropped
                // mid-flight while this builder is still in flight, responsibility is not handed
                // off and the look-aside index is left permanently incomplete for historical
                // rows -- flagged there as a deferred risk, not introduced by this phase.
                look_aside_index = la_tracker_it->second;
            }
        }

        key_fields = mutable_table->schema()->get_fields(mutable_table->schema()->get_column_names(idx_cols));

        auto internal_row_id_f = mutable_table->schema()->get_field(constant::INTERNAL_ROW_ID);

        // additional fields in the root schema to keep extent and row ids
        auto value_fields = std::make_shared<FieldArray>(1);
        uint64_t row_cnt = 0;

        LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "Indexing build in progress: {}:{}", db_id, index_id);
        auto table = TableMgr::get_instance()->get_table(db_id, tid, idx._xid, {PgExtnRegistry::get_instance()->comparator_func});

        // Build look-aside index if this is the first index getting built for the table
        // This flag is thread-safe, enabled, only for the first index for the table
        if (build_look_aside) {
            std::vector<std::string> look_aside_keys;
            look_aside_keys.push_back(constant::INTERNAL_ROW_ID);
            auto la_key_fields = mutable_table->schema()->get_fields(look_aside_keys);
            auto la_value_fields = std::make_shared<FieldArray>(2);
            uint64_t current_extent_id = 0;
            uint32_t current_row_id = 0;
            for (auto row_i = table->begin(); row_i != table->end(); ++row_i) {
                if (st.stop_requested()) {
                    look_aside_index->truncate();
                    return {AnyIndexRoot{}, key, idx, tid, look_aside_index};
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
                (*la_value_fields)[0] = std::make_shared<ConstTypeField<uint64_t>>(extent_id);
                (*la_value_fields)[1] = std::make_shared<ConstTypeField<uint32_t>>(current_row_id);

                // insert key
                auto &&row = *row_i;
                auto &&svalue = std::make_shared<KeyValueTuple>(la_key_fields, la_value_fields, &row);
                look_aside_index->insert(svalue);

                ++current_row_id;
                ++row_cnt;
            }
        }

        row_cnt = 0;
        for (auto row_i = table->begin(); row_i != table->end(); ++row_i) {
            if (st.stop_requested()) {
                root.truncate();
                if (build_look_aside) {
                    look_aside_index->truncate();
                }
                // Only the owning (build_look_aside) builder may ever hand back a non-null
                // look-aside root -- see the discovery comment above.
                return {AnyIndexRoot{}, key, idx, tid, build_look_aside ? look_aside_index : nullptr};
            }
            // check if the index was dropped
            if (row_cnt % DROP_CHECK_PERIOD == 0 && _was_dropped(key)) {
                return {root, key, idx, tid, build_look_aside ? look_aside_index : nullptr};
            }

            // insert key
            auto &&row = *row_i;
            (*value_fields)[0] = std::make_shared<ConstTypeField<uint64_t>>(internal_row_id_f->get_uint64(&row));
            auto &&svalue = std::make_shared<KeyValueTuple>(key_fields, value_fields, &row);

            // A throwing validator is only as safe as its least-protected caller (phase 9's
            // round-1/round-2 lesson): GistWriter::insert() -> gist_helpers::compress_leaf_entry()
            // throws Error for any GiST column type/opclass combination this phase doesn't support
            // (e.g. a non-TEXT column -- see gist_helpers.hh's documented scope limit), and this
            // loop runs on Indexer's own worker thread, which (like PgLogReader's DDL thread) has
            // no outer try/catch of its own. Caught here, not just at construction time (the
            // access_method dispatch above), because the throw can only actually happen once real
            // row data flows through -- confirmed directly: reusing this fixture's existing
            // INT32-typed columns for a GIST build throws exactly this, uncaught, before this fix.
            try {
                root.insert(svalue);
            } catch (const Error& e) {
                LOG_ERROR("Index build failed on row data, index will remain not-ready: {}:{} - {}",
                        db_id, index_id, e.what());
                root.truncate();
                if (build_look_aside) {
                    look_aside_index->truncate();
                }
                return {AnyIndexRoot{}, key, idx, tid, build_look_aside ? look_aside_index : nullptr};
            }

            ++row_cnt;
        }
        LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "Index build finished: {}:{}, rows={}", db_id, index_id, row_cnt);
        return {root, key, idx, tid, build_look_aside ? look_aside_index : nullptr};
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

    std::vector<std::string>
    Indexer::_get_index_opclasses(proto::IndexInfo index_info)
    {
        std::vector<std::pair<uint32_t, std::string>> temp;
        for (const auto& column : index_info.columns()) {
            temp.emplace_back(column.idx_position(), column.opclass());
        }
        std::sort(temp.begin(), temp.end(),
                 [](auto const &a, auto const &b) { return a.first < b.first; });

        std::vector<std::string> opclasses;
        opclasses.reserve(temp.size());
        for (const auto& [_, opclass] : temp) {
            opclasses.push_back(opclass);
        }
        return opclasses;
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
    Indexer::_commit_build(AnyIndexRoot root, const Key& key, const IndexParams& idx,
            uint64_t end_xid, IndexWriterPtr look_aside_root)
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
                root.truncate();
                root.finalize();
                if (look_aside_root) {
                    look_aside_root->truncate();
                    look_aside_root->finalize();
                }
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

                if (look_aside_root) {
                    look_aside_root->truncate();
                    look_aside_root->finalize();
                }
                if (work_item.is_status(IndexStatus::ABORTING)) {
                    server->set_index_state(db_id, xid, tid, index_id, sys_tbl::IndexNames::State::DELETED);
                }
            } else {
                // Index building was attempted, finalize and process build/abort
                auto extent_id = root.finalize();
                if (work_item.is_status(IndexStatus::BUILDING)) {
                    auto meta = server->get_roots(db_id, tid, end_xid);
                    meta->roots.clear();
                    if (look_aside_root) {
                        auto la_extent_id = look_aside_root->finalize();
                        meta->roots.emplace_back(constant::INDEX_LOOK_ASIDE, la_extent_id);
                    }
                    meta->roots.emplace_back(key.second, extent_id);
                    server->update_roots(db_id, tid, end_xid, *meta);
                    server->set_index_state(db_id, xid, tid, index_id, sys_tbl::IndexNames::State::READY);
                } else if (work_item.is_status(IndexStatus::ABORTING)) {
                    // the index was deleted while we were building it
                    // lets also finalize here as part of the tree
                    // may have got finalized while we were building.
                    root.truncate();
                    root.finalize();
                    if (look_aside_root) {
                        look_aside_root->truncate();
                        look_aside_root->finalize();
                    }
                    server->set_index_state(db_id, xid, tid, index_id, sys_tbl::IndexNames::State::DELETED);
                }
            }
        }

        // Cleanup table-index map. look_aside_root is non-null here iff *this* build is the one
        // that owns the table's look-aside bootstrap (indexer.cc's _build(): only the owner's
        // IndexState ever carries a non-null look_aside_root) -- only the owner may retire the
        // tracker entry, regardless of whether this call landed via success, abort, or deletion.
        _remove_index_key(db_id, tid, key, look_aside_root != nullptr);
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

        // Drop the min_index_xid pin only after every IndexState bucketed
        // under reconcile_xid is done. The Redis sorted set keys this entry
        // by xid, so a single drop_index/create_index sharing reconcile_xid
        // would otherwise yank the pin while the rest of the list is still
        // walking prev_offset chains the vacuumer could now punch.
        RedisDDL::get_instance()->remove_index_xid(db_id, reconcile_xid);

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
            _commit_build(idx_state._root, idx_state._key, idx_state._idx, end_xid, idx_state._look_aside_root);
        } else if (!idx_state._root) {
            // The build never produced a root -- for an ordinary (non-abort/non-drop) work item
            // this previously could only mean st.stop_requested() (process shutting down, so
            // this code never ran again on the stale IndexState) or _was_dropped() (which routes
            // through is_drop_while_processing above instead). Phase 9's GIST/GIN dispatch stub
            // (Indexer::_build's catch block) is a third, new way to reach a null root while the
            // work item stays BUILDING -- with the process very much still running and ordinary
            // DML on the table still triggering catch-up reconciliation for it. Skip straight to
            // _commit_build(), which is already null-root-safe (leaves the index NOT_READY)
            // rather than falling into the catch-up loop below, which unconditionally
            // dereferences idx_state._root and would crash.
            LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1,
                    "Index reconciliation skipped, build never produced a root: {}:{}", db_id, index_id);
            _commit_build(idx_state._root, idx_state._key, idx_state._idx, end_xid, idx_state._look_aside_root);
        } else {
            LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "Index reconciliation in progress: {}:{}", db_id, index_id);

            // index column positions
            auto idx_cols = _get_index_cols(idx_state._idx._index_request.index());

            // Get the next_extent from disk using the stats last offset
            auto table = TableMgr::get_instance()->get_table(db_id, idx_state._tid, idx_state._idx._xid, {PgExtnRegistry::get_instance()->comparator_func});
            auto next_eid = table->get_stats().end_offset;
            auto next_extent_result = table->read_extent_from_disk(next_eid);
            auto next_extent = next_extent_result.first;

            // To keep track of invalidated extent IDs, as multiple extents can have same previous extent
            std::unordered_set<uint64_t> invalidated_eids;
            auto value_fields = std::make_shared<FieldArray>(1);

            // look_aside_keys to be used to manage look_aside_index if exists
            std::vector<std::string> look_aside_keys;
            look_aside_keys.push_back(constant::INTERNAL_ROW_ID);

            // If next_extent is available, invalidate previous extent first and then populate using next_extent
            while (next_extent) {
                // Get the table at the next XID
                // and fetch the page for the extent
                auto next_xid = next_extent->header().xid;
                auto next_schema = TableMgr::get_instance()->get_extent_schema(db_id, idx_state._tid, XidLsn(next_xid), {PgExtnRegistry::get_instance()->comparator_func});

                // If previous offset exists and not processed before, lets invalidate that first
                if (auto prev_eid = next_extent->header().prev_offset; prev_eid != constant::UNKNOWN_EXTENT
                        && invalidated_eids.find(prev_eid) == invalidated_eids.end()) {

                    // Get the previous extent and its schema
                    auto [prev_extent, tmp_next_eid] = table->read_extent_from_disk(prev_eid);
                    // Invariant: while min_index_xid is registered for this
                    // index, the vacuum cutoff cannot advance past idx._xid,
                    // so any extent reachable via prev_offset from a
                    // post-snapshot extent must still be on disk. A null here
                    // means the registration window reopened (e.g. someone
                    // moved insert_index_xid back after commit_xid).
                    DCHECK(prev_extent != nullptr)
                        << "prev_extent unreadable at offset " << prev_eid
                        << " for db " << db_id << " table " << idx_state._tid
                        << " idx " << index_id << " idx_xid " << idx_state._idx._xid;
                    auto prev_xid = prev_extent->header().xid;
                    auto prev_schema = TableMgr::get_instance()->get_extent_schema(db_id, idx_state._tid, XidLsn(prev_xid), {PgExtnRegistry::get_instance()->comparator_func});
                    auto internal_row_id_f = prev_schema->get_field(constant::INTERNAL_ROW_ID);

                    // Invalidate look aside index if exists
                    if (idx_state._look_aside_root) {
                        indexer_helpers::invalidate_index_for_extent(prev_eid, prev_extent, idx_state._look_aside_root, look_aside_keys, prev_schema);
                    }

                    // and invalidate index for the rows in the prev page
                    auto &&idx_col_fields = prev_schema->get_fields(prev_schema->get_column_names(idx_cols));
                    for (auto &row: *prev_extent) {
                        (*value_fields)[0] = std::make_shared<ConstTypeField<uint64_t>>(internal_row_id_f->get_uint64(&row));
                        auto &&svalue = std::make_shared<KeyValueTuple>(idx_col_fields, value_fields, &row);
                        // Same "a throwing validator is only as safe as its least-protected
                        // caller" concern as _build()'s own insert loop (see that fix's comment)
                        // -- this reconciliation catch-up runs synchronously on whichever thread
                        // calls process_index_reconciliation(), which for a real daemon is a
                        // Committer worker with no outer try/catch of its own. Log and skip this
                        // one row's catch-up rather than letting the index build silently poison
                        // that caller.
                        try {
                            idx_state._root.remove(svalue);
                        } catch (const Error& e) {
                            LOG_ERROR("Index reconciliation catch-up (remove) failed on row data for "
                                    "{}:{}: {}", db_id, index_id, e.what());
                        }
                    }

                    // Insert into a set to skip for other extents pointing
                    // to the same previous extent
                    invalidated_eids.insert(prev_eid);
                }

                // Populate look aside index if exists
                if (idx_state._look_aside_root) {
                    indexer_helpers::populate_index_for_extent(next_eid, next_extent, idx_state._look_aside_root, look_aside_keys, next_schema);
                }

                // Populate index for the rows in the next page
                auto internal_row_id_f = next_schema->get_field(constant::INTERNAL_ROW_ID);
                auto &&idx_col_fields = next_schema->get_fields(next_schema->get_column_names(idx_cols));
                for (auto &row: *next_extent) {
                    (*value_fields)[0] = std::make_shared<ConstTypeField<uint64_t>>(internal_row_id_f->get_uint64(&row));
                    auto &&svalue = std::make_shared<KeyValueTuple>(idx_col_fields, value_fields, &row);
                    try {
                        idx_state._root.insert(svalue);
                    } catch (const Error& e) {
                        LOG_ERROR("Index reconciliation catch-up (insert) failed on row data for "
                                "{}:{}: {}", db_id, index_id, e.what());
                    }
                }

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
            _commit_build(idx_state._root, idx_state._key, idx_state._idx, end_xid, idx_state._look_aside_root);
        }
    }

    void Indexer::_remove_index_key(uint64_t db_id, uint64_t table_id, const Key& key, bool owns_look_aside)
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

        // Clear the look-aside build tracker *only* when this completing build is the one that
        // owns it (round-1 review finding 1). _look_aside_build_tracker[table_id]'s presence is
        // the external serialization point for architecture.md Sec 5.4 case 2 -- "all pending
        // index builds for this table_id finished" (the key_list.empty() signal above) is not an
        // equivalent condition: a non-owning sibling build can legitimately finish (and call this
        // function) before the owner's own look-aside bootstrap/catch-up/finalize does, and
        // erasing the entry then would let a third concurrent "first index" build on the same
        // table conclude it must start its own, independent look-aside bootstrap while the real
        // owner is still mid-build.
        if (owns_look_aside) {
            std::unique_lock g(_look_aside_mutex);
            _look_aside_build_tracker.erase(table_id);
        }
    }

}  // namespace springtail::gc
