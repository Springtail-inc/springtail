#include <common/constants.hh>
#include <garbage_collector/committer.hh>
#include <storage/table_mgr.hh>
#include <sys_tbl_mgr/client.hh>
#include <pg_log_mgr/pg_redis_xact.hh>

namespace springtail::gc {

    void
    Committer::run()
    {
        // initiate the worker threads
        for (int i = 0; i < _worker_count; i++) {
            _worker_threads.push_back(std::thread(&Committer::_run_worker, this));
        }

        // enter a loop polling for data from the write cache
        // XXX we are currently processing XIDs one at a time, but we should bundle together XID
        //     ranges whenever possible.
        while (!_shutdown) {
            // figure out if there's an XID to process
            // note: this is a blocking call that will timeout after 60s
            auto result = _redis.pop(_worker_id, 60);
            if (result == nullptr) {
                continue; // got a timeout, try again
            }
            uint64_t db_id = result->db_id();
            uint64_t xid = result->xid();

            // initialize the committed XID for this database if needed
            uint64_t committed_xid;
            auto itr = _committed_xids.find(db_id);
            if (itr == _committed_xids.end()) {
                committed_xid = _xid_mgr->get_committed_xid(db_id, 0);
            } else {
                committed_xid = itr->second;
            }

            SPDLOG_INFO("Commit XID: {}", xid);

            // find every table associated with this XID
            uint64_t table_cursor = 0;
            bool tid_done = false;
            while (!tid_done) {
                // query the write cache for the tables modified through this XID
                auto table_list = _write_cache->list_tables(db_id, committed_xid, xid, 100, table_cursor);

                SPDLOG_DEBUG_MODULE(LOG_GC, "Got {} tables from the write cache", table_list.size());

                // check if we are done processing this XID
                if (table_list.empty()) {
                    tid_done = true;
                    break;
                }

                uint64_t extent_cursor = 0;
                for (auto tid : table_list) {
                    uint64_t txid = 0, tlsn = 0;

                    // retrieve the set of table changes for this table
                    auto table_changes = _write_cache->fetch_table_changes(db_id, tid, committed_xid, xid);
                    for (auto &&change : table_changes) {
                        // if any of the changes are a truncate, then we will ignore all row
                        // mutations before the last truncate XID/LSN
                        // note: this assumes that the changes are coming back in XID/LSN order
                        if (change.op == WriteCacheClient::TableOp::TRUNCATE) {
                            txid = change.xid;
                            tlsn = change.xid_seq;
                        }
                    }

                    // construct the mutable table object
                    auto table = TableMgr::get_instance()->get_mutable_table(db_id, tid, committed_xid, xid, true);

                    // check if we need to perform a table truncate
                    if (txid > 0) {
                        table->truncate();
                    }

                    boost::unique_lock lock(_mutex);
                    _table_map[tid] = table;
                    lock.unlock();

                    bool eid_done = false;
                    while (!eid_done) {
                        // request the extents modified in each table
                        auto extent_list = _write_cache->list_extents(db_id, tid, committed_xid, xid, 100, extent_cursor);

                        SPDLOG_DEBUG_MODULE(LOG_GC, "Got {} extents for table {}", extent_list.size(), tid);

                        // check if we are done processing this table
                        if (extent_list.empty()) {
                            eid_done = true;
                            break;
                        }

                        // pass each extent to the worker queue
                        for (auto eid : extent_list) {
                            // increment the number of in-flight extents for this table
                            lock.lock();
                            ++_tid_count[tid];
                            lock.unlock();

                            auto entry = std::make_shared<WorkerEntry>(table, eid, xid, txid, tlsn);
                            _worker_queue.push(entry);
                        }
                    }
                }
            }

            // wait for tables to complete their processing
            SPDLOG_DEBUG_MODULE(LOG_GC, "Wait for {} tables to complete", _tid_count.size());

            // XXX ideally we could start working on the next XID while these finalize() operations
            //     are being completed.
            boost::unique_lock lock(_mutex);
            while (!_tid_count.empty()) {
                // check if any tables have completed processing
                std::vector<uint64_t> completed;
                for (auto &count : _tid_count) {
                    SPDLOG_DEBUG_MODULE(LOG_GC, "Table {} has count {}", count.first, count.second);
                    if (count.second == 0) {
                        // issue a finalize request to a worker
                        SPDLOG_DEBUG_MODULE(LOG_GC, "Issue finalize for the table {}@{}", count.first, xid);
                        auto table = _table_map[count.first];
                        auto entry = std::make_shared<WorkerEntry>(table, xid);
                        _worker_queue.push(entry);
                    } else if (count.second == -1) {
                        // mark the table to be cleared
                        completed.push_back(count.first);
                    }
                }

                // clear any completed tables
                for (auto &tid : completed) {
                    _tid_count.erase(tid);
                    _table_map.erase(tid);
                }

                // if there are still outstanding tables, wait for one or more to complete
                if (!_tid_count.empty()) {
                    SPDLOG_DEBUG_MODULE(LOG_GC, "Wait for outstanding tables");
                    _cv.wait(lock);
                }
            }
            lock.unlock();

            SPDLOG_DEBUG_MODULE(LOG_GC, "All tables to complete for XID {}", xid);

            // retrieve any schema changes available in Redis
            auto &&ddls = _redis_ddl.get_ddls_xid(db_id, xid);

            // commit the completed XID
            _xid_mgr->commit_xid(db_id, xid, !ddls.is_null());
            _committed_xids[db_id] = xid;

            // push any DDL changes to the FDWs
            if (!ddls.is_null()) {
                _redis_ddl.commit_ddl(db_id, xid, ddls);
            }

            SPDLOG_DEBUG_MODULE(LOG_GC, "XID committed {}", xid);

            // mark the XID as complete in the redis queue
            _redis.commit(_worker_id);

            // clear the DDL dependency data from the redis SortedSet
            std::string key = fmt::format(redis::SET_PG_OID_XIDS,
                                          Properties::get_db_instance_id(), db_id);
            pg_log_mgr::RSSOidValue set(key);
            set.remove_by_score(0, xid);
        }

        // join all of the worker threads
        for (auto &thread : _worker_threads) {
            thread.join();
        }
    }

    void
    Committer::shutdown()
    {
        _shutdown = true;
        // XXX close the redis connection to speed up the shutdown?
    }

    void
    Committer::_run_worker()
    {
        // note: also wait on an empty queue to ensure it is drained before shutdown
        while (!_shutdown || !_worker_queue.empty()) {
            // wait for work on the queue
            auto entry = _worker_queue.pop();
            if (entry == nullptr) {
                // note: this should only happen when the queue is shutdown
                break;
            }

            if (entry->do_finalize) {
                _process_finalize(entry->table, entry->xid);
            } else {
                if (entry->table->has_primary()) {
                    _process_rows(entry->table, entry->extent_id, entry->xid, entry->txid, entry->tlsn);
                } else {
                    _process_rows_no_primary(entry->table, entry->xid, entry->txid, entry->tlsn);
                }
            }

            // reduce the number of outstanding extents for this table
            boost::unique_lock lock(_mutex);
            int64_t outstanding = --_tid_count[entry->table->id()];
            lock.unlock();

            // if no more outstanding, notify the main loop
            if (outstanding < 1) {
                _cv.notify_one();
            }
        }
    }

    void
    Committer::_process_finalize(MutableTablePtr table,
                                 uint64_t xid)
    {
        SPDLOG_DEBUG_MODULE(LOG_GC, "Finalize table {}@{}", table->id(), xid);

        // finalize the table
        auto roots = table->finalize();
    }

    StorageCache::PagePtr
    Committer::_find_page(std::vector<StorageCache::PagePtr> pages,
                          TuplePtr key,
                          ExtentSchemaPtr schema)
    {
        // if only one page, return it
        if (pages.size() == 1) {
            return pages[0];
        }

        // otherwise, use the key to find the appropriate page
        auto page_i = std::lower_bound(pages.begin(), pages.end(), *key,
                                       [&schema](const StorageCache::PagePtr &page, const Tuple &key) {
                                           return FieldTuple(schema->get_sort_fields(), *(page->last())).less_than(key);
                                       });

        // return the correct page
        if (page_i == pages.end()) {
            return pages.back();
        }
        return *page_i;
    }

    bool
    Committer::_shift_to_xid(SchemaMetadata &meta,
                             const XidLsn &xid)
    {
        // loop through the history to remove any entries that are behind the provided XID
        auto history_i = meta.history.begin();
        for (; history_i != meta.history.end(); ++history_i) {
            XidLsn hxid(history_i->xid, history_i->lsn);
            if (hxid > xid) {
                break;
            }

            // we need to update the base columns to reflect this schema change
            if (history_i->update_type == SchemaUpdateType::NEW_COLUMN) {
                // add a new column
                meta.columns.push_back(*history_i);

            } else if (history_i->update_type == SchemaUpdateType::REMOVE_COLUMN) {
                // remove an existing column
                for (auto pos_i = meta.columns.begin(); pos_i != meta.columns.end(); ++pos_i) {
                    if (pos_i->position == history_i->position) {
                        meta.columns.erase(pos_i);
                        break;
                    }
                }

            } else {
                // update an existing column
                for (auto pos_i = meta.columns.begin(); pos_i != meta.columns.end(); ++pos_i) {
                    if (pos_i->position == history_i->position) {
                        *pos_i = *history_i;
                        break;
                    }
                }
            }
        }

        // if no history changes were applied, return false
        if (history_i == meta.history.begin()) {
            return false;
        }

        // remove the processed history entries
        meta.history.erase(meta.history.begin(), history_i);

        return true;
    }

    void
    Committer::_process_rows(MutableTablePtr table,
                             uint64_t extent_id,
                             uint64_t xid,
                             uint64_t txid,
                             uint64_t tlsn)
    {
        SPDLOG_DEBUG_MODULE(LOG_GC, "Process rows for {}:{}@{}", table->id(), extent_id, xid);
        auto schema_mgr = SchemaMgr::get_instance();
        auto sys_tbl_mgr = sys_tbl_mgr::Client::get_instance();

        // save the schema
        auto schema = table->schema();

        // determine if the provided extent_id needs to be forward mapped
        auto &&extent_ids = _write_cache->forward_map(table->db(), table->id(), xid, extent_id);

        // in the case of a truncate, we will have to create a new extent for any mutations
        if (txid > 0) {
            extent_ids.clear();
        }

        SPDLOG_DEBUG_MODULE(LOG_GC, "Extent remapped from {} to {} extents -- first is {}",
                            extent_id, extent_ids.size(),
                            extent_ids.empty() ? "unknown" : std::to_string(extent_ids[0]));

        // get the schema information
        XidLsn target_xid(xid);
        auto target_schema = schema_mgr->get_extent_schema(table->db(), table->id(), target_xid);

        // retrieve the pages that may be impacted
        std::vector<StorageCache::PagePtr> pages;
        for (auto extent_id : extent_ids) {
            auto page = table->read_page(extent_id);
            auto header = page->header();
            XidLsn access_xid(header.xid);

            // convert this page to a new schema if needed
            auto &&meta = sys_tbl_mgr->get_target_schema(table->db(), table->id(), access_xid, target_xid);
            if (!meta.history.empty()) {
                auto source_schema = std::make_shared<VirtualSchema>(meta);
                page->convert(source_schema, target_schema);
            }

            pages.push_back(page);
        }

        if (pages.empty()) {
            pages.push_back(table->read_page(constant::UNKNOWN_EXTENT));
        } else if (pages.size() > 1) {
            // sort the pages by primary key of the last entry so we can use lower_bound() to find
            // the correct page for modification later
            std::sort(pages.begin(), pages.end(),
                      [&target_schema](const auto &lhs, const auto &rhs) {
                          auto sort_fields = target_schema->get_sort_fields();
                          FieldTuple ltup(sort_fields, *(lhs->last()));
                          FieldTuple rtup(sort_fields, *(rhs->last()));
                          return ltup.less_than(rtup);
                      });
        }

        // note: at this point we've got a sorted list of pages that could be mutated, all at the
        //       correct schema for the target XID

        // construct a virtual schema from the previous commit XID to the target XID
        XidLsn access_xid(_committed_xids[table->db()]);
        auto &&meta = sys_tbl_mgr->get_target_schema(table->db(), table->id(), access_xid, target_xid);
        auto vschema = std::make_shared<VirtualSchema>(meta);
        auto row_fields = vschema->get_fields();
        auto key_fields = vschema->get_fields(schema->get_sort_keys());

        uint64_t cursor = 0;
        bool done = false;
        while (!done) {
            // request rows from the write cache for the provided extent ID
            auto rows = _write_cache->fetch_rows(table->db(), table->id(), extent_id, _committed_xids[table->db()], xid, 100, cursor);
            if (rows.empty()) {
                SPDLOG_DEBUG_MODULE(LOG_GC, "No more rows for {}:{}@{}", table->id(), extent_id, xid);
                done = true;
                break;
            }

            SPDLOG_DEBUG_MODULE(LOG_GC, "Found {} rows for {}:{}@{}", rows.size(), table->id(), extent_id, xid);

            // apply the changes to the pages
            for (const auto &row : rows) {
                // if this data is from before a truncate, ignore it
                if (txid > 0) {
                    if (row.xid < txid || (row.xid == txid && row.xid_seq < tlsn)) {
                        continue;
                    }
                }

                // construct a tuple from the row data
                // 2. apply data mutations by using the virtual schema on top of them
                //    a. we need to unroll the virtual schema as we pass by each schema change

                // check if we need to update the schema metadata
                access_xid = { row.xid, row.xid_seq };
                bool did_shift = _shift_to_xid(meta, access_xid);
                if (did_shift) {
                    // update the virtual schema based on the corrected metadata
                    vschema = std::make_shared<VirtualSchema>(meta);
                    row_fields = vschema->get_fields();
                    key_fields = vschema->get_fields(schema->get_sort_keys());
                }

                // XXX seems like we've got more copies here than strictly necessary... we could
                //     instead construct a read-only extent via pointers into the existing data
                //     object

                // check if we need to apply a table mutation to the extent

                // pass that tuple into the appropriate table mutation
                switch (row.op) {
                case (WriteCacheClient::RowOp::INSERT): {
                    ExtentHeader header(ExtentType(), xid, vschema->row_size(), 0);
                    Extent extent(header);
                    extent.deserialize(row.data);

                    auto value = std::make_shared<FieldTuple>(row_fields, extent.back());
                    auto key = std::make_shared<FieldTuple>(key_fields, extent.back());
                    SPDLOG_DEBUG("Insert row {} for {}:{}@{}", value->to_string(), table->id(), extent_id, xid);

                    // insert into the appropriate page
                    auto page = _find_page(pages, key, target_schema);
                    page->insert(value, target_schema);

                    break;
                }

                case (WriteCacheClient::RowOp::UPDATE): {
                    ExtentHeader header(ExtentType(), xid, vschema->row_size(), 0);
                    Extent extent(header);
                    extent.deserialize(row.data);

                    auto value = std::make_shared<FieldTuple>(row_fields, extent.back());
                    auto key = std::make_shared<FieldTuple>(key_fields, extent.back());
                    SPDLOG_DEBUG("Update row {} for {}:{}@{}", value->to_string(), table->id(), extent_id, xid);

                    // update in the appropriate page
                    auto page = _find_page(pages, key, target_schema);
                    page->update(value, target_schema);

                    break;
                }

                case (WriteCacheClient::RowOp::DELETE): {
                    auto pkey_schema = schema->create_schema(table->primary_key(), {}, table->primary_key());
                    ExtentHeader header(ExtentType(), xid, pkey_schema->row_size(), 0);
                    Extent extent(header);

                    extent.deserialize(row.pkey);
                    auto key = std::make_shared<FieldTuple>(key_fields, extent.back());
                    SPDLOG_DEBUG("Remove row {} for {}:{}@{}", key->to_string(), table->id(), extent_id, xid);

                    // remove from the appropriate page
                    auto page = _find_page(pages, key, target_schema);
                    page->remove(key, target_schema);

                    break;
                }

                }
            }
        }

        // now that we've applied all of the mutations, we need to release these pages back to the
        // cache via the table so that the appropriate callbacks are made when the pages are flushed
        table->release_pages(pages);
    }

    void
    Committer::_process_rows_no_primary(MutableTablePtr table,
                                        uint64_t xid,
                                        uint64_t txid,
                                        uint64_t tlsn)
    {
        SPDLOG_DEBUG_MODULE(LOG_GC, "Process rows with no primary key for {}@{}", table->id(), xid);
        auto schema_mgr = SchemaMgr::get_instance();
        auto sys_tbl_mgr = sys_tbl_mgr::Client::get_instance();

        // save the schema
        auto schema = table->schema();

        // get the schema information
        XidLsn target_xid(xid);
        auto target_schema = schema_mgr->get_extent_schema(table->db(), table->id(), target_xid);

        // construct a virtual schema from the previous commit XID to the target XID
        XidLsn access_xid(_committed_xids[table->db()]);
        auto &&meta = sys_tbl_mgr->get_target_schema(table->db(), table->id(), access_xid, target_xid);
        auto vschema = std::make_shared<VirtualSchema>(meta);
        auto row_fields = vschema->get_fields();

        uint64_t cursor = 0;
        bool done = false;
        while (!done) {
            // request rows from the write cache for the provided extent ID
            auto rows = _write_cache->fetch_rows(table->db(), table->id(), constant::UNKNOWN_EXTENT, _committed_xids[table->db()], xid, 100, cursor);
            if (rows.empty()) {
                SPDLOG_DEBUG_MODULE(LOG_GC, "No more rows for {}@{}", table->id(), xid);
                done = true;
                break;
            }

            SPDLOG_DEBUG_MODULE(LOG_GC, "Found {} rows for {}@{}", rows.size(), table->id(), xid);

            // apply the changes to the table
            for (const auto &row : rows) {
                // if this data is from before a truncate, ignore it
                if (txid > 0) {
                    if (row.xid < txid || (row.xid == txid && row.xid_seq < tlsn)) {
                        continue;
                    }
                }

                // construct a tuple from the row data
                // 2. apply data mutations by using the virtual schema on top of them
                //    a. we need to unroll the virtual schema as we pass by each schema change

                // check if we need to update the schema metadata
                access_xid = { row.xid, row.xid_seq };
                bool did_shift = _shift_to_xid(meta, access_xid);
                if (did_shift) {
                    // update the virtual schema based on the corrected metadata
                    vschema = std::make_shared<VirtualSchema>(meta);
                    row_fields = vschema->get_fields();
                }

                // XXX seems like we've got more copies here than strictly necessary... we could
                //     instead construct a read-only extent via pointers into the existing data
                //     object

                // check if we need to apply a table mutation to the extent

                // pass that tuple into the appropriate table mutation
                switch (row.op) {
                case (WriteCacheClient::RowOp::INSERT): {
                    ExtentHeader header(ExtentType(), xid, vschema->row_size(), 0);
                    Extent extent(header);
                    extent.deserialize(row.data);

                    auto value = std::make_shared<FieldTuple>(row_fields, extent.back());
                    SPDLOG_DEBUG("Insert row {} for {}@{}", value->to_string(), table->id(), xid);

                    // append the row to the table
                    table->insert(value, xid, constant::UNKNOWN_EXTENT);
                    break;
                }

                case (WriteCacheClient::RowOp::UPDATE): {
                    // note: we should never see an update from a table with no primary key
                    assert(0);
                    break;
                }

                case (WriteCacheClient::RowOp::DELETE): {
                    ExtentHeader header(ExtentType(), xid, schema->row_size(), 0);
                    Extent extent(header);
                    extent.deserialize(row.pkey);

                    auto value = std::make_shared<FieldTuple>(row_fields, extent.back());
                    SPDLOG_DEBUG("Remove row {} for {}@{}", value->to_string(), table->id(), xid);

                    // remove from the table -- will invoke an index scan
                    table->remove(value, xid, constant::UNKNOWN_EXTENT);
                    break;
                }
                }
            }
        }
    }
}
