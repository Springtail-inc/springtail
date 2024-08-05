#include <garbage_collector/committer.hh>

#include <storage/constants.hh>
#include <storage/table_mgr.hh>

namespace springtail::gc {

    void
    Committer::run()
    {
        // initiate the worker threads
        for (int i = 0; i < _worker_count; i++) {
            _worker_threads.push_back(std::thread(&Committer::_run_worker, this));
        }

        // get the most recent committed XID
        _committed_xid = _xid_mgr->get_committed_xid();

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
            uint64_t xid = result->xid();

            SPDLOG_INFO("Commit XID: {}", xid);

            // find every table associated with this XID
            uint64_t table_cursor = 0;
            bool tid_done = false;
            while (!tid_done) {
                // query the write cache for the tables modified through this XID
                auto table_list = _write_cache->list_tables(_committed_xid, xid, 100, table_cursor);

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
                    auto table_changes = _write_cache->fetch_table_changes(tid, _committed_xid, xid);
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
                    auto table = TableMgr::get_instance()->get_mutable_table(tid, _committed_xid, xid, true);

                    // check if we need to perform a table truncate
                    if (txid > 0) {
                        // XXX
                        // table->truncate();
                    }

                    boost::unique_lock lock(_mutex);
                    _table_map[tid] = table;
                    lock.unlock();

                    bool eid_done = false;
                    while (!eid_done) {
                        // request the extents modified in each table
                        auto extent_list = _write_cache->list_extents(tid, _committed_xid, xid, 100, extent_cursor);

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

            // XXX This is where we need to implement the schema changes in the FDW.  We need to
            //     perform the following actions:
            //     1. pre-commit the XID -- this will block the FDW operation for new queries
            //     2. connect to each FDW and perform the schema+metadata changes for this XID
            //     3. commit the XID -- mark the XID as ready in the XID mgr which unblocks the FDWs

            // note: if we want to implement roll-forward on query, then we could apply the schema
            //       changes after the data changes are written to the write cache

            // XXX pre-commit the XID
            // _xid_mgr->pre_commit_xid(xid);

            // perform the schema mutations in the FDWs

            
            // commit the completed XID
            _xid_mgr->commit_xid(xid);
            _committed_xid = xid;

            SPDLOG_DEBUG_MODULE(LOG_GC, "XID committed {}", xid);

            // mark the XID as complete in the redis queue
            _redis.commit(_worker_id);
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
                _process_rows(entry->table, entry->extent_id, entry->xid, entry->txid, entry->tlsn);
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

    void
    Committer::_process_rows(MutableTablePtr table,
                             uint64_t extent_id,
                             uint64_t xid,
                             uint64_t txid,
                             uint64_t tlsn)
    {
        SPDLOG_DEBUG_MODULE(LOG_GC, "Process rows for {}:{}@{}", table->id(), extent_id, xid);

        // save the schema
        auto schema = table->schema();

        // determine if the provided extent_id needs to be forward mapped
        uint64_t mapped_eid = extent_id;
        auto &&extent_ids = _write_cache->forward_map(table->id(), xid, extent_id);
        if (extent_ids.empty()) {
            // no mapping, use provided extent_id
            if (txid > 0) {
                // in the case of a truncate, we don't know the extent ID
                mapped_eid = constant::UNKNOWN_EXTENT;
            }
        } else if (extent_ids.size() == 1) {
            // single extent output, use it
            mapped_eid = extent_ids.front();
        } else {
            // XXX need to create a list of key -> extent_id to figure out which extent a given row
            // should be applied to
            assert(0);
        }

        SPDLOG_DEBUG_MODULE(LOG_GC, "Extent remapped from {} to {}", extent_id, mapped_eid);

        StorageCache::PagePtr page;
        std::vector<SchemaColumn> changes;
        if (mapped_eid != constant::UNKNOWN_EXTENT) {
            // 1) retrieve the XID at which the given extent was written
            // XXX
            // auto page = table->read_page(mapped_eid);

            // 2) apply all of the schema changes to the page
            // XXX
            // page->apply_schema_changes(_committed_xid, xid);
            // page = _apply_schema_changes(page, _committed_xid, xid, constant::MAX_LSN);
        }

            // StorageCache::PagePtr page;
            // std::vector<SchemaColumn> changes;
            // if (mapped_eid != constant::UNKNOWN_EXTENT) {
            //     // 1) retrieve the XID at which the given extent was written
            //     auto page = table->read_page(mapped_eid);

            //     // 2) apply all of the schema changes to the page
            //     page = _apply_schema_changes(page, _committed_xid, xid, constant::MAX_LSN);

            // }
            // auto change_i = changes.begin();

        uint64_t cursor = 0;
        bool done = false;
        while (!done) {
            // request rows from the write cache for the provided extent ID
            auto rows = _write_cache->fetch_rows(table->id(), extent_id, _committed_xid, xid, 100, cursor);
            if (rows.empty()) {
                SPDLOG_DEBUG_MODULE(LOG_GC, "No more rows for {}:{}@{}", table->id(), extent_id, xid);
                done = true;
                break;
            }

            SPDLOG_DEBUG_MODULE(LOG_GC, "Found {} rows for {}:{}@{}", rows.size(), table->id(), extent_id, xid);

            // apply the changes to the extent
            // XXX It would be more efficient to retrieve the page and apply all of the changes
            //     at once rather than looking it up every time.  This would require changes to
            //     the MutableTable interfaces.
            auto row_fields = schema->get_fields();
            auto key_fields = schema->get_fields(schema->get_sort_keys());

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
                auto vschema = SchemaMgr::get_instance()->get_schema(table->id(), { row.xid, row.xid_seq }, { xid, constant::MAX_LSN });
                row_fields = vschema->get_fields();
                key_fields = vschema->get_fields(schema->get_sort_keys());

                // XXX seems like we've got more copies here than strictly necessary... we could
                //     instead construct a read-only extent via pointers into the existing data
                //     object

                // check if we need to apply a table mutation to the extent

                // pass that tuple into the appropriate table mutation
                switch (row.op) {
                case (WriteCacheClient::RowOp::INSERT): {
                    ExtentHeader header(ExtentType(), xid, schema->row_size(), 0);
                    Extent extent(header);
                    extent.deserialize(row.data);

                    auto value = std::make_shared<FieldTuple>(row_fields, extent.back());
                    SPDLOG_DEBUG("Insert row {} for {}:{}@{}", value->to_string(), table->id(), mapped_eid, xid);
                    table->insert(value, xid, mapped_eid);
                    break;
                }

                case (WriteCacheClient::RowOp::UPDATE): {
                    ExtentHeader header(ExtentType(), xid, schema->row_size(), 0);
                    Extent extent(header);
                    extent.deserialize(row.data);

                    auto value = std::make_shared<FieldTuple>(row_fields, extent.back());
                    table->update(value, xid, mapped_eid);
                    break;
                }

                case (WriteCacheClient::RowOp::DELETE): {
                    auto pkey_schema = schema->create_schema(table->primary_key(), {}, table->primary_key());
                    ExtentHeader header(ExtentType(), xid, pkey_schema->row_size(), 0);
                    Extent extent(header);

                    extent.deserialize(row.pkey);
                    auto key = std::make_shared<FieldTuple>(key_fields, extent.back());
                    table->remove(key, xid, mapped_eid);
                    break;
                }

                }
            }
        }
    }

    StorageCache::PagePtr
    Committer::_apply_schema_changes(StorageCache::PagePtr page,
                                     uint64_t access_xid,
                                     uint64_t target_xid,
                                     uint64_t target_lsn)
    {
        // XXX
        uint64_t table_id = 0;
        std::filesystem::path file = "";

        // get a clean page at the target XID
        auto new_page = StorageCache::get_instance()->get(file, constant::UNKNOWN_EXTENT, target_xid);

        // construct a virtual schema to transform the original page's data to the schema at the target XID
        auto vschema = SchemaMgr::get_instance()->get_schema(table_id, XidLsn(access_xid), { target_xid, target_lsn });

        // scan the current page and write it's contents to the new page
        auto vfields = vschema->get_fields();
        // XXX
        // for (auto &&row : *page) {
        //     new_page->append(FieldTuple(vfields, row));
        // }

        return new_page;
    }
}
