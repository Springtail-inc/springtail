#include <common/coordinator.hh>
#include <common/constants.hh>
#include <common/filesystem.hh>

#include <garbage_collector/committer.hh>
#include <garbage_collector/log_parser.hh>

#include <pg_log_mgr/pg_log_mgr.hh>

#include <sys_tbl_mgr/client.hh>
#include <sys_tbl_mgr/table_mgr.hh>

#include <xid_mgr/xid_mgr_client.hh>

namespace springtail::gc {

    void
    LogParser::run()
    {
        // perform cleanup for any LogParser threads in a previous run
        cleanup();

        // start the service
        for (int i = 0; i < _reader_threads.size(); ++i) {
            _reader_threads[i] = std::thread(&Reader::run, &_reader, i);
        }

        for (int i = 0; i < _parser_threads.size(); ++i) {
            _parser_threads[i] = std::thread(&Parser::run, &_parser, i);
        }
    }

    void
    LogParser::shutdown()
    {
        // signal the readers to shutdown
        _reader.shutdown();

        // wait for the readers to complete
        for (auto &thread : _reader_threads) {
            thread.join();
        }

        // once the readers have completed, signal the queue to shutdown
        _parser_queue->shutdown();

        // wait for the parsers to complete
        for (auto &thread : _parser_threads) {
            thread.join();
        }
    }

    void
    LogParser::cleanup()
    {
        auto coordinator = Coordinator::get_instance();
        constexpr auto daemon_type = Coordinator::DaemonType::GC_MGR;

        std::vector<std::string> cleanup_threads;
        RedisDDL redis_ddl;
        RedisQueue<pg_log_mgr::PgXactMsg>
            reader_queue(fmt::format(redis::QUEUE_PG_TRANSACTIONS,
                                     Properties::get_db_instance_id()));

        // retrieve all of the threads for the daemon
        // note: we do this because there is a single GC daemon for both GC1 and GC2
        auto &&threads = coordinator->get_threads(daemon_type);
        for (const auto &thread_id : threads) {
            // check which class this is for
            std::vector<std::string> parts;
            springtail::common::split_string("_", thread_id, parts);

            // check the id is valid
            assert(parts.size() == 3 &&
                   (parts[0] == THREAD_TYPE || parts[0] == Committer::THREAD_TYPE));

            // only handle "parser" threads here
            if (parts[0] != THREAD_TYPE) {
                continue;
            }
            cleanup_threads.push_back(thread_id);

            // perform thread-type-specific cleanup
            if (parts[1] == THREAD_READER) {
                // get the work item from the reader_queue
                auto entry = reader_queue.work_item(thread_id);
                if (entry) {
                    if (entry->type == pg_log_mgr::PgXactMsg::Type::XACT_MSG) {
                        auto &msg = std::get<pg_log_mgr::PgXactMsg::XactMsg>(entry->msg);

                        // clear the queue of DDLs for the in-flight XID
                        redis_ddl.clear_ddls_xid(msg.db_id, msg.xid);
                    }

                    // abort any in-flight XID
                    reader_queue.abort(thread_id);
                }
            }
        }

        // also clear the notification queue from the GC2 since we don't need to unblock
        RedisQueue<XidReady> parser_notify(fmt::format(redis::QUEUE_GC_PARSER_NOTIFY,
                                                       Properties::get_db_instance_id()));
        parser_notify.clear();

        // unregister all parser threads from the previous run
        coordinator->unregister_threads(daemon_type, cleanup_threads);
    }

    bool
    LogParser::Backlog::empty() const
    {
        boost::shared_lock lock(_mutex);
        return _backlog.empty();
    }

    bool
    LogParser::Backlog::check(uint64_t db_id,
                              uint64_t oid,
                              uint64_t xid)
    {
        boost::shared_lock lock(_mutex);

        // check if there are any dependencies for this OID
        auto &&i = _table_deps.find({ db_id, oid });
        if (i == _table_deps.end()) {
            return false;
        }

        // check if there are any XIDs earlier than this XID with changes to the OID
        if (*i->second.begin() < xid) {
            return true;
        }

        return false;
    }

    void
    LogParser::Backlog::push(uint64_t db_id,
                             uint64_t oid,
                             uint64_t xid,
                             StatePtr entry)
    {
        boost::unique_lock lock(_mutex);

        // note: There can be a race condition between check() and push(), so we re-perform
        //       the check() here.  If it is no longer blocked, place the request on the
        //       ready queue directly.
        auto &&i = _table_deps.find({ db_id, oid });
        if (i == _table_deps.end() || *i->second.begin() > xid) {
            _ready.push_front(entry);
            return;
        }

        SPDLOG_DEBUG_MODULE(LOG_GC, "Blocking db {} @ xid {} on table {}", db_id, xid, oid);

        // now add the depencency to the backlog
        _backlog[{ db_id, xid }] = { oid, entry };
        _oid_backlog[{ db_id, oid }].insert(xid);
    }

    LogParser::StatePtr
    LogParser::Backlog::pop()
    {
        boost::unique_lock lock(_mutex);

        // if nothing is ready, return nullptr
        if (_ready.empty()) {
            return nullptr;
        }

        auto request = _ready.back();
        _ready.pop_back();
        return request;
    }

    void
    LogParser::Backlog::update_deps(uint64_t db_id)
    {
        boost::unique_lock lock(_mutex);

        // pull the dependencies from redis for this db
        uint64_t last_xid = _last_requested_xid[db_id];

        auto set_i = _oid_set.find(db_id);
        if (set_i == _oid_set.end()) {
            std::string key = fmt::format(redis::SET_PG_OID_XIDS,
                                          Properties::get_db_instance_id(), db_id);
            auto res = _oid_set.emplace(db_id, pg_log_mgr::RSSOidValue(key));
            if (!res.second) {
                throw Error();
            }

            set_i = res.first;
        }
        auto &oid_set = set_i->second;

        auto &&values = oid_set.get_by_score(last_xid + 1);
        if (!values.empty()) {
            // update the dependency mappings
            for (auto const &value : values) {
                _table_deps[{ db_id, value.oid }].insert(value.xid);
                _xid_map[{ db_id, value.xid }].insert(value.oid);
            }

            // save that we pulled data through the last seen XID
            _last_requested_xid[db_id] = values.back().xid;
        }
    }

    void
    LogParser::Backlog::clear_dep(uint64_t db_id,
                                  uint64_t xid)
    {
        boost::unique_lock lock(_mutex);

        SPDLOG_DEBUG_MODULE(LOG_GC, "clear_dep: db {}, xid {}", db_id, xid);

        auto i = _xid_map.find({ db_id, xid });
        if (i == _xid_map.end()) {
            return;
        }

        for (uint64_t oid : i->second) {
            // remove the xid from the set of blocking XIDs for the OID
            auto &&j = _table_deps.find({ db_id, oid });
            j->second.erase(xid);

            // if we removed the last dependency on this OID, release all blocked XIDs
            uint64_t next_dep_xid = 0;
            if (j->second.empty()) {
                _table_deps.erase(j);
            } else {
                // if there are still dependencies, we can only release XIDs that are less than this one
                next_dep_xid = *j->second.begin();
            }

            // unblock any XIDs that were waiting on this table
            _unblock_xids(db_id, oid, next_dep_xid);
        }

        // clear this XID from the list of blockers
        _xid_map.erase(i);
    }

    void
    LogParser::Backlog::clear_table(uint64_t db_id,
                                    uint64_t oid)
    {
        boost::unique_lock lock(_mutex);

        SPDLOG_DEBUG_MODULE(LOG_GC, "clear_table: db {}, oid {}", db_id, oid);

        // clear the record of XIDs that contain blocking operations for this table, since they won't be applied
        auto table_i = _table_deps.find({ db_id, oid });
        if (table_i != _table_deps.end()) {
            // remove the table from each XID
            for (auto xid : table_i->second) {
                auto xid_i = _xid_map.find({ db_id, xid });
                if (xid_i != _xid_map.end()) {
                    // remove the table from the set of tables this XID modifies
                    xid_i->second.erase(oid);
                    if (xid_i->second.empty()) {
                        // if the set of tables is empty, remove this XID entirely
                        _xid_map.erase(xid_i);
                    }
                }
            }

            // remove the table dependencies
            _table_deps.erase(table_i);

            // unblock any XIDs that were waiting on this table
            _unblock_xids(db_id, oid);
        }
    }

    void
    LogParser::Backlog::_unblock_xids(uint64_t db_id,
                                      uint64_t oid,
                                      uint64_t next_dep_xid)
    {
        // find the set of XIDs waiting on this OID
        auto &&xid_set_i = _oid_backlog.find({ db_id, oid });

        // note: there might not be any XIDs waiting on this OID, despite the dependency
        if (xid_set_i != _oid_backlog.end()) {
            auto &blocked_xids = xid_set_i->second;

            // release XIDs blocked on this OID
            auto &&xid_i = blocked_xids.begin();
            while (xid_i != blocked_xids.end()) {
                if (next_dep_xid && *xid_i > next_dep_xid) {
                    break;
                }

                SPDLOG_DEBUG_MODULE(LOG_GC, "Unblocking db {} @ xid {}", db_id, *xid_i);

                // move the request to the ready queue
                auto &&b = _backlog.find({ db_id, *xid_i });
                _ready.push_front(b->second.second);
                _backlog.erase(b);

                blocked_xids.erase(xid_i);
                xid_i = blocked_xids.begin();
            }

            // clear the entry for this OID
            if (blocked_xids.empty()) {
                _oid_backlog.erase(xid_set_i);
            }
        }
    }


    bool
    LogParser::SyncTracker::mark_resync(uint64_t db_id,
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
    LogParser::SyncTracker::add_sync(const pg_log_mgr::PgXactMsg::TableSyncMsg &sync_msg)
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

        // store it against the pg_xid for this sync's snapshot
        auto &db_map = (db_i == _sync_map.end()) ? _sync_map[sync_msg.db_id] : db_i->second;
        db_map[sync_msg.pg_xid] = record;

        // also keep a map to the record for each table being copied
        auto &table_map = _table_map[sync_msg.db_id];
        for (int32_t table_id : sync_msg.tids) {
            SPDLOG_DEBUG_MODULE(LOG_GC, "Add table: {}", table_id);
            table_map[table_id] = record; // add the record to the sync map
        }

        // record the target XID of the sync
        _target_xid_map[sync_msg.db_id] = sync_msg.target_xid;

        return first_table;
    }

    std::optional<XidReady>
    LogParser::SyncTracker::check_commit(uint64_t db_id,
                                         uint32_t pg_xid)
    {
        SPDLOG_DEBUG_MODULE(LOG_GC, "db {} pg_xid {}", db_id, pg_xid);
        boost::unique_lock lock(_mutex);
        
        // get the map for this database
        auto db_i = _sync_map.find(db_id);
        if (db_i == _sync_map.end()) {
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
            return {};
        }

        // We pass the target XID to the committer to be used if the completed XID is not ahead of the committed XID.
        uint64_t xid = _target_xid_map[db_id];

        auto type = XidReady::Type::TABLE_SYNC_SWAP;
        if (db_i->second.empty()) {
            type = XidReady::Type::TABLE_SYNC_COMMIT;
            _sync_map.erase(db_i);
            _target_xid_map.erase(db_id);
        }

        // construct an XidReady record from the XidRecord objects
        std::vector<uint64_t> tids;
        for (auto record : completed) {
            tids.insert(tids.end(), record->tids().begin(), record->tids().end());
        }
        SPDLOG_DEBUG_MODULE(LOG_GC, "Found {} tables", tids.size());

        return XidReady(type, db_id, XidReady::SwapMsg(xid, std::move(tids)));
    }

    void
    LogParser::SyncTracker::clear_tables(uint64_t db_id,
                                         const XidReady &commit_msg)
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
    LogParser::SyncTracker::should_skip(uint64_t db_id,
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

    void
    LogParser::Reader::run(int thread_id)
    {
        std::string worker_id = fmt::format("{}_{}_{}", THREAD_TYPE, THREAD_READER, thread_id);

        auto coordinator = Coordinator::get_instance();
        constexpr auto daemon_type = Coordinator::DaemonType::GC_MGR;

        // register the thread on startup
        coordinator->register_thread(daemon_type, worker_id);

        // loop until we are asked to shutdown; on shutdown drain the backlog
        while (!_shutdown || !_backlog.empty()) {
            // a flag used to skip sending an entire XID at the committer
            bool skip_xid = false;

            // check the backlog for an XID to process
            StatePtr state = _backlog.pop();
            if (state == nullptr) {
                // if nothing ready in backlog, pull an XID from redis
                {
                    boost::unique_lock lock(_mutex);

                    // check if we need to wait for a table sync swap/commit to occur
                    _halt_cond.wait(lock, [this]{ return !_halt_processing; });

                    // note: blocks for 1 second
                    auto xact_msg = _reader_queue.pop(worker_id, 1);
                    if (xact_msg == nullptr) {
                        // nothing ready, loop and try again
                        continue;
                    }

                    // record the table sync metadata
                    if (xact_msg->type == pg_log_mgr::PgXactMsg::Type::TABLE_SYNC_MSG) {
                        const auto &msg = std::get<pg_log_mgr::PgXactMsg::TableSyncMsg>(xact_msg->msg);
                        SPDLOG_DEBUG_MODULE(LOG_GC, "Got TABLE_SYNC_MSG on {} @ {}", msg.db_id, msg.target_xid);

                        bool sync_start = _sync_tracker.add_sync(msg);
                        if (sync_start) {
                            SPDLOG_DEBUG_MODULE(LOG_GC, "First TABLE_SYNC_MSG on {}", msg.db_id);

                            // this is the first table sync, issue a message to the Committer to stop committing XIDs
                            _gc_queue.push(XidReady(msg.db_id));
                        }

                        _reader_queue.commit(worker_id);
                        continue;
                    }

                    if (xact_msg->type == pg_log_mgr::PgXactMsg::Type::TABLE_SYNC_END_MSG) {
                        // handle the sync end message
                        const auto &msg = std::get<pg_log_mgr::PgXactMsg::TableSyncEndMsg>(xact_msg->msg);
                        SPDLOG_DEBUG_MODULE(LOG_GC, "Got TABLE_SYNC_END_MSG on {}", msg.db_id);

                        // note: currently nothing to do... from here we must wait for a transaction
                        //       that allows us to swap and commit the resync'd tables
                        _reader_queue.commit(worker_id);
                        continue;
                    }

                    assert (xact_msg->type == pg_log_mgr::PgXactMsg::Type::XACT_MSG);
                    auto &entry = std::get<pg_log_mgr::PgXactMsg::XactMsg>(xact_msg->msg);

                    // add the XID to the map to ensure we commit in-order
                    _xid_map[entry.db_id][entry.xid];

                    // check if this record's pg_xid indicates that we need to perform a sync
                    // swap/commit before proceeding
                    auto &&commit_msg = _sync_tracker.check_commit(entry.db_id, entry.pg_xid);
                    if (commit_msg) {
                        // block other Reader threads from processing newer XIDs
                        // XXX This blocks all databases, but ideally it would only block this
                        //     database.  Implementing that would require some way to separately
                        //     queue messages for this database while continuing to process messages
                        //     for other databases...
                        _halt_processing = true;

                        // wait for prior XIDs to complete processing to ensure all records that
                        // may have mutations that need to be skipped are processed
                        _wait_for_xid(entry.db_id, entry.xid, false);

                        // synchronously issue the swap/commit at the GC-2
                        SPDLOG_DEBUG_MODULE(LOG_GC, "Issue TABLE_SYNC_COMMIT on {} @ {}", entry.db_id, entry.xid);
                        _gc_queue.push(*commit_msg);

                        // block until the Committer notifies us that the swap/commit is complete
                        // XXX this actually blocks all databases, but logically we only need to
                        //     block one and could process messages for the others.  Or better yet,
                        //     we could optimistically continue and only block if we attempt to
                        //     mutate a table being swapped prior to the TABLE_SYNC_COMMIT
                        //     completing.
                        _parser_notify.pop_and_commit();

                        // once the swap/commit is complete, we can clear the entry from the sync
                        // tracker and continue processing
                        _sync_tracker.clear_tables(entry.db_id, *commit_msg);

                        // notify any blocked Reader threads
                        _halt_processing = false;
                        _halt_cond.notify_all();
                    }

                    state = std::make_shared<State>(xact_msg);
                }

                // update the schema dependencies from Redis
                _backlog.update_deps(state->entry.db_id);
            }

            SPDLOG_INFO("Process XID: {}", state->entry.xid);

            // once we have an XID, scan the individual mutations from the log
            uint64_t begin_offset = state->entry.begin_offset;
            uint64_t commit_offset = (state->entry.begin_path == state->entry.commit_path)
                ? state->entry.commit_offset
                : -1;

            std::vector<char> filter = START_FILTER;

            bool done = false;
            bool blocked = false;
            while (!done && !blocked) {
                SPDLOG_DEBUG_MODULE(LOG_GC, "Processing {} -- Read file {} {}",
                                    state->entry.xid, state->entry.begin_path, begin_offset);

                _reader.set_file(state->entry.begin_path, begin_offset, commit_offset);

                bool end_of_stream = false;
                while (!end_of_stream && !blocked) {
                    // record the position of the message we are about to read, in case we
                    // need to block and re-process it later
                    uint64_t offset = _reader.offset();

                    // read the next message, if it doesn't pass the filter, skip and continue
                    auto msg = _reader.read_message(filter);
                    ++state->lsn;

                    // handle message skipping
                    if (msg == nullptr) {
                        end_of_stream = _reader.end_of_stream();
                        continue;
                    }

                    SPDLOG_DEBUG_MODULE(LOG_GC, "Processing {} -- Got msg {}", state->entry.xid, static_cast<uint8_t>(msg->msg_type));

                    // handle the message
                    switch(msg->msg_type) {
                    case PgMsgEnum::BEGIN: {
                        auto &begin_msg = std::get<PgMsgBegin>(msg->msg);
                        if (begin_msg.xid == state->entry.pg_xid) {
                            filter = BEGIN_FILTER;
                        }
                        break;
                    }

                    case PgMsgEnum::COMMIT: {
                        // stop processing
                        end_of_stream = true;
                        done = true;
                        break;
                    }

                    case PgMsgEnum::STREAM_START: {
                        auto &stream_start_msg = std::get<PgMsgStreamStart>(msg->msg);
                        if (stream_start_msg.xid == state->entry.pg_xid) {
                            state->process_as_stream = true;
                            filter = STREAM_START_FILTER;
                        }
                        break;
                    }

                    case PgMsgEnum::STREAM_STOP: {
                        // auto &stream_stop_msg = std::get<PgMsgStreamStop>(msg->msg);
                        filter = STREAM_STOP_FILTER;
                        break;
                    }

                    case PgMsgEnum::STREAM_COMMIT: {
                        auto &stream_commit_msg = std::get<PgMsgStreamCommit>(msg->msg);
                        if (stream_commit_msg.xid == state->entry.pg_xid) {
                            // stop processing
                            end_of_stream = true;
                            done = true;
                        }
                        break;
                    }

                    case PgMsgEnum::INSERT: {
                        auto &insert_msg = std::get<PgMsgInsert>(msg->msg);

                        blocked = _process_mutation(state, insert_msg.rel_id, msg, offset);
                        break;
                    }
                    case PgMsgEnum::DELETE: {
                        auto &delete_msg = std::get<PgMsgDelete>(msg->msg);

                        blocked = _process_mutation(state, delete_msg.rel_id, msg, offset);
                        break;
                    }
                    case PgMsgEnum::UPDATE: {
                        auto &update_msg = std::get<PgMsgUpdate>(msg->msg);

                        blocked = _process_mutation(state, update_msg.rel_id, msg, offset);
                        break;
                    }
                    case PgMsgEnum::TRUNCATE: {
                        auto &truncate_msg = std::get<PgMsgTruncate>(msg->msg);

                        if (state->process_as_stream && _check_aborted_xid(state, state->entry.pg_xid)) {
                            // skip the entry due to aborted sub-transaction
                            blocked = false;

                        } else {
                            // check all of the tables referenced by the truncate
                            // note: there may be multiple due to CASCADE
                            for (auto rel_id : truncate_msg.rel_ids) {
                                // check if we should skip this specific truncate due to ongoing table sync
                                bool skip = _sync_tracker.should_skip(state->entry.db_id, rel_id,
                                                                      state->entry.pg_xid);
                                if (!skip) {
                                    // if we aren't skipping then we can check the backlog to see if we need to block
                                    blocked = _check_backlog(state, rel_id, offset);
                                    if (blocked) {
                                        break;
                                    }
                                }
                            }

                            if (!blocked) {
                                for (auto rel_id : truncate_msg.rel_ids) {
                                    // check if we should skip this specific truncate due to ongoing table sync
                                    bool skip = _sync_tracker.should_skip(state->entry.db_id, rel_id,
                                                                          state->entry.pg_xid);
                                    if (!skip) {
                                        state->mutation_count->increment();
                                        auto entry = std::make_shared<ParserEntry>(msg, state->mutation_count, state->entry.xid, state->lsn, rel_id, state->entry.db_id);
                                        _parser_queue->push(entry);
                                    }
                                }
                            }
                        }
                        break;
                    }

                    case PgMsgEnum::CREATE_TABLE: {
                        auto &table_msg = std::get<PgMsgTable>(msg->msg);

                        // check if we should ignore this message based on ongoing table sync
                        bool skip = _sync_tracker.should_skip(state->entry.db_id, table_msg.oid,
                                                              state->entry.pg_xid);
                        if (skip) {
                            blocked = false;
                        } else {
                            // schema changes should be applied in-order, so need to block this
                            // operation if there are earlier un-applied schema changes
                            blocked = _check_backlog(state, table_msg.oid, offset);
                            if (!blocked) {
                                // apply the schema change
                                XidLsn xid(state->entry.xid, state->lsn);
                                auto &&ddl_stmt = sys_tbl_mgr::Client::get_instance()->create_table(state->entry.db_id, xid, table_msg);

                                // note: we don't notify the backlog until the entire XID is
                                //       processed since there might be additional schema changes

                                // record the DDL statement for this change into Redis to eventually be provided to the FDWs
                                _redis_ddl.add_ddl(state->entry.db_id, xid.xid, ddl_stmt);
                            }
                        }
                        break;
                    }

                    case PgMsgEnum::ALTER_TABLE: {
                        auto &table_msg = std::get<PgMsgTable>(msg->msg);

                        // check if we should ignore this message based on ongoing table sync
                        bool skip = _sync_tracker.should_skip(state->entry.db_id, table_msg.oid,
                                                              state->entry.pg_xid);
                        if (skip) {
                            blocked = false;
                        } else {
                            // schema changes should be applied in-order, so need to block this
                            // operation if there are earlier un-applied schema changes
                            blocked = _check_backlog(state, table_msg.oid, offset);
                            if (!blocked) {
                                // apply the schema change
                                XidLsn xid(state->entry.xid, state->lsn);
                                auto &&ddl_stmt = sys_tbl_mgr::Client::get_instance()->alter_table(state->entry.db_id, xid, table_msg);

                                // need to re-sync the table in some cases, eg type change
                                nlohmann::json action = nlohmann::json::parse(ddl_stmt).at("action");
                                if (action.get<std::string>() == "resync") {
                                    SPDLOG_DEBUG_MODULE(LOG_GC, "Got a DDL change causing a resync: db {}, xid@{}:{}",
                                                        state->entry.db_id, xid.xid, xid.lsn);

                                    // mark the table to be ignored in the _sync_tracker
                                    bool is_first = _sync_tracker.mark_resync(state->entry.db_id, table_msg.oid);

                                    // clear the table from the backlog in case any other threads
                                    // are waiting for mutations to it (since those mutations will
                                    // now be ignored)
                                    _backlog.clear_table(state->entry.db_id, table_msg.oid);

                                    // notify the PgLogParser to resync the table
                                    auto key = fmt::format(redis::QUEUE_SYNC_TABLES,
                                                           Properties::get_db_instance_id(), state->entry.db_id);
                                    RedisQueue<std::string> table_sync_queue(key);
                                    table_sync_queue.push(std::to_string(table_msg.oid));

                                    // notify the Committer to stop committing XIDs
                                    if (is_first) {
                                        _gc_queue.push(XidReady(state->entry.db_id));
                                    }
                                } else if (action.get<std::string>() != "no_change") {
                                    // record the DDL statement for this change into Redis to eventually be provided to the FDWs
                                    _redis_ddl.add_ddl(state->entry.db_id, xid.xid, ddl_stmt);
                                }
                            }
                        }
                        break;
                    }

                    case PgMsgEnum::DROP_TABLE: {
                        auto &drop_msg = std::get<PgMsgDropTable>(msg->msg);

                        // check if we should ignore this message based on ongoing table sync
                        bool skip = _sync_tracker.should_skip(state->entry.db_id, drop_msg.oid,
                                                              state->entry.pg_xid);
                        if (skip) {
                            blocked = false;
                        } else {
                            // schema changes should be applied in-order, so need to block this
                            // operation if there are earlier un-applied schema changes
                            blocked = _check_backlog(state, drop_msg.oid, offset);
                            if (!blocked) {
                                // apply the schema change
                                XidLsn xid(state->entry.xid, state->lsn);
                                auto &&ddl_stmt = sys_tbl_mgr::Client::get_instance()->drop_table(state->entry.db_id, xid, drop_msg);

                                // note: considered doing a truncate, but shouldn't be necessary
                                //       since we check for table existence when we perform a lookup
                                //       for the roots

                                // note: we don't notify the backlog until the entire XID is
                                //       processed since there might be additional schema changes

                                // record the DDL statement for this change into Redis to eventually be provided to the FDWs
                                _redis_ddl.add_ddl(state->entry.db_id, xid.xid, ddl_stmt);
                            }
                        }
                        break;
                    }
                    case PgMsgEnum::CREATE_INDEX: {
                        auto &index_msg = std::get<PgMsgIndex>(msg->msg);

                        // check if we should ignore this message based on ongoing table sync
                        bool skip = _sync_tracker.should_skip(state->entry.db_id, index_msg.oid,
                                                              state->entry.pg_xid);
                        if (skip) {
                            blocked = false;
                        } else {
                            // schema changes should be applied in-order, so need to block this
                            // operation if there are earlier un-applied schema changes
                            blocked = _check_backlog(state, index_msg.oid, offset);
                            if (!blocked) {
                                // apply the schema change
                                XidLsn xid(state->entry.xid, state->lsn);
                                auto &&ddl_stmt = sys_tbl_mgr::Client::get_instance()->create_index(state->entry.db_id, xid, index_msg);

                                // note: we don't notify the backlog until the entire XID is
                                //       processed since there might be additional schema changes

                                // no need to record the DDL statement for this change
                            }
                        }
                        break;
                    }
                    case PgMsgEnum::DROP_INDEX: {
                        auto &index_msg = std::get<PgMsgDropIndex>(msg->msg);

                        // check if we should ignore this message based on ongoing table sync
                        bool skip = _sync_tracker.should_skip(state->entry.db_id, index_msg.oid,
                                                              state->entry.pg_xid);
                        if (skip) {
                            blocked = false;
                        } else {
                            // schema changes should be applied in-order, so need to block this
                            // operation if there are earlier un-applied schema changes
                            blocked = _check_backlog(state, index_msg.oid, offset);
                            if (!blocked) {
                                // apply the schema change
                                XidLsn xid(state->entry.xid, state->lsn);
                                auto &&ddl_stmt = sys_tbl_mgr::Client::get_instance()->drop_index(state->entry.db_id, xid, index_msg);

                                // note: we don't notify the backlog until the entire XID is
                                //       processed since there might be additional schema changes

                                // no need to record the DDL statement for this change
                            }
                        }
                        break;
                    }
                    case PgMsgEnum::COPY_SYNC: {
                        auto &copy_sync_msg = std::get<PgMsgCopySync>(msg->msg);
                        SPDLOG_DEBUG_MODULE(LOG_GC, "Got COPY_SYNC on {} @ {}, pg_xid={}",
                                            state->entry.db_id, copy_sync_msg.target_xid, copy_sync_msg.pg_xid);

                        // note: we will skip this XID at the committer since it's only for
                        //       triggering a TABLE_SYNC_COMMIT, which is done above
                        skip_xid = true;
                        break;
                    }

                    default:
                        // message should have been filtered, error?
                        SPDLOG_ERROR("Received invalid message type: {}", static_cast<int>(msg->msg_type));
                        break;
                    }
                }

                // if we can stop processing, fast exit
                if (blocked || done) {
                    if (blocked) {
                        SPDLOG_DEBUG_MODULE(LOG_GC, "Blocked {}", state->entry.xid);
                    }
                    continue;
                }

                // prepare to scan the next file
                if (state->entry.begin_path == state->entry.commit_path) {
                    // XXX can this ever be hit?  Would it be an error to not have seen a commit?
                    SPDLOG_WARN("No more files to scan, but didn't see commit");
                    done = true;
                } else {
                    // get the next file in the log rotation
                    state->entry.begin_path = fs::get_next_file(state->entry.begin_path,
                                                                pg_log_mgr::PgLogMgr::LOG_PREFIX_REPL,
                                                                pg_log_mgr::PgLogMgr::LOG_SUFFIX);
                    begin_offset = 0;
                    commit_offset = (state->entry.begin_path == state->entry.commit_path)
                        ? state->entry.commit_offset
                        : -1;
                }
            }

            // XXX do we need to clear the stream reader somehow, freeing its resources?

            // if the XID is fully processed, perform cleanup
            if (done) {
                SPDLOG_DEBUG_MODULE(LOG_GC, "Processing {} -- Waiting for completion", state->entry.xid);

                // wait for the parsers to complete the work for this XID
                state->mutation_count->wait();

                // enter the critical section
                {
                    boost::unique_lock lock(_mutex);

                    // wait for any earlier XIDs to complete so that when we perform the next
                    // operations we are the known lowest XID being processed
                    _wait_for_xid(state->entry.db_id, state->entry.xid, true);

                    // XXX notify the ExtentMapper that the XID has been fully processed
                    //     note: we would only do this if the FDW supports roll-forward of uncommited XIDs
                    // write_cache->set_lookup();

                    // clear the dependencies and commit the entry in the Redis queue
                    _backlog.clear_dep(state->entry.db_id, state->entry.xid);
                    _reader_queue.commit(worker_id);

                    if (!skip_xid) {
                        _gc_queue.push(XidReady(state->entry.db_id, XidReady::XactMsg(state->entry.xid)));
                        skip_xid = false;
                    }
                    SPDLOG_DEBUG_MODULE(LOG_GC, "Processing {} -- Complete", state->entry.xid);

                    // notify the next XID waiting for this critical section
                    if (!_xid_map[state->entry.db_id].empty()) {
                        _xid_map[state->entry.db_id].begin()->second.notify_one();
                    }
                }
            }
        }

        // unregister the thread on shutdown
        coordinator->unregister_thread(daemon_type, worker_id);
    }

    void
    LogParser::Reader::shutdown()
    {
        _shutdown = true;
    }

    void
    LogParser::Reader::_wait_for_xid(uint64_t db_id,
                                     uint64_t xid,
                                     bool do_erase)
    {
        // note: requires that we are already holding the _mutex
        boost::unique_lock lock(_mutex, boost::adopt_lock);

        auto &xm = _xid_map[db_id];
        auto xid_i = xm.find(xid);
        assert(xid_i != xm.end());
        if (xid_i != xm.begin()) {
            xid_i->second.wait(lock); // wait to be signaled when we are the first entry
        }

        if (do_erase) {
            xm.erase(xid_i);
        }

        // release the mutex without unlocking it
        lock.release();
    }

    bool
    LogParser::Reader::_check_aborted_xid(StatePtr state, uint64_t pg_xid)
    {
        if (state->entry.pg_xid == pg_xid) {
            return false;
        }

        if (state->entry.aborted_xids.contains(pg_xid)) {
            return true;
        }

        return false;
    }

    bool
    LogParser::Reader::_check_backlog(StatePtr state,
                                      uint64_t oid,
                                      uint64_t offset)
    {
        // if the mutation involves a table with a schema change in an earlier XID, then we
        // place this XID into the backlog to be picked back up later after the schema
        // change has been applied.
        if (_backlog.check(state->entry.db_id, oid, state->entry.xid)) {
            // halt processing this XID until earlier schema changes have been applied
            state->entry.begin_offset = offset;

            _backlog.push(state->entry.db_id, oid, state->entry.xid, state);
            return true;
        }

        return false;
    }

    bool
    LogParser::Reader::_process_mutation(StatePtr state,
                                         uint64_t rel_id,
                                         PgMsgPtr msg,
                                         uint64_t offset)
    {
        // check if we should skip process this message due to ongoing table sync
        if (_sync_tracker.should_skip(state->entry.db_id, rel_id,
                                      state->entry.pg_xid)) {
            return false;
        }

        // check if we should skip processing this message
        if (state->process_as_stream && _check_aborted_xid(state, state->entry.pg_xid)) {
            return false;
        }

        // check if we need to block for schema changes
        if (_check_backlog(state, rel_id, offset)) {
            return true;
        }

        // otherwise we queue this message for processing
        state->mutation_count->increment();
        auto entry = std::make_shared<ParserEntry>(msg, state->mutation_count,
                                                   state->entry.xid, state->lsn,
                                                   rel_id, state->entry.db_id);
        _parser_queue->push(entry);

        return false;
    }

    const std::vector<char> LogParser::Reader::START_FILTER = {
        pg_msg::MSG_BEGIN,
        pg_msg::MSG_STREAM_START
    };

    const std::vector<char> LogParser::Reader::BEGIN_FILTER = {
        pg_msg::MSG_COMMIT,
        pg_msg::MSG_INSERT,
        pg_msg::MSG_UPDATE,
        pg_msg::MSG_DELETE,
        pg_msg::MSG_TRUNCATE,
        pg_msg::MSG_MESSAGE
    };

    const std::vector<char> LogParser::Reader::STREAM_START_FILTER = {
        pg_msg::MSG_STREAM_STOP,
        pg_msg::MSG_INSERT,
        pg_msg::MSG_UPDATE,
        pg_msg::MSG_DELETE,
        pg_msg::MSG_TRUNCATE,
        pg_msg::MSG_MESSAGE
    };

    const std::vector<char> LogParser::Reader::STREAM_STOP_FILTER = {
        pg_msg::MSG_STREAM_START,
        pg_msg::MSG_STREAM_COMMIT
    };

    void
    LogParser::Parser::run(int thread_id)
    {
        std::string worker_id = fmt::format("{}_{}_{}", THREAD_TYPE, THREAD_PARSER, thread_id);

        auto coordinator = Coordinator::get_instance();
        constexpr auto daemon_type = Coordinator::DaemonType::GC_MGR;

        // register the thread on startup
        coordinator->register_thread(daemon_type, worker_id);

        WriteCacheClient * const write_cache = WriteCacheClient::get_instance();
        while (true) {
            // wait for a work item
            auto entry = _parser_queue->pop();
            if (entry == nullptr) {
                // note: this only happens when the queue is shutdown
                break;
            }

            // process the work item
            PgMsgPtr msg = entry->msg;

            SPDLOG_INFO("Parser got work item for: {}", entry->xid);

            // get the table information for the mutation
            auto table = TableMgr::get_instance()->get_table(entry->db_id, entry->table_id, entry->xid);

            // if has a primary key, perform a lookup in the primary index to determine the affected extent_id
            //     note: this should be safe, even in the face of schema changes, since a primary key
            //           change will result in a full table re-sync, so won't go down this code path
            // if no primary key, then:
            //     if insert, use UNKNOWN_EXTENT and perform an append of the new row in the committer
            //     if remove, must scan the data to find the impacted extent_id in the committer
            switch (msg->msg_type) {
            case PgMsgEnum::INSERT: {
                // extract the primary key from the message
                auto &insert_msg = std::get<PgMsgInsert>(msg->msg);

                // generate an extent tuple from the pg log data
                auto schema = SchemaMgr::get_instance()->get_extent_schema(entry->db_id, entry->table_id,
                                                                           { entry->xid, entry->lsn });
                auto extent = std::make_shared<Extent>(ExtentType(), entry->xid, schema->row_size());
                auto tuple = _pack_extent(extent, insert_msg.new_tuple, schema);

                uint64_t extent_id = constant::UNKNOWN_EXTENT;
                if (table->has_primary()) {
                    // extract the primary key from the tuple
                    auto pkey_tuple = schema->tuple_subset(tuple, table->primary_key());

                    // find the affected extent
                    extent_id = table->primary_lookup(pkey_tuple);
                } else {
                    // note: with no primary key we will always append these rows to the table,
                    //       so nothing to do here?
                }

                // send insert to the write cache
                WriteCacheClient::RowData data;
                data.xid = entry->xid;
                data.xid_seq = entry->lsn;
                data.data = extent->serialize();
                data.op = WriteCacheClient::RowOp::INSERT;

                SPDLOG_DEBUG_MODULE(LOG_GC, "INSERT into db {} @ xid {}:{} on table {}",
                                    entry->db_id, entry->xid, entry->lsn, entry->table_id);

                write_cache->add_rows(entry->db_id, table->id(), extent_id, { data });
                if (extent_id != constant::UNKNOWN_EXTENT) {
                    write_cache->set_lookup(entry->db_id, table->id(), entry->xid, extent_id);
                }
                break;
            }
            case PgMsgEnum::DELETE: {
                // extract the primary key from the message
                auto &delete_msg = std::get<PgMsgDelete>(msg->msg);

                // tuple type should be 'K' for primary key or 'O' for non-primary key
                assert(delete_msg.type == 'K' || delete_msg.type == 'O');

                // generate an extent with a row holding the PG tuple data
                auto schema = SchemaMgr::get_instance()->get_extent_schema(entry->db_id, entry->table_id,
                                                                           { entry->xid, entry->lsn });
                auto pkey_schema = schema;
                if (table->has_primary()) {
                    // get the specific columns of the primary key, if available
                    pkey_schema = schema->create_schema(table->primary_key(), {}, table->primary_key());
                }
                auto extent = std::make_shared<Extent>(ExtentType(), entry->xid, pkey_schema->row_size());
                auto &&tuple = _pack_extent(extent, delete_msg.tuple, pkey_schema);

                // find the affected extent
                uint64_t extent_id = constant::UNKNOWN_EXTENT;
                if (table->has_primary()) {
                    extent_id = table->primary_lookup(tuple);
                }

                // send the delete to the write cache
                WriteCacheClient::RowData data;
                data.xid = entry->xid;
                data.xid_seq = entry->lsn;
                data.pkey = extent->serialize();
                data.op = WriteCacheClient::RowOp::DELETE;

                write_cache->add_rows(entry->db_id, table->id(), extent_id, { data });
                if (extent_id != constant::UNKNOWN_EXTENT) {
                    write_cache->set_lookup(entry->db_id, table->id(), entry->xid, extent_id);
                }
                break;
            }
            case PgMsgEnum::UPDATE: {
                auto &update_msg = std::get<PgMsgUpdate>(msg->msg);

                // tuple type should be 'K' for primary key, 'O' for un-keyed tables, or null
                // when no changes to the primary key
                assert(update_msg.old_type == 'K' || update_msg.old_type == 'O' || update_msg.old_type == 0);
                assert(update_msg.new_type == 'N');

                auto schema = SchemaMgr::get_instance()->get_extent_schema(entry->db_id, entry->table_id,
                                                                           { entry->xid, entry->lsn });

                if (update_msg.old_type == 0) {
                    // note: un-keyed tables should never follow this path
                    assert(table->has_primary());

                    auto new_extent = std::make_shared<Extent>(ExtentType(), entry->xid, schema->row_size());
                    auto new_tuple = _pack_extent(new_extent, update_msg.new_tuple, schema);

                    // extract the primary key from the tuple
                    auto pkey_tuple = schema->tuple_subset(new_tuple, table->primary_key());

                    // find the affected extent
                    uint64_t extent_id = table->primary_lookup(pkey_tuple);

                    // send the update to the write cache
                    WriteCacheClient::RowData data;
                    data.xid = entry->xid;
                    data.xid_seq = entry->lsn;
                    data.data = new_extent->serialize();
                    data.op = WriteCacheClient::RowOp::UPDATE;

                    write_cache->add_rows(entry->db_id, table->id(), extent_id, { data });
                    write_cache->set_lookup(entry->db_id, table->id(), entry->xid, extent_id);
                } else {
                    // generate extents for the delete data and insert data
                    auto pkey_schema = schema;
                    if (table->has_primary()) {
                        pkey_schema = schema->create_schema(table->primary_key(), {}, table->primary_key());
                    }

                    auto old_extent = std::make_shared<Extent>(ExtentType(), entry->xid, pkey_schema->row_size());
                    auto old_pkey_tuple = _pack_extent(old_extent, update_msg.old_tuple, pkey_schema);

                    auto new_extent = std::make_shared<Extent>(ExtentType(), entry->xid, schema->row_size());
                    auto new_tuple = _pack_extent(new_extent, update_msg.new_tuple, schema);

                    // extract the primary key from the new data tuple
                    auto new_pkey_tuple = schema->tuple_subset(new_tuple, table->primary_key());

                    // they shouldn't have the same primary key in this case
                    assert(!(old_pkey_tuple == new_pkey_tuple));

                    // look up the extent_id for the old and new tuple
                    uint64_t old_extent_id = constant::UNKNOWN_EXTENT;
                    uint64_t new_extent_id = constant::UNKNOWN_EXTENT;
                    if (table->has_primary()) {
                        old_extent_id = table->primary_lookup(old_pkey_tuple);
                        new_extent_id = table->primary_lookup(new_pkey_tuple);
                    }

                    // send a delete and insert to the write cache
                    WriteCacheClient::RowData delete_data;
                    delete_data.xid = entry->xid;
                    delete_data.xid_seq = entry->lsn;
                    delete_data.pkey = old_extent->serialize();
                    delete_data.op = WriteCacheClient::RowOp::DELETE;

                    WriteCacheClient::RowData insert_data;
                    insert_data.xid = entry->xid;
                    insert_data.xid_seq = entry->lsn;
                    insert_data.data = new_extent->serialize();
                    insert_data.op = WriteCacheClient::RowOp::INSERT;

                    write_cache->add_rows(entry->db_id, table->id(), old_extent_id, { delete_data });
                    write_cache->add_rows(entry->db_id, table->id(), new_extent_id, { insert_data });
                    if (old_extent_id != constant::UNKNOWN_EXTENT) {
                        write_cache->set_lookup(entry->db_id, table->id(), entry->xid, old_extent_id);
                    }
                    if (new_extent_id != constant::UNKNOWN_EXTENT) {
                        write_cache->set_lookup(entry->db_id, table->id(), entry->xid, new_extent_id);
                    }
                }
                break;
            }
            case PgMsgEnum::TRUNCATE: {
                // record the truncate into the write cache look-aside
                WriteCacheClient::TableChange change{ entry->xid, entry->lsn,
                                                      WriteCacheClient::TableOp::TRUNCATE };
                write_cache->add_table_change(entry->db_id, table->id(), change);
                break;
            }
            default:
                SPDLOG_ERROR("Invalid message type: {}", static_cast<int>(msg->msg_type));
                break;
            }

            // decrement the outstanding work counter
            entry->counter->decrement();
        }

        // unregister the thread on shutdown
        coordinator->unregister_thread(daemon_type, worker_id);
    }

    std::shared_ptr<MutableTuple>
    LogParser::Parser::_pack_extent(ExtentPtr extent,
                                    const PgMsgTupleData &data,
                                    ExtentSchemaPtr schema)
    {
        auto fields = schema->get_mutable_fields();

        // generate a tuple from the pg log data
        FieldArrayPtr pg_fields = std::make_shared<FieldArray>();
        for (int i = 0; i < fields->size(); i++) {
            pg_fields->push_back(std::make_shared<PgLogField>(fields->at(i)->get_type(), i));
        }

        // assign the pg data to the extent tuple
        auto tuple = std::make_shared<MutableTuple>(fields, extent->append());
        tuple->assign(FieldTuple(pg_fields, &data));

        return tuple;
    }
}
