#include <common/filesystem.hh>

#include <garbage_collector/log_parser.hh>

#include <pg_log_mgr/pg_log_mgr.hh>

#include <storage/constants.hh>
#include <storage/table_mgr.hh>

namespace springtail::gc {

    bool
    LogParser::Backlog::empty() const
    {
        boost::shared_lock lock(_mutex);
        return _backlog.empty();
    }

    bool
    LogParser::Backlog::check(uint64_t xid,
                              uint64_t oid)
    {
        boost::shared_lock lock(_mutex);

        // check if there are any dependencies for this OID
        auto &&i = _table_deps.find(oid);
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
    LogParser::Backlog::push(uint64_t xid,
                             uint64_t oid,
                             std::shared_ptr<State> entry)
    {
        boost::unique_lock lock(_mutex);

        // note: There can be a race condition between check() and push(), so we re-perform
        //       the check() here.  If it is no longer blocked, place the request on the
        //       ready queue directly.
        auto &&i = _table_deps.find(oid);
        if (i == _table_deps.end() || *i->second.begin() > xid) {
            _ready.push_front(entry);
            return;
        }

        // now add the depencency to the backlog
        _backlog[xid] = { oid, entry };
        _oid_backlog[oid].insert(xid);
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
    LogParser::Backlog::update_deps()
    {
        boost::unique_lock lock(_mutex);

        // pull the dependencies from redis
        auto &&values = _oid_set.get_by_score(_last_requested_xid + 1);
        if (!values.empty()) {
            // update the dependency mappings
            for (auto const &value : values) {
                _table_deps[value.oid].insert(value.xid);
                _xid_map[value.xid].insert(value.oid);
            }

            // save that we pulled data through the last seen XID
            _last_requested_xid = values.back().xid;
        }
    }

    void
    LogParser::Backlog::clear_dep(uint64_t xid)
    {
        boost::unique_lock lock(_mutex);

        auto &&i = _xid_map.find(xid);
        if (i == _xid_map.end()) {
            return;
        }

        for (uint64_t oid : i->second) {
            // remove the xid from the set of blocking XIDs for the OID
            auto &&j = _table_deps.find(oid);
            j->second.erase(xid);

            // if we removed the last dependency on this OID, release all blocked XIDs
            uint64_t next_dep_xid = 0;
            if (!j->second.empty()) {
                // if there are still dependencies, we can only release XIDs that are less than this one
                next_dep_xid = *j->second.begin();
            }

            // find the set of XIDs waiting on this OID
            auto &&xid_set_i = _oid_backlog.find(oid);

            // note: there might not be any XIDs waiting on this OID, despite the dependency
            if (xid_set_i != _oid_backlog.end()) {
                auto &blocked_xids = xid_set_i->second;

                // release XIDs blocked on this OID
                auto &&xid_i = blocked_xids.begin();
                while (xid_i != blocked_xids.end()) {
                    if (next_dep_xid && *xid_i > next_dep_xid) {
                        break;
                    }

                    // move the request to the ready queue
                    auto &&b = _backlog.find(*xid_i);
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

        // clear this XID from the list of blockers
        _xid_map.erase(i);
    }

    void
    LogParser::Reader::run()
    {
        // loop until we are asked to shutdown; on shutdown drain the backlog
        while (!_shutdown || !_backlog->empty()) {
            // check the backlog for an XID to process
            _state = _backlog->pop();
            if (_state == nullptr) {
                // update the schema dependencies from Redis
                _backlog->update_deps();

                // if nothing ready in backlog, pull an XID from redis
                // note: block for 1 second
                auto entry = _pg_queue.pop(_worker_id, 1);
                if (entry == nullptr) {
                    // nothing ready, loop and try again
                    continue;
                }

                _state = std::make_shared<State>(entry);
            }

            SPDLOG_INFO("Process XID: {}", _state->entry->xid);

            // once we have an XID, scan the individual mutations from the log
            uint64_t begin_offset = _state->entry->begin_offset;
            uint64_t commit_offset = (_state->entry->begin_path == _state->entry->commit_path)
                ? _state->entry->commit_offset
                : -1;

            std::vector<char> filter = START_FILTER;

            bool done = false;
            bool blocked = false;
            while (!done && !blocked) {
                SPDLOG_DEBUG_MODULE(LOG_GC, "Processing {} -- Read file {} {}", _state->entry->xid, _state->entry->begin_path, begin_offset);

                _reader.set_file(_state->entry->begin_path, begin_offset, commit_offset);

                bool end_of_stream = false;
                while (!end_of_stream && !blocked) {
                    // record the position of the message we are about to read, in case we
                    // need to block and re-process it later
                    uint64_t offset = _reader.offset();

                    // read the next message, if it doesn't pass the filter, skip and continue
                    auto msg = _reader.read_message(filter);
                    ++_state->lsn;

                    // handle message skipping
                    if (msg == nullptr) {
                        end_of_stream = _reader.end_of_stream();
                        continue;
                    }

                    SPDLOG_DEBUG_MODULE(LOG_GC, "Processing {} -- Got msg {}", _state->entry->xid, static_cast<uint8_t>(msg->msg_type));

                    // handle the message
                    switch(msg->msg_type) {
                    case PgMsgEnum::BEGIN: {
                        auto &begin_msg = std::get<PgMsgBegin>(msg->msg);
                        if (begin_msg.xid == _state->entry->pg_xid) {
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
                        if (stream_start_msg.xid == _state->entry->pg_xid) {
                            _state->process_as_stream = true;
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
                        if (stream_commit_msg.xid == _state->entry->pg_xid) {
                            // stop processing
                            end_of_stream = true;
                            done = true;
                        }
                        break;
                    }

                    case PgMsgEnum::INSERT: {
                        auto &insert_msg = std::get<PgMsgInsert>(msg->msg);

                        blocked = _process_mutation(_state->entry->pg_xid, _state->entry->xid,
                                                    insert_msg.rel_id, msg,
                                                    _state->entry->begin_path, offset);
                        break;
                    }
                    case PgMsgEnum::DELETE: {
                        auto &delete_msg = std::get<PgMsgDelete>(msg->msg);

                        blocked = _process_mutation(_state->entry->pg_xid, _state->entry->xid,
                                                    delete_msg.rel_id, msg,
                                                    _state->entry->begin_path, offset);
                        break;
                    }
                    case PgMsgEnum::UPDATE: {
                        auto &update_msg = std::get<PgMsgUpdate>(msg->msg);

                        blocked = _process_mutation(_state->entry->pg_xid, _state->entry->xid,
                                                    update_msg.rel_id, msg,
                                                    _state->entry->begin_path, offset);
                        break;
                    }
                    case PgMsgEnum::TRUNCATE: {
                        auto &truncate_msg = std::get<PgMsgTruncate>(msg->msg);

                        // check if we should skip processing this message
                        if (_state->process_as_stream && !_check_xid(_state->entry->pg_xid)) {
                            // skip the entry
                            blocked = false;
                        } else {
                            // check all of the tables referenced by the truncate
                            // note: there may be multiple due to CASCADE
                            for (auto rel_id : truncate_msg.rel_ids) {
                                blocked = _check_backlog(_state->entry->xid, rel_id, _state->entry->begin_path, offset);
                                if (blocked) {
                                    break;
                                }
                            }

                            if (!blocked) {
                                for (auto rel_id : truncate_msg.rel_ids) {
                                    _state->mutation_count->increment();
                                    auto entry = std::make_shared<ParserEntry>(msg, _state->mutation_count, _state->entry->xid, _state->lsn, rel_id);
                                    _parser_queue->push(entry);
                                }
                            }
                        }
                        break;
                    }

                    case PgMsgEnum::CREATE_TABLE: {
                        auto &table_msg = std::get<PgMsgTable>(msg->msg);

                        // schema changes should be applied in-order, so need to block this
                        // operation if there are earlier un-applied schema changes
                        blocked = _check_backlog(_state->entry->xid, table_msg.oid,
                                                 _state->entry->begin_path, offset);
                        if (!blocked) {
                            // apply the schema change
                            TableMgr::get_instance()->create_table(_state->entry->xid, _state->lsn, table_msg);

                            // note: we don't notify the backlog until the entire XID is
                            //       committed since there might be additional schema changes
                        }
                        break;
                    }

                    case PgMsgEnum::ALTER_TABLE: {
                        auto &table_msg = std::get<PgMsgTable>(msg->msg);

                        // schema changes should be applied in-order, so need to block this
                        // operation if there are earlier un-applied schema changes
                        blocked = _check_backlog(_state->entry->xid, table_msg.oid,
                                                 _state->entry->begin_path, offset);
                        if (!blocked) {
                            // XXX need to stall the pipeline in some cases, eg type change

                            // apply the schema change
                            TableMgr::get_instance()->alter_table(_state->entry->xid, _state->lsn, table_msg);

                            // note: we don't notify the backlog until the entire XID is
                            //       committed since there might be additional schema changes
                        }
                        break;
                    }

                    case PgMsgEnum::DROP_TABLE: {
                        auto &drop_msg = std::get<PgMsgDropTable>(msg->msg);

                        // schema changes should be applied in-order, so need to block this
                        // operation if there are earlier un-applied schema changes
                        blocked = _check_backlog(_state->entry->xid, drop_msg.oid,
                                                 _state->entry->begin_path, offset);
                        if (!blocked) {
                            // apply the schema change
                            TableMgr::get_instance()->drop_table(_state->entry->pg_xid, _state->lsn, drop_msg);

                            // XXX also perform a truncation of the table by queueing this message?
                            _state->mutation_count->increment();
                            auto entry = std::make_shared<ParserEntry>(msg, _state->mutation_count, _state->entry->xid, _state->lsn, drop_msg.oid);
                            _parser_queue->push(entry);

                            // note: we don't notify the backlog until the entire XID is
                            //       committed since there might be additional schema changes
                        }
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
                    continue;
                }

                // prepare to scan the next file
                if (_state->entry->begin_path == _state->entry->commit_path) {
                    // XXX can this ever be hit?  Would it be an error to not have seen a commit?
                    SPDLOG_WARN("No more files to scan, but didn't see commit");
                    done = true;
                } else {
                    // XXX how to get the next file?
                    _state->entry->begin_path = fs::get_next_file(_state->entry->begin_path,
                                                                  PgLogMgr::LOG_PREFIX_REPL,
                                                                  PgLogMgr::LOG_SUFFIX);
                    begin_offset = 0;
                    commit_offset = (_state->entry->begin_path == _state->entry->commit_path)
                        ? _state->entry->commit_offset
                        : -1;
                }
            }

            // XXX clear the reader, freeing its resources?
            // _reader = nullptr;

            // if the XID is fully processed, perform cleanup
            if (done) {
                SPDLOG_DEBUG_MODULE(LOG_GC, "Processing {} -- Waiting for completion", _state->entry->xid);

                // wait for the parsers to complete the work for this XID
                _state->mutation_count->wait();

                // XXX notify the ExtentMapper that the XID has been fully processed
                // write_cache->set_lookup();

                // clear the dependencies and commit the entry in the Redis queue
                _backlog->clear_dep(_state->entry->xid);
                _pg_queue.commit(_worker_id);

                _gc_queue.push(XidReady(_state->entry->xid));
                SPDLOG_DEBUG_MODULE(LOG_GC, "Processing {} -- Complete", _state->entry->xid);
            }
        }
    }

    void
    LogParser::Reader::shutdown()
    {
        _shutdown = true;
    }

    bool
    LogParser::Reader::_check_xid(uint64_t pg_xid)
    {
        if (_state->entry->pg_xid == pg_xid) {
            return true;
        }

        if (_state->entry->aborted_xids.find(pg_xid) != _state->entry->aborted_xids.end()) {
            return false;
        }

        return true;
    }

    bool
    LogParser::Reader::_check_backlog(uint64_t xid,
                                      uint64_t oid,
                                      const std::filesystem::path &file,
                                      uint64_t offset)
    {
        // if the mutation involves a table with a schema change in an earlier XID, then we
        // place this XID into the backlog to be picked back up later after the schema
        // change has been applied.
        if (_backlog->check(xid, oid)) {
            // halt processing this XID until earlier schema changes have been applied
            _state->entry->begin_path = file;
            _state->entry->begin_offset = _reader.offset();

            _backlog->push(xid, oid, _state);
            return true;
        }

        return false;
    }

    bool
    LogParser::Reader::_process_mutation(uint64_t pg_xid,
                                         uint64_t xid,
                                         uint64_t rel_id,
                                         PgMsgPtr msg,
                                         const std::filesystem::path &file,
                                         uint64_t offset)
    {
        // check if we should skip processing this message
        if (_state->process_as_stream && !_check_xid(pg_xid)) {
            return false;
        }

        // check if we need to block for schema changes
        if (_check_backlog(xid, rel_id, file, offset)) {
            return true;
        }

        // otherwise we queue this message for processing
        _state->mutation_count->increment();
        auto entry = std::make_shared<ParserEntry>(msg, _state->mutation_count, xid, _state->lsn, rel_id);
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
    LogParser::Parser::run()
    {
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
            auto table = TableMgr::get_instance()->get_table(entry->table_id, entry->xid, entry->lsn);

            // if has a primary key, perform a lookup in the primary index to determine the affected extent_id
            if (table->has_primary()) {
                switch (msg->msg_type) {
                case PgMsgEnum::INSERT: {
                    // extract the primary key from the message
                    auto &insert_msg = std::get<PgMsgInsert>(msg->msg);

                    // generate an extent tuple from the pg log data
                    auto schema = table->extent_schema();
                    auto extent = std::make_shared<Extent>(ExtentType(), entry->xid, schema->row_size());
                    auto tuple = _pack_extent(extent, insert_msg.new_tuple, schema);

                    // extract the primary key from the tuple
                    auto pkey_tuple = schema->tuple_subset(tuple, table->primary_key());

                    // find the affected extent
                    uint64_t extent_id = table->primary_lookup(pkey_tuple);

                    // send insert to the write cache
                    WriteCacheClient::RowData data;
                    data.xid = entry->xid;
                    data.xid_seq = entry->lsn;
                    data.data = extent->serialize();
                    data.op = WriteCacheClient::RowOp::INSERT;

                    _write_cache->add_rows(table->id(), extent_id, { data });
                    break;
                }
                case PgMsgEnum::DELETE: {
                    // extract the primary key from the message
                    auto &delete_msg = std::get<PgMsgDelete>(msg->msg);

                    // tuple type should be 'K' for primary key
                    assert(delete_msg.type == 'K');

                    // generate an extent with a row holding the PG tuple data
                    auto schema = table->extent_schema();
                    auto pkey_schema = schema->create_schema(table->primary_key(), {}, table->primary_key());

                    auto extent = std::make_shared<Extent>(ExtentType(), entry->xid, pkey_schema->row_size());
                    auto &&tuple = _pack_extent(extent, delete_msg.tuple, pkey_schema);

                    // find the affected extent
                    uint64_t extent_id = table->primary_lookup(tuple);

                    // send the delete to the write cache
                    WriteCacheClient::RowData data;
                    data.xid = entry->xid;
                    data.xid_seq = entry->lsn;
                    data.pkey = extent->serialize();
                    data.op = WriteCacheClient::RowOp::DELETE;

                    _write_cache->add_rows(table->id(), extent_id, { data });
                    break;
                }
                case PgMsgEnum::UPDATE: {
                    auto &update_msg = std::get<PgMsgUpdate>(msg->msg);

                    // tuple type should be 'K' for primary key, 'O' for un-keyed tables, or null
                    // when no changes to the primary key
                    assert(update_msg.old_type == 'K' || update_msg.old_type == 'O' || update_msg.old_type == 0);
                    assert(update_msg.new_type == 'N');

                    if (update_msg.old_type == 0) {
                        auto schema = table->extent_schema();

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

                        _write_cache->add_rows(table->id(), extent_id, { data });
                    } else {
                        // generate extents for the delete data and insert data
                        auto schema = table->extent_schema();
                        auto pkey_schema = schema->create_schema(table->primary_key(), {}, table->primary_key());

                        auto old_extent = std::make_shared<Extent>(ExtentType(), entry->xid, pkey_schema->row_size());
                        auto old_pkey_tuple = _pack_extent(old_extent, update_msg.old_tuple, pkey_schema);

                        auto new_extent = std::make_shared<Extent>(ExtentType(), entry->xid, schema->row_size());
                        auto new_tuple = _pack_extent(new_extent, update_msg.new_tuple, schema);

                        // extract the primary key from the new data tuple
                        auto new_pkey_tuple = schema->tuple_subset(new_tuple, table->primary_key());

                        // look up the extent_id for the old tuple
                        uint64_t old_extent_id = table->primary_lookup(old_pkey_tuple);

                        // they shouldn't have the same primary key in this case
                        assert(!(old_pkey_tuple == new_pkey_tuple));

                        // lookup the extent for the new key
                        uint64_t new_extent_id = table->primary_lookup(new_pkey_tuple);

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

                        _write_cache->add_rows(table->id(), old_extent_id, { delete_data });
                        _write_cache->add_rows(table->id(), new_extent_id, { insert_data });
                    }
                    break;
                }
                case PgMsgEnum::TRUNCATE: {
                    // record the truncate into the write cache look-aside
                    WriteCacheClient::TableChange change{ entry->xid, entry->lsn,
                                                          WriteCacheClient::TableOp::TRUNCATE };
                    _write_cache->add_table_change(table->id(), change);
                    break;
                }
                default:
                    SPDLOG_ERROR("Invalid message type: {}", static_cast<int>(msg->msg_type));
                    break;
                }
            } else {
                // if no primary key, then:
                //    if insert, return the last extent_id in the primary index
                //    if remove, must scan the data to find the impacted extent_id
                // note: it might make sense to special-case tables without primary keys?  No need to do
                //       the lookup until we are making the actual mutation, would be better for a
                //       reader to scan the mutations to see if they impact the query
                uint64_t extent_id = constant::UNKNOWN_EXTENT;

                if (msg->msg_type == PgMsgEnum::INSERT) {
                    // get the message details
                    auto &insert_msg = std::get<PgMsgInsert>(msg->msg);

                    // generate an extent with a row holding the PG tuple data
                    auto schema = table->extent_schema();
                    auto extent = std::make_shared<Extent>(ExtentType(), entry->xid, schema->row_size());
                    auto tuple = _pack_extent(extent, insert_msg.new_tuple, schema);

                    // send the insert to the write cache
                    WriteCacheClient::RowData data;
                    data.xid = entry->xid;
                    data.xid_seq = entry->lsn;
                    data.data = extent->serialize();
                    data.op = WriteCacheClient::RowOp::INSERT;

                    _write_cache->add_rows(table->id(), extent_id, { data });

                } else if (msg->msg_type == PgMsgEnum::DELETE) {
                    // get the message details
                    auto &delete_msg = std::get<PgMsgDelete>(msg->msg);

                    // generate an extent with a row holding the PG tuple data
                    auto schema = table->extent_schema();
                    auto extent = std::make_shared<Extent>(ExtentType(), entry->xid, schema->row_size());
                    auto tuple = _pack_extent(extent, delete_msg.tuple, schema);

                    WriteCacheClient::RowData delete_data;
                    delete_data.xid = entry->xid;
                    delete_data.xid_seq = entry->lsn;
                    delete_data.pkey = extent->serialize();
                    delete_data.op = WriteCacheClient::RowOp::DELETE;

                    // send the delete to the write cache
                    _write_cache->add_rows(table->id(), extent_id, { delete_data });

                } else if (msg->msg_type == PgMsgEnum::UPDATE) {
                    // get the message details
                    auto &update_msg = std::get<PgMsgUpdate>(msg->msg);

                    // generate an extent with a row holding the PG tuple data for the delete and the insert
                    auto schema = table->extent_schema();
                    auto old_extent = std::make_shared<Extent>(ExtentType(), entry->xid, schema->row_size());
                    auto old_tuple = _pack_extent(old_extent, update_msg.old_tuple, schema);

                    auto new_extent = std::make_shared<Extent>(ExtentType(), entry->xid, schema->row_size());
                    auto new_tuple = _pack_extent(old_extent, update_msg.new_tuple, schema);

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

                    // do an insert and a delete
                    _write_cache->add_rows(table->id(), extent_id, { delete_data, insert_data });

                } else if (msg->msg_type == PgMsgEnum::TRUNCATE) {
                    // record the truncate into the write cache look-aside
                    WriteCacheClient::TableChange change{ entry->xid, entry->lsn,
                                                          WriteCacheClient::TableOp::TRUNCATE };
                    _write_cache->add_table_change(table->id(), change);
                } else {
                    // XXX invalid message type
                    SPDLOG_ERROR("Invalid message type: {}", static_cast<uint8_t>(msg->msg_type));
                }
            }

            // decrement the outstanding work counter
            entry->counter->decrement();
        }
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
