#include <garbage_collector/log_parser.hh>

namespace springtail {

    bool
    GCLogParser::Backlog::check(uint64_t xid,
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
    GCLogParser::Backlog::push(uint64_t xid,
                               uint64_t oid,
                               std::shared_ptr<State> entry)
    {
        boost::scoped_lock lock(_mutex);

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

    GCLogParser::StatePtr
    GCLogParser::Backlog::pop()
    {
        boost::scoped_lock lock(_mutex);

        // if nothing is ready, return nullptr
        if (_ready.empty()) {
            return nullptr;
        }
                
        auto request = _ready.back();
        _ready.pop_back();
        return request;
    }

    void
    GCLogParser::Backlog::add_dep(uint64_t xid,
                                  uint64_t oid)
    {
        boost::scoped_lock lock(_mutex);

        _table_deps[oid].insert(xid);
        _xid_map[xid].insert(oid);
    }

    void
    GCLogParser::Backlog::clear_dep(uint64_t xid)
    {
        boost::scoped_lock lock(_mutex);

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
    GCLogParser::Reader::run()
    {
        // loop until we are asked to shutdown; on shutdown drain the backlog
        while (!_shutdown || !_backlog.empty()) {
            // check the backlog for an XID to process
            _state = _backlog.pop();
            if (_state == nullptr) {
                // update the schema dependencies from Redis
                _backlog.update_deps();

                // if nothing ready in backlog, pull an XID from redis
                auto entry = _pg_queue.pop(_worker_id, timeout=1);
                if (entry == nullptr) {
                    // nothing ready, loop and try again
                    continue;
                }

                _state = std::make_shared<State>(entry);
            }

            // once we have an XID, scan the individual mutations from the log
            uint64_t start_offset = _state.entry->start_offset;
            uint64_t end_offset = (_state.entry->start_path == _state.entry->end_path)
                ? _state.entry->end_offset
                : -1;

            char filter[] = START_FILTER;

            bool done = false;
            bool blocked = false;
            while (!done && !blocked) {
                _reader.set_file(_state.entry->start_path, start_offset, end_offset);

                bool end_of_stream = false;
                while (!end_of_stream && !blocked) {
                    // record the position of the message we are about to read, in case we
                    // need to block and re-process it later
                    uint64_t offset = _reader.offset();

                    // read the next message, if it doesn't pass the filter, skip and continue
                    auto msg = _reader.read_message(filter);
                    ++_state.lsn;

                    // handle message skipping
                    if (msg == nullptr) {
                        end_of_stream = _reader.end_of_stream();
                        continue;
                    }

                    // handle the message
                    switch(msg->msg_type) {
                    case PgMsgEnum::BEGIN: {
                        auto &begin_msg = std::get<PgMsgBegin>(msg->msg);
                        if (begin_msg.xid == _state.entry->pg_xid) {
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
                        if (stream_start_msg.xid == _state.entry->pg_xid) {
                            _state.process_as_stream = true;
                            filter = STREAM_START_FILTER;
                        }
                        break;
                    }

                    case PgMsgEnum::STREAM_STOP: {
                        auto &stream_stop_msg = std::get<PgMsgStreamStop>(msg->msg);
                        filter = STREAM_STOP_FILTER;
                        break;
                    }

                    case PgMsgEnum::STREAM_COMMIT: {
                        auto &stream_commit_msg = std::get<PgMsgStreamCommit>(msg->msg);
                        if (stream_commit_msg.xid == _state.entry->pg_xid) {
                            // stop processing
                            end_of_stream = true;
                            done = true;
                        }
                        break;
                    }

                    case PgMsgEnum::INSERT: {
                        auto &insert_msg = std::get<PgMsgInsert>(msg->msg);

                        blocked = _process_mutation(_state.entry->pg_xid, _state.entry->xid,
                                                    insert_msg.rel_id, msg,
                                                    _state.entry->start_path, offset);
                        break;
                    }
                    case PgMsgEnum::DELETE: {
                        auto &delete_msg = std::get<PgMsgDelete>(msg->msg);

                        blocked = _process_mutation(_state.entry->pg_xid, _state.entry->xid,
                                                    delete_msg.rel_id, msg,
                                                    _state.entry->start_path, offset);
                        break;
                    }
                    case PgMsgEnum::UPDATE: {
                        auto &update_msg = std::get<PgMsgUpdate>(msg->msg);

                        blocked = _process_mutation(_state.entry->pg_xid, _state.entry->xid,
                                                    update_msg.rel_id, msg,
                                                    _state.entry->start_path, offset);
                        break;
                    }
                    case PgMsgEnum::TRUNCATE: {
                        auto &truncate_msg = std::get<PgMsgTruncate>(msg->msg);

                        blocked = _process_mutation(_state.entry->pg_xid, _state.entry->xid,
                                                    truncate_msg.rel_id, msg,
                                                    _state.entry->start_path, offset);
                        break;
                    }

                    case PgMsgEnum::CREATE_TABLE: {
                        auto &table_msg = std::get<PgMsgTable>(msg->msg);

                        // schema changes should be applied in-order, so need to block this
                        // operation if there are earlier un-applied schema changes
                        blocked = _check_backlog(_state.entry->xid, table_msg.oid,
                                                 _state.entry->start_path, offset);
                        if (!blocked) {
                            // apply the schema change
                            _table_manager.create_table(_state.entry->xid, _state.entry->lsn, table_msg);

                            // note: we don't notify the backlog until the entire XID is
                            //       committed since there might be additional schema changes
                        }
                        break;
                    }

                    case PgMsgEnum::ALTER_TABLE: {
                        auto &table_msg = std::get<PgMsgTable>(msg->msg);

                        // schema changes should be applied in-order, so need to block this
                        // operation if there are earlier un-applied schema changes
                        blocked = _check_backlog(_state.entry->xid, table_msg.oid,
                                                 _state.entry->start_path, offset);
                        if (!blocked) {
                            // XXX need to stall the pipeline in some cases, eg type change

                            // apply the schema change
                            _table_manager.alter_table(_state.entry->xid, _state.entry->lsn, table_msg);

                            // note: we don't notify the backlog until the entire XID is
                            //       committed since there might be additional schema changes
                        }
                        break;
                    }

                    case PgMsgEnum::DROP_TABLE: {
                        auto &drop_msg = std::get<PgMsgDropTable>(msg->msg);

                        // schema changes should be applied in-order, so need to block this
                        // operation if there are earlier un-applied schema changes
                        blocked = _check_backlog(_state.entry->xid, table_msg.oid,
                                                 _state.entry->start_path, offset);
                        if (!blocked) {
                            // apply the schema change
                            _table_manager.drop_table(_state.entry->pg_xid, _state.entry->lsn, drop_msg);

                            // also perform a truncation of the table by queueing this message
                            _state.mutation_count->increment();
                            _parser_queue->push({ msg, _state.mutation_count, xid, _state.lsn });

                            // note: we don't notify the backlog until the entire XID is
                            //       committed since there might be additional schema changes
                        }
                        break;
                    }

                    default:
                        // message should have been filtered, error?
                        SPDLOG_ERROR("Received invalid message type: {}", msg->msg_type);
                        break;
                    }
                }

                // if we can stop processing, fast exit
                if (blocked || done) {
                    continue;
                }

                // prepare to scan the next file
                if (_state.entry->start_path == _state.entry->end_path) {
                    // XXX can this ever be hit?  Would it be an error to not have seen a commit?
                    done = true;
                } else {
                    // XXX how to get the next file?
                    _state.entry->start_path = next_file(_state.entry->start_path);
                    start_offset = 0;
                    end_offset = (_state.entry->start_path == _state.entry->end_path)
                        ? _state.entry->end_offset
                        : -1;
                }
            }

            // clear the reader, freeing its resources
            _reader = nullptr;

            // if the XID is fully processed, perform cleanup
            if (done) {
                // wait for the parsers to complete the work for this XID
                _state.mutation_count->wait();

                // clear the dependencies and commit the entry in the Redis queue
                _backlog.clear_dep(_state.entry->xid);
                _pg_queue.commit(_worker_id);
            }
        }
    }

    void
    GCLogParser::Reader::shutdown()
    {
        _shutdown = true;
    }

    bool
    GCLogParser::Reader::_check_xid(uint64_t pg_xid)
    {
        if (_state.entry->pg_xid == pg_xid) {
            return true;
        }

        if (_state.entry->aborted_set.find(pg_xid) != _state.entry->aborted_set.end()) {
            return false;
        }

        return true;
    }

    bool
    GCLogParser::Reader::_check_backlog(uint64_t xid,
                                        uint64_t oid,
                                        const std::filesystem::path &file,
                                        uint64_t offset)
    {
        // if the mutation involves a table with a schema change in an earlier XID, then we
        // place this XID into the backlog to be picked back up later after the schema
        // change has been applied.
        if (_backlog.check(xid, oid)) {
            // halt processing this XID until earlier schema changes have been applied
            _state.entry->begin_path = file;
            _state.entry->start_offset = _reader.offset();

            _backlog.push(_state);
            return true;
        }

        return false;
    }

    bool
    GCLogParser::Reader::_process_mutation(uint64_t pg_xid,
                                           uint64_t xid,
                                           uint64_t rel_id,
                                           PgMsgPtr msg,
                                           const std::filesystem::path &file,
                                           uint64_t offset)
    {
        // check if we should skip processing this message
        if (_state.process_as_stream && !_check_xid(pg_xid)) {
            return false;
        }

        // check if we need to block for schema changes
        if (_check_backlog(xid, rel_id, file, offset)) {
            return true;
        }

        // otherwise we queue this message for processing
        _state.mutation_count->increment();
        _parser_queue->push({ msg, _state.mutation_count, xid, _state.lsn });
    }

    void
    GCLogParser::Parser::run()
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

            // get the table information for the mutation
            auto table = _table_manager.get_table(msg->table_id);

            // if has a primary key, perform a lookup in the primary index to determine the affected extent_id
            if (table->has_primary()) {
                switch (msg->msg_type) {
                case PgMsgEnum::MSG_INSERT: {
                    // extract the primary key from the message
                    auto &insert_msg = std::get<PgMsgInsert>(msg->msg);

                    // generate an extent tuple from the pg log data
                    auto schema = table->schema(entry->xid);
                    auto extent = std::make_shared<Extent>(schema, ExtentType::LEAF, entry->xid);
                    auto &&tuple = _pack_extent(extent, insert_msg.tuple);

                    // extract the primary key from the tuple
                    auto pkey_tuple = schema->tuple_subset(tuple, table->primary_key());

                    // find the affected extent
                    uint64_t extent_id = table->primary_lookup(pkey_tuple);

                    // send insert to the write cache
                    RowData data;
                    data.xid = entry->xid;
                    data.xid_seq = entry->lsn;
                    data.data = extent->serialize();
                    data.op = RowOp::INSERT;

                    _write_cache->add_rows(table->id(), extent_id, RowOp::INSERT, { data });
                    break;
                }
                case PgMsgEnum::MSG_DELETE: {
                    // extract the primary key from the message
                    auto &delete_msg = std::get<PgMsgDelete>(msg->msg);

                    // tuple type should be 'K' for primary key
                    assert(delete_msg.type == 'K');

                    // generate an extent with a row holding the PG tuple data
                    auto schema = table->schema(entry->xid);
                    auto pkey_schema = schema->create_schema(table->primary_key(), {});

                    auto extent = std::make_shared<Extent>(pkey_schema, ExtentType::LEAF, entry->xid);
                    auto &&tuple = _pack_extent(extent, delete_msg.tuple);

                    // find the affected extent
                    uint64_t extent_id = table->primary_lookup(tuple);

                    // send the delete to the write cache
                    RowData data;
                    data.xid = entry->xid;
                    data.xid_seq = entry->lsn;
                    data.data = extent->seralize();
                    data.op = RowOp::DELETE;

                    _write_cache->add_rows(table->id(), extent_id, RowOp::DELETE, { data });
                    break;
                }
                case PgMsgEnum::MSG_UPDATE: {
                    auto &update_msg = std::get<PgMsgUpdate>(msg->msg);
                    std::shared_ptr<MsgTuple> old_pkey_tuple;

                    // tuple type should be 'K' for primary key
                    assert(update_msg.old_type == 'K');

                    // generate extents for the delete data and insert data
                    auto schema = table->schema(entry->xid);
                    auto pkey_schema = schema->create_schema(table->primary_key(), {});

                    auto old_extent = std::make_shared<Extent>(pkey_schema, ExtentType::LEAF, entry->xid);
                    auto &&old_pkey_tuple = _pack_extent(old_extent, update_msg.old_tuple);

                    auto new_extent = std::make_shared<Extent>(schema, ExtentType::LEAF, entry->xid);
                    auto &&new_tuple = _pack_extent(new_extent, update_msg.new_tuple);

                    // extract the primary key from the new data tuple
                    auto new_pkey_tuple = schema->tuple_subset(new_tuple, table->primary_key());

                    // look up the extent_id for the old tuple
                    uint64_t old_extent_id = table->primary_lookup(old_pkey_tuple);

                    // check if they have the same primary key
                    if (old_pkey_tuple == new_pkey_tuple) {
                        // send the update to the write cache
                        RowData data;
                        data.xid = entry->xid;
                        data.xid_seq = entry->lsn;
                        data.pkey = old_extent->serialize();
                        data.data = new_extent->serialize();
                        data.op = RowOp::UPDATE;
                                
                        _write_cache->add_rows(table->id(), old_extent_id, RowOp::UPDATE, { data });
                    } else {
                        // lookup the extent for the new key
                        uint64_t new_extent_id = table->primary_lookup(new_pkey_tuple);

                        // send a delete and insert to the write cache
                        RowData delete_data;
                        delete_data.xid = entry->xid;
                        delete_data.xid_seq = entry->lsn;
                        delete_data.pkey = old_extent->seralize();
                        data.op = RowOp::DELETE;

                        RowData insert_data;
                        insert_data.xid = entry->xid;
                        insert_data.xid_seq = entry->lsn;
                        insert_data.data = new_extent->seralize();
                        data.op = RowOp::INSERT;

                        _write_cache->add_rows(table->id(), extent_id, RowOp::DELETE, { delete_data });
                        _write_cache->add_rows(table->id(), extent_id, RowOp::INSERT, { insert_data });
                    }
                    break;
                }
                case PgMsgEnum::MSG_TRUNCATE: {
                    // record the truncate into the write cache look-aside
                    _write_cache->add_table_change(table->id(), { entry->xid, entry->lsn, TableOp::TRUNCATE });
                    break;
                }
                default:
                    SPDLOG_ERROR("Invalid message type: {}", msg->msg_type);
                    break;
                }
            } else {
                // if no primary key, then:
                //    if insert, return the last extent_id in the primary index
                //    if remove, must scan the data to find the impacted extent_id
                // note: it might make sense to special-case tables without primary keys?  No need to do
                //       the lookup until we are making the actual mutation, would be better for a
                //       reader to scan the mutations to see if they impact the query

                if (msg->msg_type == MSG_INSERT) {
                    uint64_t extent_id = UNKNOWN_EXTENT; // XXX

                    // generate an extent with a row holding the PG tuple data
                    auto schema = table->schema(entry->xid);
                    auto extent = std::make_shared<Extent>(schema, ExtentType::LEAF, entry->xid);
                    auto &&tuple = _pack_extent(extent, insert_msg.tuple);

                    // send the insert to the write cache
                    RowData data;
                    data.xid = entry->xid;
                    data.xid_seq = entry->lsn;
                    data.data = extent->serialize();
                    data.op = RowOp::INSERT;

                    _write_cache->add_rows(table->id(), extent_id, RowOp::INSERT, { insert_data });

                } else if (msg->msg_type == MSG_DELETE) {
                    uint64_t extent_id = UNKNOWN_EXTENT; // XXX

                    // generate an extent with a row holding the PG tuple data
                    auto schema = table->schema(entry->xid);
                    auto extent = std::make_shared<Extent>(schema);

                    // populate the row from the pg tuple data
                    auto fields = schema->get_mutable_fields();
                    auto pg_fields = _gen_pg_fields(fields);

                    MutableTuple tuple(fields, extent->append());
                    tuple.assign(FieldTuple(pg_fields, delete_msg.tuple));

                    RowData delete_data;
                    delete_data.xid = entry->xid;
                    delete_data.xid_seq = entry->lsn;
                    delete_data.pkey = extent->seralize();
                    delete_data.op = RowOp::DELETE;

                    // send the delete to the write cache
                    _write_cache->add_rows(table->id(), extent_id, RowOp::DELETE, { delete_data });

                } else if (msg->msg_type == MSG_UPDATE) {
                    // generate an extent with a row holding the PG tuple data for the delete and the insert
                    auto schema = table->schema(entry->xid);

                    auto fields = schema->get_mutable_fields();
                    auto pg_fields = _gen_pg_fields(fields);

                    auto old_extent = std::make_shared<Extent>(schema);
                    MutableTuple old_tuple(fields, extent->append());
                    old_tuple.assign(FieldTuple(pg_fields, update_msg.old_tuple));

                    auto new_extent = std::make_shared<Extent>(schema);
                    MutableTuple new_tuple(fields, extent->append());
                    new_tuple.assign(FieldTuple(pg_fields, update_msg.new_tuple));

                    RowData delete_data;
                    delete_data.xid = entry->xid;
                    delete_data.xid_seq = entry->lsn;
                    delete_data.pkey = old_extent->seralize();
                    delete_data.op = RowOp::DELETE;

                    RowData insert_data;
                    insert_data.xid = entry->xid;
                    insert_data.xid_seq = entry->lsn;
                    insert_data.data = new_extent->seralize();
                    insert_data.op = RowOp::INSERT;

                    // do an insert and a delete
                    _write_cache->add_rows(table->id(), extent_id, RowOp::DELETE, { delete_data });
                    _write_cache->add_rows(table->id(), extent_id, RowOp::INSERT, { insert_data });

                } else if (msg->msg_type == MSG_TRUNCATE) {
                    // record the truncate into the write cache look-aside
                    _write_cache->add_table_change(table->id(), { xid, lsn, TableOp::TRUNCATE });
                } else {
                    // XXX invalid message type
                    SPDLOG_ERROR("Invalid message type: {}", msg->msg_type);
                }
            }

            // decrement the outstanding work counter
            entry->counter->decrement();
        }
    }

    MutableTuple
    GCLogParser::Parser::_pack_extent(ExtentPtr extent,
                                      const PgMsgTupleData &data)
    {
        auto fields = extent->schema->get_mutable_fields();

        // generate a tuple from the pg log data
        FieldArrayPtr pg_fields = std::make_shared<FieldArray>();
        for (int i = 0; i < fields->size(); i++) {
            pg_fields->push_back(std::make_shared<PgLogField>(fields->at(i)->get_type(), i));
        }

        // assign the pg data to the extent tuple
        MutableTuple tuple(fields, extent->append());
        tuple.assign(FieldTuple(pg_fields, &data));

        return tuple;
    }
}
