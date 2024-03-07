#pragma once

#include <common/redis.hh>

namespace springtail {

    /**
     * Class to handle parsing the log and creating entries for the write cache.
     *
     * The logic for the GC log parser is as follows:
     *   1. Receive a message from the PG log manager that an XID is ready to be processed
     *   2. Scan the data in the log for that XID
     *   3. Generate write cache entries from the log entries by looking up the affected extent_id for each entry
     *   4. Send the data to the write cache
     *
     * The work of the GCLogParser is performed in two phases.  In the first phase, a Reader accepts
     * a message about a complete XID from the log manager, which scans the logs for mutations which
     * as passed individually to the second phase.  In the second phase, a Parser accepts mutations
     * from the Reader, performs a lookup for the affected extent IDs, and submits the mutations to
     * the write cache.  Because this work is highly parallelizable, we maintain a pool of Reader
     * and Parser threads to perform the work.  The only blocking operations are schema changes,
     * which can cause XID processing to be blocked until those mutations are found and processed
     * within the log.
     */
    class GCLogParser {
    public:
        GCLogParser(uint32_t reader_count, uint32_t parser_count)
        {

        }

        void run()
        {
            // start the threads
        }

    private:
        /**
         * Maintains two structures:
         * 1) The dependency map of which XIDs contain schema changes for which table OIDs
         * 2) A backlog of XIDs that are blocked on schema changes to a given OID in earlier XIDs
         *
         * Using these structures, we determine when an XID can continue processing and make it
         * available to the reader threads.
         */
        template <class EntryT>
        class Backlog {
        public:
            /**
             * Checks if the given XID should block when mutating the given table OID.
             * @param xid The XID of this request.
             * @param oid The OID of the table this request is accessing.
             * @return true if the request should block, false otherwise.
             */
            bool check(uint64_t xid, uint64_t oid) {
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

            /**
             * Adds the blocked request to the backlog.  Indicates which XID it is, which OID it's
             * blocked on, and the request details.
             * @param xid The XID of this request.
             * @param oid The OID this XID is blocking on.
             * @param entry The request details.
             */
            void push(uint64_t xid, uint64_t oid, std::shared_ptr<EntryT> entry) {
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

            /**
             * Retreives a request that was previously blocked but is no longer blocked.
             * @return The request details, nullptr if there are no ready requests.
             */
            std::shared_ptr<EntryT> pop() {
                boost::scoped_lock lock(_mutex);

                // if nothing is ready, return nullptr
                if (_ready.empty()) {
                    return nullptr;
                }
                
                auto request = _ready.back();
                _ready.pop_back();
                return request;
            }

            /**
             * Adds a dependency to an XID, indicating that it mutates the schema of the provided OID.
             * @param xid The XID on which the OID is dependent.
             * @param oid The OID of the table which is being mutated.
             */
            void add_dep(uint64_t xid, uint64_t oid) {
                boost::scoped_lock lock(_mutex);

                _table_deps[oid].insert(xid);
                _xid_map[xid].insert(oid);
            }

            /**
             * Clears any requests blocked on the provided XID.
             * @param xid The XID that completed, allowing other blocked requests to proceed.
             */
            void clear_dep(uint64_t xid) {
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

        private:
            /** Guard on access to the members. */
            boost::shared_mutex _mutex;

            /** List of requests that are ready to be processed. */
            std::list<std::shared_ptr<EntryT>> _ready;

            /** Map of blocked XIDs -- XID -> <OID, Request> */
            std::map<uint64_t, std::pair<uint64_t, std::shared_ptr<EntryT>>> _backlog;

            /** Map from a given table OID to the set of blocked XIDs. */
            std::map<uint64_t, std::set<uint64_t>> _oid_backlog;

            /** Map of table dependencies -- OID -> <ordered set of XIDs> */
            std::map<uint64_t, std::set<uint64_t>> _table_deps;

            /** Map of XID to the OIDs it modifies for removing dependencies. */
            std::map<uint64_t, std::set<uint64_t>> _xid_map;
        };

        /**
         * Reads XID commit messages and processes them by reading the individual WAL entries from
         * stable storage and handing the individual mutations to the Parser threads for processing.
         * If a mutation is blocked on a schema change from an earlier XID then we place the XID
         * commit onto a backlog until those schema changes have been processed.  For this reason,
         * the Reader threads first attempt to get XID commit messages from the backlog, and then
         * from the RedisQueue.
         */
        class Reader {
        public:
            Reader(const std::string &pg_queue_key,
                   ParserQueuePtr parser_queue)
                : _pg_queue(pg_queue_key),
                  _parser_queue(parser_queue)
            { }

            /**
             * Main loop for the Reader worker threads.
             */
            void
            operator()()
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
                            // read the next message, if it doesn't pass the filter, skip and continue
                            auto msg = reader.read_message(filter);
                            ++_state.lsn;

                            // handle message skipping
                            if (msg == nullptr) {
                                end_of_stream = reader.end_of_stream();
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

                                blocked = _process_mutation(_state.entry->pg_xid, insert_msg.rel_id, msg);
                                break;
                            }
                            case PgMsgEnum::DELETE: {
                                auto &delete_msg = std::get<PgMsgDelete>(msg->msg);

                                blocked = _process_mutation(_state.entry->pg_xid, delete_msg.rel_id, msg);
                                break;
                            }
                            case PgMsgEnum::UPDATE: {
                                auto &update_msg = std::get<PgMsgUpdate>(msg->msg);

                                blocked = _process_mutation(_state.entry->pg_xid, update_msg.rel_id, msg);
                                break;
                            }
                            case PgMsgEnum::TRUNCATE: {
                                auto &truncate_msg = std::get<PgMsgTruncate>(msg->msg);

                                blocked = _process_mutation(_state.entry->pg_xid, truncate_msg.rel_id, msg);
                                break;
                            }

                            case PgMsgEnum::CREATE_TABLE: {
                                auto &table_msg = std::get<PgMsgTable>(msg->msg);

                                // apply the schema change
                                _table_manager.create_table(_state.entry->pg_xid, _state.entry->lsn, table_msg);

                                // note: we don't notify the backlog until the entire XID is
                                //       committed since there might be additional schema changes
                                break;
                            }

                            case PgMsgEnum::ALTER_TABLE: {
                                auto &table_msg = std::get<PgMsgTable>(msg->msg);

                                // apply the schema change
                                _table_manager.alter_table(_state.entry->pg_xid, _state.entry->lsn, table_msg);

                                // note: we don't notify the backlog until the entire XID is
                                //       committed since there might be additional schema changes
                                break;
                            }

                            case PgMsgEnum::DROP_TABLE: {
                                auto &drop_msg = std::get<PgMsgDropTable>(msg->msg);

                                // apply the schema change
                                _table_manager.drop_table(_state.entry->pg_xid, _state.entry->lsn, drop_msg);

                                // note: we don't notify the backlog until the entire XID is
                                //       committed since there might be additional schema changes
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

        private:
            /**
             * Checks if the provided XID should be excluded from processing based on the aborted sub-transactions.
             * @return true if the XID should be processed, false otherwise.
             */
            bool _check_xid(uint64_t pg_xid)
            {
                if (_state.entry->pg_xid == pg_xid) {
                    return true;
                }

                if (_state.entry->aborted_set.find(pg_xid) != _state.entry->aborted_set.end()) {
                    return false;
                }

                return true;
            }

            /**
             * Process a mutation message (insert, update, delete, truncate).  If the mutation
             * processing must be delayed on an earlier schema change, adds the entry to the backlog
             * and returns a flag indicating that the processing should be blocked.
             */
            bool _process_mutation(uint64_t pg_xid, uint64_t rel_id, PgMsgPtr msg)
            {
                // check if we should skip processing this message
                if (_state.process_as_stream && !_check_xid(pg_xid)) {
                    return false;
                }

                // if the mutation involves a table with a schema change in an earlier XID, then we
                // place this XID into the backlog to be picked back up later after the schema
                // change has been applied.
                if (_backlog.check(xid, rel_id)) {
                    // halt processing this XID until earlier schema changes have been applied
                    _state.entry->begin_path = file;
                    _state.entry->start_offset = _reader.offset();

                    _backlog.push(_state);
                    return true;
                }

                // otherwise we queue this message for processing
                _state.mutation_count->increment();
                _parser_queue->push({ msg, _state.mutation_count, xid, _state.lsn });
            }

        private:
            // XXX how to set this?  UUID?
            /** Unique ID of the worker.  Used for two-phase commit within the Redis queue. */
            uint64_t _worker_id;

            /** Queue for XID messages from the PG log manager. */
            RedisQueue<PgRedisXactValue> _pg_queue;

            /** Backlog queue for blocked XIDs. */
            Backlog<State> _backlog;

            /** Queue of mutations for the Parser threads. */
            ParserQueuePtr _parser_queue;

            /** Reader for the PG log files. */
            PgMsgStreamReader _reader;

            /** State machine for processing an XID. */
            std::shared_ptr<State> _state;

        private:
            /** Holds the state for processing an XID. */
            struct State {
                std::shared_ptr<PgRedisXactValue> entry; ///< The XID entry from the PG log manager.
                bool process_as_stream; ///< True if the XID is in STREAM mode.
                CounterPtr mutation_count; ///< The number of mutations outstanding to the parsers.
                uint64_t lsn; ///< Maintains the LSN for each mutation within this XID.

                State(std::shared_ptr<PgRedisXactValue> entry)
                    : entry(entry),
                      process_as_stream(false),
                      mutation_count(std::make_shared<Counter>()),
                      lsn(0)
                { }
            };

            /** The filter to use at the start of processing. */
            static constexpr std::vector<char> START_FILTER = {
                pg_msg::MSG_BEGIN,
                pg_msg::MSG_STREAM_START
            };

            /** The filter to use after seeing a BEGIN message. */
            static constexpr std::vector<char> BEGIN_FILTER = {
                pg_msg::MSG_COMMIT,
                pg_msg::MSG_INSERT,
                pg_msg::MSG_UPDATE,
                pg_msg::MSG_DELETE,
                pg_msg::MSG_TRUNCATE,
                pg_msg::MSG_MESSAGE
            };

            /** The filter to use after seeing a STREAM_START message. */
            static constexpr std::vector<char> STREAM_START_FILTER = {
                pg_msg::MSG_STREAM_STOP,
                pg_msg::MSG_INSERT,
                pg_msg::MSG_UPDATE,
                pg_msg::MSG_DELETE,
                pg_msg::MSG_TRUNCATE,
                pg_msg::MSG_MESSAGE
            };

            /** The filter to use after seeing a STREAM_STOP message. */
            static constexpr std::vector<char> STREAM_STOP_FILTER = {
                pg_msg::MSG_STREAM_START,
                pg_msg::MSG_STREAM_COMMIT
            };
        };

        /**
         * Parses individual messages, identifies which table_id and extent_id they impact, and
         * sends the complete mutation to the write cache.
         */
        class Parser {
        public:
            Parser(ParserQueuePtr parser_queue)
                : _parser_queue(parser_queue),
                  _write_cache(WriteCacheClient::get_instance());
            { }

            void
            operator()()
            {
                while (true) {
                    // wait for a work item
                    auto entry = _parser_queue.pop();
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

                                // if the insert and remove are from the same extent ID, add the update, otherwise split them into remove and insert
                                if (old_extent_id == new_extent_id) {

                                    // send the update to the write cache
                                    RowData data;
                                    data.xid = entry->xid;
                                    data.xid_seq = entry->lsn;
                                    // XXX do we need to keep the new pkey given it's in the data?
                                    // data.pkey = new_pkey_extent->seralize();
                                    data.old_pkey = old_extent->seralize();
                                    data.data = new_extent->serialize();
                                    data.op = RowOp::UPDATE;

                                    _write_cache->add_rows(table->id(), old_extent_id, RowOp::UPDATE, { data });
                                } else {
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

        protected:
            MutableTuple
            _pack_extent(ExtentPtr extent, const PgMsgTupleData &data)
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
            
        private:
            ParserQueuePtr _parser_queue;
            WriteCacheClient * const _write_cache;
        };

    private:
        /** A set of reader threads.  Thread operation defined by Reader. */
        std::vector<std::thread> _readers;

        /** A pool of parser threads that perform lookups for individual mutations. */
        std::vector<std::thread> _parsers;

    protected:
        struct ParserEntry {
            PgMsgPtr msg;
            CounterPtr counter;
            uint64_t xid;
            uint64_t lsn;
        };

        typedef ConcurrentQueue<ParserEntry> ParserQueue;
        typedef std::shared_ptr<ParserQueue> ParserQueuePtr;

        class Counter {
        public:
            void increment() {
                boost::unique_lock lock(_mutex);
                ++_count;
            }

            void decrement() {
                boost::unique_lock lock(_mutex);
                --_count;
                if (_count == 0) {
                    _cv.notify_all();
                }
            }

            void wait() {
                boost::unique_lock lock(_mutex);
                _cv.wait(lock, [this]{ return this->_count == 0; });
            }

        private:
            boost::condition_variable _cv;
            boost::mutex _mutex;
            uint64_t _count;
        };
        typedef std::shared_ptr<Counter> CounterPtr;

    private:
        ParserQueuePtr _parser_queue;
    };

}
