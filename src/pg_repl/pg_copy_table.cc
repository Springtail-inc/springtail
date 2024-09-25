#include <cstdio>
#include <cstring>
#include <cassert>
#include <vector>
#include <algorithm>

#include <fmt/core.h>

// springtail includes
#include <common/common.hh>
#include <common/redis.hh>
#include <common/redis_types.hh>
#include <common/thread_pool.hh>

#include <pg_repl/exception.hh>
#include <pg_repl/pg_types.hh>
#include <pg_repl/pg_copy_table.hh>
#include <pg_repl/libpq_connection.hh>
#include <pg_repl/pg_repl_msg.hh>

#include <storage/system_tables.hh>
#include <storage/schema.hh>
#include <storage/table.hh>
#include <storage/field.hh>
#include <storage/table_mgr.hh>

#include <sys_tbl_mgr/client.hh>

#include <xid_mgr/xid_mgr_client.hh>

extern "C" {
    #include <postgres.h>
    #include <catalog/pg_type.h>
}

/* See: https://www.postgresql.org/docs/current/datatype.html for postgres types */

namespace springtail
{
    /** Query oid from table and schema */
    static constexpr char TABLE_OID_QUERY[] =
        "SELECT relname::text, nspname::text, pg_class.oid::integer "
        "FROM pg_catalog.pg_class "
        "JOIN pg_catalog.pg_namespace "
        "ON relnamespace=pg_namespace.oid "
        "WHERE pg_class.relname='{}' and nspname='{}'";

    /** select name, position, is_nullable, default, type, and is primary key for each column */
    static constexpr char SCHEMA_QUERY[] =
        "SELECT column_name, ordinal_position, is_nullable::boolean, "
        "       column_default, atttypid, "
        "       coalesce((pga.attnum=any(pgi.indkey))::boolean, false) as is_pkey "
        "FROM pg_catalog.pg_attribute pga "
        "JOIN information_schema.columns "
        "ON column_name=pga.attname "
        "LEFT OUTER JOIN pg_catalog.pg_index pgi "
        "ON pga.attrelid=pgi.indrelid AND pgi.indisprimary "
        "WHERE pga.attrelid={} "
        "AND table_schema='{}' "
        "AND table_name='{}' "
        "ORDER BY ordinal_position";

    static constexpr char SECONDARY_INDEX_QUERY[] =
        "SELECT"
        "    i.relname AS index_name, "
        "    a.attname AS column_name, "
        "    s.snum AS secondary_index_num, "
        "    a.attnum AS column_attnum "
        "FROM pg_index idx "
        "JOIN pg_class i ON i.oid = idx.indexrelid "
        "JOIN pg_attribute a ON a.attrelid = idx.indrelid "
        "JOIN generate_subscripts(idx.indkey, 1) s(snum) ON idx.indkey[s.snum] = a.attnum "
        "LEFT JOIN pg_constraint c ON c.conindid = idx.indexrelid AND c.contype = 'p' "
        "WHERE idx.indrelid = {} "
        "  AND c.conname IS NULL "
        "ORDER BY i.relname, index_name, s.snum ";

    /** select current xmin:xmax:list of xids in progress from DB as start of this transaction;
     *  result: xmin:xmax:xid,xid,... */
    static constexpr char XID_QUERY[] = "SELECT pg_current_snapshot()";

    /** copy command, output in binary using utf-8 encoding */
    static constexpr char COPY_QUERY[] = "COPY {}.{} TO STDOUT WITH (FORMAT binary, ENCODING 'UTF-8')";

    /** Get table name, schema name, oid for all tables */
    static constexpr char TABLES_QUERY[] =
        "SELECT relname::text, nspname::text, pg_class.oid::integer "
        "FROM pg_catalog.pg_class "
        "JOIN pg_catalog.pg_namespace "
        "ON relnamespace=pg_namespace.oid "
        "WHERE relkind = 'r'  "         // regular tables
        "AND nspname NOT LIKE 'pg_%' "  // exclude system schemas
        "AND nspname != 'information_schema' "
        "ORDER BY pg_class.oid";

    /** Get table name, schema name, oid for all tables in a schema */
    static constexpr char TABLES_SCHEMA_QUERY[] =
        "SELECT relname::text, nspname::text, pg_class.oid::integer "
        "FROM pg_catalog.pg_class "
        "JOIN pg_catalog.pg_namespace "
        "ON relnamespace=pg_namespace.oid "
        "WHERE relkind = 'r' "          // regular tables
        "AND nspname NOT LIKE 'pg_%' "  // exclude system schemas
        "AND nspname != 'information_schema' "
        "AND nspname = '{}' "
        "ORDER BY pg_class.oid";

    /** Get table name, schema name, oid for a single table */
    static constexpr char TABLE_QUERY[] =
        "SELECT relname::text, nspname::text, pg_class.oid::integer "
        "FROM pg_catalog.pg_class "
        "JOIN pg_catalog.pg_namespace "
        "ON relnamespace=pg_namespace.oid "
        "WHERE relkind = 'r'  "         // regular tables
        "AND nspname NOT LIKE 'pg_%' "  // exclude system schemas
        "AND nspname != 'information_schema' "
        "AND pg_class.oid::integer in ({}) ";

    /**
     * @brief Connect to database
     *
     * @param hostname DB hostname
     * @param username DB username
     * @param password DB password
     * @param port     DB port
     * @throws PgQueryError
     */
    void PgCopyTable::connect(const std::string &hostname,
                              const std::string &username,
                              const std::string &password,
                              const int port)
    {
        if (_connection.is_connected()) {
            throw PgAlreadyConnectedError();
        }

        _connection.connect(hostname, _db_name, username, password, port, false);

        try {
            _connection.start_transaction();
        } catch (PgQueryError &e) {
            _connection.disconnect();
            SPDLOG_ERROR("Error starting transaction failed");
            throw e;
        }
    }

    /**
     * @brief Disconnect connection, ignore errors
     */
    void PgCopyTable::disconnect()
    {
        _connection.disconnect();
    }


    std::string PgCopyTable::_get_xact_xids()
    {
        _connection.exec(XID_QUERY);
        if (_connection.ntuples() == 0) {
            _connection.clear();
            SPDLOG_ERROR("Unexpected results from query: {}", XID_QUERY);
            throw PgQueryError();
        }

        _schema.xids = _connection.get_string(0, 0);

        _connection.clear();

        return _schema.xids;
    }

    void PgCopyTable::_get_secondary_indexes()
    {
        _connection.exec(fmt::format(SECONDARY_INDEX_QUERY, _schema.table_oid));
        if (_connection.ntuples() == 0) {
            _connection.clear();
            return;  // there are no secondary indexes
        }

        // iterate through results and generate vector of secondary indexes
        std::map<std::string, std::vector<std::string>> secondary_indexes;
        for (int i = 0; i < _connection.ntuples(); i++) {
            std::string index_name = _connection.get_string(i, 0);
            std::string column_name = _connection.get_string(i, 1);

            // add column name to map indexed by index_name
            // columns are ordered in query
            secondary_indexes[index_name].push_back(column_name);
        }

        // go through map and generate secondary keys in PgCopyTable object
        for (auto &index : secondary_indexes) {
            _schema.secondary_keys.push_back(index.second);
        }

        _connection.clear();
    }


    void PgCopyTable::_set_schema(const std::string &table_name,
                                  const std::string &schema_name,
                                  uint64_t table_oid)
    {
        std::string table_name_ptr = _connection.escape_identifier(table_name);
        std::string schema_name_ptr = _connection.escape_identifier(schema_name);

        _connection.exec(fmt::format(SCHEMA_QUERY, table_oid, schema_name, table_name));

        if (_connection.ntuples() == 0) {
            SPDLOG_ERROR("Table not found: {}.{}", schema_name, table_name);
            _connection.clear();
            throw PgTableNotFoundError();
        }

        if (_connection.nfields() != 6) {
            SPDLOG_ERROR("Error: unexpected data from schema query or table not found");
            SPDLOG_ERROR("fields: {}, tuples: {}", _connection.nfields(), _connection.ntuples());
            _connection.clear();
            throw PgQueryError();
        }

        try {
            _schema.db_name = _db_name;
            _schema.table_name = table_name;
            _schema.schema_name = schema_name;
            _schema.table_oid = table_oid;

            // get columns
            int rows = _connection.ntuples();
            _schema.columns.resize(rows);

            uint32_t pkey_pos = 0;
            for (int i = 0; i < rows; i++) {
                // add column to schema
                // PgColumn column;
                SchemaColumn column;

                // column_name string
                column.name = _connection.get_string(i, 0);

                // ordinal position int4
                column.position = _connection.get_int32(i, 1);

                // is_nullable varchar
                column.nullable = _connection.get_boolean(i, 2);

                // column_default varchar
                column.default_value = _connection.get_string_optional(i, 3);

                // atttypid oid
                column.pg_type = _connection.get_int32(i, 4);

                // springtail type
                column.type = pg_msg::convert_pg_type(column.pg_type);

                // is primary key
                bool is_pkey = _connection.get_boolean(i, 5);
                if (is_pkey) {
                    column.pkey_position = pkey_pos++;
                }

                SPDLOG_DEBUG_MODULE(LOG_PG_REPL,
                                    "Column: {} type={} position={} nullable={} default_value={} pkey={}",
                                    column.name, column.pg_type, column.position, column.nullable,
                                    column.default_value.value_or("NULL"), column.pkey_position);

                _schema.columns[i] = std::move(column);
            }
        } catch (...) {
            _connection.clear();

            std::exception_ptr eptr;
            if (eptr) {
                std::rethrow_exception(eptr);
            }
        }

        _connection.clear();

    }

    void
    PgCopyTable::_prepare_copy()
    {
        std::string table_name = _connection.escape_identifier(_schema.table_name);
        std::string schema_name = _connection.escape_identifier(_schema.schema_name);

        _connection.exec(fmt::format(COPY_QUERY, schema_name, table_name));

        if (_connection.status() != PGRES_COPY_OUT) {
            SPDLOG_ERROR("Copy command did not receive PGRES_COPY_OUT");
            _connection.clear();
            throw PgQueryError();
        }

        // some sanity checks
        if (_connection.binary_tuples() != 1) {
            SPDLOG_ERROR("Copy command not outputting binary");
            _connection.clear();
            throw PgQueryError();
        }

        if (static_cast<std::size_t>(_connection.nfields()) != _schema.columns.size()) {
            SPDLOG_ERROR("Mismatch in copy fields");
            _connection.clear();
            throw PgQueryError();
        }

        _connection.clear();
    }

    std::optional<std::string_view>
    PgCopyTable::_get_next_data()
    {
        char *buffer = nullptr;
        while (true) {
            int r = _connection.get_copy_data(false);
            buffer = _connection.get_copy_buffer();
            if (r == -1) {
                // end of copy, get final result
                if (_connection.status() != PGRES_COMMAND_OK) {
                    SPDLOG_ERROR("Finished copy, got not-ok status: {}",
                                 static_cast<int>(_connection.status()));
                    _connection.clear();
                    throw PgQueryError();
                }
                _connection.clear();

                return std::nullopt; // no return value means we are at the end of the COPY
            } else if (r == -2) {
                // an error occured
                SPDLOG_ERROR("Copy command error: {}", _connection.error_message());
                throw PgQueryError();
            } else if (r == 0 || buffer == nullptr) {
                continue;
            }

            return std::string_view(buffer, r);
        }
    }

    void
    PgCopyTable::_release_data()
    {
        // make sure that the connection's copy buffer is cleared
        _connection.free_copy_buffer();
    }

    void
    PgCopyTable::_copy_table(uint64_t db_id,
                             springtail::XidLsn &xid,
                             const std::string &table_name,
                             const std::string &schema_name,
                             uint64_t table_oid)
    {
        // set the schema
        _set_schema(table_name, schema_name, table_oid);

        // get secondary indexes XXX not fully supported yet
        _get_secondary_indexes();

        // store the system table operations in a JSON array
        nlohmann::json ops;

        // generate a TableRequest message
        {
            sys_tbl_mgr::TableRequest request;
            request.db_id = db_id;
            request.xid = xid.xid;
            request.lsn = 1;
            request.table.id = table_oid;
            request.table.schema = _schema.schema_name;
            request.table.name = _schema.table_name;
            for (const auto &col : _schema.columns) {
                sys_tbl_mgr::TableColumn column;
                column.__set_name(col.name);
                column.__set_type(static_cast<int8_t>(col.type));
                column.__set_pg_type(col.pg_type);
                column.__set_position(col.position);
                column.__set_is_nullable(col.nullable);
                column.__set_is_generated(false);
                if (col.pkey_position) {
                    column.__set_pk_position(*col.pkey_position);
                }
                if (col.default_value) {
                    column.__set_default_value(*col.default_value);
                }

                request.table.columns.push_back(column);
            }
            auto &&create_json = common::thrift_to_json<sys_tbl_mgr::TableRequest>(request);
            ops.push_back(create_json);
        }

        auto schema = std::make_shared<ExtentSchema>(_schema.columns);
        auto table = TableMgr::get_instance()->get_snapshot_table(db_id, _schema.table_oid, xid.xid, schema);

        // start the COPY
        _prepare_copy();

        // get a chunk of data
        auto data = _get_next_data();
        if (!data) {
            throw PgIOError();
        }

        // verify the header before processing the rows
        int32_t ext_length = _verify_copy_header(data->substr(0, 19));
        size_t pos = 19 + ext_length;

        // scan the rows and populate the table
        while (true) {
            if (data->size() == pos) {
                // release the row data
                _release_data();

                // try to get more row data
                pos = 0;
                data = _get_next_data();
                if (!data) {
                    break; // finished with the COPY
                }
                continue; // got more data, keep processing
            }

            auto fields = _parse_row(*data, pos);
            if (!fields) {
                break; // saw footer, finished with the COPY
            }

            // construct a tuple from the row
            auto tuple = std::make_shared<FieldTuple>(fields, nullptr);

            // add the row to the table
            table->insert(tuple, xid.xid, constant::UNKNOWN_EXTENT);
        }

        // flush the table data to disk
        auto &&metadata = table->finalize();

        // pack the table metadata operation
        {
            sys_tbl_mgr::UpdateRootsRequest request;
            request.db_id = db_id;
            request.xid = xid.xid;
            request.table_id = table_oid;
            request.roots.insert(request.roots.end(), metadata.roots.begin(), metadata.roots.end());
            request.stats.row_count = metadata.stats.row_count;
            request.snapshot_xid = metadata.snapshot_xid;
            auto &&update_json = common::thrift_to_json<sys_tbl_mgr::UpdateRootsRequest>(request);
            ops.push_back(update_json);
        }

        // store the system table operations into redis for the GC-2
        auto &&key = fmt::format(redis::QUEUE_SYNC_TABLE_OPS, Properties::get_db_instance_id(), db_id);
        RedisQueue<std::string> sync_table_q(key);
        sync_table_q.push(ops.dump());
    }

    int32_t PgCopyTable::_verify_copy_header(const std::string_view &header)
    {
        // verify signature
        int r = std::memcmp(header.data(), COPY_SIGNATURE, 11);
        if (r != 0) {
            SPDLOG_ERROR("Signature doesn't match");
            throw PgUnknownMessageError();
        }

        int32_t flags = recvint32(header.data() + 11);
        SPDLOG_DEBUG_MODULE(LOG_PG_REPL, "header flags: 0x{:X}", flags);
        if ((flags >> 16) & 0x1) {
            // bit 16 tells us if oids are present
            _oid_flag = true;
        }

        // return the length of any header extension
        return recvint32(header.data() + 15);
    }

    FieldArrayPtr
    PgCopyTable::_parse_row(const std::string_view &row, size_t &pos)
    {
        // start with 16 bit integer -- number of fields
        int16_t num_columns = recvint16(row.data() + pos);
        if (num_columns == -1) {
            // this is the footer
            return nullptr;
        }
        pos += 2;

        if (_oid_flag) {
            pos += 4; // skip the OID
        }

        auto fields = std::make_shared<FieldArray>();

        // iterate through columns
        for (int i = 0; i < num_columns; i++) {
            int32_t length = recvint32(row.data() + pos);
            pos += 4;

            // get the underlying springtail type
            int32_t pg_type = _schema.columns[i].pg_type;
            auto type = pg_msg::convert_pg_type(pg_type);

            // check if null
            if (length == -1) {
                fields->push_back(std::make_shared<ConstNullField>(type));
                continue;
            }

            // otherwise store the data accordingly
            switch (type) {
            case (SchemaType::TEXT):
                fields->push_back(std::make_shared<ConstTypeField<std::string>>(std::string(row.data() + pos, length)));
                pos += length;
                break;

            case (SchemaType::INT64):
                assert(length == 8);
                fields->push_back(std::make_shared<ConstTypeField<int64_t>>(recvint64(row.data() + pos)));
                pos += length;
                break;

            case (SchemaType::INT32):
                assert(length == 4);
                fields->push_back(std::make_shared<ConstTypeField<int32_t>>(recvint32(row.data() + pos)));
                pos += length;
                break;

            case (SchemaType::INT16):
                assert(length == 2);
                fields->push_back(std::make_shared<ConstTypeField<int16_t>>(recvint16(row.data() + pos)));
                pos += length;
                break;

            case (SchemaType::INT8):
                assert(length == 1);
                fields->push_back(std::make_shared<ConstTypeField<int8_t>>(recvint8(row.data() + pos)));
                ++pos;
                break;

            case (SchemaType::BOOLEAN):
                assert(length == 1);
                fields->push_back(std::make_shared<ConstTypeField<bool>>(*(row.data() + pos) == 1));
                ++pos;
                break;

            case (SchemaType::FLOAT64): {
                assert(length == 8);
                auto num = recvint64(row.data() + pos);
                double d = *reinterpret_cast<double *>(&num);
                fields->push_back(std::make_shared<ConstTypeField<double>>(d));
                pos += length;
                break;
            }

            case (SchemaType::FLOAT32): {
                assert(length == 4);
                auto num = recvint32(row.data() + pos);
                float f = *reinterpret_cast<float *>(&num);
                fields->push_back(std::make_shared<ConstTypeField<float>>(f));
                pos += length;
                break;
            }

            case (SchemaType::BINARY): {
                std::string_view tmp(row.data() + pos, length);

                SPDLOG_WARN("Converting unsupported type '{}' into BINARY -- {}",
                            pg_type, tmp);
                // XXX print out the binary data here
                std::vector<char> data(tmp.begin(), tmp.end());
                fields->push_back(std::make_shared<ConstTypeField<std::vector<char>>>(data));
                pos += length;
                break;
            }

            default:
                throw TypeError(fmt::format("PG doesn't support type: {}",
                                            static_cast<uint8_t>(type)));
            }
        }

        return fields;
    }

    void
    PgCopyTable::connect(uint64_t db_id)
    {
        std::string host, user, password;
        int port;

        // get configuration for the database
        nlohmann::json db_config = Properties::get_db_config(db_id);
        Json::get_to<std::string>(db_config, "name", _db_name);

        nlohmann::json primary_db = Properties::get_primary_db_config();
        Json::get_to<std::string>(primary_db, "host", host);
        Json::get_to<int>(primary_db, "port", port);
        Json::get_to<std::string>(primary_db, "replication_user", user);
        Json::get_to<std::string>(primary_db, "password", password);

        // connect to the database
        connect(host, user, password, port);
    }

    void
    PgCopyTable::_get_table_oids(const std::string &query,
                                 std::vector<std::tuple<std::string, std::string, int32_t>> &table_oids)
    {
        // do the tables query
        _connection.exec(query);
        if (_connection.ntuples() == 0) {
            SPDLOG_ERROR("No tables found in database");
            _connection.clear();
            return;
        }

        // iterate through the results and get the table oids
        for (int i = 0; i < _connection.ntuples(); i++) {
            // get the table name, schema name, and oid
            std::string table_name = _connection.get_string(i, 0);
            std::string schema_name = _connection.get_string(i, 1);
            int32_t table_oid = _connection.get_int32(i, 2);
            table_oids.push_back({table_name, schema_name, table_oid});
        }

        return;
    }

    void
    PgCopyTable::_end_copy()
    {
        _connection.end_transaction();
    }

    std::vector<PgCopyResultPtr>
    PgCopyTable::copy_tables(uint64_t db_id,
                            uint64_t xid,
                            std::vector<uint32_t> table_oids)
    {
        return _internal_copy(db_id, xid, std::nullopt, std::nullopt, table_oids);
    }

    std::vector<PgCopyResultPtr>
    PgCopyTable::copy_schema(uint64_t db_id,
                             uint64_t xid,
                             const std::string &schema_name)
    {
        return _internal_copy(db_id, xid, schema_name, std::nullopt, std::nullopt);
    }

    std::vector<PgCopyResultPtr>
    PgCopyTable::copy_db(uint64_t db_id, uint64_t xid)
    {
        return _internal_copy(db_id, xid, std::nullopt, std::nullopt, std::nullopt);
    }

    std::vector<PgCopyResultPtr>
    PgCopyTable::copy_table(uint64_t db_id,
                            uint64_t xid,
                            const std::string &schema_name,
                            const std::string &table_name)
    {
        return _internal_copy(db_id, xid, std::nullopt, std::pair{schema_name, table_name}, std::nullopt);
    }

    void
    PgCopyTable::_worker(uint64_t db_id,
                         uint64_t target_xid,
                         CopyQueuePtr copy_queue,
                         PgCopyResultPtr result)
    {
        // create copy table object and connect to db
        PgCopyTable copy_table;
        copy_table.connect(db_id);

        XidLsn xid(target_xid, 0);

        // start transaction and get the xids associated w/snapshot
        std::string snapshot = copy_table._get_xact_xids();
        result->set_snapshot(snapshot);

        // iterate through the copy queue
        while (true) {
            // get the next copy request
            CopyRequestPtr request = copy_queue->pop();
            if (request == nullptr) {
                if (copy_queue->empty() && copy_queue->is_shutdown()) {
                    break;
                }
                continue;
            }

            try {
                // copy the table
                copy_table._copy_table(db_id,
                                       xid,
                                       request->table_name,
                                       request->schema_name,
                                       request->table_oid);

                // add the table oid to the result
                result->add_table(request->table_oid);

            } catch (PgTableNotFoundError &e) {
                SPDLOG_ERROR("Table not found: {}.{}", request->schema_name, request->table_name);
            } catch (PgQueryError &e) {
                SPDLOG_ERROR("Error copying table: {}.{}", request->schema_name, request->table_name);
                assert(false);
            }
        }

        // end the copy
        copy_table._end_copy();
        copy_table.disconnect();
    }

    std::vector<PgCopyResultPtr>
    PgCopyTable::_internal_copy(uint64_t db_id,
                                uint64_t target_xid,
                                std::optional<std::string> schema_name,
                                std::optional<std::pair<std::string, std::string>> schema_table,
                                std::optional<std::vector<uint32_t>> table_tids)
    {
        CopyQueuePtr copy_queue = std::make_shared<CopyQueue>();

        // create copy table object and connect to db
        PgCopyTable copy_table;
        copy_table.connect(db_id);

        // fetch the table oids
        std::vector<std::tuple<std::string, std::string, int32_t>> table_oids;

        // get the table oids, depends on input
        if (schema_name.has_value()) {
            // by schema name, need to escape the schema name
            // escape the schema name
            std::string schema = copy_table._connection.escape_string(schema_name.value());
            copy_table._get_table_oids(fmt::format(TABLES_SCHEMA_QUERY, schema), table_oids);
        } else if (table_tids.has_value()) {
            // by table oids
            std::string tids = common::join_string(",", table_tids.value().begin(), table_tids.value().end());
            copy_table._get_table_oids(fmt::format(TABLE_QUERY, tids), table_oids);
        } else if (schema_table.has_value()) {
            // by schema, table pair
            std::string schema = copy_table._connection.escape_string(schema_table.value().first);
            std::string table = copy_table._connection.escape_string(schema_table.value().second);
            copy_table._get_table_oids(fmt::format(TABLE_OID_QUERY, table, schema), table_oids);
        } else {
            // all tables in db
            copy_table._get_table_oids(TABLES_QUERY, table_oids);
        }

        // close this connection
        copy_table._end_copy();
        copy_table.disconnect();

        // create a worker thread to copy the tables
        std::vector<std::thread> workers;
        std::vector<PgCopyResultPtr> table_results;
        for (int i = 0; i < std::min(static_cast<std::size_t>(WORKER_THREADS), table_oids.size()); i++) {
            PgCopyResultPtr copy_result = std::make_shared<PgCopyResult>(target_xid);
            table_results.push_back(copy_result);
            workers.push_back(std::thread(&PgCopyTable::_worker,
                              &copy_table, db_id, target_xid, copy_queue, copy_result));
        }

        // iterate through the tables and copy them
        for (const auto &table_tuple : table_oids) {
            SPDLOG_DEBUG("Dumping table {}", std::get<0>(table_tuple));

            // add the table to the copy queue
            copy_queue->push(std::make_shared<CopyRequest>(std::get<0>(table_tuple),  // table name
                                                           std::get<1>(table_tuple),  // schema name
                                                           std::get<2>(table_tuple))); // table oid
        }

        // shutdown the copy queue; blocks until queue is empty
        copy_queue->shutdown(true);
        assert (copy_queue->empty());

        // join the worker threads
        for (auto &worker : workers) {
            worker.join();
        }

        // create result object
        return table_results;
    }
} // namespace springtail
