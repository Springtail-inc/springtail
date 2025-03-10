#include <bit>
#include <cstdio>
#include <cstring>
#include <cassert>
#include <vector>
#include <algorithm>

#include <absl/log/check.h>
#include <fmt/core.h>

// springtail includes
#include <common/common.hh>
#include <pg_repl/pg_common.hh>
#include <common/redis.hh>
#include <common/redis_types.hh>
#include <common/thread_pool.hh>
#include <common/json.hh>
#include <common/constants.hh>

#include <redis/redis_containers.hh>

#include <pg_repl/exception.hh>
#include <pg_repl/pg_types.hh>
#include <pg_repl/pg_copy_table.hh>
#include <pg_repl/libpq_connection.hh>
#include <pg_repl/pg_repl_msg.hh>

#include <storage/schema.hh>
#include <storage/field.hh>

#include <sys_tbl_mgr/client.hh>
#include <sys_tbl_mgr/system_tables.hh>
#include <sys_tbl_mgr/table.hh>
#include <sys_tbl_mgr/table_mgr.hh>

#include <xid_mgr/xid_mgr_client.hh>

#include <proto/sys_tbl_mgr.pb.h>
#include <proto/pg_copy_table.pb.h>

extern "C" {
    #include <postgres.h>
    #include <catalog/pg_type.h>
}

/* See: https://www.postgresql.org/docs/current/datatype.html for postgres types */

namespace springtail
{
    /** Query all namespaces except pg_catalog and information_schema */
    static constexpr char NAMESPACE_QUERY[] =
        "SELECT oid::integer, nspname::text "
        "FROM pg_catalog.pg_namespace "
        "WHERE nspname NOT LIKE 'pg_%' "
        "{}" // Placeholder for namespace condition
        "AND nspname != 'information_schema';";

    /** Query oid from table and schema */
    static constexpr char TABLE_OID_QUERY[] =
        "SELECT relname::text, nspname::text, pg_class.oid::integer, pg_namespace.oid "
        "FROM pg_catalog.pg_class "
        "JOIN pg_catalog.pg_namespace "
        "ON relnamespace=pg_namespace.oid "
        "WHERE pg_class.relname='{}' and nspname='{}'";

    /** select name, position, is_nullable, default, type, and is primary key for each column */
    static constexpr char SCHEMA_QUERY[] =
        "SELECT column_name, ordinal_position, is_nullable::boolean, "
        "       column_default, atttypid, "
        "       coalesce((pga.attnum=any(pgi.indkey))::boolean, false) as is_pkey, "
        "       array_position(pgi.indkey, pga.attnum) "
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
        "    idx.indexrelid AS index_id, "
        "    i.relname AS index_name, "
        "    a.attname AS column_name, "
        "    s.snum AS secondary_index_num, "
        "    a.attnum AS column_attnum, "
        "    idx.indisunique AS is_unique "
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
    static constexpr char XID_QUERY[] = "SELECT pg_current_xact_id(), pg_current_snapshot()";

    /** query to send replication sync message */
    static constexpr char REPL_MSG_QUERY[] = "SELECT pg_logical_emit_message(true, '{}', '{}')";

    /** copy command, output in binary using utf-8 encoding */
    static constexpr char COPY_QUERY[] = "COPY {}.{} TO STDOUT WITH (FORMAT binary, ENCODING 'UTF-8')";

    /** Get table name, schema name, oid for all tables */
    static constexpr char TABLES_QUERY[] =
        "SELECT relname::text, nspname::text, pg_class.oid::integer, pg_namespace.oid "
        "FROM pg_catalog.pg_class "
        "JOIN pg_catalog.pg_namespace "
        "ON relnamespace=pg_namespace.oid "
        "WHERE relkind = 'r'  "         // regular tables
        "AND nspname NOT LIKE 'pg_%' "  // exclude system schemas
        "AND nspname != 'information_schema' "
        "ORDER BY pg_class.oid";

    static constexpr char INVALID_TABLES_QUERY[] =
        "SELECT "
        "    n.nspname AS schema_name, "
        "    c.relname AS table_name, "
        "    a.attname AS column_name, "
        "    t.typname AS type_name, "
        "    CASE WHEN a.attgenerated = 's' THEN "
        "        pg_get_expr(pg_attrdef.adbin, pg_attrdef.adrelid) "
        "        ELSE NULL "
        "    END AS generation_expression, "
        "    col.collname AS collation, "
        "    bool_or(a.attgenerated = 's') OVER w AS has_generated_column, "
        "    bool_or(t.typnamespace != 'pg_catalog'::regnamespace) OVER w AS has_user_defined_type, "
        "    bool_or(col.collname IS NOT NULL AND col.collname NOT IN ('C', 'en_US.UTF-8', 'default')) OVER w AS has_non_standard_collation "
        "FROM pg_attribute a "
        "JOIN pg_class c ON a.attrelid = c.oid "
        "JOIN pg_namespace n ON c.relnamespace = n.oid "
        "JOIN pg_type t ON a.atttypid = t.oid "
        "LEFT JOIN pg_attrdef ON a.attrelid = pg_attrdef.adrelid AND a.attnum = pg_attrdef.adnum "
        "LEFT JOIN pg_collation col ON a.attcollation = col.oid AND a.attcollation <> 0 "
        "WHERE c.relkind = 'r' "
        "AND a.attnum > 0 "
        "AND n.nspname NOT LIKE 'pg_%' "
        "AND n.nspname != 'information_schema' "
        "AND (a.attgenerated = 's' "
        "    OR (col.collname IS NOT NULL AND col.collname NOT IN ('C', 'en_US.UTF-8', 'default')) "
        "    OR (t.typnamespace != 'pg_catalog'::regnamespace)) " // exclude pg_catalog types and only consider user-defined types
        "WINDOW w AS (PARTITION BY n.nspname, c.relname) "
        "ORDER BY schema_name, table_name, column_name";

    /** Get table name, schema name, oid for all tables in a schema */
    static constexpr char TABLES_SCHEMA_QUERY[] =
        "SELECT relname::text, nspname::text, pg_class.oid::integer, pg_namespace.oid "
        "FROM pg_catalog.pg_class "
        "JOIN pg_catalog.pg_namespace "
        "ON relnamespace=pg_namespace.oid "
        "WHERE relkind = 'r' "          // regular tables
        "AND nspname in ({}) "
        "ORDER BY pg_class.oid";

    /** Get table name, schema name, oid for a single table given oid */
    static constexpr char TABLE_QUERY[] =
        "SELECT relname::text, nspname::text, pg_class.oid::integer, pg_namespace.oid "
        "FROM pg_catalog.pg_class "
        "JOIN pg_catalog.pg_namespace "
        "ON relnamespace=pg_namespace.oid "
        "WHERE relkind = 'r'  "         // regular tables
        "AND nspname NOT LIKE 'pg_%' "  // exclude system schemas
        "AND nspname != 'information_schema' "
        "AND pg_class.oid::integer in ({}) ";

    static constexpr char TABLE_SCHEMA_PAIR_QUERY[] =
        "SELECT "
        "    v.table_name, "
        "    v.schema_name, "
        "    c.oid as table_oid, "
        "    n.oid as schema_oid "
        "FROM (VALUES "
        "    {} " // need to substitute with "('{}', '{}'), ('{}', '{}'), ...
        ") AS v(schema_name, table_name) "
        "JOIN pg_class c ON c.relname = v.table_name "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "    AND n.nspname = v.schema_name "
        "WHERE c.relkind = 'r'";

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


    std::pair<uint64_t, std::string>
    PgCopyTable::_get_xact_xids()
    {
        _connection.exec(XID_QUERY);
        if (_connection.ntuples() == 0) {
            _connection.clear();
            SPDLOG_ERROR("Unexpected results from query: {}", XID_QUERY);
            throw PgQueryError();
        }

        uint64_t pg_xid8 = _connection.get_int64(0, 0);
        _schema.xids = _connection.get_string(0, 1);

        _connection.clear();

        return {pg_xid8, _schema.xids};
    }

    void PgCopyTable::_get_secondary_indexes()
    {
        _connection.exec(fmt::format(SECONDARY_INDEX_QUERY, _schema.table_oid));
        if (_connection.ntuples() == 0) {
            _connection.clear();
            return;  // there are no secondary indexes
        }

        SPDLOG_INFO("Secondary indexes found for table with oid {}", _schema.table_oid);

        // iterate through results and generate vector of secondary indexes
        std::map<std::string, Index> secondary_indexes;
        for (int i = 0; i < _connection.ntuples(); i++) {
            std::uint32_t index_id = _connection.get_int32(i, 0);
            std::string index_name = _connection.get_string(i, 1);
            std::string column_name = _connection.get_string(i, 2);
            std::uint32_t secondary_index_num = _connection.get_int32(i, 3);
            std::uint32_t column_attnum = _connection.get_int32(i, 4);
            bool is_unique = _connection.get_boolean(i, 5);

            Index::Column index_column;
            index_column.idx_position = secondary_index_num;
            index_column.position = column_attnum;

            std::vector<Index::Column> columns;
            // If the existing key is found, use the list of columns from that.
            if ( secondary_indexes.find(index_name) != secondary_indexes.end() ){
                columns = secondary_indexes[index_name].columns;
            }

            columns.push_back(index_column);

            Index index_obj;
            index_obj.id = index_id;
            index_obj.name = index_name;
            index_obj.schema = _schema.schema_name;
            index_obj.table_id = _schema.table_oid;
            index_obj.is_unique = is_unique;
            index_obj.columns = std::move(columns);
            // set the index state to ready since its part of the initial table copy
            index_obj.state = static_cast<uint8_t>(sys_tbl::IndexNames::State::READY);

            secondary_indexes[index_name] = std::move(index_obj);
        }

        for (const auto &index : secondary_indexes) {
            _schema.secondary_keys.push_back(index.second);
            SPDLOG_DEBUG_MODULE(LOG_PG_REPL, "Adding to secondary keys {} {}", index.second.id, index.second.name);
        }

        _connection.clear();
    }


    void PgCopyTable::_set_schema(const std::string &table_name,
                                  const std::string &schema_name,
                                  uint64_t table_oid,
                                  uint64_t schema_oid)
    {
        std::string table_name_ptr = _connection.escape_identifier(table_name);
        std::string schema_name_ptr = _connection.escape_identifier(schema_name);

        _connection.exec(fmt::format(SCHEMA_QUERY, table_oid, schema_name, table_name));

        if (_connection.ntuples() == 0) {
            SPDLOG_ERROR("Table not found: {}.{}", schema_name, table_name);
            _connection.clear();
            throw PgTableNotFoundError();
        }

        if (_connection.nfields() != 7) {
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
            _schema.schema_oid = schema_oid;

            // get columns
            int rows = _connection.ntuples();
            _schema.columns.resize(rows);

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
                column.type = convert_pg_type(column.pg_type);

                // is primary key
                bool is_pkey = _connection.get_boolean(i, 5);

                // set the primary key position if available
                auto pkey_pos = _connection.get_int32_optional(i, 6);
                if (pkey_pos) {
                    CHECK(is_pkey);
                    column.pkey_position = (*pkey_pos);
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
                             uint64_t table_oid,
                             uint64_t schema_oid)
    {
        // set the schema
        _set_schema(table_name, schema_name, table_oid, schema_oid);

        // get secondary indexes XXX not fully supported yet
        _get_secondary_indexes();

        // Create a single protobuf message containing all operations
        proto::CopyTableInfo copy_info;

        // Set namespace request
        auto* ns_req = copy_info.mutable_namespace_req();
        ns_req->set_db_id(db_id);
        ns_req->set_namespace_id(_schema.schema_oid);
        ns_req->set_name(_schema.schema_name);
        ns_req->set_xid(xid.xid);
        ns_req->set_lsn(1);

        // Set table request
        auto* table_req = copy_info.mutable_table_req();
        table_req->set_db_id(db_id);
        table_req->set_xid(xid.xid);
        table_req->set_lsn(1);

        auto* table_info = table_req->mutable_table();
        table_info->set_id(table_oid);
        table_info->set_namespace_name(_schema.schema_name);
        table_info->set_name(_schema.table_name);

        for (const auto &col : _schema.columns) {
            auto* column = table_info->add_columns();
            column->set_name(col.name);
            column->set_type(static_cast<int32_t>(col.type));
            column->set_pg_type(col.pg_type);
            column->set_position(col.position);
            column->set_is_nullable(col.nullable);
            column->set_is_generated(false);
            if (col.pkey_position) {
                column->set_pk_position(*col.pkey_position);
            }
            if (col.default_value) {
                column->set_default_value(*col.default_value);
            }
        }

        // Add index requests
        for (const auto &index : _schema.secondary_keys) {
            auto* index_req = copy_info.add_index_reqs();
            index_req->set_db_id(db_id);
            index_req->set_xid(xid.xid);
            index_req->set_lsn(constant::MAX_LSN-1);

            auto* index_info = index_req->mutable_index();
            index_info->set_id(index.id);
            index_info->set_table_id(_schema.table_oid);
            index_info->set_namespace_name(_schema.schema_name);
            index_info->set_is_unique(index.is_unique);
            index_info->set_name(index.name);

            for (const auto &column : index.columns) {
                auto* index_column = index_info->add_columns();
                index_column->set_idx_position(column.idx_position);
                index_column->set_position(column.position);
            }
        }

        auto schema = std::make_shared<ExtentSchema>(_schema.columns);
        auto table = TableMgr::get_instance()->get_snapshot_table(db_id, _schema.table_oid, xid.xid, schema, _schema.secondary_keys);

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

        // Set roots request
        auto* roots_req = copy_info.mutable_roots_req();
        roots_req->set_db_id(db_id);
        roots_req->set_xid(xid.xid);
        roots_req->set_table_id(table_oid);

        for (auto const& [index, extent]: metadata.roots) {
            auto* root_info = roots_req->add_roots();
            root_info->set_index_id(index);
            root_info->set_extent_id(extent);
        }

        auto* stats = roots_req->mutable_stats();
        stats->set_row_count(metadata.stats.row_count);
        roots_req->set_snapshot_xid(metadata.snapshot_xid);

        // store the serialized protobuf operation into redis for the GC-2
        auto &&key = fmt::format(redis::HASH_SYNC_TABLE_OPS, Properties::get_db_instance_id(), db_id);
        auto redis = RedisMgr::get_instance()->get_client();
        redis->hset(key, fmt::format("{}", table_oid), copy_info.SerializeAsString());
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
            auto type = convert_pg_type(pg_type);

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
                CHECK_EQ(length, 8);
                fields->push_back(std::make_shared<ConstTypeField<int64_t>>(recvint64(row.data() + pos)));
                pos += length;
                break;

            case (SchemaType::INT32):
                CHECK_EQ(length, 4);
                fields->push_back(std::make_shared<ConstTypeField<int32_t>>(recvint32(row.data() + pos)));
                pos += length;
                break;

            case (SchemaType::INT16):
                CHECK_EQ(length, 2);
                fields->push_back(std::make_shared<ConstTypeField<int16_t>>(recvint16(row.data() + pos)));
                pos += length;
                break;

            case (SchemaType::INT8):
                CHECK_EQ(length, 1);
                fields->push_back(std::make_shared<ConstTypeField<int8_t>>(recvint8(row.data() + pos)));
                ++pos;
                break;

            case (SchemaType::BOOLEAN):
                CHECK_EQ(length, 1);
                fields->push_back(std::make_shared<ConstTypeField<bool>>(*(row.data() + pos) == 1));
                ++pos;
                break;

            case (SchemaType::FLOAT64): {
                CHECK_EQ(length, 8);
                auto num = recvint64(row.data() + pos);
                double d = std::bit_cast<double>(num);
                fields->push_back(std::make_shared<ConstTypeField<double>>(d));
                pos += length;
                break;
            }

            case (SchemaType::FLOAT32): {
                CHECK_EQ(length, 4);
                auto num = recvint32(row.data() + pos);
                float f = std::bit_cast<float>(num);
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
        Properties::get_primary_db_config(host, port, user, password);

        // get configuration for the database
        nlohmann::json db_config = Properties::get_db_config(db_id);
        Json::get_to<std::string>(db_config, "name", _db_name);

        // connect to the database
        connect(host, user, password, port);
    }

    void
    PgCopyTable::_populate_invalid_tables()
    {
        // do the tables query
        _connection.exec(INVALID_TABLES_QUERY);

        if (_connection.ntuples() == 0) {
            SPDLOG_ERROR("No invalid tables found in database");
            _connection.clear();
            return;
        }

        nlohmann::json result;

        // iterate through the results and organize by schema and table
        for (int i = 0; i < _connection.ntuples(); i++) {
            std::string schema_name = _connection.get_string(i, 0);
            std::string table_name = _connection.get_string(i, 1);
            std::string column_name = _connection.get_string(i, 2);
            std::string type_name = _connection.get_string(i, 3);
            std::string generation_expression = _connection.get_string_optional(i, 4).value_or("");
            std::string collation = _connection.get_string(i, 5);
            bool has_generated_column = _connection.get_boolean(i, 6);
            bool has_user_defined_type = _connection.get_boolean(i, 7);
            bool has_non_standard_collation = _connection.get_boolean(i, 8);

            // Skip columns that are not UTF-8 encoded or generated columns
            if ( !has_generated_column && !has_user_defined_type && !has_non_standard_collation ) {
                continue;
            }

            // Create schema entry if it doesn't exist
            if (!result.contains(schema_name)) {
                result[schema_name] = nlohmann::json::object();
            }

            // Create table entry if it doesn't exist
            if (!result[schema_name].contains(table_name)) {
                result[schema_name][table_name] = {
                    {"columns", nlohmann::json::array()}
                };
            }

            // Add column information
            result[schema_name][table_name]["columns"].push_back({
                {"name", column_name},
                {"type_name", type_name},
                {"generation_expression", generation_expression},
                {"collation", collation}
            });
        }

        _connection.clear();
        _invalid_tables = result;
    }

    void
    PgCopyTable::_get_table_oids(const std::string &query,
                                 std::set<TableMetadata> &table_oids,
                                 uint64_t db_id)
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
            uint32_t table_oid = _connection.get_int32(i, 2);
            uint32_t schema_oid = _connection.get_int32(i, 3);

            if (_invalid_tables.contains(schema_name) &&
                _invalid_tables[schema_name].contains(table_name)) {
                SPDLOG_DEBUG_MODULE(LOG_PG_REPL, "Skipping table: {}.{}", schema_name, table_name);

                // Create JSON object for the skipped table
                nlohmann::json table_info = {
                    {"schema", schema_name},
                    {"table", table_name},
                    {"columns", _invalid_tables[schema_name][table_name]["columns"]}
                };

                // Store in Redis
                _populate_invalid_tables_in_redis(db_id, table_oid, table_info);

                continue;
            }

            table_oids.insert({schema_name, table_name, schema_oid, table_oid});
        }

        return;
    }

    void
    PgCopyTable::_get_table_oids(const nlohmann::json &include_json,
                                 std::set<TableMetadata> &table_oids,
                                 uint64_t db_id)
    {
        // get schemas array from json into vector of strings

        // check for schemas array
        if (include_json.contains("schemas") && include_json["schemas"].is_array()) {
            std::vector<std::string> schemas = include_json["schemas"];
            if (!schemas.empty()) {
                if (schemas[0] == "*") {
                    // all tables in db
                    _get_table_oids(std::string(TABLES_QUERY), table_oids, db_id);
                    return;
                }

                // construct query by joining schema names
                std::vector<std::string> schema_names;
                for (const auto &schema : schemas) {
                    schema_names.push_back(fmt::format("'{}'", _connection.escape_string(schema)));
                }

                _get_table_oids(fmt::format(TABLES_SCHEMA_QUERY,
                                common::join_string(",", schema_names.begin(), schema_names.end())),
                                table_oids, db_id);
            }
        }

        // go through the tables array (containing schema, table pairs)
        if (include_json.contains("tables") && include_json["tables"].is_array()) {
            std::vector<std::string> pairs;
            for (const auto &table : include_json["tables"]) {
                if (table.contains("schema") && table.contains("table")) {
                    std::string schema = table["schema"].get<std::string>();
                    std::string table_name = table["table"].get<std::string>();
                    pairs.push_back(fmt::format("('{}', '{}')", _connection.escape_string(schema), _connection.escape_string(table_name)));
                }
            }

            if (!pairs.empty()) {
                // issue query by joining all the schema, table pairs
                _get_table_oids(fmt::format(TABLE_SCHEMA_PAIR_QUERY, common::join_string(",", pairs.begin(), pairs.end())), table_oids, db_id);
            }
        }
    }

    void
    PgCopyTable::_end_copy()
    {
        _connection.end_transaction();
    }

    std::vector<PgCopyResultPtr>
    PgCopyTable::copy_tables(uint64_t db_id,
                             uint64_t xid,
                             const std::set<uint32_t> &table_oids)
    {
        return _internal_copy(db_id, xid, std::nullopt, std::nullopt, table_oids);
    }

    std::vector<PgCopyResultPtr>
    PgCopyTable::copy_schema(uint64_t db_id,
                             uint64_t xid,
                             const std::string &schema_name)
    {
        return _internal_copy(db_id, xid, schema_name);
    }

    std::vector<PgCopyResultPtr>
    PgCopyTable::copy_db(uint64_t db_id, uint64_t xid)
    {
        // Copy the entire database but still consider the include json
        auto db_config = Properties::get_db_config(db_id);
        auto include_json = db_config["include"];

        return _internal_copy(db_id, xid, std::nullopt, std::nullopt, std::nullopt, include_json);
    }

    std::vector<PgCopyResultPtr>
    PgCopyTable::copy_table(uint64_t db_id,
                            uint64_t xid,
                            const std::string &schema_name,
                            const std::string &table_name)
    {
        return _internal_copy(db_id, xid, std::nullopt, std::pair{schema_name, table_name});
    }

    std::vector<PgCopyResultPtr>
    PgCopyTable::copy_table(uint64_t db_id, uint64_t xid,
                            const nlohmann::json &include_json)
    {
        return _internal_copy(db_id, xid, std::nullopt, std::nullopt, std::nullopt, include_json);
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
        std::pair<uint64_t, std::string> snapshot_info = copy_table._get_xact_xids();
        result->set_snapshot(snapshot_info.first, snapshot_info.second);

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
                                       request->table_oid,
                                       request->schema_oid);

                // add the table oid to the result
                result->add_table(request->table_oid);

            } catch (PgTableNotFoundError &e) {
                SPDLOG_ERROR("Table not found: {}.{}", request->schema_name, request->table_name);
            } catch (PgQueryError &e) {
                SPDLOG_ERROR("Error copying table: {}.{}", request->schema_name, request->table_name);
                assert(false);
            }
        }

        if (result->tids.size() > 0) {
            copy_table._send_sync_msg(result);
        }

        // end the copy
        copy_table._end_copy();
        copy_table.disconnect();
    }

    void
    PgCopyTable::_send_sync_msg(PgCopyResultPtr result)
    {
        std::string sync_msg = fmt::format(R"({{"target_xid":{}, "pg_xid":{}}})", result->target_xid, result->pg_xid);
        std::string query = fmt::format(REPL_MSG_QUERY, pg_msg::MSG_PREFIX_COPY_SYNC, sync_msg);

        _connection.exec(query);
        if (_connection.status() != PGRES_TUPLES_OK) {
            SPDLOG_ERROR("Error sending sync message");
            _connection.clear();
            throw PgQueryError();
        }

        _connection.clear();
    }

    std::vector<std::pair<uint64_t, std::string>>
    PgCopyTable::_get_namespaces(uint64_t db_id, uint64_t xid)
    {
        auto db_config = Properties::get_db_config(db_id);
        auto include_json = db_config["include"];

        std::vector<std::string> schema_names;

        if (include_json.contains("schemas") && include_json["schemas"].is_array()) {
            for (const auto &schema : include_json["schemas"]) {
                std::string schema_name = schema.get<std::string>();
                if (schema_name == "*") {
                    // all schemas
                    break;
                }

                // Get the list of schema names for the query
                schema_names.push_back(fmt::format("'{}'", _connection.escape_string(schema)));
            }
        }

        if (include_json.contains("tables") && include_json["tables"].is_array()) {
            for (const auto &table : include_json["tables"]) {
                if (table.contains("schema") && table.contains("table")) {
                    std::string schema = table["schema"].get<std::string>();
                    schema_names.push_back(fmt::format("'{}'", _connection.escape_string(schema)));
                }
            }
        }

        std::string schema_condition = "";
        if (!schema_names.empty()) {
            schema_condition = fmt::format("AND nspname IN ({})", common::join_string(",", schema_names.begin(), schema_names.end()));
        }

        // get the namespaces
        _connection.exec(fmt::format(NAMESPACE_QUERY, schema_condition));

        if (_connection.ntuples() == 0) {
            // Technically this should never happen, but keep this here just in case
            _connection.clear();
            SPDLOG_ERROR("Error while getting namespaces");
            return {};
        }

        // iterate through the results and get the namespaces
        std::vector<std::pair<uint64_t, std::string>> namespaces;
        for (int i = 0; i < _connection.ntuples(); i++) {
            uint64_t schema_oid = _connection.get_int64(i, 0);
            std::string namespace_name = _connection.get_string(i, 1);
            namespaces.push_back({schema_oid, namespace_name});
        }

        _connection.clear();
        return namespaces;
    }

    void
    PgCopyTable::create_namespaces(uint64_t db_id, uint64_t xid)
    {
        PgCopyTable copy_table;

        // connect to the database
        copy_table.connect(db_id);

        // get the list of namespaces
        std::vector<std::pair<uint64_t, std::string>> namespaces = copy_table._get_namespaces(db_id, xid);

        // disconnect from the database
        copy_table.disconnect();

        auto client = sys_tbl_mgr::Client::get_instance();
        // create the namespaces
        for (const auto &namespace_info : namespaces) {
            SPDLOG_DEBUG("Creating namespace: {}", namespace_info.second);

            proto::NamespaceRequest ns_req;
            ns_req.set_db_id(db_id);
            ns_req.set_namespace_id(namespace_info.first);
            ns_req.set_name(namespace_info.second);
            ns_req.set_xid(xid);
            ns_req.set_lsn(0);

            // create the namespace
            client->create_namespace(ns_req);
        }
        // flush to disk
        client->finalize(db_id, xid);
    }

    std::vector<PgCopyResultPtr>
    PgCopyTable::_internal_copy(uint64_t db_id,
                                uint64_t target_xid,
                                std::optional<std::string> schema_name,
                                std::optional<std::pair<std::string, std::string>> schema_table,
                                std::optional<std::set<uint32_t>> table_tids,
                                std::optional<nlohmann::json> include_json)
    {
        CopyQueuePtr copy_queue = std::make_shared<CopyQueue>();

        // create copy table object and connect to db
        PgCopyTable copy_table;
        copy_table.connect(db_id);

        // populate the excluded items
        copy_table._populate_invalid_tables();

        // fetch the table oids
        std::set<TableMetadata> table_oids;

        // get the table oids, depends on input
        if (schema_name.has_value()) {
            // by schema name, need to escape the schema name
            // escape the schema name
            std::string schema = "'" + copy_table._connection.escape_string(schema_name.value()) + "'";
            copy_table._get_table_oids(fmt::format(TABLES_SCHEMA_QUERY, schema), table_oids, db_id);
        } else if (table_tids.has_value()) {
            // by table oids
            std::string tids = common::join_string(",", table_tids.value().begin(), table_tids.value().end());
            copy_table._get_table_oids(fmt::format(TABLE_QUERY, tids), table_oids, db_id);
        } else if (schema_table.has_value()) {
            // by schema, table pair
            std::string schema = copy_table._connection.escape_string(schema_table.value().first);
            std::string table = copy_table._connection.escape_string(schema_table.value().second);
            copy_table._get_table_oids(fmt::format(TABLE_OID_QUERY, table, schema), table_oids, db_id);
        } else if (include_json.has_value()) {
            copy_table._get_table_oids(include_json.value(), table_oids, db_id);
        } else {
            // all tables in db
            copy_table._get_table_oids(std::string(TABLES_QUERY), table_oids, db_id);
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
        for (const auto &table_md : table_oids) {
            SPDLOG_DEBUG("Dumping table {}", table_md.table_name);

            // add the table to the copy queue
            copy_queue->push(std::make_shared<CopyRequest>(table_md.table_name,
                                                           table_md.namespace_name,
                                                           table_md.table_oid));
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
