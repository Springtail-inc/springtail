// springtail includes
#include <common/json.hh>

#include <pg_log_mgr/sync_tracker.hh>

#include <pg_repl/exception.hh>

#include <storage/schema.hh>
#include <storage/field.hh>

#include <sys_tbl_mgr/server.hh>
#include <sys_tbl_mgr/system_tables.hh>
#include <sys_tbl_mgr/table_mgr.hh>

#include <pg_ext/extn_registry.hh>

#include <postgresql/server/catalog/pg_type_d.h>

/* See: https://www.postgresql.org/docs/current/datatype.html for postgres types */

namespace springtail
{
    /** Query enum user defined types */
    static constexpr char ENUM_QUERY[] =
        "SELECT t.oid::integer AS enum_type_oid, "
        "       n.oid::integer AS namespace_oid, "
        "       n.nspname::text AS schema, "
        "       t.typname::text AS enum_type_name, "
        "       json_agg(json_build_object(e.enumlabel::text, e.enumsortorder::float) ORDER BY e.enumsortorder)::text AS value "
        "FROM pg_enum e "
        "JOIN pg_type t ON t.oid = e.enumtypid "
        "JOIN pg_namespace n ON n.oid = t.typnamespace "
        "WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', '__pg_springtail_triggers') "
        "{}" // Placeholder for namespace condition
        "GROUP BY t.oid, t.typname, n.nspname, n.oid";

    static constexpr char EXTN_QUERY[] =
        "SELECT t.oid as enum_type_oid, "
        "       e.extname AS extension, "
        "       n.oid::integer AS namespace_oid, "
        "       n.nspname::text AS schema, "
        "       t.typname::text AS enum_type_name "
        "FROM pg_type t "
        "JOIN pg_namespace n ON n.oid = t.typnamespace "
        "JOIN pg_depend d    ON d.objid = t.oid "
        "JOIN pg_extension e ON e.oid = d.refobjid "
        "WHERE t.typtype IN ('b', 'c', 'd', 'e') "
        "AND n.nspname NOT IN ('pg_catalog', 'information_schema') "
        "{}" // Placeholder for namespace condition
        "ORDER BY e.extname, t.typname;";

    /** Enum lookup query */
    static constexpr char ENUM_LOOKUP_QUERY[] =
        "SELECT enumtypid::integer, "
        "       enumsortorder::float, "
        "       enumlabel::text "
        "FROM pg_enum";

    /** Query all namespaces except pg_catalog and information_schema */
    static constexpr char NAMESPACE_QUERY[] =
        "SELECT oid::integer, nspname::text "
        "FROM pg_catalog.pg_namespace "
        "WHERE nspname NOT LIKE 'pg_%' "
        "{}" // Placeholder for namespace condition
        "AND nspname NOT IN ('information_schema', '__pg_springtail_triggers');";

    /** Query oid from table and schema */
    static constexpr char TABLE_OID_QUERY[] =
        "SELECT relname::text, nspname::text, pg_class.oid::integer, pg_namespace.oid "
        "FROM pg_catalog.pg_class "
        "JOIN pg_catalog.pg_namespace "
        "ON relnamespace=pg_namespace.oid "
        "WHERE pg_class.relname='{}' and nspname='{}'";

    /** select name, position, is_nullable, default, type, and is primary key for each column */
    static constexpr char SCHEMA_QUERY[] =
        "SELECT "
        "    pga.attname AS column_name, "
        "    pga.attnum AS ordinal_position, "
        "    NOT pga.attnotnull AS is_nullable, "
        "    pg_get_expr(ad.adbin, ad.adrelid) AS column_default, "
        "    pga.atttypid, "
        "    pga.attgenerated = 's' AS is_generated, "
        "    (t.typnamespace <> 'pg_catalog'::regnamespace "
        "     AND t.typnamespace <> 'information_schema'::regnamespace)::boolean "
        "        AS is_user_defined_type, "
        "    COALESCE((col.collname NOT IN ('C', 'en_US.UTF-8', 'default'))::boolean, false) "
        "        AS is_non_standard_collation, "
        "    COALESCE((pga.attnum = ANY(pgi.indkey))::boolean, false) AS is_pkey, "
        "    array_position(pgi.indkey, pga.attnum) AS pkey_position, "
        "    t.typcategory AS type_category, "
        "    t.typname AS type_name, "
        "    nsp.nspname AS type_namespace "
        "FROM pg_catalog.pg_attribute pga "
        "JOIN pg_class cls "
        "    ON cls.oid = pga.attrelid "
        "JOIN pg_namespace n "
        "    ON n.oid = cls.relnamespace "
        "LEFT JOIN pg_attrdef ad "
        "    ON pga.attrelid = ad.adrelid "
        "   AND pga.attnum = ad.adnum "
        "LEFT JOIN pg_index pgi "
        "    ON pga.attrelid = pgi.indrelid "
        "   AND pgi.indisprimary "
        "LEFT JOIN pg_collation col "
        "    ON pga.attcollation = col.oid "
        "   AND pga.attcollation <> 0 "
        "LEFT JOIN pg_type t "
        "    ON pga.atttypid = t.oid "
        "LEFT JOIN pg_namespace nsp "
        "    ON nsp.oid = t.typnamespace "
        "WHERE pga.attrelid = {} "
        "  AND pga.attisdropped IS FALSE "
        "  AND n.nspname = '{}' "
        "  AND cls.relname = '{}' "
        "  AND pga.attnum > 0 "
        "ORDER BY pga.attnum";

    static constexpr char SECONDARY_INDEX_QUERY[] =
        "SELECT"
        "    idx.indexrelid AS index_id, "
        "    ns.nspname || '.' || i.relname AS index_name, "
        "    a.attname AS column_name, "
        "    s.snum AS secondary_index_num, "
        "    a.attnum AS column_attnum, "
        "    idx.indisunique AS is_unique "
        "FROM pg_index idx "
        "JOIN pg_class i ON i.oid = idx.indexrelid "
        "JOIN pg_namespace ns ON ns.oid = i.relnamespace "
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

    /** copy command with primary key */
    static constexpr char COPY_PKEY_QUERY[] = "COPY (SELECT * FROM {}.{} ORDER BY {}) TO STDOUT WITH (FORMAT binary, ENCODING 'UTF-8')";

    /** Get table name, schema name, oid for all tables */
    static constexpr char TABLES_QUERY[] =
        "SELECT relname::text, nspname::text, pg_class.oid::integer, pg_namespace.oid "
        "FROM pg_catalog.pg_class "
        "JOIN pg_catalog.pg_namespace "
        "ON relnamespace=pg_namespace.oid "
        "WHERE relkind IN ('r','p') "         // regular tables, partitioned tables
        "AND nspname NOT LIKE 'pg_%' "        // exclude system schemas
        "AND nspname NOT IN ('information_schema', '__pg_springtail_triggers') "
        "ORDER BY relkind, pg_class.oid";     // have partitioned tables first

    /** Get table name, schema name, oid for all tables in a schema */
    static constexpr char TABLES_SCHEMA_QUERY[] =
        "SELECT relname::text, nspname::text, pg_class.oid::integer, pg_namespace.oid "
        "FROM pg_catalog.pg_class "
        "JOIN pg_catalog.pg_namespace "
        "ON relnamespace=pg_namespace.oid "
        "WHERE relkind IN ('r','p') "          // regular tables, partitioned tables
        "AND nspname in ({}) "
        "ORDER BY relkind, pg_class.oid";      // have partitioned tables first

    /** Get table name, schema name, oid for a single table given oid */
    static constexpr char TABLE_QUERY[] =
        "SELECT relname::text, nspname::text, pg_class.oid::integer, pg_namespace.oid "
        "FROM pg_catalog.pg_class "
        "JOIN pg_catalog.pg_namespace "
        "ON relnamespace=pg_namespace.oid "
        "WHERE relkind IN ('r','p') "         // regular tables, partitioned tables
        "AND nspname NOT LIKE 'pg_%' "        // exclude system schemas
        "AND nspname NOT IN ('information_schema', '__pg_springtail_triggers') "
        "AND pg_class.oid::integer in ({}) "
        "ORDER BY relkind, pg_class.oid";     // have partitioned tables first

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
        "WHERE c.relkind IN ('r','p')";

    static constexpr char TABLE_PARTITION_QUERY[] =
        "SELECT "
        "    CASE WHEN c.relispartition THEN "
        "        (SELECT inhparent FROM pg_inherits WHERE inhrelid = c.oid) "
        "    END as parent_oid, "
        "    pg_get_expr(c.relpartbound, c.oid, TRUE) as partition_bound, "
        "    pg_get_partkeydef(c.oid) as partition_key "
        "FROM pg_class c "
        "WHERE c.oid = {}";

    /** Query to info about the table being synced */
    static constexpr char TABLE_INFO_QUERY[] =
        "SELECT relname::text as table_name, nspname as schema_name, "
        "       c.oid::integer as table_oid, ns.oid as namespace_oid, "
        "       c.relrowsecurity, c.relforcerowsecurity "
        "FROM pg_catalog.pg_class c "
        "JOIN pg_catalog.pg_namespace ns "
        "ON relnamespace=ns.oid "
        "WHERE c.oid = {};";

    /** Query a table's namespace and name given its OID */
    static constexpr char TABLE_NAMESPACE_NAME_QUERY[] =
        "SELECT n.nspname AS schema_name, c.relname AS table_name FROM pg_class c JOIN "
        "pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = {}";

    /** Lock a table in ACCESS SHARE mode */
    static constexpr char TABLE_LOCK_QUERY[] = "LOCK TABLE {}.{} IN ACCESS SHARE MODE";

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
            LOG_ERROR("Error starting transaction failed");
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
            LOG_ERROR("Unexpected results from query: {}", XID_QUERY);
            throw PgQueryError();
        }

        uint64_t pg_xid8 = _connection.get_int64(0, 0);
        _schema.xids = _connection.get_string(0, 1);

        _connection.clear();

        return {pg_xid8, _schema.xids};
    }

    bool PgCopyTable::_is_table_dropped(uint64_t schema_oid, uint64_t table_oid) {
        _connection.exec(
                fmt::format("SELECT 1 FROM pg_class WHERE oid = {} AND relnamespace = {}", table_oid, schema_oid));
        return (_connection.ntuples() == 0);
    }

    void PgCopyTable::_get_secondary_indexes()
    {
        _connection.exec(fmt::format(SECONDARY_INDEX_QUERY, _schema.table_oid));
        if (_connection.ntuples() == 0) {
            _connection.clear();
            return;  // there are no secondary indexes
        }

        LOG_INFO("Secondary indexes found for table with oid {}", _schema.table_oid);

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
            LOG_DEBUG(LOG_PG_REPL, LOG_LEVEL_DEBUG1, "Adding to secondary keys {} {}", index.second.id, index.second.name);
        }

        _connection.clear();
    }


    void PgCopyTable::_set_schema(uint32_t table_oid)
    {
        // refetch schema info as it may have changed
        _connection.exec(fmt::format(TABLE_INFO_QUERY, table_oid));
        if (_connection.ntuples() == 0) {
            LOG_ERROR("Table not found with oid: {}", table_oid);
            _connection.clear();
            throw PgTableNotFoundError();
        }

        if (_connection.nfields() != 6) {
            LOG_ERROR("Error: unexpected data from table info query or table not found");
            LOG_ERROR("fields: {}, tuples: {}", _connection.nfields(), _connection.ntuples());
            _connection.clear();
            throw PgQueryError();
        }

        // get table info
        std::string table_name = _connection.get_string(0, 0);
        std::string schema_name = _connection.get_string(0, 1);
        uint32_t schema_oid = _connection.get_int32(0, 3);
        bool relrowsecurity = _connection.get_boolean(0, 4);
        bool relforcerowsecurity = _connection.get_boolean(0, 5);

        std::string table_name_ptr = _connection.escape_string(table_name);
        std::string schema_name_ptr = _connection.escape_string(schema_name);

        _connection.clear();

        // get any table partition info
        _connection.exec(fmt::format(TABLE_PARTITION_QUERY, table_oid));
        if (_connection.ntuples() > 0) {
            DCHECK(_connection.nfields() == 3);
            _schema.parent_oid = _connection.get_int32_optional(0, 0);
            _schema.partition_bound = _connection.get_string_optional(0, 1);
            _schema.partition_key = _connection.get_string_optional(0, 2);
        }
        _connection.clear();

        // get the table schema (columns)
        _connection.exec(fmt::format(SCHEMA_QUERY, table_oid, schema_name_ptr, table_name_ptr));
        if (_connection.ntuples() == 0) {
            LOG_ERROR("Table not found: {}.{}", schema_name, table_name);
            _connection.clear();
            throw PgTableNotFoundError();
        }

        if (_connection.nfields() != 13) {
            LOG_ERROR("Error: unexpected data from schema query or table not found");
            LOG_ERROR("fields: {}, tuples: {}", _connection.nfields(), _connection.ntuples());
            _connection.clear();
            throw PgQueryError();
        }

        try {
            _schema.db_name = _db_name;
            _schema.table_name = table_name;
            _schema.schema_name = schema_name;
            _schema.table_oid = table_oid;
            _schema.schema_oid = schema_oid;
            _schema.rls_enabled = relrowsecurity;
            _schema.rls_forced = relforcerowsecurity;

            // get columns
            int rows = _connection.ntuples();
            _schema.columns.resize(rows);

            std::set<std::pair<int, std::string>> pkeys;
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
                // SPR-774
                if (column.pg_type == BITOID) {
                    column.pg_type = VARBITOID;
                }

                // is primary key
                column.is_generated = _connection.get_boolean(i, 5);

                // is user defined type
                column.is_user_defined_type = _connection.get_boolean(i, 6);

                // is non standard collation
                column.is_non_standard_collation = _connection.get_boolean(i, 7);

                // is primary key
                bool is_pkey = _connection.get_boolean(i, 8);

                // set the primary key position if available
                auto pkey_pos = _connection.get_int32_optional(i, 9);
                if (pkey_pos) {
                    CHECK(is_pkey);
                    column.pkey_position = (*pkey_pos);
                    pkeys.insert({*pkey_pos, column.name});
                }

                // type category of 'E' represents enum
                column.type_category = _connection.get_char(i, 10);

                // column type name
                column.type_name = _connection.get_string(i, 11);

                // column type namespace
                column.type_namespace = _connection.get_string(i, 12);

                // springtail type
                column.type = convert_pg_type(column.pg_type, column.type_category);

                LOG_DEBUG(LOG_PG_REPL, LOG_LEVEL_DEBUG1,
                                    "Column: {} type={} position={} nullable={} default_value={} pkey={} is_generated={} is_non_standard_collation={} is_user_defined_type={}",
                                    column.name, column.pg_type, column.position, column.nullable,
                                    column.default_value.value_or("NULL"), column.pkey_position, column.is_generated, column.is_non_standard_collation, column.is_user_defined_type);

                _schema.columns[i] = std::move(column);
            }

            // create the set of pkeys in order
            for (const auto &pkey : pkeys) {
                _schema.pkeys.push_back(pkey.second);
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

        if (_schema.pkeys.size() > 0) {
            // if we have primary keys, use them to order the copy
            std::string pkey_order;
            for (const auto &pkey : _schema.pkeys) {
                if (!pkey_order.empty()) {
                    pkey_order += ", ";
                }
                pkey_order += fmt::format("{}", _connection.escape_identifier(pkey));
            }
            LOG_DEBUG(LOG_PG_LOG_MGR, LOG_LEVEL_DEBUG1, "Copying table {}.{} with primary keys: {}", schema_name, table_name, pkey_order);
            _connection.exec(fmt::format(COPY_PKEY_QUERY, schema_name, table_name, pkey_order));
        } else {
            // no primary keys, just copy the table in storage order
            LOG_DEBUG(LOG_PG_LOG_MGR, LOG_LEVEL_DEBUG1, "Copying table {}.{} without primary keys", schema_name, table_name);
            _connection.exec(fmt::format(COPY_QUERY, schema_name, table_name));
        }

        if (_connection.status() != PGRES_COPY_OUT) {
            LOG_ERROR("Copy command did not receive PGRES_COPY_OUT");
            _connection.clear();
            throw PgQueryError();
        }

        // some sanity checks
        if (_connection.binary_tuples() != 1) {
            LOG_ERROR("Copy command not outputting binary");
            _connection.clear();
            throw PgQueryError();
        }

        if (static_cast<std::size_t>(_connection.nfields()) != _schema.columns.size()) {
            LOG_ERROR("Mismatch in copy fields: {} != {}",
                      _connection.nfields(), _schema.columns.size());
            for (auto &column : _schema.columns) {
                LOG_ERROR("\t{}", column.name);
            }
            _connection.clear();
            throw PgRetryError();
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
                    LOG_ERROR("Finished copy, got not-ok status: {}",
                                 static_cast<int>(_connection.status()));
                    _connection.clear();
                    throw PgQueryError();
                }
                _connection.clear();

                return std::nullopt; // no return value means we are at the end of the COPY
            } else if (r == -2) {
                // an error occured
                LOG_ERROR("Copy command error: {}", _connection.error_message());
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

    PgCopyResult::TableInfoPtr
    PgCopyTable::_copy_table(uint64_t db_id,
                             const springtail::XidLsn &xid,
                             const std::map<int32_t, std::map<std::string, float>> &user_types,
                             uint32_t table_oid,
                             const PgCopyResultPtr &snapshot_details)
    {
        // set the schema
        _set_schema(table_oid);

        LOG_INFO("Copying table {}.{} with oid {} and schema oid {} at xid {}",
                 _schema.schema_name, _schema.table_name, table_oid, _schema.schema_oid, xid.xid);

        // validate the columns to see if there are invalid columns
        auto invalid_columns = TableValidator::get_instance()->validate_columns<SchemaColumn>(_schema.columns,
                Properties::get_include_schemas(db_id));
        if (invalid_columns.size() > 0) {
            LOG_DEBUG(LOG_PG_REPL, LOG_LEVEL_DEBUG1, "Invalid columns found as part of _copy_table for table_oid {}", table_oid);
            nlohmann::json table_info = {
                {"schema", _schema.schema_name},
                {"table", table_oid},
                {"columns", invalid_columns}
            };

            // mark the table as invalid, we won't copy it
            TableValidator::get_instance()->mark_invalid(table_oid, table_info);

            // mark the copy as "in-flight" to wake up the log reader
            // note: we don't need a schema since we are going to ignore this table
            pg_log_mgr::SyncTracker::get_instance()->mark_inflight(db_id, _schema.table_oid, xid,
                                                                   snapshot_details, nullptr);

            return std::make_shared<PgCopyResult::TableInfo>(table_oid, nullptr, nullptr);
        }

        // get secondary indexes
        _get_secondary_indexes();

        auto copy_info = std::make_shared<proto::CopyTableInfo>();

        // Set namespace request
        auto* ns_req = copy_info->mutable_namespace_req();
        ns_req->set_db_id(db_id);
        ns_req->set_namespace_id(_schema.schema_oid);
        ns_req->set_name(_schema.schema_name);
        ns_req->set_xid(xid.xid);
        ns_req->set_lsn(1);

        // Set table request
        auto* table_req = copy_info->mutable_table_req();
        table_req->set_db_id(db_id);
        table_req->set_xid(xid.xid);
        table_req->set_lsn(1);

        auto* table_info = table_req->mutable_table();
        table_info->set_id(table_oid);
        table_info->set_namespace_name(_schema.schema_name);
        table_info->set_name(_schema.table_name);
        table_info->set_rls_enabled(_schema.rls_enabled);
        table_info->set_rls_forced(_schema.rls_forced);

        // partition info
        if (_schema.parent_oid.has_value()) {
            table_info->set_parent_table_id(static_cast<int32_t>(_schema.parent_oid.value()));
        } else {
            table_info->set_parent_table_id(constant::INVALID_TABLE);
        }

        if (_schema.partition_key.has_value()) {
            table_info->set_partition_key(_schema.partition_key.value());
        }

        if (_schema.partition_bound.has_value()) {
            table_info->set_partition_bound(_schema.partition_bound.value());
        }

        for (const auto &col : _schema.columns) {
            auto* column = table_info->add_columns();
            column->set_name(col.name);
            column->set_type(static_cast<int32_t>(col.type));
            column->set_pg_type(col.pg_type);
            column->set_position(col.position);
            column->set_is_nullable(col.nullable);
            column->set_is_generated(false);
            column->set_type_name(col.type_name);
            column->set_type_namespace(col.type_namespace);
            if (col.pkey_position) {
                column->set_pk_position(*col.pkey_position);
            }
            if (col.default_value) {
                column->set_default_value(*col.default_value);
            }
        }

        // Add index requests
        for (const auto &index : _schema.secondary_keys) {
            auto* index_req = copy_info->add_index_reqs();
            index_req->set_db_id(db_id);
            index_req->set_xid(xid.xid);
            index_req->set_lsn(constant::MAX_LSN-1);

            auto* index_info = index_req->mutable_index();
            index_info->set_id(index.id);
            index_info->set_table_id(_schema.table_oid);
            index_info->set_namespace_name(_schema.schema_name);
            index_info->set_is_unique(index.is_unique);
            index_info->set_name(index.name);
            index_info->set_state(index.state);

            for (const auto &column : index.columns) {
                auto* index_column = index_info->add_columns();
                index_column->set_idx_position(column.idx_position);
                index_column->set_position(column.position);
            }
        }

        ExtensionCallback extension_callback = {PgExtnRegistry::get_instance()->comparator_func};
        auto schema = std::make_shared<ExtentSchema>(_schema.columns, extension_callback, false, false);
        auto table = TableMgr::get_instance()->get_snapshot_table(db_id, _schema.table_oid, xid.xid,
                                                                  schema, _schema.secondary_keys);

        // mark the copy as inflight and record the snapshot details
        // note: we create a version of the schema that may contain undefined data so that we can
        //       correctly record updates with unchanged data from the replication stream
        auto update_schema = std::make_shared<ExtentSchema>(_schema.columns, extension_callback, true, false);
        pg_log_mgr::SyncTracker::get_instance()->mark_inflight(db_id, _schema.table_oid, xid,
                                                               snapshot_details, update_schema);

        // only do the COPY if there are no partition keys
        // if there are partition keys, then this is not a leaf table
        if (!_schema.partition_key.has_value()) {
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

                auto fields = _parse_row(*data, user_types, pos);
                if (!fields) {
                    break; // saw footer, finished with the COPY
                }

                // Append internal_row_id
                fields->push_back(std::make_shared<ConstTypeField<uint64_t>>(
                            table->get_next_internal_row_id()));

                // construct a tuple from the row
                auto tuple = std::make_shared<FieldTuple>(fields, nullptr);

                // add the row to the table
                table->insert(tuple, constant::UNKNOWN_EXTENT);
            }
        }

        // flush the table data to disk
        auto &&metadata = table->finalize();

        // Set roots request
        auto* roots_req = copy_info->mutable_roots_req();
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
        stats->set_end_offset(metadata.stats.end_offset);
        stats->set_last_internal_row_id(metadata.stats.last_internal_row_id);
        roots_req->set_snapshot_xid(metadata.snapshot_xid);

        copy_info->set_is_table_dropped(_is_table_dropped(_schema.schema_oid, table_oid));
        LOG_INFO("Copied table {}.{} with oid {} and schema oid {} and xid {}",
                 _schema.schema_name, _schema.table_name, table_oid, _schema.schema_oid, xid.xid);

        return std::make_shared<PgCopyResult::TableInfo>(table_oid, copy_info, schema);
    }

    int32_t PgCopyTable::_verify_copy_header(const std::string_view &header)
    {
        // verify signature
        int r = std::memcmp(header.data(), COPY_SIGNATURE, 11);
        if (r != 0) {
            LOG_ERROR("Signature doesn't match");
            throw PgUnknownMessageError();
        }

        int32_t flags = recvint32(header.data() + 11);
        LOG_DEBUG(LOG_PG_REPL, LOG_LEVEL_DEBUG1, "header flags: 0x{:X}", flags);
        if ((flags >> 16) & 0x1) {
            // bit 16 tells us if oids are present
            _oid_flag = true;
        }

        // return the length of any header extension
        return recvint32(header.data() + 15);
    }

    ConstTypeFieldPtr<float>
    PgCopyTable::_get_enum_field(int32_t pg_type,
                                 const std::string &label,
                                 const std::map<int32_t, std::map<std::string, float>> &type_map)
    {
        // lookup type oid
        auto it = type_map.find(pg_type);
        if (it == type_map.end()) {
            LOG_ERROR("Enum type {} not found in user types", pg_type);
            throw TypeError(fmt::format("Enum type not found: {}", pg_type));
        }

        // lookup enum value (label)
        auto enum_it = it->second.find(label);
        if (enum_it == it->second.end()) {
            LOG_ERROR("Enum value {} not found in user types", label);
            throw TypeError(fmt::format("Enum value not found: {}", label));
        }

        return std::make_shared<ConstTypeField<float>>(enum_it->second);
    }

    FieldArrayPtr
    PgCopyTable::_parse_row(const std::string_view &row,
                            const std::map<int32_t, std::map<std::string, float>> &type_map,
                            size_t &pos)
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
            char pg_type_category = _schema.columns[i].type_category;
            auto type = convert_pg_type(pg_type, pg_type_category);

            // check if null
            if (length == -1) {
                fields->push_back(std::make_shared<ConstNullField>(type));
                continue;
            }

            // handle user defined types, enums
            if (pg_type_category == constant::USER_TYPE_ENUM) {
                CHECK(type == SchemaType::FLOAT32);
                fields->push_back(_get_enum_field(pg_type, std::string(row.data() + pos, length), type_map));
                pos += length;
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

            case (SchemaType::NUMERIC): {
                std::string_view tmp(row.data() + pos, length);
                std::shared_ptr<numeric::NumericData> numeric_value = numeric::numeric_receive(tmp.begin(), length, 0);
                auto buf = (char*)(numeric_value.get());
                std::vector<char> value(buf, buf + numeric_value->varsize());
                fields->push_back(std::make_shared<ConstTypeField<
                        std::shared_ptr<numeric::NumericData>>>(std::move(value)));
                pos += length;
                break;
            }

            case (SchemaType::BINARY): {
                std::string_view tmp(row.data() + pos, length);
                std::vector<char> data(tmp.begin(), tmp.end());
                fields->push_back(std::make_shared<ConstTypeField<std::vector<char>>>(data));
                pos += length;
                break;
            }

            case (SchemaType::EXTENSION): {
                std::string_view tmp(row.data() + pos, length);

                LOG_DEBUG(LOG_PG_LOG_MGR, LOG_LEVEL_DEBUG3, "Converting extension type '{}' into EXTENSION", pg_type);
                std::vector<char> data(tmp.begin(), tmp.end());
                fields->push_back(std::make_shared<ConstTypeField<std::vector<char>>>(data, true));
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
    PgCopyTable::lock_table(uint32_t table_oid)
    {
        std::string name_query = fmt::format(TABLE_NAMESPACE_NAME_QUERY, table_oid);

        int retry = 3;
        while (retry) {
            try {
                _connection.exec(name_query);
                if (_connection.ntuples() != 1) {
                    throw PgTableNotFoundError();
                }

                std::string schema_name =
                    _connection.escape_identifier(_connection.get_string(0, 0));
                std::string table_name =
                    _connection.escape_identifier(_connection.get_string(0, 1));
                std::string lock_query = fmt::format(TABLE_LOCK_QUERY, schema_name, table_name);

                _connection.exec(lock_query);
                return;
            } catch (PgQueryError &e) {
                // re-start the transaction and retry
                _connection.rollback_transaction();
                _connection.start_transaction();
            }

            --retry;
        }

        throw PgQueryError();
    }

    void
    PgCopyTable::_get_table_oids(const std::string &query,
                                 std::set<uint32_t> &table_oids)
    {
        // do the tables query
        _connection.exec(query);
        if (_connection.ntuples() == 0) {
            LOG_ERROR("No tables found in database");
            _connection.clear();
            return;
        }

        // iterate through the results and get the table oids
        for (int i = 0; i < _connection.ntuples(); i++) {
            // get the table name, schema name, and oid
            uint32_t table_oid = _connection.get_int32(i, 2);
            table_oids.insert(table_oid);
        }

        return;
    }

    void
    PgCopyTable::_get_table_oids(const nlohmann::json &include_json,
                                 std::set<uint32_t> &table_oids)
    {
        // get schemas array from json into vector of strings

        // check for schemas array
        if (include_json.contains("schemas") && include_json["schemas"].is_array()) {
            std::vector<std::string> schemas = include_json["schemas"];
            if (!schemas.empty()) {
                if (schemas[0] == "*") {
                    // all tables in db
                    _get_table_oids(std::string(TABLES_QUERY), table_oids);
                    return;
                }

                // construct query by joining schema names
                std::vector<std::string> schema_names;
                for (const auto &schema : schemas) {
                    schema_names.push_back(fmt::format("'{}'", _connection.escape_string(schema)));
                }

                _get_table_oids(fmt::format(TABLES_SCHEMA_QUERY,
                                common::join_string(",", schema_names.begin(), schema_names.end())),
                                table_oids);
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
                _get_table_oids(fmt::format(TABLE_SCHEMA_PAIR_QUERY, common::join_string(",", pairs.begin(), pairs.end())), table_oids);
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
                             const std::unordered_set<uint32_t> &table_oids)
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
        return _internal_copy(db_id, xid, std::nullopt, std::nullopt, std::nullopt, std::optional{include_json});
    }

    void
    PgCopyTable::_worker(uint64_t db_id,
                         uint64_t target_xid,
                         CopyQueuePtr copy_queue,
                         std::vector<PgCopyResultPtr> &result)
    {
        XidLsn xid(target_xid, 0);

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

            PgCopyResultPtr copy_result = std::make_shared<PgCopyResult>(target_xid);

            LOG_INFO("Copy table start: oid {}, xid {}", request->table_oid, xid.xid);

            // create copy table object and connect to db
            PgCopyTable copy_table;
            copy_table.connect(db_id);

            // get a lock on the table to prevent schema changes from being applied

            // note: This is an issue because concurrent DDL changes to the table may result in the
            //       snapshot transaction's relcache to get invalidated.  If that happens then the
            //       view of the system tables that we query may differ from the actual schema of
            //       the table data that is returned, which can cause failures.
            copy_table.lock_table(request->table_oid);

            // get the xids associated w/snapshot
            std::pair<uint64_t, std::string> snapshot_info = copy_table._get_xact_xids();
            copy_result->set_snapshot(snapshot_info.first, snapshot_info.second);

            LOG_INFO("Copy table -> Snapshot info: xid {}, xids {}", snapshot_info.first, snapshot_info.second);

            // get the list of user defined types
            auto &&user_type_map = copy_table._get_user_types();

            try {
                // copy the table
                auto info = copy_table._copy_table(db_id,
                                                   xid,
                                                   user_type_map,
                                                   request->table_oid,
                                                   copy_result);

                bool is_partition_table = false;
                if (info->info != nullptr) {
                    // Get the details about the table to find out if its a partitioned tables
                    // If the partition_keys is not empty or partition_bound is not empty, then it is a partitioned table
                    auto table_req_info = info->info->table_req().table();
                    is_partition_table = !table_req_info.partition_bound().empty() || !table_req_info.partition_key().empty();
                }

                // add the table oid to the result
                copy_result->add_table(info);
                if (copy_result->tids.size() > 0) {
                    result.push_back(copy_result);
                    // If the table is not a partitioned table, send sync message
                    // If it is a partitioned table, wait for all partition tables to be processed,
                    //                               i.e., all the tables in the current copy_queue are processed
                    if (!is_partition_table || copy_queue->empty()) {
                        copy_table._send_sync_msg(copy_result);
                    }
                }
            } catch (PgRetryError &e) {
                LOG_ERROR("Unexpected error, will retry table copy: {}", request->table_oid);
                copy_queue->push(request);
            } catch (PgTableNotFoundError &e) {
                LOG_ERROR("Table not found: oid {}", request->table_oid);
            } catch (PgQueryError &e) {
                e.log_backtrace();
                LOG_ERROR("Error copying table: oid {}", request->table_oid);
                CHECK(false);
            }

            // end the copy
            copy_table._end_copy();
            copy_table.disconnect();

            LOG_INFO("Copy table complete: oid {}, xid: {}", request->table_oid, xid.xid);

            // reset schema object for next table
            copy_table._reset_schema();
        }
    }

    void
    PgCopyTable::_send_sync_msg(PgCopyResultPtr result)
    {
        std::string sync_msg = fmt::format(R"({{"target_xid":{}, "pg_xid":{}}})", result->target_xid, result->pg_xid);
        std::string query = fmt::format(REPL_MSG_QUERY, pg_msg::MSG_PREFIX_COPY_SYNC, sync_msg);

        LOG_INFO("Copy table -> Sending sync message: {}", query);
        _connection.exec(query);
        if (_connection.status() != PGRES_TUPLES_OK) {
            LOG_ERROR("Error sending sync message");
            _connection.clear();
            throw PgQueryError();
        }

        _connection.clear();
    }

    std::string
    PgCopyTable::_get_schema_condition(LibPqConnection &connection, uint64_t db_id)
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
                schema_names.push_back(fmt::format("'{}'", connection.escape_string(schema)));
            }
        }

        if (include_json.contains("tables") && include_json["tables"].is_array()) {
            for (const auto &table : include_json["tables"]) {
                if (table.contains("schema") && table.contains("table")) {
                    std::string schema = table["schema"].get<std::string>();
                    schema_names.push_back(fmt::format("'{}'", connection.escape_string(schema)));
                }
            }
        }

        std::string schema_condition = "";
        if (!schema_names.empty()) {
            schema_condition = fmt::format("AND nspname IN ({})", common::join_string(",", schema_names.begin(), schema_names.end()));
        }

        return schema_condition;
    }

    std::vector<std::pair<uint64_t, std::string>>
    PgCopyTable::_get_namespaces(uint64_t db_id, uint64_t xid)
    {
        // get the namespaces
        std::string schema_condition = _get_schema_condition(_connection, db_id);
        _connection.exec(fmt::format(NAMESPACE_QUERY, schema_condition));

        if (_connection.ntuples() == 0) {
            // Technically this should never happen, but keep this here just in case
            _connection.clear();
            LOG_ERROR("Error while getting namespaces");
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
    PgCopyTable::create_usertypes(uint64_t db_id, uint64_t xid)
    {
        PgCopyTable copy_table;

        // connect to the database
        copy_table.connect(db_id);

        // get the list of user defined types
        std::string schema_condition = _get_schema_condition(copy_table._connection, db_id);
        copy_table._connection.exec(fmt::format(ENUM_QUERY, schema_condition));

        if (copy_table._connection.ntuples() == 0) {
            LOG_INFO("No user defined types found for db_id: {}", db_id);
            copy_table._connection.clear();
            copy_table.disconnect();
            return;
        }

        auto server = sys_tbl_mgr::Server::get_instance();
        // iterate through the results and get the user defined types
        for (int i = 0; i < copy_table._connection.ntuples(); i++) {
            uint32_t enum_type_oid = copy_table._connection.get_int32(i, 0);
            uint32_t namespace_oid = copy_table._connection.get_int32(i, 1);
            std::string namespace_name = copy_table._connection.get_string(i, 2);
            std::string enum_type_name = copy_table._connection.get_string(i, 3);
            std::string enum_value_json = copy_table._connection.get_string(i, 4);

            LOG_DEBUG(LOG_PG_LOG_MGR, LOG_LEVEL_DEBUG1, "Creating user defined type: {}, values: {}", enum_type_name, enum_value_json);

            PgMsgUserType msg;
            msg.lsn = 0;
            msg.oid = enum_type_oid;
            msg.xid = xid;
            msg.namespace_id = namespace_oid;
            msg.type = constant::USER_TYPE_ENUM; // only support enum types
            msg.namespace_name = namespace_name;
            msg.name = enum_type_name;
            msg.value_json = enum_value_json;

            server->create_usertype(db_id, {xid, 0}, msg);
        }

        // disconnect from the database
        copy_table.disconnect();
    }

    void
    PgCopyTable::create_extn_types(uint64_t db_id, uint64_t xid)
    {
        PgCopyTable copy_table;

        // connect to the database
        copy_table.connect(db_id);

        // get the list of user defined types
        std::string schema_condition = _get_schema_condition(copy_table._connection, db_id);
        copy_table._connection.exec(fmt::format(EXTN_QUERY, schema_condition));

        if (copy_table._connection.ntuples() == 0) {
            LOG_INFO("No extension types found for db_id: {}", db_id);
            copy_table._connection.clear();
            copy_table.disconnect();
            return;
        }

        auto server = sys_tbl_mgr::Server::get_instance();
        // iterate through the results and get the user defined types
        for (int i = 0; i < copy_table._connection.ntuples(); i++) {
            uint32_t enum_type_oid = copy_table._connection.get_int32(i, 0);
            std::string extension_name = copy_table._connection.get_string(i, 1);
            uint32_t namespace_oid = copy_table._connection.get_int32(i, 2);
            std::string namespace_name = copy_table._connection.get_string(i, 3);
            std::string extn_type_name = copy_table._connection.get_string(i, 4);

            PgMsgUserType msg;
            msg.lsn = 0;
            msg.oid = enum_type_oid;
            msg.xid = xid;
            msg.namespace_id = namespace_oid;
            msg.namespace_name = namespace_name;
            msg.name = extn_type_name;
            msg.value_json = "{}";
            msg.type = constant::USER_TYPE_EXTENSION;

            LOG_DEBUG(LOG_PG_LOG_MGR, LOG_LEVEL_DEBUG2, "Creating extension type: {}", extn_type_name);

            server->create_usertype(db_id, {xid, 0}, msg);
        }

        // disconnect from the database
        copy_table.disconnect();
    }

    void
    PgCopyTable::_load_extn_types(uint64_t db_id, const std::string& extension)
    {
        auto extn_registry = PgExtnRegistry::get_instance();
        PgCopyTable copy_table;

        // connect to the database
        copy_table.connect(db_id);

        copy_table._connection.exec(fmt::format(PgExtnRegistry::TYPE_QUERY, extension));

        for (int i = 0; i < copy_table._connection.ntuples(); i++) {
            uint32_t type_oid = copy_table._connection.get_int32(i, 0);
            std::string type_input = copy_table._connection.get_string(i, 1);
            std::string type_output = copy_table._connection.get_string(i, 2);
            std::string type_receive = copy_table._connection.get_string(i, 3);
            std::string type_send = copy_table._connection.get_string(i, 4);

            LOG_DEBUG(LOG_PG_LOG_MGR, LOG_LEVEL_DEBUG2, "Adding type: {}, input: {}, output: {}, receive: {}, send: {} for extension: {}", type_oid, type_input, type_output, type_receive, type_send, extension);
            extn_registry->add_type(extension, type_oid, type_input, type_output, type_receive, type_send);
        }

        copy_table.disconnect();
    }

    void
    PgCopyTable::_load_extn_operators(uint64_t db_id, const std::string& extension)
    {
        auto extn_registry = PgExtnRegistry::get_instance();
        PgCopyTable copy_table;

        // connect to the database
        copy_table.connect(db_id);

        copy_table._connection.exec(fmt::format(PgExtnRegistry::OPER_QUERY, extension));

        for (int i = 0; i < copy_table._connection.ntuples(); i++) {
            uint32_t oper_oid = copy_table._connection.get_int32(i, 0);
            std::string oper_name = copy_table._connection.get_string(i, 1);
            std::string oper_proc = copy_table._connection.get_string(i, 2);
            std::string proc_name = copy_table._connection.get_string(i, 3);

            LOG_DEBUG(LOG_PG_LOG_MGR, LOG_LEVEL_DEBUG1, "Adding operator: {}, name: {}, proc: {} for extension: {}", oper_oid, oper_name, proc_name, extension);
            extn_registry->add_operator(extension, oper_oid, oper_name, proc_name);
        }

        copy_table.disconnect();
    }

    void
    PgCopyTable::init_pg_extn_registry(uint64_t db_id, uint64_t xid)
    {
        nlohmann::json extension_config_json = Properties::get(Properties::EXTENSION_CONFIG);
        if (extension_config_json.is_null()) {
            LOG_WARN("No extension configuration found for database {}", db_id);
            return;
        }

        // Get the extensions for this specific database ID
        std::string db_id_str = std::to_string(db_id);
        if (!extension_config_json.contains(db_id_str)) {
            LOG_WARN("No extensions configured for database {}", db_id);
            return;
        }

        auto db_extensions = extension_config_json[db_id_str];
        std::string lib_path = extension_config_json.value("lib_path", "/usr/lib/postgresql/16/lib/");

        for (auto& ext : db_extensions.items()) {
            auto extension_name = ext.key();
            std::string extension_lib_path = fmt::format("{}{}.so", lib_path, extension_name);

            LOG_DEBUG(LOG_PG_LOG_MGR, LOG_LEVEL_DEBUG1, "Initializing library for extension: {} for db_id: {}",
                      extension_name, db_id);
            PgExtnRegistry::get_instance()->init_libraries(db_id, extension_name, extension_lib_path);
            _load_extn_types(db_id, extension_name);
            _load_extn_operators(db_id, extension_name);
        }
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

        auto server = sys_tbl_mgr::Server::get_instance();
        // create the namespaces
        for (const auto &namespace_info : namespaces) {
            LOG_DEBUG(LOG_PG_LOG_MGR, LOG_LEVEL_DEBUG1, "Creating namespace: {}", namespace_info.second);

            PgMsgNamespace msg;
            msg.oid = namespace_info.first;
            msg.name = namespace_info.second;

            // create the namespace
            server->create_namespace(db_id, {xid, 0}, msg);
        }
    }

    std::map<int32_t, std::map<std::string, float>>
    PgCopyTable::_get_user_types()
    {
        std::map<int32_t, std::map<std::string, float>> user_types;

        // get the list of user defined types
        _connection.exec(fmt::format(ENUM_LOOKUP_QUERY));

        if (_connection.ntuples() == 0) {
            LOG_INFO("No user defined types found");
            _connection.clear();
            return user_types;
        }

        // iterate through the results and get the user defined types
        for (int i = 0; i < _connection.ntuples(); i++) {
            uint32_t enum_type_oid = _connection.get_int32(i, 0);
            float enum_sortorder = _connection.get_float(i, 1);
            std::string enum_label = _connection.get_string(i, 2);

            // add the enum type to the map
            user_types[enum_type_oid][enum_label] = enum_sortorder;
        }

        _connection.clear();
        return user_types;
    }


    std::vector<PgCopyResultPtr>
    PgCopyTable::_internal_copy(uint64_t db_id,
                                uint64_t target_xid,
                                std::optional<std::string> schema_name,
                                std::optional<std::pair<std::string, std::string>> schema_table,
                                std::optional<std::unordered_set<uint32_t>> table_tids,
                                std::optional<nlohmann::json> include_json)
    {
        CopyQueuePtr copy_queue = std::make_shared<CopyQueue>();

        // create copy table object and connect to db
        PgCopyTable copy_table;
        copy_table.connect(db_id);

        // fetch the table oids
        std::set<uint32_t> table_oids;

        // get the table oids, depends on input
        if (schema_name.has_value()) {
            // by schema name, need to escape the schema name
            // escape the schema name
            std::string schema = "'" + copy_table._connection.escape_string(schema_name.value()) + "'";
            copy_table._get_table_oids(fmt::format(TABLES_SCHEMA_QUERY, schema), table_oids);
        } else if (table_tids.has_value()) {
            // by table oids; copy the tids to the table_oids set
            if (!table_tids.value().empty()) {
                // copy the tids
                table_oids.insert(table_tids.value().begin(), table_tids.value().end());
            }
        } else if (schema_table.has_value()) {
            // by schema, table pair
            std::string schema = copy_table._connection.escape_string(schema_table.value().first);
            std::string table = copy_table._connection.escape_string(schema_table.value().second);
            copy_table._get_table_oids(fmt::format(TABLE_OID_QUERY, table, schema), table_oids);
        } else if (include_json.has_value()) {
            copy_table._get_table_oids(include_json.value(), table_oids);
        } else {
            // all tables in db
            copy_table._get_table_oids(std::string(TABLES_QUERY), table_oids);
        }

        // close this connection
        copy_table._end_copy();
        copy_table.disconnect();

        auto token_init = open_telemetry::OpenTelemetry::get_instance()->set_context_variables(
            {{"db_id", std::to_string(db_id)}, {"xid", std::to_string(target_xid)}});

        // create a worker thread to copy the tables
        std::vector<std::thread> workers;
        int worker_count = std::min(static_cast<std::size_t>(WORKER_THREADS), table_oids.size());
        std::vector<std::vector<PgCopyResultPtr>> worker_results(worker_count);
        for (int i = 0; i < worker_count; i++) {
            std::string thread_name = fmt::format("PgCopyWorker_{}", i);
            workers.emplace_back(&PgCopyTable::_worker, db_id, target_xid, copy_queue,
                                          std::ref(worker_results[i]));
            pthread_setname_np(workers.back().native_handle(), thread_name.c_str());
        }

        // iterate through the tables and copy them
        for (const auto &table_oid : table_oids) {
            LOG_DEBUG(LOG_PG_LOG_MGR, LOG_LEVEL_DEBUG1, "Queueing copy for table_oid: {}, target_xid: {}", table_oid, target_xid);

            // add the table to the copy queue
            copy_queue->push(std::make_shared<CopyRequest>(table_oid));
        }

        // shutdown the copy queue; blocks until queue is empty
        copy_queue->shutdown(true);
        assert (copy_queue->empty());

        // join the worker threads
        for (auto &worker : workers) {
            worker.join();
        }

        // create result object
        std::vector<PgCopyResultPtr> table_results;
        for (auto &results : worker_results) {
            table_results.insert(table_results.end(), results.begin(), results.end());
        }
        return table_results;
    }

    void
    PgCopyTable::_reset_schema()
    {
        _schema_name = std::string();
        _table_name = std::string();
        _oid_flag = false;
        _schema = {};
    }

} // namespace springtail
