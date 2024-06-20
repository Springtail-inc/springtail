#include <fmt/core.h>

#include <common/common.hh>
#include <common/logging.hh>

#include <pg_fdw/pg_fdw_mgr.hh>

extern "C" {
    #include <postgres.h>
    #include <postgres_ext.h>
    #include <utils/builtins.h>
    #include <nodes/pg_list.h>
    #include <nodes/primnodes.h>
}

namespace springtail {
    PgFdwMgr* PgFdwMgr::_instance {nullptr};

    std::once_flag PgFdwMgr::_init_flag;

    PgFdwMgr*
    PgFdwMgr::_init()
    {
        springtail_init();
        _instance = new PgFdwMgr();
        return _instance;
    }

    PgFdwState *
    PgFdwMgr::fdw_begin(uint64_t tid, uint64_t xid)
    {
        if (xid == 0) {
            xid = XidMgrClient::get_instance()->get_committed_xid();
        }

        SPDLOG_DEBUG_MODULE(LOG_FDW, "fdw_begin: tid: {}, xid: {}", tid, xid);

        TablePtr table = TableMgr::get_instance()->get_table(tid, xid, constant::MAX_LSN);
        PgFdwState *state = new PgFdwState{table, tid, xid, table->begin()};
        return state;
    }

    void
    PgFdwMgr::fdw_end(PgFdwState *state)
    {
        SPDLOG_DEBUG_MODULE(LOG_FDW, "fdw_end: tid: {}", state->tid);
        delete state;
    }

    void
    PgFdwMgr::fdw_reset_scan(PgFdwState *state)
    {
        SPDLOG_DEBUG_MODULE(LOG_FDW, "fdw_reset_scan: tid: {}", state->tid);
        state->iter.reset();
        state->iter.emplace(Table::Iterator(state->table->begin()));
    }

    bool
    PgFdwMgr::fdw_iterate_scan(PgFdwState *state, Datum *values, bool *nulls)
    {
        // check iterator is valid
        if (!state->iter.has_value()) {
            return false;
        }

        // check if iterator is at end
        if (*state->iter == state->table->end()) {
            return false;
        }

        SPDLOG_DEBUG_MODULE(LOG_FDW, "fdw_iterate_scan: tid: {}", state->tid);

        // get current row
        Extent::Row row = *(*state->iter);
        // iterate through fields
        for (size_t i = 0; i < state->fields->size(); i++) {
            // get field
            FieldPtr field = state->fields->at(i);
            // set null
            nulls[i] = field->is_null(row);

            // set value
            if (!nulls[i]) {
                values[i] = _get_datum_from_field(field, row);
            } else {
                values[i] = 0;
            }
        }

        // increment iterator
        ++(*state->iter);

        return true;
    }

    List *
    PgFdwMgr::fdw_can_sort(SpringtailPlanState *state, List *sortgroup)
    {
        // XXX not implemented
        return NIL;
    }

    List *
    PgFdwMgr::fdw_get_path_keys(SpringtailPlanState *state)
    {
        // XXX not implemented
        return NIL;
    }

    void
    PgFdwMgr::fdw_get_rel_size(SpringtailPlanState *planstate, List *target_list, List *qual_list, double *rows, int *width)
    {
        // XXX not implemented
        *rows = 0;
        *width = 0;
    }

    Datum
    PgFdwMgr::_get_datum_from_field(FieldPtr field, const Extent::Row &row)
    {
        switch (field->get_type()) {
            case SchemaType::INT64:
                return Int64GetDatum(field->get_int64(row));
            case SchemaType::UINT64:
                return UInt64GetDatum(field->get_uint64(row));
            case SchemaType::INT32:
                return Int32GetDatum(field->get_int32(row));
            case SchemaType::UINT32:
                return UInt32GetDatum(field->get_uint32(row));
            case SchemaType::INT16:
                return Int16GetDatum(field->get_int16(row));
            case SchemaType::UINT16:
                return UInt16GetDatum(field->get_uint16(row));
            case SchemaType::INT8:
                return Int8GetDatum(field->get_int8(row));
            case SchemaType::UINT8:
                return UInt8GetDatum(field->get_uint8(row));
            case SchemaType::BOOLEAN:
                return BoolGetDatum(field->get_bool(row));
            case SchemaType::FLOAT64:
                return Float8GetDatum(field->get_float64(row));
            case SchemaType::FLOAT32:
                return Float4GetDatum(field->get_float32(row));
            case SchemaType::TEXT: {
                char *duped_str = pstrdup(field->get_text(row).c_str());
                return CStringGetTextDatum(duped_str);
            }

            // XXX no getters in field for
            case SchemaType::TIMESTAMP:
            case SchemaType::DATE:
            case SchemaType::TIME:
            case SchemaType::DECIMAL128:

            default:
                return 0;
        }
    }

    std::string
    PgFdwMgr::_gen_fdw_table_sql(const std::string &server_name,
                                 const std::string &table,
                                 uint64_t tid,
                                 std::vector<std::tuple<std::string, uint8_t, bool, std::optional<std::string>>> &columns)
    {
        // no schema name needed
        std::string create = fmt::format("CREATE FOREIGN TABLE {} (\n", quote_identifier(table.c_str()));

        // iterate over the columns, adding each to the create statement
        // name, type, is_nullable, default value
        for (int i = 0; i < columns.size(); i++) {
            const auto &[column_name, type, nullable, default_value] = columns[i];
            std::string column = fmt::format("  {} ", quote_identifier(column_name.c_str()));

            switch (static_cast<SchemaType>(type)) {
                case SchemaType::UINT8: // XXX no good mapping; use smallint for now
                case SchemaType::INT8:
                    column += "SMALLINT";
                    break;
                case SchemaType::UINT32:
                case SchemaType::INT32:
                    column += "INTEGER";
                    break;
                case SchemaType::UINT16:
                case SchemaType::INT16:
                    column += "SMALLINT";
                    break;
                case SchemaType::UINT64:
                case SchemaType::INT64:
                    column += "BIGINT";
                    break;
                case SchemaType::FLOAT32:
                    column += "FLOAT4";
                    break;
                case SchemaType::FLOAT64:
                    column += "FLOAT8";
                    break;
                case SchemaType::BOOLEAN:
                    column += "BOOLEAN";
                    break;
                case SchemaType::TEXT:
                    column += "TEXT";
                    break;
                case SchemaType::DATE:
                    column += "DATE";
                    break;
                case SchemaType::TIME:
                    column += "TIME";
                    break;
                case SchemaType::TIMESTAMP:
                    column += "TIMESTAMP";
                    break;
                case SchemaType::BINARY:
                    column += "BYTEA";
                    break;
                default:
                    SPDLOG_ERROR("Unknown type {}", type);
                    return "";
            }

            // add nullability and default
            if (!nullable) {
                column += " NOT NULL";
            }

            if (default_value.has_value() && !default_value.value().empty()) {
                column += fmt::format(" DEFAULT {}", default_value.value());
            }

            if (i < columns.size() - 1) {
                column += ",\n";
            }

            create += column;
        }

        create += fmt::format("\n) SERVER {} OPTIONS (tid '{}');", quote_identifier(server_name.c_str()), tid);

        SPDLOG_DEBUG_MODULE(LOG_FDW, "Generated SQL: {}", create);

        return create;
    }

    std::string
    PgFdwMgr::_gen_fdw_system_table(const std::string &server,
                                    const std::string &table_name,
                                    uint64_t tid,
                                    const std::vector<SchemaColumn> &column_schema)
    {
        // column description: name, type, nullable, default
        std::vector<std::tuple<std::string, uint8_t, bool, std::optional<std::string>>> columns;

        for (const auto &column : column_schema) {
            columns.push_back({column.name, (uint8_t)column.type, column.nullable, column.default_value});
        }

        return _gen_fdw_table_sql(server, table_name, tid, columns);
    }

    List *
    PgFdwMgr::_import_springtail_catalog(const std::string &server,
                                         const std::set<std::string> table_set,
                                         bool exclude, bool limit)
    {
        List        *commands = NIL;
        std::string  sql;

        // go through system tables, make sure that they are not excluded and add them to the list
        if (!((exclude && table_set.contains(CATALOG_TABLE_NAMES)) ||
              (limit && !table_set.contains(CATALOG_TABLE_NAMES)))) {
            sql = _gen_fdw_system_table(server, CATALOG_TABLE_NAMES, sys_tbl::TableNames::ID, sys_tbl::TableNames::Data::SCHEMA);
            commands = lappend(commands, pstrdup(sql.c_str()));
        }

        if (!((exclude && table_set.contains(CATALOG_TABLE_ROOTS)) ||
              (limit && !table_set.contains(CATALOG_TABLE_ROOTS)))) {
            sql = _gen_fdw_system_table(server, CATALOG_TABLE_ROOTS, sys_tbl::TableRoots::ID, sys_tbl::TableRoots::Data::SCHEMA);
            commands = lappend(commands, pstrdup(sql.c_str()));
        }

        if (!((exclude && table_set.contains(CATALOG_TABLE_INDEXES)) ||
              (limit && !table_set.contains(CATALOG_TABLE_INDEXES)))) {
            sql = _gen_fdw_system_table(server, CATALOG_TABLE_INDEXES, sys_tbl::Indexes::ID, sys_tbl::Indexes::Data::SCHEMA);
            commands = lappend(commands, pstrdup(sql.c_str()));
        }

        if (!((exclude && table_set.contains(CATALOG_TABLE_SCHEMAS)) ||
              (limit && !table_set.contains(CATALOG_TABLE_SCHEMAS)))) {
            sql = _gen_fdw_system_table(server, CATALOG_TABLE_SCHEMAS, sys_tbl::Schemas::ID, sys_tbl::Schemas::Data::SCHEMA);
            commands = lappend(commands, pstrdup(sql.c_str()));
        }

        return commands;
    }

    List *
    PgFdwMgr::fdw_import_foreign_schema(const std::string &server,
                                        const std::string &schema,
                                        const List *table_list,
                                        bool exclude, bool limit)
    {
        List                 *commands = NIL;
        std::set<std::string> table_set;

        // construct list of either excluded or limited tables
        if (exclude || limit) {
            ListCell *lc;
            foreach(lc, table_list) {
                RangeVar *rv = (RangeVar *)lfirst(lc);
                table_set.insert(rv->relname);
            }
        }

        // if we are importing the catalog schema, handle it separately
        if (schema == CATALOG_SCHEMA_NAME) {
            return _import_springtail_catalog(server, table_set, exclude, limit);
        }

        // get the table names table to iterate over
        auto table = TableMgr::get_instance()->get_table(sys_tbl::TableNames::ID,
                                                         constant::LATEST_XID,
                                                         constant::MAX_LSN);
        // get field array
        auto fields = table->extent_schema()->get_fields();

        // map from table name -> <table id, xid>
        std::map<std::string, std::pair<uint64_t,uint64_t>> table_map;

        // iterate over the table names table and populate the table map
        for (auto row : (*table)) {
            std::string schema_name = fields->at(sys_tbl::TableNames::Data::NAMESPACE)->get_text(row);

            // check for schema match
            if (schema_name != schema) {
                continue;
            }

            std::string table_name = fields->at(sys_tbl::TableNames::Data::NAME)->get_text(row);
            // handle limit and exclude
            if (exclude && table_set.contains(table_name)) {
                SPDLOG_DEBUG_MODULE(LOG_FDW, "Excluding table {}.{}", schema_name, table_name);
                continue;
            }

            // XXX should really stop after we have found all tables in limit
            if (limit && !table_set.contains(table_name)) {
                SPDLOG_DEBUG_MODULE(LOG_FDW, "Limit, skipping table {}.{}", schema_name, table_name);
                continue;
            }

            uint64_t tid = fields->at(sys_tbl::TableNames::Data::TABLE_ID)->get_uint64(row);
            uint64_t xid = fields->at(sys_tbl::TableNames::Data::XID)->get_uint64(row);

            bool exists = fields->at(sys_tbl::TableNames::Data::EXISTS)->get_bool(row);
            if (!exists) {
                // find table and compare xids, remove if this xid is >= to the one in the map
                auto entry = table_map.find(table_name);
                if (entry != table_map.end()) {
                    if (xid >= entry->second.second) {
                        // remove this table entry
                        table_map.erase(entry);
                    }
                }
                continue;
            }

            SPDLOG_DEBUG_MODULE(LOG_FDW, "Found table {}.{} tid={}, xid={}", schema_name, table_name, tid, xid);

            // lookup table in map, if found the xid if it is newer
            auto entry = table_map.insert({table_name, {tid, xid}});
            if (entry.second == false) {
                SPDLOG_DEBUG_MODULE(LOG_FDW, "Table {} already exists in schema {}", table_name, schema_name);
                // update if xid is newer
                if (xid > entry.first->second.second) {
                    entry.first->second = {tid, xid};
                }
            }
        }

        // reorganize the table_map to be from tid -> {xid, table}
        std::map<uint64_t, std::tuple<uint64_t, std::string>> tid_map;
        for (const auto &[table_name, table_info] : table_map) {
            tid_map[table_info.first] = {table_info.second, table_name};
        }

        // Move on to iterating through the schemas table

        // column list: name, type, nullable, default
        std::vector<std::tuple<std::string, uint8_t, bool, std::optional<std::string>>> columns;

        uint64_t current_tid=0;
        std::string current_table;

        // get the schemas table
        table = TableMgr::get_instance()->get_table(sys_tbl::Schemas::ID,
                                                    constant::LATEST_XID,
                                                    constant::MAX_LSN);

        auto idx_table = TableMgr::get_instance()->get_table(sys_tbl::Indexes::ID,
                                                            constant::LATEST_XID,
                                                            constant::MAX_LSN);

        auto idx_fields = idx_table->extent_schema()->get_fields();

        // iterate through it
        fields = table->extent_schema()->get_fields();
        for (auto row : (*table)) {
            uint64_t tid = fields->at(sys_tbl::Schemas::Data::TABLE_ID)->get_uint64(row);

            SPDLOG_DEBUG_MODULE(LOG_FDW, "Found table in schemas table: {}", tid);

            // check if we have moved to next tid
            if (tid != current_tid) {

                if (!current_table.empty()) {
                    // dump this table
                    std::string sql = _gen_fdw_table_sql(server, current_table, current_tid, columns);
                    commands = lappend(commands, pstrdup(sql.c_str()));
                }

                // reset state
                columns.clear();
                current_table = "";

                // do lookup of new tid in map
                auto it = tid_map.find(tid);
                if (it == tid_map.end()) {
                    // not found skip it
                    SPDLOG_DEBUG_MODULE(LOG_FDW, "Table {} not found in table map, skipping", tid);
                    continue;
                }

                // update current vars based on this tid and info from tid_map
                current_tid = tid;
                current_table = std::get<1>(it->second);
            }

            bool exists = fields->at(sys_tbl::Schemas::Data::EXISTS)->get_bool(row);
            if (!exists) {
                continue;
            }

            // add column if it exists
            std::string column_name = fields->at(sys_tbl::Schemas::Data::NAME)->get_text(row);
            uint8_t type = fields->at(sys_tbl::Schemas::Data::TYPE)->get_uint8(row);
            bool nullable = fields->at(sys_tbl::Schemas::Data::NULLABLE)->get_bool(row);
            std::optional<std::string> default_value{};

            if (!fields->at(sys_tbl::Schemas::Data::DEFAULT)->is_null(row)) {
                default_value = fields->at(sys_tbl::Schemas::Data::DEFAULT)->get_text(row);
            }

            columns.push_back({column_name, type, nullable, default_value});
        }

        // process last table
        if (columns.size() > 0) {
            // dump this table
            std::string sql = _gen_fdw_table_sql(server, current_table, current_tid, columns);
            commands = lappend(commands, pstrdup(sql.c_str()));
        }

        return commands;
    }

}