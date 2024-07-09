#include <fmt/core.h>

#include <common/common.hh>
#include <common/exception.hh>
#include <common/logging.hh>

#include <pg_fdw/pg_fdw_mgr.hh>

extern "C" {
    #include <postgres.h>
    #include <postgres_ext.h>
    #include <access/htup_details.h>
    #include <catalog/pg_type.h>
    #include <utils/builtins.h>
    #include <utils/syscache.h>
    #include <utils/typcache.h>
    #include <nodes/pg_list.h>
    #include <nodes/primnodes.h>
    #include <varatt.h>
    #include <lib/stringinfo.h>
}

namespace springtail {
namespace pg_fdw {

    PgFdwMgr* PgFdwMgr::_instance {nullptr};

    std::once_flag PgFdwMgr::_init_flag;

    PgFdwMgr*
    PgFdwMgr::_init()
    {
        _instance = new PgFdwMgr();
        return _instance;
    }

    void
    PgFdwMgr::fdw_init(const char *config_file)
    {
        springtail_init(LOG_ALL, config_file);
        SPDLOG_DEBUG_MODULE(LOG_FDW, "Initializing, config file: {}", config_file);
    }

    PgFdwState *
    PgFdwMgr::fdw_create_state(uint64_t tid, uint64_t pg_xid)
    {
        uint64_t xid; // springtail xid

        // lookup pg_xid in xid_map;
        // if doesn't exist, get a new xid from xid_mgr and add to map
        auto it = _xid_map.find(pg_xid);
        if (it == _xid_map.end()) {
            xid = XidMgrClient::get_instance()->get_committed_xid();
            _xid_map[pg_xid] = xid;
        } else {
            xid = it->second;
        }

        SPDLOG_DEBUG_MODULE(LOG_FDW, "fdw_create_state: tid: {}, xid: {}, pg_xid: {}",
                            tid, xid, pg_xid);

        TablePtr table = TableMgr::get_instance()->get_table(tid, xid, constant::MAX_LSN);
        PgFdwState *state = new PgFdwState{table, tid, xid};

        return state;
    }

    void
    PgFdwMgr::fdw_begin_scan(PgFdwState *state, List *target_list, List *qual_list, List *sortgroup)
    {
        SPDLOG_DEBUG_MODULE(LOG_FDW, "fdw_begin_scan: tid: {}", state->tid);
        state->iter.emplace(Table::Iterator(state->table->begin()));
        state->qual_list = qual_list;

        // copy lists into state structure in a more CPP friendly way

        // init target list vector
        ListCell *lc;
        std::vector<std::string> target_colnames;
        int i = 0;
        foreach(lc, target_list) {
            int attno = intVal(lfirst(lc));
            target_colnames.push_back(state->columns[attno].name);
            state->target_columns.insert({attno,i++});
            SPDLOG_DEBUG_MODULE(LOG_FDW, "Target list column: {}:{}",
                                attno, state->columns[attno].name);
        }

        if (target_colnames.empty()) {
            // if no target columns, use all columns
            state->fields = state->table->extent_schema()->get_fields();
        } else {
            // otherwise, use target columns (by name
            state->fields = state->table->extent_schema()->get_fields(target_colnames);
        }

        // init sort group vector
        foreach(lc, sortgroup) {
            DeparsedSortGroup *pathkey = static_cast<DeparsedSortGroup *>(lfirst(lc));
            PgFdwSortGroupPtr pathkey_ptr = std::make_shared<PgFdwSortGroup>(pathkey);
            state->sort_columns.push_back(pathkey_ptr);
            SPDLOG_DEBUG_MODULE(LOG_FDW, "Sort group column: {}:{}", pathkey_ptr->attnum, pathkey_ptr->attname);
        }

        // dump out qual list
        foreach(lc, qual_list) {
            BaseQual *qual = static_cast<BaseQual *>(lfirst(lc));
            SPDLOG_DEBUG_MODULE(LOG_FDW, "Qual: varattno: {}, right_type: {}, typeoid: {}, opname: {}, isArray: {}, useOr: {}",
                                qual->varattno, (int)qual->right_type, qual->typeoid, qual->opname, qual->isArray, qual->useOr);
        }
    }

    void
    PgFdwMgr::fdw_end_scan(PgFdwState *state)
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
    PgFdwMgr::fdw_iterate_scan(PgFdwState *state,
                               int num_attrs,
                               Form_pg_attribute *attrs,
                               Datum *values,
                               bool *nulls)
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

        // iterate through attributes passed in
        for (int i = 0; i < num_attrs; i++) {
            int attno = attrs[i]->attnum;

            // check if this column is in target list, if not skip
            if (!state->target_columns.contains(attno)) {
                nulls[i] = true;
                values[i] = 0;
                SPDLOG_DEBUG_MODULE(LOG_FDW, "Skipping column: {}", attno);
                continue;
            }

            SPDLOG_DEBUG_MODULE(LOG_FDW, "Fetching column: {}", attno);

            // get field idx that matches this attrno, then fetch the field and data
            int field_idx = state->target_columns[attno];
            FieldPtr field = state->fields->at(field_idx);

            // set null
            nulls[i] = field->is_null(row);

            // set value
            if (!nulls[i]) {
                assert (attrs[i]->atttypid == state->columns[attno].pg_type);
                values[i] = _get_datum_from_field(field, row, state->columns[attno].pg_type, attrs[i]->atttypmod);
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
        List       *result = NULL;
        ListCell   *lc;

        PgFdwState *pg_state = static_cast<PgFdwState *>(state->pg_fdw_state);

        SPDLOG_DEBUG_MODULE(LOG_FDW, "fdw_can_sort");

        // XXX only looking at primary keys for now
        // build up a list of pathkeys that match the sortgroup from the primary key
        // in order, stop when no more matches found
        int i = 0;
        foreach(lc, sortgroup) {
            // check if there are any more primary keys
            if (i >= pg_state->pkey_column_ids.size()) {
                break;
            }

            // get the next path key and check if it is next in primary key list
            DeparsedSortGroup *pathkey = static_cast<DeparsedSortGroup *>(lfirst(lc));
            int attnum = pathkey->attnum;

            SPDLOG_DEBUG_MODULE(LOG_FDW, "Checking pathkey attnum: {} against pkey id: {}",
                                attnum, pg_state->pkey_column_ids[i]);

            // check if this attnum matches next id in primary key id list, and sort order matches
            // XXX ignore collation for now
            if (pathkey->nulls_first || pathkey->reversed ||
                attnum != pg_state->pkey_column_ids[i]) {
                SPDLOG_DEBUG_MODULE(LOG_FDW, "Pathkey does not match, or sort order wrong");
                break;
            }

            // add to result
            result = lappend(result, pathkey);

            i++;
        }

        return result;
    }

    List *
    PgFdwMgr::fdw_get_path_keys(SpringtailPlanState *state)
    {
        List      *result = NULL;
        List      *attnums = NULL;
        List      *item = NULL;

        double rows = 1; // number of rows with unique key

        PgFdwState *pg_state = static_cast<PgFdwState *>(state->pg_fdw_state);

        SPDLOG_DEBUG_MODULE(LOG_FDW, "fdw_get_path_keys");

        // generate list of elements, each element is: list of attnums, followed by row count
        // [(('id',),1)]

        // for now only look at primary key
        for (const auto id: pg_state->pkey_column_ids) {
            SPDLOG_DEBUG_MODULE(LOG_FDW, "adding pathkey attnum: {}", id);
            attnums = list_append_unique_int(attnums, id);
        }
        item = lappend(item, attnums);
        item = lappend(item, makeConst(INT4OID,
                                       -1, InvalidOid, 4, rows, false, true));
        result = lappend(result, item);

        return result;
    }

    void
    PgFdwMgr::fdw_get_rel_size(SpringtailPlanState *planstate, List *target_list, List *qual_list, double *rows, int *width)
    {
        // fetch stats from state for row count
        PgFdwState *state = static_cast<PgFdwState *>(planstate->pg_fdw_state);
        *rows = state->stats.row_count;

        // estimate width based on target list using most common types
        ListCell *lc;
        *width = 0;
        foreach(lc, target_list) {
            int attno = intVal(lfirst(lc));
            switch (state->columns[attno].pg_type) {
            case FLOAT8OID:
            case INT8OID:
            case TIMESTAMPOID:
            case TIMESTAMPTZOID:
            case TIMEOID:
                *width += 8;
                break;
            case DATEOID:
            case FLOAT4OID:
            case INT4OID:
                *width += 4;
                break;
            case INT2OID:
                *width += 2;
                break;
            case BOOLOID:
                *width += 1;
                break;
            case VARCHAROID:
            case TEXTOID:
                *width += 16; // estimate
                break;
            default:
                // XXX need to handle binary types
                *width += 8;
                break;
            }
        }
    }

    void
    PgFdwMgr::fdw_commit_rollback(uint64_t pg_xid, bool commit)
    {
        // remove transaction ID mapping on a commit or rollback
        SPDLOG_DEBUG_MODULE(LOG_FDW, "fdw_commit_rollback: pg_xid: {}, commit: {}", pg_xid, commit);
        _xid_map.erase(pg_xid);
    }

    void
    PgFdwMgr::_handle_exception(const Error &error)
    {
        error.log_backtrace();
        SPDLOG_ERROR("Exception: {}", error.what());
        elog(ERROR, "Springtail exception: %s", error.what());
    }

    Datum
    PgFdwMgr::_get_datum_from_field(FieldPtr field,
                                    const Extent::Row &row,
                                    int32_t pg_type,
                                    int32_t atttypmod)
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
            const std::string_view value(field->get_text(row));
            char *duped_str = pnstrdup(value.data(), value.size());
            return CStringGetTextDatum(duped_str);
        }
        case SchemaType::BINARY: {
            // retrieve the type's entry from the pg_type table
            HeapTuple tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(pg_type));
            if (!HeapTupleIsValid(tuple)) {
                elog(ERROR, "FDW: cache lookup failed for type %u", pg_type);
            }

            // get the receive function
            regproc typeinput = ((Form_pg_type) GETSTRUCT(tuple))->typreceive;

            // handle array types by retrieving the subscript element type
            Oid typelem = ((Form_pg_type) GETSTRUCT(tuple))->typelem;
            if (typelem != 0) {
                pg_type = typelem;
            }

            ReleaseSysCache(tuple);

            auto &&value = field->get_binary(row);

            // note: we need to store the data into a StringInfo so that the receive function can
            // unpack it for us
            StringInfoData string;
            initStringInfo(&string);
            appendBinaryStringInfoNT(&string, value.data(), value.size());
            Datum datum = PointerGetDatum(&string);

            // call the recieve function
            Datum value_datum = OidFunctionCall3(typeinput, datum, ObjectIdGetDatum(pg_type), Int32GetDatum(atttypmod));

            return value_datum;
        }

        default:
            return 0;
        }
    }

    std::string
    PgFdwMgr::_gen_fdw_table_sql(const std::string &server_name,
                                 const std::string &table,
                                 uint64_t tid,
                                 std::vector<std::tuple<std::string, int32_t, bool, std::optional<std::string>>> &columns)
    {
        // no schema name needed
        std::string create = fmt::format("CREATE FOREIGN TABLE {} (\n", quote_identifier(table.c_str()));

        // iterate over the columns, adding each to the create statement
        // name, type, is_nullable, default value
        for (int i = 0; i < columns.size(); i++) {
            const auto &[column_name, pg_type, nullable, default_value] = columns[i];
            std::string column = fmt::format("  {} ", quote_identifier(column_name.c_str()));

            // get the type name
            HeapTuple tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(pg_type));
            if (!HeapTupleIsValid(tuple)) {
		elog(ERROR, "cache lookup failed for type%u", pg_type);
            }

            std::string type_name = ((Form_pg_type) GETSTRUCT(tuple))->typname.data;
            ReleaseSysCache(tuple);

            column += type_name;

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
        // column description: name, pg_type, nullable, default
        std::vector<std::tuple<std::string, int32_t, bool, std::optional<std::string>>> columns;

        for (const auto &column : column_schema) {
            columns.push_back({column.name, column.pg_type, column.nullable, column.default_value});
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

        if (!((exclude && table_set.contains(CATALOG_TABLE_STATS)) ||
              (limit && !table_set.contains(CATALOG_TABLE_STATS)))) {
            sql = _gen_fdw_system_table(server, CATALOG_TABLE_STATS, sys_tbl::TableStats::ID, sys_tbl::TableStats::Data::SCHEMA);
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
            std::string schema_name(fields->at(sys_tbl::TableNames::Data::NAMESPACE)->get_text(row));

            // check for schema match
            if (schema_name != schema) {
                continue;
            }

            std::string table_name(fields->at(sys_tbl::TableNames::Data::NAME)->get_text(row));
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
        std::vector<std::tuple<std::string, int32_t, bool, std::optional<std::string>>> columns;

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
            std::string column_name(fields->at(sys_tbl::Schemas::Data::NAME)->get_text(row));
            int32_t pg_type(fields->at(sys_tbl::Schemas::Data::PG_TYPE)->get_int32(row));
            bool nullable = fields->at(sys_tbl::Schemas::Data::NULLABLE)->get_bool(row);
            std::optional<std::string> default_value{};

            if (!fields->at(sys_tbl::Schemas::Data::DEFAULT)->is_null(row)) {
                default_value = fields->at(sys_tbl::Schemas::Data::DEFAULT)->get_text(row);
            }

            columns.push_back({column_name, pg_type, nullable, default_value});
        }

        // process last table
        if (columns.size() > 0) {
            // dump this table
            std::string sql = _gen_fdw_table_sql(server, current_table, current_tid, columns);
            commands = lappend(commands, pstrdup(sql.c_str()));
        }

        return commands;
    }

} // namespace pg_fdw
} // namespace springtail
