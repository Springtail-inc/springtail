#include <common/constants.hh>
#include <stdlib.h>
#include <shared_mutex>

#include <fmt/core.h>
#include <nlohmann/json.hpp>

#include <common/common.hh>
#include <common/exception.hh>
#include <common/logging.hh>
#include <common/properties.hh>
#include <common/redis.hh>
#include <common/json.hh>
#include <common/redis_types.hh>

#include <redis/redis_ddl.hh>

#include <pg_fdw/exception.hh>
#include <pg_fdw/pg_fdw_mgr.hh>

#include <sys_tbl_mgr/client.hh>

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
    #include <libpq-fe.h>
    #include <miscadmin.h>
    #include <postmaster/interrupt.h>
    #include <postmaster/bgworker.h>
    #include <storage/pmsignal.h>
    #include <storage/ipc.h>
    #include <tcop/utility.h>
}

namespace springtail::pg_fdw {
    using springtail::Index;

    // This is to return an intersection between Index columns and quals.
    // The intersection must start at the first index column and be
    // continuous.
    std::vector<ConstQualPtr>
    _get_index_quals(Index const& idx, List const* qual_list) {

        if (!qual_list) {
            return {};
        }

        auto find_qual = [&qual_list](auto pos) -> ConstQualPtr {
            const ListCell *lc{};
            foreach(lc, qual_list) {
                ConstQualPtr qual = static_cast<ConstQualPtr>(lfirst(lc));
                if (PgFdwMgr::_is_type_sortable(qual->base.typeoid, qual->base.op) && 
                        qual->base.isArray == false &&  
                        qual->base.varattno == pos ) {
                    return qual;
                }

            }
            return nullptr;
        };

        std::vector<ConstQualPtr> quals;

        for (auto const& c: idx.columns) {
            auto qual = find_qual(c.position);
            if (find_qual(c.position)) {
                quals.push_back(qual);
            } else {
                break;
            }
        }

        return quals;
    }
}

using namespace springtail;

namespace springtail::pg_fdw {

    PgFdwMgr* PgFdwMgr::_instance {nullptr};

    std::once_flag PgFdwMgr::_init_flag;

    PgFdwMgr*
    PgFdwMgr::_init()
    {
        elog(NOTICE, "Initializing PgFdwMgr");

        // initialize logging
        try {
            springtail_init(PG_FDW_LOG_FILE_PREFIX, std::nullopt, LOG_ALL);
        } catch (const std::exception &e) {
            elog(ERROR, "Error initializing logging: %s", e.what());
        }

        _instance = new PgFdwMgr();

        return _instance;
    }


    /* called from PG_init */
    void
    PgFdwMgr::fdw_init(const char *config_file)
    {
        if (config_file != nullptr) {
            // set env variables based on redis config
            // we don't reload redis config here, just set the env variables
            elog(NOTICE, "Setting properties from file: %s", config_file);
            Properties::set_env_from_file(config_file);
            ::unsetenv("SPRINGTAIL_PROPERTIES_FILE");
        }

        SPDLOG_DEBUG_MODULE(LOG_FDW, "Initializing PgFdwMgr");

        // initialize the singleton
        std::call_once(_init_flag, _init);
    }

    PgFdwState *
    PgFdwMgr::fdw_create_state(uint64_t db_id,
                               uint64_t tid,
                               uint64_t pg_xid,
                               uint64_t schema_xid)
    {
        uint64_t xid; // springtail xid

        // check if the schema_xid has progressed, if so, invalidate the schema cache
        const uint64_t prev_schema_xid = _schema_xid.exchange(schema_xid);
        if (prev_schema_xid < schema_xid) {
            sys_tbl_mgr::Client::get_instance()->invalidate_db(db_id, XidLsn(schema_xid));
        }

        // lookup pg_xid in xid_map;
        // if doesn't exist, get a new xid from xid_mgr and add to map
        std::shared_lock<std::shared_mutex> rd_lock(_mutex);
        auto it = _xid_map.find(pg_xid);
        if (it == _xid_map.end()) {
            rd_lock.unlock();
            // don't hold lock through get call, can only have one operation
            // for this transaction in flight at once
            xid = XidMgrClient::get_instance()->get_committed_xid(db_id, schema_xid);
            std::unique_lock<std::shared_mutex> lock(_mutex);
            _xid_map[pg_xid] = xid;
            lock.unlock();
        } else {
            xid = it->second;
            rd_lock.unlock();
        }

        SPDLOG_DEBUG_MODULE(LOG_FDW, "fdw_create_state: db_id: {}, tid: {}, xid: {}, pg_xid: {}",
                            db_id, tid, xid, pg_xid);

        TablePtr table = TableMgr::get_instance()->get_table(db_id, tid, xid);
        PgFdwState *state = new PgFdwState{table, tid, xid};

        return state;
    }

    void
    PgFdwMgr::fdw_begin_scan(PgFdwState *state, List *target_list, List *qual_list, List *sortgroup)
    {
        SPDLOG_DEBUG_MODULE(LOG_FDW, "fdw_begin_scan: tid: {}", state->tid);

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

        // init quals
        CHECK_EQ(state->filtered_quals.empty(), true);
        _init_quals(state, qual_list);

        // note: it is possible state->filtered_quals is empty if no quals are usable
        // go through qual columns and make sure they are part of the target columns
        i = state->target_columns.size();
        for (int j = 0; j < state->filtered_quals.size(); j++) {
            int attno = state->filtered_quals[j]->base.varattno;
            if (!state->target_columns.contains(attno)) {
                target_colnames.push_back(state->columns[attno].name);
                state->target_columns.insert({attno, i++});
            }
        }

        // set target columns; will contain filtered qual columns as well
        if (target_colnames.empty()) {
            // if no target columns, use all columns
            state->fields = state->table->extent_schema()->get_fields();
        } else {
            // otherwise, use target columns (by name
            state->fields = state->table->extent_schema()->get_fields(target_colnames);
        }

        // set the iterators for the scan taking quals into consideration
        _set_scan_iterators(state);
    }


    FieldTuplePtr
    PgFdwMgr::_gen_qual_tuple(const std::vector<ConstQual*> &quals, const FieldArrayPtr qual_fields)
    {
        // create the field tuple used for bounds, it is based on the number of EQUAL quals
        // the tuple always has at least the first qual field from the primary key
        // if additional fields are EQUALS, they are added to the tuple
        FieldArrayPtr fields = std::make_shared<FieldArray>();
        fields->push_back(qual_fields->at(0));

        // this is an optimzation where multiple keys are compared with EQUALS
        // and it works with both the start/end iterators
        // XXX additional optimization could be added for other types of quals but would
        // be dependent on whether it is for the start or the end iterator, the tuple wouldn't
        // be common for both
        if (quals[0]->base.op == EQUALS) {
            for (int i = 1; i < quals.size(); i++) {
                if (quals[i]->base.op == EQUALS) {
                    fields->push_back(qual_fields->at(i));
                } else {
                    break;
                }
            }
        }

        return std::make_shared<FieldTuple>(fields, nullptr);
    }

    void
    PgFdwMgr::_set_scan_iterators(PgFdwState *state)
    {
        // this will return a pair of iterators based on the conditions.
        // for ASC order, scan from iter_start to iter_end with (iter_start++) 
        // for DESC order, scan from iter_end to iter_end with (iter_end--)
        // make sure to handle the special case for NOT_EQUALS while scanning
        if (!state->index.has_value()) {
            SPDLOG_DEBUG_MODULE(LOG_FDW, "Setting up iterators for full table scan: tid={}, ASC={}",
                    state->tid, state->scan_asc);
            state->iter_start.emplace(state->table->begin());
            state->iter_end.emplace(state->table->end());
            return;
        }

        if (state->filtered_quals.empty()) {
            // Usually the index is defined by sortgroup in this case.
            SPDLOG_DEBUG_MODULE(LOG_FDW, "Setting up iterators for full index scan: tid={}, index={}, ASC={}",
                    state->tid ,state->index->id, state->scan_asc);
            state->iter_start.emplace(state->table->begin(state->index->id));
            state->iter_end.emplace(state->table->end(state->index->id));
            return;
        }

        // setup scan based on first qual
        ConstQual *qual = state->filtered_quals[0];

        // create the field tuple used for bounds
        FieldTuplePtr tuple = _gen_qual_tuple(state->filtered_quals, state->qual_fields);
        QualOpName op = qual->base.op;

        SPDLOG_DEBUG_MODULE(LOG_FDW, "Setting up iterators for qual scan: tid: {}, index: {}, op: {}, fields: {}",
                            state->tid, state->index->id, qual->base.opname, tuple->to_string());

        switch (op) {
            case LESS_THAN:
                state->iter_start.emplace(state->table->begin(state->index->id));
                state->iter_end.emplace(state->table->lower_bound(tuple, state->index->id));
                break;
            case LESS_THAN_EQUALS:
                state->iter_start.emplace(state->table->begin(state->index->id));
                state->iter_end.emplace(state->table->upper_bound(tuple, state->index->id));
                break;
            case NOT_EQUALS: 
                if (state->scan_asc) {
                    state->iter_start.emplace(state->table->begin(state->index->id));
                    state->iter_end.emplace(state->table->lower_bound(tuple, state->index->id));
                } else {
                    state->iter_start.emplace(state->table->upper_bound(tuple, state->index->id));
                    state->iter_end.emplace(state->table->end(state->index->id));
                }
                break;
            case EQUALS:
                state->iter_start.emplace(state->table->lower_bound(tuple, state->index->id));
                state->iter_end.emplace(state->table->upper_bound(tuple, state->index->id));
                break;
            case GREATER_THAN_EQUALS:
                state->iter_start.emplace(state->table->lower_bound(tuple, state->index->id));
                state->iter_end.emplace(state->table->end(state->index->id));
                break;
            case GREATER_THAN:
                state->iter_start.emplace(state->table->upper_bound(tuple, state->index->id));
                state->iter_end.emplace(state->table->end(state->index->id));
                break;
            case UNSUPPORTED: 
                CHECK(false);
                break;
        }
    }

    void
    PgFdwMgr::_init_quals(PgFdwState *state, List *qual_list)
    {
        if (!state->sortgroup_index) {
            // If the sortgroup index isn't set, select the best index to use from quals.
            std::optional<Index> best_index;
            std::vector<ConstQualPtr> best;

            for (auto const& idx: state->indexes) {
                auto index_quals = _get_index_quals(idx, qual_list);
                if (index_quals.empty()) {
                    continue;
                }
                // equal score
                if (index_quals.size() == best.size()) {
                    // pick primary
                    if (idx.id == constant::INDEX_PRIMARY) {
                        best = std::move(index_quals);
                        best_index = idx;
                    }
                } else if (index_quals.size() > best.size()) {
                    best = std::move(index_quals);
                    best_index = idx;
                }
            }
            state->index = std::move(best_index);
            state->filtered_quals = std::move(best);
        } else {
            // Always use the sortgroup index
            state->index = *state->sortgroup_index;

            auto index_quals = _get_index_quals(*state->sortgroup_index, qual_list);
            if (!index_quals.empty()) {
                state->filtered_quals = std::move(index_quals);
            }
        }

        // note: just because we have some quals doesn't mean we can use them
        // so filtered_quals may be empty even if qual_list is not
        if (state->filtered_quals.empty()) {
            return;
        }

        // create the fields and tuple for the iterator
        state->qual_fields = std::make_shared<FieldArray>(state->filtered_quals.size());

        // iterate through the quals and add them to the key fields
        for (int i = 0; i < state->filtered_quals.size(); i++) {
            // create a const field for the qual and add it to the field array
            _make_const_field(state->qual_fields, i, state->filtered_quals[i]);
        }
    }

    void
    PgFdwMgr::fdw_end_scan(PgFdwState *state)
    {
        SPDLOG_DEBUG_MODULE(LOG_FDW, "fdw_end: tid: {}, rows fetched: {}, rows skipped: {}",
                            state->tid, state->rows_fetched, state->rows_skipped);
        delete state;
    }

    void
    PgFdwMgr::fdw_reset_scan(PgFdwState *state)
    {
        SPDLOG_DEBUG_MODULE(LOG_FDW, "fdw_reset_scan: tid: {}", state->tid);
        _set_scan_iterators(state);
    }

    bool
    PgFdwMgr::fdw_iterate_scan(PgFdwState *state,
                               int num_attrs,
                               Form_pg_attribute *attrs,
                               Datum *values,
                               bool *nulls,
                               bool *eos)
    {
        // Note: for now always scan up, so we don't need to check if we are scanning down
        SPDLOG_DEBUG_MODULE(LOG_FDW, "fdw_iterate_scan: tid: {}", state->tid);

        // default to not end of scan
        *eos = false;

        // check iterator is valid
        if (!state->iter_start.has_value()) {
            *eos = true;
            return false;
        }

        // check if we are scanning up and iterator is at the end
        if (*state->iter_start == *state->iter_end) {

            SPDLOG_DEBUG_MODULE(LOG_FDW, "fdw_iterate_scan: iter_start == iter_end, done");
            if (state->filtered_quals.empty() || state->filtered_quals[0]->base.op != NOT_EQUALS) {
                *eos = true;
                return false;
            }

            if (state->scan_asc) {
                // check if we need to switch iterators for not equals
                // we start scanning from begin -> lower-bound, then switch to upper-bound -> end
                if (state->index.has_value() && state->iter_end != state->table->end(state->index->id)) {
                    auto tuple = std::make_shared<FieldTuple>(state->qual_fields, nullptr);
                    state->iter_start.emplace(state->table->upper_bound(tuple, state->index->id));
                    state->iter_end.emplace(state->table->end(state->index->id));
                    return false;
                } else if (!state->index.has_value() && state->iter_end != state->table->end()) {
                    auto tuple = std::make_shared<FieldTuple>(state->qual_fields, nullptr);
                    state->iter_start.emplace(state->table->upper_bound(tuple));
                    state->iter_end.emplace(state->table->end());
                    return false;
                }
            } else {
                // check if we need to switch iterators for not equals
                // we start scanning from end -> upper-bound, then switch to lower-bound -> begin
                if (state->index.has_value() && state->iter_start !=state->table->begin(state->index->id)) {
                    auto tuple = std::make_shared<FieldTuple>(state->qual_fields, nullptr);
                    state->iter_start.emplace(state->table->begin(state->index->id));
                    state->iter_end.emplace(state->table->lower_bound(tuple, state->index->id));
                    return false;
                } else if (!state->index.has_value() && state->iter_start !=state->table->begin() ) {
                    auto tuple = std::make_shared<FieldTuple>(state->qual_fields, nullptr);
                    state->iter_start.emplace(state->table->begin());
                    state->iter_end.emplace(state->table->lower_bound(tuple));
                    return false;
                }
            }

            *eos = true;
            return false;
        }

        if (!state->scan_asc) {
            --(*state->iter_end);
        }

        // get current row
        Extent::Row row{state->scan_asc? *(*state->iter_start) : *(*state->iter_end)};
        state->rows_fetched++;

        // go through the qual fields and see how they compare to the values with in the row
        // if they all match then we can return the row
        if (state->qual_fields != nullptr) {
            for (int i = 0; i < state->filtered_quals.size(); i++) {
                // extract the attrno and field index to find the field in the row
                ConstQual *qual = state->filtered_quals[i];
                int attno = qual->base.varattno;
                assert(state->target_columns.contains(attno));
                int field_idx = state->target_columns[attno];

                // compare the qual field to the field in the row
                bool res = _compare_field(row, state->fields->at(field_idx),
                                          state->qual_fields->at(i), qual->base.op);
                if (res) {
                    continue;
                }

                // qual doesn't match, so this row must be skipped
                // since it isn't the first qual, we can skip to the next row
                SPDLOG_DEBUG_MODULE(LOG_FDW, "Qual not equal, skipping row");
                state->rows_skipped++;
                // increment iterator if scanning up
                if (state->scan_asc) {
                    ++(*state->iter_start);
                }
                return false;
            }
        }

        // iterate through attributes passed in
        for (int i = 0; i < num_attrs; i++) {
            int attno = attrs[i]->attnum;

            // check if this column is in target list, if not skip
            if (!state->target_columns.contains(attno)) {
                nulls[i] = true;
                values[i] = 0;
                SPDLOG_WARN("Skipping column: {}; not found in target column", attno);
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

        // increment iterator if scanning up
        if (state->scan_asc) {
            ++(*state->iter_start);
        }

        return true;
    }

    List *
    PgFdwMgr::fdw_can_sort(SpringtailPlanState *state, List *sortgroup)
    {
        PgFdwState *pg_state = static_cast<PgFdwState *>(state->pg_fdw_state);

        SPDLOG_DEBUG_MODULE(LOG_FDW, "fdw_can_sort");

        // verify that the sort order is the same for all attributes
        // and null_first/reverse combination is supported
        {
            int i = 0;
            ListCell   *lc;
            bool reversed = false;
            foreach(lc, sortgroup) {
                DeparsedSortGroup *pathkey = static_cast<DeparsedSortGroup *>(lfirst(lc));

                if (i == 0) {
                    reversed = pathkey->reversed;
                } else if (reversed != pathkey->reversed) {
                    SPDLOG_DEBUG_MODULE(LOG_FDW, "The sort order must be the same for all attributes: {}!={}", reversed, pathkey->reversed);
                    return {};
                }

                if (!(pathkey->nulls_first? pathkey->reversed: !pathkey->reversed)) {
                    SPDLOG_DEBUG_MODULE(LOG_FDW, "This combination isn't supported: null_first={}, reversed={}", 
                            pathkey->nulls_first, pathkey->reversed);
                    return {};
                }
                ++i;
            }
            // reversed=true means DESC direction in PG executor
            pg_state->scan_asc = (reversed == false);
        }

        auto check_index = [](const Index& idx, const List* sortgroup) -> List* {
            int i = 0;
            ListCell   *lc;
            std::vector<DeparsedSortGroup*> keys;
            foreach(lc, sortgroup) {
                DeparsedSortGroup *pathkey = static_cast<DeparsedSortGroup *>(lfirst(lc));
                int attnum = pathkey->attnum;

                // must match sortgroup completely
                if (i == idx.columns.size() || attnum != idx.columns[i].position) {
                    return {};
                }
                keys.push_back(pathkey);
                i++;
            }

            if (!keys.empty()) {
                SPDLOG_DEBUG_MODULE(LOG_FDW, "Matching sortgroup index found: {}", idx.id);
            }

            List *r = nullptr;
            for (auto pk: keys) {
                r = lappend(r, pk);
            }
            return r;
        };

        // try the primary index first
        auto it = std::find_if(pg_state->indexes.begin(), pg_state->indexes.end(),
                [](auto const& idx) {
                    return idx.id == constant::INDEX_PRIMARY;
                });
        if (it != pg_state->indexes.end()) {
            List* p = check_index(*it, sortgroup);
            if (p) {
                pg_state->sortgroup_index = *it;
                return p;
            }
        }

        for (auto const& idx: pg_state->indexes) {
            if (idx.id == constant::INDEX_PRIMARY) {
                // we already checked the primary index
                continue;
            }
            List* p = check_index(idx, sortgroup);
            if (p) {
                pg_state->sortgroup_index = idx;
                return p;
            }
        }

        return {};
    }

    List *
    PgFdwMgr::fdw_get_path_keys(SpringtailPlanState *state)
    {
        List      *result = NULL;

        PgFdwState *pg_state = static_cast<PgFdwState *>(state->pg_fdw_state);

        SPDLOG_DEBUG_MODULE(LOG_FDW, "fdw_get_path_keys");

        // generate list of elements, each element is: list of attnums, followed by row count
        // [(('id',),1)]

        for (auto const& idx: pg_state->indexes) {
            List      *attnums = NULL;
            List      *item = NULL;
            SPDLOG_DEBUG_MODULE(LOG_FDW, "adding index path: {}", idx.id);
            for (const auto col: idx.columns) {
                SPDLOG_DEBUG_MODULE(LOG_FDW, "adding pathkey attnum: {}", col.position);
                attnums = list_append_unique_int(attnums, col.position);
            }
            item = lappend(item, attnums);
            
            double rows = 1; // number of rows with unique key
            if (!idx.is_unique) {
                rows = 100; //just some number to indicate different cost
            }
            item = lappend(item, makeConst(INT4OID,
                        -1, InvalidOid, 4, rows, false, true));
            result = lappend(result, item);
        }

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
                case CHAROID:
                    *width += 1;
                    break;
                case UUIDOID:
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
    PgFdwMgr::_get_type_name(int32_t pg_type)
    {
        // get the type name from the system cache
        HeapTuple tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(pg_type));
        if (!HeapTupleIsValid(tuple)) {
            elog(ERROR, "cache lookup failed for type%u", pg_type);
            ReleaseSysCache(tuple);

            throw FdwError(fmt::format("Failed to find type name for {}", pg_type));
        }

        std::string type_name = ((Form_pg_type) GETSTRUCT(tuple))->typname.data;
        ReleaseSysCache(tuple);

        return type_name;
    }

    std::string
    PgFdwMgr::_gen_fdw_table_sql(const std::string &server_name,
                                 const std::string &schema,
                                 const std::string &table,
                                 uint64_t tid,
                                 std::vector<std::tuple<std::string, std::string, bool>> &columns)
    {
        // no schema name needed
        std::string create = fmt::format("CREATE FOREIGN TABLE {}.{} (\n",
                                         quote_identifier(schema.c_str()),
                                         quote_identifier(table.c_str()));

        // iterate over the columns, adding each to the create statement
        // name, type, is_nullable, default value
        for (int i = 0; i < columns.size(); i++) {
            const auto &[column_name, type_name, nullable] = columns[i];
            std::string column = fmt::format("  {} ", quote_identifier(column_name.c_str()));

            // set the type name
            column += type_name;

            // add nullability and default
            if (!nullable) {
                column += " NOT NULL";
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
        std::vector<std::tuple<std::string, std::string, bool>> columns;

        for (const auto &column : column_schema) {
            std::string type_name = _get_type_name(column.pg_type);
            columns.push_back({ column.name, type_name, column.nullable });
        }

        return _gen_fdw_table_sql(server, CATALOG_SCHEMA_NAME, table_name, tid, columns);
    }

    List *
    PgFdwMgr::_import_springtail_catalog(const std::string &server,
                                         const std::set<std::string> table_set,
                                         bool exclude, bool limit)
    {
        List        *commands = NIL;
        std::string  sql;

        auto import_catalog = [&]<typename T>(const auto& tab_name) {
            if (!((exclude && table_set.contains(tab_name)) ||
                  (limit && !table_set.contains(tab_name)))) {
                sql = _gen_fdw_system_table(server, tab_name, T::ID, T::Data::SCHEMA);
                commands = lappend(commands, pstrdup(sql.c_str()));
            }
        };

        import_catalog.operator()<sys_tbl::TableNames>(CATALOG_TABLE_NAMES);
        import_catalog.operator()<sys_tbl::TableRoots>(CATALOG_TABLE_ROOTS);
        import_catalog.operator()<sys_tbl::Indexes>(CATALOG_TABLE_INDEXES);
        import_catalog.operator()<sys_tbl::Schemas>(CATALOG_TABLE_SCHEMAS);
        import_catalog.operator()<sys_tbl::TableStats>(CATALOG_TABLE_STATS);
        import_catalog.operator()<sys_tbl::IndexNames>(CATALOG_INDEX_NAMES);

        return commands;
    }

    List *
    PgFdwMgr::fdw_import_foreign_schema(const std::string &server,
                                        const std::string &schema,
                                        const List *table_list,
                                        bool exclude, bool limit,
                                        uint64_t db_id,
                                        const std::string &db_name,
                                        uint64_t schema_xid)
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

        SPDLOG_DEBUG_MODULE(LOG_FDW, "Importing schema: {} <=> {}\n", schema, CATALOG_SCHEMA_NAME);

        // if we are importing the catalog schema, handle it separately
        if (schema == std::string(CATALOG_SCHEMA_NAME)) {
            return _import_springtail_catalog(server, table_set, exclude, limit);
        }

        // get the table names table to iterate over
        auto table = TableMgr::get_instance()->get_table(db_id, sys_tbl::TableNames::ID,
                                                         schema_xid);
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

        // column list: name, type, nullable
        std::vector<std::tuple<std::string, std::string, bool>> columns;

        uint64_t current_tid=0;
        std::string current_table;

        // get the schemas table
        table = TableMgr::get_instance()->get_table(db_id, sys_tbl::Schemas::ID,
                                                    schema_xid);

        auto idx_table = TableMgr::get_instance()->get_table(db_id, sys_tbl::Indexes::ID,
                                                             schema_xid);

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
                    std::string sql = _gen_fdw_table_sql(server, schema, current_table,
                                                         current_tid, columns);
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

            columns.push_back({column_name, _get_type_name(pg_type), nullable});
        }

        // process last table
        if (columns.size() > 0) {
            // dump this table
            std::string sql = _gen_fdw_table_sql(server, schema, current_table, current_tid, columns);
            commands = lappend(commands, pstrdup(sql.c_str()));
        }

        return commands;
    }

    bool
    PgFdwMgr::_is_type_sortable(Oid pg_type, QualOpName op)
    {
        if (op == UNSUPPORTED) {
            return false;
        }

        // these types can be sorted and used by primary key where clauses
        // an entry must exist in _make_const_field for these types
        switch (pg_type) {
            case MONEYOID:
            case INT8OID:
            case INT4OID:
            case INT2OID:
            case FLOAT8OID:
            case FLOAT4OID:
            case DATEOID:
            case TIMESTAMPOID:
            case TIMESTAMPTZOID:
            case TIMEOID:
            case BOOLOID:
            case CHAROID:
            case UUIDOID:
                return true;
            case VARCHAROID:
            case TEXTOID:
                // due to different collations/encodings we only support equality for text
                return (op == EQUALS || op == NOT_EQUALS);
            default:
                return false;
        }
    }

    void
    PgFdwMgr::_make_const_field(FieldArrayPtr fields, int idx, ConstQual *qual)
    {
        // Generate a const field based on type; populate it into fields array
        switch (qual->base.typeoid) {
            case MONEYOID:
            case TIMESTAMPOID:
            case TIMESTAMPTZOID:
            case TIMEOID:
            case INT8OID:
                fields->at(idx) = std::make_shared<ConstTypeField<int64_t>>(DatumGetInt64(qual->value));
                break;
            case DATEOID:
            case INT4OID:
                fields->at(idx) = std::make_shared<ConstTypeField<int32_t>>(DatumGetInt32(qual->value));
                break;
            case INT2OID:
                fields->at(idx) = std::make_shared<ConstTypeField<int16_t>>(DatumGetInt16(qual->value));
                break;
            case FLOAT8OID:
                fields->at(idx) = std::make_shared<ConstTypeField<double>>(DatumGetFloat8(qual->value));
                break;
            case FLOAT4OID:
                fields->at(idx) = std::make_shared<ConstTypeField<float>>(DatumGetFloat4(qual->value));
                break;
            case BOOLOID:
                fields->at(idx) = std::make_shared<ConstTypeField<bool>>(DatumGetBool(qual->value));
                break;
            case CHAROID:
                fields->at(idx) = std::make_shared<ConstTypeField<int8_t>>(DatumGetBool(qual->value));
                break;
            case UUIDOID: {
                std::vector<char> uuid(16);
                memcpy(uuid.data(), DatumGetPointer(qual->value), 16);
                fields->at(idx) = std::make_shared<ConstTypeField<std::vector<char>>>(uuid);
                break;
            }
            case VARCHAROID:
            case TEXTOID: {
                const char *str = TextDatumGetCString(qual->value);
                fields->at(idx) = std::make_shared<ConstTypeField<std::string>>(str);
                break;
            }
            default:
                elog(ERROR, "Unsupported type for constant field: %d", qual->base.typeoid);
                break;
        }
    }

    bool
    PgFdwMgr::_compare_field(const std::any &row,
                             FieldPtr val_field,
                             FieldPtr key_field,
                             QualOpName op)
    {
        // determine how the val field (from the row) compares to the key field from the qual
        switch (op) {
            case EQUALS:
                return key_field->equal(nullptr, val_field, row);
            case NOT_EQUALS:
                return !key_field->equal(nullptr, val_field, row);
            case LESS_THAN:
                return val_field->less_than(row, key_field, nullptr);
            case LESS_THAN_EQUALS:
                return (val_field->less_than(row, key_field, nullptr) ||
                        key_field->equal(nullptr, val_field, row));
            case GREATER_THAN:
                return (!val_field->less_than(row, key_field, nullptr) &&
                        !key_field->equal(nullptr, val_field, row));
            case GREATER_THAN_EQUALS:
                return !val_field->less_than(row, key_field, nullptr);
            default:
                return false;
        }
    }

    PgFdwState::PgFdwState(TablePtr table, uint64_t tid, uint64_t xid)
            : table(table), tid(tid), xid(xid), stats(table->get_stats())
    {
        columns = SchemaMgr::get_instance()->get_columns(table->db(), tid, { xid, constant::MAX_LSN });
        if (tid > constant::MAX_SYSTEM_TABLE_ID) {
            auto &&meta = sys_tbl_mgr::Client::get_instance()->get_schema(table->db(), tid, { xid, constant::MAX_LSN });
            indexes = meta->indexes;
        }
    }
} // namespace springtail::pg_fdw
