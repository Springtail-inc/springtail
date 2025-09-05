#include <algorithm>
#include <stdlib.h>
#include <memory>
#include <shared_mutex>
#include <sstream>

#include <fmt/core.h>
#include <nlohmann/json.hpp>

#include <common/constants.hh>
#include <common/exception.hh>
#include <common/init.hh>
#include <common/json.hh>
#include <common/logging.hh>
#include <common/open_telemetry.hh>
#include <common/properties.hh>
#include <common/redis.hh>
#include <common/redis_types.hh>

#include <redis/redis_ddl.hh>

#include <pg_repl/pg_common.hh>

#include <pg_fdw/exception.hh>
#include <pg_fdw/pg_fdw_mgr.hh>

#include <sys_tbl_mgr/client.hh>
#include <sys_tbl_mgr/shm_cache.hh>
#include <sys_tbl_mgr/system_tables.hh>

//#define SPRINGTAIL_INCLUDE_TIME_TRACES 1
#include <common/time_trace.hh>

extern "C" {
    #include <postgres.h>
    #include <postgres_ext.h>
    #include <access/htup_details.h>
    #include <access/transam.h>
    #include <catalog/pg_type.h>
    #include <catalog/pg_enum.h>
    #include <utils/builtins.h>
    #include <utils/syscache.h>
    #include <utils/typcache.h>
    #include <utils/numeric.h>
    #include <utils/lsyscache.h>
    #include <nodes/pg_list.h>
    #include <nodes/primnodes.h>
    #include <varatt.h>
    #include <lib/stringinfo.h>
    #include <libpq-fe.h>
    #include <libpq/pqformat.h>
    #include <miscadmin.h>
    #include <postmaster/interrupt.h>
    #include <postmaster/bgworker.h>
    #include <storage/pmsignal.h>
    #include <storage/ipc.h>
    #include <tcop/utility.h>
}

namespace springtail::pg_fdw {
    using springtail::Index;

    static std::string
    _get_value_string(const ConstQual& qual)
    {
        if (qual.isnull) {
            return "NULL";
        }

        std::ostringstream ss;

        switch (qual.base.typeoid) {
            case MONEYOID:
            case TIMESTAMPOID:
            case TIMESTAMPTZOID:
            case TIMEOID:
            case INT8OID:
                ss << DatumGetInt64(qual.value);
                break;

            case DATEOID:
            case INT4OID:
                ss << DatumGetInt32(qual.value);
                break;

            case INT2OID:
                ss << DatumGetInt16(qual.value);
                break;
            case FLOAT8OID:
                ss << DatumGetFloat8(qual.value);
                break;
            case FLOAT4OID:
                ss << DatumGetFloat4(qual.value);
                break;
            case BOOLOID:
                ss << DatumGetBool(qual.value);
                break;
            case CHAROID:
                ss << DatumGetBool(qual.value);
                break;
            case UUIDOID: {
                const char* p = reinterpret_cast<const char*>(DatumGetPointer(qual.value));
                for (int i = 0; i != 16; ++i) {
                    ss << p[i];
                }
                ss << "::UUID";
                break;
            }
            case VARCHAROID:
            case TEXTOID: {
                const char *str = TextDatumGetCString(qual.value);
                ss << "'" << str << "'";
                break;
            }
            case NUMERICOID: // DECIMAL(x,y)
                {
                    auto v = DatumGetNumeric(qual.value);
                    ss << numeric_normalize(v);
                    ss << "::NUMERIC";
                }
                break;
            default:
                // handle enum user defined type
                if (qual.base.typeoid >= FirstNormalObjectId) {
                    Oid oid = DatumGetObjectId(qual.value);
                    ss << oid << "::USER";
                    break;
                }
                break;
        }
        return ss.str();
    }

    static const std::map<QualOpName, std::string>&
    _op_symbols()
    {
        static const std::map<QualOpName, std::string> ops =
        {
            {UNSUPPORTED, "~~~"},
            {EQUALS, "="},
            {NOT_EQUALS, "!="},
            {LESS_THAN, "<"},
            {LESS_THAN_EQUALS, "<="},
            {GREATER_THAN, ">"},
            {GREATER_THAN_EQUALS, ">="}
        };
        return ops;
    }

    template<typename From, typename To>
    static bool
    check_roundtrip_conversion(From from, ConstQualPtr qual)
    {
        // check if the value can be converted to the other type
        if (std::is_same_v<From, To>) {
            return true;
        }

        To temp = static_cast<To>(from);
        From temp2 = static_cast<From>(temp);
        if (temp2 != from) {
            // value cannot be represented
            return false;
        }

        // handle conversions for float/double inline
        if constexpr (std::is_same_v<To, float>) {
            qual->base.typeoid = FLOAT4OID;
            qual->value = Float4GetDatum(temp);
        } else if constexpr(std::is_same_v<To, double>) {
            qual->base.typeoid = FLOAT8OID;
            qual->value = Float8GetDatum(temp);
        }

        return true;
    }

    bool
    PgFdwMgr::convert_qual(ConstQualPtr qual, SchemaType from, SchemaType to)
    {
    #ifndef USE_FLOAT8_BYVAL  // used by get datum
        #error "USE_FLOAT8_BYVAL must be defined"
    #endif

        LOG_DEBUG(LOG_FDW, "Converting qual from {} to {}", to_string(from), to_string(to));

        // if switching between int types no need to convert the underlying datum
        if (from == SchemaType::INT8 || from == SchemaType::INT16 ||
            from == SchemaType::INT32 || from == SchemaType::INT64) {

                int64_t value;
                switch (from) {
                    case SchemaType::INT8:
                        value = DatumGetChar(qual->value);
                        break;
                    case SchemaType::INT16:
                        value = DatumGetInt16(qual->value);
                        break;
                    case SchemaType::INT32:
                        value = DatumGetInt32(qual->value);
                        break;
                    case SchemaType::INT64:
                        value = DatumGetInt64(qual->value);
                        break;
                    default:
                        return false;
                }

                switch (to) {
                case SchemaType::INT8:
                    qual->value = Int8GetDatum(static_cast<int8_t>(value));
                    qual->base.typeoid = CHAROID;
                    return true;
                case SchemaType::INT16:
                    qual->value = Int16GetDatum(static_cast<int16_t>(value));
                    qual->base.typeoid = INT2OID;
                    return true;
                case SchemaType::INT32:
                    qual->value = Int32GetDatum(static_cast<int32_t>(value));
                    qual->base.typeoid = INT4OID;
                    return true;
                case SchemaType::INT64:
                    qual->value = Int64GetDatum(static_cast<int64_t>(value));
                    qual->base.typeoid = INT8OID;
                    return true;
                case SchemaType::FLOAT32:
                    return check_roundtrip_conversion<int64_t, float>(value, qual);
                case SchemaType::FLOAT64:
                    return check_roundtrip_conversion<int64_t, double>(value, qual);
                default:
                    break;
            }
        }

        // converting from float to double; should always be safe
        if (from == SchemaType::FLOAT32 && to == SchemaType::FLOAT64) {
            qual->base.typeoid = FLOAT8OID;
            qual->value = Float8GetDatum(DatumGetFloat4(qual->value));
            return true;
        }

        // converting from double to float; check if value can be represented
        if (from == SchemaType::FLOAT64 && to == SchemaType::FLOAT32) {
            auto value = DatumGetFloat8(qual->value);
            return check_roundtrip_conversion<double, float>(value, qual);
        }

        return false;
    }

    bool
    PgFdwMgr::check_type_compatibility(const SchemaColumn &column, ConstQualPtr qual)
    {
        LOG_DEBUG(LOG_FDW, "Checking type compatibility for column {}:{} and qual oid: {}, is_null: {}",
                  column.name, to_string(column.type), qual->base.typeoid, qual->isnull);

        int32_t pg_type = qual->base.typeoid;
        // check if the types are compatible
        if (column.pg_type == pg_type) {
            return true;
        }

        // check if both types are enums
        if (column.pg_type >= constant::FIRST_USER_DEFINED_PG_OID &&
            pg_type >= constant::FIRST_USER_DEFINED_PG_OID) {
            return true;
        }

        // check if one type is a user defined type and the other is not
        if ((column.pg_type >= constant::FIRST_USER_DEFINED_PG_OID &&
            pg_type < constant::FIRST_USER_DEFINED_PG_OID) ||
            (column.pg_type < constant::FIRST_USER_DEFINED_PG_OID &&
            pg_type >= constant::FIRST_USER_DEFINED_PG_OID)) {
            // one is a user defined type and the other is not
            return false;
        }

        // check if they are the same springtail schema type
        // the type category doesn't matter for these checks since enum check is done above
        SchemaType pg_schema_type = convert_pg_type(pg_type, 'N');
        if (column.type == pg_schema_type) {
            if (pg_schema_type == SchemaType::BINARY) {
                return false;
            }
            return true;
        }

        // if the qual is null then it can be converted to any type
        if (qual->isnull) {
            qual->base.typeoid = column.pg_type;
            qual->value = (Datum) 0;
            return true;
        }

        // check if the pg schema type can be upconverted to the column type
        switch (column.type) {
            case SchemaType::INT64:
                if (pg_schema_type == SchemaType::INT16 ||
                    pg_schema_type == SchemaType::INT32 ||
                    pg_schema_type == SchemaType::INT8) {
                    return PgFdwMgr::convert_qual(qual, pg_schema_type, column.type);
                }
                break;
            case SchemaType::INT32:
                if (pg_schema_type == SchemaType::INT16 ||
                    pg_schema_type == SchemaType::INT8) {
                    return PgFdwMgr::convert_qual(qual, pg_schema_type, column.type);
                }
                if (pg_schema_type == SchemaType::INT64) {
                    int64_t value = DatumGetInt64(qual->value);
                    // check if the value is within the range of int32
                    if (value >= INT32_MIN && value <= INT32_MAX) {
                        return PgFdwMgr::convert_qual(qual, pg_schema_type, column.type);
                    }
                }
                break;
            case SchemaType::INT16:
                if (pg_schema_type == SchemaType::INT8) {
                    return convert_qual(qual, pg_schema_type, column.type);
                }
                if (pg_schema_type == SchemaType::INT32 ||
                    pg_schema_type == SchemaType::INT64) {
                    int64_t value = DatumGetInt64(qual->value);
                    // check if the value is within the range of int16
                    if (value >= INT16_MIN && value <= INT16_MAX) {
                        return PgFdwMgr::convert_qual(qual, pg_schema_type, column.type);
                    }
                }
                break;

            case SchemaType::FLOAT32:
            case SchemaType::FLOAT64:
                if (pg_schema_type == SchemaType::FLOAT64 ||
                    pg_schema_type == SchemaType::FLOAT32 ||
                    pg_schema_type == SchemaType::INT64 ||
                    pg_schema_type == SchemaType::INT32 ||
                    pg_schema_type == SchemaType::INT16 ||
                    pg_schema_type == SchemaType::INT8) {
                    return PgFdwMgr::convert_qual(qual, pg_schema_type, column.type);
                }
            default:
                break;
        }

        return false;
    }

    // This is to return an intersection between Index columns and quals.
    // The intersection must start at the first index column and be
    // continuous.
    std::vector<ConstQualPtr>
    _get_index_quals(const PgFdwState *state, Index const& idx, List const* qual_list)
    {
        if (!qual_list) {
            return {};
        }

        auto find_qual = [&state, &qual_list](auto pos) -> ConstQualPtr {
            const ListCell *lc{};
            foreach(lc, qual_list) {
                ConstQualPtr qual = static_cast<ConstQualPtr>(lfirst(lc));

                // check if type is sortable
                if (qual->base.varattno != pos ||
                    qual->base.isArray == true ||
                    qual->base.useOr == true ||
                    !PgFdwMgr::_is_type_sortable(qual->base.typeoid, qual->base.op)) {
                    continue;
                }

                // must be of the same internal type
                auto column = state->columns.at(pos);
                LOG_DEBUG(LOG_FDW, "Checking qual {}:{} against column {} {}:{}, for op {}",
                         qual->base.typeoid, to_string(convert_pg_type(qual->base.typeoid, 'N')),
                         column.name, column.pg_type, to_string(column.type), (int)qual->base.op);
                if (PgFdwMgr::check_type_compatibility(column, qual)) {
                    LOG_DEBUG(LOG_FDW, "Qual match successful");
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

    std::unique_ptr<PgFdwState> _create_scan_state(SpringtailPlanState *planstate, List *qual_list, List* join_quals, double *rows)
    {
        // we create a temporary scan state here because for historical reasons
        // it has some API's need by this function. The state will be deleted
        // when the function exist.
        SpringtailPlanState::TableRef tr = planstate->get_table_ref();

        TablePtr table = TableMgr::get_instance()->get_table(tr.db_id, tr.tid, tr.xid);

        std::unique_ptr<PgFdwState> state = std::make_unique<PgFdwState>(table, tr.db_id, tr.tid, tr.xid);

        // fetch stats from state for row count
        *rows = state->stats.row_count;

        // let's see if we have an unique index in qual_list
        for (auto const& idx: state->indexes) {
            // primary indexes could have no user columns
            DCHECK(idx.columns.size() || idx.id == constant::INDEX_PRIMARY);
            auto index_quals = _get_index_quals(state.get(), idx, qual_list);
            // check for the full match
            if (index_quals.size() == idx.columns.size() &&
                    // ... and all must be EQUALS
                std::ranges::find_if(index_quals, [](const auto& v) {return v->base.op != EQUALS;}) == index_quals.end()) {
                state->qual_indexes.push_back(idx.id);

                if (!idx.is_unique) {
                    if (*rows >= state->stats.row_count) {
                        // We don't know cardinality stats. Just set to a number that
                        // is less than the total rows.
                        *rows = *rows/10;
                        if (*rows == 0) {
                            *rows = 2;
                        }
                    }
                } else {
                    *rows = 1;
                }
            }
        }

        // Now let's see if we have joinable indexes, that are delayed.
        for (auto const& idx: state->indexes) {
            auto index_quals = _get_index_quals(state.get(), idx, join_quals);
            // check for the full match
            if (index_quals.size() == idx.columns.size() &&
                    // ... and all must be EQUALS
                std::ranges::find_if(index_quals, [](const auto& v) {return v->base.op != EQUALS;}) == index_quals.end()) {

                state->join_indexes.push_back(idx.id);
            }
        }

        for (size_t i = 0; i != planstate->count_target_columns(); ++i) {
            auto column = planstate->get_target_column(i);

            auto name_i = state->name_map.find(column.name);
            if (name_i == state->name_map.end()) {
                LOG_WARN("Couldn't find column in name_map: {}", column.name);
                continue;
            }
            state->attr_map.try_emplace(column.attrno, name_i->second);
        }

        return state;
    }


}

using namespace springtail;

namespace springtail::pg_fdw {
    PgFdwMgr::~PgFdwMgr() {
        LOG_INFO("PgFdwMgr delete");
    }

    void
    PgFdwMgr::init(const char *db_name, bool ddl_connection)
    {
        std::string db_name_string(db_name);

        _fdw_id = Properties::get_fdw_id();
        nlohmann::json fdw_config;
        fdw_config = Properties::get_fdw_config(_fdw_id);
        if (fdw_config.contains("db_prefix")) {
            // if the FDW is using a prefix, prepend it
            std::string db_prefix = fdw_config.at("db_prefix").get<std::string>();
            if (db_name_string.starts_with(db_prefix)) {
                db_name_string = db_name_string.substr(db_prefix.length());
            }
        }

        _ddl_connection = ddl_connection;
        _db_id = Properties::get_db_id(db_name_string);

        LOG_INFO("FDW process started for database id: {}, ddl_connection: {}", _db_id, _ddl_connection);

        if (!_ddl_connection) {
            _xid_collector_client.init(_db_id);
        }

        // NOTE: first call to XidMgrClient needs to be done on the main thread to prevent occasional
        //      deadlock during shutdown for short-lived FDW processes.
        (void)XidMgrClient::get_instance();
        start_thread();
        LOG_INFO("FDW process finished initialization");
    }

    void
    PgFdwMgr::_internal_run()
    {
        if (_ddl_connection) {
            return;
        }

        RedisDDL redis_ddl;
        while (!_is_shutting_down()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(THREAD_SLEEP_INTERVAL_MSEC));
            if (_in_transaction) {
                    continue;
            }

            // read latest schema xid from redis
            uint64_t schema_xid = redis_ddl.get_schema_xid(_fdw_id, _db_id);
            std::unique_lock xid_lock(_xid_update_mutex);
            if (schema_xid == 0) {
                schema_xid = _schema_xid;
            } else {
                CHECK(schema_xid >= _schema_xid);
                if (schema_xid > _schema_xid) {
                    _schema_xid = schema_xid;
                    sys_tbl_mgr::Client::get_instance()->invalidate_db(_db_id, XidLsn(schema_xid));
                }
            }

            LOG_DEBUG(LOG_FDW, "Obtaining and sending data to xid collector for schema xid: {}", schema_xid);
            // TODO: running this function from the thread resulted in a crash
            //      find out why this is happening and determine if it should be called
            //      from the thread to begin with. Maybe a better place would be to call
            //      it once from init() function.
            // _try_create_cache();
            uint64_t xid = _update_last_xid(schema_xid);
            if (xid > _last_xid) {
                _last_xid = xid;
                _xid_collector_client.send_data(_db_id, xid);
            }
        }
    }

    uint64_t
    PgFdwMgr::_update_last_xid(uint64_t schema_xid)
    {
        uint64_t xid = XidMgrClient::get_instance()->get_committed_xid(_db_id, schema_xid);
        LOG_DEBUG(LOG_FDW, "XidMgrClient returned xid = {}", xid);

        // TODO: fix _root_cache so that we can acquire correct xid
        /*
        std::optional<uint64_t> cached_xid;
        {
            std::shared_lock<std::shared_mutex> rc_lock(_rc_mutex);
            if (_roots_cache) {
                cached_xid = _roots_cache->get_committed_xid(_db_id);
            }
        }

        uint64_t xid = constant::INVALID_XID;
        if (!cached_xid.has_value() || cached_xid.value() < _schema_xid) {
            xid = XidMgrClient::get_instance()->get_committed_xid(_db_id, schema_xid);
            LOG_DEBUG(LOG_FDW, "XidMgrClient returned xid = {}", xid);
        } else {
            xid = cached_xid.value();
            LOG_DEBUG(LOG_FDW, "Cached xid returned xid = {}", xid);
        }
        */
        return xid;
    }

    // called from the PG exit callback
    void
    PgFdwMgr::fdw_exit()
    {
        LOG_DEBUG(LOG_FDW, "Shutting down springtail");
        springtail_shutdown();
    }

    /* called from PG_init */
    void
    PgFdwMgr::fdw_init(const char *config_file, bool init)
    {
        LOG_DEBUG(LOG_FDW, "Initializing PgFdwMgr");

        if (config_file != nullptr && strlen(config_file) > 0) {
            // set env variables based on redis config
            // we don't reload redis config here, just set the env variables
            elog(INFO, "Setting properties from file: %s", config_file);
            Properties::set_env_from_file(config_file);
            ::unsetenv("SPRINGTAIL_PROPERTIES_FILE");
        }

        if (init) {
            springtail_init(false, PG_FDW_LOG_FILE_PREFIX, LOG_FDW);
        }
    }

    void
    PgFdwMgr::_try_create_cache()
    {
        std::unique_lock<std::shared_mutex> lock(_rc_mutex);
        if (_roots_cache && _roots_cache->is_alive()) {
            return;
        }
        try {
            auto cache = std::make_shared<sys_tbl_mgr::ShmCache>(sys_tbl_mgr::SHM_CACHE_ROOTS);
            if (cache) {
                _roots_cache = cache;
                // start using the new cache
                sys_tbl_mgr::Client::get_instance()->use_roots_cache(_roots_cache);
            } else {
                // If (!cache) continue with the existing cache anyway.
                // It'll still work as a cache but without
                // the advantages of push notifications.
                // If xid_subscriber comes online, we'll try to
                // open the new (live) IPC cache the next time we come here.
                LOG_WARN("The IPC roots cache is dead.");
            }
        } catch (const boost::interprocess::bad_alloc&) {
            // the cache hasn't been created
            // this could happen if xid_mgr_subscriber isn't running
            LOG_ERROR("fdw_create_state unable to open the roots cache");
        } catch (const std::exception& e) {
            LOG_ERROR("fdw_create_state exception:{} ", e.what());
            throw;
        }
    }

    List*
    PgFdwMgr::fdw_create_state(uint64_t db_id,
                               uint64_t tid,
                               uint64_t pg_xid,
                               uint64_t schema_xid)
    {
        _in_transaction = true;
        DCHECK(db_id == _db_id);
        uint64_t xid; // springtail xid

        // try to use the cache
        _try_create_cache();

        std::unique_lock xid_lock(_xid_update_mutex);

        // check if the schema_xid has progressed, if so, invalidate the schema cache
        DCHECK(schema_xid >= _schema_xid);
        if (schema_xid > _schema_xid) {
            _schema_xid = schema_xid;
            sys_tbl_mgr::Client::get_instance()->invalidate_db(db_id, XidLsn(schema_xid));
        }

        // lookup pg_xid in xid_map;
        // if doesn't exist, get a new xid from xid_mgr and add to map
        std::shared_lock<std::shared_mutex> rd_lock(_mutex);
        if (_trans_pg_xid != pg_xid) {
            rd_lock.unlock();

            xid = _update_last_xid(schema_xid);

            std::unique_lock<std::shared_mutex> lock(_mutex);
            _trans_pg_xid = pg_xid;
            _trans_xid = xid;
        } else {
            xid = _trans_xid;
            rd_lock.unlock();
        }

        if (!_ddl_connection) {
            while (xid < _last_xid) {
                LOG_DEBUG(LOG_FDW, "Trying to get valid xid, current xid = {}, _last_xid = {}", xid, _last_xid);
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                xid = _update_last_xid(schema_xid);
            }
            if (xid > _last_xid) {
                _last_xid = xid;
                _xid_collector_client.send_data(db_id, xid);
            }
        }

        xid_lock.unlock();

        LOG_DEBUG(LOG_FDW, "fdw_create_state: db_id: {}, tid: {}, xid: {}, pg_xid: {}, schema_xid: {}",
                            db_id, tid, xid, pg_xid, schema_xid);

        SpringtailPlanState ps{db_id, tid, xid};
        return ps.fdw_private();
    }

    PgFdwState* 
    PgFdwMgr::create_scan_state(SpringtailPlanState *planstate, List* quals, List* join_quals)
    {
        double dummy = 0; 
        auto state = _create_scan_state(planstate, quals, join_quals, &dummy);
        return state.release();
    }

    PgFdwState*
    PgFdwMgr::fdw_begin_scan_x(SpringtailPlanState *planstate,
            int num_attrs,
            const Form_pg_attribute* attrs,
            List *quals)
    {
        // we create a temporary scan state here because for historical reasons
        // it has some API's need by this function. The state will be deleted
        // when the function exist.
        double dummy = 0; 
        auto state = _create_scan_state(planstate, quals, NIL, &dummy);
        LOG_DEBUG(LOG_FDW, "fdw_begin_scan: tid: {}, {}", state->tid, num_attrs);

        for (size_t i = 0; i != num_attrs; ++i) {
            state->_attrs.emplace_back(attrs[i]->atttypid, attrs[i]->atttypmod, attrs[i]->attnum);
        }
        state->scan_asc = planstate->is_scan_asc();

        // do we have sort index
        auto sort_index_id = planstate->get_sort_index();
        if (sort_index_id.has_value()) {
            auto it = std::ranges::find_if(state->indexes, [sort_index_id](const auto& v) {return v.id == *sort_index_id;});
            CHECK(it != state->indexes.end());
            state->sortgroup_index = *it;
        }


        // copy lists into state structure in a more CPP friendly way

        // init target list vector
        std::vector<std::string> target_colnames;

        // reset quals
        _init_quals(state.get(), quals);

        int col_ind = 0;
        // we add filters at the beginning so that iterate_scan checks them first
        // note: it is possible state->filtered_quals is empty if no quals are usable
        // go through qual columns and make sure they are part of the target columns
        for (int j = 0; j < state->filtered_quals.size(); j++) {
            int attno = state->filtered_quals[j]->base.varattno;

            PgFdwState::TargetColumn::Filter filter{state->filtered_quals[j]->base.op,
                state->qual_fields->at(j)};

            auto column = state->columns.at(state->attr_map.at(attno));
            target_colnames.push_back(column.name);
            state->target_columns.emplace_back(
                   col_ind++, column.pg_type, state->_attrs[attno-1], column.name, std::move(filter));
        }

        col_ind = state->target_columns.size();
        for (size_t i = 0; i != planstate->count_target_columns(); ++i) {
            // note: This is the attnum from the FDW's representation of the external table, not the
            //       column ID on the primary.  We need to map the local attnum to the primary's column ID.
            auto column = planstate->get_target_column(i);
            int attno = column.attrno;

            auto it = std::ranges::find_if(state->target_columns,
                    [attno](auto v) {return v == attno;},
                    [](const auto& c) {return c.pg_attr.attnum;}
            );

            if (it != state->target_columns.end()) {
                continue;
            }

            auto col_i = state->columns.find(state->attr_map.at(attno));
            if (col_i == state->columns.end()) {
                LOG_WARN("Couldn't find FDW attribute number: {}", attno);
                continue;
            }

            target_colnames.push_back(col_i->second.name);
            DCHECK_GT(attno, 0);
            DCHECK_LE(attno, state->_attrs.size());

            state->target_columns.emplace_back(
                    col_ind++, col_i->second.pg_type, state->_attrs[attno-1], col_i->second.name);

            LOG_DEBUG(LOG_FDW, "Target list column: {}:{}",
                                attno, col_i->second.name);
        }

        // set target columns; will contain filtered qual columns as well
        if (!target_colnames.empty() && state->index.has_value() && state->index->id != constant::INDEX_PRIMARY) {
            // check if all target columns are part of the index
            auto index_colnames = state->table->get_index_column_names(state->index->id);
            if (std::ranges::all_of(target_colnames, [&index_colnames](const auto& n) ->bool {
                        return std::ranges::find(index_colnames, n) != index_colnames.end();
                        })) {
                auto ind_schema = state->table->get_index_schema(state->index->id);
                state->fields = ind_schema->get_fields(target_colnames);
                state->index_only_scan  = true;
            }
        }

        if (!state->fields) {
            state->fields = state->table->extent_schema()->get_fields(target_colnames);
        }

        // set the iterators for the scan taking quals into consideration
        _set_scan_iterators(state.get());

        // reset the user type cache since cached types may have changed
        _user_type_cache.clear();

        // reset the timer stats
        time_trace::traces.reset();
        return state.release();
    }

    FieldTuplePtr
    PgFdwMgr::_gen_qual_tuple(const std::vector<ConstQualPtr> &quals, const FieldArrayPtr qual_fields)
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
            LOG_DEBUG(LOG_FDW, "Setting up iterators for full table scan: tid={}, ASC={}, quals={}",
                    state->tid, state->scan_asc, state->filtered_quals.size());
            state->iter_start.emplace(state->table->begin());
            state->iter_end.emplace(state->table->end());
            return;
        }

        if (state->filtered_quals.empty()) {
            // Usually the index is defined by sortgroup in this case.
            LOG_DEBUG(LOG_FDW, "Setting up iterators for full index scan: tid={}, index={}, ASC={}",
                    state->tid ,state->index->id, state->scan_asc);
            state->iter_start.emplace(state->table->begin(state->index->id, state->index_only_scan));
            state->iter_end.emplace(state->table->end(state->index->id, state->index_only_scan));
            return;
        }

        // setup scan based on first qual
        ConstQual *qual = state->filtered_quals[0];

        // create the field tuple used for bounds
        FieldTuplePtr tuple = _gen_qual_tuple(state->filtered_quals, state->qual_fields);
        QualOpName op = qual->base.op;

        LOG_DEBUG(LOG_FDW, "Setting up iterators for qual scan: tid: {}, index: {}, op: {}, fields: {}, index cols: {}",
                            state->tid, state->index->id, qual->base.opname, tuple->to_string(), state->index_only_scan);

        switch (op) {
            case LESS_THAN:
                state->iter_start.emplace(state->table->begin(state->index->id, state->index_only_scan));
                state->iter_end.emplace(state->table->lower_bound(tuple, state->index->id, state->index_only_scan));
                break;
            case LESS_THAN_EQUALS:
                state->iter_start.emplace(state->table->begin(state->index->id, state->index_only_scan));
                state->iter_end.emplace(state->table->upper_bound(tuple, state->index->id, state->index_only_scan));
                break;
            case NOT_EQUALS:
                if (state->scan_asc) {
                    state->iter_start.emplace(state->table->begin(state->index->id, state->index_only_scan));
                    state->iter_end.emplace(state->table->lower_bound(tuple, state->index->id, state->index_only_scan));
                } else {
                    state->iter_start.emplace(state->table->upper_bound(tuple, state->index->id, state->index_only_scan));
                    state->iter_end.emplace(state->table->end(state->index->id, state->index_only_scan));
                }
                break;
            case EQUALS:
                state->iter_start.emplace(state->table->lower_bound(tuple, state->index->id, state->index_only_scan));
                state->iter_end.emplace(state->table->upper_bound(tuple, state->index->id, state->index_only_scan));
                break;
            case GREATER_THAN_EQUALS:
                state->iter_start.emplace(state->table->lower_bound(tuple, state->index->id, state->index_only_scan));
                state->iter_end.emplace(state->table->end(state->index->id, state->index_only_scan));
                break;
            case GREATER_THAN:
                state->iter_start.emplace(state->table->upper_bound(tuple, state->index->id, state->index_only_scan));
                state->iter_end.emplace(state->table->end(state->index->id, state->index_only_scan));
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
                CHECK(static_cast<sys_tbl::IndexNames::State>(idx.state) == sys_tbl::IndexNames::State::READY);
                auto index_quals = _get_index_quals(state, idx, qual_list);
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
            state->index = *state->sortgroup_index;
            auto index_quals = _get_index_quals(state, *state->sortgroup_index, qual_list);
            state->filtered_quals = std::move(index_quals);
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
            ConstQual *qual = state->filtered_quals[i];
            auto &column = state->columns.at(state->attr_map.at(qual->base.varattno));

            _make_const_field(state, column, i, state->filtered_quals[i]);
        }
    }

    void
    PgFdwMgr::fdw_end_scan(PgFdwState *state)
    {
#ifdef SPRINGTAIL_INCLUDE_TIME_TRACES
        LOG_WARN("{}", time_trace::traces.format());
        time_trace::traces.reset();
#endif

        LOG_DEBUG(LOG_FDW, "fdw_end: tid: {}, rows fetched: {}, rows skipped: {}",
                            state->tid, state->rows_fetched, state->rows_skipped);
        delete state;
    }

    void
    PgFdwMgr::fdw_reset_scan(PgFdwState *state, List *qual_list)
    {
        LOG_DEBUG(LOG_FDW, "fdw_reset_scan: tid: {}", state->tid);

        state->filtered_quals.clear();

        // init quals
        _init_quals(state, qual_list);

        int i = 0;
        for (int j = 0; j < state->filtered_quals.size(); ++j) {
            int attno = state->filtered_quals[j]->base.varattno;

            auto it = std::ranges::find_if( state->target_columns,
                    [attno, state](const PgFdwState::PgAttr& v) {return v == state->_attrs[attno-1];},
                    [](const auto& c) {return c.pg_attr;} );

            if (it == state->target_columns.end()) {
                DCHECK(false); //fail in debug to investigate
                LOG_WARN("fdw_reset_scan unknown qual: {} - {}", state->tid, attno);
                continue;
            }

            PgFdwState::TargetColumn::Filter filter{state->filtered_quals[i]->base.op,
                state->qual_fields->at(j)};

            it->filter = std::move(filter);
        }

        // set the iterators for the scan taking quals into consideration
        _set_scan_iterators(state);
    }

    bool
    PgFdwMgr::fdw_iterate_scan(PgFdwState *state,
                               Datum *values,
                               bool *nulls,
                               bool *eos)
    {
        TIME_TRACE_SCOPED(time_trace::traces, iterate_scan_total);

        // Note: for now always scan up, so we don't need to check if we are scanning down
        LOG_DEBUG(LOG_FDW, "fdw_iterate_scan: tid: {}", state->tid);

        size_t num_attrs = state->_attrs.size();

        // default to not end of scan
        *eos = false;

        // check iterator is valid
        if (!state->iter_start.has_value()) {
            *eos = true;
            return false;
        }

        // check if we are scanning up and iterator is at the end
        if (*state->iter_start == *state->iter_end) {

            LOG_DEBUG(LOG_FDW, "fdw_iterate_scan: iter_start == iter_end, done");
            if (state->filtered_quals.empty() || state->filtered_quals[0]->base.op != NOT_EQUALS) {
                *eos = true;
                return false;
            }

            if (state->scan_asc) {
                // check if we need to switch iterators for not equals
                // we start scanning from begin -> lower-bound, then switch to upper-bound -> end
                if (state->index.has_value() && state->iter_end != state->table->end(state->index->id, state->index_only_scan)) {
                    auto tuple = std::make_shared<FieldTuple>(state->qual_fields, nullptr);
                    state->iter_start.emplace(state->table->upper_bound(tuple, state->index->id, state->index_only_scan));
                    state->iter_end.emplace(state->table->end(state->index->id, state->index_only_scan));
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
                if (state->index.has_value() && state->iter_start !=state->table->begin(state->index->id, state->index_only_scan)) {
                    auto tuple = std::make_shared<FieldTuple>(state->qual_fields, nullptr);
                    state->iter_start.emplace(state->table->begin(state->index->id, state->index_only_scan));
                    state->iter_end.emplace(state->table->lower_bound(tuple, state->index->id, state->index_only_scan));
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
        const Extent::Row& row{state->scan_asc? *(*state->iter_start) : *(*state->iter_end)};
        state->rows_fetched++;

        memset(nulls, true, num_attrs * sizeof(bool));
        memset(values, 0, num_attrs * sizeof(values[0]));

        // iterate through attributes passed in
        for (const auto& c: state->target_columns) {
            auto attno = c.pg_attr.attnum;
            LOG_DEBUG(LOG_FDW, "Fetching column: {}, {}", attno, c.name);

            DCHECK_LE(attno, num_attrs);


            // get field idx that matches this attrno, then fetch the field and data
            const FieldPtr& field = state->fields->at(c.field_idx);

            // check filter
            if (c.filter.has_value()) {
                TIME_TRACE_SCOPED(time_trace::traces, iterate_scan_compare);
                // compare the qual field to the field in the row
                bool res = _compare_field(&row, field, c.filter->field, c.filter->op);
                if (!res) {
                    // qual doesn't match, so this row must be skipped
                    // since it isn't the first qual, we can skip to the next row
                    LOG_DEBUG(LOG_FDW, "Qual not equal, skipping row");
                    state->rows_skipped++;
                    // increment iterator if scanning up
                    if (state->scan_asc) {
                        ++(*state->iter_start);
                    }
                    return false;
                }
            }

            {
                TIME_TRACE_SCOPED(time_trace::traces, iterate_scan_datum);
                // set value
                if (!field->is_null(&row)) {
                    values[attno-1] = _get_datum_from_field(state, field.get(), row, c.sp_pg_type, c.pg_attr.atttypid, c.pg_attr.atttypmod);
                    nulls[attno-1] = false;
                }
            }
        }

        // increment iterator if scanning up
        {
            TIME_TRACE_SCOPED(time_trace::traces, iterate_scan_next);
            if (state->scan_asc) {
                ++(*state->iter_start);
            }
        }

        return true;
    }

    List *
    PgFdwMgr::fdw_can_sort(SpringtailPlanState* planstate, PgFdwState* pg_state, List *sortgroup, List* quals, bool use_secondary)
    {
        LOG_DEBUG(LOG_FDW, "fdw_can_sort");

        struct FinalizePlanState
        {
            SpringtailPlanState* _planstate;
            PgFdwState* _pg_state;

            FinalizePlanState(SpringtailPlanState* planstate, PgFdwState* pg_state)
                :_planstate{planstate}, _pg_state{pg_state}
            {}
            ~FinalizePlanState()
            {
                _planstate->set_scan_direction(_pg_state->scan_asc);
                if (_pg_state->sortgroup_index.has_value()) {
                    _planstate->set_sort_index(_pg_state->sortgroup_index->id);
                }
            }
        };

        FinalizePlanState fp{planstate, pg_state};

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
                    LOG_DEBUG(LOG_FDW, "The sort order must be the same for all attributes: {}!={}", reversed, pathkey->reversed);
                    return {};
                }

                if (!(pathkey->nulls_first? pathkey->reversed: !pathkey->reversed)) {
                    LOG_DEBUG(LOG_FDW, "This combination isn't supported: null_first={}, reversed={}",
                            pathkey->nulls_first, pathkey->reversed);
                    return {};
                }
                ++i;
            }
            // reversed=true means DESC direction in PG executor
            pg_state->scan_asc = (reversed == false);
        }

        auto check_index = [pg_state](const Index& idx, const List* sortgroup) -> List* {
            int i = 0;
            ListCell *lc;
            std::vector<DeparsedSortGroup*> keys;
            foreach(lc, sortgroup) {
                DeparsedSortGroup *pathkey = static_cast<DeparsedSortGroup *>(lfirst(lc));

                // must match sortgroup completely
                if (i == idx.columns.size() || pathkey->attnum != idx.columns[i].position) {
                    return {};
                }

                CHECK(idx.columns[i].position > 0);
                if (!_is_type_sortable(pg_state->columns.at(idx.columns[i].position).pg_type, LESS_THAN)) {
                    return {};
                }

                keys.push_back(pathkey);
                i++;
            }

            if (!keys.empty()) {
                LOG_DEBUG(LOG_FDW, "Matching sortgroup index found: {}", idx.id);
            }

            List *r = nullptr;
            for (auto pk: keys) {
                r = lappend(r, pk);
            }
            return r;
        };

        // we'll use the qual indexes to figure out if we should reply
        // with a sort index. We prioritize qual indexes.
        CHECK_EQ(pg_state->filtered_quals.empty(), true);
        _init_quals(pg_state, quals);

        // we have a qual index
        if (!pg_state->filtered_quals.empty()) {
            CHECK(pg_state->index.has_value());
            List* p = check_index(pg_state->index.value(), sortgroup);
            if (p) {
                // we can use the same index for sorting
                pg_state->sortgroup_index = pg_state->index;
                return p;
            }
            // don't do sort push down
            return {};
        }

        // no qual indexes found

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

        List* sort_list = nullptr;

        for (auto const& idx: pg_state->indexes) {
            if (idx.id == constant::INDEX_PRIMARY) {
                // we already checked the primary index
                continue;
            }
            sort_list = check_index(idx, sortgroup);
            if (sort_list) {
                pg_state->sortgroup_index = idx;
                break;
            }
        }

        if (!sort_list) {
            return {};
        }

        // We don't use secondary indexes for full table scans by default.
        // Change the default (use_secondary = true) in the function signature
        // if you need to enable secondary index scans.
        if (use_secondary && sort_list) {
            return sort_list;
        }

        CHECK(sort_list);

        // let's see if the target columns are part of the found sort index
        // in what case we'll sort by the secondary index because
        // the target values are part of the index itself and should be faster
        // then scan by primary plus merge sort
        for (size_t i = 0; i != planstate->count_target_columns(); ++i) {
            auto column = planstate->get_target_column(i);
            // see if the target colum is in the sort index
            auto it = std::ranges::find_if(pg_state->sortgroup_index->columns,
                    [&column](const auto& v) {return v.position == column.attrno;});

            if (it == pg_state->sortgroup_index->columns.end()) {
                pg_state->sortgroup_index = {};
                return {};
            }
        }

        return sort_list;
    }

    List *
    PgFdwMgr::fdw_get_path_keys_x(SpringtailPlanState *planstate, PgFdwState* state)
    {
        List* result = NULL;
        uint64_t rel_rows = planstate->get_rel_rows();

        for (auto const& idx: state->indexes) {
            // note: state->rows has taken quals into account in fdw_get_rel_size
            auto rows = rel_rows;

            // The paths related to join clauses seem to be handled by PG 
            // differently. For example, if there are no normal quals,
            // it seems to be much better to give PG the full count of rows
            // from fdw_get_rel_size and then create a low cost path for the join index.
            // TODO: consider removing paths that are not present in baserel completely
            // from fdw_get_path_keys.
            //
            // Only check the index to be join_indexes if it isn't already in quals
            if (std::ranges::find(state->qual_indexes, idx.id) == state->qual_indexes.end() &&
                std::ranges::find(state->join_indexes, idx.id) != state->join_indexes.end()) {
                if (idx.is_unique) {
                    rows = 1;
                } else {
                    rows /= 100;
                    if (!rows) {
                        rows = 2;
                    }
                }
            }

            List      *attnums = NULL;
            List      *item = NULL;
            LOG_DEBUG(LOG_FDW, "adding index path: {}", idx.id);
            for (const auto col: idx.columns) {
                LOG_DEBUG(LOG_FDW, "adding pathkey attnum: {}", col.position);
                attnums = list_append_unique_int(attnums, col.position);
            }
            item = lappend(item, attnums);

            // cost multiplier
            auto cost = SPRINGTAIL_PRIMARY_COST;
            if (idx.id != constant::INDEX_PRIMARY) {
                if (idx.is_unique) {
                    cost = SPRINGTAIL_SECONDARY_LOOKUP_COST;
                } else {
                    cost = SPRINGTAIL_SECONDARY_SCAN_COST;
                }
            }

            item = lappend(item, makeConst(INT8OID,
                        -1, InvalidOid, 8, rows, false, true));
            item = lappend(item, makeConst(INT4OID,
                        -1, InvalidOid, 4, cost, false, true));
            result = lappend(result, item);
        }

        return result;
    }

    void PgFdwMgr::fdw_get_rel_size_x(SpringtailPlanState *planstate, List *qual_list, List* join_quals, double *rows, int *width)
    {
        // TODO: we create a temporary scan state here because for historical reasons
        // it has some API's need by this function. The state will be deleted
        // when the function exist.
        // 
        auto state = _create_scan_state(planstate, qual_list, join_quals, rows);
        // estimate width based on target list using most common types
        *width = 0;
        for (size_t i = 0; i != planstate->count_target_columns(); ++i) {
            auto column = planstate->get_target_column(i);

            auto name_i = state->name_map.find(column.name);
            if (name_i == state->name_map.end()) {
                LOG_WARN("Couldn't find column: {}", column.name);
                continue;
            }

            auto col_i = state->columns.find(name_i->second);
            if (col_i == state->columns.end()) {
                LOG_ERROR("Couldn't find column position: {}", name_i->second);
                continue;
            }

            switch (col_i->second.pg_type) {
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
        //planstate->set_rel_size(static_cast<uint64_t>(*rows), static_cast<uint64_t>(*width));
        planstate->set_rel_size(static_cast<uint64_t>(*rows), static_cast<uint64_t>(*width));
    }

    void
    PgFdwMgr::fdw_commit_rollback(uint64_t pg_xid, bool commit)
    {
        // remove transaction ID mapping on a commit or rollback
        LOG_DEBUG(LOG_FDW, "fdw_commit_rollback: pg_xid: {}, commit: {}", pg_xid, commit);
        _in_transaction = false;
        std::unique_lock<std::shared_mutex> lock(_mutex);
        _trans_pg_xid = 0;
        _trans_xid = 0;
    }

    std::vector<std::pair<std::string, std::string>>
    PgFdwMgr::fdw_explain_scan(const PgFdwState *state)
    {
        std::vector<std::pair<std::string, std::string>> r;
        r.emplace_back("FDW name", "springtail");

        std::ostringstream ss;

        // collect target columns
        for (const auto& c: state->target_columns) {
            (ss.tellp()? ss << ", ": ss) << c.name;

        }

        if (!ss.str().empty()) {
            r.emplace_back("   Targets", ss.str());
        }

        // collect indexes
        ss.str("");
        for (const auto& idx: state->indexes) {
            if (ss.tellp())
                ss << ", ";
            ss << idx.name;
            ss << "[unique:";
            if (idx.is_unique) {
                ss << "true";
            } else {
                ss << "false";
            }
            ss << "]";
        }
        if (!ss.str().empty()) {
            r.emplace_back("   All indexes", ss.str());
        }

        // sortgroup index
        if (state->sortgroup_index) {
            r.emplace_back("   Sort index", state->sortgroup_index->name);
        }

        // scan index
        if (state->index) {
            r.emplace_back("   Scan index", state->index->name);
            if (state->index_only_scan) {
                r.emplace_back("   Scan type", "index only");
            }
        }

        // collect quals
        ss.str("");

        for (const ConstQual *qual: state->filtered_quals) {
            const auto& column = state->columns.at(state->attr_map.at(qual->base.varattno));
            (ss.tellp()? ss << ", ": ss) <<
                std::format("({} {} {})", column.name, _op_symbols().at(qual->base.op), _get_value_string(*qual));
        }

        if (!ss.str().empty()) {
            r.emplace_back("   Filters", ss.str());
        }

        return r;
    }

    void
    PgFdwMgr::_handle_exception(const Error &error)
    {
        error.log_backtrace();
        LOG_ERROR("Exception: {}", error.what());
        elog(ERROR, "Springtail exception: %s", error.what());
    }

    float
    PgFdwMgr::_get_enum_id_from_pg(const PgFdwState *state,
                                   int32_t springtail_oid,
                                   Oid pg_oid,
                                   Oid label_oid)
    {
        // retrieve the type's entry from the pg_enum table
        HeapTuple tup = SearchSysCache1(ENUMOID, ObjectIdGetDatum(label_oid));
        if (!HeapTupleIsValid(tup)) {
            elog(ERROR, "FDW: cache lookup failed for enum %u", label_oid);
        }

        // get the enum label
        char *label = ((Form_pg_enum) GETSTRUCT(tup))->enumlabel.data;
        ReleaseSysCache(tup);

        // lookup the label in springtail enum cache
        auto label_str = std::string(label);
        UserTypePtr utp = _enum_cache_lookup(state->db_id, springtail_oid, state->xid);
        CHECK_NE(utp, nullptr);

        // lookup label and get the index
        auto &&it = utp->enum_label_map.find(label_str);
        if (it == utp->enum_label_map.end()) {
            LOG_ERROR("Label {} not found for enum oid: {}", label_str, springtail_oid);
            CHECK(false);
        }

        return it->second;
    }

    Datum
    PgFdwMgr::_get_enum_datum(const PgFdwState *state,
                              int32_t springtail_oid,
                              Oid pg_oid,
                              float sort_order)
    {
        UserTypePtr utp = _enum_cache_lookup(state->db_id, springtail_oid, state->xid);
        CHECK_NE(utp, nullptr);

        // lookup the index and get the string label
        auto &&it = utp->enum_index_map.find(sort_order);
        if (it == utp->enum_index_map.end()) {
            LOG_ERROR("Index {} not found for enum oid: {}", sort_order, springtail_oid);
            CHECK(false);
        }

        // convert label to datum for lookup
        auto label = it->second;
        char *name = pnstrdup(label.data(), label.size());
        auto tup = SearchSysCache2(ENUMTYPOIDNAME,
            ObjectIdGetDatum(pg_oid),
            CStringGetDatum(name));

        if (!HeapTupleIsValid(tup)) {
            elog(ERROR, "FDW: cache lookup failed for enum %u", pg_oid);
        }

        // This comes from pg_enum.oid and stores system oids in user tables.
        Oid enum_oid = ((Form_pg_enum) GETSTRUCT(tup))->oid;

        ReleaseSysCache(tup);

        // create a datum from the enum's entry oid
        LOG_DEBUG(LOG_FDW, "Enum datum: springtail_oid: {}, pg_oid: {}, enum_oid: {}",
                            springtail_oid, pg_oid, enum_oid);

        return ObjectIdGetDatum(enum_oid);
    }

    Datum
    PgFdwMgr::_get_datum_from_field(const PgFdwState *state,
                                    const Field *field,
                                    const Extent::Row &row,
                                    int32_t springtail_oid,
                                    Oid pg_oid,
                                    int32_t atttypmod)
    {
        // check for user defined type
        if (springtail_oid >= FirstNormalObjectId) {
            // user type; enum, lookup index to label
            assert(field->get_type() == SchemaType::FLOAT32);
            return _get_enum_datum(state, springtail_oid, pg_oid, field->get_float32(&row));
        }

        // otherwise convert they row by the schema type
        switch (field->get_type()) {
        case SchemaType::INT64:
            return Int64GetDatum(field->get_int64(&row));
        case SchemaType::UINT64:
            return UInt64GetDatum(field->get_uint64(&row));
        case SchemaType::INT32:
            return Int32GetDatum(field->get_int32(&row));
        case SchemaType::UINT32:
            return UInt32GetDatum(field->get_uint32(&row));
        case SchemaType::INT16:
            return Int16GetDatum(field->get_int16(&row));
        case SchemaType::UINT16:
            return UInt16GetDatum(field->get_uint16(&row));
        case SchemaType::INT8:
            return Int8GetDatum(field->get_int8(&row));
        case SchemaType::UINT8:
            return UInt8GetDatum(field->get_uint8(&row));
        case SchemaType::BOOLEAN:
            return BoolGetDatum(field->get_bool(&row));
        case SchemaType::FLOAT64:
            return Float8GetDatum(field->get_float64(&row));
        case SchemaType::FLOAT32:
            return Float4GetDatum(field->get_float32(&row));
        case SchemaType::TEXT: {
            const std::string_view value(field->get_text(&row));
            return PointerGetDatum(cstring_to_text_with_len(value.data(), value.size()));
        }
        case SchemaType::NUMERIC: {
            auto &&value = field->get_numeric(&row);
            int32_t size = value->varsize();
            Numeric data = reinterpret_cast<Numeric>(palloc(size));
            memcpy(data, value.get(), size);
            return PointerGetDatum(data);
        }
        case SchemaType::BINARY: {
            auto &&value = field->get_binary(&row);
            return _binary_to_datum(value, pg_oid, atttypmod);
        }
        default:
            return 0;
        }
    }

    Datum
    PgFdwMgr::_binary_to_datum(const std::span<const char> &value,
                               Oid pg_oid,
                               int32_t atttypmod)
    {
        // retrieve the type's entry from the pg_type table
        HeapTuple tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(pg_oid));
        if (!HeapTupleIsValid(tuple)) {
            elog(ERROR, "FDW: cache lookup failed for type %u", pg_oid);
        }

        // get the receive function
        regproc typeinput = ((Form_pg_type) GETSTRUCT(tuple))->typreceive;

        // handle array types by retrieving the subscript element type
        Form_pg_type typeform = (Form_pg_type) GETSTRUCT(tuple);
        Oid typelem = typeform->typelem;
        if (typelem != 0) {
            pg_oid = typelem;
        }

        ReleaseSysCache(tuple);

        // note: we need to store the data into a StringInfo so that the receive function can
        // unpack it for us
        StringInfoData string;
        initStringInfo(&string);

        appendBinaryStringInfoNT(&string, value.data(), value.size());
        Datum datum = PointerGetDatum(&string);

        // call the recieve function
        return OidFunctionCall3(typeinput, datum, ObjectIdGetDatum(pg_oid), Int32GetDatum(atttypmod));
    }

    std::string
    PgFdwMgr::_get_type_name(int32_t pg_type,
                             const std::unordered_map<uint64_t, std::string> &user_types)
    {
        if (pg_type < FirstNormalObjectId) {
           // get the type name from the system cache
            HeapTuple tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(pg_type));
            if (!HeapTupleIsValid(tuple)) {
                elog(ERROR, "cache lookup failed for type %u", pg_type);
                ReleaseSysCache(tuple);

                throw FdwError(fmt::format("Failed to find type name for {}", pg_type));
            }
            std::string type_name = ((Form_pg_type) GETSTRUCT(tuple))->typname.data;
            ReleaseSysCache(tuple);

            return type_name;
        }

        // otherwise, check if the type is a user type
        auto it = user_types.find(pg_type);
        if (it != user_types.end()) {
            return it->second;
        }
        elog(ERROR, "cache lookup failed for type %u", pg_type);
        throw FdwError(fmt::format("Failed to find type name for {}", pg_type));
    }

    std::string
    PgFdwMgr::_gen_fdw_table_sql(const std::string &server_name,
                                 const std::string &schema,
                                 const std::string &table,
                                 uint64_t tid,
                                 const std::vector<std::tuple<std::string, std::string, bool>> &columns)
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

        LOG_DEBUG(LOG_FDW, "Generated SQL: {}", create);

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
            std::string type_name = _get_type_name(column.pg_type, {});
            columns.push_back({ column.name, type_name, column.nullable });
        }

        return _gen_fdw_table_sql(server, CATALOG_SCHEMA_NAME, table_name, tid, columns);
    }

    List *
    PgFdwMgr::_import_springtail_catalog(const std::string &server,
                                         const std::set<std::string, std::less<>> &table_set,
                                         bool exclude, bool limit)
    {
        List        *commands = NIL;
        std::string  sql;

        auto import_catalog = [exclude, limit, &table_set, &server, &commands, &sql]<typename T>(const auto& tab_name) {
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
        import_catalog.operator()<sys_tbl::NamespaceNames>(CATALOG_NAMESPACE_NAMES);
        import_catalog.operator()<sys_tbl::UserTypes>(CATALOG_USER_TYPES);

        return commands;
    }

    std::unordered_map<uint64_t, std::string>
    PgFdwMgr::_load_user_types(uint64_t db_id,
                               const std::string &namespace_name,
                               uint64_t namespace_id,
                               uint64_t schema_xid)
    {
        std::unordered_map<uint64_t, std::string> user_types;

        // get the user types table to iterate over
        auto table = TableMgr::get_instance()->get_table(db_id, sys_tbl::UserTypes::ID,
                                                         schema_xid);
        // get field array
        auto fields = table->extent_schema()->get_fields();

        // escape the namespace name; used for qualified type names
        std::string escaped_namespace = quote_identifier(namespace_name.c_str());

        // iterate over the user types table and populate the user type map
        for (auto row : (*table)) {
            auto type_ns_id = fields->at(sys_tbl::UserTypes::Data::NAMESPACE_ID)->get_uint64(&row);
            auto xid = fields->at(sys_tbl::UserTypes::Data::XID)->get_uint64(&row);

            if (xid > schema_xid) {
                continue;
            }

            // check for schema-namespace match
            if (type_ns_id != namespace_id) {
                continue;
            }

            uint64_t pg_type = fields->at(sys_tbl::UserTypes::Data::TYPE_ID)->get_uint64(&row);
            bool exists = fields->at(sys_tbl::UserTypes::Data::EXISTS)->get_bool(&row);
            if (!exists) {
                // find type and remove if it exists
                user_types.erase(pg_type);
                continue;
            }

            // generate a fully qualified quoted type name and add to map
            std::string type_name(fields->at(sys_tbl::UserTypes::Data::NAME)->get_text(&row));
            std::string qualified_type_name = fmt::format("{}.{}", escaped_namespace, quote_identifier(type_name.c_str()));
            user_types.insert({pg_type, qualified_type_name});
        }

        return user_types;
    }

    List* vector_to_string_list(const std::vector<std::string>& vec)
    {
        int len = vec.size();

        auto list = (List*) palloc(sizeof(List));
        if (!list) return nullptr;

        list->type = T_List;
        list->length = len;
        list->max_length = len;

        list->elements = (ListCell*) palloc(sizeof(ListCell) * len);
        if (!list->elements) {
            pfree(list);
            return nullptr;
        }

        for (int i = 0; i < len; ++i) {
            list->elements[i].ptr_value = pstrdup(vec[i].c_str());
        }

        return list;
    }

    List *
    PgFdwMgr::fdw_import_foreign_schema(const std::string &server,
                                        const std::string &namespace_name,
                                        const List *table_list,
                                        bool exclude, bool limit,
                                        uint64_t db_id,
                                        const std::string &db_name,
                                        uint64_t schema_xid)
    {
        auto token = open_telemetry::OpenTelemetry::get_instance()->set_context_variables({{"db_id", std::to_string(db_id)}, {"xid", std::to_string(schema_xid)}});
        std::set<std::string, std::less<>> table_set;

        // construct list of either excluded or limited tables
        if (exclude || limit) {
            ListCell *lc;
            foreach(lc, table_list) {
                RangeVar *rv = (RangeVar *)lfirst(lc);
                table_set.emplace(rv->relname);
            }
        }

        LOG_DEBUG(LOG_FDW, "Importing schema: {} <=> {}\n", namespace_name, CATALOG_SCHEMA_NAME);

        // if we are importing the catalog schema, handle it separately
        if (namespace_name == std::string(CATALOG_SCHEMA_NAME)) {
            return _import_springtail_catalog(server, table_set, exclude, limit);
        }

        std::vector<std::string> ddl = PgFdwCommon::get_schema_ddl(db_id, schema_xid, server, namespace_name, exclude, limit, table_set,
                              [&db_id, &namespace_name, &schema_xid](uint32_t pg_type, uint64_t namespace_id) {
                                  return _get_type_name(pg_type, _load_user_types(db_id, namespace_name, namespace_id, schema_xid));
                              },
                              [](const std::string &name) {
                                  return quote_identifier(name.c_str());
                              }, true);

        return vector_to_string_list(ddl);
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
            case NUMERICOID:    // DECIMAL(x,y)
                return true;
            case VARCHAROID:
            case TEXTOID:
                // due to different collations/encodings we only support equality for text
                return (op == EQUALS || op == NOT_EQUALS);
            default:
                if (pg_type >= FirstNormalObjectId) {
                    // enum type; ordering based on sort order, treat as float
                    LOG_DEBUG(LOG_FDW, "Found user defined type for sorting: {}", pg_type);
                    return true;
                }
                LOG_DEBUG(LOG_FDW, "Type not suitable for sorting: {}", pg_type);
                return false;
        }
    }

    void
    PgFdwMgr::_make_const_field(const PgFdwState *state,
                                const SchemaColumn &column,
                                int idx,
                                const ConstQual *qual)
    {
        DCHECK(qual->base.right_type == T_Const);

        FieldArrayPtr fields = state->qual_fields;

        if (qual->isnull) {
            // NULL constant; set to null
            fields->at(idx) = std::make_shared<ConstNullField>(column.type);
            return;
        }

        // Generate a const field based on type; populate it into fields array
        // Upconvert types to schema type if possible; there is a check in
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
                CHECK_EQ(column.type, SchemaType::INT32);
                fields->at(idx) = std::make_shared<ConstTypeField<int32_t>>(DatumGetInt32(qual->value));
                break;
            case INT2OID:
                CHECK_EQ(column.type, SchemaType::INT16);
                fields->at(idx) = std::make_shared<ConstTypeField<int16_t>>(DatumGetInt16(qual->value));
                break;
            case FLOAT8OID:
                fields->at(idx) = std::make_shared<ConstTypeField<double>>(DatumGetFloat8(qual->value));
                break;
            case FLOAT4OID:
                CHECK_EQ(column.type, SchemaType::FLOAT32);
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
            case NUMERICOID: {// DECIMAL(x,y)
                std::shared_ptr<numeric::NumericData> numeric_datum(
                    reinterpret_cast<numeric::Numeric>(qual->value),
                    [](numeric::Numeric ptr) {
                        // this is shared pointer to the data inside ConstQual
                        // as this data is not owned by this pointer, no need to remove it
                    });
                auto buf = (char*)(numeric_datum.get());
                std::vector<char> value(buf, buf + numeric_datum->varsize());

                fields->at(idx) = std::make_shared<ConstTypeField<
                        std::shared_ptr<numeric::NumericData>>>(std::move(value));
                break;
            }
            default:
                // handle enum user defined type
                if (qual->base.typeoid >= FirstNormalObjectId) {
                    Oid oid = DatumGetObjectId(qual->value);
                    LOG_DEBUG(LOG_FDW, "Found user defined type datum qual field: {}", oid);

                    // do reverse mapping lookup to get the enum idx from springtail
                    float enum_id = _get_enum_id_from_pg(state, column.pg_type, qual->base.typeoid, oid);
                    fields->at(idx) = std::make_shared<ConstTypeField<float>>(enum_id);
                    break;
                }
                elog(ERROR, "Unsupported type for constant field: %d", qual->base.typeoid);
                break;
        }
    }

    bool
    PgFdwMgr::_compare_field(const void *row,
                             const FieldPtr& val_field,
                             const FieldPtr& key_field,
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

    UserTypePtr
    PgFdwMgr::_enum_cache_lookup(uint64_t db_id, int32_t oid, uint64_t xid)
    {
        // first check the _user_type_cache for the oid
        LOG_DEBUG(LOG_FDW, "Enum cache lookup for oid: {}", oid);

        // lookup oid in user type cache, if not there fetch from systbl mgr
        UserTypePtr utp = _user_type_cache.get(oid);
        if (utp == nullptr) {
            XidLsn xidlsn{xid};
            utp = SchemaMgr::get_instance()->get_usertype(db_id, oid, xidlsn);
            CHECK_NE(utp, nullptr);
            _user_type_cache.insert(oid, utp);
        }

        return utp;
    }

    PgFdwState::PgFdwState(TablePtr table, uint64_t db_id, uint64_t tid, uint64_t xid)
            : table(table), db_id(db_id), tid(tid), xid(xid), stats(table->get_stats())
    {
        columns = SchemaMgr::get_instance()->get_columns(table->db(), tid, { xid, constant::MAX_LSN });
        for (const auto &entry : columns) {
            name_map[entry.second.name] = entry.second.position;
        }

        if (tid > constant::MAX_SYSTEM_TABLE_ID) {
            auto &&meta = sys_tbl_mgr::Client::get_instance()->get_schema(table->db(), tid, { xid, constant::MAX_LSN });
            for (auto& v: meta->indexes) {
                if (static_cast<sys_tbl::IndexNames::State>(v.state) != sys_tbl::IndexNames::State::READY) {
                    continue;
                }
                indexes.push_back(v);
            }
        }
    }
} // namespace springtail::pg_fdw
