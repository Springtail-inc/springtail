#pragma once

#include <mutex>
#include <memory>
#include <optional>

#include <storage/constants.hh>
#include <storage/table.hh>
#include <storage/table_mgr.hh>
#include <storage/constants.hh>
#include <storage/field.hh>
#include <storage/system_tables.hh>
#include <storage/schema.hh>
#include <storage/schema_mgr.hh>

#include <xid_mgr/xid_mgr_client.hh>

/* These are defined by Thrift imported from xid_mgr_client.h and
 * must be undefined before including postgres.h */
#undef PACKAGE_STRING
#undef PACKAGE_VERSION
#undef UINT64CONST

extern "C" {
    #include <postgres.h>
    #include <nodes/pg_list.h>
    #include <c.h>
}

#include <pg_fdw/pg_fdw_common.h>

namespace springtail  {

    struct PgFdwSortGroup {
        std::string attname;
        int attnum;
        bool reversed;
        bool nulls_first;
        std::string collate;

        PgFdwSortGroup(const DeparsedSortGroup *sort_group)
            : attname(sort_group->attname == NULL ? "" : sort_group->attname),
              attnum(sort_group->attnum),
              reversed(sort_group->reversed),
              nulls_first(sort_group->nulls_first),
              collate(sort_group->collate == NULL ? "" : sort_group->collate)
        {}
    };
    using PgFdwSortGroupPtr = std::shared_ptr<PgFdwSortGroup>;


    /** Internal state used to track table scan */
    struct PgFdwState {
        TablePtr table;
        uint64_t tid;
        uint64_t xid;
        FieldArrayPtr fields;
        std::optional<Table::Iterator> iter;
        std::map<uint32_t, SchemaColumn> columns; ///< Column mapping from column ID to column metadata
        std::vector<uint32_t> pkey_column_ids;    ///< Primary key column IDs

        std::vector<std::string> target_columns;         ///< List of target columns
        std::vector<PgFdwSortGroupPtr> sort_columns;  ///< List of sort group columns

        List *qual_list;    ///< List of predicate clauses (BaseQual)

        /** Constructor */
        PgFdwState(TablePtr table, uint64_t tid, uint64_t xid)
            : table(table), tid(tid), xid(xid), iter(std::nullopt)
        {
            // fetch the fields for the table
            fields = table->extent_schema()->get_fields();

            // fetch the columns and column IDs for the table
            columns = SchemaMgr::get_instance()->get_columns(tid, xid, constant::MAX_LSN);

            // populate pkey column ids
            int num_pkeys = 0;
            pkey_column_ids.resize(columns.size());
            for (const auto &col : columns) {
                if (col.second.exists && col.second.pkey_position.has_value()) {
                    pkey_column_ids[col.second.pkey_position.value()] = col.first;
                    num_pkeys++;
                }
            }
            pkey_column_ids.resize(num_pkeys);
        }
    };
    using PgFdwStatePtr = std::shared_ptr<PgFdwState>;

    /** Singleton manager for handling table scan operations */
    class PgFdwMgr {
    public:
        static constexpr char CATALOG_SCHEMA_NAME[] = "__pg_springtail";  ///< Schema name for catalog tables
        static constexpr char CATALOG_TABLE_NAMES[] = "table_names";      ///< Table name for system table names
        static constexpr char CATALOG_TABLE_ROOTS[] = "table_roots";      ///< Table name for system table roots
        static constexpr char CATALOG_TABLE_INDEXES[] = "indexes";        ///< Table name for system table indexes
        static constexpr char CATALOG_TABLE_SCHEMAS[] = "schemas";        ///< Table name for system table schemas

        /** Get singleton instance */
        static PgFdwMgr* get_instance() {
            std::call_once(_init_flag, _init);
            return _instance;
        }

        /**
         * Init call, pass in config file path;
         * Ideally, call before first get_instance()
         */
        static void fdw_init(const char *config_file);

        /** Create state based on table ID
         * @param tid Table ID
         * @param pg_xid Postgres XID of current transaction
         */
        PgFdwState *fdw_create_state(uint64_t tid, uint64_t pg_xid);

        /** Begin scan
         * @param state PgFdwState
         * @param target_list List of target columns (Value or String)
         * @param qual_list List of predicate clauses (BaseQual)
         * @param sortgroup List of sort group columns (DeparsedSortGroup)
         */
        void fdw_begin_scan(PgFdwState *state, List *target_list, List *qual_list, List *sortgroup);

        /** Iterate scan -- get next row */
        bool fdw_iterate_scan(PgFdwState *state, Datum *values, bool *isnull);

        /** End scan -- free state */
        void fdw_end_scan(PgFdwState *state);

        /** Reset scan -- set iterator to beginning */
        void fdw_reset_scan(PgFdwState *state);

        /** Import foreign schema -- scan through system table generating sql for create foreign table */
        List *fdw_import_foreign_schema(const std::string &server, const std::string &schema,
                                        const List *table_list, bool exclude, bool limit);

        /** Helper return list / subset of sortable columns if table is sortable by sort group
         *  Called from get_foreign_paths
         * @param state Plan state
         * @param sortgroup List of DeparsedSortGroup
         * @return List or sublist of path keys based on sort group
         */
        List *fdw_can_sort(SpringtailPlanState *state, List *sortgroup);

        /** Get list of path keys -- indexes
         * @param state Planstate
         * @return List of a List of path keys (key attnum, num rows)
         */
        List *fdw_get_path_keys(SpringtailPlanState *state);

        /** Get estimate of row width/number of rows
         * @param planstate Plan state
         * @param target_list List of target columns (String or Value)
         * @param qual_list List of predicate clauses (BaseQual)
         */
        void fdw_get_rel_size(SpringtailPlanState *planstate, List *target_list, List *qual_list, double *rows, int *width);

        /** Commit or rollback a transaction, remove the XID mappings
         * @param pg_xid Postgres XID
         * @param commit True if commit, false if rollback
         */
        void fdw_commit_rollback(uint64_t pg_xid, bool commit);

    private:
        /** Delete constructor */
        PgFdwMgr() {}
        PgFdwMgr(const PgFdwMgr&) = delete;
        PgFdwMgr& operator=(const PgFdwMgr&) = delete;

        static PgFdwMgr* _instance;        ///< Singleton instance
        static std::once_flag _init_flag;  ///< Initialization flag
        static PgFdwMgr* _init();          ///< Initialize singleton

        std::map<uint64_t, uint64_t> _xid_map;  ///< Map of pg XID to springtail XID

        /** Helper to convert field to PG Datum */
        static Datum _get_datum_from_field(FieldPtr field, const Extent::Row &row);

        /** Helper to generate create foreign table sql */
        static std::string _gen_fdw_table_sql(const std::string &server,
                                              const std::string &table,
                                              uint64_t tid,
                                              std::vector<std::tuple<std::string, uint8_t, bool, std::optional<std::string>>> &columns);

        /** Helper to generate a system table create foreign table sql */
        static std::string _gen_fdw_system_table(const std::string &server,
                                                 const std::string &table,
                                                 uint64_t tid,
                                                 const std::vector<SchemaColumn> &columns);

        /** Helper to iterate through system tables to generate import command list */
        static List *_import_springtail_catalog(const std::string &server,
                                                const std::set<std::string> table_set,
                                                bool exclude, bool limit);

        static void _handle_exception(const Error &e);
    };
}