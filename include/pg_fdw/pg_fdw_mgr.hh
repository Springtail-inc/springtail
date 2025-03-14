#pragma once

#include <mutex>
#include <memory>
#include <shared_mutex>
#include <optional>

#include <common/constants.hh>
#include <common/concurrent_queue.hh>

#include <redis/redis_ddl.hh>

#include <storage/field.hh>
#include <storage/schema.hh>

#include <sys_tbl_mgr/schema_mgr.hh>
#include <sys_tbl_mgr/system_tables.hh>
#include <sys_tbl_mgr/table.hh>
#include <sys_tbl_mgr/table_mgr.hh>
#include <sys_tbl_mgr/client.hh>
#include <sys_tbl_mgr/shm_cache.hh>

#include <xid_mgr/xid_mgr_client.hh>

extern "C" {
    #include <postgres.h>
    #include <nodes/pg_list.h>
    #include <c.h>
    #include <utils/builtins.h>
    #include <libpq-fe.h>
}

#include <pg_fdw/pg_fdw_common.h>

namespace springtail::pg_fdw {

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
        FieldArrayPtr fields = nullptr;       ///< Fields for the columns from the target list
        FieldArrayPtr qual_fields = nullptr;  ///< Fields for the columns from the qual list
        TableStats stats;                     ///< Table statistics
        int rows_fetched = 0;                 ///< Number of rows fetched
        int rows_skipped = 0;                 ///< Number of rows skipped
        bool scan_asc = true;                 ///< Scan direction for iterator as defined by ORDER BY <col> ASC/DESC

        ///< Start iterator for table scan
        std::optional<Table::Iterator> iter_start = std::nullopt;
        ///< End iterator for table scan
        std::optional<Table::Iterator> iter_end = std::nullopt;

        std::map<uint32_t, SchemaColumn> columns;     ///< Column map from ID to column metadata
        std::map<int,int> target_columns;             ///< Map of target columns, from attno to field idx
        std::vector<ConstQualPtr> filtered_quals;     ///< List of quals (for where clause)
        std::vector<Index> indexes; ///< List of table indexes including the primary index.
                                    /// Index columns are sorted by their position in the index.
        std::optional<Index> sortgroup_index; ///< Index matching the sortgroup.
        std::optional<Index> index; ///< The final index to use for scanning

        /** Constructor */
        PgFdwState(TablePtr table, uint64_t tid, uint64_t xid);
    };
    using PgFdwStatePtr = std::shared_ptr<PgFdwState>;

    /** Singleton manager for handling table scan operations */
    class PgFdwMgr {
    public:
        static constexpr char CATALOG_SCHEMA_NAME[] = SPRINGTAIL_FDW_CATALOG_SCHEMA;  ///< Schema name for catalog tables
        static constexpr char CATALOG_TABLE_NAMES[] = "table_names";      ///< Table name for system table names
        static constexpr char CATALOG_TABLE_ROOTS[] = "table_roots";      ///< Table name for system table roots
        static constexpr char CATALOG_TABLE_INDEXES[] = "indexes";        ///< Table name for system table indexes
        static constexpr char CATALOG_TABLE_SCHEMAS[] = "schemas";        ///< Table name for system table schemas
        static constexpr char CATALOG_TABLE_STATS[] = "table_stats";      ///< Table name for system table stats
        static constexpr char CATALOG_INDEX_NAMES[] = "index_names";      ///< Table name for system index names
        static constexpr char CATALOG_NAMESPACE_NAMES[] = "namespace_names";      ///< Table name for system index names

        static constexpr char PG_FDW_LOG_FILE_PREFIX[] = "pg_fdw";         ///< Log file prefix

        /** Get singleton instance */
        static PgFdwMgr* get_instance() {
            assert (_instance != nullptr);
            return _instance;
        }

        /**
         * Init call, pass in config file path;
         * Ideally, call before first get_instance()
         */
        static void fdw_init(const char *config_file=nullptr, bool init=true);

        /** Create state based on table ID
         * @param tid Table ID
         * @param pg_xid Postgres XID of current transaction
         * @param schema_xid Schema XID optained from the foreign table import option
         */
        PgFdwState *fdw_create_state(uint64_t db_id,
                                     uint64_t tid,
                                     uint64_t pg_xid,
                                     uint64_t schema_xid);

        /** Begin scan
         * @param state PgFdwState
         * @param target_list List of target columns (Value or String)
         * @param qual_list List of predicate clauses (BaseQual)
         * @param sortgroup List of sort group columns (DeparsedSortGroup)
         */
        void fdw_begin_scan(PgFdwState *state,
                            List *target_list,
                            List *qual_list,
                            List *sortgroup);

        /** Iterate scan -- get next row
         * @param state PgFdwState
         * @param num_attrs Number of attributes
         * @param attrs Array of pg attributes
         * @param values Array of Datum values (output)
         * @param isnull Array of null flags (output)
         * @param eos End of scan flag; no more data (output)
         * @return True if row is valid, false row not valid, check eos for more data
         */
        bool fdw_iterate_scan(PgFdwState *state,
                              int num_attrs,
                              Form_pg_attribute *attrs,
                              Datum *values,
                              bool *isnull,
                              bool *eos);

        /** End scan -- free state */
        void fdw_end_scan(PgFdwState *state);

        /** Reset scan -- set iterator to beginning */
        void fdw_reset_scan(PgFdwState *state);

        /** Import foreign schema -- scan through system table generating sql for create foreign table */
        List *fdw_import_foreign_schema(const std::string &server,
                                        const std::string &schema,
                                        const List *table_list,
                                        bool exclude,
                                        bool limit,
                                        uint64_t db_id,
                                        const std::string &db_name,
                                        uint64_t schema_xid);

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
        PgFdwMgr() {};
        PgFdwMgr(const PgFdwMgr&) = delete;
        PgFdwMgr& operator=(const PgFdwMgr&) = delete;

        static PgFdwMgr* _instance;        ///< Singleton instance
        static std::once_flag _init_flag;  ///< Initialization flag
        static PgFdwMgr* _init();          ///< Initialize singleton

        std::shared_mutex _mutex;               ///< Mutex for xid map
        std::map<uint64_t, uint64_t> _xid_map;  ///< Map of pg XID to springtail XID

        std::atomic<uint64_t> _schema_xid; ///< The most recently seen schema XID

        std::shared_ptr<sys_tbl_mgr::ShmCache> _roots_cache; ///< An IPC cache shared by pg_xid_subscriber_daemon

        // static methods

        /** Helper to convert field to PG Datum */
        static Datum _get_datum_from_field(FieldPtr field,
                                           const Extent::Row &row,
                                           int32_t pg_type,
                                           int32_t atttypmod);

        /** Helper to convert a PG type OID to a type name using the PG system cache. */
        static std::string _get_type_name(int32_t pg_type);

        /** Helper to generate create foreign table sql */
        static std::string _gen_fdw_table_sql(const std::string &server_name,
                                              const std::string &schema,
                                              const std::string &table,
                                              uint64_t tid,
                                              std::vector<std::tuple<std::string, std::string, bool>> &columns);

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

        /** Helper to determine if a type can be used in a where clause */
        static bool _is_type_sortable(Oid pg_type, QualOpName op);

        /** Helper to setup quals and scan iterator in state, called from begin_scan */
        static void _init_quals(PgFdwState *state, List *qual_list);

        /** Helper to setup itertor based on filtered qual list */
        static void _init_qual_fields(PgFdwState *state);

        /** Helper to create constant field from qual and add to field array */
        static void _make_const_field(FieldArrayPtr fields, int idx, ConstQual *qual);

        /** Helper to compare a primary key const qual field to the data within a row */
        static bool _compare_field(const std::any &row,
                                   FieldPtr row_field,
                                   FieldPtr key_field,
                                   QualOpName op);

        /** Helper to set/reset scan iterators from beginning based on quals */
        static void _set_scan_iterators(PgFdwState *state);

        /** Helper to generate tuple used in upper/lower bound calculations for scan iter */
        static FieldTuplePtr _gen_qual_tuple(const std::vector<ConstQualPtr> &quals,
                                             const FieldArrayPtr qual_fields);

        friend std::vector<ConstQualPtr>
        _get_index_quals(const PgFdwState *state, Index const& idx, List const* qual_list);
    };
} // namespace springtail::pg_fdw
