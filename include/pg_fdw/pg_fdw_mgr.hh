#pragma once

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

#include <pg_fdw/pg_fdw_ddl_common.hh>
#include <pg_fdw/pg_fdw_plan_state.hh>
#include <pg_fdw/pg_xid_collector_client.hh>

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
        uint64_t db_id;
        uint64_t tid;
        uint64_t xid;
        FieldArrayPtr fields = nullptr;       ///< Fields for the columns from the target list
        bool index_only_scan = false;        ///< indicates that fields are part of the index itself
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
        std::unordered_map<int, uint32_t> attr_map; ///< Map from FDW local attribute number to Springtail's column position
        std::unordered_map<std::string, uint32_t> name_map; ///< Map from column name to Springtail column position

        struct PgAttr {
            uint32_t atttypid;
            uint32_t atttypmod;
            uint32_t attnum;

            bool operator==(const PgAttr&) const = default;
        };
        std::vector<PgAttr> _attrs; ///< Scan tuple attributes

        struct TargetColumn {
            int field_idx; ///< field idx in the fields array of PgFdwState
            int32_t sp_pg_type; ///< Springtail column pg type
            PgAttr pg_attr; ///< PG attribute info
            std::string name; ///< column name to be used in EXPLAIN

            struct Filter {
                QualOpName op;
                FieldPtr field;
            };

            std::optional<Filter> filter; ///< The target has a filter
        };

        ///< Vector of targets including filtered quals
        std::vector<TargetColumn> target_columns;

        std::vector<ConstQualPtr> filtered_quals;     ///< List of quals (for where clause)
        std::vector<Index> indexes; ///< List of table indexes including the primary index.
                                    /// Index columns are sorted by their position in the index.
        std::optional<Index> sortgroup_index; ///< Index matching the sortgroup.
        std::optional<Index> index; ///< The final index to use for scanning
        std::vector<uint64_t> join_indexes; ///< List of table index ids that a present in outer join clauses.
        std::vector<uint64_t> qual_indexes; ///< List of table index ids that a present in restric clauses.

        /** Constructor */
        PgFdwState(TablePtr table, uint64_t db_id, uint64_t tid, uint64_t xid);
    };
    using PgFdwStatePtr = std::shared_ptr<PgFdwState>;

    /** Singleton manager for handling table scan operations */
    class PgFdwMgr : public Singleton<PgFdwMgr> {
        friend class Singleton<PgFdwMgr>;
    public:
        static constexpr char CATALOG_SCHEMA_NAME[] = SPRINGTAIL_FDW_CATALOG_SCHEMA;  ///< Schema name for catalog tables
        static constexpr char CATALOG_TABLE_NAMES[] = "table_names";      ///< Table name for system table names
        static constexpr char CATALOG_TABLE_ROOTS[] = "table_roots";      ///< Table name for system table roots
        static constexpr char CATALOG_TABLE_INDEXES[] = "indexes";        ///< Table name for system table indexes
        static constexpr char CATALOG_TABLE_SCHEMAS[] = "schemas";        ///< Table name for system table schemas
        static constexpr char CATALOG_TABLE_STATS[] = "table_stats";      ///< Table name for system table stats
        static constexpr char CATALOG_INDEX_NAMES[] = "index_names";      ///< Table name for system index names
        static constexpr char CATALOG_NAMESPACE_NAMES[] = "namespace_names";      ///< Table name for namespace schema names
        static constexpr char CATALOG_USER_TYPES[] = "user_types";        ///< Table name for system user defined types
        static constexpr char PG_FDW_LOG_FILE_PREFIX[] = "pg_fdw";        ///< Log file prefix

        /** Maximum number of user type definitions to cache */
        static constexpr int MAX_USER_TYPE_CACHE = 100;

        /** Sleep interval for FDW background thread */
        static constexpr int THREAD_SLEEP_INTERVAL_MSEC = 1000;

        /**
         * Init call, pass in config file path;
         * Ideally, call before first get_instance()
         */
        static void fdw_init(const char *config_file=nullptr, bool init=true);

        /**
         * Exit the current FDW session.
         */
        static void fdw_exit();

        /** Create state based on table ID
         * @param tid Table ID
         * @param pg_xid Postgres XID of current transaction
         * @param schema_xid Schema XID optained from the foreign table import option
         * @return List* that can be used to create SpringtailPlanState
         */
        List* fdw_create_state(uint64_t db_id,
                uint64_t tid,
                uint64_t pg_xid,
                uint64_t schema_xid);

        /** Begin scan
         * @param planstate plan state
         * @param num_attrs Number of attributes
         * @param attrs Array of pg attributes
         * @param target_list List of target columns (Value or String)
         * @param qual_list List of predicate clauses (BaseQual)
         */
        PgFdwState* fdw_begin_scan(const SpringtailPlanState *planstate,
                int num_attrs,
                const Form_pg_attribute* attrs,
                const List *quals);

        /** Iterate scan -- get next row
         * @param state PgFdwState
         * @param values Array of Datum values (output)
         * @param isnull Array of null flags (output)
         * @param eos End of scan flag; no more data (output)
         * @return True if row is valid, false row not valid, check eos for more data
         */
        bool fdw_iterate_scan(PgFdwState *state,
                              Datum *values,
                              bool *isnull,
                              bool *eos);

        /** End scan -- free state */
        void fdw_end_scan(PgFdwState *state);

        /** Reset scan -- set iterator to beginning */
        void fdw_reset_scan(PgFdwState *state, const List *qual_list);

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
         * @param use_secondary Make use the secondary indexes to match the sortgroup
         * @return List or sublist of path keys based on sort group
         */
        List* fdw_can_sort(SpringtailPlanState* planstate, PgFdwState* pg_state, const List *sortgroup, const List* quals, bool use_secondary = false);

        /** Get list of path keys -- indexes
         * @param planstate Planstate
         * @param state Scan state
         * @return List of a List of path keys (key attnum, num rows)
         */
        List *fdw_get_path_keys(const SpringtailPlanState *planstate, PgFdwState* state);

        /**
         */
        PgFdwState* create_scan_state(const SpringtailPlanState *state, const List* qual_indexes, const List* join_quals);

        /** Get estimate of row width/number of rows
         * @param planstate Plan state
         * @param qual_list List of predicate clauses (BaseQual)
         * @param join_quals List of predicate clauses (BaseQual) that are part of join clauses
         */
        void fdw_get_rel_size(SpringtailPlanState *planstate, const List *qual_list, const List* join_quals, double *rows, int *width);

        /** Commit or rollback a transaction, remove the XID mappings
         * @param pg_xid Postgres XID
         * @param commit True if commit, false if rollback
         */
        void fdw_commit_rollback(uint64_t pg_xid, bool commit);

        /** Explain scan
         * @param state PgFdwState
         * @return Vector of name:description pairs that should be added by PG to its output
         */
        std::vector<std::pair<std::string, std::string>> fdw_explain_scan(const PgFdwState *state);


        // public for testing...
        /**
         * @brief Convert a qual of one schema type to another; changes qual oid and value
         * @param qual qual to convert
         * @param from schema type of springtail column
         * @param to schema type of pg qual value
         * @return true if conversion was successful
         * @return false if conversion failed
         */
        static bool convert_qual(ConstQualPtr qual, SchemaType from, SchemaType to);

        /**
         * @brief Check if a qual is compatible with a schema column
         * @param column schema column
         * @param qual qual to check
         * @return true if the qual is compatible with the column
         * @return false if the qual is not compatible with the column
         */
        static bool check_type_compatibility(const SchemaColumn &column, ConstQualPtr qual);

        /**
         * @brief Initialization function
         *
         * @param db_name - database name
         * @param ddl_connection - ddl connection flag
         */
        void init(const char *db_name, bool ddl_connection);

    private:
        /**
         * @brief Construct a new Pg Fdw Mgr object
         *
         */
        PgFdwMgr() :
            Singleton(ServiceId::PgFdwMgrId),
            _user_type_cache(MAX_USER_TYPE_CACHE)
        {};

        /**
         * @brief Destroy the Pg Fdw Mgr object
         *
         */
        ~PgFdwMgr();

        std::shared_mutex _mutex;               ///< Mutex for xid map

        std::shared_mutex _rc_mutex;    ///< roots cache mutex
        std::shared_ptr<sys_tbl_mgr::ShmCache> _roots_cache; ///< An IPC cache shared by pg_xid_subscriber_daemon

        LruObjectCache<int32_t, UserType> _user_type_cache; ///< cache of user types

        PgXidCollectorClient _xid_collector_client;    ///< xid collector client
        std::string _fdw_id;                           ///< fdw id
        std::mutex _xid_update_mutex;                  ///< mutex for updating xid
        uint64_t _db_id{0};                            ///< database id
        uint64_t _schema_xid{0};        ///< The most recently seen schema XID
        uint64_t _last_xid{0};          ///< last known xid
        uint64_t _trans_xid{0};         ///< current transaction XID
        uint64_t _trans_pg_xid{0};      ///< current transaction PG XID
        std::atomic<bool> _in_transaction{false};   ///< in transaction flag, when set disables background thread
        std::atomic<bool> _ddl_connection{false};   ///< ddl connection flag, when set does not send updates to collector

        /**
         * @brief Function for running background thread
         *
         */
        virtual void _internal_run();

        /**
         * @brief Lookup enum user type from cache based on oid and index
         * @param db_id db_id of the database
         * @param oid pg oid of type (in springtail)
         * @param index enum index
         * @param xid xid for this request
         * @return user type pointer
         */
        UserTypePtr _enum_cache_lookup(uint64_t db_id,
                                       int32_t oid,
                                       uint64_t xid);

        /** Helper to convert a springtail enum user type to a datum */
        Datum _get_enum_datum(const PgFdwState *state,
                              int32_t springtail_oid,
                              Oid pg_oid,
                              float sort_order);

        /** Helper to convert a postgres enum type to springtail enum id (index/sortorder) */
        float _get_enum_id_from_pg(const PgFdwState *state,
                                   int32_t springtail_oid,
                                   Oid pg_oid,
                                   Oid label_oid);

        /** Helper to convert field to PG Datum */
        Datum _get_datum_from_field(const PgFdwState *state,
                                    const Field *field,
                                    const Extent::Row &row,
                                    int32_t springtail_oid,
                                    Oid pg_oid,
                                    int32_t atttypmod);

        /** Helper to setup quals and scan iterator in state, called from begin_scan */
        void _init_quals(PgFdwState *state, const List *qual_list);

        /** Helper to create constant field from qual and add to field array */
        void _make_const_field(const PgFdwState *state, const SchemaColumn &column, int idx, const ConstQual *qual);

        // static methods

        /** Helper to convert a PG type OID to a type name using the PG system cache. */
        static std::string _get_type_name(int32_t pg_type,
                                          const std::unordered_map<uint64_t, std::string> &user_types);

        /** Helper to generate create foreign table sql */
        static std::string _gen_fdw_table_sql(const std::string &server_name,
                                              const std::string &schema,
                                              const std::string &table,
                                              uint64_t tid,
                                              const std::vector<std::tuple<std::string, std::string, bool>> &columns);

        /** Helper to generate a system table create foreign table sql */
        static std::string _gen_fdw_system_table(const std::string &server,
            const std::string &table,
            uint64_t tid,
            const std::vector<SchemaColumn> &columns);

        /** Helper to iterate through system tables to generate import command list */
        static List *_import_springtail_catalog(const std::string &server,
                                                const std::set<std::string, std::less<>> &table_set,
                                                bool exclude, bool limit);

        static void _handle_exception(const Error &e);

        /** Helper to determine if a type can be used in a where clause */
        static bool _is_type_sortable(Oid pg_type, QualOpName op);

        /** Helper to compare a primary key const qual field to the data within a row */
        static bool _compare_field(const void* row,
                                   const FieldPtr& row_field,
                                   const FieldPtr& key_field,
                                   QualOpName op);

        /** Helper to set/reset scan iterators from beginning based on quals */
        static void _set_scan_iterators(PgFdwState *state);

        /** Helper to generate tuple used in upper/lower bound calculations for scan iter */
        static FieldTuplePtr _gen_qual_tuple(const std::vector<ConstQualPtr> &quals,
                                             const FieldArrayPtr qual_fields);

        /** Helper to get the user type names for a namespace used by import foreign schema */
        static std::unordered_map<uint64_t, std::string> _load_user_types(uint64_t db_id,
            const std::string &namespace_name,
            uint64_t namespace_id,
            uint64_t schema_xid);

        /** Helper to convert binary field to datum */
        static Datum _binary_to_datum(const std::span<const char> &value,
                                      Oid pg_oid,
                                      int32_t atttypmod);

        /** Helper to get the index quals for a given index */
        friend std::vector<ConstQualPtr>
        _get_index_quals(const PgFdwState *state, Index const& idx, List const* qual_list);

        /** Helper to create an IPC cache for table roots */
        void _try_create_cache();

        /** Helper function to get the last xid */
        uint64_t _update_last_xid(uint64_t schema_xid);
    };
} // namespace springtail::pg_fdw
