#pragma once

#include <mutex>
#include <memory>
#include <optional>

#include <storage/table.hh>
#include <storage/table_mgr.hh>
#include <storage/constants.hh>
#include <storage/field.hh>
#include <storage/system_tables.hh>
#include <storage/schema.hh>

#include <xid_mgr/xid_mgr_client.hh>

/* These are defined by Thrift imported from xid_mgr_client.h */
#undef PACKAGE_STRING
#undef PACKAGE_VERSION
#undef UINT64CONST

extern "C" {
    #include <postgres.h>
    #include <nodes/pg_list.h>
}

namespace springtail  {

    /** Internal state used to track table scan */
    struct PgFdwState {
        TablePtr table;
        uint64_t tid;
        uint64_t xid;
        FieldArrayPtr fields;
        std::optional<Table::Iterator> iter;

        /** Constructor */
        PgFdwState(TablePtr table, uint64_t tid, uint64_t xid, Table::Iterator iter)
            : table(table), tid(tid), xid(xid), iter(iter)
        {
            fields = table->extent_schema()->get_fields();
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

        /** Begin scan */
        PgFdwState *fdw_begin(uint64_t tid, uint64_t xid=0);

        /** Iterate scan -- get next row */
        bool fdw_iterate_scan(PgFdwState *state, Datum *values, bool *isnull);

        /** End scan -- free state */
        void fdw_end(PgFdwState *state);

        /** Reset scan -- set iterator to beginning */
        void fdw_reset_scan(PgFdwState *state);

        /** Import foreign schema -- scan through system table generating sql for create foreign table */
        List *fdw_import_foreign_schema(const std::string &server, const std::string &schema,
                                        const List *table_list, bool exclude, bool limit);

    private:
        /** Delete constructor */
        PgFdwMgr() {}
        PgFdwMgr(const PgFdwMgr&) = delete;
        PgFdwMgr& operator=(const PgFdwMgr&) = delete;

        static PgFdwMgr* _instance;        ///< Singleton instance
        static std::once_flag _init_flag;  ///< Initialization flag
        static PgFdwMgr* _init();          ///< Initialize singleton

        /** Helper to convert field to PG Datum */
        static Datum _get_datum_from_field(FieldPtr field, const Extent::Row &row);

        /** Helper to generate create foreign table sql */
        static std::string _gen_fdw_table_sql(const std::string &server,
                                              const std::string &schema,
                                              const std::string &table,
                                              uint64_t tid,
                                              std::vector<std::tuple<std::string, uint8_t, bool, std::optional<std::string>>> &columns);

        /** Helper to generate a system table create foreign table sql */
        static std::string _gen_fdw_system_table(const std::string &server,
                                                 const std::string &schema,
                                                 const std::string &table,
                                                 uint64_t tid,
                                                 const std::vector<SchemaColumn> &columns);

        /** Helper to iterate through system tables to generate import command list */
        static List *_import_springtail_catalog(const std::string &server, const std::string &schema,
                                                const std::set<std::string> table_set, bool exclude, bool limit);
    };
}