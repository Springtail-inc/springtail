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
}

namespace springtail  {

    /** Internall state used to track table scan */
    struct PgFdwState {
        TablePtr table;
        uint64_t tid;
        uint64_t xid;
        FieldArrayPtr fields;
        std::optional<Table::Iterator> iter;

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
        static PgFdwMgr* get_instance() {
            std::call_once(_init_flag, _init);
            return _instance;
        }

        PgFdwState *fdw_begin(uint64_t tid);

        bool fdw_iterate_scan(PgFdwState *state, Datum *values, bool *isnull);

        void fdw_end(PgFdwState *state);

        void fdw_reset_scan(PgFdwState *state);

    private:
        PgFdwMgr() {}
        PgFdwMgr(const PgFdwMgr&) = delete;
        PgFdwMgr& operator=(const PgFdwMgr&) = delete;

        static PgFdwMgr* _instance;
        static std::once_flag _init_flag;
        static PgFdwMgr* _init();

        static Datum _get_datum_from_field(FieldPtr field, const Extent::Row &row);
    };
}