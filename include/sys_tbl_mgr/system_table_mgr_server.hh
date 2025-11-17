#pragma once

#include <common/init.hh>
#include <sys_tbl_mgr/system_table_mgr.hh>

namespace springtail {

    // Forward declarations
    class MutableTable;
    typedef std::shared_ptr<MutableTable> MutableTablePtr;

    /**
     * Server-side system table manager. Provides read-only and mutable access to system tables.
     * This is a singleton used by server daemons.
     */
    class SystemTableMgrServer : public Singleton<SystemTableMgrServer>,
                                 public SystemTableMgr
    {
    public:
        SystemTableMgrServer();
        ~SystemTableMgrServer() override = default;

        /**
         * Get a mutable system table for modification operations.
         * Only available on the server side.
         */
        MutableTablePtr
        get_mutable_system_table(uint64_t db_id, uint64_t table_id,
                                uint64_t access_xid, uint64_t target_xid);
    };

} // namespace springtail
