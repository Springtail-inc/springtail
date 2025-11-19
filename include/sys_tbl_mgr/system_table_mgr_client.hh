#pragma once

#include <common/init.hh>
#include <sys_tbl_mgr/system_table_mgr.hh>

namespace springtail {

    /**
     * Client-side system table manager. Provides read-only access to system tables.
     * This is a singleton used by FDW and other client-side code.
     */
    class SystemTableMgrClient : public Singleton<SystemTableMgrClient>,
                                 public SystemTableMgr
    {
    public:
        SystemTableMgrClient();
        ~SystemTableMgrClient() override = default;
    };

} // namespace springtail
