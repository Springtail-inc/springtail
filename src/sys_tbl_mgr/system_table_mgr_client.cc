#include <sys_tbl_mgr/system_table_mgr_client.hh>

using namespace springtail;

SystemTableMgrClient::SystemTableMgrClient()
    : Singleton<SystemTableMgrClient>(ServiceId::SystemTableMgrClientId),
      SystemTableMgr()
{
}
