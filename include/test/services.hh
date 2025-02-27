#pragma once
#include <common/service_register.hh>

namespace springtail::test {
    std::vector<ServiceRunner *> getServices(bool xid_mgr, bool sys_tbl_mgr, bool write_cache);
}
