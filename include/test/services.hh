#pragma once
#include <common/service_register.hh>

namespace springtail::test {
    std::vector<std::unique_ptr<ServiceRunner>> get_services(bool xid_mgr, bool sys_tbl_mgr, bool write_cache);
}
