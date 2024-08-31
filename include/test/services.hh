#pragma once

#include <vector>
#include <thread>

namespace springtail::test {
    class Services {
    public:
        Services(bool xid_mgr, bool sys_tbl_mgr, bool write_cache);

        void init(bool reset);
        void shutdown();

    private:
        bool _xid_mgr, _sys_tbl_mgr, _write_cache;
        std::vector<std::thread> _threads;
    };
}
