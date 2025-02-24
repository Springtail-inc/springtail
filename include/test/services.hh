#pragma once

namespace springtail::test {
    class Services {
    public:
        Services(bool xid_mgr, bool sys_tbl_mgr, bool write_cache);

        void init();

        void shutdown();

    private:
        bool _xid_mgr, _sys_tbl_mgr, _write_cache;
    };
}
