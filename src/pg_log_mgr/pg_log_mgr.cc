#include <pg_log_mgr/pg_log_mgr.hh>

namespace springtail {
    std::once_flag PgLogMgr::_init_flag;
    PgLogMgr *PgLogMgr::_instance {nullptr};

    void
    PgLogMgr::init()
    {
        _instance = new PgLogMgr();
    }

}