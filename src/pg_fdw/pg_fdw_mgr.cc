
#include <pg_fdw/pg_fdw_mgr.h>

namespace springtail {
    PgFdwMgr* PgFdwMgr::_instance {nullptr};

    std::once_flag PgFdwMgr::_init_flag;

    PgFdwMgr*
    PgFdwMgr::_init()
    {
        _instance = new PgFdwMgr();
        return _instance;
    }

    PgFdwMgr::PgFdwMgr() {
    }
}