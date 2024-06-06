#pragma once

#include <mutex>

namespace springtail  {
    class PgFdwMgr {
    public:
        PgFdwMgr();
        PgFdwMgr* get_instance() {
            std::call_once(_init_flag, _init);
            return _instance;
        }
    private:
        static PgFdwMgr* _instance;
        static std::once_flag _init_flag;

        static PgFdwMgr* _init();
    };
}