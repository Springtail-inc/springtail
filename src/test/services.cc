#include <test/services.hh>

#include <common/json.hh>
#include <common/properties.hh>
#include <storage/index_engine/shared_extent_pool.hh>
#include <sys_tbl_mgr/client.hh>
#include <sys_tbl_mgr/server.hh>
#include <write_cache/write_cache_client.hh>
#include <write_cache/write_cache_server.hh>
#include <xid_mgr/xid_mgr_client.hh>
#include <xid_mgr/xid_mgr_server.hh>

namespace springtail::test {

    void
    start_xid_mgr()
    {
        std::filesystem::path xid_dir;

        auto json = Properties::get(Properties::LOG_MGR_CONFIG);
        if (json.is_null() || !json.contains("rpc_config")) {
            throw Error("LOG Mgr config incorrect");
        }

        Json::get_to<std::filesystem::path>(json, "transaction_log_path", xid_dir);
        xid_dir = Properties::make_absolute_path(xid_dir);
        std::filesystem::remove_all(xid_dir);
        xid_mgr::XidMgrServer::get_instance()->cleanup_on_shutdown();
    }

    void
    start_sys_tbl_mgr()
    {
        std::filesystem::path table_dir;

        auto json = Properties::get(Properties::STORAGE_CONFIG);
        Json::get_to<std::filesystem::path>(json, "table_dir", table_dir);
        table_dir = Properties::make_absolute_path(table_dir);
        std::filesystem::remove_all(table_dir);

        // SharedExtentPool (architecture.md "btree" redesign Sec 6) is deliberately host-wide and
        // outlives any one process -- that's the whole point in production, where many backend
        // processes on one host share it without any of them wiping it on their own
        // start/shutdown. But that same persistence means it does NOT get the fresh-start
        // `table_dir` above just got: unrelated test binaries that each simulate "a fresh system
        // from scratch" deterministically reuse the same low (file, extent_id) keys, and -- unlike
        // production, where a table's directory encodes a monotonically-unique table_id/xid that's
        // never reused for different content -- would otherwise silently hand a later test
        // whatever a completely unrelated earlier test published under the same key. Clear it here,
        // in the same place table_dir itself gets reset, rather than in production-reachable code.
        SharedExtentPool::remove(SharedExtentPool::DEFAULT_NAME);

        sys_tbl_mgr::Server::get_instance();
    }

    void
    start_services(bool xid_mgr, bool sys_tbl_mgr, bool write_cache)
    {
        if (sys_tbl_mgr) {
            xid_mgr = true;
        }

        if (xid_mgr) {
            start_xid_mgr();
        }

        if (sys_tbl_mgr) {
            start_sys_tbl_mgr();
        }

        if (write_cache) {
            WriteCacheServer::get_instance();
        }
    }
}
