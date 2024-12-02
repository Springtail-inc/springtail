
#include <common/properties.hh>

#include <pg_log_mgr/pg_log_coordinator.hh>
#include <pg_log_mgr/pg_log_mgr.hh>

namespace springtail::pg_log_mgr {

    void
    PgLogCoordinator::_internal_shutdown()
    {
        std::unique_lock lock(_mutex);
        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Shutting down {} log mgrs", _log_mgrs.size());
        for (auto &lm: _log_mgrs) {
            lm.second->shutdown();
            lm.second->join();
        }
        lock.unlock();
    }

    void
    PgLogCoordinator::add_database(uint64_t db_id)
    {
        // read instance config
        std::string host, user_name, password;
        int port;
        Properties::get_primary_db_config(host, port, user_name, password);

        // read log mgr config
        nlohmann::json log_mgr_config = Properties::get(Properties::LOG_MGR_CONFIG);
        std::filesystem::path repl_log_path = Properties::make_absolute_path(log_mgr_config["replication_log_path"]) / std::to_string(db_id);
        std::filesystem::path xact_log_path = Properties::make_absolute_path(log_mgr_config["transaction_log_path"]) / std::to_string(db_id);

        // read db config for this db id
        nlohmann::json db_config = Properties::get_db_config(db_id);
        std::string db_name = db_config["name"];
        std::string pub_name = db_config["publication_name"];
        std::string slot_name = db_config["replication_slot"];

        // create log mgr
        std::unique_lock lock(_mutex);

        auto itr = _log_mgrs.find(db_id);
        if (itr != _log_mgrs.end()) {
            return;
        }

        PgLogMgrPtr log_mgr = std::make_shared<PgLogMgr>(db_id, repl_log_path, xact_log_path, host, db_name, user_name, password, pub_name, slot_name, port);
        _log_mgrs[db_id] = log_mgr;

        lock.unlock();

        // startup log mgr
        log_mgr->startup();
    }

    void
    PgLogCoordinator::remove_database(uint64_t db_id)
    {
        std::unique_lock lock(_mutex);

        auto itr = _log_mgrs.find(db_id);
        if (itr == _log_mgrs.end()) {
            return;
        }
        _log_mgrs.erase(itr);
        lock.unlock();

        itr->second->shutdown();
        itr->second->join();
    }
}
