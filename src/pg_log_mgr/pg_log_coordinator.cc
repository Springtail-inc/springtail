#include <fmt/format.h>

#include <common/json.hh>
#include <common/properties.hh>

#include <pg_log_mgr/pg_log_coordinator.hh>
#include <pg_log_mgr/pg_log_mgr.hh>

#include <write_cache/write_cache_server.hh>

namespace springtail::pg_log_mgr {

    void
    PgLogCoordinator::_internal_shutdown()
    {
        // shutdown redis pubsub thread
        _config_sub_thread.shutdown();

        // shutdown the write cache thread
        WriteCacheServer::shutdown();

        // shut down all log managers
        std::unique_lock lock(_mutex);
        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Shutting down {} log mgrs", _log_mgrs.size());
        for (auto &lm: _log_mgrs) {
            lm.second->shutdown();
            lm.second->join();
        }
        lock.unlock();
    }

    void
    PgLogCoordinator::init()
    {
        // read instance config
        Properties::get_primary_db_config(_host, _port, _user_name, _password);

        // read log mgr config
        nlohmann::json log_mgr_config = Properties::get(Properties::LOG_MGR_CONFIG);
        auto optional_repl_log = Json::get<std::string>(log_mgr_config, "replication_log_path");
        auto optional_trans_log = Json::get<std::string>(log_mgr_config, "transaction_log_path");
        if (optional_repl_log.has_value() && optional_trans_log.has_value()) {
            _repl_log = optional_repl_log.value();
            _trans_log = optional_trans_log.value();
        } else {
            SPDLOG_ERROR("Error when reading pg_log_mgr config");
        }

        // get instance id
        _db_instance_id = Properties::get_db_instance_id();

        std::string db_change_channel = fmt::format(redis::PUBSUB_DB_CONFIG_CHANGES, _db_instance_id);
        _config_sub_thread.add_subscriber(db_change_channel,
            [this]() {
                this->_init_db_change_subscriber();
            },
            [this](const std::string &msg) {
                _handle_db_changes(msg);
            });

         _config_sub_thread.start();

         // create a thread for the write cache
         _write_cache_thread = std::thread([](){
             WriteCacheServer::startup();
         });
   }

    void
    PgLogCoordinator::_add_database(uint64_t db_id)
    {
        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Adding database {}", db_id);

        if (_log_mgrs.contains(db_id)) {
            return;
        }

        std::filesystem::path repl_log_path = Properties::make_absolute_path(_repl_log) / std::to_string(db_id);
        std::filesystem::path xact_log_path = Properties::make_absolute_path(_trans_log) / std::to_string(db_id);

        // read db config for this db id
        nlohmann::json db_config = Properties::get_db_config(db_id);
        std::string db_name = db_config["name"];
        std::string pub_name = db_config["publication_name"];
        std::string slot_name = db_config["replication_slot"];

        // acquire lock
        std::unique_lock lock(_mutex);

        // create log mgr
        PgLogMgrPtr log_mgr = std::make_shared<PgLogMgr>(db_id, repl_log_path, xact_log_path, _host, db_name, _user_name, _password, pub_name, slot_name, _port);
        _log_mgrs[db_id] = log_mgr;

        lock.unlock();

        // startup log mgr
        log_mgr->startup();
    }

    void
    PgLogCoordinator::_remove_database(uint64_t db_id)
    {
        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Removing database {}", db_id);
        std::unique_lock lock(_mutex);

        auto itr = _log_mgrs.find(db_id);
        if (itr == _log_mgrs.end()) {
            return;
        }
        auto log_mgr = itr->second;
        _log_mgrs.erase(itr);
        lock.unlock();

        log_mgr->shutdown();
        log_mgr->join();
    }

    void
    PgLogCoordinator::_init_db_change_subscriber()
    {
        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Initializing DB Change subscriber");
        std::map<uint64_t, std::string> db_ids = Properties::get_databases();
        for (auto &db: db_ids) {
            uint64_t db_id = db.first;
            _add_database(db_id);
        }
    }

    void
    PgLogCoordinator::_handle_db_changes(const std::string &msg)
    {
        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Handling DB Change Action message: {}", msg);
        uint64_t db_id;
        redis::db_state_change::DBAction action;
        redis::db_state_change::parse_db_action(msg, db_id, action);
        switch (action) {
            case redis::db_state_change::DB_ACTION_ADD:
                _add_database(db_id);
                break;
            case redis::db_state_change::DB_ACTION_REMOVE:
                _remove_database(db_id);
                break;
            default:
                SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Unsupported action: {}", redis::db_state_change::db_action_to_name[action]);
        }

    }
}
