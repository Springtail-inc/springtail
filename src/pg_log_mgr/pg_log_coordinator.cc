#include <fmt/format.h>

#include <common/json.hh>
#include <common/properties.hh>

#include <pg_log_mgr/pg_log_coordinator.hh>
#include <pg_log_mgr/pg_log_mgr.hh>
#include <pg_log_mgr/xid_ready.hh>

#include <write_cache/write_cache_server.hh>

namespace springtail::pg_log_mgr {

    void
    PgLogCoordinator::_internal_shutdown()
    {
        std::shared_ptr<RedisCache> redis_cache = Properties::get_instance()->get_cache();
        redis_cache->remove_callback(Properties::DATABASE_IDS_PATH, _cache_watcher);

        // shut down all log managers
        std::unique_lock lock(_mutex);
        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Shutting down {} log mgrs", _log_mgrs.size());
        for (auto &lm: _log_mgrs) {
            lm.second->shutdown();
            lm.second->join();
        }
        lock.unlock();

        // stop committer thread
        _committer->shutdown();
        _committer_thread.join();
    }

    PgLogCoordinator::PgLogCoordinator()
    {
        _cache_watcher = std::make_shared<RedisCache::RedisChangeWatcher>(
            [this](const std::string &path, const nlohmann::json &new_value) -> void {
                SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Replicated databases: {}", new_value.dump(4));
                CHECK_EQ(path, Properties::DATABASE_IDS_PATH);
                // get a vector of old database ids from _log_mgrs
                std::unique_lock<std::mutex> lock(_mutex);
                auto keys = std::views::keys(_log_mgrs);
                lock.unlock();
                std::vector<uint64_t> old_db_ids{ keys.begin(), keys.end() };
                // get a vector of new database ids from new_value
                std::vector<uint64_t> new_db_ids = Properties::get_instance()->get_database_ids(new_value);
                // diff the vectors
                RedisCache::array_diff(old_db_ids, new_db_ids, true, true);
                // everything in old_db_ids needs to be removed
                for (auto db_id: old_db_ids) {
                    _remove_database(db_id);
                }
                // everything in new_db_ids needs to be added
                for (auto db_id: new_db_ids) {
                    _add_database(db_id);
                }
            }
        );
    }

    void
    PgLogCoordinator::init()
    {
        // read instance config
        Properties::get_primary_db_config(_host, _port, _user_name, _password);

        // initialize committer queue
        _committer_queue = std::make_shared<ConcurrentQueue<committer::XidReady>>();

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

        // Start the committer thread
        _committer = std::make_shared<springtail::committer::Committer>(1, _committer_queue);
        _committer_thread = std::thread(&springtail::committer::Committer::run, _committer);

        // get instance id
        _db_instance_id = Properties::get_db_instance_id();

        std::shared_ptr<RedisCache> redis_cache = Properties::get_instance()->get_cache();
        redis_cache->add_callback(Properties::DATABASE_IDS_PATH, _cache_watcher);

        std::map<uint64_t, std::string> db_ids = Properties::get_databases();
        for (auto &db: db_ids) {
            uint64_t db_id = db.first;
            _add_database(db_id);
        }
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
        PgLogMgrPtr log_mgr = std::make_shared<PgLogMgr>(db_id, repl_log_path, xact_log_path, _host, db_name, _user_name, _password, pub_name, slot_name, _port, _committer_queue);
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
}
