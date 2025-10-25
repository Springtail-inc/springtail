#include <common/coordinator.hh>
#include <common/filesystem.hh>
#include <common/json.hh>
#include <common/properties.hh>

#include <pg_log_mgr/pg_log_coordinator.hh>
#include <pg_log_mgr/pg_log_mgr.hh>
#include <pg_log_mgr/xid_ready.hh>

#include <storage/vacuumer.hh>
#include <sys_tbl_mgr/table_mgr.hh>
#include <write_cache/write_cache_server.hh>
#include <xid_mgr/xid_mgr_server.hh>

namespace springtail::pg_log_mgr {

    void
    PgLogCoordinator::_internal_shutdown()
    {
        std::shared_ptr<RedisCache> redis_cache = Properties::get_instance()->get_cache();
        redis_cache->remove_callback(Properties::DATABASE_IDS_PATH, _db_id_watcher);
        redis_cache->remove_callback(Properties::DATABASE_STATE_PATH, _db_state_watcher);

        // shut down all log managers
        std::unique_lock lock(_mutex);
        LOG_DEBUG(LOG_PG_LOG_MGR, LOG_LEVEL_DEBUG1, "Shutting down {} log mgrs", _log_mgrs.size());
        for (auto &lm: _log_mgrs) {
            lm.second->shutdown();
            lm.second->join();
        }
        lock.unlock();

        // stop committer thread
        _committer->shutdown();
        _committer_thread.join();

        if (AdminServer::exists()) {
            AdminServer::get_instance()->deregister_get_route("/info");
        }
    }

    PgLogCoordinator::PgLogCoordinator() : Singleton<PgLogCoordinator>(ServiceId::PgLogCoordinatorId)
    {
        _db_id_watcher = std::make_shared<RedisCache::RedisChangeWatcher>(
            [this](const std::string &path, const nlohmann::json &new_value) -> void {
                LOG_DEBUG(LOG_PG_LOG_MGR, LOG_LEVEL_DEBUG1, "Replicated databases: {}", new_value.dump(4));
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

        _db_state_watcher = std::make_shared<RedisCache::RedisChangeWatcher>(
            [this](const std::string &path, const nlohmann::json &new_value) -> void {
                LOG_DEBUG(LOG_PG_LOG_MGR, LOG_LEVEL_DEBUG1, "Replicated databases states: {}", new_value.dump(4));
                CHECK_EQ(path, Properties::DATABASE_STATE_PATH);

                // get new database keys from json object
                std::unique_lock<std::mutex> lock(_mutex);
                auto db_states_copy = _db_states;
                _db_states.clear();
                for (auto& [key, value] : new_value.items()) {
                    // get integer db_id from key
                    uint64_t db_id = std::stoull(key);

                    // get state
                    std::string state;
                    if (value.is_string()) {
                        state = value.get<std::string>();
                    } else {
                        throw Error("Invalid type of state value");
                    }

                    // add state
                    _db_states[db_id] = state;

                    // compare current state to the previous value
                    auto it = db_states_copy.find(db_id);
                    if (it != db_states_copy.end() && it->second != state) {
                        if (it->second == redis::db_state_change::REDIS_STATE_FAILED) {
                            // add database
                            lock.unlock();
                            _add_database(db_id);
                            lock.lock();
                        }
                        if (state == redis::db_state_change::REDIS_STATE_FAILED) {
                            // remove database
                            lock.unlock();
                            _remove_database(db_id);
                            lock.lock();
                        }
                    }
                }
            }
        );

        // register "/info" route with AdminServer
        if (AdminServer::exists()) {
            LOG_INFO("Registering admin server get path {}", program_invocation_short_name);

            AdminServer::get_instance()->register_get_route(
                "/info",
                [this]([[maybe_unused]] const std::string &path,
                   [[maybe_unused]] const httplib::Params &params,
                   nlohmann::json &json_response) {
                    json_response["write_cache"] = WriteCacheServer::get_instance()->get_memory_stats();
                    json_response["log_coordinator"] = get_stats();
                });
        }

    }

    void
    PgLogCoordinator::init()
    {
        // read instance config
        Properties::get_primary_db_config(_host, _port, _user_name, _password);

        // initialize committer queue
        _committer_queue = std::make_shared<ConcurrentQueue<committer::XidReady>>();

        // Initialize Index reconciliation queue manager
        _index_reconciliation_queue_mgr = std::make_shared<IndexReconciliationQueueManager>();

        // Initialize Index requests manager
        _index_requests_mgr = std::make_shared<IndexRequestsManager>();

        // read log mgr config
        nlohmann::json log_mgr_config = Properties::get(Properties::LOG_MGR_CONFIG);
        auto optional_repl_log = Json::get<std::string>(log_mgr_config, "replication_log_path");
        auto optional_trans_log = Json::get<std::string>(log_mgr_config, "transaction_log_path");
        auto indexer_worker_threads = Json::get_or<uint32_t>(log_mgr_config, "indexer_worker_threads", 1);
        _log_size_rollover_threshold = Json::get_or<uint64_t>(log_mgr_config, "log_size_rollover_threshold", PgLogMgr::LOG_ROLLOVER_SIZE_BYTES);
        _archive_logs = Json::get_or<bool>(log_mgr_config, "archive_logs", false);

        if (optional_repl_log.has_value() && optional_trans_log.has_value()) {
            _repl_log = optional_repl_log.value();
            _trans_log = optional_trans_log.value();
        } else {
            LOG_ERROR("Error when reading pg_log_mgr config");
        }

        // Start the committer thread
        _committer = std::make_shared<springtail::committer::Committer>(1, _committer_queue, _index_reconciliation_queue_mgr, _index_requests_mgr, indexer_worker_threads);
        _committer_thread = std::thread(&springtail::committer::Committer::run, _committer);
        pthread_setname_np(_committer_thread.native_handle(), "LogMgrCommitter");

        // get instance id
        _db_instance_id = Properties::get_db_instance_id();

        std::shared_ptr<RedisCache> redis_cache = Properties::get_instance()->get_cache();
        redis_cache->add_callback(Properties::DATABASE_IDS_PATH, _db_id_watcher);
        redis_cache->add_callback(Properties::DATABASE_STATE_PATH, _db_state_watcher);

        std::map<uint64_t, std::string> db_ids = Properties::get_databases();

        // acquire lock
        std::unique_lock lock(_mutex);
        for (auto &[db_id, db_name]: db_ids) {
            std::string state = Properties::get_db_state(db_id);
            _db_states[db_id] = state;
        }
        lock.unlock();

        for (auto &[db_id, db_name]: db_ids) {
            std::string state = Properties::get_db_state(db_id);
            if (state != redis::db_state_change::REDIS_STATE_FAILED) {
                _add_database(db_id);
            }
        }
    }

    nlohmann::json
    PgLogCoordinator::get_stats()
    {
        nlohmann::json json_stats =
            nlohmann::json::object({
                {"instance_id", _db_instance_id},
                {"archive_logs", _archive_logs},
                {"rollover_size", _log_size_rollover_threshold},
                {"host", _host},
                {"port", _port},
                {"user_name", _user_name},
                {"password", _password},
                {"log_mgrs", nullptr},
                {"db_states", nullptr}
            });
        {
            std::unique_lock lock(_mutex);
            nlohmann::json log_mgrs = nlohmann::json::object();
            for (auto &[db_id, log_mgr]: _log_mgrs) {
                log_mgrs[std::to_string(db_id)] = log_mgr->get_stats();
            }
            json_stats["log_mgrs"] = log_mgrs;
            nlohmann::json db_states = nlohmann::json::object();
            for (auto &[db_id, db_state]: _db_states) {
                db_states[std::to_string(db_id)] = db_state;
            }
            json_stats["db_states"] = db_states;
        }
        return json_stats;
    }

    void
    PgLogCoordinator::_add_database(uint64_t db_id)
    {
        LOG_DEBUG(LOG_PG_LOG_MGR, LOG_LEVEL_DEBUG1, "Adding database {}", db_id);

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

        // Add index reconciliation queue
        _index_reconciliation_queue_mgr->add_queue(db_id);

        // create log mgr
        auto log_mgr = std::make_shared<PgLogMgr>(db_id, repl_log_path, xact_log_path, _host, db_name, _user_name,
                                                         _password, pub_name, slot_name, _log_size_rollover_threshold,
                                                         _port, _archive_logs, _committer_queue, _index_reconciliation_queue_mgr, _index_requests_mgr);
        _log_mgrs[db_id] = log_mgr;

        lock.unlock();

        // startup log mgr
        log_mgr->startup();
    }

    void
    PgLogCoordinator::cleanup_database_dir(uint64_t db_id)
    {
        std::filesystem::path table_path = TableMgr::get_instance()->get_table_base() / std::to_string(db_id);
        // Remove database directory and everything inside it
        fs::remove_dir(table_path);
    }

    void
    PgLogCoordinator::_remove_database(uint64_t db_id)
    {
        LOG_DEBUG(LOG_PG_LOG_MGR, LOG_LEVEL_DEBUG1, "Removing database {}", db_id);
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

        // Remove index reconciliation queue for the db
        _index_reconciliation_queue_mgr->remove_queue(db_id);

        _committer->remove_db(db_id);

        // Cleanup from vacuumer
        Vacuumer::get_instance()->cleanup_db(db_id);

        // Cleanup write cache
        WriteCacheServer::get_instance()->drop_database(db_id);

        // Cleanup database in xid manager
        xid_mgr::XidMgrServer::get_instance()->cleanup(db_id);

        // Cleanup database in sys table manager
        sys_tbl_mgr::Server::get_instance()->remove_db(db_id);

        // Cleanup storage cache
        StorageCache::get_instance()->evict_for_database(db_id);

        // create database path and clear out all file handlers in IOMgr that start with this path
        std::filesystem::path db_path = TableMgr::get_instance()->get_table_base() / std::to_string(db_id);
        IOMgr::get_instance()->drop_all_fh(db_path);

        // cleanup database directory
        cleanup_database_dir(db_id);

        // cleanup replication logs directory
        std::filesystem::path repl_log_path = Properties::make_absolute_path(_repl_log) / std::to_string(db_id);
        fs::remove_dir(repl_log_path);

        // cleanup redis DDLs
        RedisDDL::get_instance()->clear_ddls(db_id);
    }
}
