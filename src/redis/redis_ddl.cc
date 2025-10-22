#include <common/common.hh>
#include <common/constants.hh>
#include <common/logging.hh>
#include <common/properties.hh>
#include <common/redis_types.hh>

#include <redis/redis_ddl.hh>
#include <redis/redis_db_tables.hh>
#include <redis/redis_containers.hh>

namespace {
    using namespace springtail;

    // Store DDL value in the pre-commit hash for crash recovery
    // Note: DDLs are stored in-memory and only moved to Redis pre-commit phase
    void _precommit(
            RedisClient& redis,
            uint64_t db_id,
            uint64_t xid,
            nlohmann::json ddls
            )
    {
        nlohmann::json op;
        op["db_id"] = db_id;
        op["xid"] = xid;
        op["ddls"] = ddls;
        std::string value = nlohmann::to_string(op);

        std::string precommit_key = fmt::format(redis::HASH_DDL_PRECOMMIT,
                                                Properties::get_db_instance_id());
        std::string hkey = fmt::format("{}:{}", db_id, xid);

        // Store the DDL value in the pre-commit hash for crash recovery
        redis.hset(precommit_key, hkey, value);
    }
}

namespace springtail {

    void
    RedisDDL::add_ddl(uint64_t db_id,
                      uint64_t xid,
                      const std::string &ddl)
    {
        // Store DDL in in-memory cache for fast access by Committer
        {
            std::unique_lock<std::shared_mutex> lock(_ddl_cache_mutex);
            _ddl_cache[db_id][xid].push_back(nlohmann::json::parse(ddl));
        }
    }

    nlohmann::json
    RedisDDL::get_ddls_xid(uint64_t db_id,
                           uint64_t xid)
    {
        // Retrieve DDL from in-memory cache
        {
            std::shared_lock<std::shared_mutex> lock(_ddl_cache_mutex);
            auto db_it = _ddl_cache.find(db_id);
            if (db_it != _ddl_cache.end()) {
                auto xid_it = db_it->second.find(xid);
                if (xid_it != db_it->second.end()) {
                    nlohmann::json ddls;
                    for (const auto &ddl : xid_it->second) {
                        ddls.push_back(ddl);
                    }
                    return ddls;
                }
            }
        }
        // Return empty array if not found (no DDLs for this XID)
        return nlohmann::json::array();
    }


    void
    RedisDDL::clear_ddls_xid(uint64_t db_id,
                             uint64_t xid)
    {
        // Clear from in-memory cache
        {
            std::unique_lock<std::shared_mutex> lock(_ddl_cache_mutex);
            auto db_it = _ddl_cache.find(db_id);
            if (db_it != _ddl_cache.end()) {
                db_it->second.erase(xid);
                // Clean up empty db_id entries to prevent unbounded map growth
                if (db_it->second.empty()) {
                    _ddl_cache.erase(db_id);
                }
            }
        }
    }

    void
    RedisDDL::precommit_ddl(uint64_t db_id,
                            uint64_t xid,
                            nlohmann::json ddls)
    {
        // Move DDLs to Redis pre-commit phase for crash recovery
        _precommit(*_redis, db_id, xid, ddls);

        // Clear from in-memory cache now that they're safely in Redis pre-commit storage
        clear_ddls_xid(db_id, xid);
    }

    void
    RedisDDL::commit_ddl(uint64_t db_id,
                         uint64_t xid)
    {
        uint64_t db_instance_id = Properties::get_db_instance_id();
        std::string precommit_key = fmt::format(redis::HASH_DDL_PRECOMMIT, db_instance_id);

        // get the pre-committed DDLs and figure out which ones we can commit based on the given XID
        // note: we use a map to make sure we apply things in XID order
        std::map<uint64_t, std::string> commit_keys;
        std::vector<std::string> hkeys;

        _redis->hkeys(precommit_key, std::back_inserter(hkeys));
        for (const auto &key : hkeys) {
            std::vector<std::string> split;
            common::split_string(":", key, split);

            uint64_t entry_xid = stoull(split[1]);
            if (stoull(split[0]) == db_id && entry_xid <= xid) {
                commit_keys.try_emplace(entry_xid, key);
            }
        }

        // if there are no keys to commit, then return
        if (commit_keys.empty()) {
            return;
        }

        // pull each key's value and prepare for the transaction
        // note: this is safe because the values of individual keys won't change once written
        std::map<std::string, std::string> commit_map;
        for (const auto &entry : commit_keys) {
            const auto &key = entry.second;
            auto &&value = _redis->hget(precommit_key, key);
            CHECK(value.has_value());

            commit_map.emplace(key, *value);
        }

        // move from the pre-commit to the DDL queue of each FDW, all in a single transaction
        auto ts = _redis->transaction(false, false);

        for (const auto &entry : commit_map) {
            const auto &key = entry.first;
            nlohmann::json ddls = nlohmann::json::parse(entry.second);

            // iterate through the DDL statements and see if any
            // result in the addition or removal of a table/schema
            // or the renaming of a table and add them to the table set for this db
            for (auto ddl: ddls.at("ddls")) {
                assert(ddl.is_object());
                assert(ddl.contains("action"));
                auto &action = ddl.at("action");

                // only care about create, drop and rename
                if (action == "create") {
                    auto schema = ddl.at("schema").get<std::string>();
                    auto table = ddl.at("table").get<std::string>();
                    RedisDbTables::add_table(ts, db_id, table, schema);
                } else if (action == "drop") {
                    auto schema = ddl.at("schema").get<std::string>();
                    auto table = ddl.at("table").get<std::string>();
                    RedisDbTables::remove_table(ts, db_id, table, schema);
                } else if (action == "rename") {
                    auto schema = ddl.at("schema").get<std::string>();
                    auto table = ddl.at("table").get<std::string>();
                    auto old_schema = ddl.at("old_schema").get<std::string>();
                    auto old_table = ddl.at("old_table").get<std::string>();
                    RedisDbTables::remove_table(ts, db_id, old_table, old_schema);
                    RedisDbTables::add_table(ts, db_id, table, schema);
                }
            }

            // get the set of FDWs
            std::vector<std::string> fdw_ids = Properties::get_fdw_ids();

            for (const std::string &fdw_id : fdw_ids) {
                std::string fdw_key = fmt::format(redis::QUEUE_DDL_FDW, db_instance_id, fdw_id);
                // note: this is equivalent to RedisQueue::push()
                ts.lpush(fdw_key, entry.second);
            }
            ts.hdel(precommit_key, key);
        }
        ts.exec();
    }

    std::vector<std::pair<uint64_t, uint64_t>>
    RedisDDL::get_precommit_ddl()
    {
        std::vector<std::pair<uint64_t, uint64_t>> keys;

        // retrieve the pre-commit keys
        std::vector<std::string> hkeys;
        std::string precommit_key = fmt::format(redis::HASH_DDL_PRECOMMIT,
                                                Properties::get_db_instance_id());
        _redis->hkeys(precommit_key, std::back_inserter(hkeys));

        // keys are stored as "db_id:xid", so split and store them
        for (const auto &key : hkeys) {
            std::vector<std::string> split_key;
            common::split_string(":", key, split_key);

            keys.push_back({ stoull(split_key[0]), stoull(split_key[1]) });
        }

        return keys;
    }

    void
    RedisDDL::abort_ddl(uint64_t db_id,
                        uint64_t xid)
    {
        std::string precommit_key = fmt::format(redis::HASH_DDL_PRECOMMIT,
                                                Properties::get_db_instance_id());
        std::string hkey = fmt::format("{}:{}", db_id, xid);

        _redis->hdel(precommit_key, hkey);
    }


    std::vector<nlohmann::json>
    RedisDDL::get_next_ddls(const std::string &fdw_id)
    {
        // retrieve the next set of DDLs to apply for the given FDW; this blocks
        std::string key = fmt::format(redis::QUEUE_DDL_FDW, Properties::get_db_instance_id(), fdw_id);
        RedisQueue<std::string> queue(key);

        auto value = queue.pop("active", 2);
        if (value == nullptr) {
            return {};
        }

        std::vector<nlohmann::json> ddls;
        do {
            ddls.push_back(nlohmann::json::parse(*value));
            value = queue.try_pop("active");
        } while (value != nullptr);

        return ddls;
    }

    void
    RedisDDL::abort_fdw(const std::string &fdw_id)
    {
        std::string key = fmt::format(redis::QUEUE_DDL_FDW, Properties::get_db_instance_id(), fdw_id);
        RedisQueue<std::string> queue(key);
        queue.abort("active");
    }

    void
    RedisDDL::commit_fdw_no_update(const std::string &fdw_id)
    {
        std::string key = fmt::format(redis::QUEUE_DDL_FDW, Properties::get_db_instance_id(), fdw_id);
        RedisQueue<std::string> queue(key);
        queue.commit("active");
    }

    void
    RedisDDL::update_schema_xid(const std::string &fdw_id,
                                uint64_t db_id,
                                uint64_t schema_xid)
    {
        // update the hash entry for the FDW with the latest schema XID
        std::string key = fmt::format(redis::HASH_DDL_FDW, Properties::get_db_instance_id());
        _redis->hset(key, fmt::format("{}:{}", db_id, fdw_id), std::to_string(schema_xid));

        // commit the DDLs that we just applied now that the schema XID is stored in Redis
        std::string queue_key = fmt::format(redis::QUEUE_DDL_FDW, Properties::get_db_instance_id(), fdw_id);
        RedisQueue<std::string> queue(queue_key);
        queue.commit("active");
    }

    uint64_t
    RedisDDL::get_schema_xid(const std::string &fdw_id, uint64_t db_id)
    {
        // update the hash entry for the FDW with the latest schema XID
        std::string key = fmt::format(redis::HASH_DDL_FDW, Properties::get_db_instance_id());
        // read latest schema xid from redis
        uint64_t schema_xid = 0;
        std::string redis_key = fmt::format("{}:{}", fdw_id, db_id);
        std::optional<std::string> schema_xid_value = _redis->hget(key, redis_key);
        if (schema_xid_value.has_value()) {
            schema_xid = std::stoull(schema_xid_value.value());
        }
        return schema_xid;
    }

    uint64_t
    RedisDDL::min_schema_xid(uint64_t db_id)
    {
        std::string key = fmt::format(redis::HASH_DDL_FDW, Properties::get_db_instance_id());

        // retrieve the schema XID for all FDWs for db_id
        std::string match = fmt::format("{}:*", db_id);
        std::map<std::string, std::string> values;

        // redis hscan hash_key match
        auto cursor = 0;
        while (true) {
            cursor = _redis->hscan(key, cursor, match, 100, std::inserter(values, values.begin()));
            if (cursor == 0) {
                break;
            }
        }

        // find the minimum XID across the FDWs
        uint64_t min_xid = constant::LATEST_XID;
        for (const auto &value : values) {
            uint64_t xid = std::stoull(value.second);
            if (xid < min_xid) {
                min_xid = xid;
            }
        }

        return min_xid;
    }

    uint64_t
    RedisDDL::min_fdw_xid(uint64_t db_id)
    {
        /*---------------- Check if FDW is starting up for the DB ---------------------------------*/
        std::string fdw_pid_key = fmt::format(redis::SET_FDW_PID, Properties::get_db_instance_id());
        const std::string token = ":" + std::to_string(db_id) + ":";
        uint64_t cursor = 0;

        do {
            std::vector<std::string> batch;
            cursor = _redis->sscan(fdw_pid_key, cursor, "*", 100, std::back_inserter(batch));

            // Check if there is any entry with {}:db_id:{}, if so, return min_xid as 0
            for (const auto& val : batch) {
                if (val.find(token) != std::string::npos) {
                    return 0;
                }
            }

        } while (cursor != 0);

        /*---------------- End of FDW startup check ----------------------------------------------*/

        /*---------------- Get min XID across FDWs for DB ----------------------------------------*/

        std::string fdw_xid_key = fmt::format(redis::HASH_MIN_XID, Properties::get_db_instance_id());
        std::map<std::string, std::string> values;
        std::string match = fmt::format("*:{}", db_id);

        cursor = 0;
        while (true) {
            cursor = _redis->hscan(fdw_xid_key, cursor, match, 100, std::inserter(values, values.begin()));
            if (cursor == 0) {
                break;
            }
        }

        uint64_t min_xid = constant::LATEST_XID;
        std::string suffix = ":" + db_id;

        for (const auto& [field, value] : values) {
            DCHECK(field.ends_with(suffix));
            uint64_t xid = std::stoull(value);
            if (xid < min_xid) {
                min_xid = xid;
            }
        }

        return min_xid;
        /*---------------- End of min fdw XID retrieval ------------------------------------------*/
    }

    void
    RedisDDL::insert_index_xid(uint64_t db_id, uint64_t xid)
    {
        std::string key = fmt::format(redis::SET_DB_INDEX_XIDS, Properties::get_db_instance_id(), db_id);
        RedisSortedSet<std::string> index_xid_set(key);
        index_xid_set.add(std::to_string(xid), xid);
    }

    void
    RedisDDL::remove_index_xid(uint64_t db_id, uint64_t xid)
    {
        std::string key = fmt::format(redis::SET_DB_INDEX_XIDS, Properties::get_db_instance_id(), db_id);
        RedisSortedSet<std::string> index_xid_set(key);
        index_xid_set.remove(std::to_string(xid));
    }

    uint64_t
    RedisDDL::min_index_xid(uint64_t db_id)
    {
        std::string key = fmt::format(redis::SET_DB_INDEX_XIDS, Properties::get_db_instance_id(), db_id);
        RedisSortedSet<std::string> index_xid_set(key);
        auto first_xid_entry = index_xid_set.get_first_by_score();
        auto min_xid = constant::LATEST_XID;
        if (first_xid_entry) {
            min_xid = std::stoull(first_xid_entry.value());
        }
        return min_xid;
    }
}
