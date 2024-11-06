#include <common/common.hh>
#include <common/constants.hh>
#include <common/logging.hh>
#include <common/properties.hh>
#include <common/redis_types.hh>

#include <redis/redis_ddl.hh>
#include <redis/redis_db_tables.hh>
#include <redis/redis_containers.hh>

namespace springtail {

    void
    RedisDDL::add_ddl(uint64_t db_id,
                      uint64_t xid,
                      const std::string &ddl)
    {
        std::string key = fmt::format(redis::QUEUE_DDL_XID,
                                      Properties::get_db_instance_id(), db_id, xid);

        // RPUSH ddl_queue:xid ddl
        _redis->rpush(key, ddl);
    }

    nlohmann::json
    RedisDDL::get_ddls_xid(uint64_t db_id,
                           uint64_t xid)
    {
        std::string ddl_key = fmt::format(redis::QUEUE_DDL_XID,
                                          Properties::get_db_instance_id(), db_id, xid);

        // retrieve the list of DDL operations for this XID
        std::vector<std::string> values;
        _redis->lrange(ddl_key, 0, -1, std::back_inserter(values));

        nlohmann::json ddls;
        for (std::string &value : values) {
            ddls.push_back(nlohmann::json::parse(value));
        }

        return ddls;
    }

    void
    RedisDDL::clear_ddls_xid(uint64_t db_id,
                             uint64_t xid)
    {
        std::string ddl_key = fmt::format(redis::QUEUE_DDL_XID,
                                          Properties::get_db_instance_id(), db_id, xid);
        _redis->del(ddl_key);
    }

    void
    RedisDDL::precommit_ddl(uint64_t db_id,
                            uint64_t xid,
                            nlohmann::json ddls)
    {
        // construct the DDL value and place it into the pre-commit hash in a single transaction
        // with clearing the DDL_XID queue

        nlohmann::json op;
        op["db_id"] = db_id;
        op["xid"] = xid;
        op["ddls"] = ddls;
        std::string value = nlohmann::to_string(op);

        std::string precommit_key = fmt::format(redis::HASH_DDL_PRECOMMIT,
                                                Properties::get_db_instance_id());
        std::string hkey = fmt::format("{}:{}", db_id, xid);
        std::string ddl_key = fmt::format(redis::QUEUE_DDL_XID,
                                          Properties::get_db_instance_id(), db_id, xid);

        // perform the pre-commit in a single transaction
        auto ts = _redis->transaction(false, false);
        ts.hset(precommit_key, hkey, value).del(ddl_key).exec();
    }

    void
    RedisDDL::commit_ddl(uint64_t db_id,
                         uint64_t xid)
    {
        uint64_t db_instance_id = Properties::get_db_instance_id();
        std::string precommit_key = fmt::format(redis::HASH_DDL_PRECOMMIT, db_instance_id);

        // get the pre-committed DDLs and figure out which ones we can commit based on the given XID
        std::vector<std::string> commit_keys;
        std::vector<std::string> hkeys;

        _redis->hkeys(precommit_key, std::back_inserter(hkeys));
        for (const auto &key : hkeys) {
            std::vector<std::string> split;
            common::split_string(":", key, split);
            if (stoull(split[0]) == db_id && stoull(split[1]) <= xid) {
                commit_keys.push_back(key);
            }
        }

        // move from the pre-commit to the DDL queue of each FDW, all in a single transaction
        for (const auto &key : commit_keys) {
            auto ts = _redis->transaction(false, false);
            auto r = ts.redis();
            // NOTE: if the precommit_key hash could change, then we should do a watch here
            auto &&value = r.hget(precommit_key, key);
            assert (value.has_value());

            // iterate through the DDL statements and see if any
            // result in the addition or removal of a table/schema
            // or the renaming of a table and add them to the table set for this db
            nlohmann::json ddls = nlohmann::json::parse(*value);
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
                ts.lpush(fdw_key, *value);
            }
            ts.hdel(precommit_key, key).exec();
        }
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


    nlohmann::json
    RedisDDL::get_next_ddls(const std::string &fdw_id)
    {
        nlohmann::json ddls;

        // retrieve the next set of DDLs to apply for the given FDW; this blocks
        std::string key = fmt::format(redis::QUEUE_DDL_FDW, Properties::get_db_instance_id(), fdw_id);
        RedisQueue<std::string> queue(key);

        auto value = queue.pop("active", 2);
        if (value == nullptr) {
            return ddls;
        }

        ddls = nlohmann::json::parse(*value);
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
                                uint64_t schema_xid)
    {
        // update the hash entry for the FDW with the latest schema XID
        std::string key = fmt::format(redis::HASH_DDL_FDW, Properties::get_db_instance_id());
        _redis->hset(key, fdw_id, std::to_string(schema_xid));

        // commit the DDLs that we just applied now that the schema XID is stored in Redis
        std::string queue_key = fmt::format(redis::QUEUE_DDL_FDW, Properties::get_db_instance_id(), fdw_id);
        RedisQueue<std::string> queue(queue_key);
        queue.commit("active");
    }

    uint64_t
    RedisDDL::min_schema_xid()
    {
        std::string key = fmt::format(redis::HASH_DDL_FDW, Properties::get_db_instance_id());

        // retrieve the schema XID for all FDWs
        std::vector<std::string> values;
        _redis->hvals(key, std::back_inserter(values));

        // find the minimium XID across the FDWs
        uint64_t min_xid = constant::LATEST_XID;
        for (const auto &value : values) {
            uint64_t xid = std::stoull(value);
            if (xid < min_xid) {
                min_xid = xid;
            }
        }

        return min_xid;
    }
}
