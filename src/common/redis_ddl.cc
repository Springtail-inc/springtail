#include <common/constants.hh>
#include <common/logging.hh>
#include <common/redis_ddl.hh>
#include <common/redis_types.hh>
#include <common/properties.hh>

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
    RedisDDL::commit_ddl(uint64_t db_id, uint64_t xid, nlohmann::json ddls)
    {
        nlohmann::json op;
        op["db_id"] = db_id;
        op["xid"] = xid;
        op["ddls"] = ddls;

        std::string value = nlohmann::to_string(op);

        // get the set of FDWs
        uint64_t db_instance_id = Properties::get_db_instance_id();
        std::vector<std::string> fdw_ids = Properties::get_fdw_ids();

        // add the DDLs to the queue for each FDW
        for (const std::string &fdw_id : fdw_ids) {
            std::string key = fmt::format(redis::QUEUE_DDL_FDW, db_instance_id, fdw_id);
            _redis->rpush(key, value);
        }
    }

    nlohmann::json
    RedisDDL::get_next_ddls(const std::string &fdw_id)
    {
        nlohmann::json ddls;

        // retrieve the next set of DDLs to apply for the given FDW; this blocks
        std::string key = fmt::format(redis::QUEUE_DDL_FDW, Properties::get_db_instance_id(), fdw_id);
        auto &&res = _redis->blpop(key);
        if (!res) {
            return ddls;
        }

        ddls = nlohmann::json::parse(res->second);
        return ddls;
    }

    void
    RedisDDL::update_schema_xid(const std::string &fdw_id,
                                uint64_t schema_xid)
    {
        // update the hash entry for the FDW with the latest schema XID
        std::string key = fmt::format(redis::HASH_DDL_FDW, Properties::get_db_instance_id());
        _redis->hset(key, fdw_id, std::to_string(schema_xid));
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
