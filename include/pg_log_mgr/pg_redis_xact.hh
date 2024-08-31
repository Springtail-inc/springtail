#pragma once

#include <string>
#include <filesystem>
#include <set>
#include <vector>
#include <cassert>

#include <fmt/core.h>

#include <common/common.hh>
#include <common/redis.hh>

namespace springtail::pg_log_mgr {
    /**
     * @brief Postgres Redis Value for transaction queue
     */
    class PgRedisXactValue
    {
    public:
        /**
         * @brief Construct a new Pg Redis Xact Value object
         * @param begin_path   file path holding begin message
         * @param commit_path  file path holding commit message
         * @param db_id        database id
         * @param begin_offset file offset for begin message
         * @param commit_offset file offset for commit message
         * @param xact_lsn transaction LSN
         * @param xid springtail XID
         * @param pg_xid postgres XID
         */
        PgRedisXactValue(const std::filesystem::path &begin_path,
                         const std::filesystem::path &commit_path,
                         uint64_t db_id, uint64_t begin_offset, uint64_t commit_offset,
                         uint64_t xact_lsn, uint64_t xid, uint32_t pg_xid,
                         const std::set<uint32_t> &aborted_xids)
            : begin_path(begin_path), commit_path(commit_path),
              begin_offset(begin_offset), commit_offset(commit_offset),
              xact_lsn(xact_lsn), xid(xid), db_id(db_id), pg_xid(pg_xid),
              aborted_xids(aborted_xids)
        {}

        /**
         * @brief Deserialization constructor
         * @param string_value
         */
        PgRedisXactValue(const std::string &string_value)
        {
            std::vector<std::string> split;
            common::split_string(":", string_value, split);

            assert(split.size() >= 7);

            // serialized order: xid, pg_xid, xact_lsn, begin_path, begin_offset, commit_path, commit_offset
            db_id = std::stoull(split[0]); // db_id
            xid = std::stoull(split[1]); // xid
            pg_xid = std::stoul(split[2]); // pg_xid
            xact_lsn = std::stoull(split[3]); // lsn
            begin_path = std::filesystem::path(split[4]); // begin path
            begin_offset = std::stoull(split[5]); // begin offset
            commit_path = std::filesystem::path(split[6]); // commit path
            commit_offset = std::stoull(split[7]); // commit offset
            int aborted_xids_size = std::stoi(split[8]); // aborted xids size

            for (int i = 0; i < aborted_xids_size; i++) { // aborted xids
                aborted_xids.insert(std::stoul(split[9 + i], nullptr, 16));
            }
        }

        /**
         * @brief Serialize data to string
         * @return std::string
         */
        std::string serialize() const
        {
            std::string xid_str = fmt::format("{}:", aborted_xids.size());
            for (auto xid : aborted_xids) {
                xid_str += fmt::format("{:X}:", xid);
            }
            return fmt::format("{}:{}:{}:{}:{}:{}:{}:{}:{}", db_id, xid, pg_xid, xact_lsn, begin_path.c_str(),
                               begin_offset, commit_path.c_str(), commit_offset, xid_str);
        }

        std::filesystem::path begin_path;
        std::filesystem::path commit_path;
        uint64_t begin_offset;
        uint64_t commit_offset;
        uint64_t xact_lsn;
        uint64_t xid;
        uint64_t db_id; ///< database id
        uint32_t pg_xid;
        std::set<uint32_t> aborted_xids;
    };

    /**
     * @brief Postgres Redis Value for OID sorted set.  Score is xid, value is xid:oid
     */
    class PgRedisOidValue {
    public:
        PgRedisOidValue(uint64_t oid, uint64_t xid)
            : oid(oid), xid(xid)
        {}

        PgRedisOidValue(const std::string &string_value)
        {
            std::vector<std::string> split;
            common::split_string(":", string_value, split);

            assert(split.size() == 2);

            oid = std::stoull(split[0]);
            xid = std::stoull(split[1]);
        }

        std::string serialize() const
        {
            return fmt::format("{}:{}", oid, xid);
        }

        uint64_t oid;
        uint64_t xid;
    };
    using RSSOidValue = RedisSortedSet<PgRedisOidValue>;
    using RSSOidValuePtr = std::shared_ptr<RSSOidValue>;

} // namespace springtail::pg_log_mgr
