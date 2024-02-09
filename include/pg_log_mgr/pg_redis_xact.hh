#pragma once

#include <string>
#include <filesystem>
#include <vector>
#include <cassert>

#include <fmt/core.h>

#include <common/common.hh>
#include <common/redis.hh>

namespace springtail {
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
         * @param begin_offset file offset for begin message
         * @param commit_offset file offset for commit message
         * @param xact_lsn transaction LSN
         * @param xid springtail XID
         * @param pg_xid postgres XID
         */
        PgRedisXactValue(const std::filesystem::path &begin_path,
                         const std::filesystem::path &commit_path,
                         uint64_t begin_offset, uint64_t commit_offset,
                         uint64_t xact_lsn, uint64_t xid, uint32_t pg_xid)
            : begin_path(begin_path), commit_path(commit_path),
              begin_offset(begin_offset), commit_offset(commit_offset),
              xact_lsn(xact_lsn), xid(xid), pg_xid(pg_xid)
        {}

        /**
         * @brief Deserialization constructor
         * @param string_value
         */
        PgRedisXactValue(const std::string &string_value)
        {
            std::vector<std::string> split;
            common::split_string(":", string_value, split);

            assert(split.size() == 7);

            // serialized order: xid, pg_xid, xact_lsn, begin_path, begin_offset, commit_path, commit_offset
            xid = std::stoull(split[0]); // xid
            pg_xid = std::stoul(split[1]); // pg_xid
            xact_lsn = std::stoull(split[2]); // lsn
            begin_path = std::filesystem::path(split[3]); // begin path
            begin_offset = std::stoull(split[4]); // begin offset
            commit_path = std::filesystem::path(split[5]); // commit path
            commit_offset = std::stoull(split[6]); // commit offset
        }

        /**
         * @brief Serialize data to string
         * @return std::string
         */
        std::string serialize() const
        {
            return fmt::format("{}:{}:{}:{}:{}:{}:{}", xid, pg_xid, xact_lsn, begin_path.c_str(),
                               begin_offset, commit_path.c_str(), commit_offset);
        }

        std::filesystem::path begin_path;
        std::filesystem::path commit_path;
        uint64_t begin_offset;
        uint64_t commit_offset;
        uint64_t xact_lsn;
        uint64_t xid;
        uint32_t pg_xid;
    };
}