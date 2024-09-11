#pragma once

#include <string>
#include <filesystem>
#include <set>
#include <vector>
#include <cassert>
#include <variant>

#include <fmt/core.h>

#include <common/common.hh>
#include <common/redis.hh>

namespace springtail::pg_log_mgr {
    /**
     * @brief Postgres Redis Value for transaction queue
     */
    class PgXactMsg
    {
    public:
        /** Message type */
        enum Type : char {
            XACT_MSG = 'X',
            TABLE_SYNC_MSG = 'T'
        };

        /** Transaction message */
        struct XactMsg {
            std::filesystem::path begin_path;
            std::filesystem::path commit_path;
            uint64_t begin_offset;
            uint64_t commit_offset;
            uint64_t xact_lsn;
            uint64_t xid;
            uint64_t db_id; ///< database id
            uint32_t pg_xid;
            std::set<uint32_t> aborted_xids;

            XactMsg(const std::filesystem::path &begin_path,
                  const std::filesystem::path &commit_path,
                  uint64_t db_id, uint64_t begin_offset, uint64_t commit_offset,
                  uint64_t xact_lsn, uint64_t xid, uint32_t pg_xid,
                  const std::set<uint32_t> &aborted_xids)
            : begin_path(begin_path), commit_path(commit_path),
              begin_offset(begin_offset), commit_offset(commit_offset),
              xact_lsn(xact_lsn), xid(xid), db_id(db_id), pg_xid(pg_xid),
              aborted_xids(aborted_xids) {}

            XactMsg(const std::vector<std::string> &split)
            {
                // serialized order: xid, pg_xid, xact_lsn, begin_path, begin_offset, commit_path, commit_offset
                int idx = 0;
                db_id = std::stoull(split[idx++]); // db_id
                xid = std::stoull(split[idx++]); // xid
                pg_xid = std::stoul(split[idx++]); // pg_xid
                xact_lsn = std::stoull(split[idx++]); // lsn
                begin_path = std::filesystem::path(split[idx++]); // begin path
                begin_offset = std::stoull(split[idx++]); // begin offset
                commit_path = std::filesystem::path(split[idx++]); // commit path
                commit_offset = std::stoull(split[idx++]); // commit offset
                int aborted_xids_size = std::stoi(split[idx++]); // aborted xids size

                for (int i = 0; i < aborted_xids_size; i++) { // aborted xids
                    aborted_xids.insert(std::stoul(split[idx++], nullptr, 16));
                }
            }

            std::string serialize() const
            {
                std::string xid_str = fmt::format("{}:", aborted_xids.size());
                for (auto xid : aborted_xids) {
                    xid_str += fmt::format("{:X}:", xid);
                }
                return fmt::format("{}:{}:{}:{}:{}:{}:{}:{}:{}", db_id, xid, pg_xid, xact_lsn, begin_path.c_str(),
                                   begin_offset, commit_path.c_str(), commit_offset, xid_str);
            }
        };

        /** Table sync message -- from copy table */
        struct TableSyncMsg {
            uint64_t db_id;
            uint64_t xid;
            uint64_t tid;

            // XXX -- not currently implemented in serialize/deserialize
            int32_t xmin;
            int32_t xmax;
            std::vector<int32_t> pgxids; // in progress


            TableSyncMsg(uint64_t db_id, uint64_t xid, uint64_t tid)
                : db_id(db_id), xid(xid), tid(tid)
            {}

            TableSyncMsg(const std::vector<std::string> &split)
            {
                // serialized order: db_id, xid, tid
                int idx = 0;
                db_id = std::stoull(split[idx++]); // db_id
                xid = std::stoull(split[idx++]); // xid
                tid = std::stoull(split[idx++]); // tid
            }

            std::string serialize() const
            {
                return fmt::format("{}:{}:{}", db_id, xid, tid);
            }
        };

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
        PgXactMsg(const std::filesystem::path &begin_path,
                  const std::filesystem::path &commit_path,
                  uint64_t db_id, uint64_t begin_offset, uint64_t commit_offset,
                  uint64_t xact_lsn, uint64_t xid, uint32_t pg_xid,
                  const std::set<uint32_t> &aborted_xids)
            : type(XACT_MSG),
              msg(XactMsg(begin_path, commit_path, db_id, begin_offset,
                          commit_offset, xact_lsn, xid, pg_xid, aborted_xids))
        {}

        PgXactMsg(uint64_t db_id, uint64_t xid, uint64_t tid)
            : type(TABLE_SYNC_MSG),
              msg(TableSyncMsg(db_id, xid, tid))
        {}

        /**
         * @brief Deserialization constructor
         * @param string_value
         */
        explicit PgXactMsg(const std::string &string_value)
            : msg(0) // msg needs to be initialized
        {
            std::vector<std::string> split;
            common::split_string(":", string_value, split);

            std::string type_str = split.back();
            split.pop_back();

            type = (type_str == "X") ? XACT_MSG : TABLE_SYNC_MSG;

            if (type == XACT_MSG) {
                assert(split.size() >= 8);
                msg = XactMsg(split);
            } else if (type == TABLE_SYNC_MSG) {
                assert(split.size() >= 3);
                msg = TableSyncMsg(split);
            } else {
                assert(false);
            }
        }

        /**
         * @brief Serialize data to string
         * @return std::string
         */
        std::string serialize() const
        {
            if (type == XACT_MSG) {
                const auto &xact_msg = std::get<XactMsg>(msg);
                return xact_msg.serialize() + ":X";
            } else if (type == TABLE_SYNC_MSG) {
                const auto &table_sync_msg = std::get<TableSyncMsg>(msg);
                return table_sync_msg.serialize() + ":T";
            } else {
                assert(false);
            }
        }

        Type type;  ///< message type
        std::variant<TableSyncMsg, XactMsg, int> msg;  ///< message data
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
