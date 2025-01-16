#pragma once

#include <string>
#include <filesystem>
#include <set>
#include <vector>
#include <cassert>
#include <variant>

#include <fmt/core.h>

#include <pg_repl/pg_copy_table.hh>

#include <common/common.hh>
#include <common/redis.hh>

#include <redis/redis_containers.hh>

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
            TABLE_SYNC_MSG = 'T',
            TABLE_SYNC_END_MSG = 'E'
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

            explicit operator std::string() const
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
            uint64_t target_xid;
            uint32_t pg_xid;               ///< postgres xid
            uint32_t xmin;                 ///< xmin; lowest xid still active
            uint32_t xmax;                 ///< xmax; one past highest completed xid
            uint32_t xmin_epoch;
            uint32_t xmax_epoch;
            std::vector<int32_t> tids;    ///< table ids
            std::vector<uint32_t> xips;

            TableSyncMsg(uint64_t db_id, PgCopyResultPtr copy_result)
                : db_id(db_id), target_xid(copy_result->target_xid),
                  pg_xid(copy_result->pg_xid),
                  xmin(copy_result->xmin), xmax(copy_result->xmax),
                  xmin_epoch(copy_result->xmin_epoch), xmax_epoch(copy_result->xmax_epoch),
                  tids(copy_result->tids), xips(copy_result->xips)
            {}

            TableSyncMsg(const std::vector<std::string> &split)
            {
                // serialized order: db_id, target_xid, xmin xid, xmax xid, xmin epoch, xmax epoch, tids, xips
                int idx = 0;
                db_id = std::stoull(split[idx++]); // db_id
                target_xid = std::stoull(split[idx++]); // target_xid
                pg_xid = std::stoul(split[idx++]); // pg_xid
                xmin = std::stoul(split[idx++]); // xmin
                xmax = std::stoul(split[idx++]); // xmax
                xmin_epoch = std::stoul(split[idx++]); // xmin epoch
                xmax_epoch = std::stoul(split[idx++]); // xmax epoch

                std::string tids = split[idx++];
                std::string xips = split[idx++];

                // decode tids vector
                if (!tids.empty()) {
                    std::vector<std::string> tids_split;
                    common::split_string(",", tids, tids_split);
                    for (const auto &tid : tids_split) {
                        this->tids.push_back(std::stoi(tid));
                    }
                }

                // decode xips vector
                if (!xips.empty()) {
                    std::vector<std::string> xips_split;
                    common::split_string(",", xips, xips_split);
                    for (const auto &xip : xips_split) {
                        this->xips.push_back(std::stoul(xip));
                    }
                }
            }

            /** Serialize table sync message to string */
            explicit operator std::string() const
            {
                return fmt::format("{}:{}:{}:{}:{}:{}:{}:{}:{}",
                    db_id, target_xid, pg_xid, xmin, xmax, xmin_epoch, xmax_epoch,
                    common::join_string(",", tids.begin(), tids.end()),
                    common::join_string(",", xips.begin(), xips.end()));
            }
        };

        struct TableSyncEndMsg {
            uint64_t db_id;

            TableSyncEndMsg(uint64_t db_id)
                : db_id(db_id)
            {}

            TableSyncEndMsg(const std::string &string_value)
            {
                db_id = std::stoull(string_value);
            }

            explicit operator std::string() const
            {
                return fmt::format("{}", db_id);
            }
        };

        /**
         * @brief Construct a new PgXactMsg object of type XACT_MSG
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

        /**
         * @brief Construct a new PgXactMsg object of type TABLE_SYNC_MSG
         * @param db_id database id
         * @param copy_result copy table result
         */
        PgXactMsg(uint64_t db_id, PgCopyResultPtr copy_result)
            : type(TABLE_SYNC_MSG),
              msg(TableSyncMsg(db_id, copy_result))
        {}

        explicit PgXactMsg(uint64_t db_id)
            : type(TABLE_SYNC_END_MSG),
              msg(TableSyncEndMsg(db_id))
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

            if (type_str == "X") {
                assert(split.size() >= 8);
                type = XACT_MSG;
                msg = XactMsg(split);
            } else if (type_str == "T") {
                assert(split.size() >= 8);
                type = TABLE_SYNC_MSG;
                msg = TableSyncMsg(split);
            } else if (type_str == "E") {
                assert(split.size() == 1);
                type = TABLE_SYNC_END_MSG;
                msg = TableSyncEndMsg(split[0]);
            } else {
                assert(false);
            }
        }

        /**
         * @brief Serialize data to string
         * @return std::string
         */
        explicit operator std::string() const
        {
            if (type == XACT_MSG) {
                const auto &xact_msg = std::get<XactMsg>(msg);
                return static_cast<std::string>(xact_msg) + ":X";
            } else if (type == TABLE_SYNC_MSG) {
                const auto &table_sync_msg = std::get<TableSyncMsg>(msg);
                return static_cast<std::string>(table_sync_msg) + ":T";
            } else if (type == TABLE_SYNC_END_MSG) {
                const auto &table_sync_end_msg = std::get<TableSyncEndMsg>(msg);
                return static_cast<std::string>(table_sync_end_msg) + ":E";
            } else {
                CHECK(false);
            }
        }

        Type type;  ///< message type
        std::variant<XactMsg, TableSyncMsg, TableSyncEndMsg, int> msg;  ///< message data
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

        explicit operator std::string() const
        {
            return fmt::format("{}:{}", oid, xid);
        }

        uint64_t oid;
        uint64_t xid;
    };
    using RSSOidValue = RedisSortedSet<PgRedisOidValue>;
    using RSSOidValuePtr = std::shared_ptr<RSSOidValue>;

} // namespace springtail::pg_log_mgr
