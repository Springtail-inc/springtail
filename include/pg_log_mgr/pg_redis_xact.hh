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
#include <proto/pg_copy_table.pb.h>
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
            std::vector<std::pair<int32_t, std::shared_ptr<proto::CopyTableInfo>>> tids;    ///< table ids and rpc info
            std::vector<uint32_t> xips;

            TableSyncMsg(uint64_t db_id, PgCopyResultPtr copy_result)
                : db_id(db_id), target_xid(copy_result->target_xid),
                  pg_xid(copy_result->pg_xid),
                  xmin(copy_result->xmin), xmax(copy_result->xmax),
                  xmin_epoch(copy_result->xmin_epoch), xmax_epoch(copy_result->xmax_epoch),
                  tids(copy_result->tids), xips(copy_result->xips)
            {}
        };

        struct TableSyncEndMsg {
            uint64_t db_id;

            TableSyncEndMsg(uint64_t db_id)
                : db_id(db_id)
            {}
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
