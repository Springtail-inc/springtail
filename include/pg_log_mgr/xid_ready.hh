#pragma once

#include <common/common.hh>
#include <fmt/ranges.h>
#include <proto/pg_copy_table.pb.h>

#include <pg_log_mgr/wal_progress_tracker.hh>

namespace springtail::committer {

    /**
     * Object used to communicate which XID the LogParser has completed so that the Committer can
     * begin operation.  Passed via a Redis queue.
     */
    class XidReady {
    public:
        /**
         * A sub-class holding data used by the XACT_MSG type.
         */
        class XactMsg {
        public:
            explicit XactMsg(uint64_t xid)
                : _xid(xid)
            { }

            uint64_t xid() const {
                return _xid;
            }

        private:
            uint64_t _xid;
        };

        /**
         * A sub-class holding data used by the TABLE_SYNC_SWAP and TABLE_SYNC_COMMIT types.
         */
        class SwapMsg {
        public:
            using TableInfo = std::pair<int32_t, std::shared_ptr<proto::CopyTableInfo>>;

            SwapMsg(uint64_t xid, std::vector<TableInfo> &&tids)
                : _xid(xid),
                  _tids(tids)
            { }

            const uint64_t xid() const {
                return _xid;
            }

            const std::vector<TableInfo> &tids() const {
                return _tids;
            }

        private:
            uint64_t _xid;
            std::vector<TableInfo> _tids;
        };

        /**
         * A sub-class holding data used by the RECONCILE_INDEX type.
         */
        class ReconcileMsg {
        public:
            ReconcileMsg(uint64_t xid, uint64_t reconcile_xid)
                : _xid(xid),
                _reconcile_xid(reconcile_xid)
            { }

            uint64_t xid() const {
                return _xid;
            }

            uint64_t reconcile_xid() const {
                return _reconcile_xid;
            }

        private:
            uint64_t _xid;
            uint64_t _reconcile_xid;
        };

        /**
         * Type enum for the various XidReady messages to the Committer.
         */
        enum Type : char {
            XACT_MSG = 'X',
            TABLE_SYNC_START = 'S',
            TABLE_SYNC_SWAP = 'W',
            TABLE_SYNC_COMMIT = 'C',
            RECONCILE_INDEX = 'R'
        };

        /** Constructor for SWAP and COMMIT messages. */
        XidReady(const Type &type, uint64_t db_id, SwapMsg &&msg)
            : _type(type),
              _db_id(db_id),
              _msg(msg)
        {
            assert(_type == Type::TABLE_SYNC_SWAP || _type == Type::TABLE_SYNC_COMMIT);
        }

        /** Constructor for TABLE_SYNC_START messages. */
        explicit XidReady(uint64_t db_id)
            : _type(Type::TABLE_SYNC_START),
              _db_id(db_id)
        { }

        /** Constructor for messages that are XACT_MSG. */
        XidReady(uint64_t db_id, XactMsg &&msg, pg_log_mgr::WalProgressTrackerPtr xid_tracker = nullptr)
            : _type(Type::XACT_MSG),
              _db_id(db_id),
              _msg(msg),
              _xid_tracker(xid_tracker)
        { }

        /** Constructor for messages that are RECONCILE_INDEX. */
        XidReady(uint64_t db_id, ReconcileMsg &&msg)
            : _type(Type::RECONCILE_INDEX),
              _db_id(db_id),
              _msg(msg)
        { }

        /** A getter for the type. */
        Type type() const {
            return _type;
        }

        /** A getter for the database ID. */
        uint64_t db() const {
            return _db_id;
        }

        /** A getter for the XactMsg. */
        const XactMsg &xact() const {
            return std::get<XactMsg>(*_msg);
        }

        /** A getter for the SwapMsg. */
        const SwapMsg &swap() const {
            return std::get<SwapMsg>(*_msg);
        }

        /** A getter for the ReconcileMsg. */
        const ReconcileMsg &reconcile() const {
            return std::get<ReconcileMsg>(*_msg);
        }

        /** A function to notify tracker about xid. */
        void notify_tracker(uint64_t xid) {
            CHECK(_xid_tracker);
            _xid_tracker->remove_xid(xid);
        }

    private:
        Type _type; ///< The message type.
        uint64_t _db_id; ///< The database ID.
        std::optional<std::variant<XactMsg, SwapMsg, ReconcileMsg>> _msg; ///< The underlying message data.
        pg_log_mgr::WalProgressTrackerPtr _xid_tracker;
    };

}
