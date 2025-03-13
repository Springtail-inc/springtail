#pragma once

#include <common/common.hh>
#include <fmt/ranges.h>

#include <pg_log_mgr/timestamps_and_xids.hh>

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
            friend XidReady;

            /** Internal constructor for constructing from a packed string. */
            explicit XactMsg(const std::span<std::string> &values)
            {
                _xid = std::stoull(values[0]);
            }

        public:
            explicit XactMsg(uint64_t xid)
                : _xid(xid)
            { }

            explicit operator std::string() const {
                return fmt::format("{}", _xid);
            }

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
            friend XidReady;

            /** Internal constructor for constructing from a packed string. */
            explicit SwapMsg(const std::span<std::string> &values)
            {
                // sanity check
                assert(values.size() > 1);

                // parse the XID
                _xid = std::stoull(values[0]);

                // copy the table IDs, starting after the XID
                std::transform(values.begin() + 1, values.end(), std::back_inserter(_tids),
                               [](const std::string &v) {
                                   return std::stoull(v);
                               });
            }

        public:
            SwapMsg(uint64_t xid, std::vector<uint64_t> &&tids)
                : _xid(xid),
                  _tids(tids)
            { }

            explicit operator std::string() const {
                return fmt::format("{}:{}", _xid, fmt::join(_tids, ":"));
            }

            const uint64_t xid() const {
                return _xid;
            }

            const std::vector<uint64_t> &tids() const {
                return _tids;
            }

        private:
            uint64_t _xid;
            std::vector<uint64_t> _tids;
        };

        /**
         * Type enum for the various XidReady messages to the Committer.
         */
        enum Type : char {
            XACT_MSG = 'X',
            TABLE_SYNC_START = 'S',
            TABLE_SYNC_SWAP = 'W',
            TABLE_SYNC_COMMIT = 'C'
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
        XidReady(uint64_t db_id, XactMsg &&msg, pg_log_mgr::TimestampsAndXidsPtr xid_tracker = nullptr)
            : _type(Type::XACT_MSG),
              _db_id(db_id),
              _msg(msg),
              _xid_tracker(xid_tracker)
        { }

        /** Constructor for parsing an XidReady out of a redis value. */
        explicit XidReady(const std::string &value)
        {
            std::vector<std::string> split;
            common::split_string(":", value, split);
            assert(split.size() >= 2);
            assert(split[0].size() == 1);

            _type = static_cast<Type>(split[0][0]);
            _db_id = std::stoull(split[1]);
            std::span<std::string> remaining(split.begin() + 2, split.end());

            switch (_type) {
            case Type::XACT_MSG:
                _msg = XactMsg(remaining);
                break;

            case Type::TABLE_SYNC_START:
                // nothing to do
                break;

            case Type::TABLE_SYNC_SWAP:
            case Type::TABLE_SYNC_COMMIT:
                _msg = SwapMsg(remaining);
                break;

            default:
                SPDLOG_ERROR("Invalid type: {}", value[0]);
                assert(0);
            }
        }

        /** Serialize an XidReady into a string to store in redis. */
        explicit operator std::string() const {
            switch (_type) {
            case Type::XACT_MSG:
                return fmt::format("{}:{}:{}",
                                   static_cast<char>(_type), _db_id,
                                   static_cast<std::string>(xact()));

            case Type::TABLE_SYNC_START:
                return fmt::format("{}:{}",
                                   static_cast<char>(_type), _db_id);

            case Type::TABLE_SYNC_SWAP:
            case Type::TABLE_SYNC_COMMIT:
                return fmt::format("{}:{}:{}",
                                   static_cast<char>(_type), _db_id,
                                   static_cast<std::string>(swap()));
            }

            SPDLOG_ERROR("Unknown type: {}", static_cast<char>(_type));
            assert(0);
        }

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

        /** A function to notify tracker about xid. */
        void notify_tracker(uint64_t xid) {
            if (_xid_tracker != nullptr) {
                _xid_tracker->remove_xid(xid);
            }
        }

    private:
        Type _type; ///< The message type.
        uint64_t _db_id; ///< The database ID.
        std::optional<std::variant<XactMsg, SwapMsg>> _msg; ///< The underlying message data.
        pg_log_mgr::TimestampsAndXidsPtr _xid_tracker;
    };

}
