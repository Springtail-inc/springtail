#pragma once

#include <common/common.hh>

namespace springtail::gc {

    /**
     * Object used to communicate which XID the LogParser has completed so that the Committer can
     * begin operation.  Passed via a Redis queue.
     */
    class XidReady {
    public:
        /**
         * Type enum for the various XidReady messages to the Committer.
         */
        enum Type : char {
            XACT_MSG = 'X',
            TABLE_SYNC_START = 'S',
            TABLE_SYNC_COMMIT = 'C'
        };

        /** Constructor for messages that aren't XACT_MSG. */
        XidReady(const Type &type,
                 uint64_t db_id,
                 uint64_t xid = 0)
            : _type(type),
              _db_id(db_id),
              _xid(xid)
        {
            assert(_type != Type::XACT_MSG);
        }

        /** Constructor for messages that are XACT_MSG. */
        XidReady(uint64_t db_id,
                 uint64_t xid)
            : _type(Type::XACT_MSG),
              _db_id(db_id),
              _xid(xid)
        { }

        /** Constructor for parsing an XidReady out of a redis value. */
        XidReady(const std::string &value)
        {
            std::vector<std::string> split;
            common::split_string(":", value, split);

            // verify that the type is just one character
            assert(split[0].size() == 1);

            // set the internal values
            _type = Type(split[0][0]);
            _db_id = std::stoull(split[1]);
            _xid = std::stoull(split[2]);
        }

        /** Serialize an XidReady into a string to store in redis. */
        explicit operator std::string() const {
            return fmt::format("{}:{}:{}", static_cast<char>(_type), _db_id, _xid);
        }

        /** A getter for the type. */
        Type type() const {
            return _type;
        }

        /** A getter for the database ID. */
        uint64_t db_id() const {
            return _db_id;
        }

        /** A getter for the XID. */
        uint64_t xid() const {
            return _xid;
        }

    private:
        Type _type; ///< The message type.
        uint64_t _db_id; ///< The database ID.
        uint64_t _xid; ///< The XID (for XACT_MSG types).
    };

}
