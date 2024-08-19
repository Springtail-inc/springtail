#pragma once

#include <common/common.hh>

namespace springtail::gc {

    /**
     * Object used to communicate which XID the LogParser has completed so that the Committer can
     * begin operation.  Passed via a Redis queue.
     */
    class XidReady {
    public:
        XidReady(uint64_t db_id,
                 uint64_t xid)
            : _db_id(db_id),
              _xid(xid)
        { }

        XidReady(const std::string &value)
        {
            std::vector<std::string> split;
            common::split_string(":", value, split);

            _db_id = std::stoull(value);
            _xid = std::stoull(value);
        }

        std::string serialize() const {
            return fmt::format("{}:{}", _db_id, _xid);
        }

        uint64_t db_id() const {
            return _db_id;
        }

        uint64_t xid() const {
            return _xid;
        }

    private:
        uint64_t _db_id;
        uint64_t _xid;
    };

}
