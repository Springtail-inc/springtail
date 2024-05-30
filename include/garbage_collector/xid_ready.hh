#pragma once

namespace springtail::gc {

    /**
     * Object used to communicate which XID the LogParser has completed so that the Committer can
     * begin operation.  Passed via a Redis queue.
     */
    class XidReady {
    public:
        XidReady(uint64_t xid)
            : _xid(xid)
        { }

        XidReady(const std::string &value)
        {
            _xid = static_cast<uint64_t>(std::atoll(value.c_str()));
        }

        std::string serialize()
        {
            return fmt::format("{}", _xid);
        }

        uint64_t xid() const {
            return _xid;
        }

    private:
        uint64_t _xid;
    };

}
