#include <chrono>

#include <fmt/format.h>

#include <common/common.hh>
#include <common/logging.hh>
#include <common/redis.hh>
#include <common/redis_types.hh>

#include <xid_mgr/xid_mgr_client.hh>

#include <pg_log_mgr/pg_xact_handler.hh>
#include <pg_log_mgr/pg_redis_xact.hh>

namespace springtail {

    PgXactHandler::PgXactHandler(const std::filesystem::path &base_path)
        : _base_path(base_path)
    {}

    void
    PgXactHandler::_create_logger()
    {
        int offset = 0;
        std::filesystem::path file;
        do {
            file = _base_path;
            file.append(fmt::format("{}", common::get_time_in_millis() + offset));
            // shouldn't ever have to loop here...
            offset++;
        } while (std::filesystem::exists(file));

        _logger = std::make_shared<PgXactLogWriter>(file);
    }

    uint64_t
    PgXactHandler::_allocate_xid()
    {
        // first check if we've run out of xids, if so get new range from xid_mgr
        if (_next_xid == _last_xid) {
            // need to get a new xid range
            XidMgrClient *xid_mgr = XidMgrClient::get_instance();
            std::pair<uint64_t, uint64_t> xids = xid_mgr->get_xid_range(_last_xid);
            _next_xid = xids.first;
            _last_xid = xids.second;
        }

        return _next_xid++;
    }

    void
    PgXactHandler::process(const PgReplMsgStream::PgTransactionPtr xact)
    {
        // first allocate an xid for this xact
        uint64_t xid = _allocate_xid();

        // next issue log request
        _logger->log_data(xact, xid);

        // finally send notification to GC
        PgRedisXactValue redis_xact(xact->begin_path, xact->commit_path, xact->begin_offset,
                                    xact->commit_offset, xact->xact_lsn, xid, xact->xid);

        // XXX need to add customer ID
        _redis_queue.push(redis::QUEUE_PG_TRANSACTIONS, redis_xact);
    }
}