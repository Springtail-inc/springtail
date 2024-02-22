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
        // return next_xid, and increment it
        return _next_xid++;
    }

    void
    PgXactHandler::process(const PgReplMsgStream::PgTransactionPtr xact)
    {
        // first allocate an xid for this xact
        uint64_t xid = _allocate_xid();

        // next issue log request
        _logger->log_data(xact, xid);

        // go through the oid map and update redis
        for (auto &oid : xact->oids) {
            _oid_set.add(redis::SET_PG_OID_XIDS, PgRedisOidValue(oid, xid), xid);
        }

        // finally send notification to GC
        PgRedisXactValue redis_xact(xact->begin_path, xact->commit_path, _db_id, xact->begin_offset,
                                    xact->commit_offset, xact->xact_lsn, xid, xact->xid);

        // XXX need to add customer ID
        _redis_queue.push(redis::QUEUE_PG_TRANSACTIONS, redis_xact);
    }
}