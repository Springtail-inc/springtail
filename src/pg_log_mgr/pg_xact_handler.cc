#include <chrono>

#include <fmt/format.h>

#include <common/common.hh>
#include <common/logging.hh>
#include <common/redis.hh>
#include <common/redis_types.hh>
#include <common/filesystem.hh>

#include <xid_mgr/xid_mgr_client.hh>

#include <pg_log_mgr/pg_xact_handler.hh>
#include <pg_log_mgr/pg_redis_xact.hh>

namespace springtail {

    PgXactHandler::PgXactHandler(const std::filesystem::path &base_dir)
        : _base_dir(base_dir),
          _redis_queue(redis::QUEUE_PG_TRANSACTIONS),
          _oid_set(redis::SET_PG_OID_XIDS)
    {
        _create_logger();

        // XXX need to fetch latest xid from xid mgr
    }

    void
    PgXactHandler::_create_logger()
    {
        std::filesystem::path file = fs::find_latest_modified_file(_base_dir);
        if (file.empty()) {
            file = _base_dir / fmt::format("{}{:04d}{}", LOG_PREFIX, 0, LOG_SUFFIX);
        } else {
            file = get_next_logfile(file);
        }
        _current_logfile = file;
        _logger = std::make_shared<PgXactLogWriter>(file);
    }

    uint64_t
    PgXactHandler::_allocate_xid()
    {
        // return next_xid, and increment it
        return _next_xid++;
    }

    void
    PgXactHandler::process(const PgTransactionPtr xact)
    {
        // if stream start, just log it
        if (xact->type == PgTransaction::TYPE_STREAM_START ||
            xact->type == PgTransaction::TYPE_STREAM_ABORT) {
            _logger->log_stream_msg(xact);
            return;
        }

        // first allocate an xid for this xact
        uint64_t xid = _allocate_xid();

        // next log the data
        assert (xact->type == PgTransaction::TYPE_COMMIT);
        xact->springtail_xid = xid;
        _logger->log_commit(xact);

        // go through the oid map and update redis
        for (auto &oid : xact->oids) {
            _oid_set.add(PgRedisOidValue(oid, xid), xid);
        }

        // finally send notification to GC
        PgRedisXactValue redis_xact(xact->begin_path, xact->commit_path, _db_id, xact->begin_offset,
                                    xact->commit_offset, xact->xact_lsn, xid, xact->xid, xact->aborted_xids);

        // XXX need to add customer ID
        _redis_queue.push(redis_xact);
    }

    std::vector<PgRedisXactValue>
    PgXactHandler::get_redis_xacts(long long start, long long end)
    {
        return _redis_queue.range(start, end);
    }
}
