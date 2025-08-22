#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>

#include <common/logging.hh>

namespace springtail::pg_log_mgr {
    /**
    * @brief This class keeps track of Postgres and Springtail XIDs and of
    *      the log file timestamps that this transactions initially show up.
    *
    */
    class WalProgressTracker {
    public:
        /**
         * @brief Default constructor
         *
         */
        WalProgressTracker() = default;

        /**
         * @brief Default destructor
         *
         */
        ~WalProgressTracker() = default;

        /**
         * @brief This function add Postgres Xid and associated timestamp id
         *
         * @param pg_xid - Postgres Xid
         * @param ts     - timestamp id
         */
        void
        add_pg_xid(int32_t pg_xid, uint64_t ts)
        {
            std::unique_lock<std::shared_mutex> lock(_mt);

            // verify that Postgress XID is not somehow already inserted
            DCHECK(!_pg_xid_to_ts.contains(pg_xid));

            // insert into pg_xid -> ts map
            _pg_xid_to_ts.emplace(pg_xid, ts);

            // increment count in ts -> pg_xid count map
            if (!_ts_to_pg_xid_count.contains(ts)) {
                _ts_to_pg_xid_count.emplace(ts, 0);
            }
            _ts_to_pg_xid_count[ts]++;
        }

        /**
         * @brief This function removes Postgres Xid
         *
         * @param pg_xid - Postgres Xid
         */
        void
        remove_pg_xid(int32_t pg_xid)
        {
            std::unique_lock<std::shared_mutex> lock(_mt);

            // verify that Postgress XID is known
            if(!_pg_xid_to_ts.contains(pg_xid)) {
                return;
            }

            // Get Postgress XID timestamp
            uint64_t ts = _pg_xid_to_ts[pg_xid];

            // Erase Postgress XID from pg_xid -> ts map
            _pg_xid_to_ts.erase(pg_xid);

            // verify that ts exists in ts -> pg_xid count map
            DCHECK(_ts_to_pg_xid_count.contains(ts));

            // decrement count in ts -> pg_xid count map
            _ts_to_pg_xid_count[ts]--;
            if (_ts_to_pg_xid_count[ts] == 0) {
                _ts_to_pg_xid_count.erase(ts);
            }
        }

        /**
         * @brief This function adds Xid for the give Postgres Xid.
         *
         * @param pg_xid - Postgres Xid
         * @param xid    - Springtail Xid
         */
        void
        add_xid(int32_t pg_xid, uint64_t xid)
        {
            std::unique_lock<std::shared_mutex> lock(_mt);

            // verify that Postgress XID is known
            if(!_pg_xid_to_ts.contains(pg_xid)) {
                return;
            }

            // Get Postgress XID timestamp
            uint64_t ts = _pg_xid_to_ts[pg_xid];

            // Erase Postgress XID from pg_xid -> ts map
            _pg_xid_to_ts.erase(pg_xid);

            // verify that ts exists in ts -> pg_xid count map
            DCHECK(_ts_to_pg_xid_count.contains(ts));

            // decrement count in ts -> pg_xid count map
            _ts_to_pg_xid_count[ts]--;
            DCHECK(_ts_to_pg_xid_count[ts] >= 0);
            if (_ts_to_pg_xid_count[ts] == 0) {
                _ts_to_pg_xid_count.erase(ts);
            }

            // verify that Springtail XID is not somehow already inserted
            DCHECK(!_xid_to_ts.contains(xid));

            // insert into xid -> ts map
            _xid_to_ts.emplace(xid, ts);

            // increment count in ts -> xid count map
            if (!_ts_to_xid_count.contains(ts)) {
                _ts_to_xid_count.emplace(ts, 0);
            }
            _ts_to_xid_count[ts]++;
        }

        /**
         * @brief This function removes Xid
         *
         * @param xid - Springtail Xid
         */
        void
        remove_xid(uint64_t xid)
        {
            std::unique_lock<std::shared_mutex> lock(_mt);

            // verify that XID is known
            if(!_xid_to_ts.contains(xid)) {
                return;
            }

            // Get XID timestamp
            uint64_t ts = _xid_to_ts[xid];

            // Erase XID from xid -> ts map
            _xid_to_ts.erase(xid);

            // verify that ts exists in ts -> xid count map
            DCHECK(_ts_to_xid_count.contains(ts));

            // decrement count in ts -> pg_xid count map
            _ts_to_xid_count[ts]--;
            DCHECK(_ts_to_xid_count[ts] >= 0);
            if (_ts_to_xid_count[ts] == 0) {
                _ts_to_xid_count.erase(ts);
            }
        }

        /**
         * @brief Get the min timestamp id recorded for Postgress and Springtail Xids
         *
         * @return uint64_t - timestamp id
         */
        uint64_t
        get_min_timestamp()
        {
            std::shared_lock<std::shared_mutex> lock(_mt);
            uint64_t pg_xid_ts = UINT64_MAX;
            uint64_t xid_ts = UINT64_MAX;
            if (auto it = _ts_to_pg_xid_count.begin(); it != _ts_to_pg_xid_count.end()) {
                pg_xid_ts = it->first;
            }
            if (auto it = _ts_to_xid_count.begin(); it != _ts_to_xid_count.end()) {
                xid_ts = it->first;
            }
            uint64_t min_ts = std::min(pg_xid_ts, xid_ts);
            LOG_DEBUG(LOG_PG_LOG_MGR, "pg_xid_ts = {}, xid_ts = {},  min timestamp = {}, returning timestamp = {}",
                pg_xid_ts, xid_ts, min_ts, (min_ts == UINT64_MAX)? 0 : min_ts);
            return (min_ts == UINT64_MAX)? 0 : min_ts;
        }

    private:
        std::map<int32_t, uint64_t> _pg_xid_to_ts;          ///< map Postgres Xid to timestamp id
        std::map<uint64_t, uint32_t> _ts_to_pg_xid_count;   ///< map to keep the number of times that timestamp ids are used for Postgres Xid
        std::map<uint64_t, uint64_t> _xid_to_ts;            ///< map Springtail Xid to timestamp id
        std::map<uint64_t, uint32_t> _ts_to_xid_count;      ///< map to keep the number of times that timestamp ids are used for Springtail Xid
        std::shared_mutex _mt;                              ///< mutext for access to this class data structures
    } ;
    using WalProgressTrackerPtr = std::shared_ptr<WalProgressTracker>;

}