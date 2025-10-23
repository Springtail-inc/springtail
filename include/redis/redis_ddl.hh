#pragma once

#include <memory>
#include <string>
#include <vector>
#include <utility>
#include <shared_mutex>
#include <unordered_map>
#include <nlohmann/json.hpp>

#include <common/redis.hh>
#include <common/singleton.hh>

namespace springtail {

    /**
     * Shared class among GC-1, GC-2, the FDW and the XidMgr to coordinate DDL changes through the
     * system.  It operates as follows:
     *
     * - GC-1 generates a list of DDL statements as it processes the log
     * - GC-2 takes the complete list of DDL statements and places them into a queue for each FDW
     * - FDW listens for new items on the queue and generates the set of DDL statements to apply as a
     *   transaction.  Once the DDL statements have been applied, it updates it's schema XID in Redis.
     * - XidMgr checks the set of schema XIDs at each FDW and updates it's history based on the minimum
     *   schema XID reached across all FDWs
     */
    class RedisDDL : public Singleton<RedisDDL> {
        friend class Singleton<RedisDDL>;

    public:

        /**
         * Used by gc::LogParser (GC-1) to record DDL statements against the XID.
         * @param xid The XID at which this DDL statement needs to be applied.
         * @param ddl A JSON representation of the DDL statement.
         */
        void add_ddl(uint64_t db_id, uint64_t xid, const std::string &ddl);

        /**
         * Used by gc::Committer (GC-2) to retrieve the set of DDL statements recorded against the
         * XID we are about to commit.
         * @param xid The XID we are about to commit.
         * @return A JSON array containing the ordered set of DDLs to apply at each FDW.
         */
        nlohmann::json get_ddls_xid(uint64_t db_id, uint64_t xid);

        /**
         * Used by gc::LogParser (GC-1) to clear DDL statements it recorded against a given XID.
         * @param uint64_t db_id The database ID associated with the XID.
         * @param uint64_t xid The XID to clear.
         */
        void clear_ddls_xid(uint64_t db_id, uint64_t xid);

        /**
         * Clear all DDL statements for a given database.
         * @param uint64_t db_id The database ID to clear.
         */
        void clear_ddls(uint64_t db_id);

        /**
         * Used by the gc::Committer (GC-2) to pre-commit the DDL statements prior to committing the
         * associated XID.  This allows for either roll-back if there is a failure prior to the XID
         * commit, or roll-forward if the XID commit succeeds but there is a failure prior to the
         * commit_ddl().
         * @param db_id The ID of the database instance we are updating.
         * @param xid The XID at which these DDL statements were applied.
         * @param ddls A JSON array of DDL statements to apply, retrieved from get_ddls_xid()
         */
        void precommit_ddl(uint64_t db_id, uint64_t xid, nlohmann::json ddls);

        /**
         * Used by gc::Committer (GC-2) to provide the list of DDL statements to the FDWs.
         * @param db_id The ID of the database instance we are updating.
         * @param xid The XID at which these DDL statements were applied.
         */
        void commit_ddl(uint64_t db_id, uint64_t xid);

        /**
         * Used by the gc::Committer (GC-2) to perform a cleanup of the pre-commit DDLs.
         * @return A list of <db_id, xid> pairs in the pre-commit step.
         */
        std::vector<std::pair<uint64_t, uint64_t>> get_precommit_ddl();

        /**
         * Used by the gc::Committer (GC-2) to abort incomplete XIDs that are in the pre-commit
         * phase.
         * @param db_id The ID of the database instance we are updating.
         * @param xid The XID at which these DDL statements were applied.
         */
        void abort_ddl(uint64_t db_id, uint64_t xid);

        /**
         * Used by the FDW to retrieve the next set of DDL statements that need to be applied.
         * @param fdw_id The ID of the FDW we are updating.
         * @return A JSON object containing the XID at which the DDLs were applied and an array of
         *         the DDL statements themselves.
         */
        std::vector<nlohmann::json> get_next_ddls(const std::string &fdw_id);

        /**
         * Used by the FDW to abort applying a set of DDL statements and place them back on the
         * processing queue.
         */
        void abort_fdw(const std::string &fdw_id);

        /**
         * Used by the FDW to record the latest schema XID that it has applied.
         * @param fdw_id The ID of the FDW we are updating.
         * @param db_id The ID of the database instance we are updating.
         * @param schema_xid The XID from the last get_next_ddls() call that was applied.
         */
        void update_schema_xid(const std::string &fdw_id, uint64_t db_id, uint64_t schema_xid);

        /**
         * Used by FDW to retrieve the most recent schema XID for the database
         * @brief Get the schema xid object
         *
         * @param fdw_id The ID of the FDW we are updating.
         * @param db_id The ID of the database instance we are updating.
         * @return uint64_t - the most recent schema XID
         */
        uint64_t get_schema_xid(const std::string &fdw_id, uint64_t db_id);

        /**
         * Used by the FDW to commit the record without updating the schema XID.
         * This is used when the FDW already has the change applied and needs to remove
         * it from the queue.
         * @param fdw_id The ID of the FDW we are updating.
         */
        void commit_fdw_no_update(const std::string &fdw_id);

        /**
         * Used by the XidMgr to identify when it can remove XIDs from it's schema XID history.
         * @param db_id The ID of the database instance we are fetching XIDs for.
         * @return The minimum schema XID applied across all of the FDWs.
         */
        uint64_t min_schema_xid(uint64_t db_id);

        /**
         * @brief Returns the minimum XID which is inflight in FDW for a DB
         *
         * @param db_id Database ID
         * @return minimum XID
         */
        uint64_t min_fdw_xid(uint64_t db_id);

        /**
         * @brief Insert index XID in the tracker
         *
         * @param db_id Database ID
         * @param xid   Index XID
         */
        void insert_index_xid(uint64_t db_id, uint64_t xid);

        /**
         * @brief Remove index XID from the tracker
         *
         * @param db_id Database ID
         * @param xid   Index XID
         */
        void remove_index_xid(uint64_t db_id, uint64_t xid);

        /**
         * @brief Get minimum index XID from the tracker for the db
         *
         * @param db_id Database ID
         * @return minimum index XID
         */
        uint64_t min_index_xid(uint64_t db_id);

    private:
        RedisDDL()
            : _redis(RedisMgr::get_instance()->get_client())
        { }

        ~RedisDDL() override = default;

        RedisClientPtr _redis;

        // In-memory DDL storage: db_id -> (xid -> [ddl_statements])
        // Used for the hot path between GC-1 (PgLogReader) and GC-2 (Committer)
        // Entries are transient and cleared after precommit_ddl() moves them to Redis
        std::unordered_map<uint64_t, std::unordered_map<uint64_t, std::vector<nlohmann::json>>> _ddl_cache;
        mutable std::shared_mutex _ddl_cache_mutex;
    };

}
