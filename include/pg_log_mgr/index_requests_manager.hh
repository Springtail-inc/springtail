#pragma once

namespace springtail::pg_log_mgr {
    /**
     * Manages index requests(create/drop) per XID per DB
     */
    class IndexRequestsManager {
        public:
            /**
             * @brief Add an IndexProcessRequest entry for the given db_id and xid.
             *
             * Thread-safe method that inserts the provided IndexProcessRequest into the internal map
             * under the specified db_id and xid. Automatically creates entries if they don't exist.
             *
             * @param db_id The database ID.
             * @param xid The transaction ID.
             * @param ia_response The IndexProcessRequest object to add.
             */
            void add_index_request(uint64_t db_id, uint64_t xid, proto::IndexProcessRequest ia_response) {
                std::lock_guard<std::mutex> lock(_idx_req_mutex);

                // Insert the response into the list associated with the given db_id and xid
                // Creates the inner map or list automatically if missing
                _index_requests_map[db_id][xid].push_back(ia_response);
            }

            /**
             * @brief Retrieve and remove all IndexProcessRequest entries for a given db_id and xid.
             *
             * Thread-safe method that returns the list of IndexProcessRequest objects associated with the given
             * db_id and xid. After retrieval, the entry is removed from the map. Returns an empty list if not found.
             *
             * @param db_id The database ID.
             * @param xid The transaction ID.
             * @return A list of IndexProcessRequest objects, or an empty list if none exist.
             */
            std::list<proto::IndexProcessRequest> get_index_requests(uint64_t db_id, uint64_t xid) {
                std::lock_guard<std::mutex> lock(_idx_req_mutex);

                // Attempt to find the db_id in the outer map
                auto db_it = _index_requests_map.find(db_id);
                if (db_it == _index_requests_map.end()) {
                    // db_id not found; return an empty list
                    return {};
                }

                // Get the inner map for the xid
                auto& xid_map = db_it->second;

                // Attempt to find the xid in the inner map
                auto xid_it = xid_map.find(xid);
                if (xid_it == xid_map.end()) {
                    // xid not found under the db_id; return an empty list
                    return {};
                }

                // Move the list of responses out of the map
                std::list<proto::IndexProcessRequest> result = std::move(xid_it->second);

                // Remove the xid entry from the inner map
                xid_map.erase(xid_it);

                // If the inner map is now empty, remove the db_id entry from the outer map
                if (xid_map.empty()) {
                    _index_requests_map.erase(db_it);
                }

                // Return the list of responses
                return result;
            }
        private:
            std::unordered_map<uint64_t, std::unordered_map<uint64_t,
                std::list<proto::IndexProcessRequest>>> _index_requests_map;
            std::mutex _idx_req_mutex;  ///< Protects access to the index requests map.
    };
}

