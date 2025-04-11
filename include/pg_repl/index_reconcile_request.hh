#pragma once

namespace springtail {

    class IndexReconcileRequest {
        public:
            /**
             * @brief Constructs an IndexReconcileRequest with the given database ID and reconcile XID.
             * @param db_id The database ID.
             * @param reconcile_xid The XID associated with the index reconciliation.
             */
            IndexReconcileRequest(const uint64_t &db_id, const uint64_t &reconcile_xid)
                : _db_id(db_id), _reconcile_xid(reconcile_xid) {}

            /**
             * @brief Gets the db id.
             * @return _db_id.
             */
            const uint64_t &db_id() const {
                return _db_id;
            }

            /**
             * @brief Gets the reconcile XID.
             * @return The reconcile XID.
             */
            const uint64_t &reconcile_xid() const {
                return _reconcile_xid;
            }

        private:
            uint64_t _db_id;
            uint64_t _reconcile_xid;
    };
}
