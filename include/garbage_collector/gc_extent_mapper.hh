#pragma once

#include <cstdint>
#include <list>
#include <map>
#include <vector>

#include <boost/thread.hpp>

namespace springtail::gc {
    /**
     * Tracks the actions of GC-1 and GC-2 to coordinate the rollforward and to allow query nodes to
     * perform individual extent rollforward as needed from the write cache.  See additional details
     * in Notion:
     *
     * https://www.notion.so/springtail-hq/GC-Extent-Mapper-7f49c226e1c04d1497cb1848945fbae5?pvs=4
     */
    class TableExtentMapper {
    public:
        /**
         * When the GC-2 updates an extent (or one of the query nodes performs a hurry-up), the
         * mapping from the old extent ID to the new extent ID(s) is recorded at the target XID.
         * This allows other actors in the system to retrieve the new extent IDs using forward_map()
         * and avoid performing the roll-forward themselves.
         */
        void add_mapping(uint64_t target_xid, uint64_t old_eid, const std::vector<uint64_t> &new_eids);

        /**
         * When the GC-1 adds data to the write cache, it records the XID at which it performed the
         * lookup for the given extent ID.  This way, when the GC-2 runs, it can correctly identify
         * if additional changes occurred to the extent ID it gets from the write cache.
         */
        void set_lookup(uint64_t target_xid, uint64_t extent_id);

        /**
         * Finds the set of new extent IDs that were generated through a roll-forward so that the
         * changes can be correctly applied to them instead of to the extent ID provided by the
         * write cache from GC-1.
         */
        std::vector<uint64_t> forward_map(uint64_t target_xid, uint64_t extent_id);

        /**
         * Checks if the extent_id has outstanding changes in the write cache with a lookup_xid <
         * access_xid and with a target_xid < the provided target_xid.  Returns the extent IDs that
         * were stored in the write cache containing such changes.
         */
        std::vector<uint64_t> reverse_map(uint64_t access_xid, uint64_t target_xid, uint64_t extent_id);

        /**
         * Removes any metadata stored in the ExtentMapper with a target_xid <= commit_xid.
         */
        bool expire(uint64_t commit_xid);

    private:
        /**
         * A structure holding an entry for the forward mapping.
         */
        struct ForwardEntry {
            uint64_t xid; ///< The XID at which a mutation occurred.
            std::vector<uint64_t> eids; ///< The new EID(s) that were generated in the XID.
        };

        struct LookupEntry {
            uint64_t start_xid;
            uint64_t latest_xid;
        };

        // note: always lock lookup before forward, before reverse to avoid deadlock
        boost::shared_mutex _mutex;

        /** Map from referenced EID -> latest XID. */
        std::map<uint64_t, LookupEntry> _lookup_map;

        /** Map from referenced EID -> list of forward mapping entries. */
        std::map<uint64_t, std::list<ForwardEntry>> _forward_map;

        /** Map from referenced EID -> list of previous versions of the EID. */
        std::map<uint64_t, std::vector<uint64_t>> _reverse_map;

        /** Map from XID -> list of EIDs referenced in the write cache. */
        std::map<uint64_t, std::vector<uint64_t>> _xid_map;
    };

    class ExtentMapper {
    public:
        /**
         * When the GC-2 updates an extent (or one of the query nodes performs a hurry-up), the
         * mapping from the old extent ID to the new extent ID(s) is recorded at the target XID.
         * This allows other actors in the system to retrieve the new extent IDs using forward_map()
         * and avoid performing the roll-forward themselves.
         */
        void add_mapping(uint64_t tid, uint64_t target_xid, uint64_t old_eid, const std::vector<uint64_t> &new_eids);

        /**
         * When the GC-1 adds data to the write cache, it records the XID at which it performed the
         * lookup for the given extent ID.  This way, when the GC-2 runs, it can correctly identify
         * if additional changes occurred to the extent ID it gets from the write cache.
         */
        void set_lookup(uint64_t tid, uint64_t target_xid, uint64_t extent_id);

        /**
         * Finds the set of new extent IDs that were generated through a roll-forward so that the
         * changes can be correctly applied to them instead of to the extent ID provided by the
         * write cache from GC-1.
         */
        std::vector<uint64_t> forward_map(uint64_t tid, uint64_t target_xid, uint64_t extent_id);

        /**
         * Checks if the extent_id has outstanding changes in the write cache with a lookup_xid <
         * access_xid and with a target_xid < the provided target_xid.  Returns the extent IDs that
         * were stored in the write cache containing such changes.
         */
        std::vector<uint64_t> reverse_map(uint64_t tid, uint64_t access_xid, uint64_t target_xid, uint64_t extent_id);

        /**
         * Removes any metadata stored in the ExtentMapper with a target_xid <= commit_xid.
         */
        void expire(uint64_t tid, uint64_t commit_xid);

    private:
        /**
         * Retrieves the TableExtentMapper based on the table ID.
         */
        std::shared_ptr<TableExtentMapper> _get_table(uint64_t tid, bool is_write);

        /**
         * Updates the writer count on the table, potentially removing it from the map.
         */
        void _put_table(uint64_t tid, bool is_expire) noexcept;

    private:
        /**
         * RAII wrapper that calls _get_table() and _put_table() accordingly.
         */
        class Mapper {
        public:
            Mapper(ExtentMapper *mapper, uint64_t tid, bool is_write = false)
                : _mapper(mapper),
                  _tid(tid),
                  _is_write(is_write)
            {
                _table = _mapper->_get_table(_tid, _is_write);
            }

            ~Mapper()
            {
                if (_is_write) {
                    _mapper->_put_table(_tid, _try_evict);
                }
            }

            void try_evict() {
                _try_evict = true;
            }

            bool is_null() const {
                return (_table == nullptr);
            }

            TableExtentMapper &operator*() {
                return *_table;
            }

            TableExtentMapper *operator->() {
                return _table.get();
            }

        private:
            ExtentMapper *_mapper;
            uint64_t _tid;
            bool _is_write;
            bool _try_evict = false;
            std::shared_ptr<TableExtentMapper> _table;
        };

        using TableEntry = std::pair<int, std::shared_ptr<TableExtentMapper>>;

        boost::mutex _mutex;

        /** Map from Table ID -> <writer count, TableExtentMapper> */
        std::map<uint64_t, TableEntry> _table_map;
    };
}
