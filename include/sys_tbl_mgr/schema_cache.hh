#include <list>

#include <sys_tbl_mgr/table.hh>

namespace springtail::sys_tbl_mgr {

    /**
     * A class for holding schema metadata from the sys_tbl_mgr::Service.  It currently only caches
     * the full schema metadata.
     *
     * Originally I had planned to also cache the history of changes, but integrating the history of
     * changes to identify gaps and connect them to the correct base schemas was turning into a lot
     * of complexity.  Instead, I would suggest that we replace the entire SysTblMgr with a cache of
     * system table rows that works using the WriteCache.  Changes that haven't been sync'd to disk
     * can be merged into the results, allowing each node in the system to locally cache whatever
     * schema and change information it needs without contacting a centralized SysTblMgr service.
     *
     * Once that change is made, if needed, we can expand this to layer a more complex cache of
     * constructed objects on top of that, but it becomes a lot cheaper to construct missing
     * metadata since the centralized page cache will hold the recently used data we need.
     */
    class SchemaCache {
    public:
        using PopulateFn = std::function<SchemaMetadataPtr(uint64_t, uint64_t, const XidLsn &)>;

    public:
        explicit SchemaCache(int capacity)
            : _capacity(capacity)
        { }

        /** Retrieve a schema from the cache based on an access key.  If it's a cache miss, populate
            the cache using the provided function. */
        SchemaMetadataPtr get(uint64_t db, uint64_t tid, const XidLsn &xid, PopulateFn populate);
        
        /**
         * Checks for an entry that contains the key and re-indexes it with the key as the ending
         * position for the entry.  Used to mark the "current" entry for a schema or root as ending
         * at the given key.
         */
        void invalidate_table(uint64_t db, uint64_t tid, const XidLsn &xid);

        /**
         * Performs an invalidate() but uses the index ID to lookup the table ID for use in the case
         * of dropping an index.  Necessary because we aren't provided the table ID in that case.
         */
        void invalidate_by_index(uint64_t db, uint64_t index_id, const XidLsn &xid);

        /**
         * Invalidates all of the tables within a given DB.  Records the provided XID as the most
         * recently seen DDL change.  Used by the FDW.
         */
        void invalidate_db(uint64_t db, const XidLsn &xid);

    private:
        struct SchemaEntry;
        using SchemaEntryPtr = std::shared_ptr<SchemaEntry>;
        using SchemaMap = std::map<std::pair<uint64_t, uint64_t>, SchemaEntryPtr>;
        using LruList = std::list<typename SchemaMap::iterator>;
        using IndexMap = std::map<std::pair<uint64_t, uint64_t>, uint64_t>;

        struct SchemaEntry {
            XidLsn start_xid;
            SchemaMetadataPtr schema;

            LruList::iterator lru_i;
            bool fetching = true;
            bool invalidated = false;
            std::condition_variable cond;
        };

    private:
        /**
         * Internal helper to populate the cache using the provided populate function.
         */
        SchemaEntryPtr _fetch_locked(std::unique_lock<std::mutex> &lock,
                                     uint64_t db,
                                     uint64_t tid,
                                     const XidLsn &xid,
                                     PopulateFn populate);

        void _invalidate_locked(uint64_t db, uint64_t tid, const XidLsn &xid);

        void _remove_locked(SchemaMap::iterator schema_i);

        void _make_space_locked();

    private:
        std::mutex _mutex;

        SchemaMap _schema_map; ///< Cache of SchemaEntry with lookup by Key
        LruList _lru; ///< LRU list of cached schema entries
        IndexMap _index_map; ///< Map of db id -> index oid -> table oid

        uint64_t _capacity; ///< Max number of entries in the cache

        std::map<uint64_t, XidLsn> _latest_xid; ///< Map from db -> latest known XID with a DDL change
    };
}
