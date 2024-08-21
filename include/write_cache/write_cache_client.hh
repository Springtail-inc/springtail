#pragma once

#include <mutex>
#include <memory>
#include <vector>
#include <string>
#include <string_view>
#include <iostream>

#include <common/object_pool.hh>

#include <thrift/write_cache/ThriftWriteCache.h> // generated file

namespace springtail {

    class WriteCacheClient
    {
    public:
        /**
         * @brief Table operation type
         */
        enum TableOp : char {
            TRUNCATE='T',
            SCHEMA_CHANGE='S'
        };

        /**
         * @brief Row operation type
         */
        enum RowOp : char {
            INSERT='I',
            UPDATE='U',
            DELETE='D'
        };

        /**
         * @brief data describing a row
         */
        struct RowData {
            uint64_t xid;
            uint64_t xid_seq;
            std::string pkey;
            std::string old_pkey; // only for update that updates pkey
            std::string data;
            RowOp op;
        };

        /**
         * @brief Data returned when table changes are fetched
         */
        struct TableChange {
            uint64_t xid;
            uint64_t xid_seq;
            TableOp  op;
        };

        /**
         * @brief Get the singleton write cache client instance object
         * @return WriteCacheClient *
         */
        static WriteCacheClient *get_instance();

        /**
         * @brief Shutdown cache
         */
        static void shutdown();

        /**
         * @brief Ping the server
         */
        void ping();

        /**
         * @brief Marks table has having a table change that may affect data
         * @param db_id Database ID
         * @param tid Table ID
         * @param change table change
         */
        void add_table_change(uint64_t db_id, uint64_t tid, TableChange &change);

        /**
         * @brief Fetch all table changes for a table up to and including XID
         * @param db_id Database ID
         * @param tid Table ID
         * @param start_xid start of xid range (exclusive) (start, end]
         * @param end_xid   end of xid range (inclusive)
         * @return std::vector<TableChange>
         */
        std::vector<TableChange> fetch_table_changes(uint64_t db_id, uint64_t tid, uint64_t start_xid, uint64_t end_xid);

        /**
         * @brief Evict table changes between XID range
         * @param db_id Database ID
         * @param tid table ID
         * @param start_xid start of xid range (exclusive)
         * @param end_xid end of xid range (inclusive)
         */
        void evict_table_changes(uint64_t db_id, uint64_t tid, uint64_t start_xid, uint64_t end_xid);

        /**
         * @brief Add one or more rows to the cache for a specific table and extent and operation type
         * @param db_id Database ID
         * @param tid  Table ID
         * @param eid  Extent ID
         * @param rows Set of rows
         */
        void add_rows(uint64_t db_id, uint64_t tid, uint64_t eid, std::vector<RowData> &&rows);

        /**
         * @brief Fetch list of table IDs that have been dirtied prior to and up to XID
         * @param db_id Database ID
         * @param start_xid start of xid range (exclusive) (start, end]
         * @param end_xid   end of xid range (inclusive)
         * @param count Max TIDs to return (may return less)
         * @param cursor In/Out cursor, in: set to 0 for start of range, out: current position
         * @return std::vector<uint64_t> a list of table IDs; if count > vector size, no more items
         */
        std::vector<uint64_t> list_tables(uint64_t db_id, uint64_t start_xid, uint64_t end_xid, uint32_t count, uint64_t &cursor);

        /**
         * @brief Fetch list of extent IDs that have been dirtied prior to and up to XID
         * @param db_id Database ID
         * @param tid Table ID for extent
         * @param start_xid start of xid range (exclusive) (start, end]
         * @param end_xid   end of xid range (inclusive)
         * @param count Max EIDs to return (may return less)
         * @param cursor In/Out cursor, in: set to 0 for start of range, out: current position
         * @return std::vector<uint64_t> a list of extent IDs; if count > vector size, no more items
         */
        std::vector<uint64_t> list_extents(uint64_t db_id, uint64_t tid, uint64_t start_xid, uint64_t end_xid, uint32_t count, uint64_t &cursor);

        /**
         * @brief Fetch list of ALL row IDs that have been dirtied prior to and up to XID
         * @param db_id Database ID
         * @param tid Table ID for extent
         * @param eid Extent ID for row
         * @param start_xid start of xid range (exclusive) (start, end]
         * @param end_xid   end of xid range (inclusive)
         * @param cursor In/Out cursor, in: set to 0 for start of range, out: current position
         * @return std::vector<uint64_t> a list of row IDs; if count > vector size, no more items
         */
        std::vector<RowData> fetch_rows(uint64_t db_id, uint64_t tid, uint64_t eid, uint64_t start_xid,
                        uint64_t end_xid, uint32_t count, uint64_t &cursor);

        /**
         * @brief Mark a previously dirty table as clean; removes all row data for that
         *        table by XID up to and including provided XID; fixes up indexes up the chain
         * @param db_id Database ID
         * @param tid Table ID
         * @param start_xid start of xid range (exclusive) (start, end]
         * @param end_xid   end of xid range (inclusive)
         */
        void evict_table(uint64_t db_id, uint64_t tid, uint64_t start_xid, uint64_t end_xid);

        /**
         * @brief Mark an extent as clean
         * @param db_id Database ID
         * @param tid Table ID
         * @param eid Extent ID
         * @param start_xid start of xid range (exclusive) (start, end]
         * @param end_xid   end of xid range (inclusive)
         */
        void set_clean_flag(uint64_t db_id, uint64_t tid, uint64_t eid, uint64_t start_xid, uint64_t end_xid);

        /**
         * @brief Reset the extent clean flag (unset it)
         * @param db_id Database ID
         * @param tid Table ID
         * @param start_xid start of xid range (exclusive) (start, end]
         * @param end_xid   end of xid range (inclusive)
         */
        void reset_clean_flag(uint64_t db_id, uint64_t tid, uint64_t start_xid, uint64_t end_xid);

        /**
         * @brief Add a GC-2 mapping to the extent mapper.  See TableExtentMapper::add_mapping().
         * @param db_id Database ID
         * @param tid Table ID
         * @param target_xid The XID at which the mapping occurred.
         * @param old_eid The extent ID that was modified.
         * @param new_eids The new extent IDs that were written to replace the old_eid.
         */
        void add_mapping(uint64_t db_id, uint64_t tid, uint64_t target_xid, uint64_t old_eid,
                 const std::vector<uint64_t> &new_eids);

        /**
         * @brief Add a record of the use of and extent_id at a given XID within GC-1.  See
         *        TableExtentMapper::set_lookup().
         * @param db_id Database ID
         * @param tid Table ID
         * @param target_xid The XID at which the lookup occurred.
         * @param extent_id The extent ID that was referenced in the GC-1 lookup.
         */
        void set_lookup(uint64_t db_id, uint64_t tid, uint64_t target_xid, uint64_t extent_id);

        /**
         * @brief Maps the extent_id used at GC-1 lookup into the correct set of extent IDs that
         *        exist at the given XID given GC-2 mutations. See TableExtentMapper::forward_map().
         * @param db_id Database ID
         * @param tid Table ID
         * @param target_xid The XID at which the lookup occurred.
         * @param extent_id The extent ID that was referenced in the GC-1 lookup.
         */
        std::vector<uint64_t> forward_map(uint64_t db_id, uint64_t tid, uint64_t target_xid, uint64_t extent_id);

        /**
         * @brief Maps the extent_id found at a given XID to the set of extent IDs used in GC-1 to
         *        populate the write cache. Used by hurry-ups in the query nodes.  GC-2 doesn't need
         *        to do this since it knows its already applied all changes from the write
         *        cache. See TableExtentMapper::reverse_map().
         * @param db_id Database ID
         * @param tid Table ID
         * @param target_xid The XID at which the lookup occurred.
         * @param extent_id The extent ID that was referenced in the GC-1 lookup.
         */
        std::vector<uint64_t> reverse_map(uint64_t db_id, uint64_t tid, uint64_t access_xid, uint64_t target_xid,
                          uint64_t extent_id);

        /**
         * @brief Clear any mappings stored against a given table up through the provided commit XID.
         * @param db_id Database ID
         * @param tid The table being expired.
         * @param commit_xid The XID at through which all changes have been applied.
         */
        void expire_map(uint64_t db_id, uint64_t tid, uint64_t commit_xid);
        /** Singleton write cache client instance */
        static WriteCacheClient *_instance;

        /** Mutex protecting _instance in get_instance() */
        static std::mutex _instance_mutex;

        /**
         * @brief Construct a new Write Cache Client object
         */
        WriteCacheClient();

        /**
         * @brief Destroy the Write Cache Client object; shouldn't be called directly use shutdown()
         */
        ~WriteCacheClient() {}

    private:
        // delete copy constructor
        WriteCacheClient(const WriteCacheClient &) = delete;
        void operator=(const WriteCacheClient &)   = delete;

        // the following is for handling cached thrift clients from the object pool
        // we wrap the client in a struct whose deallocator will release it back to the pool

        /** Thrift client object pool */
        std::shared_ptr<ObjectPool<thrift::write_cache::ThriftWriteCacheClient>> _thrift_client_pool;

        /** Struct to wrap the client pool and client object to ensure it gets release back */
        struct ThriftClient {
            std::shared_ptr<ObjectPool<thrift::write_cache::ThriftWriteCacheClient>> pool;
            std::shared_ptr<thrift::write_cache::ThriftWriteCacheClient> client;
            ~ThriftClient() {
                pool->put(client);
            }
        };

        /**
         * @brief Helper function to fetch a thrift client from the object pool wrapped in
         *        a struct to ensure its proper release to the pool
         */
        inline ThriftClient _get_client()
        {
            std::shared_ptr<thrift::write_cache::ThriftWriteCacheClient> client = _thrift_client_pool->get();
            ThriftClient c = { _thrift_client_pool, client };
            return c;
        }

    };

} // namespace springtail
