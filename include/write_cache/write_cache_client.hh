#pragma once

#include <mutex>
#include <memory>
#include <vector>
#include <string>
#include <string_view>
#include <iostream>

#include <common/object_pool.hh>

namespace springtail {

    class ThriftWriteCacheClient; // forward decl to avoid messy includes

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
            std::shared_ptr<std::string_view> pkey;
            std::shared_ptr<std::string_view> data;
            bool delete_flag; // only used in response
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
         * @param tid Table ID
         * @param changes List of table changes
         */
        void add_table_changes(uint64_t tid, std::vector<TableChange> changes);

        /**
         * @brief Fetch all table changes for a table up to and including XID
         * @param tid Table ID
         * @param start_xid start of xid range (exclusive) (start, end]
         * @param end_xid   end of xid range (inclusive)
         * @return std::vector<TableChange> 
         */
        std::vector<TableChange> fetch_table_changes(uint64_t tid, uint64_t start_xid, uint64_t end_xid);

        /**
         * @brief Add one or more rows to the cache for a specific table and extent and operation type
         * @param tid  Table ID
         * @param eid  Extent ID
         * @param op   Row operation type
         * @param rows Set of rows
         */
        void add_rows(uint64_t tid, uint64_t eid, RowOp op, std::vector<RowData> rows);

        /**
         * @brief Fetch list of table IDs that have been dirtied prior to and up to XID
         * @param start_xid start of xid range (exclusive) (start, end]
         * @param end_xid   end of xid range (inclusive)
         * @param count Max TIDs to return (may return less)
         * @param cursor In/Out cursor, in: set to 0 for start of range, out: set to 0 indicates no more data
         * @return std::vector<uint64_t> a list of table IDs
         */
        std::vector<uint64_t> list_tables(uint64_t start_xid, uint64_t end_xid, int count, uint64_t &cursor);

        /**
         * @brief Fetch list of extent IDs that have been dirtied prior to and up to XID
         * @param tid Table ID for extent
         * @param start_xid start of xid range (exclusive) (start, end]
         * @param end_xid   end of xid range (inclusive)
         * @param count Max EIDs to return (may return less)
         * @param cursor In/Out cursor, in: set to 0 for start of range, out: set to 0 indicates no more data
         * @return std::vector<uint64_t> a list of extent IDs
         */
        std::vector<uint64_t> list_extents(uint64_t tid, uint64_t start_xid, uint64_t end_xid, int count, uint64_t&cursor);

        /**
         * @brief Fetch list of ALL row IDs that have been dirtied prior to and up to XID
         * @param tid Table ID for extent
         * @param eid Extent ID for row
         * @param start_xid start of xid range (exclusive) (start, end]
         * @param end_xid   end of xid range (inclusive)
         * @param cursor In/Out cursor, in: set to 0 for start of range, out: set to 0 indicates no more data
         * @return std::vector<uint64_t> a list of row IDs
         */
        std::vector<RowData> fetch_rows(uint64_t tid, uint64_t eid, uint64_t start_xid, 
                                        uint64_t end_xid, int count, uint64_t &cursor);

        /**
         * @brief Mark a previously dirty extent as clean; removes all row data for that
         *        extent by XID up to and including provided XID; fixes up indexes up the chain
         * @param tid Table ID
         * @param eid Extent ID (offset)
         * @param start_xid start of xid range (exclusive) (start, end]
         * @param end_xid   end of xid range (inclusive)
         */
        void evict_extent(uint64_t tid, uint64_t eid, uint64_t start_xid, uint64_t end_xid);

    protected:
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
        std::shared_ptr<ObjectPool<thrift::ThriftWriteCacheClient>> _thrift_client_pool;

        /** Struct to wrap the client pool and client object to ensure it gets release back */
        struct ThriftClient {
            std::shared_ptr<ObjectPool<thrift::ThriftWriteCacheClient>> pool;
            std::shared_ptr<thrift::ThriftWriteCacheClient> client;
            ~ThriftClient() { std::cout << "Releasing client to pool\n"; pool->put(client); }
        };

        /** 
         * @brief Helper function to fetch a thrift client from the object pool wrapped in 
         *        a struct to ensure its proper release to the pool
         */
        inline ThriftClient _get_client()
        {
            std::shared_ptr<thrift::ThriftWriteCacheClient> client = _thrift_client_pool->get();
            ThriftClient c = { _thrift_client_pool, client };
            return c;
        }

    };

} // namespace springtail