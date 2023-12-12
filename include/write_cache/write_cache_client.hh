#pragma once

#include <mutex>
#include <memory>
#include <vector>
#include <string>
#include <string_view>

#include <common/grpc_channel_pool.hh>

#include <write_cache/common.hh>

namespace springtail {

    class WriteCacheClient 
    {
    public:
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
         * @brief Marks table has having a table change that may affect data
         * @param tid Table ID
         * @param xid XID
         * @param xid LSN
         * @param op  Operation type (e.g., truncate)
         */
        void insert_table_change(uint64_t tid, uint64_t xid, uint64_t LSN, WriteCache::TableOp op);

        /**
         * @brief Fetch all table changes for a table up to and including XID
         * @param tid Table ID
         * @param xid Upper bound on XID (inclusive)
         * @return std::vector<TableChange> 
         */
        std::vector<std::shared_ptr<WriteCache::TableChange>> fetch_table_changes(uint64_t tid, uint64_t xid);

        /**
         * @brief Insert row into cache
         * @param tid  Table ID
         * @param eid  Extent ID (offset)
         * @param xid  XID
         * @param LSN  log seq number
         * @param pkey Primary key
         * @param data Row data
         */
        void insert_row(uint64_t tid, uint64_t eid, uint64_t xid, uint64_t LSN,
                        const std::string_view &pkey, const std::string_view &data);

        /**
         * @brief Update a row, internally results in a delete entry for old row and insert of new row
         * @param tid      Table ID
         * @param old_eid  Old extent ID (one being updated)
         * @param new_eid  New extent ID (may be same as old)
         * @param xid      XID
         * @param LSN      log seq number
         * @param old_pkey Old primary key
         * @param new_pkey New primary key (may be the same as old)
         * @param data     Row data
         */
        void update_row(uint64_t tid, uint64_t old_eid, uint64_t new_eid,
                        uint64_t xid, uint64_t LSN, const std::string_view &old_pkey,
                        const std::string_view &new_pkey, const std::string_view &data);

        /**
         * @brief Delete row, internally marks row as deleted
         * @param tid  Table ID
         * @param eid  Extent ID
         * @param xid  XID
         * @param LSN  log seq number
         * @param pkey Primary key
         */
        void delete_row(uint64_t tid, uint64_t eid, uint64_t xid, uint64_t LSN, const std::string_view &pkey);

        /**
         * @brief Fetch list of table IDs that have been dirtied prior to and up to XID
         * @param xid Upper bound on XID (inclusive)
         * @param count Max TIDs to return (may return less)
         * @param offset Optional offset to start from
         * @return std::vector<uint64_t> a list of table IDs
         */
        std::vector<uint64_t> fetch_tables(uint64_t xid, int count, uint64_t offset=0);

        /**
         * @brief Fetch list of extent IDs that have been dirtied prior to and up to XID
         * @param tid Table ID for extent
         * @param xid Upper bound on XID (inclusive)
         * @param count Max EIDs to return (may return less)
         * @param offset Optional offset to start from
         * @return std::vector<uint64_t> a list of extent IDs
         */
        std::vector<uint64_t> fetch_extents(uint64_t tid, uint64_t xid, int count, uint64_t offset=0);

        /**
         * @brief Fetch list of ALL row IDs that have been dirtied prior to and up to XID
         * @param tid Table ID for extent
         * @param eid Extent ID for row
         * @return std::vector<uint64_t> a list of row IDs
         */
        std::vector<uint64_t> fetch_rows(uint64_t tid, uint64_t eid, uint64_t xid, int count, uint64_t offset=0);

        /**
         * @brief Fetch data for a row by row ID
         * @param tid Table ID
         * @param eid Extent ID 
         * @param rid Row ID (this is returned in fetch_rows(); it is a hashed value of the row pkey)
         * @return std::shared_ptr<RowData> row data includes pkey, LSN, XID, row data
         */
        std::shared_ptr<WriteCache::RowData> fetch_row(uint64_t tid, uint64_t eid, uint64_t rid, uint64_t xid);

        // store RID: [ data@xid, ... ]  when fetching data only request latest data prior to xid

        /**
         * @brief Mark a previously dirty extent as clean; removes all row data for that
         *        extent by XID up to and including provided XID; fixes up indexes up the chain
         * @param tid Table ID
         * @param eid Extent ID (offset)
         * @param xid Upper bound on XID (inclusive) -- the current GC XID
         */
        void clean_extent(uint64_t tid, uint64_t eid, uint64_t xid);

        /**
         * @brief Evict all data for a specific XID (XID may have aborted)
         * @param xid XID to remove (single XID only)
         */
        void evict(uint64_t xid);


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

        std::shared_ptr<GrpcChannelPool> _channel_pool;
    };

} // namespace springtail