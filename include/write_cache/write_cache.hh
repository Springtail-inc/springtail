#pragma once

#include <mutex>
#include <memory>
#include <vector>
#include <string>
#include <string_view>

#include <fmt/core.h>

namespace springtail {
    /**
     * @brief Write cache singleton interface
     */
    class WriteCache {
    
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
            DELETE='D'
        };

        /**
         * @brief Data returned when a row is fetched
         */
        struct RowData {
            uint64_t XID;
            uint64_t LSN;         
            uint64_t RID;   
            RowOp op;
            const std::string_view pkey;
            const std::string_view data;
            const std::string raw_data;

            /**
             * @brief Construct a new Row Data object; keep a reference to orig_data since
             * the pkey and data string_views are created from ptrs within it.
             */
            RowData(RowOp op, uint64_t LSN, uint64_t RID, uint64_t XID, const char * const pkey,
                     int pkey_len, const char * const data, int data_len, const std::string &&orig_data) :
                XID(XID), LSN(LSN), RID(RID), op(op), pkey(pkey, pkey_len), data(data, data_len), raw_data(orig_data)
            { }
        };
        
        /**
         * @brief Data returned when table changes are fetched
         */
        struct TableChange {
            uint64_t XID;
            uint64_t LSN;
            TableOp  op;

            /**
             * @brief Construct a new Table Change object
             */
            TableChange(TableOp op, uint64_t LSN, uint64_t XID) : XID(XID), LSN(LSN), op(op)
            { }
        };

        /**
         * @brief Get the singleton write cache instance object
         * @return WriteCache* 
         */
        static WriteCache *get_instance();

        /**
         * @brief Shutdown cache
         */
        static void shutdown();

        /**
         * @brief Start new GC @ XID; updates metadata in cache for ongoing GC
         * @param xid Upper bound on XID (inclusive)
         */
        void start_gc(uint64_t xid);

        /**
         * @brief Complete GC @ XID; update metadata to mark GC done
         * @param xid Upper bound on XID (inclusive)
         */
        void complete_gc(uint64_t xid);

        /**
         * @brief Marks table has having a table change that may affect data
         * @param tid Table ID
         * @param xid XID
         * @param xid LSN
         * @param op  Operation type (e.g., truncate)
         */
        void table_change(uint64_t tid, uint64_t xid, uint64_t LSN, TableOp op);

        /**
         * @brief Fetch all table changes for a table up to and including XID
         * @param tid Table ID
         * @param xid Upper bound on XID (inclusive)
         * @return std::vector<TableChange> 
         */
        std::vector<std::shared_ptr<TableChange>> fetch_table_changes(uint64_t tid, uint64_t xid);

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
        void delete_row(uint64_t tid, uint64_t eid, uint64_t xid, uint64_t LSN, std::string_view &pkey);

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
        std::shared_ptr<RowData> fetch_row(uint64_t tid, uint64_t eid, uint64_t rid, uint64_t xid);

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
        /** Singleton write cache instance */
        static WriteCache *_instance;

        /** Mutex protecting _instance in get_instance() */
        static std::mutex _instance_mutex;

        /**
         * @brief Construct a new Write Cache object
         */
        WriteCache() {}

        /**
         * @brief Destroy the Write Cache object; shouldn't be called directly use shutdown()
         */
        ~WriteCache() {}

    private:
        // delete copy constructor
        WriteCache(const WriteCache &)     = delete;
        void operator=(const WriteCache &) = delete;

        /** root table index name */
        static inline constexpr char TABLE_INDEX_NAME[] = "TBLIDX";

        // redis helpers

        /**
         * @brief Helper that calls _get_sorted_set_by_xid and converts results to a list of ids
         * @param key    string index name
         * @param xid    XID upper bound (inclusive)
         * @param count  max number of entries to return (-1 = no limit)
         * @param offset starting offset (default = 0)
         * @return std::vector<uint64_t> 
         */
        std::vector<uint64_t> _fetch_ids(const std::string &key, uint64_t xid, int count, uint64_t offset=0);

        /**
         * @brief Fetch storted set from Redis based on XID as upper bound; includes data and XID pair
         * @param key index name
         * @param xid XID upper bound (inclusive)
         * @param count  max number of entries to return (-1 = no limit)
         * @param offset starting offset (default = 0)
         * @return std::vector<std::pair<std::string, double>> data + XID (score)
         */
        std::vector<std::pair<std::string, double>> _get_sorted_set_by_xid(const std::string &key, uint64_t xid, int count=-1, uint64_t offset=0);

        /**
         * @brief Add data w/ XID as score into sorted set
         * @param key  index name
         * @param data data to insert
         * @param xid  XID as score
         */
        void _add_sorted_set_by_xid(const std::string &key, const std::string_view &data, uint64_t xid);

        /**
         * @brief Delete range of values from sorted set based on XID as upper bound
         * @param key index name
         * @param xid XID upper bound (inclusive)
         */
        void _remove_sorted_set_by_xid(const std::string &key, uint64_t xid);

        // serialization helpers

        /**
         * @brief Serialize a row to a string for upload 
         * @param pkey primary key
         * @param data row data
         * @param LSN  log sequence number
         * @param Op   row operation
         * @return std::string serialized data
         */
        std::string _serialize_row(const std::string_view &pkey, const std::string_view &data, uint64_t LSN, RowOp op);

        /**
         * @brief Serialize table change operation to a string for upload
         * @param LSN log sequence number
         * @param op  operation type
         * @return std::string 
         */
        std::string _serialize_table_change(uint64_t LSN, TableOp op);

        /**
         * @brief Deserialize row data into structure (zero copy)
         * @param data row data to deserialize -- moved into RowData
         * @param XID XID for row
         * @param RID row ID for row
         * @return std::shared_ptr<RowData> 
         */
        std::shared_ptr<RowData> _deserialize_row(std::string &data, uint64_t XID, uint64_t RID);

        /**
         * @brief Deserialize table change data into structure
         * @param data table change data to deserialize
         * @param XID XID for table change
         * @return std::shared_ptr<TableOp> 
         */
        std::shared_ptr<TableChange> _deserialize_table_change(const std::string &data, uint64_t XID);

        // helpers to get table index names

        /** Get table change index name */
        inline std::string _get_table_change_index(uint64_t tid) {
            return fmt::format("CHG:TID:{}", tid);
        }

        /** Get root table index name */
        inline std::string _get_table_index() {
            return TABLE_INDEX_NAME;
        }
        
        /** Get table index name for specific table: TID:tid */
        inline std::string _get_tid_index(uint64_t tid) {
            return fmt::format("TID:{}", tid);
        }

        /** Get extent index name for specific table, extent: TID:tid:EID:eid */
        inline std::string _get_eid_index(uint64_t tid, uint64_t eid) {
            return fmt::format("TID:{}:EID:{}", tid, eid);
        }

        /** Get extent index name for specific table, extent, row: TID:tid:EID:eid:RID:rid */
        inline std::string _get_rid_index(uint64_t tid, uint64_t eid, uint64_t rid) {
            return fmt::format("TID:{}:EID:{}:RID:{}", tid, rid);
        }
    };
}