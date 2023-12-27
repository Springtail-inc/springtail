#pragma once

#include <mutex>
#include <string_view>

namespace springtail {
    /**
     * @brief Write cache singleton interface
     */
    class WriteCache {
    
    public:
        /**
         * @brief Get the singleton write cache instance object
         * @return WriteCache* 
         */
        static WriteCache *get_instance();

        /**
         * @brief Shutdown cache
         */
        void shutdown();

        /**
         * @brief Insert row into cache
         * @param tid   Table ID
         * @param extid Extent ID (offset)
         * @param xid   XID
         * @param pkey  Primary key (or full row)
         * @param data  Row data
         */
        void insert_row(uint64_t tid, uint64_t extid, uint64_t xid, 
                        const std::string_view &pkey, const std::string_view &data);

        fetch_tables_by_xid(uint64_t xid, int count)

    protected:
        /** Singleton write cache instance */
        static WriteCache *_instance;

        /** Mutex protecting _instance in get_instance() */
        static std::mutex _instance_mutex;

        WriteCache();

        ~WriteCache() {}

    private:
        // delete copy constructor
        WriteCache(const WriteCache &)     = delete;
        void operator=(const WriteCache &) = delete;
    };
}