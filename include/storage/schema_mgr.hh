#pragma once

#include <boost/thread.hpp>

#include <storage/schema.hh>
#include <storage/constants.hh>

namespace springtail {

    class ExtentType;

    /** Interface for accessing all of the schemas for a specific table.  This includes retrieving
     *  the data schema, primary and secondary index schemas, and data in the write cache -- all at
     *  a specific XID. */
    class SchemaMgr {
    public:
        /**
         * @brief getInstance() of singleton SchemaMgr; create if it doesn't exist.
         * @return instance of SchemaMgr
         */
        static SchemaMgr *get_instance();

        /**
         * @brief Shutdown the SchemaMgr singleton.
         */
        static void shutdown();

        /**
         * Retrieve the column metadata for a given table at a given XID/LSN.
         */
        std::map<uint32_t, SchemaColumn> get_columns(uint64_t table_id, uint64_t xid, uint64_t lsn);

        /**
         * Retrieve the schema for a given table at a given point in time.
         * @param table_id The ID of the table being requested.
         * @param extent_xid The XID of the extent being processed.
         * @param target_xid The XID that the query is executing at.
         * @param lsn An optional LSN (logical sequence number) which tells you which schema changes within a given XID to apply up through.
         */
        std::shared_ptr<Schema> get_schema(uint64_t table_id, uint64_t extent_xid, uint64_t target_xid, uint64_t lsn=constant::MAX_LSN);

        /**
         * Retrieve an ExtentSchema for a given table at a given XID that can be used for writing /
         * updating the extent.  This function assumes we are retrieving the schema of the table's
         * underlying data.
         *
         * @param table_id The table we need the schema for.
         * @param xid The XID that we need the schema at.
         */
        std::shared_ptr<ExtentSchema> get_extent_schema(uint64_t table_id, uint64_t xid);

        /**
         * Helper function to generate a SchemaUpdate that defines the change between the old and
         * new schema.  The old and new schema must be guaranteed to be the result of only a single
         * schema modification.
         * @param old_schema The columns of the previous version of the schema.
         * @param new_schema The columns of the new version of the schema.
         * @param xid The XID that the modification occurred at.
         * @param lsn The LSN in the PG log of the modification to ensure correct ordering when applying changes.
         */
        SchemaColumn generate_update(const std::map<uint32_t, SchemaColumn> &old_schema,
                                     const std::map<uint32_t, SchemaColumn> &new_schema,
                                     uint64_t xid, uint64_t lsn);

    protected:
        static SchemaMgr *_instance; ///< static instance (singleton)
        static boost::mutex _instance_mutex; ///< protects lookup/creation of singleton _instance

        /**
         * @brief Construct a new SchemaMgr object
         */
        SchemaMgr();

    private:
        /** A helper class that holds all of the information about a table schema in-memory. */
        class SchemaInfo {
        public:
            /**
             * SchemaInfo constructor.  Reads the schema metadata from the system tables and
             * populates its internal structures.
             */
            SchemaInfo(uint64_t table_id);

            /**
             * Retrieve the keys for an index of the table.
             *
             * @param index_id The index to retrieve the keys for.
             * @param xid The XID at which to retrieve the keys.
             */
            std::vector<std::string> get_index_keys(uint64_t index_id, uint64_t xid);

            /**
             * Retrieve the schema for an extent written at a specific XID.
             * 
             * @param extent_xid The XID of the extent being processed.
             */
            std::shared_ptr<ExtentSchema> get_extent_schema(uint64_t extent_xid);

            /**
             * Construct a VirtualSchema on top of an ExtentSchema that brings the schema forward to
             * the provided target XID and LSN so that data can be read from the extent as thought
             * it were at the target XID.
             *
             * @param extent_xid The XID that the base extent was written at.
             * @param target_xid The XID that the query is executing at.
             * @param lsn An optional LSN (logical sequence number) which tells you which schema changes within a given XID to apply up through.
             */
            std::shared_ptr<VirtualSchema> get_virtual_schema(uint32_t extent_xid, uint64_t target_xid, uint64_t lsn=constant::MAX_LSN);

            std::map<uint32_t, SchemaColumn> get_columns_for_xid_lsn(uint64_t xid, uint64_t lsn);

        private:
            /**
             * Retrieve the set of columns that are valid at the provided XID.
             * @param xid The XID that the schema should be generated for.
             */
            std::map<uint32_t, SchemaColumn> _get_columns_for_xid(uint64_t xid);


            /**
             * Read the schema metadata for the table.  Pulls the full schema history so always need
             * to read the latest available XID.
             * @param table_id The table to read schema information for.
             */
            void _read_schema_table(uint64_t table_id);

            void _read_indexes_table(uint64_t table_id);

            /**
             * Read the schema history for the table.  Pulls the full history of changes so always
             * need to read the latest available XID.
             * @param table_id The table to read schema history for.
             */
            void _read_schema_history_table(uint64_t table_id);

        private:
            /**
             * A map from <position, xid> -> list of schema columns.  The map's XID is ordered in
             * reverse xid order to quickly find which SchemaColumn list should be used for a given
             * xid using upper_bound.  The list is the ordered column definitions by LSN within the
             * XID.
             */
            std::map<uint32_t, std::map<uint64_t, std::vector<SchemaColumn>, std::greater<uint64_t>>> _column_map;

            /**
             * A map from <xid> to <primary key>, where primary key is defined as an ordered set of
             * columns that make up the primary index.
             */
            std::map<uint64_t, std::vector<uint32_t>, std::greater<uint64_t>> _primary_index;

            std::map<uint64_t, std::map<uint64_t, std::vector<uint32_t>, std::greater<uint64_t>>> _secondary_indexes;
        };

    private:
        /**
         * A key for the system schema cache.
         */
        struct SystemKey {
            uint64_t table_id;
            uint64_t index_id;
            bool is_leaf;

            SystemKey(uint64_t t, uint64_t i, bool l)
                : table_id(t),
                  index_id(i),
                  is_leaf(l)
            { }

            bool operator==(const SystemKey &other) const
            {
                return (table_id == other.table_id &&
                        index_id == other.index_id &&
                        is_leaf == other.is_leaf);
            }

            friend std::size_t hash_value(const SystemKey &k)
            {
                std::size_t seed = 0;

                boost::hash_combine(seed, k.table_id);
                boost::hash_combine(seed, k.index_id);
                boost::hash_combine(seed, k.is_leaf);

                return seed;
            }
        };

        /** A map of fixed system schemas.  Maps from System Table ID to the ExtentSchema for that table. */
        std::unordered_map<SystemKey, std::shared_ptr<ExtentSchema>, boost::hash<SystemKey>> _system_cache;

        /** A cache of SchemaInfo objects. */
        LruObjectCache<uint64_t, SchemaInfo> _cache;
    };

}
