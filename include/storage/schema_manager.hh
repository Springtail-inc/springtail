#pragma once

#include <storage/schema.hh>

namespace springtail {

    /** Interface for accessing all of the schemas for a specific table.  This includes retrieving
     *  the data schema, primary and secondary index schemas, and data in the write cache -- all at
     *  a specific XID. */
    class SchemaManager {
    private:
        static const uint64_t SYSTEM_TABLES_MAX = 256;

        // IDs for system schemas
        static const uint64_t TABLES_TID = 1;
        static const uint64_t SCHEMAS_TID = 2;
        static const uint64_t SCHEMAS_HISTORY_TID = 3;
        // static const uint64_t PRIMARY_INDEXES_TID = 4;

    private:
        /** Column definitions for the "tables" system table. */
        static const std::vector<SchemaColumn> TABLES_SCHEMA;

        /** Column definitions for the "schemas" system table. */
        static const std::vector<SchemaColumn> SCHEMAS_SCHEMA;

        /** Column definitions for the "schemas_history" system table. */
        static const std::vector<SchemaColumn> SCHEMAS_HISTORY_SCHEMA;

    private:
        /** A helper class that holds all of the information about a table schema in-memory. */
        class SchemaInfo {
        private:
            /** A map from <position, end_xid> to quickly find which SchemaColumn definition should be used for a given xid. */
            std::map<uint32_t, std::map<uint64_t, SchemaColumn>> _column_map;

            /** A map from <position, xid> to quickly find the set of SchemaUpdate objects that
                need to be applied to a Schema to get to the target_xid and lsn. */
            std::map<uint32_t, std::map<uint64_t, std::vector<SchemaUpdate>>> _column_updates;

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


            /**
             * Read the schema history for the table.  Pulls the full history of changes so always
             * need to read the latest available XID.
             * @param table_id The table to read schema history for.
             */
            void _read_schema_history_table(uint64_t table_id);

        public:
            /**
             * SchemaInfo constructor.  Reads the schema metadata from the system tables and
             * populates its internal structures.
             */
            SchemaInfo(uint64_t table_id);

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
            std::shared_ptr<VirtualSchema> get_virtual_schema(uint32_t extent_xid, uint64_t target_xid, uint64_t lsn=0);
        };

    private:
        /** A map of fixed system schemas.  Maps from System Table ID to the ExtentSchema for that table. */
        std::unordered_map<uint64_t, std::shared_ptr<ExtentSchema>> _system_cache;

        /** A cache of SchemaInfo objects. */
        LruObjectCache<uint64_t, SchemaInfo> _cache;

    public:
        /**
         * Default construtor.  Generates the ExtentSchema objects for the system cache.
         */
        SchemaManager();

        /**
         * Retrieve the schema for a given table at a given point in time.
         * @param table_id The ID of the table being requested.
         * @param extent_xid The XID of the extent being processed.
         * @param target_xid The XID that the query is executing at.
         * @param lsn An optional LSN (logical sequence number) which tells you which schema changes within a given XID to apply up through.
         */
        std::shared_ptr<const Schema> get_schema(uint64_t table_id, uint64_t extent_xid, uint64_t target_xid, uint64_t lsn = 0);

        /**
         * Retrieve an ExtentSchema for a given table at a given XID that can be used for writing / updating the extent.
         *
         * @param table_id The table we need the schema for.
         * @param xid The XID that we need the schema at.
         */
        std::shared_ptr<const ExtentSchema> get_extent_schema(uint64_t table_id, uint64_t xid);

        /**
         * Helper function to generate a SchemaUpdate that defines the change between the old and
         * new schema.  The old and new schema must be guaranteed to be the result of only a single
         * schema modification.
         * @param old_schema The columns of the previous version of the schema.
         * @param new_schema The columns of the new version of the schema.
         * @param xid The XID that the modification occurred at.
         * @param lsn The LSN in the PG log of the modification to ensure correct ordering when applying changes.
         */
        SchemaUpdate generate_update(const std::map<uint32_t, SchemaColumn> &old_schema,
                                     const std::map<uint32_t, SchemaColumn> &new_schema,
                                     uint64_t xid, uint64_t lsn);
    };

}
