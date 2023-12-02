#pragma once

#include <map>
#include <string>
#include <vector>

#include <common/object_cache.hh>
#include <storage/schema_column.hh>

namespace springtail {
    // pre-declare classes
    class Field;

    /** The available types for fields. */
    enum class SchemaType : uint8_t {
        ARRAY = 1,
        TEXT = 3,
        UINT64 = 4,
        INT64 = 5,
        UINT32 = 6,
        INT32 = 7,
        UINT16 = 8,
        INT16 = 9,
        UINT8 = 10,
        INT8 = 11,
        BOOLEAN = 12,
        DECIMAL128 = 13,
        FLOAT64 = 14,
        FLOAT32 = 15,
        BINARY = 16
    };

    /** The types of schema column updates that can be applied to a schema. */
    enum class SchemaUpdateType : uint8_t {
        NEW_COLUMN = 0,
        REMOVE_COLUMN = 1,
        NAME_CHANGE = 2,
        NULLABLE_CHANGE = 3,
        TYPE_CHANGE = 4
    };

    /**
     * An object that holds all of the information about a column over a given xid range.
     */
    struct SchemaColumn {
        uint64_t start_xid;
        uint64_t end_xid;
        std::string name;
        uint32_t position;
        SchemaType type;
        bool nullable;
        std::optional<std::string> default_value;

        SchemaColumn(uint64_t start_xid,
                     uint64_t end_xid,
                     const std::string &name,
                     uint32_t position,
                     SchemaType type,
                     bool nullable,
                     std::optional<std::string> default_value=std::optional<std::string>())
            : start_xid(start_xid),
              end_xid(end_xid),
              name(name),
              position(position),
              type(type),
              nullable(nullable),
              default_value(default_value)
        { }

        SchemaColumn(const std::string &name,
                     uint32_t position,
                     SchemaType type,
                     bool nullable,
                     std::optional<std::string> default_value=std::optional<std::string>())
            : start_xid(0),
              end_xid(std::numeric_limits<uint64_t>::max),
              name(name),
              position(position),
              type(type),
              nullable(nullable),
              default_value(default_value)
        { }

        // default copy constructor
        SchemaColumn(const SchemaColumn &column) = default;
    };

    /**
     * An object that holds all of the information about a column modification in the
     * form of the updated column information.
     */
    struct SchemaUpdate {
        uint64_t xid;
        uint64_t lsn;
        SchemaUpdateType update_type;
        uint32_t position;
        std::string name;
        SchemaType type;
        bool nullable;
        std::optional<std::string> default_value;
    };


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
        static const uint64_t PRIMARY_INDEXES_TID = 4;

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
        /** A map of fixed system schemas. */
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

    /**
     * The interface for schema objects.
     */
    class Schema {
    public:
        virtual ~Schema() = default;

        /** Checks if a given column exists. */
        virtual bool has_field(const std::string &name) = 0;

        /** Returns a field accessor for the requested column. */
        virtual std::shared_ptr<Field> get_field(const std::string &name) const = 0;
    };

    /**
     * Defines the schema for a specific table.  Creates field accessors to retrieve data at a
     * specified target XID + LSN from an extent that was written at a potentially earlier base XID.
     */
    class ExtentSchema : public Schema {
    private:
        /** The width of fixed data for a single row. */
        uint32_t _row_size;

        /** Map of column names to fields. */
        std::map<std::string, std::shared_ptr<MutableField>> _field_map;

    protected:
        /**
         * Construct the set of column fields based on the column definitions.
         * @param columns A map from column position to column definition.
         */
        void _populate(const std::map<uint32_t, SchemaColumn> columns);

    public:
        /**
         * Constructor.
         * @param columns Map from column position to the SchemaColumn definition.
         */
        ExtentSchema(const std::vector<SchemaColumn> &columns) {
            std::map<uint32_t, SchemaColumn> column_map;
            for (auto &&column : columns) {
                column_map[column.position] = column;
            }

            // populate the field map using the column definitions
            _populate(column_map);
        }

        /**
         * Constructor.
         * @param columns Map from column position to the SchemaColumn definition.
         */
        ExtentSchema(const std::map<uint32_t, SchemaColumn> columns)
        {
            _populate(columns);
        }

        /** Copy constructor. */
        ExtentSchema(const Schema &schema) = default;

        /** Returns the fixed width for a single row. */
        uint32_t row_size() const {
            return _row_size;
        }

        /**
         * Retrieve a mutable field bound to a given column for reading from and writing to extents.
         * @param name The name of the column to retrieve the field for.
         * @return A pointer to the mutable field accessor.
         */
        std::shared_ptr<MutableField>
        get_mutable_field(const std::string &name)
        {
            auto &&i = _field_map.find(name);
            if (i == _field_map.end()) {
                throw SchemaError("No such column");
            }
            return i.second;
        }

        /**
         * Retrieve a non-mutable field bound to a given column for reading from extents.
         * @param name The name of the column to retrieve the field for.
         */
        std::shared_ptr<Field>
        get_field(const std::string &name)
        {
            return get_mutable_field(name);
        }

        /**
         * Check for the existence of a field.
         * @param name The name of the column to check existence.
         */
        bool has_field(const std::string &name)
        {
            auto &&i = _field_map.find(name);
            return (i != _field_map.end());
        }
    };

    /**
     * Manages a virtual schema that is layered on top of the ExtentSchema.  The VirtualSchema uses
     * the history of schema updates to bring the ExtentSchema forward to a specific target XID +
     * LSN, providing a way to access data as if it's at the target XID even though it's stored in
     * the extent at an earlier XID.
     */
    class VirtualSchema : public Schema {
    private:
        /** Map from column name to field accessor.  */
        std::map<std::string, std::shared_ptr<Field>> _field_map;

    private:
        /**
         * Helper function to construct a field with a constant value.
         * @param type The type of the column data.
         * @param value The constant value to return, stored as a string.
         */
        std::shared_ptr<Field> _make_const(SchemaType type, const std::string &value);

        /**
         * Helper function to construct a field that returns a provided fallback value if the
         * underlying field is null.  Otherwise returns the value of the underlying field.
         * @param field The underlying field.
         * @param fallback The fallback value, stored as a string.
         */
        std::shared_ptr<Field> _make_null_wrapper(std::shared_ptr<Field> field, const std::string &fallback);

    public:
        /**
         * Constructor to populate the field map for this schema based on the provided base schema and schema updates.
         * @param extent_schema The schema for the underlying extent data.
         * @param columns The column definitions for the underlying extent data.
         * @param updates The updates to apply to the extent schema to generate the virtual schema.
         */
        VirtualSchema(std::shared_ptr<ExtentSchema> extent_schema,
                      const std::map<uint32_t, SchemaColumn> &columns,
                      const std::vector<SchemaUpdate> &updates);

        /**
         * Checks if the column exists within the virtual schema.
         * @param name The name of the column being requested.
         */
        bool has_field(const std::string &name)
        {
            return (_field_map.find(name) != _field_map.end());
        }

        /**
         * Returns a field accessor for the column within the virtual schema.
         * @param name The name of the column being requested.
         */
        std::shared_ptr<Field> get_field(const std::string &name) const
        {
            auto &&i = _field_map.find(name);
            if (i == _field_map.end()) {
                throw SchemaError("Could not find requested column");
            }

            return i.second;
        }
    };
}
