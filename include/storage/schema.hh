#pragma once

#include <map>
#include <string>
#include <vector>

#include <common/object_cache.hh>
#include <storage/exception.hh>
#include <storage/schema_type.hh>

namespace springtail {
    // pre-declare field classes
    class Field;
    typedef std::shared_ptr<Field> FieldPtr;

    class MutableField;
    typedef std::shared_ptr<MutableField> MutableFieldPtr;

    class Tuple;

    /** The types of schema column updates that can be applied to a schema. */
    enum class SchemaUpdateType : uint8_t {
        NEW_COLUMN = 0,
        REMOVE_COLUMN = 1,
        NAME_CHANGE = 2,
        NULLABLE_CHANGE = 3,
        RESYNC = 4,
        NEW_INDEX = 5,
        DROP_INDEX = 6,
        NO_CHANGE = 7
    };

    /**
     * An object that holds all of the information about a column over a given xid range.
     */
    struct SchemaColumn {
        uint64_t xid = 0;
        uint64_t lsn = 0;
        std::string name;
        uint32_t position;  ///< position and postgres column ID, can have holes
        SchemaType type;
        int32_t pg_type;
        bool exists;
        bool nullable;
        std::optional<uint32_t> pkey_position; ///< position in primary key, if any, 0 based no holes
        std::optional<std::string> default_value;
        SchemaUpdateType update_type = SchemaUpdateType::NEW_COLUMN;

        SchemaColumn() = default;

        SchemaColumn(uint64_t xid,
                     uint64_t lsn,
                     const std::string_view &name,
                     uint32_t position,
                     SchemaType type,
                     int32_t pg_type,
                     bool exists,
                     bool nullable,
                     std::optional<uint32_t> pkey_position=std::optional<uint32_t>(),
                     std::optional<std::string> default_value=std::optional<std::string>())
            : xid(xid),
              lsn(lsn),
              name(name),
              position(position),
              type(type),
              pg_type(pg_type),
              exists(exists),
              nullable(nullable),
              pkey_position(pkey_position),
              default_value(default_value)
        { }

        SchemaColumn(const std::string_view &name,
                     uint32_t position,
                     SchemaType type,
                     int32_t pg_type,
                     bool nullable,
                     std::optional<uint32_t> pkey_position=std::optional<uint32_t>(),
                     std::optional<std::string> default_value=std::optional<std::string>())
            : name(name),
              position(position),
              type(type),
              pg_type(pg_type),
              nullable(nullable),
              pkey_position(pkey_position),
              default_value(default_value)
        { }

        /**
         * Default copy constructor.
         */
        SchemaColumn(const SchemaColumn &column) = default;
    };

    /**
     * Object representing the table index.
     */
    struct Index {
        struct Column {
            uint32_t idx_position;
            uint32_t position;
        };
        uint64_t id;
        std::string schema;
        std::string name;
        uint64_t table_id;
        bool is_unique;
        uint8_t state;
        std::vector<Column> columns;
    };

    /**
     * Object representing the metadata of a schema.
     */
    struct SchemaMetadata {
        std::vector<SchemaColumn> columns;
        std::vector<SchemaColumn> history;
        std::vector<Index> indexes;
    };

    /**
     * The interface for schema objects.
     */
    class Schema {
    public:
        virtual ~Schema() = default;

        /** Checks if a given column exists. */
        virtual bool has_field(const std::string &name) const = 0;

        /** Returns a field accessor for the requested column. */
        virtual std::shared_ptr<Field> get_field(const std::string &name) const = 0;

        /** Returns a Tuple representing all of the columns of the schema. */
        virtual std::shared_ptr<std::vector<FieldPtr>> get_fields() const = 0;

        /** Returns a Tuple representing an ordered subset of the columns in the schema. */
        virtual std::shared_ptr<std::vector<FieldPtr>> get_fields(const std::vector<std::string> &columns) const = 0;
    };
    typedef std::shared_ptr<Schema> SchemaPtr;

    /**
     * Defines the schema for a physical extent in a table.  Creates mutable field accessors to
     * retrieve data from the extent at a specified target XID.
     */
    class ExtentSchema : public Schema {
    private:
        /** The width of fixed data for a single row. */
        uint32_t _row_size;

        /** Map of column names to <field, order> pairs. */
        std::map<std::string, std::pair<MutableFieldPtr, int>> _field_map;

        /** The order of the columns. */
        std::vector<std::string> _column_order;

        /** The set of SQL types in column order. */
        std::vector<int32_t> _pg_types;

        /** The sort columns of the schema. */
        std::shared_ptr<std::vector<FieldPtr>> _sort_fields;

        /** The sort column names of the schema. */
        std::vector<std::string> _sort_keys;

    protected:
        /**
         * Construct the set of column fields based on the column definitions.
         * @param columns A map from column position to column definition.
         */
        void _populate(const std::map<uint32_t, SchemaColumn>& columns);

    public:
        /**
         * Constructor.
         * @param columns Map from column position to the SchemaColumn definition.
         */
        explicit ExtentSchema(const std::vector<SchemaColumn> &columns) {
            std::map<uint32_t, SchemaColumn> column_map;
            for (auto &&column : columns) {
                column_map.insert({column.position, column});
            }

            // populate the field map using the column definitions
            _populate(column_map);
        }

        /**
         * Constructor.
         * @param columns Map from column position to the SchemaColumn definition.
         */
        explicit ExtentSchema(const std::map<uint32_t, SchemaColumn> columns)
        {
            _populate(columns);
        }

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
        get_mutable_field(const std::string &name) const
        {
            auto &&i = _field_map.find(name);
            if (i == _field_map.end()) {
                throw SchemaError(fmt::format("No such column: {}", name));
            }
            return i->second.first;
        }

        /**
         * Retrieve a non-mutable field bound to a given column for reading from extents.
         * @param name The name of the column to retrieve the field for.
         */
        std::shared_ptr<Field> get_field(const std::string &name) const override;

        /**
         * Check for the existence of a field.
         * @param name The name of the column to check existence.
         */
        bool has_field(const std::string &name) const override
        {
            auto &&i = _field_map.find(name);
            return (i != _field_map.end());
        }

        /**
         * Generate a new ExtentSchema, based on a list of columns from this schema, as well as
         * additional provided columns.  Used in the creation of schemas for BTree indexes.
         */
        std::shared_ptr<ExtentSchema>
        create_schema(const std::vector<std::string> &old_columns,
                      const std::vector<SchemaColumn> &new_columns,
                      const std::vector<std::string> &sort_columns) const;

        /**
         * Retrieve the list of column types.
         */
        std::vector<int32_t> get_types() const {
            return _pg_types;
        }

        /**
         * Generate a list of all of the fields in the schema.
         */
        std::shared_ptr<std::vector<FieldPtr>> get_fields() const override;

        /**
         * Generate a list of all of the fields in the schema.
         */
        std::shared_ptr<std::vector<MutableFieldPtr>> get_mutable_fields() const;

        /**
         * Return a list of fields that form the sort columns of this schema.
         */
        std::shared_ptr<std::vector<FieldPtr>> get_sort_fields() const {
            return _sort_fields;
        }

        /**
         * Return a list of field names that form the sort columns of this schema.
         */
        const std::vector<std::string> &get_sort_keys() const {
            return _sort_keys;
        }

        /**
         * Generate a list of fields based on an ordered list of columns.
         */
        std::shared_ptr<std::vector<FieldPtr>> get_fields(const std::vector<std::string> &columns) const override;

        /**
         * Generate a list of fields based on an ordered list of columns.
         */
        std::shared_ptr<std::vector<MutableFieldPtr>> get_mutable_fields(const std::vector<std::string> &columns) const;

        /**
         * Generate a subset of the provided Tuple that contains only the requested columns.  The
         * assumption is that the provided Tuple matches the full set of schema columns.
         */
        std::shared_ptr<Tuple> tuple_subset(std::shared_ptr<Tuple> tuple,
                                            const std::vector<std::string> &columns) const;
        std::shared_ptr<std::vector<std::shared_ptr<Field>>>
        fieldarray_subset(std::shared_ptr<Tuple> tuple,
                          const std::vector<std::string> &columns) const;

        /**
         * Return the order of the columns in the schema.
         */
        std::vector<std::string>
        column_order() const
        {
            return _column_order;
        }
    };
    typedef std::shared_ptr<ExtentSchema> ExtentSchemaPtr;

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
        ExtentSchemaPtr _extent_schema; ///< The underlying schema of the on-disk data.

        /** Column order of the virtual schema. */
        std::vector<std::string> _column_order;

    public:
        /**
         * Constructor to populate the field map for this schema based on the provided base schema and schema updates.
         * @param extent_schema The schema for the underlying extent data.
         * @param columns The column definitions for the underlying extent data.
         * @param updates The updates to apply to the extent schema to generate the virtual schema.
         */
        VirtualSchema(const SchemaMetadata &meta);

        /**
         * Checks if the column exists within the virtual schema.
         * @param name The name of the column being requested.
         */
        bool has_field(const std::string &name) const override
        {
            return (_field_map.find(name) != _field_map.end());
        }

        /**
         * Returns a field accessor for the column within the virtual schema.
         * @param name The name of the column being requested.
         */
        std::shared_ptr<Field> get_field(const std::string &name) const override
        {
            auto &&i = _field_map.find(name);
            if (i == _field_map.end()) {
                throw SchemaError("Could not find requested column");
            }

            return i->second;
        }

        /**
         * Return a ConstFieldTuple containing all of the columns in this schema.
         */
        std::shared_ptr<std::vector<FieldPtr>> get_fields() const override;

        /**
         * Return a FieldTuple containing the specified columns from this schema.
         * @param columns A list of requested columns for the returned ConstFieldTuple
         */
        std::shared_ptr<std::vector<FieldPtr>> get_fields(const std::vector<std::string> &columns) const override;

        /** Returns the fixed width for a single row of the underlying ExtentSchema. */
        uint32_t row_size() const {
            return _extent_schema->row_size();
        }

    };
    typedef std::shared_ptr<VirtualSchema> VirtualSchemaPtr;

}
