#pragma once

#include <map>
#include <string>
#include <vector>
#include <nlohmann/json.hpp>

#include <common/object_cache.hh>

#include <storage/exception.hh>
#include <storage/schema_type.hh>
#include <storage/xid.hh>

namespace springtail {
    // pre-declare field classes
    class Field;
    using FieldPtr = std::shared_ptr<Field>;

    class MutableField;
    using MutableFieldPtr = std::shared_ptr<MutableField>;

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
        int32_t position;  ///< position and postgres column ID, can have holes
        SchemaType type;
        int32_t pg_type;
        bool exists;
        bool nullable;
        bool is_generated;
        bool is_non_standard_collation;
        bool is_user_defined_type;
        char type_category;  ///< type category of 'E' represents enum
        std::string type_name;
        std::string type_namespace;
        std::string collation;
        std::optional<uint32_t> pkey_position; ///< position in primary key, if any, 0 based no holes
        std::optional<std::string> default_value;
        SchemaUpdateType update_type = SchemaUpdateType::NEW_COLUMN;

        SchemaColumn() = default;

        SchemaColumn(uint64_t xid,
                     uint64_t lsn,
                     const std::string_view &name,
                     int32_t position,
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
                     int32_t position,
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
        XidRange access_range;
        XidRange target_range;
        std::vector<SchemaColumn> columns;
        std::vector<SchemaColumn> history;
        std::vector<Index> indexes;
    };
    using SchemaMetadataPtr = std::shared_ptr<SchemaMetadata>;

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

        /** Returns the sort keys of the schema -- represents the primary key.  Empty if none. */
        virtual const std::vector<std::string> &get_sort_keys() const = 0;

        /** Returns the columns in their order within the schema. */
        virtual std::vector<std::string> column_order() const = 0;

        /**
         * This will convert column positions to column names based on the table schema
         */
        std::vector<std::string>
        get_column_names(const std::vector<uint32_t>& col_position) const
        {
            std::vector<std::string> col_names;
            auto co = column_order(); // calls either ExtentSchema or VirtualSchema depending on object
            for (auto position: col_position) {
                // the index positions start with one
                assert(position <= co.size());
                col_names.emplace_back(std::move(co[position-1]));
            }
            return col_names;
        }
    };
    using SchemaPtr = std::shared_ptr<Schema>;

    /**
     * Defines the schema for a physical extent in a table or table update.  Creates mutable field
     * accessors to store and retrieve column data from an extent row.
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

        /** The set of Springtail types in column order. */
        std::vector<uint8_t> _field_types;

        /** The sort columns of the schema. */
        std::shared_ptr<std::vector<FieldPtr>> _sort_fields;

        /** The sort column names of the schema. */
        std::vector<std::string> _sort_keys;

    protected:
        /**
         * Construct the set of column fields based on the column definitions.
         * @param columns A map from column position to column definition.
         */
        void _populate(const std::map<uint32_t, SchemaColumn>& columns, const ComparatorCallback comparator_callback = {}, bool allow_undefined = false);

    public:
        /**
         * Constructor.
         * @param columns Map from column position to the SchemaColumn definition.
         */
        explicit ExtentSchema(const std::vector<SchemaColumn> &columns, const ComparatorCallback comparator_callback = {}, bool allow_undefined = false) {
            std::map<uint32_t, SchemaColumn> column_map;
            for (auto &&column : columns) {
                column_map.insert({column.position, column});
            }

            // populate the field map using the column definitions
            _populate(column_map, comparator_callback, allow_undefined);
        }

        /**
         * Constructor.
         * @param columns Map from column position to the SchemaColumn definition.
         */
        explicit ExtentSchema(const std::map<uint32_t, SchemaColumn> columns, const ComparatorCallback comparator_callback = {}, bool allow_undefined = false)
        {
            _populate(columns, comparator_callback, allow_undefined);
        }

        /** Returns the fixed width for a single row. */
        uint32_t row_size() const {
            return _row_size;
        }

        /** Returns a vector with the column types. */
        const std::vector<uint8_t> &field_types() const {
            return _field_types;
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
         * additional provided columns.  Used in the creation of schemas for BTree indexes and to
         * populate the WriteCache.
         */
        std::shared_ptr<ExtentSchema>
        create_schema(const std::vector<std::string> &old_columns,
                      const std::vector<SchemaColumn> &new_columns,
                      const std::vector<std::string> &sort_columns,
                      const ComparatorCallback comparator_callback = {},
                      bool allow_undefined = false) const;

        /**
         * Retrieve the list of column pg types.
         * @return std::vector<int32_t, int>
         */
        std::vector<std::pair<int32_t, int>> get_pg_types() const {
            std::vector<std::pair<int32_t, int>> pg_types;
            for (size_t i = 0; i < _pg_types.size(); i++) {
                auto field = _field_map.at(_column_order[i]);
                pg_types.push_back({_pg_types[i], field.second});
            }
            return pg_types;
        }

        /**
         * Retrieve sort key pg types.
         * @return std::vector<std::pair<int32_t, int>> pair of pg_type and column position (0 based)
         */
        std::vector<std::pair<int32_t, int>> get_sort_key_pg_types() const {
            std::vector<std::pair<int32_t, int>> sort_key_pg_types;
            for (auto &&key : _sort_keys) {
                auto field = _field_map.at(key);
                auto pg_type = get_type(key);
                sort_key_pg_types.push_back({pg_type, field.second});
            }
            return sort_key_pg_types;
        }

        /**
         * @brief Get the pg type of a column by name
         * @param name The name of the column being requested
         * @return int32_t pg_type
         */
        int32_t get_type(const std::string &name) const {
            auto &&i = _field_map.find(name);
            if (i == _field_map.end()) {
                throw SchemaError(fmt::format("No such column: {}", name));
            }
            return _pg_types[i->second.second];
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
        const std::vector<std::string> &get_sort_keys() const override {
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
        std::vector<std::string> column_order() const override {
            return _column_order;
        }
    };
    using ExtentSchemaPtr = std::shared_ptr<ExtentSchema>;

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
        explicit VirtualSchema(const SchemaMetadata &meta, const ComparatorCallback comparator_callback = {});

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

        /**
         * Returns the sort keys of the underlying ExtentSchema, since they must match -- a change
         * in the sort order would result in a table resync and a VirtualSchema could not be
         * constructed.
         */
        const std::vector<std::string> &get_sort_keys() const override {
            return _extent_schema->get_sort_keys();
        }

        /** Returns the fixed width for a single row of the underlying ExtentSchema. */
        uint32_t row_size() const {
            return _extent_schema->row_size();
        }

        /**
         * Return the order of the columns in the schema.
         */
        std::vector<std::string> column_order() const override {
            return _column_order;
        }
    };
    using VirtualSchemaPtr = std::shared_ptr<VirtualSchema>;

    /** UserType class for holding user defined types */
    struct UserType {
        uint64_t id;                // pg type oid
        uint64_t namespace_id;      // pg namespace oid
        std::string name;           // pg type name
        nlohmann::json value_json;  // json representation of the user type value;
        bool exists;

        // for enum, it is a json array of objects with label: index pairs
        std::unordered_map<std::string, float> enum_label_map;
        std::unordered_map<float, std::string> enum_index_map;

        /** Enum type for user defined types */
        enum Type : int8_t {
            ENUM = 'E',
            EXTENSION = 'U'
        } type;

        UserType(uint64_t id, bool exists=false, int8_t type=constant::USER_TYPE_ENUM)
            : id(id),
            exists(exists),
            type(static_cast<Type>(type))
        {
            DCHECK(type == constant::USER_TYPE_ENUM); // only support enum for now
        }

        UserType(uint64_t id, uint64_t namespace_id, int8_t type, const std::string &name, const std::string &value, bool exists=true)
            : id(id),
            namespace_id(namespace_id),
            name(name),
            value_json(nlohmann::json::parse(value)),
            exists(exists),
            type(static_cast<Type>(type))
        {
            DCHECK(type == constant::USER_TYPE_ENUM || type == constant::USER_TYPE_EXTENSION);
            if (type == constant::USER_TYPE_ENUM) {
                DCHECK(value_json.is_array());
                for (const auto &obj : value_json) {
                    DCHECK(obj.is_object());
                    auto it = obj.begin();  // Only one key-value pair per object
                    float idx = it.value().get<float>();
                    enum_label_map[it.key()] = idx;
                    enum_index_map[idx] = it.key();
                }
            }
        }
    };
    using UserTypePtr = std::shared_ptr<UserType>;

}
