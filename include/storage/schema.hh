#pragma once

#include <map>
#include <string>
#include <vector>

#include "schema_column.hh"

namespace springtail {
    // pre-declare classes
    class Field;

    /** Describes a table schema. */
    class Schema {
    private:
        /** The width of fixed data for a single row. */
        uint32_t _row_size;

        /** The starting position of boolean bits. */
        uint32_t _bool_bit_pos;

        /** The starting position of null bits. */
        uint32_t _null_bit_pos;
        
        /** The schema itself is an array of columns */
        std::vector<std::shared_ptr<SchemaColumn>> _columns;

        /** A map of names to column indexes */
        std::unordered_map<std::string, uint32_t> _name_map;

        /** The ID of the schema. */
        uint64_t _id;

    protected:
        /** Create a column object based on the provided column details. */
        std::shared_ptr<SchemaColumn> _create_column(const SchemaColumnDetails &details);

        /** Update the positions of the columns within the row. */
        void _update_positions();

    public:
        /** Constructor. */
        Schema(uint64_t id);

        /** Copy constructor. */
        Schema(const Schema &schema);

        /** Returns the fixed width for a single row. */
        uint32_t row_size() const {
            return _row_size;
        }

        /** Returns the ID of the schema. */
        uint64_t id() const {
            return _id;
        }

        /** Adds a column to the schema.
         *
         *  @param name The name of the column.
         *  @param details The details of the column (type, nullable, default value, etc.).
         */
        void add_column(const std::string &name,
                        const SchemaColumnDetails &details);

        /** Removes a column from the schema.
         * 
         *  @param name The column to remove.
         */
        void remove_column(const std::string &name);

        /** Update the details of a column.
         *
         *  @param from The column to alter.
         *  @param to The new name of the column (may be the same if unchanged).
         *  @param details The updated column details.
         */
        void alter_column(const std::string &from,
                          const std::string &to,
                          const SchemaColumnDetails &details);

        /** Retrieve a field bound to a given column for reading data from extents.
         *
         *  @param column_index The column to retrieve the field for.
         *  @return A pointer to the field accessor.
         */
        std::shared_ptr<Field> get_field(const std::string &name);
    };
}
