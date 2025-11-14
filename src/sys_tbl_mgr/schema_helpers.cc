#include <common/constants.hh>
#include <sys_tbl_mgr/schema_helpers.hh>

namespace springtail::schema_helpers {

    ExtentSchemaPtr get_roots_schema()
    {
        // Static singleton - initialized once
        static ExtentSchemaPtr roots_schema = []() {
            std::vector<SchemaColumn> columns = {
                { "root", 1, SchemaType::UINT64, 20, true },
                { "index_id", 2, SchemaType::UINT64, 20, false },
                { "last_internal_row_id", 3, SchemaType::UINT64, 20, false }
            };
            return std::make_shared<ExtentSchema>(columns, ExtensionCallback{}, false, false);
        }();
        return roots_schema;
    }

    ExtentSchemaPtr get_look_aside_schema()
    {
        // Static singleton - initialized once
        static ExtentSchemaPtr look_aside_schema = []() {
            SchemaColumn internal_row_id(constant::INTERNAL_ROW_ID, 1, SchemaType::UINT64, 0, false, 0);
            SchemaColumn extent_c(constant::INDEX_EID_FIELD, 2, SchemaType::UINT64, 0, false);
            SchemaColumn row_c(constant::INDEX_RID_FIELD, 3, SchemaType::UINT32, 0, false);
            std::vector<SchemaColumn> columns = { internal_row_id, extent_c, row_c };
            return std::make_shared<ExtentSchema>(columns, ExtensionCallback{}, false, false);
        }();
        return look_aside_schema;
    }

    ExtentSchemaPtr create_index_schema(
        ExtentSchemaPtr base_schema,
        const std::vector<uint32_t>& index_columns,
        uint64_t index_id,
        const ExtensionCallback& extension_callback)
    {
        // Get the column names in the order they appear in the index
        auto col_names = base_schema->get_column_names(index_columns);

        // Add the index-specific field (only __internal_row_id for secondary indexes)
        SchemaColumn internal_row_id(constant::INTERNAL_ROW_ID, 0, SchemaType::UINT64, 0, false);

        auto key_columns = col_names;
        key_columns.push_back(constant::INTERNAL_ROW_ID);

        // Create the index schema using create_index_schema()
        return base_schema->create_index_schema(col_names, { internal_row_id }, key_columns, extension_callback);
    }

    ExtentSchemaPtr create_pg_log_batch_schema(
        ExtentSchemaPtr base_schema,
        const ExtensionCallback& extension_callback)
    {
        // Get all columns from the table
        auto columns = base_schema->column_order();

        // Get the sort keys and add __springtail_lsn
        auto sort_keys = base_schema->get_sort_keys();
        sort_keys.push_back("__springtail_lsn");

        // Add the PgLogReader-specific fields
        SchemaColumn op("__springtail_op", 0, SchemaType::UINT8, 0, false);
        SchemaColumn lsn("__springtail_lsn", 0, SchemaType::UINT64, 0, false);

        std::vector<SchemaColumn> new_columns{op, lsn};

        // Create the batch schema
        return base_schema->create_schema(columns, new_columns, sort_keys, extension_callback, true);
    }

} // namespace springtail::schema_helpers
