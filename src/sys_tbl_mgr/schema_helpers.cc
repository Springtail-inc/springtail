#include <common/constants.hh>
#include <sys_tbl_mgr/schema_helpers.hh>

namespace springtail::schema_helpers {

    ExtentSchemaPtr get_roots_schema()
    {
        // Singleton schema - created on demand, never destroyed during normal shutdown
        // to avoid static destruction order issues
        static ExtentSchemaPtr roots_schema = std::make_shared<ExtentSchema>(
            std::vector<SchemaColumn>{
                { "root", 1, SchemaType::UINT64, 20, true },
                { "index_id", 2, SchemaType::UINT64, 20, false },
                { "last_internal_row_id", 3, SchemaType::UINT64, 20, false }
            },
            ExtensionCallback{}, false, false
        );
        return roots_schema;
    }

    ExtentSchemaPtr get_look_aside_schema()
    {
        // Singleton schema - created on demand, never destroyed during normal shutdown
        // to avoid static destruction order issues
        static ExtentSchemaPtr look_aside_schema = std::make_shared<ExtentSchema>(
            std::vector<SchemaColumn>{
                SchemaColumn(constant::INTERNAL_ROW_ID, 1, SchemaType::UINT64, 0, false, 0),
                SchemaColumn(constant::INDEX_EID_FIELD, 2, SchemaType::UINT64, 0, false),
                SchemaColumn(constant::INDEX_RID_FIELD, 3, SchemaType::UINT32, 0, false)
            },
            ExtensionCallback{}, false, false
        );
        return look_aside_schema;
    }

    ExtentSchemaPtr create_gin_index_schema(
        ExtentSchemaPtr base_schema,
        const ExtensionCallback& extension_callback)
    {
        SchemaColumn idx_position_c(constant::INDEX_POSITION_FIELD, 0, SchemaType::UINT32, 0, false);
        SchemaColumn idx_gin_token_c(constant::INDEX_GIN_TOKEN_FIELD, 0, SchemaType::TEXT, 0, false);
        SchemaColumn internal_row_id(constant::INTERNAL_ROW_ID, 0, SchemaType::UINT64, 0, false);

        std::vector<std::string> gin_index_keys;
        gin_index_keys.push_back(constant::INDEX_POSITION_FIELD);
        gin_index_keys.push_back(constant::INDEX_GIN_TOKEN_FIELD);
        gin_index_keys.push_back(constant::INTERNAL_ROW_ID);

        return base_schema->create_index_schema({},
                { idx_position_c, idx_gin_token_c, internal_row_id },
                gin_index_keys, extension_callback);
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
