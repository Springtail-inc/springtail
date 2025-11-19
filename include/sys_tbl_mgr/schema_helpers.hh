#pragma once

#include <memory>
#include <vector>

#include <storage/schema.hh>

namespace springtail {

    /**
     * Helper functions for creating commonly-used ExtentSchema objects.
     * These functions are used by both client-side (Table) and server-side (TableMgr) code.
     */
    namespace schema_helpers {

        /**
         * Get the singleton roots schema.
         * This schema is used for storing table roots metadata and is the same for all tables.
         * @return Shared pointer to the roots schema.
         */
        ExtentSchemaPtr get_roots_schema();

        /**
         * Get the singleton look-aside schema.
         * This schema maps __internal_row_id to (__extent_id, __row_id) and is the same for all tables.
         * @return Shared pointer to the look-aside schema.
         */
        ExtentSchemaPtr get_look_aside_schema();

        /**
         * Create an index schema for a secondary index.
         * The index schema includes the indexed columns plus __internal_row_id.
         * @param base_schema The base table schema.
         * @param index_columns The column positions to include in the index.
         * @param index_id The index ID (for logging/debugging).
         * @param extension_callback Extension callback for custom types.
         * @return A new index schema.
         */
        ExtentSchemaPtr create_index_schema(
            ExtentSchemaPtr base_schema,
            const std::vector<uint32_t>& index_columns,
            uint64_t index_id,
            const ExtensionCallback& extension_callback = {});

        /**
         * Create an index schema for a GIN secondary index.
         * The index schema includes columns - column position, token column and __internal_row_id
         * @param base_schema The base table schema.
         * @param extension_callback Extension callback for custom types.
         * @return A new GIN index schema.
         */
        ExtentSchemaPtr create_gin_index_schema(
            ExtentSchemaPtr base_schema,
            const ExtensionCallback& extension_callback = {});

        /**
         * Create a PgLogReader batch schema from a base table schema.
         * The batch schema includes all table columns plus __springtail_op and __springtail_lsn fields.
         * @param base_schema The base table schema.
         * @param extension_callback Extension callback for custom types.
         * @return A new batch schema.
         */
        ExtentSchemaPtr create_pg_log_batch_schema(
            ExtentSchemaPtr base_schema,
            const ExtensionCallback& extension_callback = {});

    } // namespace schema_helpers

} // namespace springtail
