#include <fmt/ranges.h>

#include <common/init.hh>
#include <common/constants.hh>
#include <common/json.hh>

#include <storage/field.hh>
#include <storage/io_mgr.hh>
#include <sys_tbl_mgr/system_tables.hh>
#include <sys_tbl_mgr/schema_mgr.hh>
#include <sys_tbl_mgr/table.hh>

using namespace springtail;

// get base path to database files
std::filesystem::path table_base;

template<typename Tbl>
std::pair<TablePtr, std::shared_ptr<std::vector<FieldPtr>>>
get_table_and_fields(uint64_t db_id)
{
    auto schema = std::make_shared<ExtentSchema>(Tbl::Data::SCHEMA);
    std::vector<Index> secondary_keys;
    TableMetadata tbl_meta;
    tbl_meta.snapshot_xid = 1;

    uint64_t xid = constant::LATEST_XID;

    XidLsn access_xid(xid);
    TablePtr table = std::make_shared<Table>(db_id, Tbl::ID, xid, table_base, Tbl::Primary::KEY, secondary_keys, tbl_meta, schema);
    std::shared_ptr<std::vector<FieldPtr>> fields = schema->get_fields();

    return std::make_pair(table, fields);
}

std::vector<SchemaColumn>
read_schema_columns(uint64_t db_id, uint64_t table_id, XidLsn &access_start, XidLsn &access_end)
{
    std::vector<SchemaColumn> columns;

    // read schema table
    auto [schema_table, schema_table_fields] = get_table_and_fields<sys_tbl::Schemas>(db_id);
    // read everything with the given table_id
    auto search_key = sys_tbl::Schemas::Primary::key_tuple(table_id, 0, 0, 0);
    auto schema_table_iter = schema_table->lower_bound(search_key);
    for (; schema_table_iter != schema_table->end(); ++schema_table_iter) {
        auto &schema_table_row = *schema_table_iter;

        uint64_t schema_table_id = schema_table_fields->at(sys_tbl::Schemas::Data::TABLE_ID)->get_uint64(&schema_table_row);
        if (schema_table_id != table_id) {
            break;
        }
        uint32_t position = schema_table_fields->at(sys_tbl::Schemas::Data::POSITION)->get_uint32(&schema_table_row);
        uint64_t xid = schema_table_fields->at(sys_tbl::Schemas::Data::XID)->get_uint64(&schema_table_row);
        uint64_t lsn = schema_table_fields->at(sys_tbl::Schemas::Data::LSN)->get_uint64(&schema_table_row);
        const XidLsn row_xid(xid, lsn);
        if (access_start < row_xid) {
            access_start = row_xid;
        }

        bool exists = schema_table_fields->at(sys_tbl::Schemas::Data::EXISTS)->get_bool(&schema_table_row);
        if (!exists) {
            // Remove any existing column at this position
            for (int i = 0; i < columns.size(); i++) {
                if (columns[i].position == position) {
                    columns.erase(columns.begin() + i);
                    break;
                }
            }
            continue;
        }
        std::string column_name(schema_table_fields->at(sys_tbl::Schemas::Data::NAME)->get_text(&schema_table_row));
        uint8_t type = schema_table_fields->at(sys_tbl::Schemas::Data::TYPE)->get_uint8(&schema_table_row);
        int32_t pg_type = schema_table_fields->at(sys_tbl::Schemas::Data::PG_TYPE)->get_int32(&schema_table_row);
        bool nullable = schema_table_fields->at(sys_tbl::Schemas::Data::NULLABLE)->get_bool(&schema_table_row);
        std::optional<std::string> default_value;
        if (!schema_table_fields->at(sys_tbl::Schemas::Data::DEFAULT)->is_null(&schema_table_row)) {
            default_value = schema_table_fields->at(sys_tbl::Schemas::Data::DEFAULT)->get_text(&schema_table_row);
        }

        std::optional<uint32_t> pk_position;
        SchemaColumn column(xid, lsn, column_name, position, static_cast<SchemaType>(type), pg_type, exists, nullable, pk_position, default_value);

        // Find position where column should be (either existing or new)
        int idx = 0;
        for (; idx < columns.size(); idx++) {
            if (columns[idx].position >= position) {
                break;
            }
        }

        if (idx < columns.size() && columns[idx].position == position) {
            // Update existing column at this position
            columns[idx] = column;
        } else {
            // new column, put it in order
            columns.insert(columns.begin() + idx, column);
        }
    }

    if (columns.empty()) {
        return columns;
    }

    // read indexes table
    auto [indexes_table, indexes_table_fields] = get_table_and_fields<sys_tbl::Indexes>(db_id);

    // read everything with the given table_id
    auto indexes_search_key = sys_tbl::Indexes::Primary::key_tuple(table_id, constant::INDEX_PRIMARY, access_end.xid, access_end.lsn, 0);
    auto indexes_table_iter = indexes_table->inverse_lower_bound(indexes_search_key);

    if (indexes_table_iter == indexes_table->end()) {
        return columns;
    }

    // determine the XID we found and only read those entries
    auto index_table_row = *indexes_table_iter;
    uint64_t index_xid = indexes_table_fields->at(sys_tbl::Indexes::Data::XID)->get_uint64(&index_table_row);
    uint64_t index_lsn = indexes_table_fields->at(sys_tbl::Indexes::Data::LSN)->get_uint64(&index_table_row);
    XidLsn index_xid_lsn(index_xid, index_lsn);

    bool done = false;
    while (!done) {
        index_table_row = *indexes_table_iter;

        // ensure we are reading data for the requested table
        uint64_t tid = indexes_table_fields->at(sys_tbl::Indexes::Data::TABLE_ID)->get_uint64(&index_table_row);
        if (tid != table_id) {
            // if we have read all of the entries for this table ID, stop processing
            break;
        }

        uint64_t xid = indexes_table_fields->at(sys_tbl::Indexes::Data::XID)->get_uint64(&index_table_row);
        uint64_t lsn = indexes_table_fields->at(sys_tbl::Indexes::Data::LSN)->get_uint64(&index_table_row);

        // ensure we are still reading the correct XID/LSN
        if (index_xid_lsn != XidLsn(xid, lsn)) {
            break;
        }

        // update the primary key details in the schema columns
        uint32_t column_id = indexes_table_fields->at(sys_tbl::Indexes::Data::COLUMN_ID)->get_uint32(&index_table_row);
        uint32_t index_pos = indexes_table_fields->at(sys_tbl::Indexes::Data::POSITION)->get_uint32(&index_table_row);
        bool found = false;
        for (auto& column : columns) {
            if (column.position == column_id) {
                column.pkey_position = index_pos;
                found = true;
                break;
            }
        }
        CHECK(found) << "Failed to find matching column for primary key";

        done = (index_pos == 0);
        if (!done) {
            --indexes_table_iter;
        }
    }

    return columns;
}

std::shared_ptr<const SchemaMetadata>
get_schema(uint64_t db_id, uint64_t table_id, const XidLsn &xid_lsn)
{
    XidLsn access_start(0, 0);
    XidLsn access_end(constant::LATEST_XID, constant::MAX_LSN);
    XidLsn target_start(0, 0);
    XidLsn target_end(constant::LATEST_XID, constant::MAX_LSN);

    std::vector<SchemaColumn> columns = read_schema_columns(db_id, table_id, access_start, access_end);

    auto metadata = std::make_shared<SchemaMetadata>();

    metadata->access_range = XidRange(access_start, access_end);
    metadata->target_range = XidRange(target_start, target_end);
    metadata->columns = columns;
    metadata->indexes = std::vector<Index>();
    return metadata;
}

std::shared_ptr<ExtentSchema>
get_extent_schema(uint64_t db_id, uint64_t table_id, const XidLsn &xid)
{
    auto &&meta = get_schema(db_id, table_id, xid);

    // construct the schema from the provided schema metadata
    return std::make_shared<ExtentSchema>(meta->columns);
}

TableMetadataPtr
get_roots(uint64_t db_id, uint64_t table_id, uint64_t xid)
{
    std::vector<TableRoot> roots;

    // read table roots table
    auto [table_roots, table_roots_fields] = get_table_and_fields<sys_tbl::TableRoots>(db_id);

    auto table_roots_search_key = sys_tbl::TableRoots::Primary::key_tuple(table_id, constant::INDEX_PRIMARY, xid);

    // not found in cached roots, go to the table
    auto search_key = sys_tbl::TableRoots::Primary::key_tuple(table_id, constant::INDEX_PRIMARY, xid);
    auto table_roots_iter = table_roots->inverse_lower_bound(table_roots_search_key);
    CHECK(table_roots_iter != table_roots->end());
    auto &table_roots_row = *table_roots_iter;
    uint64_t roots_tid = table_roots_fields->at(sys_tbl::TableRoots::Data::TABLE_ID)->get_uint64(&table_roots_row);
    uint64_t roots_iid = table_roots_fields->at(sys_tbl::TableRoots::Data::INDEX_ID)->get_uint64(&table_roots_row);
    uint64_t roots_xid = table_roots_fields->at(sys_tbl::TableRoots::Data::XID)->get_uint64(&table_roots_row);
    uint64_t roots_eid = table_roots_fields->at(sys_tbl::TableRoots::Data::EXTENT_ID)->get_uint64(&table_roots_row);
    uint64_t roots_sxid = table_roots_fields->at(sys_tbl::TableRoots::Data::SNAPSHOT_XID)->get_uint64(&table_roots_row);
    CHECK(roots_tid == table_id);
    CHECK(roots_iid == constant::INDEX_PRIMARY);
    CHECK(roots_xid <= xid);

    roots.push_back({constant::INDEX_PRIMARY, roots_eid});

    // access the stats table
    auto [table_stats, table_stats_fields] = get_table_and_fields<sys_tbl::TableStats>(db_id);

    auto table_stats_search_key = sys_tbl::TableStats::Primary::key_tuple(table_id, constant::LATEST_XID);
    auto table_stats_iter = table_stats->inverse_lower_bound(table_stats_search_key);
    CHECK(table_stats_iter != table_stats->end());
    auto &table_stats_row = *table_stats_iter;
    uint64_t stats_tid = table_stats_fields->at(sys_tbl::TableStats::Data::TABLE_ID)->get_uint64(&table_stats_row);
    uint64_t stats_xid = table_stats_fields->at(sys_tbl::TableStats::Data::XID)->get_uint64(&table_stats_row);
    uint64_t row_count = table_stats_fields->at(sys_tbl::TableStats::Data::ROW_COUNT)->get_uint64(&table_stats_row);
    uint64_t end_offset = table_stats_fields->at(sys_tbl::TableStats::Data::END_OFFSET)->get_uint64(&table_stats_row);
    CHECK(stats_tid == table_id);
    CHECK(stats_xid <= xid);

    auto metadata = std::make_shared<TableMetadata>();
    metadata->roots = roots;

    metadata->stats.row_count = row_count;
    metadata->stats.end_offset = end_offset;
    metadata->snapshot_xid = roots_sxid;

    return metadata;
}

TablePtr
get_table(const std::filesystem::path &table_base, uint64_t db_id, uint64_t table_id, uint64_t xid)
{
    // retrieve the roots and stats of the table
    auto &&tbl_meta = get_roots(db_id, table_id, xid);
    auto &&schema_meta = get_schema(db_id, table_id, {xid, constant::MAX_LSN});

    ExtentSchemaPtr schema = std::make_shared<ExtentSchema>(schema_meta->columns);

    auto filtered = std::views::filter(schema_meta->indexes, [](auto const& v) { return v.id != constant::INDEX_PRIMARY; });
    std::vector<Index> secondary_indexes(filtered.begin(), filtered.end());

    // construct the table and return it
    return std::make_shared<Table>(db_id, table_id, xid, table_base,
                                    schema->get_sort_keys(), secondary_indexes,
                                    *tbl_meta, schema);
}

int
main(int argc, char *argv[])
{
    // no logging
    springtail_init(false, std::nullopt, LOG_NONE);

    // get all database ids
    std::map<uint64_t, std::string> databases = Properties::get_databases();

    // get base path to database files
    nlohmann::json json = Properties::get(Properties::STORAGE_CONFIG);
    Json::get_to<std::filesystem::path>(json, "table_dir", table_base);
    table_base = Properties::make_absolute_path(table_base);
    LOG_INFO("Verifying tables at table_base = {}", table_base.string());

    // iterate over databases
    for (const auto &db_id_name: databases) {
        uint64_t db_id = db_id_name.first;
        const std::string &db_name = db_id_name.second;

        // read table names table
        auto [table, table_fields] = get_table_and_fields<sys_tbl::TableNames>(db_id);

        for (auto row: (*table)) {
            auto table_ns_id = table_fields->at(sys_tbl::TableNames::Data::NAMESPACE_ID)->get_uint64(&row);
            std::string table_name(table_fields->at(sys_tbl::TableNames::Data::NAME)->get_text(&row));
            uint64_t tid = table_fields->at(sys_tbl::TableNames::Data::TABLE_ID)->get_uint64(&row);
            uint64_t xid = table_fields->at(sys_tbl::TableNames::Data::XID)->get_uint64(&row);
            bool exists = table_fields->at(sys_tbl::TableNames::Data::EXISTS)->get_bool(&row);
            if (!exists) {
                LOG_INFO("DB {}:{}: Skipping non-existing table {}.{} tid={}, xid={}", db_id, db_name, table_ns_id, table_name, tid, xid);
                continue;
            }
            LOG_INFO("DB {}:{}: Found table {}.{} tid={}, xid={}", db_id, db_name, table_ns_id, table_name, tid, xid);

            TablePtr table = get_table(table_base, db_id, tid, constant::LATEST_XID);
            CHECK(table.get() != nullptr);

            LOG_INFO("\tTable dir: {}", table->get_dir_path().c_str());

            ExtentSchemaPtr table_schema = get_extent_schema(db_id, tid, XidLsn(table->get_xid()));;
            FieldArrayPtr table_key_fields = table_schema->get_sort_fields();

            BTreePtr table_btree = table->index(constant::INDEX_PRIMARY);
            CHECK(table_btree.get() != nullptr);
            LOG_INFO("\tPrimary index created with path = {}, xid = {}, primary_schema size = {}, extent id = {}",
                    table_btree->get_file_path().c_str(), table_btree->get_xid(), table_btree->get_schema()->get_sort_keys().size(), table_btree->get_root_offset());

            ExtentSchemaPtr index_schema = table_btree->get_schema();
            FieldPtr extent_id_field = index_schema->get_field(constant::INDEX_EID_FIELD);
            FieldArrayPtr key_fields = index_schema->get_sort_fields();

            LOG_INFO("\t\ttable_key_fields->size() = {}, key_fields->size() = {}, table->has_primary() = {}",
                        table_key_fields->size(), key_fields->size(), table->has_primary());

            LOG_INFO("\tTable schema has {} sort keys:", table_schema->get_sort_keys().size());
            for (auto &key: table_schema->get_sort_keys()) {
                LOG_INFO("\t\t{}", key );
            }

            LOG_INFO("\tPrimary index schema has {} sort keys:", index_schema->get_sort_keys());
            for (auto &key: index_schema->get_sort_keys()) {
                LOG_INFO("\t\t{}", key );
            }

            // Verifying primary key sizes in table, schema, and index schema
            if (table->has_primary()) {
                CHECK(table_key_fields->size() == key_fields->size());
                CHECK(table_schema->get_sort_keys().size() == index_schema->get_sort_keys().size());
            }

            // Verifying field types
            for (size_t i = 0; i < table_key_fields->size(); i++) {
                CHECK(table_key_fields->at(i)->get_type() == key_fields->at(i)->get_type());
            }

            // Verify extents for the primary key
            BTree::Iterator btree_iter = table_btree->begin();
            while(btree_iter != table_btree->end()) {
                const Extent::Row &btree_row = *btree_iter;
                uint64_t extent_id = extent_id_field->get_uint64(&btree_row);
                LOG_INFO("\tVerifying extent_id = {}", extent_id);

                StorageCache::SafePagePtr page = table->read_page(extent_id);
                StorageCache::Page::Iterator page_iter = page->last();
                Extent::Row table_extent_last_row = *(page_iter);
                if (table->has_primary()) {
                    FieldTuple key_tuple(key_fields, &btree_row);
                    FieldTuple table_extent_last_row_tuple(table_key_fields, &table_extent_last_row);
                    CHECK(table_extent_last_row_tuple.equal_strict(key_tuple));
                }

                ++btree_iter;
            }

            // TODO: add validation of secondary indexes
            // TODO: add validation of previous xid snapshot
        }
    }

    std::cout << std::endl;
    springtail_shutdown();

}