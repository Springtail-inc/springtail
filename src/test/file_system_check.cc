#include <common/json.hh>
#include <common/properties.hh>
#include <storage/vacuumer.hh>
#include <sys_tbl_mgr/system_tables.hh>

#include <test/file_system_check.hh>

using namespace springtail;
using namespace springtail::test;

FSCheck::FSCheck( std::optional<uint64_t> db_id, uint64_t max_xid, bool all_xids) : _max_xid(max_xid), _all_xids(all_xids), _db_id(db_id)
{
    // get all database ids
    _databases = Properties::get_databases();

    // get base path to database files
    nlohmann::json json = Properties::get(Properties::STORAGE_CONFIG);
    Json::get_to<std::filesystem::path>(json, "table_dir", _table_base);
    _table_base = Properties::make_absolute_path(_table_base);
    LOG_INFO("Verifying tables at table_base = {}, max_xid = {}", _table_base.string(), _max_xid);

    VacuumerUtils vacuumer_utils;
    bool vacuumer_enabled = vacuumer_utils.is_enabled();
    for (const auto &[db_id, db_name]: _databases) {
        if (_db_id && _db_id != db_id) {
            continue;
        }

        uint64_t cutoff_xid = 0;
        if (vacuumer_enabled) {
            cutoff_xid = vacuumer_utils.get_last_seen_cutoff_xid(db_id);
        }
        if (!vacuumer_enabled || _max_xid >= cutoff_xid) {
            _db_id_to_cutoff_xid.insert(std::make_pair(db_id, cutoff_xid));
        }
    }
}

template<typename T, typename = void>
struct has_secondary_key : std::false_type {};

template<typename T>
struct has_secondary_key<T, std::void_t<decltype(T::Secondary::KEY)>> : std::true_type {};

template<typename Tbl>
std::pair<TablePtr, std::shared_ptr<std::vector<FieldPtr>>>
FSCheck::_get_table_and_fields(uint64_t db_id)
{
    auto schema = std::make_shared<ExtentSchema>(Tbl::Data::SCHEMA);
    std::vector<Index> secondary_keys;
    if constexpr (has_secondary_key<Tbl>::value) {
        std::vector<Index> keys;
        Index idx;
        idx.id = 1;
        idx.table_id = Tbl::ID;
        idx.is_unique = false;
        idx.state = static_cast<uint8_t>(sys_tbl::IndexNames::State::READY);

        uint32_t idx_position = 0;
        for (auto const& col: Tbl::Secondary::KEY) {
            // find column position in schema
            auto it = std::ranges::find_if(Tbl::Data::SCHEMA, [&](auto const& v)
                    {
                    return col == v.name;
                    }
                    );
            assert(it != Tbl::Data::SCHEMA.end());
            idx.columns.emplace_back(idx_position, it->position);
            ++idx_position;
        }
        secondary_keys.push_back(idx);
    }
    TableMetadata tbl_meta{};
    tbl_meta.snapshot_xid = 1;

    uint64_t xid = constant::LATEST_XID;

    TablePtr table = std::make_shared<SystemTable>(db_id, Tbl::ID, xid, _table_base, Tbl::Primary::KEY, secondary_keys, tbl_meta, schema);
    std::shared_ptr<std::vector<FieldPtr>> fields = schema->get_fields();

    return std::make_pair(table, fields);
}

void
FSCheck::_read_namespaces(uint64_t db_id, uint64_t max_xid)
{
    LOG_INFO("Namespaces for db {}", db_id);
    auto [table, fields] = _get_table_and_fields<sys_tbl::NamespaceNames>(db_id);

    for (auto row: (*table)) {
        uint64_t ns_id = fields->at(sys_tbl::NamespaceNames::Data::NAMESPACE_ID)->get_uint64(&row);
        std::string name(fields->at(sys_tbl::NamespaceNames::Data::NAME)->get_text(&row));
        uint64_t xid = fields->at(sys_tbl::NamespaceNames::Data::XID)->get_uint64(&row);
        uint64_t lsn = fields->at(sys_tbl::NamespaceNames::Data::LSN)->get_uint64(&row);
        bool exists = fields->at(sys_tbl::NamespaceNames::Data::EXISTS)->get_bool(&row);
        LOG_INFO("\tNamespace {}:{}", ns_id, name);
        _record_max_xid(xid);

        if (xid > max_xid) {
            break;
        }

        if (!exists) {
            _db_ns_id_map.erase(std::make_pair(db_id, ns_id));
        } else {
            FSNamespace ns_data{ns_id, name, xid, lsn, exists};
            _db_ns_id_map[std::make_pair(db_id, ns_id)] = ns_data;
        }
    }
}

void
FSCheck::_read_tables(uint64_t db_id, uint64_t max_xid)
{
    LOG_INFO("Tables for db {}", db_id);
    auto [table, fields] = _get_table_and_fields<sys_tbl::TableNames>(db_id);

    // 1. read all the tables and filter out the tables that have not been deleted yet
    for (auto row: (*table)) {
        uint64_t ns_id = fields->at(sys_tbl::TableNames::Data::NAMESPACE_ID)->get_uint64(&row);
        std::string name(fields->at(sys_tbl::TableNames::Data::NAME)->get_text(&row));
        uint64_t table_id = fields->at(sys_tbl::TableNames::Data::TABLE_ID)->get_uint64(&row);
        uint64_t xid = fields->at(sys_tbl::TableNames::Data::XID)->get_uint64(&row);
        uint64_t lsn = fields->at(sys_tbl::TableNames::Data::LSN)->get_uint64(&row);
        bool exists = fields->at(sys_tbl::TableNames::Data::EXISTS)->get_bool(&row);
        _record_max_xid(xid);

        if (xid > max_xid) {
            break;
        }

        if (!exists) {
            _db_tbl_id_map.erase(std::pair(db_id, table_id));
            LOG_INFO("Removed table: db_id {}, namespace_id {}, table_id {}, xid {}", db_id, ns_id, table_id, xid);
        } else {
            FSTable table_data{ns_id, name, table_id, xid, lsn, exists};
            _db_tbl_id_map[std::make_pair(db_id, table_id)] = table_data;
            LOG_INFO("Added table: db_id {}, namespace_id {}, table_id {}, xid {}", db_id, ns_id, table_id, xid);
        }
        LOG_INFO("Table {}:{}", table_id, name);
    }

    // 2. iterate over the found tables
    for (auto it = _db_tbl_id_map.lower_bound(std::make_pair(db_id, 0));
            it != _db_tbl_id_map.end() && it->first.first == db_id; ++it) {
        uint64_t table_id_key = it->second.table_id;
        uint64_t table_xid = it->second.xid;

        // 3. read all columns per table and filter out existing columns
        {
            LOG_INFO("Getting columns for table {}:{}, xid {}", table_id_key, it->second.name, it->second.xid);
            std::map<uint32_t, SchemaColumn> pos_to_column;
            auto [schema_table, schema_fields] = _get_table_and_fields<sys_tbl::Schemas>(db_id);
            auto search_key = sys_tbl::Schemas::Primary::key_tuple(table_id_key, 0, 0, 0);
            auto table_iter = schema_table->lower_bound(search_key);
            for (; table_iter != schema_table->end(); ++table_iter) {
                auto &row = *table_iter;
                uint64_t table_id = fields->at(sys_tbl::Schemas::Data::TABLE_ID)->get_uint64(&row);
                if (table_id != table_id_key) {
                    break;
                }
                uint32_t position = schema_fields->at(sys_tbl::Schemas::Data::POSITION)->get_uint32(&row);
                uint64_t xid = schema_fields->at(sys_tbl::Schemas::Data::XID)->get_uint64(&row);
                uint64_t lsn = schema_fields->at(sys_tbl::Schemas::Data::LSN)->get_uint64(&row);
                bool exists = schema_fields->at(sys_tbl::Schemas::Data::EXISTS)->get_bool(&row);
                std::string name(schema_fields->at(sys_tbl::Schemas::Data::NAME)->get_text(&row));
                uint8_t type = schema_fields->at(sys_tbl::Schemas::Data::TYPE)->get_uint8(&row);
                uint32_t pg_type = schema_fields->at(sys_tbl::Schemas::Data::PG_TYPE)->get_int32(&row);
                bool nullable = schema_fields->at(sys_tbl::Schemas::Data::NULLABLE)->get_bool(&row);
                std::optional<std::string> default_value;
                if (!schema_fields->at(sys_tbl::Schemas::Data::DEFAULT)->is_null(&row)) {
                    default_value = schema_fields->at(sys_tbl::Schemas::Data::DEFAULT)->get_text(&row);
                }
                uint8_t update_type = schema_fields->at(sys_tbl::Schemas::Data::UPDATE_TYPE)->get_uint8(&row);
                _record_max_xid(xid);

                if (xid < table_xid || xid > max_xid) {
                    continue;
                }

                if (!exists) {
                    pos_to_column.erase(position);
                    LOG_INFO("\tRemoved column: db_id {}, table_id {}, xid {}, name '{}'", db_id, table_id, xid, name);
                } else {
                    std::optional<uint32_t> pk_position;
                    SchemaColumn column(xid, lsn, name, position, static_cast<SchemaType>(type), pg_type, exists, nullable,
                                        pk_position, default_value);
                    column.update_type = static_cast<SchemaUpdateType>(update_type);
                    pos_to_column[position] = column;
                    LOG_INFO("\tAdded column: db_id {}, table_id {}, xid {}, name '{}'", db_id, table_id, xid, name);
                }
            }

            // 4. copy all found columns into the table data
            _db_tbl_id_map.at(std::make_pair(db_id, table_id_key)).pos_to_column.insert(pos_to_column.begin(), pos_to_column.end());
        }

        // 5. read all indexes in READY state for this table and filter out columns
        {
            LOG_INFO("Getting indexes for table {}:{}, xid {}", table_id_key, it->second.name, it->second.xid);
            std::map<uint64_t, FSIndex> id_to_index;
            auto [index_table, index_fields] = _get_table_and_fields<sys_tbl::IndexNames>(db_id);
            auto search_key = sys_tbl::IndexNames::Primary::key_tuple(table_id_key, 0, 0, 0);
            auto table_iter = index_table->lower_bound(search_key);
            for (; table_iter != index_table->end(); ++table_iter) {
                auto &row = *table_iter;
                uint64_t table_id = index_fields->at(sys_tbl::IndexNames::Data::TABLE_ID)->get_uint64(&row);
                if (table_id != table_id_key) {
                    break;
                }
                uint64_t index_id = index_fields->at(sys_tbl::IndexNames::Data::INDEX_ID)->get_uint64(&row);
                uint64_t xid = index_fields->at(sys_tbl::IndexNames::Data::XID)->get_uint64(&row);
                uint64_t lsn = index_fields->at(sys_tbl::IndexNames::Data::LSN)->get_uint64(&row);
                uint64_t namespace_id = index_fields->at(sys_tbl::IndexNames::Data::NAMESPACE_ID)->get_uint64(&row);
                std::string name(index_fields->at(sys_tbl::IndexNames::Data::NAME)->get_text(&row));
                uint8_t state = index_fields->at(sys_tbl::IndexNames::Data::STATE)->get_uint8(&row);
                bool is_unique = index_fields->at(sys_tbl::IndexNames::Data::IS_UNIQUE)->get_bool(&row);
                _record_max_xid(xid);

                if (xid < table_xid || xid > max_xid) {
                    continue;
                }

                if (static_cast<sys_tbl::IndexNames::State>(state) != sys_tbl::IndexNames::State::READY) {
                    id_to_index.erase(index_id);
                    LOG_INFO("\tRemoved index: index_id {}, xid {}, name '{}', state {}", index_id, xid, name, state);
                } else {
                    LOG_INFO("\tAdding index: index_id {}, xid {}, name '{}'", index_id, xid, name);
                    std::string schema_name = _db_ns_id_map.at(std::make_pair(db_id, namespace_id)).ns_name;
                    FSIndex fs_index{xid, lsn, {index_id, schema_name, name, table_id, is_unique, state}};

                    // read index columns
                    auto [idx_table, idx_fields] = _get_table_and_fields<sys_tbl::Indexes>(db_id);
                    auto idx_search_key = sys_tbl::Indexes::Primary::key_tuple(table_id_key, index_id, xid, lsn, 0);
                    auto idx_table_iter = idx_table->lower_bound(idx_search_key);

                    std::map<uint32_t, uint32_t> columns;
                    for (; idx_table_iter != idx_table->end(); ++idx_table_iter) {
                        auto &idx_row = *idx_table_iter;
                        uint64_t idx_table_id = idx_fields->at(sys_tbl::Indexes::Data::TABLE_ID)->get_uint64(&idx_row);
                        uint64_t idx_index_id = idx_fields->at(sys_tbl::Indexes::Data::INDEX_ID)->get_uint64(&idx_row);
                        uint64_t idx_xid = idx_fields->at(sys_tbl::Indexes::Data::XID)->get_uint64(&idx_row);
                        uint64_t idx_lsn = idx_fields->at(sys_tbl::Indexes::Data::LSN)->get_uint64(&idx_row);
                        if (idx_table_id != table_id || idx_index_id != index_id || idx_xid != xid || idx_lsn != lsn) {
                            break;
                        }
                        uint32_t position = idx_fields->at(sys_tbl::Indexes::Data::POSITION)->get_uint32(&idx_row);
                        uint32_t column_id = idx_fields->at(sys_tbl::Indexes::Data::COLUMN_ID)->get_uint32(&idx_row);
                        columns[position] = column_id;
                        LOG_INFO("\t\tAdding index column: index_id {}, index_xid {}, position = {}, column_id {}", index_id, idx_xid, position, column_id);
                    }
                    for (const auto &[idx_position, col_position]: columns) {
                        Index::Column column{idx_position, col_position};
                        fs_index.index.columns.push_back(column);
                    }
                    id_to_index[index_id] = fs_index;
                    LOG_INFO("\tAdded index: index_id {}, xid {}, name '{}'", index_id, xid, name);
                }
            }

            // 6. set indexes in the table
            _db_tbl_id_map.at(std::make_pair(db_id, table_id_key)).id_to_index = id_to_index;

            // 7. read all roots per index and set primary key positions
            LOG_INFO("Getting roots for table {}:{}, xid {}", table_id_key, it->second.name, it->second.xid);
            for (const auto &[index_id, fs_index]: id_to_index) {
                LOG_INFO("\tGetting roots for index {}", index_id);

                // 8. set primary key positions in the table
                if (index_id == constant::INDEX_PRIMARY) {
                    for (auto col_iter: fs_index.index.columns) {
                        _db_tbl_id_map.at(std::make_pair(db_id, table_id_key)).pos_to_column.at(col_iter.position).pkey_position = col_iter.idx_position;
                    }
                }

                // 9. read all roots for this table and index
                std::vector<FSRoot> roots;
                auto [root_table, root_fields] = _get_table_and_fields<sys_tbl::TableRoots>(db_id);
                auto root_search_key = sys_tbl::TableRoots::Primary::key_tuple(table_id_key, index_id, 0);
                auto root_table_iter = root_table->lower_bound(root_search_key);
                for (; root_table_iter != root_table->end(); ++root_table_iter) {
                    auto &root_row = *root_table_iter;
                    uint64_t root_table_id = root_fields->at(sys_tbl::TableRoots::Data::TABLE_ID)->get_uint64(&root_row);
                    uint64_t root_index_id = root_fields->at(sys_tbl::TableRoots::Data::INDEX_ID)->get_uint64(&root_row);
                    if (root_table_id != table_id_key || root_index_id != index_id) {
                        break;
                    }
                    uint64_t root_xid = root_fields->at(sys_tbl::TableRoots::Data::XID)->get_uint64(&root_row);
                    uint64_t root_extent_id = root_fields->at(sys_tbl::TableRoots::Data::EXTENT_ID)->get_uint64(&root_row);
                    uint64_t root_snapshot_xid = root_fields->at(sys_tbl::TableRoots::Data::SNAPSHOT_XID)->get_uint64(&root_row);
                    _record_max_xid(root_xid);
                    _record_max_xid(root_snapshot_xid);

                    if (root_xid < table_xid) {
                        continue;
                    }

                    if (root_xid > max_xid) {
                        break;
                    }

                    FSRoot root{root_xid, root_extent_id, root_snapshot_xid};
                    roots.push_back(root);
                    LOG_INFO("\t\tAdded root for root_xid {}, extent_id {}, snapshot_xid {}", root_xid, root_extent_id, root_snapshot_xid);
                }

                // 10. set the roots in the table
                for (auto root: roots) {
                    _db_tbl_id_map.at(std::make_pair(db_id, table_id_key))
                        .index_xid_to_root
                        .try_emplace({index_id, root.xid}, root);
                }
            }
        }

        // 11. read all stats for this table
        {
            LOG_INFO("Getting stats for table {}:{}, xid {}", table_id_key, it->second.name, it->second.xid);
            std::map<uint64_t, FSStats> xid_to_stats;

            auto [stats_table, stats_fields] = _get_table_and_fields<sys_tbl::TableStats>(db_id);
            auto search_key = sys_tbl::TableStats::Primary::key_tuple(table_id_key, 0);
            auto table_iter = stats_table->lower_bound(search_key);
            for (; table_iter != stats_table->end(); ++table_iter) {
                auto &row = *table_iter;
                uint64_t table_id = stats_fields->at(sys_tbl::TableStats::Data::TABLE_ID)->get_uint64(&row);
                if (table_id != table_id_key) {
                    break;
                }
                uint64_t xid = stats_fields->at(sys_tbl::TableStats::Data::XID)->get_uint64(&row);
                uint64_t row_count = stats_fields->at(sys_tbl::TableStats::Data::ROW_COUNT)->get_uint64(&row);
                uint64_t end_offset = stats_fields->at(sys_tbl::TableStats::Data::END_OFFSET)->get_uint64(&row);
                _record_max_xid(xid);

                if (xid < table_xid) {
                    continue;
                }

                if (xid > max_xid) {
                    break;
                }

                FSStats stats{xid, row_count, end_offset};
                xid_to_stats[xid] = stats;
                LOG_INFO("\tAdded stats for xid {}, row_count {}, end_offser {}", xid, row_count, end_offset);
            }
            _db_tbl_id_map.at(std::make_pair(db_id, table_id_key)).xid_to_stats = xid_to_stats;
        }
    }
}

void
FSCheck::_record_max_xid(uint64_t xid)
{
    if (xid > _max_recorded_xid) {
        _max_recorded_xid = xid;
    }
}

void
FSCheck::_read_database_info(uint64_t db_id, uint64_t max_xid)
{
    _max_recorded_xid = 0;
    _db_ns_id_map.clear();
    _db_tbl_id_map.clear();
    _read_namespaces(db_id, max_xid);
    _read_tables(db_id, max_xid);
}

void
FSCheck::check_dbs()
{
    uint64_t first_xid = 1;
    if (!_all_xids) {
        first_xid = _max_xid;
    }

    // iterate over databases
    for (const auto &[db_id, cutoff_xid]: _db_id_to_cutoff_xid) {
        _check_db(db_id, first_xid, cutoff_xid);
    }
}

void
FSCheck::check_db(uint64_t db_id)
{
    uint64_t first_xid = 1;
    if (!_all_xids) {
        first_xid = _max_xid;
    }

    uint64_t cutoff_xid = _db_id_to_cutoff_xid.at(db_id);
    _check_db(db_id, first_xid, cutoff_xid);
}

void
FSCheck::_check_db(uint64_t db_id, uint64_t first_xid, uint64_t cutoff_xid)
{
    const std::string &db_name = _databases.at(db_id);
    LOG_INFO("Verifying database {}:{} with first_xid = {} and cuttoff_xid = {}",
            db_id, db_name, first_xid, cutoff_xid);

    uint64_t start_xid = (first_xid < cutoff_xid)? cutoff_xid : first_xid;
    for (uint64_t max_xid = start_xid; max_xid <= _max_xid; ++max_xid) {
        LOG_INFO("Verifying database {}:{} iteration max_xid = {}",
            db_id, db_name, max_xid);
        _read_database_info(db_id, max_xid);
        if (_all_xids && _max_recorded_xid < max_xid) {
            break;
        }

        for (auto it = _db_tbl_id_map.lower_bound(std::make_pair(db_id, 0));
                it != _db_tbl_id_map.end() && it->first.first == db_id; ++it) {
            LOG_INFO("Verifying table {}:{}:{}", it->first.first, it->first.second, it->second.name);
            _check_db_table(db_id, db_name, it->second);
        }

        if (!_all_xids) {
            break;
        }
    }
}

void
FSCheck::check_db_table(uint64_t db_id, uint64_t table_id, bool all_xids)
{
    uint64_t first_xid = 1;
    if (!all_xids) {
        first_xid = _max_xid;
    }

    uint64_t cutoff_xid = _db_id_to_cutoff_xid.at(db_id);
    const std::string &db_name = _databases.at(db_id);
    LOG_INFO("Verifying database {}:{} table {} with first_xid = {} and cuttoff_xid = {}",
        db_id, db_name, table_id, first_xid, cutoff_xid);
    uint64_t start_xid = (first_xid < cutoff_xid)? cutoff_xid : first_xid;
    for (uint64_t max_xid = start_xid; max_xid < _max_xid; ++max_xid) {
        LOG_INFO("Verifying database {}:{} table {} iteration max_xid = {}",
            db_id, db_name, table_id, max_xid);
        _read_database_info(db_id, max_xid);
        if (_max_recorded_xid < max_xid) {
            break;
        }

        std::pair<uint64_t, uint64_t> key = std::make_pair(db_id, table_id);
        auto it = _db_tbl_id_map.lower_bound(key);
        if (it == _db_tbl_id_map.end() || it->first != key) {
            LOG_ERROR("Database {}:{}: table {} is not found", db_id, db_name, table_id);
            CHECK(false);
        }
        _check_db_table(db_id, db_name, it->second);
    }
}

void
FSCheck::_validate_primary_extent(std::shared_ptr<Table> table, ExtentSchemaPtr table_schema)
{
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
    auto btree_iter = table_btree->begin();
    while(btree_iter != table_btree->end()) {
        const Extent::Row &btree_row = *btree_iter;
        uint64_t extent_id = extent_id_field->get_uint64(&btree_row);
        LOG_INFO("\t\tVerifying extent_id = {}", extent_id);

        StorageCache::SafePagePtr page = table->read_page(extent_id);
        StorageCache::Page::Iterator page_iter = page->last();
        Extent::Row table_extent_last_row = *page_iter;
        if (table->has_primary()) {
            FieldTuple key_tuple(key_fields, &btree_row);
            FieldTuple table_extent_last_row_tuple(table_key_fields, &table_extent_last_row);
            CHECK(table_extent_last_row_tuple.equal_strict(key_tuple));
        }

        ++btree_iter;
    }
}

void
FSCheck::_validate_secondary_extents(std::shared_ptr<Table> table, ExtentSchemaPtr table_schema)
{
    FieldArrayPtr table_fields = table_schema->get_fields();
    std::vector<uint64_t> index_ids = table->get_secondary_idx_ids();
    for (auto id: index_ids) {
        BTreePtr table_btree = table->index(id);
        CHECK(table_btree.get() != nullptr);
        LOG_INFO("\tSecondary index {} created with path = {}, xid = {}, primary_schema size = {}, extent id = {}",
                id, table_btree->get_file_path().c_str(), table_btree->get_xid(),
                table_btree->get_schema()->get_sort_keys().size(), table_btree->get_root_offset());

        std::vector<std::string> index_table_cols = table->get_index_column_names(id);
        ExtentSchemaPtr index_btree_schema = table_btree->get_schema();

        FieldPtr extent_id_field = index_btree_schema->get_field(constant::INDEX_EID_FIELD);
        FieldPtr row_field = index_btree_schema->get_field(constant::INDEX_RID_FIELD);
        FieldArrayPtr key_fields = index_btree_schema->get_sort_fields();
        LOG_INFO("\tSecondary index: schema size {}, fields size {}", index_btree_schema->get_sort_keys().size(), key_fields->size());

        // Verify extents for the secondary key
        std::set<uint64_t> extent_set;
        auto btree_iter = table_btree->begin();
        while(btree_iter != table_btree->end()) {
            const Extent::Row &btree_row = *btree_iter;
            uint64_t extent_id = extent_id_field->get_uint64(&btree_row);
            uint32_t row_id = row_field->get_uint32(&btree_row);
            extent_set.insert(extent_id);

            StorageCache::SafePagePtr page = table->read_page(extent_id);
            auto page_iter = page->begin();
            page_iter += row_id;
            Extent::Row table_extent_row = *page_iter;

            auto key_tuple = std::make_shared<FieldTuple>(key_fields, &btree_row);
            auto btree_keys = index_btree_schema->tuple_subset(key_tuple, index_table_cols);

            auto table_extent_row_tuple = std::make_shared<FieldTuple>(table_fields, &table_extent_row);
            auto table_keys = table_schema->tuple_subset(table_extent_row_tuple, index_table_cols);
            CHECK(btree_keys->size() == table_keys->size());
            CHECK(table_keys->equal_prefix(*btree_keys));

            ++btree_iter;
        }
        LOG_INFO("\tVerified {} extents; found extents: ", extent_set.size());
        for (auto extent_id: extent_set) {
            LOG_INFO("\t\t{}", extent_id);
        }
    }
}

void
FSCheck::_check_db_table(uint64_t db_id, const std::string &db_name, const FSTable &fs_table)
{
    // 1. Verify table namespace and xids
    uint64_t ns_id = fs_table.ns_id;
    FSNamespace ns = _db_ns_id_map.at(std::make_pair(db_id, ns_id));
    LOG_INFO("Verifying Database {}:{}; Namespace {}:{}; Table {}:{}",
        db_id, db_name, ns.ns_id, ns.ns_name, fs_table.table_id, fs_table.name);
    CHECK(ns.exists);
    CHECK(fs_table.xid >= ns.xid);

    // 2. Verify column xids
    std::vector<SchemaColumn> columns;
    for (const auto &[pos, column]: fs_table.pos_to_column) {
        LOG_INFO("\tVerifying Column {}:{}, type {}, pg_type {}, nullable {}, pkey_position {}, default: {}",
            column.position, column.name, to_string(column.type), column.pg_type, column.nullable,
            (column.pkey_position.has_value())? column.pkey_position.value(): -1,
            (column.default_value.has_value())? column.default_value.value(): ""
        );
        CHECK(column.exists);
        CHECK(column.xid >= fs_table.xid);
        columns.push_back(column);
    }

    // 3. Verify indexes xids and roots
    std::vector<TableRoot> roots;
    uint64_t root_sxid = constant::INVALID_XID;
    uint64_t row_count = 0;
    uint64_t end_offset = 0;
    std::vector<Index> secondary_indexes;
    for (const auto &[index_id, fs_index]: fs_table.id_to_index) {
        const struct Index index = fs_index.index;
        LOG_INFO("\tVerifying Index {}:{}:{}, is_unique {}, state {}",
            index.schema, index.id, index.name, index.is_unique, index.state
        );
        for (auto idx_col_it: index.columns) {
            LOG_INFO("\t\tIndex position: {}, table column: {}:{}",
                idx_col_it.idx_position, idx_col_it.position, fs_table.pos_to_column.at(idx_col_it.position).name
            );
        }
        CHECK(index.state == static_cast<uint8_t>(sys_tbl::IndexNames::State::READY));
        CHECK(fs_index.xid >= fs_table.xid);

        // 4. Verify roots
        const struct FSRoot *last_root = nullptr;
        const struct FSStats *last_stat = nullptr;
        for (auto idx_root_it = fs_table.index_xid_to_root.lower_bound(std::make_pair(index_id, 0));
                idx_root_it != fs_table.index_xid_to_root.end() && idx_root_it->first.first == index_id;
                ++idx_root_it ) {
            const struct FSRoot &root = idx_root_it->second;
            last_root = &(idx_root_it->second);
            LOG_INFO("\t\tVerifying roots: root.xid = {}, fs_table.xid = {}", root.xid, fs_table.xid);
            CHECK(root.xid >= fs_table.xid);
            auto stat_it = fs_table.xid_to_stats.find(root.xid);
            CHECK(stat_it != fs_table.xid_to_stats.end());
            last_stat = &(stat_it->second);
        }

        if (last_root != nullptr) {
            roots.push_back({index_id, last_root->extent_id});
        }
        if (index_id == constant::INDEX_PRIMARY) {
            CHECK(last_root != nullptr);
            root_sxid = last_root->snapshot_xid;
            CHECK(last_stat != nullptr);
            row_count = last_stat->row_count;
            end_offset = last_stat->end_offset;
            continue;
        }
        secondary_indexes.push_back(index);
    }

    CHECK(root_sxid != constant::LATEST_XID);

    // 5. Create table
    auto schema = std::make_shared<ExtentSchema>(columns);

    TableMetadata tbl_meta{};
    tbl_meta.roots = roots;

    tbl_meta.stats.row_count = row_count;
    tbl_meta.stats.end_offset = end_offset;
    tbl_meta.snapshot_xid = root_sxid;

    auto table = std::make_shared<Table>(db_id, fs_table.table_id, fs_table.xid, _table_base,
                                schema->get_sort_keys(), secondary_indexes, tbl_meta, schema);

    LOG_INFO("\tValidata Table indexes for table {}, dir: {}, row_count: {}, end_offset: {}, sxid: {}",
            table->id(), table->get_dir_path().c_str(), row_count, end_offset, root_sxid);

    // 6. Validate primary index extent
    _validate_primary_extent(table, schema);

    // 7. Validate secondary index extent
    _validate_secondary_extents(table, schema);
}
