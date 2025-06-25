#include <common/constants.hh>
#include <memory>
#include <sys_tbl_mgr/client.hh>
#include <sys_tbl_mgr/system_tables.hh>
#include <sys_tbl_mgr/table.hh>
#include <sys_tbl_mgr/table_mgr.hh>
#include <write_cache/write_cache_client.hh>
#include <common/json.hh>
#include <common/properties.hh>

//#define SPRINGTAIL_INCLUDE_TIME_TRACES 1
#include <common/time_trace.hh>

namespace springtail {

namespace table_helpers {

std::filesystem::path
get_table_dir(const std::filesystem::path &base,
              uint64_t db_id,
              uint64_t table_id,
              uint64_t snapshot_xid)
{
    std::string db_dir = std::to_string(db_id);
    std::string table_dir = fmt::format("{}-{}", table_id, snapshot_xid);
    return base / db_dir / table_dir;
}

} // namespace table_helpers

namespace indexer_helpers {

    /**
     * @brief Compile-time selector for the secondary-index operation.
     */
    enum class IndexOperation { Insert, Remove };

    template <IndexOperation op,            // Operation on the index - insert/remove
             typename RowPtrT>              // SafePagePtr | std::shared_ptr<Extent>
    static void _update_secondary_index(
            uint64_t                        extent_id,
            const RowPtrT                  &rows,        // pointer-like, supports *rows
            const MutableBTreePtr          &root,
            const std::vector<uint32_t>    &idx_cols,
            const ExtentSchemaPtr          &schema)
    {
        /* 1. Column metadata is the same for every row – fetch it once. */
        const auto column_names = schema->get_column_names(idx_cols);
        const auto key_fields   = schema->get_fields(column_names);

        /* 2. Build the (extent_id , row_id) value tuple incrementally. */
        auto value_fields   = std::make_shared<FieldArray>(2);
        (*value_fields)[0]  = std::make_shared<ConstTypeField<uint64_t>>(extent_id);

        uint32_t row_id = 0;
        for (auto &row : *rows) {
            (*value_fields)[1] = std::make_shared<ConstTypeField<uint32_t>>(row_id);

            auto kv = std::make_shared<KeyValueTuple>(key_fields, value_fields, &row);
            if constexpr (op == IndexOperation::Insert) {
                root->insert(kv);
            } else { /* op == IndexOperation::Remove */
                root->remove(kv);
            }
            ++row_id;
        }

        LOG_DEBUG(LOG_BTREE, "{} {} secondary rows", 
            (op == IndexOperation::Insert) ? "Populated"
            : "Invalidated",
            row_id);
    }

    /* ------------------------------  PAGE  ----------------------------------- */
    void populate_index_for_page(uint64_t extent_id,
            const StorageCache::SafePagePtr &page,
            const MutableBTreePtr          &root,
            const std::vector<uint32_t>    &idx_cols,
            const ExtentSchemaPtr          &schema)
    {
        _update_secondary_index<IndexOperation::Insert>(extent_id, page, root,
                idx_cols, schema);
    }

    void invalidate_index_for_page(uint64_t extent_id,
            const StorageCache::SafePagePtr &page,
            const MutableBTreePtr          &root,
            const std::vector<uint32_t>    &idx_cols,
            const ExtentSchemaPtr          &schema)
    {
        _update_secondary_index<IndexOperation::Remove>(extent_id, page, root,
                idx_cols, schema);
    }

    /* -----------------------------  EXTENT  ---------------------------------- */
    void populate_index_for_extent(uint64_t extent_id,
            const std::shared_ptr<Extent> &extent,
            const MutableBTreePtr         &root,
            const std::vector<uint32_t>   &idx_cols,
            const ExtentSchemaPtr         &schema)
    {
        _update_secondary_index<IndexOperation::Insert>(extent_id, extent, root,
                idx_cols, schema);
    }

    void invalidate_index_for_extent(uint64_t extent_id,
            const std::shared_ptr<Extent> &extent,
            const MutableBTreePtr         &root,
            const std::vector<uint32_t>   &idx_cols,
            const ExtentSchemaPtr         &schema)
    {
        _update_secondary_index<IndexOperation::Remove>(extent_id, extent, root,
                idx_cols, schema);
    }
} // namespace indexer_helpers

    namespace {
        const static std::vector<SchemaColumn> ROOTS_SCHEMA = {
            { "root", 1, SchemaType::UINT64, 20, true },
            { "index_id", 2, SchemaType::UINT64, 20, false },
        };

        std::shared_ptr<ExtentSchema> 
        _create_index_schema(ExtentSchemaPtr schema, const std::vector<uint32_t>& index_columns)
        {

            // get the column names in the order they appear in the index
            auto &&col_names = schema->get_column_names(index_columns);

            SchemaColumn extent_c(constant::INDEX_EID_FIELD, 0, SchemaType::UINT64, 0, false);
            SchemaColumn row_c(constant::INDEX_RID_FIELD, 1, SchemaType::UINT32, 0, false);

            auto key = col_names;
            key.push_back(constant::INDEX_EID_FIELD);
            key.push_back(constant::INDEX_RID_FIELD);

            return schema->create_schema(col_names, { extent_c, row_c }, key);
        }
    }

    Table::Table(uint64_t db_id,
                 uint64_t table_id,
                 uint64_t xid,
                 const std::filesystem::path &table_base,
                 const std::vector<std::string> &primary_key,
                 const std::vector<Index> &secondary,
                 const TableMetadata &metadata,
                 ExtentSchemaPtr schema)
        : _db_id(db_id),
          _id(table_id),
          _xid(xid),
          _primary_key(primary_key),
          _schema(schema),
          _stats(metadata.stats)
    {
        // construct the table's data directory
        _table_dir = table_helpers::get_table_dir(table_base, db_id, table_id, metadata.snapshot_xid);

        // check if the table directory exists; if not, table is considered vacant/empty
        if (!std::filesystem::exists(_table_dir)) {
            _primary_index = nullptr;
            return;
        }

        // store the roots schema / field
        _roots_schema = std::make_shared<ExtentSchema>(ROOTS_SCHEMA);
        _roots_root_f = _roots_schema->get_field("root");
        _roots_index_id_f = _roots_schema->get_field("index_id");

                // handle if the roots were not provided
        auto roots = metadata.roots;
        if (roots.empty()) {
            if (std::filesystem::exists(_table_dir / constant::ROOTS_FILE)) {
                // read the roots from the look-aside file
                auto roots_path = std::filesystem::read_symlink(_table_dir / constant::ROOTS_FILE);
                auto root_handle = IOMgr::get_instance()->open(roots_path, IOMgr::IO_MODE::READ, true);
                auto response = root_handle->read(0);
                auto extent = std::make_shared<Extent>(response->data);
                for (auto &row : *extent) {
                    roots.push_back(
                            {_roots_index_id_f->get_uint64(&row),
                            _roots_root_f->get_uint64(&row)});
                }
                // XXX is this the right thing to do?  forces the XID to the known XID of the roots
                xid = extent->header().xid;
            } else {
                // fill the root offsets with UNKNOWN_EXTENT to indicate an empty tree
                roots.push_back({constant::INDEX_PRIMARY, constant::UNKNOWN_EXTENT});
            }
        }
        assert(!roots.empty());

        for (auto const& idx: secondary) {
            if (idx.state != static_cast<uint8_t>(sys_tbl::IndexNames::State::READY)) {
                continue;
            }
            assert(idx.id != constant::INDEX_PRIMARY);

            auto it = std::ranges::find_if(roots, [&](auto const &v) { return v.index_id == idx.id; });
            if (it == roots.end()) {
                // fill the root offsets with UNKNOWN_EXTENT to indicate an empty tree
                roots.emplace_back(idx.id, constant::UNKNOWN_EXTENT);
            }
        }

        SchemaColumn extent_c(constant::INDEX_EID_FIELD, 0, SchemaType::UINT64, 0, false);

        ExtentSchemaPtr primary_schema;
        if (primary_key.empty()) {
            std::vector<std::string> non_primary_key = { constant::INDEX_EID_FIELD };
            primary_schema = _schema->create_schema({}, { extent_c }, non_primary_key);
        } else {
            primary_schema = _schema->create_schema(primary_key, { extent_c }, primary_key);
        }

        auto it = std::ranges::find_if(roots, [](auto const &v) { return v.index_id == constant::INDEX_PRIMARY; });
        assert(it != roots.end());

        _primary_index = std::make_shared<BTree>(_table_dir / constant::INDEX_PRIMARY_FILE,
                                                 xid,
                                                 primary_schema,
                                                 it->extent_id);

        _primary_extent_id_f = primary_schema->get_field(constant::INDEX_EID_FIELD);
        _pkey_fields = primary_schema->get_fields();

        // deal with secondary indexes
        for (auto const& idx: secondary) {
            if (idx.state != static_cast<uint8_t>(sys_tbl::IndexNames::State::READY)) {
                continue;
            }
            assert(idx.id != constant::INDEX_PRIMARY);

            // work with the index
            std::vector<uint32_t> idx_cols;
            for (auto const &col: idx.columns) {
                idx_cols.push_back(col.position);
            }

            if (!idx_cols.empty()) {
                auto it = std::ranges::find_if(roots, [&](auto const &v) { return v.index_id == idx.id; });
                assert(it != roots.end());
                auto btree =  _create_index_root(idx.id, idx_cols, it->extent_id);
                assert(_secondary_indexes.find(idx.id) == _secondary_indexes.end());
                _secondary_indexes[idx.id] = {btree, idx_cols};
            }
        }
    }

    bool
    Table::has_primary()
    {
        return !_primary_key.empty();
    }

    uint64_t
    Table::primary_lookup(TuplePtr tuple)
    {
        // check if the table is vacant
        if (_primary_index == nullptr) {
            return constant::UNKNOWN_EXTENT; // indicates that data should be appended
        }

        // always returns an iterator to a leaf entry where the key *could* exist in the table
        auto &&i = _primary_index->lower_bound(tuple, true);
        if (i == _primary_index->end()) {
            // this can only happen if the table is empty, in which case we need to use a
            // special extent_id that indicates an append
            return constant::UNKNOWN_EXTENT;
        }

        // extract the extent_id and return it
        auto &&row = *i;
        return _primary_extent_id_f->get_uint64(&row);
    }

    ExtentSchemaPtr
    Table::extent_schema() const
    {
        return SchemaMgr::get_instance()->get_extent_schema(_db_id, _id, XidLsn(_xid));
    }

    SchemaPtr
    Table::schema(uint64_t extent_xid) const
    {
        return SchemaMgr::get_instance()->get_schema(_db_id, _id, XidLsn(extent_xid), XidLsn(_xid));
    }

    Table::Iterator
    Table::lower_bound(TuplePtr search_key, uint32_t index_id)
    {
        // check if the table is vacant
        if (_primary_index == nullptr) {
            return end(index_id);
        }

        // check for secondary index lookup
        if (index_id != constant::INDEX_PRIMARY) {
            auto const& [btree, cols] = _secondary_indexes.at(index_id);
            auto index_schema = _create_index_schema(_schema, cols);

            // find the extent that could contain the lower_bound() key
            auto &&i = btree->lower_bound(search_key);
            if (i == btree->end()) {
                return end(index_id);
            }
            return Iterator(this, btree, i, index_schema);
        }

        BTreePtr btree = index(index_id);

        // find the extent that could contain the lower_bound() key
        auto &&i = btree->lower_bound(search_key);
        if (i == btree->end()) {
            return end();
        }

        // read the extent and find the lower_bound() of the key within it
        auto page = _read_page_via_primary(i);

        // find the lower_bound() of the key within the data extent
        auto &&j = page->lower_bound(search_key, _schema);

        // note: the primary index indicates that there is a value >= the search_key in this page
        assert(j != page->end());

        return Iterator(this, _primary_index, i, std::move(page), j);
    }

    Table::Iterator
    Table::upper_bound(TuplePtr search_key, uint32_t index_id)
    {
        // check if the table is vacant
        if (_primary_index == nullptr) {
            return end(index_id);
        }

        if (index_id != constant::INDEX_PRIMARY) {
            auto const& [btree, cols] = _secondary_indexes.at(index_id);
            auto index_schema = _create_index_schema(_schema, cols);

            // find the extent that could contain the lower_bound() key
            auto &&i = btree->upper_bound(search_key);
            if (i == btree->end()) {
                return end(index_id);
            }
            return Iterator(this, btree, i, index_schema);
        }

        // find the extent that could contain the upper_bound() key
        auto &&i = _primary_index->upper_bound(search_key);
        if (i == _primary_index->end()) {
            return end();
        }

        // read the extent and find the upper_bound() of the key within it
        auto page = _read_page_via_primary(i);

        // find the upper_bound() of the key within the data extent
        auto &&j = page->upper_bound(search_key, _schema);

        // note: the primary index indicates that there is a value >= the search_key in this page
        assert(j != page->end());

        return Iterator(this, _primary_index, i, std::move(page), j);
    }

    Table::Iterator
    Table::inverse_lower_bound(TuplePtr search_key, uint32_t index_id)
    {
        // check if the table is vacant
        if (_primary_index == nullptr) {
            return end(index_id);
        }

        // check if it's a secondary index lookup
        if (index_id != constant::INDEX_PRIMARY) {
            auto const& [btree, cols] = _secondary_indexes.at(index_id);

            // find the extent that contains the row matching the inverse_lower_bound() key
            auto &&i = btree->inverse_lower_bound(search_key);

            if (i == btree->end()) {
                return end(index_id);
            }

            auto index_schema = _create_index_schema(_schema, cols);
            return Iterator(this, btree, i, index_schema);
        }

        // if the priamry index is empty, return end()
        if (_primary_index->empty()) {
            return end();
        }

        // find the extent that could contain the inverse_lower_bound() key
        auto &&i = _primary_index->lower_bound(search_key);
        if (i == _primary_index->end()) {
            --i;
        }

        // read the extent and find the inverse_lower_bound() of the key within it
        auto page = _read_page_via_primary(i);

        // find the inverse_lower_bound() of the key within the data extent
        auto &&j = page->inverse_lower_bound(search_key, _schema);

        // note: the index found this page, but if it's the first page in the table, the key may be
        //       less than the first entry, meaning no such inverse_lower_bound() exists
        if (j == page->end()) {
            return end();
        }

        return Iterator(this, _primary_index, i, std::move(page), j);
    }

    bool
    Table::empty() const
    {
        // check if the table is vacant
        if (_primary_index == nullptr) {
            return true;
        }

        // check if the table is constructed but empty
        if (_primary_index->begin() == _primary_index->end()) {
            return true;
        }

        return false;
    }

    Table::Iterator
    Table::begin(uint32_t index_id)
    {
        // check if the table is vacant
        if (_primary_index == nullptr) {
            return end(index_id);
        }

        if (index_id == constant::INDEX_PRIMARY) {
            // check if the table is empty
            auto &&index_i = _primary_index->begin();
            if (index_i == _primary_index->end()) {
                return end();
            }

            auto page = _read_page_via_primary(index_i);
            auto begin = page->begin();
            return Iterator(this, _primary_index, index_i, std::move(page), begin);
        } else {
            auto const& [btree, cols] = _secondary_indexes.at(index_id);
            auto index_schema = _create_index_schema(_schema, cols);

            // find the extent that could contain the lower_bound() key
            auto i = btree->begin();
            if (i == btree->end()) {
                return end(index_id);
            }
            return Iterator(this, btree, i, index_schema);
        }
    }

    std::pair<std::shared_ptr<Extent>, uint64_t>
    Table::read_extent_from_disk(uint64_t extent_id) const
    {
        // XXX: When an extent is asked from the page,
        // and if the extent's XID is different than the XID passed
        // update page cache XID with extent's XID. This can avoid
        // direct IO access from here
        ExtentId eid(extent_id);
        auto path = _table_dir / constant::DATA_FILE;
        path += fmt::format(".{:08x}", eid.file_id());

        auto data_file_handle = IOMgr::get_instance()->open(path, IOMgr::IO_MODE::READ, true);
        auto response = data_file_handle->read(eid.offset());
        if (response->data.empty()) {
            return {nullptr, 0};
        } else {
            auto extent = std::make_shared<Extent>(response->data);
            return {extent, response->next_offset};
        }
    }

    StorageCache::SafePagePtr
    Table::read_page(uint64_t extent_id) const
    {
        return _read_page(extent_id);
    }

    StorageCache::SafePagePtr
    Table::_read_page_via_primary(BTree::Iterator &pos) const
    {
        auto &&row = *pos;
        uint64_t extent_id = _primary_extent_id_f->get_uint64(&row);
        return _read_page(extent_id);
    }

    StorageCache::SafePagePtr
    Table::_read_page(uint64_t extent_id) const
    {
        return StorageCache::get_instance()->get(_table_dir / constant::DATA_FILE, extent_id, _xid);
    }

    BTreePtr 
    Table::_create_index_root(uint64_t index_id, const std::vector<uint32_t>& index_columns, uint64_t offset)
    {
        auto index_schema = _create_index_schema(_schema, index_columns);
        auto btree = std::make_shared<BTree>(_table_dir / fmt::format(constant::INDEX_FILE, index_id),
                _xid, index_schema,
                offset);
        return btree;
    }


    MutableTable::MutableTable(uint64_t db_id,
                               uint64_t table_id,
                               uint64_t access_xid,
                               uint64_t target_xid,
                               const std::filesystem::path &table_base,
                               const std::vector<std::string> &primary_key,
                               const std::vector<Index> &secondary,
                               const TableMetadata &metadata,
                               ExtentSchemaPtr schema,
                               bool for_gc)
    : _db_id(db_id),
      _id(table_id),
      _access_xid(access_xid),
      _target_xid(target_xid),
      _snapshot_xid(metadata.snapshot_xid),
      _primary_key(primary_key),
      _schema(schema),
      _stats(metadata.stats),
      _for_gc(for_gc)
    {
        // construct the table's data directory
        _table_dir = table_helpers::get_table_dir(table_base, db_id, table_id, metadata.snapshot_xid);
        _data_file = _table_dir / constant::DATA_FILE;

        // make sure that the table directory exists
        std::filesystem::create_directories(_table_dir);

        // store the roots schema / field
        _roots_schema = std::make_shared<ExtentSchema>(ROOTS_SCHEMA);
        _roots_root_f = _roots_schema->get_mutable_field("root");
        _roots_index_id_f = _roots_schema->get_mutable_field("index_id");

        auto roots = metadata.roots;
        if (roots.empty()) {
            if (std::filesystem::exists(_table_dir / constant::ROOTS_FILE)) {
                // read the roots from the look-aside file
                auto roots_path = std::filesystem::read_symlink(_table_dir / constant::ROOTS_FILE);
                auto root_handle = IOMgr::get_instance()->open(roots_path, IOMgr::IO_MODE::READ, true);
                auto response = root_handle->read(0);
                auto extent = std::make_shared<Extent>(response->data);
                for (auto &row : *extent) {
                    roots.push_back({_roots_index_id_f->get_uint64(&row), _roots_root_f->get_uint64(&row)});
                }
            } else {
                // fill the root offsets with UNKNOWN_EXTENT to indicate an empty tree
                roots.push_back({constant::INDEX_PRIMARY, constant::UNKNOWN_EXTENT});
            }
        }
        assert(!roots.empty());

        for (auto const& idx: secondary) {
            if (idx.state != static_cast<uint8_t>(sys_tbl::IndexNames::State::READY)) {
                continue;
            }
            assert(idx.id != constant::INDEX_PRIMARY);

            auto it = std::ranges::find_if(roots, [&](auto const &v) { return v.index_id == idx.id; });
            if (it == roots.end()) {
                // fill the root offsets with UNKNOWN_EXTENT to indicate an empty tree
                roots.emplace_back(idx.id, constant::UNKNOWN_EXTENT);
            }
        }

        // construct the primary index btree
        SchemaColumn extent_c(constant::INDEX_EID_FIELD, 0, SchemaType::UINT64, 0, false);
        SchemaColumn row_c(constant::INDEX_RID_FIELD, 1, SchemaType::UINT32, 0, false);

        
        ExtentSchemaPtr primary_schema;
        if (primary_key.empty()) {
            std::vector<std::string> non_primary_key = { constant::INDEX_EID_FIELD };
            primary_schema = _schema->create_schema({}, { extent_c }, non_primary_key);

            _primary_index = std::make_shared<MutableBTree>(_table_dir / constant::INDEX_PRIMARY_FILE,
                                                            non_primary_key,
                                                            primary_schema,
                                                            _target_xid);
        } else {
            primary_schema = _schema->create_schema(primary_key, { extent_c }, primary_key);

            _primary_index = std::make_shared<MutableBTree>(_table_dir / constant::INDEX_PRIMARY_FILE,
                                                            primary_key,
                                                            primary_schema,
                                                            _target_xid);
        }



        // find primary index root
        auto it = std::ranges::find_if(roots, [](auto const &v) { return v.index_id == constant::INDEX_PRIMARY; });
        assert(it != roots.end());

        // initialize the primary index
        if (it->extent_id != constant::UNKNOWN_EXTENT) {
            _primary_index->init(it->extent_id);
        } else {
            _primary_index->init_empty();
        }
        _use_empty = _primary_index->empty();
        _primary_extent_id_f = primary_schema->get_field(constant::INDEX_EID_FIELD);

        // deal with secondary indexes
        for (auto const& idx: secondary) {
            if (idx.state != static_cast<uint8_t>(sys_tbl::IndexNames::State::READY)) {
                continue;
            }
            assert(idx.id != constant::INDEX_PRIMARY);
            // work with the index
            std::vector<uint32_t> idx_cols;
            for (auto const &col: idx.columns) {
                idx_cols.push_back(col.position);
            }
            if (!idx_cols.empty()) {

                auto btree = create_index_root(idx.id, idx_cols);

                auto it = std::ranges::find_if(roots, [&](auto const &v) { return v.index_id == idx.id; });
                assert(it != roots.end());

                if (it->extent_id != constant::UNKNOWN_EXTENT) {
                    btree->init(it->extent_id);
                } else {
                    btree->init_empty();
                }
                assert(_secondary_indexes.find(idx.id) == _secondary_indexes.end());
                _secondary_indexes[idx.id] = {btree, idx_cols};
            }
        }
    }

    void
    MutableTable::insert(TuplePtr value,
                         uint64_t extent_id)
    {
        if (extent_id == constant::UNKNOWN_EXTENT) {
            if (_primary_key.empty()) {
                _insert_append(value);
            } else {
                _insert_by_lookup(value);
            }
        } else {
            _insert_direct(value, extent_id);
        }

        // update the stats
        if (_id > constant::MAX_SYSTEM_TABLE_ID) {
            ++_stats.row_count;
        }
    }

    void
    MutableTable::upsert(TuplePtr value,
                         uint64_t extent_id)
    {
        bool did_insert = false;

        if (extent_id == constant::UNKNOWN_EXTENT) {
            if (_primary_key.empty()) {
                // with no primary key, we just resort to a separate removal and insert
                _remove_by_scan(value);
                _insert_append(value);
            } else {
                did_insert = _upsert_by_lookup(value);
            }
        } else {
            did_insert = _upsert_direct(value, extent_id);
        }

        // update the stats
        if (did_insert && _id > constant::MAX_SYSTEM_TABLE_ID) {
            ++_stats.row_count;
        }
    }

    void
    MutableTable::remove(TuplePtr key,
                         uint64_t extent_id)
    {
        // perform the removal
        if (extent_id == constant::UNKNOWN_EXTENT) {
            if (_primary_key.empty()) {
                // note: in this case the key will actually be the full row
                _remove_by_scan(key);
            } else {
                _remove_by_lookup(key);
            }
        } else {
            _remove_direct(key, extent_id);
        }

        // update the stats
        if (_id > constant::MAX_SYSTEM_TABLE_ID) {
            --_stats.row_count;
            if (_stats.row_count == 0) {
                // we've emptied the table, need to switch to using the _empty_page
                // note: this is because the pages no longer have on-disk locations that can be
                //       stored into the primary index
                _use_empty = true;
            }
        }
    }

    void
    MutableTable::update(TuplePtr value,
                         uint64_t extent_id)
    {
        if (extent_id == constant::UNKNOWN_EXTENT) {
            // note: cannot perform an update() with no primary key, should be split into a remove() and insert()
            assert(!_primary_key.empty());

            _update_by_lookup(value);
        } else {
            _update_direct(value, extent_id);
        }

        // note: no change in the stats.row_count
    }

    void
    MutableTable::truncate()
    {
        // remove any dirty cached pages for this table since they don't need to be written
        StorageCache::get_instance()->drop_for_truncate(_data_file);

        // clear the indexes
        TableMetadata metadata;
        metadata.snapshot_xid = _snapshot_xid;
        metadata.roots = {{ constant::INDEX_PRIMARY, constant::UNKNOWN_EXTENT }};
        _primary_index->truncate();

        for (auto& [index_id, idx]: _secondary_indexes) {
            idx.first->truncate();
            metadata.roots.emplace_back(index_id, constant::UNKNOWN_EXTENT);
        }

        // update the roots and stats
        sys_tbl_mgr::Client::get_instance()->update_roots(_db_id, _id, _target_xid, metadata);
    }

    StorageCache::SafePagePtr
    MutableTable::read_page(uint64_t extent_id)
    {
        return StorageCache::get_instance()->get(_data_file, extent_id,
                                                 _access_xid, _target_xid, false,
                                                 [this](StorageCache::PagePtr page) {
                                                     return _flush_handler(page);
                                                 });
    }

    bool
    MutableTable::_flush_handler(StorageCache::PagePtr page)
    {
        // first invalidate the index entries based on the original page
        _invalidate_indexes(page);

        // then flush the generated page and populate the index entries based on the new pages
        _flush_and_populate_indexes(page.get());

        // return success
        return true;
    }

    void
    MutableTable::_invalidate_indexes(StorageCache::PagePtr page)
    {
        uint64_t old_eid = page->key().first;

        // if there was no previous page, nothing to invalidate
        if (old_eid == constant::UNKNOWN_EXTENT) {
            return;
        }

        // get the original page to use for index updates
        auto orig_page = StorageCache::get_instance()->get(_data_file, old_eid, _access_xid);

        // INVALIDATE PRIMARY INDEX
        TuplePtr pkey;
        if (_primary_key.empty()) {
            // no primary key, so use the old extent ID as the primary key
            auto pkey_fields = std::make_shared<FieldArray>(1);
            pkey_fields->at(0) = std::make_shared<ConstTypeField<uint64_t>>(orig_page->key().first);
            pkey = std::make_shared<FieldTuple>(pkey_fields, nullptr);
        } else {
            // has a primary key, get the last row of the original page for the primary index
            auto pkey_fields = _schema->get_fields(_primary_key);
            auto &&row = *orig_page->last();
            pkey = std::make_shared<FieldTuple>(pkey_fields, &row);
        }

        // remove the old primary index entry
        _primary_index->remove(pkey);

        // INVALIDATE SECONDARY INDEXES

        for (auto const& [index_id, idx]: _secondary_indexes) {
            indexer_helpers::invalidate_index_for_page(orig_page->key().first, orig_page, idx.first, idx.second, _schema);
        }
    }

    void
    MutableTable::_flush_and_populate_indexes(StorageCache::PagePtr::element_type* page)
    {
        uint64_t old_eid = page->key().first;

        // if the page is now empty, do nothing since the indexes will be flushed as empty
        if (page->empty()) {
            return;
        }

        // note: this will be empty if there is no primary key, but okay because it won't be used
        auto pkey_fields = _schema->get_fields(_primary_key);

        // retrieve the extent offsets of the new page
        ExtentHeader header(ExtentType(), _target_xid, _schema->row_size(), _schema->field_types(), old_eid);
        auto &&offsets = page->flush(header);

        auto value_fields = std::make_shared<FieldArray>(1);
        for (auto extent_id : offsets) {
            auto new_page = StorageCache::get_instance()->get(_data_file, extent_id, _target_xid);

            // POPULATE PRIMARY INDEX
            TuplePtr pkey;

            // create the new primary index entry
            (*value_fields)[0] = std::make_shared<ConstTypeField<uint64_t>>(extent_id);
            if (_primary_key.empty()) {
                // no primary key, use the extent ID itself as the primary key
                pkey = std::make_shared<FieldTuple>(value_fields, nullptr);
            } else {
                // has a primary key, use the primary key fields
                auto &&row = *new_page->last();
                pkey = std::make_shared<KeyValueTuple>(pkey_fields, value_fields, &row);
            }

            // insert the new primary index entry
            _primary_index->insert(pkey);

            // POPULATE SECONDARY INDEXES
            for (auto const& [index_id, idx]: _secondary_indexes) {
                indexer_helpers::populate_index_for_page(extent_id, new_page, idx.first, idx.second, _schema);
            }
        }
    }

    TableMetadata
    MutableTable::finalize()
    {
        // in the case of having an (initially) empty table, there are no invalidations... we can
        // flush the single Page and update the indexes
        if (_empty_page) {
            _flush_and_populate_indexes(_empty_page->ptr());
            // this will release the page to the cache
            _empty_page.reset();
        }

        // flush the dirty data pages of the table to disk
        auto end_offset = StorageCache::get_instance()->flush(_data_file);

        // now flush the indexes, capturing the roots
        TableMetadata metadata;
        metadata.roots.push_back({constant::INDEX_PRIMARY, _primary_index->finalize()});

        // now flush the indexes, capturing the roots
        for (auto secondary : _secondary_indexes) {
            metadata.roots.emplace_back(secondary.first, secondary.second.first->finalize());
        }

        metadata.stats = _stats;
        metadata.snapshot_xid = _snapshot_xid;

        // Store file end offset for xid
        // to be used later to catch-up index if needed
        metadata.stats.end_offset = end_offset;

        // store the roots into a look-aside root file
        // XXX maybe we only need to do this for system tables?  or even just the table_roots table?
        auto extent = std::make_shared<Extent>(ExtentType(), _target_xid, _roots_schema->row_size(), _roots_schema->field_types());
        for (auto root : metadata.roots) {
            auto &&row = extent->append();
            _roots_root_f->set_uint64(&row, root.extent_id);
            _roots_index_id_f->set_uint64(&row, root.index_id);
        }
        auto filename = fmt::format(constant::ROOTS_XID_FILE, _target_xid);
        auto root_handle = IOMgr::get_instance()->open(_table_dir / filename,
                                                       IOMgr::IO_MODE::APPEND, true);

        // flush and wait for completion
        extent->async_flush(root_handle).wait();
        root_handle->sync();

        // swap the symlink
        std::filesystem::create_symlink(_table_dir / filename,
                                        _table_dir / constant::ROOTS_TMP_FILE);
        std::filesystem::rename(_table_dir / constant::ROOTS_TMP_FILE,
                                _table_dir / constant::ROOTS_FILE);

        return metadata;
    }

    MutableBTreePtr 
    MutableTable::create_index_root(uint64_t index_id, const std::vector<uint32_t>& index_columns)
    {
        // get the column names in the order they appear in the index
        auto &&col_names = _schema->get_column_names(index_columns);

        SchemaColumn extent_c(constant::INDEX_EID_FIELD, 0, SchemaType::UINT64, 0, false);
        SchemaColumn row_c(constant::INDEX_RID_FIELD, 1, SchemaType::UINT32, 0, false);

        auto key = col_names;
        key.push_back(constant::INDEX_EID_FIELD);
        key.push_back(constant::INDEX_RID_FIELD);

        auto index_schema = _schema->create_schema(col_names, { extent_c, row_c }, key);

        auto btree = std::make_shared<MutableBTree>(_table_dir / fmt::format(constant::INDEX_FILE, index_id),
                key, index_schema,
                _target_xid);
        return btree;
    }

    void
    MutableTable::_insert_direct(TuplePtr value,
                                 uint64_t extent_id)
    {
        // get the page from the cache
        auto page = StorageCache::get_instance()->get(_data_file, extent_id, _access_xid, _target_xid, false, 
                                                      [this](StorageCache::PagePtr page) { return _flush_handler(page); } );

        // check if we need to convert the page contents to a new schema
        _check_convert_page(page);

        // add the row to the page
        page->insert(value, _schema);
    }

    void
    MutableTable::_insert_empty(TuplePtr value)
    {
        // get the page from the cache if we don't have one
        if (!_empty_page) {
            _empty_page = std::make_unique<StorageCache::SafePagePtr>(
                    StorageCache::get_instance()->get(_data_file, constant::UNKNOWN_EXTENT, _access_xid, _target_xid));
        }

        // add the row to the page
        (*_empty_page)->insert(value, _schema);
    }

    void
    MutableTable::_append_empty(TuplePtr value)
    {
        // get the page from the cache if we don't have one
        if (!_empty_page) {
            _empty_page = std::make_unique<StorageCache::SafePagePtr>(
                    StorageCache::get_instance()->get(_data_file, constant::UNKNOWN_EXTENT, _access_xid, _target_xid));
        }

        // add the row to the page
        (*_empty_page)->append(value, _schema);
    }

    void
    MutableTable::_insert_append(TuplePtr value)
    {
        // if the primary_lookup tree is empty, we will maintain a single page of data that we will
        // keep against the table and use for all operations.
        if (_use_empty) {
            _append_empty(value);
            return;
        }

        // note: in this case there is no explicit primary key, so we need to append the row to the
        //       end of the file
        auto leaf_i = _primary_index->last();
        auto row = *leaf_i;
        uint64_t extent_id = _primary_extent_id_f->get_uint64(&row);

        // get the page from the cache
        auto page = StorageCache::get_instance()->get(_data_file, extent_id, _access_xid, _target_xid,
                false,
                [this](StorageCache::PagePtr page) { return _flush_handler(page); } );

        // check if we need to convert the page contents to a new schema
        _check_convert_page(page);

        // append the value to the extent
        page->append(value, _schema);
    }

    void
    MutableTable::_insert_by_lookup(TuplePtr value)
    {
        assert(!_primary_key.empty());

        // if the primary_lookup tree is empty, we will maintain a single page of data that we will
        // keep against the table and use for all operations.
        if (_use_empty) {
            _insert_empty(value);
            return;
        }

        // we didn't receive an extent_id, so we need to look up the extent from the primary index
        auto search_key = _schema->tuple_subset(value, _primary_key);
        auto i = _primary_index->lower_bound(search_key, true);
        auto &&row = *i;

        // if the primary index is not empty, get the target extent
        uint64_t extent_id = _primary_extent_id_f->get_uint64(&row);

        // then we can do a direct insert
        _insert_direct(value, extent_id);
    }

    bool
    MutableTable::_upsert_direct(TuplePtr value, uint64_t extent_id)
    {
        // get the page from the cache
        auto page = StorageCache::get_instance()->get(_data_file, extent_id, _access_xid, _target_xid, false,
                [this](StorageCache::PagePtr page) { return _flush_handler(page); } );

        // check if we need to convert the page contents to a new schema
        _check_convert_page(page);

        // add the row to the page
        bool did_insert = page->upsert(value, _schema);

        return did_insert;
    }

    bool
    MutableTable::_upsert_empty(TuplePtr value)
    {
        // get the page from the cache if we don't have one
        if (!_empty_page) {
            _empty_page = std::make_unique<StorageCache::SafePagePtr>( 
                    StorageCache::get_instance()->get(_data_file, constant::UNKNOWN_EXTENT, _access_xid, _target_xid));
        }

        // add the row to the page
        return (*_empty_page)->upsert(value, _schema);
    }

    bool
    MutableTable::_upsert_by_lookup(TuplePtr value)
    {
        assert(!_primary_key.empty());

        // if the primary_lookup tree is empty, we will maintain a single page of data that we will
        // keep against the table and use for all operations.
        if (_use_empty) {
            return _upsert_empty(value);
        }

        // we didn't receive an extent_id, so we need to look up the extent from the primary index
        auto search_key = _schema->tuple_subset(value, _primary_key);
        auto i = _primary_index->lower_bound(search_key, true);
        auto &&row = *i;

        // if the primary index is not empty, get the target extent
        uint64_t extent_id = _primary_extent_id_f->get_uint64(&row);

        // then we can do a direct insert
        return _upsert_direct(value, extent_id);
    }

    void
    MutableTable::_remove_direct(TuplePtr value, uint64_t extent_id)
    {
        // get the page from the cache
        auto page = StorageCache::get_instance()->get(_data_file, extent_id, _access_xid, _target_xid, false,
                [this](StorageCache::PagePtr page) { return _flush_handler(page); } );

        // check if we need to convert the page contents to a new schema
        _check_convert_page(page);

        // remove the row from the page
        // note: this can only be used when a primary key is present, otherwise use _remove_by_scan()
        page->remove(value, _schema);
    }

    void
    MutableTable::_remove_empty(TuplePtr value)
    {
        // note: if we are performing a remove, there must be a page already
        CHECK(_empty_page != nullptr);

        // add the row to the page
        (*_empty_page)->remove(value, _schema);
    }

    void
    MutableTable::_remove_by_lookup(TuplePtr key)
    {
        // if the primary_lookup tree is empty, we will maintain a single page of data that we will
        // keep against the table and use for all operations.
        if (_use_empty) {
            _remove_empty(key);
            return;
        }

        // we didn't receive an extent_id, but we have a primary index, so perform a lookup of the key
        auto i = _primary_index->lower_bound(key, true);
        auto &&row = *i;

        // if the primary index is not empty, get the target extent
        uint64_t extent_id = _primary_extent_id_f->get_uint64(&row);

        // then we can do a direct removal
        _remove_direct(key, extent_id);
    }

    void
    MutableTable::_remove_by_scan(TuplePtr value)
    {
        // we didn't receive an extent_id, and there is no primary index, so we must scan the
        // file to find the row to remove
        // note: in this case, it must be a full row match
        // note: it would be much more performant to perform all of the scan-based removals in
        //       an XID at once as a batch, since the table is likely to be much larger than the
        //       set of removals

        // if the primary_lookup tree is empty, we will maintain a single page of data that we will
        // keep against the table and use for all operations.
        if (_use_empty) {
            // note: if we are performing a remove, there must be a page already
            CHECK(_empty_page != nullptr);

            // add the row to the page
            (*_empty_page)->try_remove_by_scan(value, _schema);
            return;
        }

        // scan the index
        bool found = false;
        auto i = _primary_index->begin();
        while (!found && i != _primary_index->end()) {
            auto &&row = *i;
            // scan each extent, looking for a match
            uint64_t extent_id = _primary_extent_id_f->get_uint64(&row);

            auto page = StorageCache::get_instance()->get(_data_file, extent_id, _access_xid, _target_xid, false,
                [this](StorageCache::PagePtr page) { return _flush_handler(page); } );

            // check if we need to convert the page contents to a new schema
            _check_convert_page(page);

            // pass the value tuple and the schema down to the page
            found = page->try_remove_by_scan(value, _schema);

            if (!found) {
                ++i;
            }
        }
    }

    void
    MutableTable::_update_direct(TuplePtr value, uint64_t extent_id)
    {
        // get the page from the cache
        auto page = StorageCache::get_instance()->get(_data_file, extent_id, _access_xid, _target_xid,
                false,
                [this](StorageCache::PagePtr page) { return _flush_handler(page); } );

        // check if we need to convert the page contents to a new schema
        _check_convert_page(page);

        // update the row in the page
        // note: this can only be used when a primary key is present, otherwise update should have been split
        page->update(value, _schema);
    }

    void
    MutableTable::_update_empty(TuplePtr value)
    {
        // get the page from the cache if we don't have one
        if (!_empty_page) {
            _empty_page = std::make_unique<StorageCache::SafePagePtr>(
                    StorageCache::get_instance()->get(_data_file, constant::UNKNOWN_EXTENT, _access_xid, _target_xid));
        }

        // add the row to the page
        (*_empty_page)->update(value, _schema);
    }

    void
    MutableTable::_update_by_lookup(TuplePtr value)
    {
        assert(!_primary_key.empty());

        // if the primary_lookup tree is empty, we will maintain a single page of data that we will
        // keep against the table and use for all operations.
        if (_use_empty) {
            _update_empty(value);
            return;
        }

        // we didn't receive an extent_id, but we have a primary index, so perform a lookup of the key
        auto search_key = _schema->tuple_subset(value, _primary_key);
        auto i = _primary_index->lower_bound(search_key, true);
        auto &&row = *i;

        // if the primary index is not empty, get the target extent
        uint64_t extent_id = _primary_extent_id_f->get_uint64(&row);

        // then we can do a direct update
        _update_direct(value, extent_id);
    }

    void
    MutableTable::_check_convert_page(StorageCache::SafePagePtr &page)
    {
#if ENABLE_SCHEMA_MUTATES
        auto header = page->header();
        XidLsn access_xid(header.xid);
        XidLsn target_xid(_target_xid);

        auto schema = SchemaMgr::get_instance()->get_schema(_db_id, _id, access_xid, target_xid);
        auto virtual_schema = std::dynamic_pointer_cast<VirtualSchema>(schema);

        // the schema has changed
        if (virtual_schema) {
            page->convert(virtual_schema, _schema, _target_xid);
        }
#else
        // don't need to convert pages if we aren't supporting schema layout mutations
#endif
    }

    void Table::Iterator::Primary::next()
    {
        if (!_end) {
            _end = _page->end();
        }

        // move to the next row in the data extent
        ++_page_i;
        if (_page_i != *_end) {
            return;
        }

        _end = {};

        // no more rows in the extent, so need to move to the next data extent
        ++_btree_i;
        if (_btree_i == _btree->end()) {
            return;
        }

        // retrieve the data extent
        _page = _table->_read_page_via_primary(_btree_i);
        _page_i = _page->begin();
    }

    void Table::Iterator::Primary::prev()
    {
        // check if this is end()
        if (_page.empty()) {
            // move to the final page referenced by the primary index
            assert(_btree_i == _btree->end());
            --_btree_i;

            // read the page and reference the end() of that page
            _page = _table->_read_page_via_primary(_btree_i);
            _page_i = _page->end();
        }

        // check if we are on the first row
        if (_page_i == _page->begin()) {
            // need to move to the previous page
            --_btree_i;

            // read the page and reference the end() of that page
            _page = _table->_read_page_via_primary(_btree_i);
            _page_i = _page->end();
        }

        // move to the previous row
        --_page_i;
    }

    Table::Iterator::Secondary::Secondary(const Table *table,
            BTreePtr btree, const BTree::Iterator &btree_i,
            ExtentSchemaPtr schema )
        : 
            Tracker{table, btree, btree_i},
            _cache_size{Json::get_or<uint64_t>(Properties::get(Properties::STORAGE_CONFIG), "page_cache_size", 16384)},
            _eid_buffer{_cache_size/2}
    {
        DCHECK(_cache_size);

        _extent_id_f = schema->get_field(constant::INDEX_EID_FIELD);
        _row_id_f = schema->get_field(constant::INDEX_RID_FIELD);
        if (_btree_i != btree->end()) {
            update_page();
        }
    }

    void Table::Iterator::Secondary::next()
    {
        ++_btree_i;
        if (_btree_i == _btree->end()) {
            _page_map.clear();
            return;
        }
        update_page();
    }
    void Table::Iterator::Secondary::prev()
    {
        --_btree_i;
        update_page();
    }

    void Table::Iterator::Secondary::update_page()
    {
        DCHECK(_btree_i != _btree->end());
        DCHECK(_page_map.size() <= _cache_size);
        DCHECK(_eid_buffer.size() <= _cache_size);
        auto &&row = *_btree_i;
        uint64_t eid = _extent_id_f->get_uint64(&row);

        if (_page_map.empty() || _extent_id != eid) {
            _extent_id = eid;
            auto it = _page_map.find(eid);
            if (it == _page_map.end()) {
                TIME_TRACE_SCOPED(time_trace::traces, table_iterator_read_page);

                // check if need to free space in the page map
                if (_page_map.size() == _cache_size) {
                    DCHECK(!_eid_buffer.empty());
                    auto cached_eid = _eid_buffer.next();
                    auto erase_it = _page_map.find(cached_eid);
                    DCHECK(erase_it != _page_map.end());
                    _page_map.erase(erase_it);
                }

                auto page = _table->_read_page(_extent_id);
                //TODO: is this correct?
                DCHECK(page->extent_count() == 1);

                auto begin_it = page->begin();
                PageMapItem pi{std::move(page), std::move(begin_it)};
                auto [inserted_it, _] = _page_map.try_emplace(_extent_id, std::move(pi));
                _eid_buffer.put(_extent_id);
                _page_i_begin = inserted_it->second.it_begin;
            } else {
                _page_i_begin = it->second.it_begin;
            }
        }

        auto row_id = _row_id_f->get_uint32(&row);
        _page_i = _page_i_begin;
        _page_i += row_id;
    }

    const Extent::Row& Table::Iterator::Secondary::row() const
    {
        return *_page_i;
    }

    Table::Iterator::Iterator(const Table *table, uint32_t index_id)
    { 
        if (index_id == constant::INDEX_PRIMARY) {
            _tracker.emplace<Primary>(table, table->_primary_index, 
                    table->_primary_index->end(), 
                    StorageCache::SafePagePtr{}, 
                    StorageCache::Page::Iterator{});
        } else {
            auto const& [btree, cols] = table->_secondary_indexes.at(index_id);
            auto index_schema = _create_index_schema(table->_schema, cols);
            _tracker.emplace<Secondary>(table, btree, 
                    btree->end(), index_schema );
        }
    }
}
