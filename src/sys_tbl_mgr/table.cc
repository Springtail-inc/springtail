#include <common/constants.hh>
#include <memory>
#include <span>
#include <sys_tbl_mgr/client.hh>
#include <sys_tbl_mgr/system_tables.hh>
#include <sys_tbl_mgr/table.hh>
#include <sys_tbl_mgr/table_mgr.hh>
#include <write_cache/write_cache_client.hh>

#define SPRINGTAIL_INCLUDE_TIME_TRACES 1
#include <common/time_trace.hh>


namespace springtail
{
    struct trace
    {
        TIME_TRACE(one);
        std::string _n;

        trace(std::string n) : _n{std::move(n)} {
            TIME_TRACE_START(one);
        }

        ~trace() {
            TIME_TRACE_STOP(one);
            TIME_TRACESET_UPDATE(traces, _n, one);
        }
    };
}


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

    namespace {
        const static std::vector<SchemaColumn> ROOTS_SCHEMA = {
            { "root", 1, SchemaType::UINT64, 20, true },
            { "index_id", 2, SchemaType::UINT64, 20, false },
        };

        std::vector<std::string> 
        _get_column_names(ExtentSchemaPtr schema, const std::vector<uint32_t>& col_position)
        {
            std::vector<std::string> col_names;
            auto column_order = schema->column_order();
            for (auto position: col_position) {
                // the index positions start with one
                assert(position <= column_order.size());
                col_names.emplace_back(std::move(column_order[position-1]));
            }
            return col_names;
        }

        std::shared_ptr<ExtentSchema> 
        _create_index_schema(ExtentSchemaPtr schema, const std::vector<uint32_t>& index_columns)
        {

            // get the column names in the order they appear in the index
            std::vector<std::string> col_names = _get_column_names(schema, index_columns);

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
                            {_roots_index_id_f->get_uint64(row),
                            _roots_root_f->get_uint64(row)});
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

    std::vector<std::string> 
    Table::get_column_names(const std::vector<uint32_t>& col_position)
    {
        return _get_column_names(_schema, col_position);
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
        return _primary_extent_id_f->get_uint64(*i);
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
            auto index_schema = _create_index_schema(_schema, cols);

            // find the extent that could contain the lower_bound() key
            auto &&i = btree->lower_bound(search_key);

            // for secondary indexes, it's a row-based index, so finding begin() means there's no
            // row before the search key
            if (i == btree->begin()) {
                return end(index_id);
            }

            // for secondary indexes, always decrement since it's a row-based index
            --i;

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

    StorageCache::SafePagePtr
    Table::read_page(uint64_t extent_id) const
    {
        return _read_page(extent_id);
    }

    StorageCache::SafePagePtr
    Table::_read_page_via_primary(BTree::Iterator &pos) const
    {
        uint64_t extent_id = _primary_extent_id_f->get_uint64(*pos);
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
                    roots.push_back({_roots_index_id_f->get_uint64(row), _roots_root_f->get_uint64(row)});
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



        // find primary index
        auto it = std::ranges::find_if(roots, [](auto const &v) { return v.index_id == constant::INDEX_PRIMARY; });
        assert(it != roots.end());

        if (it->extent_id != constant::UNKNOWN_EXTENT) {
            _primary_index->init(it->extent_id);
        } else {
            _primary_index->init_empty();
        }

        _primary_lookup = std::make_shared<BTree>(_table_dir / constant::INDEX_PRIMARY_FILE,
                                                  access_xid,
                                                  primary_schema,
                                                  it->extent_id);

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
                         uint64_t xid,
                         uint64_t extent_id)
    {
        if (extent_id == constant::UNKNOWN_EXTENT) {
            if (_primary_key.empty()) {
                _insert_append(value, xid);
            } else {
                _insert_by_lookup(value, xid);
            }
        } else {
            _insert_direct(value, xid, extent_id);
        }

        // update the stats
        if (_id > constant::MAX_SYSTEM_TABLE_ID) {
            ++_stats.row_count;
        }
    }

    void
    MutableTable::upsert(TuplePtr value,
                         uint64_t xid,
                         uint64_t extent_id)
    {
        bool did_insert = false;

        if (extent_id == constant::UNKNOWN_EXTENT) {
            if (_primary_key.empty()) {
                // with no primary key, we just resort to a separate removal and insert
                _remove_by_scan(value, xid);
                _insert_append(value, xid);
            } else {
                did_insert = _upsert_by_lookup(value, xid);
            }
        } else {
            did_insert = _upsert_direct(value, xid, extent_id);
        }

        // update the stats
        if (did_insert && _id > constant::MAX_SYSTEM_TABLE_ID) {
            ++_stats.row_count;
        }
    }

    void
    MutableTable::remove(TuplePtr key,
                         uint64_t xid,
                         uint64_t extent_id)
    {
        // perform the removal
        if (extent_id == constant::UNKNOWN_EXTENT) {
            if (_primary_key.empty()) {
                // note: in this case the key will actually be the full row
                _remove_by_scan(key, xid);
            } else {
                _remove_by_lookup(key, xid);
            }
        } else {
            _remove_direct(key, xid, extent_id);
        }

        // update the stats
        if (_id > constant::MAX_SYSTEM_TABLE_ID) {
            --_stats.row_count;
        }
    }

    void
    MutableTable::update(TuplePtr value,
                         uint64_t xid,
                         uint64_t extent_id)
    {
        if (extent_id == constant::UNKNOWN_EXTENT) {
            // note: cannot perform an update() with no primary key, should be split into a remove() and insert()
            assert(!_primary_key.empty());

            _update_by_lookup(value, xid);
        } else {
            _update_direct(value, xid, extent_id);
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
        uint64_t old_eid = page->key().second;

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
            pkey_fields->at(0) = std::make_shared<ConstTypeField<uint64_t>>(orig_page->key().second);
            pkey = std::make_shared<FieldTuple>(pkey_fields, nullptr);
        } else {
            // has a primary key, get the last row of the original page for the primary index
            auto pkey_fields = _schema->get_fields(_primary_key);
            pkey = std::make_shared<FieldTuple>(pkey_fields, *orig_page->last());
        }

        // remove the old primary index entry
        _primary_index->remove(pkey);

        // INVALIDATE SECONDARY INDEXES

        FieldArrayPtr value_fields = std::make_shared<FieldArray>(2);
        value_fields->at(0) = std::make_shared<ConstTypeField<uint64_t>>(orig_page->key().second);

        // go through each row and pass the relevant key to each of the secondary indexes for removal
        uint32_t row_id = 0;
        for (auto &&row : *orig_page) {
            value_fields->at(1) = std::make_shared<ConstTypeField<uint32_t>>(row_id);

            for (auto const& [index_id, idx]: _secondary_indexes) {
                auto &secondary = idx.first;
                auto keys = get_column_names(idx.second);
                auto key_fields = _schema->get_fields(keys);
                auto &&skey = std::make_shared<KeyValueTuple>(key_fields, value_fields, row);
                secondary->remove(skey);
            }
            ++row_id;
        }
    }

    void
    MutableTable::_flush_and_populate_indexes(StorageCache::PagePtr::element_type* page)
    {
        uint64_t old_eid = page->key().second;

        // if the page is now empty, do nothing since the indexes will be flushed as empty
        if (page->empty()) {
            return;
        }

        // note: this will be empty if there is no primary key, but okay because it won't be used
        auto pkey_fields = _schema->get_fields(_primary_key);

        // retrieve the extent offsets of the new page
        ExtentHeader header(ExtentType(), _target_xid, _schema->row_size(), old_eid);
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
                pkey = std::make_shared<KeyValueTuple>(pkey_fields, value_fields, *new_page->last());
            }

            // insert the new primary index entry
            _primary_index->insert(pkey);

            // POPULATE SECONDARY INDEXES

            // go through each row and pass the relevant key to each of the secondary indexes for insertion
            value_fields->resize(2);
            uint32_t row_id = 0;
            for (auto &row : *new_page) {
                (*value_fields)[1] = std::make_shared<ConstTypeField<uint32_t>>(row_id);

                for (auto const& [index_id, idx]: _secondary_indexes) {
                    auto &secondary = idx.first;
                    auto keys = get_column_names(idx.second);
                    auto key_fields = _schema->get_fields(keys);
                    auto &&svalue = std::make_shared<KeyValueTuple>(key_fields, value_fields, row);
                    // note: uncomment if you need to debug the entries being populated into the secondary indexes
                    // SPDLOG_DEBUG_MODULE(LOG_BTREE, "Secondary populate {}", svalue->to_string());
                    secondary->insert(svalue);
                }
                ++row_id;
            }

            SPDLOG_DEBUG_MODULE(LOG_BTREE, "Populated {} secondary rows", row_id);
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
        StorageCache::get_instance()->flush(_data_file);

        // now flush the indexes, capturing the roots
        TableMetadata metadata;
        metadata.roots.push_back({constant::INDEX_PRIMARY, _primary_index->finalize()});

        // now flush the indexes, capturing the roots
        for (auto secondary : _secondary_indexes) {
            metadata.roots.emplace_back(secondary.first, secondary.second.first->finalize());
        }
        metadata.stats = _stats;
        metadata.snapshot_xid = _snapshot_xid;

        // store the roots into a look-aside root file
        // XXX maybe we only need to do this for system tables?  or even just the table_roots table?
        auto extent = std::make_shared<Extent>(ExtentType(), _target_xid, _roots_schema->row_size());
        for (auto root : metadata.roots) {
            auto &&row = extent->append();
            _roots_root_f->set_uint64(row, root.extent_id);
            _roots_index_id_f->set_uint64(row, root.index_id);
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

    std::vector<std::string> 
    MutableTable::get_column_names(const std::vector<uint32_t>& col_position)
    {
        return _get_column_names(_schema, col_position);
    }

    MutableBTreePtr 
    MutableTable::create_index_root(uint64_t index_id, const std::vector<uint32_t>& index_columns)
    {
        // get the column names in the order they appear in the index
        std::vector<std::string> col_names = _get_column_names(_schema, index_columns);

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
                                 uint64_t xid,
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
    MutableTable::_insert_empty(TuplePtr value,
                                uint64_t xid)
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
    MutableTable::_append_empty(TuplePtr value,
                                uint64_t xid)
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
    MutableTable::_insert_append(TuplePtr value,
                                 uint64_t xid)
    {
        // if the primary_lookup tree is empty, we will maintain a single page of data that we will
        // keep against the table and use for all operations.
        if (_primary_lookup->empty()) {
            _append_empty(value, xid);
            return;
        }

        // note: in this case there is no explicit primary key, so we need to append the row to the
        //       end of the file
        auto pos = --(_primary_lookup->end());
        uint64_t extent_id = _primary_extent_id_f->get_uint64(*pos);

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
    MutableTable::_insert_by_lookup(TuplePtr value,
                                    uint64_t xid)
    {
        assert(!_primary_key.empty());

        // if the primary_lookup tree is empty, we will maintain a single page of data that we will
        // keep against the table and use for all operations.
        if (_primary_lookup->empty()) {
            _insert_empty(value, xid);
            return;
        }

        // we didn't receive an extent_id, so we need to look up the extent from the primary index
        auto search_key = _schema->tuple_subset(value, _primary_key);
        auto i = _primary_lookup->lower_bound(search_key, true);

        uint64_t extent_id = constant::UNKNOWN_EXTENT;
        if (i != _primary_lookup->end()) {
            // if the primary index is not empty, get the target extent
            extent_id = _primary_extent_id_f->get_uint64(*i);
        }

        // then we can do a direct insert
        _insert_direct(value, xid, extent_id);
    }

    bool
    MutableTable::_upsert_direct(TuplePtr value,
                                 uint64_t xid,
                                 uint64_t extent_id)
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
    MutableTable::_upsert_empty(TuplePtr value,
                                uint64_t xid)
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
    MutableTable::_upsert_by_lookup(TuplePtr value,
                                    uint64_t xid)
    {
        assert(!_primary_key.empty());

        // if the primary_lookup tree is empty, we will maintain a single page of data that we will
        // keep against the table and use for all operations.
        if (_primary_lookup->empty()) {
            return _upsert_empty(value, xid);
        }

        // we didn't receive an extent_id, so we need to look up the extent from the primary index
        auto search_key = _schema->tuple_subset(value, _primary_key);
        auto i = _primary_lookup->lower_bound(search_key, true);

        uint64_t extent_id = constant::UNKNOWN_EXTENT;
        if (i != _primary_lookup->end()) {
            // if the primary index is not empty, get the target extent
            extent_id = _primary_extent_id_f->get_uint64(*i);
        }

        // then we can do a direct insert
        return _upsert_direct(value, xid, extent_id);
    }

    void
    MutableTable::_remove_direct(TuplePtr value,
                                 uint64_t xid,
                                 uint64_t extent_id)
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
    MutableTable::_remove_empty(TuplePtr value,
                                uint64_t xid)
    {
        // get the page from the cache if we don't have one
        if (!_empty_page) {
            _empty_page = std::make_unique< StorageCache::SafePagePtr>(
                    StorageCache::get_instance()->get(_data_file, constant::UNKNOWN_EXTENT, _access_xid, _target_xid));
        }

        // add the row to the page
       (*_empty_page)->remove(value, _schema);
    }

    void
    MutableTable::_remove_by_lookup(TuplePtr key,
                                    uint64_t xid)
    {
        // if the primary_lookup tree is empty, we will maintain a single page of data that we will
        // keep against the table and use for all operations.
        if (_primary_lookup->empty()) {
            _remove_empty(key, xid);
            return;
        }

        // we didn't receive an extent_id, but we have a primary index, so perform a lookup of the key
        auto i = _primary_lookup->lower_bound(key, true);

        // if the key isn't available, then it may be in the
        uint64_t extent_id = constant::UNKNOWN_EXTENT;
        if (i != _primary_lookup->end()) {
            // if the primary index is not empty, get the target extent
            extent_id = _primary_extent_id_f->get_uint64(*i);
        }

        // then we can do a direct removal
        _remove_direct(key, xid, extent_id);
    }

    void
    MutableTable::_remove_by_scan(TuplePtr value,
                                  uint64_t xid)
    {
        // we didn't receive an extent_id, and there is no primary index, so we must scan the
        // file to find the row to remove
        // note: in this case, it must be a full row match
        // note: it would be much more performant to perform all of the scan-based removals in
        //       an XID at once as a batch, since the table is likely to be much larger than the
        //       set of removals
        auto fields = _schema->get_fields();

        // scan the index
        bool found = false;
        auto &&i = _primary_lookup->begin();
        while (!found && i != _primary_lookup->end()) {
            // scan each extent, looking for a match
            uint64_t extent_id = _primary_extent_id_f->get_uint64(*i);

            auto page = StorageCache::get_instance()->get(_data_file, extent_id, _access_xid, _target_xid, false,
                [this](StorageCache::PagePtr page) { return _flush_handler(page); } );

            // check if we need to convert the page contents to a new schema
            _check_convert_page(page);

            auto &&j = page->begin();
            while (!found && j != page->end()) {
                if (value->equal(FieldTuple(fields, *j))) {
                    page->remove(j);
                    found = true;
                } else {
                    ++j;
                }
            }

            if (!found) {
                ++i;
            }
        }
    }

    void
    MutableTable::_update_direct(TuplePtr value,
                                 uint64_t xid,
                                 uint64_t extent_id)
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
    MutableTable::_update_empty(TuplePtr value,
                                uint64_t xid)
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
    MutableTable::_update_by_lookup(TuplePtr value,
                                    uint64_t xid)
    {
        assert(!_primary_key.empty());

        // if the primary_lookup tree is empty, we will maintain a single page of data that we will
        // keep against the table and use for all operations.
        if (_primary_lookup->empty()) {
            _update_empty(value, xid);
            return;
        }

        // we didn't receive an extent_id, but we have a primary index, so perform a lookup of the key
        auto search_key = _schema->tuple_subset(value, _primary_key);
        auto i = _primary_lookup->lower_bound(search_key, true);

        uint64_t extent_id = constant::UNKNOWN_EXTENT;
        if (i != _primary_lookup->end()) {
            // if the primary index is not empty, get the target extent
            extent_id = _primary_extent_id_f->get_uint64(*i);
        }

        // then we can do a direct update
        _update_direct(value, xid, extent_id);
    }

    void
    MutableTable::_check_convert_page(StorageCache::SafePagePtr &page)
    {
        auto header = page->header();
        XidLsn access_xid(header.xid);
        XidLsn target_xid(_target_xid);

        auto schema = SchemaMgr::get_instance()->get_schema(_db_id, _id, access_xid, target_xid);
        auto virtual_schema = std::dynamic_pointer_cast<VirtualSchema>(schema);

        // the schema has changed
        if (virtual_schema) {
            page->convert(virtual_schema, _schema, _target_xid);
        }
    }

    void Table::Iterator::Primary::next()
    {
        trace tr("table_primary_next_total");

        // move to the next row in the data extent
        {
        trace tr("table_primary_next_1");
        ++_page_i;
        if (_page_i != _page->end()) {
            return;
        }
        }

        // no more rows in the extent, so need to move to the next data extent
        {
        trace tr("table_primary_next_2");
        ++_btree_i;
        if (_btree_i == _btree->end()) {
            return;
        }
        }

        // retrieve the data extent
        {
        trace tr("table_primary_next_3");
        _page = _table->_read_page_via_primary(_btree_i);
        _page_i = _page->begin();
        }
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

    void Table::Iterator::Secondary::next()
    {
        ++_btree_i;
        if (_btree_i == _btree->end()) {
            _page = {};
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
        CHECK(_btree_i != _btree->end());

        uint64_t eid = _extent_id_f->get_uint64(*_btree_i);
        if (_page.empty() || _extent_id != eid) {
            _extent_id = eid;
            _page = _table->_read_page(_extent_id);
        }
        uint64_t row_id = _row_id_f->get_uint32(*_btree_i);
        _page_i = _page->at(row_id);
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
