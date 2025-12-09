#include <memory>

#include <common/constants.hh>
#include <common/json.hh>
#include <common/properties.hh>

#include <sys_tbl_mgr/system_tables.hh>
#include <sys_tbl_mgr/table.hh>

//#define SPRINGTAIL_INCLUDE_TIME_TRACES 1
#include <common/time_trace.hh>

namespace springtail {

static auto get_max_extent_size() {
    return constant::MAX_EXTENT_SIZE;
}

static auto get_max_extent_size_secondary() {
    return constant::MAX_EXTENT_SIZE_SECONDARY;
}

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

    Table::Table(uint64_t db_id,
                 uint64_t table_id,
                 uint64_t xid,
                 const std::filesystem::path &table_base,
                 const std::vector<std::string> &primary_key,
                 const std::vector<Index> &secondary,
                 const TableMetadata &metadata,
                 ExtentSchemaPtr schema,
                 const ExtensionCallback &extension_callback)
        : _db_id(db_id),
          _id(table_id),
          _xid(xid),
          _primary_key(primary_key),
          _schema(schema),
          _extension_callback(extension_callback)
    {
        std::vector<TableRoot> roots;
        uint64_t snapshot_xid = 0;
        _stats = metadata.stats;
        roots = metadata.roots;
        snapshot_xid = metadata.snapshot_xid;

        // construct the table's data directory
        _table_dir = table_helpers::get_table_dir(table_base, db_id, table_id, snapshot_xid);

        // check if the table directory exists; if not, table is considered vacant/empty
        if (!std::filesystem::exists(_table_dir)) {
            _primary_index = nullptr;
            return;
        }

        // Get the singleton roots schema
        _roots_schema = schema_helpers::get_roots_schema();
        _roots_root_f = _roots_schema->get_field("root");
        _roots_index_id_f = _roots_schema->get_field("index_id");

        // handle if the roots were not provided
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
                roots.push_back({constant::INDEX_LOOK_ASIDE, constant::UNKNOWN_EXTENT});
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
        SchemaColumn row_c(constant::INDEX_RID_FIELD, 0, SchemaType::UINT32, 0, false);
        SchemaColumn internal_row_id(constant::INTERNAL_ROW_ID, 0, SchemaType::UINT64, 0, false);

        ExtentSchemaPtr primary_schema;
        if (primary_key.empty()) {
            std::vector<std::string> non_primary_key = { constant::INDEX_EID_FIELD };
            primary_schema = _schema->create_index_schema({}, { extent_c }, non_primary_key, extension_callback);
        } else {
            primary_schema = _schema->create_index_schema(primary_key, { extent_c }, primary_key, extension_callback);
        }

        auto it = std::ranges::find_if(roots, [](auto const &v) { return v.index_id == constant::INDEX_PRIMARY; });
        assert(it != roots.end());

        _primary_index = std::make_shared<BTree>(_db_id,
                                                 _table_dir / constant::INDEX_PRIMARY_FILE,
                                                 xid,
                                                 primary_schema,
                                                 it->extent_id,
                                                 get_max_extent_size(),
                                                 extension_callback);

        _primary_extent_id_f = primary_schema->get_field(constant::INDEX_EID_FIELD);
        _pkey_fields = primary_schema->get_fields();

        // deal with secondary indexes
        for (auto const& idx: secondary) {
            if (idx.state != static_cast<uint8_t>(sys_tbl::IndexNames::State::READY)) {
                continue;
            }
            assert(idx.id != constant::INDEX_PRIMARY);

            // Cache the READY index metadata for later retrieval without RPC calls
            _ready_indexes.push_back(idx);

            // work with the index
            std::vector<uint32_t> idx_cols;
            idx_cols.reserve(idx_cols.size());
            for (auto const &col: idx.columns) {
                idx_cols.push_back(col.position);
            }

            if (!idx_cols.empty()) {
                auto it = std::ranges::find_if(roots, [&](auto const &v) { return v.index_id == idx.id; });
                assert(it != roots.end());
                auto btree =  _create_index_root(idx.id, idx_cols, it->extent_id, extension_callback);
                assert(_secondary_indexes.find(idx.id) == _secondary_indexes.end());
                _secondary_indexes[idx.id] = {btree, idx_cols};
            }

            // Push to GIN indexes if GIN
            if (idx.index_type == constant::INDEX_TYPE_GIN) {
                _gin_indexes_lookup.emplace(idx.id, idx);
            }
        }

        // Get the singleton look-aside schema
        _look_aside_schema = schema_helpers::get_look_aside_schema();

        // Initialize look aside index
        it = std::ranges::find_if(roots, [](auto const &v) { return v.index_id == constant::INDEX_LOOK_ASIDE; });
        if (it != roots.end()) {
            _look_aside_index = std::make_shared<BTree>(_db_id,
                    _table_dir / constant::INDEX_LOOK_ASIDE_FILE,
                    xid,
                    _look_aside_schema,
                    it->extent_id,
                    get_max_extent_size_secondary(), extension_callback);
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

    Table::Iterator
    Table::lower_bound(TuplePtr search_key, uint64_t index_id, bool index_only)
    {
        // check if the table is vacant
        if (_primary_index == nullptr) {
            return end(index_id);
        }

        // check for secondary index lookup
        if (index_id != constant::INDEX_PRIMARY) {
            auto const& [btree, cols] = _secondary_indexes.at(index_id);

            auto gin_idx_i = _gin_indexes_lookup.find(index_id);
            auto search_key_str = search_key->to_string();
            if (gin_idx_i != _gin_indexes_lookup.end()) {
                auto gin_idx = gin_idx_i->second;
                auto col = gin_idx.columns.front();
                auto index_schema = schema_helpers::create_index_schema(_schema, cols, index_id, _extension_callback);
                return Iterator(this, btree, btree->begin(), index_schema, std::string(constant::INDEX_TYPE_GIN));
            } else {
                // find the extent that could contain the lower_bound() key
                auto &&i = btree->lower_bound(search_key);
                if (i == btree->end()) {
                    return end(index_id, index_only);
                }

                if (!index_only) {
                    auto index_schema = schema_helpers::create_index_schema(_schema, cols, index_id, _extension_callback);
                    return Iterator(this, btree, i, index_schema);
                }
                return Iterator(this, btree, i);
            }
        }

        CHECK(!index_only);

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
    Table::upper_bound(TuplePtr search_key, uint64_t index_id, bool index_only)
    {
        // check if the table is vacant
        if (_primary_index == nullptr) {
            return end(index_id);
        }

        if (index_id != constant::INDEX_PRIMARY) {
            auto const& [btree, cols] = _secondary_indexes.at(index_id);

            auto gin_idx = _gin_indexes_lookup.find(index_id);

            if (gin_idx != _gin_indexes_lookup.end()) {
                auto index_schema = schema_helpers::create_index_schema(_schema, cols, index_id, _extension_callback);
                return Iterator(this, btree, btree->begin(), index_schema, std::string(constant::INDEX_TYPE_GIN));
            } else {
                // find the extent that could contain the lower_bound() key
                auto &&i = btree->upper_bound(search_key);
                if (i == btree->end()) {
                    return end(index_id, index_only);
                }

                if (!index_only) {
                    auto index_schema = schema_helpers::create_index_schema(_schema, cols, index_id, _extension_callback);
                    return Iterator(this, btree, i, index_schema);
                }
                return Iterator(this, btree, i);
            }
        }

        CHECK(!index_only);

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
    Table::inverse_lower_bound(TuplePtr search_key, uint64_t index_id, bool index_only)
    {
        // check if the table is vacant or empty
        if (_primary_index == nullptr || _primary_index->empty()) {
            return end(index_id);
        }

        // check if it's a secondary index lookup
        if (index_id != constant::INDEX_PRIMARY) {
            auto const& [btree, cols] = _secondary_indexes.at(index_id);

            // find the extent that contains the row matching the inverse_lower_bound() key
            auto &&i = btree->inverse_lower_bound(search_key);

            if (i == btree->end()) {
                return end(index_id, index_only);
            }

            if (!index_only) {
                auto index_schema = schema_helpers::create_index_schema(_schema, cols, index_id, _extension_callback);
                return Iterator(this, btree, i, index_schema);
            }
            return Iterator(this, btree, i);
        }

        CHECK(!index_only);

        // the table's inverse_lower_bound() record may be the last record in the extent referenced
        // by the index's inverse_lower_bound() or anywhere in the extent referenced by the index's
        // lower_bound() -- so we first search the lower_bound() entry and then if we don't find it
        // we check the inverse_lower_bound() entry
        auto &&i = _primary_index->lower_bound(search_key);

        // check the lower_bound() entry
        if (i != _primary_index->end()) {
            // read the extent and find the inverse_lower_bound() of the key within it
            auto page = _read_page_via_primary(i);

            // find the inverse_lower_bound() of the key within the data extent
            auto &&j = page->inverse_lower_bound(search_key, _schema);

            // if we found it, return it
            if (j != page->end()) {
                return Iterator(this, _primary_index, i, std::move(page), j);
            }
        }

        // not in the lower_bound() entry, go to the last entry of the inverse_lower_bound() entry
        // note: if the iterator is pointing to begin(), this decrement will set it to end()
        --i;

        // if that was the first entry of the index, then inverse_lower_bound() isn't present
        if (i == _primary_index->end()) {
            return end();
        }

        // read the extent and find the last entry within it, since it's guaranteed to be less than the search key
        auto page = _read_page_via_primary(i);
        auto &&j = page->last();

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
    Table::begin(uint64_t index_id, bool index_only, std::vector<std::string> tokens)
    {
        // check if the table is vacant
        if (_primary_index == nullptr) {
            return end(index_id);
        }

        if (index_id == constant::INDEX_PRIMARY) {
            CHECK(!index_only);

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
            auto gin_idx = _gin_indexes_lookup.find(index_id);

            if (gin_idx != _gin_indexes_lookup.end()) {
                auto index_schema = schema_helpers::create_index_schema(_schema, cols, index_id, _extension_callback);
                return Iterator(this, btree, btree->begin(), index_schema, std::string(constant::INDEX_TYPE_GIN), tokens);
            } else {
                // find the extent that could contain the lower_bound() key
                auto i = btree->begin();
                if (i == btree->end()) {
                    return end(index_id, index_only);
                }

                if (index_only) {
                    return Iterator(this, btree, i);
                }

                auto index_schema = schema_helpers::create_index_schema(_schema, cols, index_id, _extension_callback);
                return Iterator(this, btree, i, index_schema);
            }
        }
    }

    ExtentSchemaPtr
    Table::get_index_schema(uint64_t index_id) const
    {
        auto const& [btree, cols] = _secondary_indexes.at(index_id);
        return schema_helpers::create_index_schema(_schema, cols, index_id, _extension_callback);
    }

    std::vector<std::string>
    Table::get_index_column_names(uint64_t index_id) const
    {
        return _schema->get_column_names(_secondary_indexes.at(index_id).second);
    }


    std::pair<std::shared_ptr<Extent>, uint64_t>
    Table::read_extent_from_disk(uint64_t extent_id) const
    {
        // XXX: When an extent is asked from the page,
        // and if the extent's XID is different than the XID passed
        // update page cache XID with extent's XID. This can avoid
        // direct IO access from here
        auto data_file_handle = IOMgr::get_instance()->open(_table_dir / constant::DATA_FILE, IOMgr::IO_MODE::READ, true);
        auto response = data_file_handle->read(extent_id);
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
        return StorageCache::get_instance()->get(_db_id, _table_dir / constant::DATA_FILE, extent_id, _xid, constant::LATEST_XID, get_max_extent_size());
    }

    BTreePtr
    Table::_create_index_root(uint64_t index_id, const std::vector<uint32_t>& index_columns, uint64_t offset, const ExtensionCallback &extension_callback)
    {
        auto index_schema = schema_helpers::create_index_schema(_schema, index_columns, index_id, extension_callback);
        auto btree = std::make_shared<BTree>(_db_id,
                _table_dir / fmt::format(constant::INDEX_FILE, index_id),
                _xid, index_schema,
                offset,
                index_id == constant::INDEX_PRIMARY? get_max_extent_size(): get_max_extent_size_secondary(),
                extension_callback);
        return btree;
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
            Tracker{table, btree, btree_i}
    {
        _look_aside_key_fields = std::make_shared<FieldArray>(1);
        _extent_id_f = table->look_aside_schema()->get_field(constant::INDEX_EID_FIELD);
        _row_id_f = table->look_aside_schema()->get_field(constant::INDEX_RID_FIELD);
        _internal_row_id_f = schema->get_field(constant::INTERNAL_ROW_ID);
        if (_btree_i != btree->end()) {
            update_page();
        }
    }

    Table::Iterator::GINSecondary::GINSecondary(const Table *table,
            BTreePtr btree, const BTree::Iterator &btree_i,
            ExtentSchemaPtr schema, const std::vector<std::string> tokens)
        :
            Tracker{table, btree, btree_i},
            _tokens(tokens)
    {
        _look_aside_key_fields = std::make_shared<FieldArray>(1);
        _extent_id_f = table->look_aside_schema()->get_field(constant::INDEX_EID_FIELD);
        _row_id_f = table->look_aside_schema()->get_field(constant::INDEX_RID_FIELD);
        _internal_row_id_f = schema->get_field(constant::INTERNAL_ROW_ID);
        if (_btree_i != btree->end()) {
            update_page();
        }
    }

    void Table::Iterator::GINSecondary::update_page(bool prev)
    {
        DCHECK(_btree_i != _btree->end());
        uint64_t internal_row_id = 0;
        while (true) {
            auto &&index_row = *_btree_i;
            internal_row_id = _internal_row_id_f->get_uint64(&index_row);

            // Found an unvisited row: mark and keep iterator here.
            if (!_visited_internal_row_ids.contains(internal_row_id)) {
                _visited_internal_row_ids.emplace(internal_row_id);
                break;
            }

            // Current row was already visited: move iterator.
            if (prev) {
                if (_btree_i == _btree->begin()) {
                    // No more candidates in this direction.
                    return;
                }
                --_btree_i;
            } else {
                ++_btree_i;
                if (_btree_i == _btree->end()) {
                    // No more candidates in this direction.
                    return;
                }
            }
        }

        // Get the extent and row ids from the look_aside_index
        // using the internal_row_id as the key
        uint64_t eid, row_id;
        auto &&look_aside_index = _table->look_aside_index();

        // Construct and set the key for lookup
        _look_aside_key_fields->at(0) = std::make_shared<ConstTypeField<uint64_t>>(internal_row_id);
        auto lookup_tuple = std::make_shared<FieldTuple>(_look_aside_key_fields, nullptr);

        // Look-aside entry must exist if entry exists in secondary index
        auto &&lookup_i = look_aside_index->lower_bound(lookup_tuple);
        DCHECK(lookup_i != look_aside_index->end());

        // Get the extent and row id from the row
        auto &&row = *lookup_i;
        eid = _extent_id_f->get_uint64(&row);
        row_id = _row_id_f->get_uint32(&row);

        auto page = _table->_read_page(eid);
        DCHECK(page->extent_count() == 1);
        _page_i = page->begin();
        _page_i += row_id;
    }

    void Table::Iterator::Secondary::next()
    {
        ++_btree_i;
        if (_btree_i == _btree->end()) {
            return;
        }
        update_page();
    }

    void Table::Iterator::Secondary::prev()
    {
        --_btree_i;
        update_page();
    }

    void Table::Iterator::GINSecondary::next()
    {
        ++_btree_i;
        if (_btree_i == _btree->end()) {
            return;
        }
        update_page();
    }

    void Table::Iterator::GINSecondary::prev()
    {
        --_btree_i;
        update_page(true);
    }

    void Table::Iterator::Secondary::update_page()
    {
        DCHECK(_btree_i != _btree->end());
        auto &&index_row = *_btree_i;

        // Get the internal_row_id from the index row first
        uint64_t internal_row_id = _internal_row_id_f->get_uint64(&index_row);

        // Get the extent and row ids from the look_aside_index
        // using the internal_row_id as the key
        uint64_t eid, row_id;
        auto &&look_aside_index = _table->look_aside_index();

        // Construct and set the key for lookup
        _look_aside_key_fields->at(0) = std::make_shared<ConstTypeField<uint64_t>>(internal_row_id);
        auto lookup_tuple = std::make_shared<FieldTuple>(_look_aside_key_fields, nullptr);

        // Look-aside entry must exist if entry exists in secondary index
        auto &&lookup_i = look_aside_index->lower_bound(lookup_tuple);
        DCHECK(lookup_i != look_aside_index->end());

        // Get the extent and row id from the row
        auto &&row = *lookup_i;
        eid = _extent_id_f->get_uint64(&row);
        row_id = _row_id_f->get_uint32(&row);

        auto page = _table->_read_page(eid);
        DCHECK(page->extent_count() == 1);
        _page_i = page->begin();
        _page_i += row_id;
    }

    Table::Iterator::SecondaryIndexOnly::SecondaryIndexOnly(const Table *table,
            BTreePtr btree, const BTree::Iterator &btree_i)
        :
            Tracker{table, btree, btree_i}
    {}

    Table::Iterator::Iterator(const Table *table, uint32_t index_id, bool index_only, std::vector<std::string> tokens)
    {
        auto gin_idx = table->_gin_indexes_lookup.find(index_id);

        if (index_id == constant::INDEX_PRIMARY) {
            _tracker.emplace<Primary>(table, table->_primary_index,
                    table->_primary_index->end(),
                    StorageCache::SafePagePtr{},
                    StorageCache::Page::Iterator{});
        } else if (gin_idx != table->_gin_indexes_lookup.end()) {
            auto const& [btree, cols] = table->_secondary_indexes.at(index_id);
            auto index_schema = schema_helpers::create_gin_index_schema(table->_schema, table->_extension_callback);
            _tracker.emplace<GINSecondary>(table, btree, btree->end(), index_schema, tokens);
        } else if (index_only) {
            auto const& [btree, _] = table->_secondary_indexes.at(index_id);
            _tracker.emplace<SecondaryIndexOnly>(table, btree,
                    btree->end());
        } else {
            auto const& [btree, cols] = table->_secondary_indexes.at(index_id);
            auto index_schema = schema_helpers::create_index_schema(table->_schema, cols, index_id, table->_extension_callback);
            _tracker.emplace<Secondary>(table, btree,
                    btree->end(), index_schema );
        }
    }

    void Table::advance_to_xid(uint64_t new_xid, const std::vector<TableRoot> &new_roots)
    {
        // Update the table's XID
        _xid = new_xid;

        // Update each index based on its root in new_roots
        for (const auto &root : new_roots) {
            if (root.index_id == constant::INDEX_PRIMARY) {
                // Update primary index if it exists
                if (_primary_index != nullptr) {
                    _primary_index->set_root_and_xid(new_xid, root.extent_id);
                }
            } else if (root.index_id == constant::INDEX_LOOK_ASIDE) {
                // Update look-aside index if it exists
                if (_look_aside_index != nullptr) {
                    _look_aside_index->set_root_and_xid(new_xid, root.extent_id);
                }
            } else {
                // Update secondary index if it exists
                auto it = _secondary_indexes.find(root.index_id);
                if (it != _secondary_indexes.end()) {
                    it->second.first->set_root_and_xid(new_xid, root.extent_id);
                }
            }
        }
    }
}
