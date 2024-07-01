#include <storage/table.hh>
#include <write_cache/write_cache_client.hh>

namespace springtail {

    namespace {
        const static std::vector<SchemaColumn> ROOTS_SCHEMA = {
            { 0, 0, "root", 0, SchemaType::UINT64, "uint8", true, false }
        };
    }

    Table::Table(uint64_t table_id,
                 uint64_t xid,
                 const std::filesystem::path &table_dir,
                 const std::vector<std::string> &primary_key,
                 const std::vector<std::vector<std::string>> &secondary_keys,
                 std::vector<uint64_t> root_offsets,
                 ExtentSchemaPtr schema)
        : _id(table_id),
          _xid(xid),
          _table_dir(table_dir),
          _primary_key(primary_key),
          _secondary_keys(secondary_keys),
          _schema(schema)
    {
        // make sure that the table directory exists
        std::filesystem::create_directory(_table_dir);

        // store the roots schema / field
        _roots_schema = std::make_shared<ExtentSchema>(ROOTS_SCHEMA);
        _roots_root_f = _roots_schema->get_field("root");

        // handle if the roots were not provided
        if (root_offsets.empty()) {
            if (std::filesystem::exists(table_dir / constant::ROOTS_FILE)) {
                // read the roots from the look-aside file
                auto roots_path = std::filesystem::read_symlink(table_dir / constant::ROOTS_FILE);
                auto root_handle = IOMgr::get_instance()->open(roots_path, IOMgr::IO_MODE::READ, true);
                auto response = root_handle->read(0);
                auto extent = std::make_shared<Extent>(response->data);
                for (auto &row : *extent) {
                    root_offsets.push_back(_roots_root_f->get_uint64(row));
                }

                // XXX is this the right thing to do?  forces the XID to the known XID of the roots
                xid = extent->header().xid;
            } else {
                // fill the root offsets with UNKNOWN_EXTENT to indicate an empty tree
                root_offsets.push_back(constant::UNKNOWN_EXTENT);
                for (auto secondary : secondary_keys) {
                    root_offsets.push_back(constant::UNKNOWN_EXTENT);
                }
            }
        }

        SchemaColumn extent_c(constant::INDEX_EID_FIELD, 0, SchemaType::UINT64, "uint8", false);
        SchemaColumn row_c(constant::INDEX_RID_FIELD, 0, SchemaType::UINT32, "uint4", false);
        auto primary_schema = _schema->create_schema(primary_key, { extent_c }, primary_key);
        _primary_index = std::make_shared<BTree>(table_dir / constant::INDEX_PRIMARY_FILE,
                                                 xid,
                                                 primary_schema,
                                                 root_offsets[0]);

        _primary_extent_id_f = primary_schema->get_field(constant::INDEX_EID_FIELD);
        _pkey_fields = primary_schema->get_fields();

        for (int i = 0; i < secondary_keys.size(); i++) {
            auto secondary_key = secondary_keys[i];

            // add the additional secondary key columns
            secondary_key.push_back(constant::INDEX_EID_FIELD);
            secondary_key.push_back(constant::INDEX_RID_FIELD);

            auto secondary_schema = _schema->create_schema(secondary_keys[i], { extent_c, row_c }, secondary_key);

            auto btree = std::make_shared<BTree>(table_dir / fmt::format(constant::INDEX_FILE, (i + 1)),
                                                 xid,
                                                 secondary_schema,
                                                 root_offsets[i + 1]);
            _secondary_indexes.push_back(btree);
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
        return SchemaMgr::get_instance()->get_extent_schema(_id, _xid);
    }

    SchemaPtr
    Table::schema(uint64_t extent_xid) const
    {
        return SchemaMgr::get_instance()->get_schema(_id, extent_xid, _xid);
    }

    Table::Iterator
    Table::lower_bound(TuplePtr search_key)
    {
        // find the extent that could contain the lower_bound() key
        auto &&i = _primary_index->lower_bound(search_key);
        if (i == _primary_index->end()) {
            return end();
        }

        // read the extent and find the lower_bound() of the key within it
        auto page = _read_page_via_primary(i);

        // find the lower_bound() of the key within the data extent
        auto &&j = page->lower_bound(search_key, _schema);

        // note: the primary index indicates that there is a value >= the search_key in this page
        assert(j != page->end());

        return Iterator(this, _primary_index, i, page, j);
    }

    Table::Iterator
    Table::begin()
    {
        auto &&index_i = _primary_index->begin();
        if (index_i == _primary_index->end()) {
            return end();
        }

        auto page = _read_page_via_primary(index_i);
        return Iterator(this, _primary_index, index_i, page, page->begin());
    }

    StorageCache::PagePtr
    Table::read_page(uint64_t extent_id) const
    {
        return _read_page(extent_id);
    }

    StorageCache::PagePtr
    Table::_read_page_via_primary(BTree::Iterator &pos) const
    {
        uint64_t extent_id = _primary_extent_id_f->get_uint64(*pos);
        return _read_page(extent_id);
    }

    StorageCache::PagePtr
    Table::_read_page(uint64_t extent_id) const
    {
        return StorageCache::get_instance()->get(_table_dir / constant::DATA_FILE, extent_id, _xid);
    }


    MutableTable::MutableTable(uint64_t id,
                               uint64_t access_xid,
                               uint64_t target_xid,
                               std::vector<uint64_t> root_offsets,
                               const std::filesystem::path &table_dir,
                               const std::vector<std::string> &primary_key,
                               const std::vector<std::vector<std::string>> &secondary_keys,
                               ExtentSchemaPtr schema,
                               bool for_gc)
    : _id(id),
      _access_xid(access_xid),
      _target_xid(target_xid),
      _table_dir(table_dir),
      _data_file(table_dir / constant::DATA_FILE),
      _primary_key(primary_key),
      _secondary_keys(secondary_keys),
      _schema(schema),
      _for_gc(for_gc)
    {
        // make sure that the table directory exists
        std::filesystem::create_directory(_table_dir);

        // store the roots schema / field
        _roots_schema = std::make_shared<ExtentSchema>(ROOTS_SCHEMA);
        _roots_root_f = _roots_schema->get_mutable_field("root");

        // handle if the roots were not provided
        if (root_offsets.empty()) {
            if (std::filesystem::exists(table_dir / constant::ROOTS_FILE)) {
                // read the roots from the look-aside file
                auto roots_path = std::filesystem::read_symlink(table_dir / constant::ROOTS_FILE);
                auto root_handle = IOMgr::get_instance()->open(roots_path, IOMgr::IO_MODE::READ, true);
                auto response = root_handle->read(0);
                auto extent = std::make_shared<Extent>(response->data);
                for (auto &row : *extent) {
                    root_offsets.push_back(_roots_root_f->get_uint64(row));
                }
            } else {
                // fill the root offsets with UNKNOWN_EXTENT to indicate an empty tree
                root_offsets.push_back(constant::UNKNOWN_EXTENT);
                for (auto secondary : secondary_keys) {
                    root_offsets.push_back(constant::UNKNOWN_EXTENT);
                }
            }
        }

        // construct the primary index btree
        SchemaColumn extent_c(constant::INDEX_EID_FIELD, 0, SchemaType::UINT64, "uint8", false);
        SchemaColumn row_c(constant::INDEX_RID_FIELD, 1, SchemaType::UINT32, "uint4", false);

        auto primary_schema = _schema->create_schema(primary_key, { extent_c }, primary_key);

        _primary_index = std::make_shared<MutableBTree>(table_dir / constant::INDEX_PRIMARY_FILE,
                                                        primary_key,
                                                        primary_schema,
                                                        _target_xid);
        if (root_offsets[0] != constant::UNKNOWN_EXTENT) {
            _primary_index->init(root_offsets[0]);
        } else {
            _primary_index->init_empty();
        }

        _primary_lookup = std::make_shared<BTree>(table_dir / constant::INDEX_PRIMARY_FILE,
                                                  access_xid,
                                                  primary_schema,
                                                  root_offsets[0]);

        _primary_extent_id_f = primary_schema->get_field(constant::INDEX_EID_FIELD);

        // construct the secondary index btrees
        for (int i = 0; i < secondary_keys.size(); i++) {
            int idx = i + 1;

            auto secondary_key = secondary_keys[i];
            secondary_key.push_back(constant::INDEX_EID_FIELD);
            secondary_key.push_back(constant::INDEX_RID_FIELD);

            auto secondary_schema = _schema->create_schema(secondary_keys[i], { extent_c, row_c }, secondary_key);

            auto btree = std::make_shared<MutableBTree>(table_dir / fmt::format(constant::INDEX_FILE, idx),
                                                        secondary_key, secondary_schema,
                                                        _target_xid);

            if (root_offsets[idx] != constant::UNKNOWN_EXTENT) {
                btree->init(root_offsets[idx]);
            } else {
                btree->init_empty();
            }

            _secondary_indexes.push_back(btree);
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
    }

    void
    MutableTable::upsert(TuplePtr value,
                         uint64_t xid,
                         uint64_t extent_id)
    {
        if (extent_id == constant::UNKNOWN_EXTENT) {
            if (_primary_key.empty()) {
                // with no primary key, we just resort to a separate removal and insert
                auto search_key = _schema->tuple_subset(value, _primary_key);
                _remove_by_scan(search_key, xid);
                _insert_append(value, xid);
            } else {
                _upsert_by_lookup(value, xid);
            }
        } else {
            _upsert_direct(value, xid, extent_id);
        }
    }

    void
    MutableTable::remove(TuplePtr key,
                         uint64_t xid,
                         uint64_t extent_id)
    {
        if (extent_id == constant::UNKNOWN_EXTENT) {
            if (_primary_key.empty()) {
                _remove_by_scan(key, xid);
            } else {
                _remove_by_lookup(key, xid);
            }
        } else {
            _remove_direct(key, xid, extent_id);
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
    }

    bool
    MutableTable::_flush_handler(StorageCache::PagePtr page)
    {
        // first invalidate the index entries based on the original page
        _invalidate_indexes(page);

        // then flush the generated page and populate the index entries based on the new pages
        _flush_and_populate_indexes(page);

        // return success
        return true;
    }

    void
    MutableTable::_invalidate_indexes(StorageCache::PagePtr page)
    {
        uint64_t old_eid = page->key().second;

        // get the original page to use for index updates
        auto orig_page = StorageCache::get_instance()->get(_data_file, old_eid, _access_xid);

        // INVALIDATE PRIMARY INDEX

        // get the last row of the original page for the primary index
        auto pkey_fields = _schema->get_fields(_primary_key);
        TuplePtr pkey = std::make_shared<FieldTuple>(pkey_fields, *orig_page->last());

        // remove the old primary index entry
        _primary_index->remove(pkey);

        // INVALIDATE SECONDARY INDEXES

        FieldArrayPtr value_fields = std::make_shared<FieldArray>(2);
        value_fields->at(0) = std::make_shared<ConstTypeField<uint64_t>>(orig_page->key().second);

        // go through each row and pass the relevant key to each of the secondary indexes for removal
        uint32_t row_id = 0;
        for (auto &&row : *orig_page) {
            value_fields->at(1) = std::make_shared<ConstTypeField<uint32_t>>(row_id);

            for (int i = 0; i < _secondary_indexes.size(); ++i) {
                auto &secondary = _secondary_indexes[i];
                auto key_fields = _schema->get_fields(_secondary_keys[i]);

                auto &&skey = std::make_shared<KeyValueTuple>(key_fields, value_fields, row);
                secondary->remove(skey);
            }

            ++row_id;
        }

        StorageCache::get_instance()->put(orig_page);
    }

    void
    MutableTable::_flush_and_populate_indexes(StorageCache::PagePtr page)
    {
        uint64_t old_eid = page->key().second;
        auto pkey_fields = _schema->get_fields(_primary_key);

        // retrieve the extent offsets of the new page
        ExtentHeader header(ExtentType(), _target_xid, _schema->row_size(), old_eid);
        auto &&offsets = page->flush(header);

        // record the mapping into the extent map
        if (_for_gc) {
            WriteCacheClient::get_instance()->add_mapping(_id, _target_xid, old_eid, offsets);
        }

        auto value_fields = std::make_shared<FieldArray>(1);
        for (auto extent_id : offsets) {
            auto new_page = StorageCache::get_instance()->get(_data_file, extent_id, _target_xid);

            // POPULATE PRIMARY INDEX

            // create the new primary index entry
            (*value_fields)[0] = std::make_shared<ConstTypeField<uint64_t>>(extent_id);

            auto pkey = std::make_shared<KeyValueTuple>(pkey_fields, value_fields, *new_page->last());

            // insert the new primary index entry
            _primary_index->insert(pkey);

            // POPULATE SECONDARY INDEXES

            // go through each row and pass the relevant key to each of the secondary indexes for insertion
            value_fields->resize(2);
            uint32_t row_id = 0;
            for (auto &row : *new_page) {
                (*value_fields)[1] = std::make_shared<ConstTypeField<uint32_t>>(row_id);

                for (int i = 0; i < _secondary_indexes.size(); ++i) {
                    auto &secondary = _secondary_indexes[i];
                    auto key_fields = _schema->get_fields(_secondary_keys[i]);

                    auto &&svalue = std::make_shared<KeyValueTuple>(key_fields, value_fields, row);
                    SPDLOG_DEBUG_MODULE(LOG_BTREE, "Secondary populate {}", svalue->to_string());
                    secondary->insert(svalue);
                }

                ++row_id;
            }
            StorageCache::get_instance()->put(new_page);

            SPDLOG_DEBUG_MODULE(LOG_BTREE, "Populated {} secondary rows", row_id);
        }
    }

    std::vector<uint64_t>
    MutableTable::finalize()
    {
        // in the case of having an empty table, there are no invalidations... we can flush the
        // single Page and update the indexes
        if (_empty_page) {
            _flush_and_populate_indexes(_empty_page);

            StorageCache::get_instance()->put(_empty_page);
            _empty_page = nullptr;

            // XXX should we still call StorageCache::flush() to make sure that the file is sync()'d?
        } else {
            // flush the dirty data pages of the table to disk
            StorageCache::get_instance()->flush(_data_file);
        }

        // now flush the indexes, capturing the roots
        std::vector<uint64_t> roots;
        roots.push_back(_primary_index->finalize());

        for (auto secondary : _secondary_indexes) {
            roots.push_back(secondary->finalize());
        }

        // store the roots into a look-aside root file
        auto extent = std::make_shared<Extent>(ExtentType(), _target_xid, _roots_schema->row_size());
        for (auto root : roots) {
            auto &&row = extent->append();
            _roots_root_f->set_uint64(row, root);
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

        return roots;
    }

    void
    MutableTable::_insert_direct(TuplePtr value,
                                 uint64_t xid,
                                 uint64_t extent_id)
    {
        // get the page from the cache
        auto page = StorageCache::get_instance()->get(_data_file, extent_id, _access_xid, _target_xid);

        // add the row to the page
        page->insert(value, _schema);

        // release the page back to the write cache
        StorageCache::get_instance()->put(page, std::bind(&MutableTable::_flush_handler,
                                                          this, std::placeholders::_1));
    }

    void
    MutableTable::_insert_empty(TuplePtr value,
                                uint64_t xid)
    {
        // get the page from the cache if we don't have one
        if (!_empty_page) {
            _empty_page = StorageCache::get_instance()->get(_data_file, constant::UNKNOWN_EXTENT, _access_xid, _target_xid);
        }

        // add the row to the page
        _empty_page->insert(value, _schema);
    }

    void
    MutableTable::_insert_append(TuplePtr value,
                                 uint64_t xid)
    {
        // note: in this case there is no explicit primary key, so we need to append the row to the
        //       end of the file
        auto pos = --(_primary_lookup->end());
        uint64_t extent_id = _primary_extent_id_f->get_uint64(*pos);

        // get the page from the cache
        auto page = StorageCache::get_instance()->get(_data_file, extent_id, _access_xid, _target_xid);

        // append the value to the extent
        page->append(value, _schema);

        // release the extent back to the write cache
        // note: the primary index is just a btree of extent IDs in the no-primary-key scenario
        StorageCache::get_instance()->put(page, std::bind(&MutableTable::_flush_handler,
                                                          this, std::placeholders::_1));
    }

    void
    MutableTable::_insert_by_lookup(TuplePtr value,
                                    uint64_t xid)
    {
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

    void
    MutableTable::_upsert_direct(TuplePtr value,
                                 uint64_t xid,
                                 uint64_t extent_id)
    {
        // get the page from the cache
        auto page = StorageCache::get_instance()->get(_data_file, extent_id, _access_xid, _target_xid);

        // add the row to the page
        page->upsert(value, _schema);

        // release the page back to the write cache
        StorageCache::get_instance()->put(page, std::bind(&MutableTable::_flush_handler,
                                                          this, std::placeholders::_1));
    }

    void
    MutableTable::_upsert_empty(TuplePtr value,
                                uint64_t xid)
    {
        // get the page from the cache if we don't have one
        if (!_empty_page) {
            _empty_page = StorageCache::get_instance()->get(_data_file, constant::UNKNOWN_EXTENT, _access_xid, _target_xid);
        }

        // add the row to the page
        _empty_page->upsert(value, _schema);
    }

    void
    MutableTable::_upsert_by_lookup(TuplePtr value,
                                    uint64_t xid)
    {
        // if the primary_lookup tree is empty, we will maintain a single page of data that we will
        // keep against the table and use for all operations.
        if (_primary_lookup->empty()) {
            _upsert_empty(value, xid);
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
        _upsert_direct(value, xid, extent_id);
    }

    void
    MutableTable::_remove_direct(TuplePtr value,
                                 uint64_t xid,
                                 uint64_t extent_id)
    {
        // get the page from the cache
        auto page = StorageCache::get_instance()->get(_data_file, extent_id, _access_xid, _target_xid);

        // remove the row from the page
        // note: this can only be used when a primary key is present, otherwise use _remove_by_scan()
        page->remove(value, _schema);

        // release the page back to the write cache
        StorageCache::get_instance()->put(page, std::bind(&MutableTable::_flush_handler,
                                                          this, std::placeholders::_1));
    }

    void
    MutableTable::_remove_empty(TuplePtr value,
                                uint64_t xid)
    {
        // get the page from the cache if we don't have one
        if (!_empty_page) {
            _empty_page = StorageCache::get_instance()->get(_data_file, constant::UNKNOWN_EXTENT, _access_xid, _target_xid);
        }

        // add the row to the page
        _empty_page->remove(value, _schema);
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

            auto page = StorageCache::get_instance()->get(_data_file, extent_id, _access_xid, _target_xid);

            auto &&j = page->begin();
            while (!found && j != page->end()) {
                if (value->equal(FieldTuple(fields, *j))) {
                    page->remove(j);
                    found = true;
                } else {
                    ++j;
                }
            }

            StorageCache::get_instance()->put(page, std::bind(&MutableTable::_flush_handler,
                                                              this, std::placeholders::_1));

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
        auto page = StorageCache::get_instance()->get(_data_file, extent_id, _access_xid, _target_xid);

        // update the row in the page
        // note: this can only be used when a primary key is present, otherwise update should have been split
        page->update(value, _schema);

        // release the page back to the write cache
        StorageCache::get_instance()->put(page, std::bind(&MutableTable::_flush_handler,
                                                          this, std::placeholders::_1));
    }

    void
    MutableTable::_update_empty(TuplePtr value,
                                uint64_t xid)
    {
        // get the page from the cache if we don't have one
        if (!_empty_page) {
            _empty_page = StorageCache::get_instance()->get(_data_file, constant::UNKNOWN_EXTENT, _access_xid, _target_xid);
        }

        // add the row to the page
        _empty_page->update(value, _schema);
    }

    void
    MutableTable::_update_by_lookup(TuplePtr value,
                                    uint64_t xid)
    {
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

}
