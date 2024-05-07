#include <storage/table.hh>

namespace springtail {

    namespace {
        const static std::vector<SchemaColumn> ROOTS_SCHEMA = {
            { 0, 0, "root", 0, SchemaType::UINT64, true, false }
        };
    }

    Table::Table(uint64_t table_id,
                 uint64_t xid,
                 const std::filesystem::path &table_dir,
                 const std::vector<std::string> &primary_key,
                 const std::vector<std::vector<std::string>> &secondary_keys,
                 std::vector<uint64_t> root_offsets,
                 ExtentSchemaPtr schema,
                 ExtentCachePtr cache)
        : _id(table_id),
          _xid(xid),
          _table_dir(table_dir),
          _primary_key(primary_key),
          _secondary_keys(secondary_keys),
          _schema(schema),
          _cache(cache)
    {
        // make sure that the table directory exists
        std::filesystem::create_directory(_table_dir);

        // store the roots schema / field
        _roots_schema = std::make_shared<ExtentSchema>(ROOTS_SCHEMA);
        _roots_root_f = _roots_schema->get_field("root");

        // handle if the roots were not provided
        if (root_offsets.empty()) {
            if (std::filesystem::exists(table_dir / "roots")) {
                // read the roots from the look-aside file
                auto roots_path = std::filesystem::read_symlink(table_dir / "roots");
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

        _handle = IOMgr::get_instance()->open(table_dir / "raw", IOMgr::IO_MODE::READ, true);

        SchemaColumn extent_c("extent_id", 0, SchemaType::UINT64, false);
        SchemaColumn row_c("row_id", 0, SchemaType::UINT32, false);
        auto primary_schema = _schema->create_schema(primary_key, { extent_c });
        _primary_index = std::make_shared<BTree>(table_dir / "0.idx",
                                                 xid,
                                                 primary_schema,
                                                 root_offsets[0]);

        _primary_extent_id_f = primary_schema->get_field("extent_id");
        _pkey_fields = primary_schema->get_fields();

        for (int i = 0; i < secondary_keys.size(); i++) {
            auto secondary_key = secondary_keys[i];
            auto secondary_schema = _schema->create_schema(secondary_key, { extent_c, row_c });

            // add the additional secondary key columns
            secondary_key.push_back("extent_id");
            secondary_key.push_back("row_id");

            auto btree = std::make_shared<BTree>(table_dir / fmt::format("{}.idx", (i + 1)),
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
        auto &&i = _primary_index->lower_bound(search_key, _xid);
        if (i == _primary_index->end()) {
            return end();
        }

        // read the extent and find the lower_bound() of the key within it
        ExtentPtr extent = _read_extent_via_primary(i);

        // find the lower_bound() of the key within the data extent
        auto &&j = std::lower_bound(extent->begin(), extent->end(), search_key,
                                    [this](const Extent::Row &row, TuplePtr key)
                                    {
                                        return FieldTuple(this->_pkey_fields, row).less_than(key);
                                    });

        return Iterator(this, _primary_index, i, extent, j);
    }

    Table::Iterator
    Table::begin()
    {
        auto &&index_i = _primary_index->begin();
        if (index_i == _primary_index->end()) {
            return end();
        }

        auto extent = _read_extent_via_primary(index_i);
        return Iterator(this, _primary_index, index_i, extent, extent->begin());
    }

    // XXX we should encapsulate the extent access
    ExtentPtr
    Table::read_extent(uint64_t extent_id) const
    {
        return _read_extent(extent_id);
    }


    ExtentPtr
    Table::_read_extent_via_primary(BTree::Iterator &pos) const
    {
        uint64_t extent_id = _primary_extent_id_f->get_uint64(*pos);
        return _read_extent(extent_id);
    }

    ExtentPtr
    Table::_read_extent(uint64_t extent_id) const
    {
        auto response = _handle->read(extent_id);
        return std::make_shared<Extent>(response->data);
    }


    MutableTable::MutableTable(uint64_t id,
                               uint64_t target_xid,
                               std::vector<uint64_t> root_offsets,
                               const std::filesystem::path &table_dir,
                               const std::vector<std::string> &primary_key,
                               const std::vector<std::vector<std::string>> &secondary_keys,
                               ExtentSchemaPtr schema,
                               DataCachePtr cache,
                               MutableBTree::PageCachePtr page_cache,
                               ExtentCachePtr read_cache)
    : _id(id),
      _target_xid(target_xid),
      _table_dir(table_dir),
      _data_file(table_dir / "raw"),
      _primary_key(primary_key),
      _secondary_keys(secondary_keys),
      _schema(schema),
      _cache(cache)
    {
        // make sure that the table directory exists
        std::filesystem::create_directory(_table_dir);

        // store the roots schema / field
        _roots_schema = std::make_shared<ExtentSchema>(ROOTS_SCHEMA);
        _roots_root_f = _roots_schema->get_mutable_field("root");

        // handle if the roots were not provided
        if (root_offsets.empty()) {
            if (std::filesystem::exists(table_dir / "roots")) {
                // read the roots from the look-aside file
                auto roots_path = std::filesystem::read_symlink(table_dir / "roots");
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
        SchemaColumn extent_c("extent_id", 0, SchemaType::UINT64, false);
        SchemaColumn row_c("row_id", 1, SchemaType::UINT32, false);
        auto primary_schema = _schema->create_schema(primary_key, { extent_c });
        _primary_index = std::make_shared<MutableBTree>(table_dir / "0.idx",
                                                        primary_key,
                                                        page_cache,
                                                        primary_schema);
        if (root_offsets[0] != constant::UNKNOWN_EXTENT) {
            _primary_index->init(root_offsets[0]);
        } else {
            _primary_index->init_empty();
        }
        _primary_index->set_xid(_target_xid);

        _primary_lookup = std::make_shared<BTree>(table_dir / "0.idx",
                                                  _target_xid,
                                                  primary_schema,
                                                  root_offsets[0]);

        _primary_extent_id_f = primary_schema->get_field("extent_id");

        // construct the secondary index btrees
        for (int i = 0; i < secondary_keys.size(); i++) {
            int idx = i + 1;

            auto secondary_key = secondary_keys[i];

            auto secondary_schema = _schema->create_schema(secondary_key, { extent_c, row_c });
            secondary_key.push_back("extent_id");
            secondary_key.push_back("row_id");

            auto btree = std::make_shared<MutableBTree>(table_dir / fmt::format("{}.idx", idx),
                                                        secondary_key, page_cache, secondary_schema);

            if (root_offsets[idx] != constant::UNKNOWN_EXTENT) {
                btree->init(root_offsets[idx]);
            } else {
                btree->init_empty();
            }
            btree->set_xid(_target_xid);
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
            if (_primary_key.empty()) {
                // XXX error -- cannot perform an update() with no primary key, should be split into a remove() and insert()
            } else {
                _update_by_lookup(value, xid);
            }
        } else {
            _update_direct(value, xid, extent_id);
        }
    }

    void
    MutableTable::invalidate_indexes(uint64_t extent_id,
                                     ExtentPtr extent)
    {
        // get the key from the last row of the extent and remove it from the primary index
        FieldArrayPtr key_fields = _schema->get_fields(_primary_key);
            
        // remove the primary index entry
        auto &&pkey = std::make_shared<FieldTuple>(key_fields, extent->back());
        _primary_index->remove(pkey);

        SPDLOG_DEBUG_MODULE(LOG_BTREE, "invalidate primary key: {}", pkey->to_string());

        // setup the value fields for the secondary indexes
        FieldArrayPtr value_fields = std::make_shared<FieldArray>(2);
        value_fields->at(0) = std::make_shared<ConstTypeField<uint64_t>>(extent_id);

        // go through each row and pass the relevant key to each of the secondary indexes for removal
        uint32_t row_id = 0;
        for (auto &&row : *extent) {
            value_fields->at(1) = std::make_shared<ConstTypeField<uint32_t>>(row_id);

            for (int i = 0; i < _secondary_indexes.size(); ++i) {
                auto &secondary = _secondary_indexes[i];
                key_fields = _schema->get_fields(_secondary_keys[i]);

                auto &&skey = std::make_shared<KeyValueTuple>(key_fields, value_fields, row);
                secondary->remove(skey);
            }

            ++row_id;
        }

        SPDLOG_DEBUG_MODULE(LOG_BTREE, "Invalidated {} secondary rows", extent->row_count());
    }

    void
    MutableTable::populate_indexes(uint64_t extent_id,
                                   ExtentPtr extent)
    {
        // get the key from the last row of the extent and add it to the primary index
        FieldArrayPtr key_fields = _schema->get_fields(_primary_key);
        FieldArrayPtr value_fields = std::make_shared<FieldArray>(1);
        (*value_fields)[0] = std::make_shared<ConstTypeField<uint64_t>>(extent_id);

        auto &&pvalue = std::make_shared<KeyValueTuple>(key_fields, value_fields, extent->back());
        _primary_index->insert(pvalue);

        SPDLOG_DEBUG_MODULE(LOG_BTREE, "populate primary key: {}", pvalue->to_string());

        // go through each row and pass the relevant key to each of the secondary indexes for insertion
        value_fields->resize(2);
        uint32_t row_id = 0;
        for (auto &&row : *extent) {
            (*value_fields)[1] = std::make_shared<ConstTypeField<uint32_t>>(row_id);

            for (int i = 0; i < _secondary_indexes.size(); ++i) {
                auto &secondary = _secondary_indexes[i];
                key_fields = _schema->get_fields(_secondary_keys[i]);

                auto &&svalue = std::make_shared<KeyValueTuple>(key_fields, value_fields, row);
                SPDLOG_DEBUG_MODULE(LOG_BTREE, "Secondary populate {}", svalue->to_string());
                secondary->insert(svalue);
            }

            ++row_id;
        }
        SPDLOG_DEBUG_MODULE(LOG_BTREE, "Populated {} secondary rows", extent->row_count());
    }

    std::vector<uint64_t>
    MutableTable::finalize()
    {
        // flush the dirty pages of the table to disk
        _cache->evict(shared_from_this());

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
        auto filename = fmt::format("roots.{}", _target_xid);
        auto root_handle = IOMgr::get_instance()->open(_table_dir / filename,
                                                       IOMgr::IO_MODE::APPEND, true);

        // flush and wait for completion
        extent->async_flush(root_handle).wait();
        root_handle->sync();

        // swap the symlink
        std::filesystem::create_symlink(_table_dir / filename, _table_dir / "roots.new");
        std::filesystem::rename(_table_dir / "roots.new", _table_dir / "roots");

        return roots;
    }

    void
    MutableTable::_insert_direct(TuplePtr value,
                                 uint64_t xid,
                                 uint64_t extent_id)
    {
        // get the page from the cache
        auto page = _cache->get(extent_id, shared_from_this());

        // add the row to the page
        page->insert(value);

        // release the page back to the write cache
        _cache->release(page);
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
        auto page = _cache->get(extent_id, shared_from_this());

        // append the value to the extent
        page->append(value);

        // release the extent back to the write cache
        // note: the primary index is just a btree of extent IDs in the no-primary-key scenario
        _cache->release(page);
    }

    void
    MutableTable::_insert_by_lookup(TuplePtr value,
                                    uint64_t xid)
    {
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
        auto page = _cache->get(extent_id, shared_from_this());

        // add the row to the page
        page->upsert(value);

        // release the page back to the write cache
        _cache->release(page);
    }

    void
    MutableTable::_upsert_by_lookup(TuplePtr value,
                                    uint64_t xid)
    {
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
        auto page = _cache->get(extent_id, shared_from_this());

        // remove the row from the page
        // note: this can only be used when a primary key is present, otherwise use _remove_by_scan()
        page->remove(value);

        // release the page back to the write cache
        _cache->release(page);
    }

    void
    MutableTable::_remove_by_lookup(TuplePtr key,
                                    uint64_t xid)
    {
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

            // XXX need a way to get a clean page that can be preferentially released if it doesn't contain the row
            auto page = _cache->get(extent_id, shared_from_this());

            auto &&j = page->begin();
            while (!found && j != page->end()) {
                if (value->equal(FieldTuple(fields, *j))) {
                    page->remove(j);
                    found = true;
                } else {
                    ++j;
                }
            }

            _cache->release(page);

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
        auto page = _cache->get(extent_id, shared_from_this());

        // update the row in the page
        // note: this can only be used when a primary key is present, otherwise update should have been split
        page->update(value);

        // release the page back to the write cache
        _cache->release(page);
    }

    void
    MutableTable::_update_by_lookup(TuplePtr value,
                                    uint64_t xid)
    {
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
