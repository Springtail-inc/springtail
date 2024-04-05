#include <storage/table.hh>

namespace springtail {

    bool
    Table::has_primary()
    {
        return !_primary_key.empty();
    }

    uint64_t
    Table::primary_lookup(TuplePtr tuple)
    {
        // always returns an iterator to a leaf entry where the key *could* exist in the table
        auto &&i = _primary_index->find_for_update(tuple, _xid);
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
        auto &&index_i = _primary_index->begin(_xid);
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
        return std::make_shared<Extent>(_schema, response->data);
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
        FieldArrayPtr key_fields = _primary_index->get_key_fields();
            
        // remove the primary index entry
        auto &&pkey = std::make_shared<FieldTuple>(key_fields, extent->back());
        _primary_index->remove(pkey);

        // setup the value fields for the secondary indexes
        FieldArrayPtr value_fields = std::make_shared<FieldArray>(2);
        value_fields->at(0) = std::make_shared<ConstTypeField<uint64_t>>(extent_id);

        // go through each row and pass the relevant key to each of the secondary indexes for removal
        uint32_t row_id;
        for (auto &&row : *extent) {
            value_fields->at(1) = std::make_shared<ConstTypeField<uint32_t>>(row_id);

            for (auto &&secondary : _secondary_indexes) {
                key_fields = secondary->get_key_fields();

                auto &&skey = std::make_shared<KeyValueTuple>(key_fields, value_fields, row);
                secondary->remove(skey);
            }

            ++row_id;
        }
    }

    void
    MutableTable::populate_indexes(uint64_t extent_id,
                                   ExtentPtr extent)
    {
        // get the key from the last row of the extent and add it to the primary index
        FieldArrayPtr key_fields = _primary_index->get_key_fields();
        FieldArrayPtr value_fields = std::make_shared<FieldArray>(1);
        (*value_fields)[0] = std::make_shared<ConstTypeField<uint64_t>>(extent_id);

        auto &&pvalue = std::make_shared<KeyValueTuple>(key_fields, value_fields, extent->back());
        _primary_index->insert(pvalue);

        // go through each row and pass the relevant key to each of the secondary indexes for insertion
        value_fields->resize(2);
        uint32_t row_id;
        for (auto &&row : *extent) {
            (*value_fields)[1] = std::make_shared<ConstTypeField<uint32_t>>(row_id);

            for (auto &&secondary : _secondary_indexes) {
                key_fields = secondary->get_key_fields();

                auto &&svalue = std::make_shared<KeyValueTuple>(key_fields, value_fields, row);
                secondary->insert(svalue);
            }
        }
    }

    std::vector<uint64_t>
    MutableTable::finalize()
    {
        // flush the dirty pages of the table to disk
        _cache->evict(shared_from_this());

        // now flush the indexes, storing the roots
        std::vector<uint64_t> roots;
        roots.push_back(_primary_index->finalize());

        for (auto secondary : _secondary_indexes) {
            roots.push_back(secondary->finalize());
        }

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
        auto i = _primary_lookup->lower_bound(search_key, xid);
        uint64_t extent_id = _primary_extent_id_f->get_uint64(*i);

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
        page->insert(value);

        // release the page back to the write cache
        _cache->release(page);
    }

    void
    MutableTable::_upsert_by_lookup(TuplePtr value,
                                    uint64_t xid)
    {
        // we didn't receive an extent_id, so we need to look up the extent from the primary index
        auto search_key = _schema->tuple_subset(value, _primary_key);
        auto i = _primary_lookup->lower_bound(search_key, xid);
        uint64_t extent_id = _primary_extent_id_f->get_uint64(*i);

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
        auto i = _primary_lookup->lower_bound(key, xid);
        uint64_t extent_id = _primary_extent_id_f->get_uint64(*i);

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
        auto &&i = _primary_lookup->begin(xid);
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
                    continue;
                }
                ++j;
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
        auto i = _primary_lookup->lower_bound(search_key, xid);
        uint64_t extent_id = _primary_extent_id_f->get_uint64(*i);

        // then we can do a direct update
        _update_direct(value, xid, extent_id);
    }

}
