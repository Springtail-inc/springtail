#include <storage/data_cache.hh>
#include <storage/table.hh>

namespace springtail {
    DataCache::Page::Page(uint64_t extent_id,
                          std::shared_ptr<MutableTable> table)
        : _index(nullptr),
          _table(table)
    {
        // read the initial extent
        auto extent = _read_extent(extent_id);
        _extent_map.insert({ extent_id, { extent } });
        _size += extent->byte_count();
    }

    void
    DataCache::Page::insert(TuplePtr value)
    {
        // lock the page
        boost::unique_lock lock(_mutex);

        auto key = _schema->tuple_subset(value, _keys);
        auto pos_i = _lower_bound(key);

        // insert the new row
        ExtentPtr e = *(pos_i.extent_i);
        Extent::Row row = e->insert(pos_i.row_i);
        MutableTuple(_fields, row).assign(value);

        // check if we need to split the extent
        _check_split(pos_i);
    }

    void
    DataCache::Page::remove(TuplePtr key)
    {
        boost::unique_lock lock(_mutex);

        // find the entry
        auto pos_i = _lower_bound(key);

        // make sure we got an exact match
        if (key->less_than(MutableTuple(_key_fields, *pos_i))) {
            SPDLOG_ERROR("Tried to remove non-existant key");
            return;
        }

        // remove the row
        ExtentPtr e = *(pos_i.extent_i);
        e->remove(pos_i.row_i);

        // note: we currently rely on the GC-3 to perform all data merging, but it would
        //       make sense to merge in-memory extents here
    }

    void
    DataCache::Page::update(TuplePtr value)
    {
        boost::unique_lock lock(_mutex);

        // find the entry
        auto key = _schema->tuple_subset(value, _keys);
        auto pos_i = _lower_bound(key);

        // make sure we got an exact match
        if (key->less_than(MutableTuple(_key_fields, *pos_i))) {
            SPDLOG_ERROR("Tried to remove non-existant key");
            return;
        }

        // update the row
        ExtentPtr e = *(pos_i.extent_i);
        MutableTuple(_fields, *(pos_i.row_i)).assign(value);

        // check if we need to split the extent
        _check_split(pos_i);
    }

    void
    DataCache::Page::flush(bool finalize)
    {
        // write the dirty extents to the data file
        for (auto &&entry : _extent_map) {
            // remove the internal btree index entry for the old extent ID
            // note: no need to remove if the entry is still the original extent
            if (entry.first != _id) {
                auto tuple = std::make_shared<MutableTuple>(_key_fields, entry.second.back()->back());
                _index->remove(tuple);
            }

            // get the fields of the btree which are the <key fields, extent ID field>
            FieldArrayPtr key_fields = _index->get_key_fields();
            FieldArrayPtr value_fields = std::make_shared<FieldArray>(1);

            // write out each extent from the list
            for (auto &&extent : entry.second) {
                uint64_t extent_id = _write_extent(extent);

                // set the extent ID as the value
                (*value_fields)[0] = std::make_shared<ConstTypeField<uint64_t>>(extent_id);

                // add the new extent ID into the internal btree index
                auto &&row = extent->back();
                _index->insert(std::make_shared<KeyValueTuple>(key_fields, value_fields, row));

                if (_cache->_for_gc) {
                    // re-populate the indexes of the table
                    _table->populate_indexes(extent_id, extent);
                }
            }
        }

        // clear the extent map
        // note: clean extents are cached in the main data cache if they need to be re-read
        _extent_map.clear();
    }

    uint64_t
    DataCache::Page::table_id() const
    {
        return _table->id();
    }

    DataCache::Page::Iterator&
    DataCache::Page::Iterator::operator++()
    {
        // move to the next row in the extent
        ++row_i;
        if (row_i != (*extent_i)->end()) {
            return *this;
        }

        // if we reached the end of the extent, move to the next extent in the vector
        ++extent_i;
        if (extent_i != map_i->second.end()) {
            row_i = (*extent_i)->begin();
            return *this;
        }

        // if we reached the end of the vector, move to the next extent in the BTree
        ++index_i;
        if (index_i != page->_index->end()) {
            // check if we have the extent in memory
            uint64_t extent_id = page->_extent_id_f->get_uint64(*index_i);
            map_i = page->_extent_map.find(extent_id);
            if (map_i == page->_extent_map.end()) {
                // read the extent into memory
                auto extent = page->_read_extent(extent_id);
                map_i = page->_extent_map.insert({extent_id, { extent } }).first;
            }

            // we now are guaranteed to have the extent in memory
            extent_i = map_i->second.begin();
            row_i = (*extent_i)->begin();
            return *this;
        }

        return *this;
    }

    DataCache::Page::Iterator
    DataCache::Page::_lower_bound(TuplePtr search_key)
    {
        // if the extent_map contains the original extent_id, then we've never flushed data
        // to disk, so no need to search the btree index
        TempBTree::Iterator index_i = _index->end();
        auto map_i = _extent_map.find(_id);
        if (map_i == _extent_map.end()) {
            // the original extent was flushed, so we need to search the btree
            index_i = _index->lower_bound(search_key);
            auto extent_id = _extent_id_f->get_uint64(*index_i);

            // check if the extent we need is already cached
            map_i = _extent_map.find(extent_id);
            if (map_i == _extent_map.end()) {
                // need to read the extent from disk
                auto extent = _read_extent(extent_id);
                map_i = _extent_map.insert({ extent_id, { extent } }).first;
            }
        }

        // once we have the list of extents, search them for the insert position
        auto &extents = map_i->second;

        // search the extent vector
        auto extent_i = std::lower_bound(extents.begin(), extents.end(), search_key,
                                         [this](const ExtentPtr &extent, TuplePtr key) {
                                             return MutableTuple(this->_key_fields, extent->back()).less_than(key);
                                         });

        // search within the extent
        ExtentPtr e = *extent_i;
        auto row_i = std::lower_bound(e->begin(), e->end(), search_key,
                                      [this](const Extent::Row &row, TuplePtr key) {
                                          return MutableTuple(this->_key_fields, row).less_than(key);
                                      });

        return Iterator(this, index_i, map_i, extent_i, row_i);
    }

    ExtentPtr
    DataCache::Page::_read_extent(uint64_t extent_id)
    {
        // read the extent from disk and create a page for it
        auto response = _cache->_handle->read(extent_id);
            
        // unpack the header to determine the extent type
        ExtentHeader header(response->data[0]);

        // construct the extent
        ExtentPtr extent = std::make_shared<Extent>(_table->schema(), response->data);

        if (_cache->_for_gc) {
            // we assume that the extent is being read for modification, so we need to
            // invalidate the secondary index entries for this extent
            _table->invalidate_indexes(extent_id, extent);
        }

        return extent;
    }

    uint64_t
    DataCache::Page::_write_extent(ExtentPtr extent)
    {
        // write the extent to disk
        // XXX when not performing GC we should keep these extents in a look-aside file; how to track?
        auto &&response = _extent->async_flush(_cache->_handle).get();
        auto extent_id = response->offset;

        // cache the clean page in case it's needed again
        _cache->release(extent_id, extent);

        return extent_id;
    }

    void
    DataCache::Page::_check_split(Iterator pos_i)
    {
        ExtentPtr e = *(pos_i.extent_i);

        // check the size of the extent
        if (e->byte_count() < constant::MAX_EXTENT_SIZE) {
            return;
        }

        // extent has grown too large, split it
        auto &&pair = e->split();
        _size -= e->byte_count();

        // XXX remove the existing entry and insert the two new ones
        auto pos = pos_i.map_i->second.erase(pos_i.extent_i);
        pos = pos_i.map_i->second.insert(pos, pair.second);
        pos_i.map_i->second.insert(pos, pair.first);

        _size += pair.first->byte_count() + pair.second->byte_count();

        // XXX if the page is getting too large then we may need to proactively flush it to
        //     disk like we do for the BTree.  Less critical here given the lack of lock
        //     contention, but from a memory pressure perspective it will be necessary.

        // XXX would be great if we could somehow keep an LRU of dirty extents and use that
        //     to evict just a single extent, or a single vector of extents
    }

    DataCache::PagePtr
    DataCache::get(uint64_t extent_id,
                   std::shared_ptr<MutableTable> table)
    {
        boost::unique_lock lock(_mutex);

        // check the cache
        auto page_i = _cache.find({ table->id(), extent_id });
        if (page_i != _cache.end()) {
            // increase the use count
            ++std::get<2>(page_i->second);

            // remove from the LRU list
            _lru.erase(std::get<1>(page_i->second));
            std::get<1>(page_i->second) = _lru.end();

            // return the page
            return std::get<0>(page_i->second);
        }

        // create and populate the page
        auto page = std::make_shared<Page>(extent_id, table);
        _cache.insert({ { table->id(), extent_id }, { page, _lru.end(), 0 } });

        // XXX how / when do we flush pages out of the cache?  Pages increase when being mutated...

        return page;
    }

    void
    DataCache::release(PagePtr page)
    {
        boost::unique_lock lock(_mutex);

        // find the cache entry
        auto cache_i = _cache.find({ page->table_id(), page->id() });
        assert(cache_i != _cache.end());

        // reduce the usage count on the page
        --std::get<2>(cache_i->second);

        // place the page onto the LRU list
        if (std::get<2>(cache_i->second) == 0) {
            auto pos = _lru.insert(_lru.end(), page);
            std::get<1>(cache_i->second) = pos;
        }
    }

    void
    DataCache::flush(std::shared_ptr<Table> table)
    {
        boost::unique_lock lock(_mutex);

        auto i = _cache.lower_bound({ table->id(), 0 });
        while (i != _cache.end() && i->first.first == table->id()) {
            // no one can be using the table's pages
            assert(std::get<2>(i->second) == 0);

            // flush the page
            std::get<0>(i->second)->flush(false);

            // clear the page from the LRU list?
            _lru.erase(std::get<1>(i->second));

            // move to the next entry
            ++i;
        }
    }

    void
    DataCache::discard(std::shared_ptr<Table> table)
    {
        boost::unique_lock lock(_mutex);

        auto i = _cache.lower_bound({ table->id(), 0 });
        while (i != _cache.end() && i->first.first == table->id()) {
            // no one can be using the table's pages
            assert(std::get<2>(i->second) == 0);

            // flush the page
            std::get<0>(i->second)->flush(true);

            // move to the next entry
            auto j = i++;

            // remove the page from the cache
            _lru.erase(std::get<1>(j->second));
            _cache.erase(j);
        }
    }

}
