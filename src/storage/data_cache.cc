#include <storage/data_cache.hh>
#include <storage/table.hh>

namespace springtail {
    DataCache::Page::Page(DataCache *cache,
                          uint64_t extent_id,
                          std::shared_ptr<MutableTable> table)
        : _cache(cache),
          _id(extent_id),
          _table(table)
    {
        // read the initial extent
        ExtentPtr extent;
        if (extent_id == constant::UNKNOWN_EXTENT) {
            extent = std::make_shared<Extent>(_table->schema(), ExtentType(), _table->target_xid());
        } else {
            extent = _read_extent(_id);
        }
        _extents.push_back(extent);
        _size += extent->byte_count();

        _key_fields = _table->schema()->get_mutable_fields(_table->primary_key());
        _fields = _table->schema()->get_mutable_fields();
    }

    void
    DataCache::Page::insert(TuplePtr value)
    {
        // lock the page
        boost::unique_lock lock(_mutex);

        auto key = _table->schema()->tuple_subset(value, _table->primary_key());
        auto pos_i = _lower_bound(key);

        if (pos_i == end()) {
            // if this value is past the end, append it instead
            ExtentPtr e = _extents.back();
            Extent::Row row = e->append();
            MutableTuple(_fields, row).assign(value);

            // move the iterator back to the last entry
            --pos_i.extent_i;
        } else {
            // insert the new row
            ExtentPtr e = *(pos_i.extent_i);
            Extent::Row row = e->insert(pos_i.row_i);
            MutableTuple(_fields, row).assign(value);
        }

        // check if we need to split the extent
        _check_split(pos_i.extent_i);
    }

    void
    DataCache::Page::append(TuplePtr value)
    {
        // lock the page
        boost::unique_lock lock(_mutex);

        // append the new row
        ExtentPtr e = _extents.back();
        Extent::Row row = e->append();
        MutableTuple(_fields, row).assign(value);

        // check if we need to split the extent
        _check_split(--(_extents.end()));
    }

    void
    DataCache::Page::upsert(TuplePtr value)
    {
        // lock the page
        boost::unique_lock lock(_mutex);

        auto key = _table->schema()->tuple_subset(value, _table->primary_key());
        auto pos_i = _lower_bound(key);

        if (pos_i == end()) {
            // if this value is past the end, append it instead
            ExtentPtr e = _extents.back();
            Extent::Row row = e->append();
            MutableTuple(_fields, row).assign(value);

            // move the iterator back to the last entry
            --pos_i.extent_i;
        } else {
            // check if it's an exact match
            if (key->less_than(MutableTuple(_key_fields, *pos_i))) {
                // key doesn't exist, do a regular insert
                ExtentPtr e = *(pos_i.extent_i);
                Extent::Row row = e->insert(pos_i.row_i);
                MutableTuple(_fields, row).assign(value);
            } else {
                // key exists, replace it
                MutableTuple(_fields, *pos_i).assign(value);
            }
        }

        // check if we need to split the extent
        _check_split(pos_i.extent_i);
    }

    void
    DataCache::Page::remove(TuplePtr key)
    {
        boost::unique_lock lock(_mutex);

        // find the entry
        auto pos = _lower_bound(key);

        // make sure we got an exact match
        if (key->less_than(MutableTuple(_key_fields, *pos))) {
            SPDLOG_ERROR("Tried to remove non-existant key: {} != {}",
                         key->to_string(), MutableTuple(_key_fields, *pos).to_string());
            return;
        }

        // call the internal remove
        _remove(pos);
    }

    void
    DataCache::Page::remove(const Iterator &pos)
    {
        boost::unique_lock lock(_mutex);

        // call the internal remove
        _remove(pos);
    }

    void
    DataCache::Page::update(TuplePtr value)
    {
        boost::unique_lock lock(_mutex);

        // find the entry
        auto key = _table->schema()->tuple_subset(value, _table->primary_key());
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
        _check_split(pos_i.extent_i);
    }

    void
    DataCache::Page::flush()
    {
        // only flush to disk if this is for the GC?
        if (!_cache->_for_gc) {
            return;
        }

        // write out each extent from the list
        for (auto &&extent : _extents) {
            uint64_t extent_id = _write_extent(extent);

            // re-populate the indexes of the table
            _table->populate_indexes(extent_id, extent);
        }
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
        if (extent_i != page->_extents.end()) {
            row_i = (*extent_i)->begin();
            return *this;
        }

        // we've reached the end of the page
        return *this;
    }

    void
    DataCache::Page::_remove(const Iterator &pos)
    {
        // remove the row
        ExtentPtr e = *(pos.extent_i);
        e->remove(pos.row_i);

        // note: we currently rely on the GC-3 to perform all data merging, but it would
        //       make sense to merge in-memory extents here
    }

    DataCache::Page::Iterator
    DataCache::Page::_lower_bound(TuplePtr search_key)
    {
        // check for empty page
        if (_extents.size() == 1 && _extents[0]->empty()) {
            return end();
        }

        // search the extent vector
        auto extent_i = std::lower_bound(_extents.begin(), _extents.end(), search_key,
                                         [this](const ExtentPtr &extent, TuplePtr key) {
                                             MutableTuple tuple(this->_key_fields, extent->back());
                                             return tuple.less_than(key);
                                         });
        if (extent_i == _extents.end()) {
            return end();
        }

        // search within the extent
        ExtentPtr e = *extent_i;
        auto row_i = std::lower_bound(e->begin(), e->end(), search_key,
                                      [this](const Extent::Row &row, TuplePtr key) {
                                          MutableTuple tuple(this->_key_fields, row);
                                          return tuple.less_than(key);
                                      });

        return Iterator(this, extent_i, row_i);
    }

    ExtentPtr
    DataCache::Page::_read_extent(uint64_t extent_id)
    {
        // read the extent from disk and create a page for it
        auto handle = IOMgr::get_instance()->open(_table->data_file(),
                                                  IOMgr::IO_MODE::READ, true);
        auto response = handle->read(extent_id);
            
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
        auto handle = IOMgr::get_instance()->open(_table->data_file(),
                                                  IOMgr::IO_MODE::APPEND, true);
        auto &&response = extent->async_flush(handle).get();
        auto extent_id = response->offset;

        return extent_id;
    }

    void
    DataCache::Page::_check_split(std::vector<ExtentPtr>::iterator extent_i)
    {
        ExtentPtr e = *extent_i;

        // check the size of the extent
        if (e->byte_count() < constant::MAX_EXTENT_SIZE) {
            return;
        }

        // extent has grown too large, split it
        auto &&pair = e->split();
        _size -= e->byte_count();

        // remove the existing entry and insert the two new ones
        auto pos = _extents.erase(extent_i);
        pos = _extents.insert(pos, pair.second);
        _extents.insert(pos, pair.first);

        _size += pair.first->byte_count() + pair.second->byte_count();

        // XXX Eventually, if the page is getting too large then we will need to proactively flush
        //     it to disk like we do for the BTree.  But not doing yet.

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
            if (std::get<1>(page_i->second) != _lru.end()) {
                _lru.erase(std::get<1>(page_i->second));
                std::get<1>(page_i->second) = _lru.end();
            }

            // return the page
            return std::get<0>(page_i->second);
        }

        // create and populate the page
        auto page = std::make_shared<Page>(this, extent_id, table);
        _cache.insert({ { table->id(), extent_id }, { page, _lru.end(), 1 } });

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
    DataCache::evict(std::shared_ptr<MutableTable> table,
                     bool flush)
    {
        boost::unique_lock lock(_mutex);

        auto i = _cache.lower_bound({ table->id(), 0 });
        while (i != _cache.end() && i->first.first == table->id()) {
            // no one can be using the table's pages
            assert(std::get<2>(i->second) == 0);

            if (flush) {
                // flush the page
                std::get<0>(i->second)->flush();
            }

            // move to the next entry, prepare to remove current entry
            auto j = i++;

            // clear the page from the cache
            _lru.erase(std::get<1>(j->second));
            _cache.erase(j);
        }
    }

}
