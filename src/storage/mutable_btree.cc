#include <common/constants.hh>
#include <common/json.hh>
#include <common/properties.hh>
#include <storage/mutable_btree.hh>
#include <storage/vacuumer.hh>

namespace springtail {

    MutableBTree::MutableBTree(const std::filesystem::path &file,
                               const std::vector<std::string> &keys,
                               ExtentSchemaPtr schema,
                               uint64_t xid, uint64_t max_extent_size)
        : _file(file),
          _sort_keys(keys),
          _xid(xid),
          _max_extent_size(max_extent_size),
          _finalized(true)
    {
        nlohmann::json json = Properties::get(Properties::STORAGE_CONFIG);
        uint64_t size = Json::get_or<uint64_t>(json, "btree_cache_size", 512);
        _max_extent_per_page = Json::get_or<uint64_t>(json, "max_extent_per_page", MAX_EXTENT_COUNT);

        _cache = std::make_shared<PageCache>(size);

        // initialize the schema information
        _init_schemas(schema, keys);
    }

    void
    MutableBTree::init_empty()
    {
        // must not have already called init() or init_empty()
        CHECK_EQ(_root, nullptr);

        // construct an empty extent
        auto cache_page = StorageCache::get_instance()->get(_file, constant::UNKNOWN_EXTENT, _xid, constant::LATEST_XID, _max_extent_size);

        // create an empty root
        _root = std::make_shared<Page>(this, std::move(cache_page), _leaf_schema);

        // add the root to the cache
        // note: we do not release the root, leaving it's use-count in the local cache permanently at 1
        boost::unique_lock cache_lock(_cache->mutex);
        _cache_insert(_root, cache_lock, false);

        // mark the btree as mutable
        _finalized = false;
    }

    void
    MutableBTree::init(uint64_t root_offset)
    {
        // must not have already called init() or init_empty()
        CHECK_EQ(_root, nullptr);

        // construct an empty page to populate
        _root = std::make_shared<Page>(this, root_offset);

        // read the root
        _read_page_internal(_root);

        // add the root to the cache
        // note: we do not release it, leaving it's use-count permanently at 1
        boost::unique_lock cache_lock(_cache->mutex);
        _cache_insert(_root, cache_lock, false);

        // mark the btree as mutable
        _finalized = false;
    }

    void
    MutableBTree::insert(TuplePtr value)
    {
        // must have called init() or init_empty()
        assert(_root != nullptr);

        // acquire a shared lock on the btree
        boost::shared_lock tree_lock(_mutex);

        // make sure that we can modify this tree
        assert(!_finalized);

        // get the search key for this value
        TuplePtr search_key = _leaf_schema->tuple_subset(value, _sort_keys);

        // traverse the tree to find the correct leaf page along with the pages along the path
        // to it from the root, get exclusive lock on the leaf
        NodePtr node = _find_leaf(search_key);
        PagePtr parent = (node->parent) ? node->parent->page : nullptr;

        // insert the value into the page
        boost::unique_lock page_lock(node->page->mutex);
        node->page->insert(search_key, value, _leaf_fields);

        // check if this page needs to be flushed
        bool do_flush = node->page->check_flush();
        page_lock.unlock();

        if (do_flush) {
            // will first lock the parent, then lock the page and flush it
            auto root_node = _lock_and_flush_page(node);
            if (root_node) {
                CHECK((*root_node)->parent == nullptr);
                // no longer need to hold the lock on the entire tree
                tree_lock.unlock();

                // will exclusive lock the tree and flush the root
                _lock_and_flush_root(*root_node);
            }
        }

        boost::unique_lock cache_lock(_cache->mutex);
        if (!do_flush) {
            // need to update the cache with the updated size of the page
            _cache_update_size(node->page, cache_lock);
        }

        // release the nodes back to the cache
        // note: we don't call release on the root since it's never acquired via _cache_get()
        while (node->parent != nullptr) {
            _cache_release(node->page);

            // note: the auto-destructor will implicitly unlock the node's lock if one is held
            node = node->parent;
        }
    }

    void
    MutableBTree::remove(TuplePtr value)
    {
        // must have called init() or init_empty()
        assert(_root != nullptr);

        // acquire a shared lock on the btree
        boost::shared_lock tree_lock(_mutex);

        // make sure that we can modify this tree
        assert(!_finalized);

        // get the search key for this value
        TuplePtr search_key = _leaf_schema->tuple_subset(value, _sort_keys);

        // traverse the tree to find the appropriate page to remove from
        NodePtr node = _find_leaf(search_key);

        // remove the value from the page
        boost::unique_lock page_lock(node->page->mutex);
        node->page->remove(search_key);

        // check if this page needs to be removed
        bool is_empty = node->page->empty();
        page_lock.unlock();

        if (is_empty && node->parent == nullptr) {
            // if the root page is empty, we can return immediately
            return;
        }

        // check for empty non-root page
        if (is_empty) {
            _lock_and_remove_page(node);
        }

        boost::unique_lock cache_lock(_cache->mutex);
        if (!is_empty) {
            // need to update the cache with the updated size of the page
            _cache_update_size(node->page, cache_lock);
        }

        // release the nodes back to the cache
        // note: we don't call release on the root since it's never acquired via _cache_get()
        while (node->parent != nullptr) {
            _cache_release(node->page);

            // note: the auto-destructor will implicitly unlock the node's lock if one is held
            node = node->parent;
        }
    }

    void
    MutableBTree::truncate()
    {
        // must have called init() or init_empty()
        assert(_root != nullptr);

        // acquire an exclusive lock on the tree here
        boost::unique_lock lock(_mutex);

        // clear the root
        _remove_root();

        // Smart vacuum if index exists
        if (std::filesystem::exists(_file)) {
            Vacuumer::get_instance()->expire_extent(_file, 0, std::filesystem::file_size(_file), _xid);
        } else {
            LOG_INFO("TRUNCATE: File: {} doesn't exist to report to vacuum", _file);
        }
    }

    uint64_t
    MutableBTree::finalize()
    {
        // must have called init() or init_empty()
        assert(_root != nullptr);

        // acquire an exclusive lock on the tree here
        boost::unique_lock lock(_mutex);

        // flush from the root of the tree
        // note: we call _flush_root() directly since we want to hold the tree mutex for longer than
        //       just the flush call
        PagePtr new_root = _flush_root(_root);
        if (new_root != nullptr) {
            _root = new_root;
        }

        // sync the file to ensure all data is written to disk
        auto handle = IOMgr::get_instance()->open(_file, IOMgr::IO_MODE::WRITE, true);
        handle->sync();

        // mark the tree as finalized so that additional changes can't be made at this XID
        _finalized = true;

        // return the new extent_id of the root
        return _root->extent_id;
    }

MutableBTree::NodePtr
MutableBTree::_next_leaf(NodePtr node)
{
    // if the leaf has no parent, then there is no next leaf
    if (node->parent == nullptr) {
        return nullptr;
    }

    // move up the tree
    while (node->page != _root) {
        // create the key for the current page
        auto value = std::make_shared<FieldArray>();
        value->push_back(std::make_shared<ConstTypeField<uint64_t>>(node->page->extent_id));
        auto kv = std::make_shared<KeyValueTuple>(node->page->prev_key->fields(),
                                                  value, node->page->prev_key->row());

        // lock the parent for access
        boost::shared_lock parent_lock(node->parent->page->mutex);

        // find it in the parent
        auto parent_i = node->parent->page->find(kv);

        // move to the next entry
        ++parent_i;

        // release the current page back to the cache
        {
            boost::unique_lock cache_lock(_cache->mutex);
            _cache_release(node->page);
        }

        if (parent_i == node->parent->page->end()) {
            // if that was the last entry, move up the tree further
            node = node->parent;
        } else {
            // we found the next branch to follow, setup the node for the page
            uint64_t extent_id = _branch_child_f->get_uint64(&*parent_i);

            // read the next child page
            NodePtr child = _read_page(extent_id, node->parent, parent_lock);

            // if the child was flushed while we were reading the page, we need to re-find it in the parent
            if (!child) {
                continue;
            }

            // recurse to the child
            node = child;
        }
    }

    // if we were at the last leaf, return nullptr to indiciate end()
    if (node->page == _root) {
        return nullptr;
    }

    // from here, if we don't have a leaf node, then we need to find the first entry of the first
    // leaf node of this sub-tree
    while (node->page->type.is_branch()) {
        // lock page for read access
        boost::shared_lock page_lock(node->page->mutex);

        // use a lower_bound() check to find the appropriate child branch
        auto &&i = node->page->begin();

        // retrieve the child offset
        uint64_t extent_id = _branch_child_f->get_uint64(&*i);

        // read the child page; will handle updating the locks
        // note: this will release the page_lock once it has a pointer to the child in memory
        NodePtr child = _read_page(extent_id, node, page_lock);

        // if the child was flushed while we were reading the page, we need to re-find it in the parent
        if (!child) {
            continue;
        }

        // recurse to the child
        node = child;
    }

    return node;
}

MutableBTree::Iterator
MutableBTree::begin()
{
    // must have called init() or init_empty()
    CHECK(_root != nullptr);

    // acquire a shared lock on the btree
    boost::shared_lock tree_lock(_mutex);

    // safe because the root pointer can't change if we are holding the btree lock
    NodePtr node = std::make_shared<Node>(nullptr, _root);

    // iterate through the levels until we find a leaf page
    while (node->page->type.is_branch()) {
        // lock page for read access
        boost::shared_lock page_lock(node->page->mutex);

        // use a lower_bound() check to find the appropriate child branch
        auto &&i = node->page->begin();

        // retrieve the child offset
        uint64_t extent_id = _branch_child_f->get_uint64(&*i);

        // read the child page; will handle updating the locks
        // note: this will release the page_lock once it has a pointer to the child in memory
        NodePtr child = _read_page(extent_id, node, page_lock);

        // if the child was flushed while we were reading the page, we need to re-find it in the parent
        if (!child) {
            continue;
        }

        // recurse to the child
        node = child;
    }

    return Iterator(this, node);
}

MutableBTree::Iterator
MutableBTree::last()
{
    // must have called init() or init_empty()
    CHECK(_root != nullptr);

    // acquire a shared lock on the btree
    boost::shared_lock tree_lock(_mutex);

    // safe because the root pointer can't change if we are holding the btree lock
    NodePtr node = std::make_shared<Node>(nullptr, _root);

    // iterate through the levels until we find a leaf page
    boost::shared_lock<boost::shared_mutex> page_lock;
    while (node->page->type.is_branch()) {
        // lock page for read access
        page_lock = boost::shared_lock(node->page->mutex);

        // use last() to find the appropriate child branch
        auto &&i = node->page->last();

        // retrieve the child offset
        uint64_t extent_id = _branch_child_f->get_uint64(&*i);

        // read the child page; will handle updating the locks
        // note: this will release the page_lock once it has a pointer to the child in memory
        NodePtr child = _read_page(extent_id, node, page_lock);

        // if the child was flushed while we were reading the page, we need to re-find it in the parent
        if (!child) {
            continue;
        }

        // recurse to the child
        node = child;
    }

    return Iterator(this, node, node->page->last(), std::move(page_lock));
}

MutableBTree::Iterator
MutableBTree::lower_bound(TuplePtr search_key,
                          bool for_update)
{
    // must have called init() or init_empty()
    CHECK(_root != nullptr);

    // acquire a shared lock on the btree
    boost::shared_lock tree_lock(_mutex);

    // traverse the tree to find the page that could contain the search_key
    NodePtr node = _find_leaf(search_key);

    // find the specific entry in the page
    boost::shared_lock page_lock(node->page->mutex);
    auto page_i = node->page->lower_bound(search_key);
    if (page_i == node->page->end()) {
        auto next_node = _next_leaf(node);
        if (next_node == nullptr) {
            if (for_update) {
                page_i = node->page->last();
            } else {
                return Iterator();
            }
        } else {
            node = next_node;
            page_i = node->page->begin();
        }
    }

    return Iterator(this, node, std::move(page_i), std::move(page_lock));
}


    MutableBTree::Page::Iterator
    MutableBTree::Page::lower_bound(TuplePtr search_key)
    {
        return _cache_page->lower_bound(search_key, _schema);
    }

    MutableBTree::Page::Iterator
    MutableBTree::Page::find(TuplePtr search_key)
    {
        // use lower_bound() to do a binary search for the entry
        auto &&i = this->lower_bound(search_key);
        if (i == this->end()) {
            return i;
        }

        // if the key is < the returned row, then there is no matching entry, return end()
        auto row = *i;
        auto key = std::make_shared<MutableTuple>(_key_fields, &row);
        if (search_key->less_than(key)) {
            return this->end();
        }

        // return the Iterator
        return i;
    }

    void
    MutableBTree::Page::update_branch(std::shared_ptr<Page> old_page,
                                      std::vector<std::shared_ptr<Page>> new_pages,
                                      MutableFieldPtr offset_f)
    {
        // remove the old page's key from this branch page
        // note: key should always exist since it's a pointer to an existing child
        _cache_page->remove(old_page->prev_key, _schema);

        // mark the page dirty
        _dirty = true;

        // add the new child pages to the entries
        // note: add them in reverse order so we can keep inserting at the same position
        for (auto &&i = new_pages.rbegin(); i != new_pages.rend(); i++) {
            auto &page = *i;

            LOG_DEBUG(LOG_BTREE, LOG_LEVEL_DEBUG1, "Adding branch entry to child extent_id: {}", page->extent_id);

            // XXX need a better way to create a combined tuple
            auto value = std::make_shared<FieldArray>();
            value->push_back(std::make_shared<ConstTypeField<uint64_t>>(page->extent_id));
            auto kv = std::make_shared<KeyValueTuple>(page->prev_key->fields(), value, page->prev_key->row());

            _cache_page->insert(kv, _schema);
        }
    }

    void
    MutableBTree::Page::insert(TuplePtr search_key,
                               TuplePtr value,
                               MutableFieldArrayPtr fields)
    {
        // mark this page as dirty
        _dirty = true;

        // insert into the backing page
        _cache_page->insert(value, _schema);
    }

    void
    MutableBTree::Page::remove(TuplePtr search_key, bool is_root)
    {
        // mark this page as dirty
        _dirty = true;

        // remove the entry from the backing page
        _cache_page->remove(search_key, _schema);

        // changes the root to a leaf when it has no more children
        // XXX it would be better if the root re-positioned down a level once it had only one entry
        //     in it
        if (is_root && _cache_page->empty()) {
            this->type = ExtentType(false, true);
            _schema = _btree->_leaf_schema;
        }

        // XXX re-merge extents if possible?

        // note: we currently don't perform cross-Page merge here as it's handled during GC vacuum
        //       but we could check if the size of this extent and the one before / after would make
        //       the threshold for a single extent.  One potentially better approach is to keep the
        //       size of the child in the pointer and then do split / merge as we traverse down the
        //       tree rather than cleaning up afterward.
    }

    std::vector<std::shared_ptr<MutableBTree::Page>>
    MutableBTree::Page::flush_empty_root(uint64_t xid)
    {
        // note: we must be holding the disk_mutex, so no need to lock the access mutex
        assert(_cache_page->empty());

        // flushing an empty Page will write an empty extent to disk
        ExtentHeader header(this->type, xid, _schema->row_size(), _schema->field_types(), this->extent_id);
        auto extent_id = _cache_page->flush_empty(header);

        // XXX how to handle the XIDs?
        auto cache_page = StorageCache::get_instance()->get(_btree->_file, extent_id, _btree->_xid, constant::LATEST_XID, _btree->_max_extent_size);
        auto page = std::make_shared<Page>(_btree, extent_id);
        page->set_cache_page(std::move(cache_page), _schema);

        return std::vector<PagePtr>({page});
    }

    std::vector<std::shared_ptr<MutableBTree::Page>>
    MutableBTree::Page::flush(uint64_t xid)
    {
        auto flush_future = async_flush(xid);
        return flush_future.get();
    }

    std::future<std::vector<std::shared_ptr<MutableBTree::Page>>>
    MutableBTree::Page::async_flush(uint64_t xid, std::function<void(std::vector<std::shared_ptr<MutableBTree::Page>>&&)> callback)
    {
        // if the root was split, then remove the root flag from the type
        ExtentType type = (_cache_page->extent_count() > 1)
            ? ExtentType(this->type.is_branch())
            : this->type;

        // flush the backing page
        ExtentHeader header(type, xid, _schema->row_size(), _schema->field_types(), this->extent_id);

        auto promise = std::make_shared<std::promise<std::vector<std::shared_ptr<MutableBTree::Page>>>>();
        auto future = promise->get_future();
        LOG_INFO("ABOUT TO FLUSH EXTENTS");
        _cache_page->async_flush(std::move(header), [p = std::move(promise), callback = std::move(callback), this](std::vector<uint64_t> ids){
                std::vector<std::shared_ptr<MutableBTree::Page>> new_pages;

                LOG_INFO("GOT THE NEW EXTENT IDS");
                for (auto id : ids) {
                    // XXX how to handle XIDs?
                    auto cache_page = StorageCache::get_instance()->get(_btree->_file, id, _btree->_xid, constant::LATEST_XID, _btree->_max_extent_size);

                    // XXX need a better way to create these combined tuples
                    ValueTuplePtr value_key;
                    {
                        auto row_i = cache_page->last(); // note: need to hold this iterator to pin extent
                        auto row = *row_i;
                        auto key = std::make_shared<MutableTuple>(_key_fields, &row);
                        value_key = std::make_shared<ValueTuple>(key);
                    }

                    auto page = std::make_shared<Page>(_btree, id, std::move(value_key), std::move(cache_page), _schema);

                    LOG_DEBUG(LOG_BTREE, LOG_LEVEL_DEBUG1, "Creating MutableBTree Page: {} {}", id, page->extent_id);

                    new_pages.push_back(std::move(page));
                }

                p->set_value(new_pages);
                if (callback) {
                    callback(std::move(new_pages));
                }
        });

        return future;
    }

    void
    MutableBTree::Page::set_cache_page(StoragePagePtr cache_page,
                                       ExtentSchemaPtr schema)
    {
        // note: we must be holding the disk_mutex, so no need to lock the access mutex

        // note: page should be empty when this is called
        assert(_cache_page.empty() || _cache_page.ptr()->empty());

        // set the type based on the extent header
        this->type = cache_page->header().type;

        // set the key fields for the page
        _schema = schema;
        _key_fields = schema->get_mutable_fields(schema->get_sort_keys());

        // if the page is not the root then we store the key of the entry we used to find this page
        if (!this->type.is_root()) {
            // force a copy of the values into a ValueTuple
            auto row_i = cache_page->last(); // note: need to hold this iterator to pin extent
            auto row = *row_i;
            auto key = std::make_shared<MutableTuple>(_key_fields, &row);
            this->prev_key = std::make_shared<ValueTuple>(key);
        }

        // replace the backing page
        _cache_page = std::move(cache_page);
    }

    MutableBTree::PagePtr
    MutableBTree::_cache_get(uint64_t extent_id)
    {
        // find the entry if it exists
        auto &&i = _cache->lookup.find({_file, extent_id});
        if (i == _cache->lookup.end()) {
            return nullptr;
        }
        PageCache::LookupEntry &entry = i->second;

        // remove the page from the LRU list if on the list
        if (std::get<2>(entry) == 0) {
            _cache->lru.erase(std::get<1>(entry));
        }

        // increment the usage counter
        ++std::get<2>(entry);

        // return the page
        return std::get<0>(entry);
    }

    void
    MutableBTree::_cache_release(PagePtr page)
    {
        // find the entry; must exist in the cache since can't be chosen for eviction while it is in use
        auto &&i = _cache->lookup.find(page->get_lookup_id());

        // it's possible that the page was evicted due to a flush, in which case it will no
        // longer be in the cache
        if (i == _cache->lookup.end()) {
            return;
        }

        // decrement the usage count
        int &usage = std::get<2>(i->second);
        --usage;

        // if zero then add to the LRU list
        if (usage == 0) {
            _cache->lru.push_front(page);
            std::get<1>(i->second) = _cache->lru.begin();
        }
    }

    void
    MutableBTree::_cache_insert_empty(PagePtr page)
    {
        // place the page into the cache, but not into the LRU queue
        // note: set the use count to 1 since the caller is actively using the page
        PageCache::LookupEntry entry{ page, _cache->lru.end(), 1, 0 };
        _cache->lookup.insert_or_assign(page->get_lookup_id(), std::move(entry));
    }

    void
    MutableBTree::_cache_insert(PagePtr page,
                                boost::unique_lock<boost::shared_mutex> &cache_lock,
                                bool release)
    {
        // place the page into the cache, but not into the LRU queue
        _cache_insert_empty(page);

        // update the size of the cache
        _cache_update_size(page, cache_lock);

        // release the page back if it's not going to be in use
        if (release) {
            _cache_release(page);
        }
    }

    void
    MutableBTree::_cache_update_size(PagePtr update_page,
                                     boost::unique_lock<boost::shared_mutex> &cache_lock)
    {
        // update the size of the cache given the page size
        auto &&i = _cache->lookup.find(update_page->get_lookup_id());
        assert(i != _cache->lookup.end());
        PageCache::LookupEntry &entry = i->second;

        // update the size of the cache based on the new size of the entry
        // note: atomic access to the size
        uint32_t page_size = 1; // update_page->size;

        _cache->size = _cache->size - std::get<3>(entry) + page_size;
        std::get<3>(entry) = page_size;

        // evict pages until the size is under the watermark
        while (_cache->size > _cache->max_size) {
            // there should always be an evictable page
            CHECK(!_cache->lru.empty());

            // get a page
            // note: coming off the LRU we know that no one else is currently using this page
            PagePtr page = _cache->lru.back();
            _cache->lru.pop_back();

            // find the hash entry and set the usage count
            auto &lru_entry = _cache->lookup.find(page->get_lookup_id())->second;
            std::get<2>(lru_entry) = 1;

            // save the entry size to reduce the cache size after the page flush
            uint32_t entry_size = std::get<3>(lru_entry);

            // check if we need to try another entry
            // note: if the page is the root we can't evict; if the page has potentially dirty
            //       children, must evict them first
            if (page->type.is_root() || page->has_children()) {
                _cache->lru.push_front(page);
                std::get<1>(lru_entry) = _cache->lru.begin();
                std::get<2>(lru_entry) = 0;
                continue;
            }

            // check if we can remove immediately
            // note: if the page is not dirty, was already flushed, or has never been accessed since being
            //       created from a flush, then can evict immedately
            PagePtr parent = page->parent();
            if (!page->is_dirty() || page->flushed || parent == nullptr) {
                _cache->lookup.erase(page->get_lookup_id());
                _cache->size -= entry_size;
                continue;
            }

            // unlock the cache for others to proceed
            cache_lock.unlock();

            // scoped for locks
            {
                // currently looks evictable and requires a flush; acquire the parent disk_mutex to
                // ensure it doesn't get flushed
                // note: not actually required since the parent would attempt to flush the dirty child before itself
                // boost::shared_lock parent_lock(parent->disk_mutex);

                // lock the page's disk_mutex since we are about to flush
                // note: should never block since we removed the entry from the cache while it wasn't in use
                boost::unique_lock page_lock(page->disk_mutex);

                // check if the page was flushed while we were acquiring locks
                if (!page->flushed) {
                    // flush the page
                    // note: we don't cache the newly created pages since we are trying to free space
                    _flush_page_internal(page, parent);
                }
            }

            // re-lock the cache and continue
            cache_lock.lock();

            // re-find the cache entry
            auto &&i = _cache->lookup.find(page->get_lookup_id());

            // free the space before the next check
            _cache->size -= std::get<3>(i->second);

            // remove the page from the cache now that it has been replaced
            // note: we don't do this earlier since a reader could come in requesting the page and
            //       then there would be a race which could result in the page being re-read from
            //       disk and modified, resulting in invalid behavior
            _cache->lookup.erase(i);
        }
    }

    void
    MutableBTree::_cache_evict(uint64_t extent_id)
    {
        // find the entry if it exists
        auto &&i = _cache->lookup.find({_file, extent_id});
        if (i == _cache->lookup.end()) {
            return; // not an error since someone else may have evicted
        }

        // retrieve the value from the lookup entry
        PageCache::LookupEntry &entry = i->second;
        PagePtr page = std::get<0>(entry);

        // note: no need to flush the page since a direct eviction should only come after
        //       flushing a page
        assert(page->flushed);

        // during a finalize() the entries are flushed without being acquired out of the cache,
        // so in that case we also need to remove it from the LRU list
        if (std::get<2>(entry) == 0) {
            _cache->lru.erase(std::get<1>(entry));
        }

        // remove the entry from the cache
        _cache->size -= std::get<3>(entry);
        _cache->lookup.erase(i);
    }

    void
    MutableBTree::_cache_clear()
    {
        // clear all of the entries from the cache without flushing them
        _cache->lookup.clear();
        _cache->lru.clear();
        _cache->size = 0;
    }

    void
    MutableBTree::_read_page_internal(PagePtr page)
    {
        // now populate the page

        // get the backing page
        // XXX how should we handle the access XID here??
        auto cache_page = StorageCache::get_instance()->get(_file, page->extent_id, _xid, constant::LATEST_XID, _max_extent_size);

        // determine the schema for this page
        ExtentSchemaPtr schema = (cache_page->header().type.is_branch())
            ? _branch_schema
            : _leaf_schema;

        // tie the backing page into the btree page
        page->set_cache_page(std::move(cache_page), schema);
    }

    MutableBTree::NodePtr
    MutableBTree::_read_page(uint64_t extent_id,
                             NodePtr parent,
                             boost::shared_lock<boost::shared_mutex> &parent_lock)
    {
        // lock the cache
        boost::unique_lock cache_lock(_cache->mutex);

        // check if the entry already exists
        PagePtr page = _cache_get(extent_id);
        if (page != nullptr) {
            // no longer need to access the cache, release the lock
            cache_lock.unlock();

            // once we have a pointer to the page we can release the parent
            parent_lock.unlock();

            // note: acquiring the disk_mutex of the new page ensures that the page data has been
            //       populated before continuing
            NodePtr node = std::make_shared<Node>(parent, page);

            // if the page got flushed while we were acquiring the disk_mutex, return and retry
            if (page->flushed) {
                // note: implicitly unlocks the disk_mutex
                return nullptr;
            }

            // update the parent/child inter-link; may already be set but won't change
            page->set_parent(parent->page);
            parent->page->add_child(page);

            // return the new node
            return node;
        }

        // no page in the cache, so construct a page that we can populate
        page = std::make_shared<Page>(this, extent_id);

        // prevent others from using the page until the data is available
        // note: this will never block
        boost::unique_lock data_lock(page->disk_mutex);

        // place the page into the cache, but not into the LRU queue
        // note: this placeholder could be found by other threads, but will block since we will
        //       hold an exclusive lock on the page
        _cache_insert_empty(page);

        // release the cache for other threads until we have populated the data for this page
        // note: the exclusive lock on the page will prevent others from progressing even if
        //       they try to access this page
        cache_lock.unlock();

        // once we have a pointer to the page we can release the parent
        parent_lock.unlock();

        // update the page book-keeping
        page->set_parent(parent->page);
        parent->page->add_child(page);

        // read extent from disk and update the page metadata
        _read_page_internal(page);

        // data has been read, so can let others proceed by downgrading the lock
        NodePtr node = std::make_shared<Node>(parent, page, std::move(data_lock));

        // now re-acquire the cache lock to update the size of the cache
        cache_lock.lock();

        // update the size of the page in the cache; may cause evictions
        _cache_update_size(page, cache_lock);

        // return the node for the new page
        return node;
    }

    MutableBTree::NodePtr
    MutableBTree::_find_leaf(TuplePtr key)
    {
        // safe because the root pointer can't change if we are holding the btree lock
        NodePtr node = std::make_shared<Node>(nullptr, _root);

        // if the root is the leaf, lock and return
        if (_root->type.is_leaf()) {
            return node;
        }

        // iterate through the levels until we find a leaf page
        while (node->page->type.is_branch()) {
            // lock page for read access
            boost::shared_lock page_lock(node->page->mutex);

            // use a lower_bound() check to find the appropriate child branch
            auto &&i = node->page->lower_bound(key);

            // note: we know the tree isn't empty since the root would be a leaf in that case
            if (i == node->page->end()) {
                // if the key is greater than the last entry, then we need to follow the last child
                i = node->page->last();
            }
            Extent::Row row = *i;

            // retrieve the child offset
            uint64_t extent_id = _branch_child_f->get_uint64(&row);

            // read the child page; will handle updating the locks
            // note: this will release the page_lock once it has a pointer to the child in memory
            NodePtr child = _read_page(extent_id, node, page_lock);

            // if the child was flushed while we were reading the page, we need to re-find it in the parent
            if (!child) {
                continue;
            }

            // recurse to the child
            node = child;
        }

        // releases the direct parent's shared lock here
        return node;
    }

    std::future<std::vector<MutableBTree::PagePtr>>
    MutableBTree::_async_flush_page_internal(PagePtr page,
                                       PagePtr parent, std::function<void(std::vector<PagePtr>&&)> callback)
    {
        auto promise = std::make_shared<std::promise<std::vector<PagePtr>>>();
        auto future = promise->get_future();

        page->async_flush(_xid, [page = std::move(page), parent = std::move(parent),
                p = std::move(promise), callback = std::move(callback), this](std::vector<PagePtr> &&new_pages) {
            // flush the page's extent(s) to disk
            // note: it would be nice if we could perform the disk writes without holding the parent
            //       mutex, but then there is a potential race condition between updating the
            //       parent's pointers and flushing the parent.  It might be possible with more
            //       complex logic.

            // lock the parent for exclusive access
            boost::unique_lock parent_lock(parent->mutex);

            // update the parent's pointers
            parent->update_branch(page, new_pages, _branch_child_f);

            // remove the page from the parent's children
            parent->remove_child(page->extent_id);

            // mark the page as flushed
            page->flushed = true;

            p->set_value(new_pages);

            if (callback) {
                callback(std::move(new_pages));
            }

        });

        return future;
    }

    std::vector<MutableBTree::PagePtr>
    MutableBTree::_flush_page_internal(PagePtr page,
                                       PagePtr parent)
    {
        auto flush_future = _async_flush_page_internal(page, parent);
        return flush_future.get();
    }

    std::optional<std::future<void>>
    MutableBTree::_flush_page(PagePtr page,
                              PagePtr parent)
    {
        // acquire exclusive access to the page to write to disk
        boost::unique_lock data_lock(page->disk_mutex);

        // if the page was already flushed, then nothing to do
        if (page->flushed) {
            return std::nullopt;
        }

        // flush the children first
        // note: safe to traverse the children like this because we are holding exclusive lock
        //       on the page
        page->flush_children();

        // if the page was not modified while flushing children, then no need to flush it
        // note: this could occur if pages were accessed for read
        if (!page->is_dirty()) {
            return std::nullopt;
        }

        // check if the page is empty
        if (page->empty()) {
            // if so then we need to process a removal on the parent
            _remove_page_internal(page, parent);

            // evict the removed page from the cache
            boost::unique_lock cache_lock(_cache->mutex);
            _cache_evict(page->extent_id);
            return std::nullopt;
        } else {
            return _flush_and_update_cache(std::move(page), std::move(parent), std::move(data_lock));
        }
    }

    std::future<void>
    MutableBTree::_flush_and_update_cache(PagePtr page, PagePtr parent,
        boost::unique_lock<boost::shared_mutex> data_lock)
    {
        auto promise = std::make_shared<std::promise<void>>();
        auto future = promise->get_future();

        auto flush_future = _async_flush_page_internal(page, parent,
            [page = std::move(page),
            p = std::move(promise), data_lock_ref = &data_lock, this](std::vector<PagePtr> &&new_pages) {

            // evict the page from the cache and add the new pages
            boost::unique_lock cache_lock(_cache->mutex);

            _cache_evict(page->extent_id);
            for (auto &&new_page : new_pages) {
                _cache_insert(new_page, cache_lock);
            }

            p->set_value();
        });

        return future;
    }

    MutableBTree::PagePtr
    MutableBTree::_flush_root(PagePtr page)
    {
        // acquire exclusive access to the root page
        boost::unique_lock data_lock(page->disk_mutex);

        // if the page was flushed, then nothing to do
        if (page->flushed) {
            return nullptr;
        }

        // flush the children first
        // note: safe to traverse the children like this because we are holding
        //       exclusive lock on the page
        page->flush_children();

        // check if this page has actually been modified by the child flushes
        if (!page->is_dirty()) {
            return page;
        }

        // flush the page's extent(s) to disk
        // note: it would be nice if we could perform the disk writes without holding the parent
        //       mutex, but then there is a potential race condition between updating the
        //       parent's pointers and flushing the parent.  It might be possible with more
        //       complex logic.
        std::vector<PagePtr> &&new_pages = (page->empty())
            ? page->flush_empty_root(_xid)
            : page->flush(_xid);

        // check if we need to create a new root above this page
        PagePtr new_root;
        if (new_pages.size() > 1) {
            // construct the new root's extent
            auto cache_page = StorageCache::get_instance()->get(_file, constant::UNKNOWN_EXTENT, _xid, constant::LATEST_XID, _max_extent_size);

            // add pointers to the new root for each new page
            for (PagePtr child : new_pages) {
                auto child_keys = child->index_keys();
                auto child_itr = child->last();
                Extent::Row child_row = *child_itr;
                auto key = std::make_shared<MutableTuple>(child_keys, &child_row);

                LOG_DEBUG(LOG_BTREE, LOG_LEVEL_DEBUG1, "Adding root entry to child extent_id: {}", child->extent_id);

                // XXX need a better way to populate this data
                auto value = std::make_shared<FieldArray>();
                value->push_back(std::make_shared<ConstTypeField<uint64_t>>(child->extent_id));
                auto kv = std::make_shared<KeyValueTuple>(key->fields(), value, key->row());

                cache_page->append(kv, _branch_schema);
            }

            // write the new extent to disk
            ExtentHeader header(ExtentType(true, true), _xid, _branch_schema->row_size(),
                                _branch_schema->field_types(), page->extent_id);
            auto &&ids = cache_page->flush(header);

            // construct a new root page based on the new extent
            uint64_t extent_id = ids[0];
            ValueTuplePtr value_key;
            {
                auto row_i = cache_page->last(); // note: need to hold this iterator to pin extent
                auto row = *row_i;
                auto key = std::make_shared<MutableTuple>(_branch_keys, &row);
                value_key = std::make_shared<ValueTuple>(key);
            }

            new_root = std::make_shared<Page>(this, extent_id,
                                              value_key, std::move(cache_page), _branch_schema);

            // note: we don't need to add these new pages as children because they are clean
            //       pages that haven't been traversed for modification
        } else {
            // if there's just one page, it will replace the root
            new_root = new_pages[0];
            new_pages.pop_back();
        }

        // mark the old root as flushed
        page->flushed = true;

        // evict the old root and add the new pages to the cache since they are no longer roots
        // note: no one should be using this page since we have exclusive lock on both the
        //       parent and the page, meaning no one is holding the page and no one could have
        //       found the page again after releasing it
        boost::unique_lock cache_lock(_cache->mutex);

        _cache_evict(page->extent_id);
        for (auto &&new_page : new_pages) {
            _cache_insert(new_page, cache_lock);
        }

        // insert the new root with a use-count of 1
        _cache_insert(new_root, cache_lock, false);

        return new_root;
    }

    void
    MutableBTree::_lock_and_flush_root(NodePtr node)
    {
        // release the shared disk_mutex on the root since we want to flush it
        node->lock.unlock();

        // acquire an exclusive lock on the tree here
        boost::unique_lock lock(_mutex);

        // check if this is still the root of the tree or if it's already been flushed
        if (node->page != _root) {
            return;
        }

        // now flush the root
        PagePtr new_root = _flush_root(node->page);

        // set the root pointer for the BTree
        // note; if returned null then was flushed by another thread
        if (new_root != nullptr) {
            _root = new_root;
        }
    }

    std::optional<MutableBTree::NodePtr>
    MutableBTree::_lock_and_flush_page(NodePtr node)
    {
        if (!node->parent) {
            return node;
        }

        std::optional<NodePtr> root_to_flush;
        while (node->parent) {
            PagePtr parent = node->parent->page;

            // note: we are holding a shared lock on the parent's disk_mutex already
            // if the parent has already been flushed, then by definition this page was also flushed
            if (parent->flushed) {
                break;
            }

            // release the shared lock on the disk_mutex since we want to flush the page
            node->lock.unlock();

            // now that we are holding the parent, flush the page
            auto flush_future_opt = _flush_page(node->page, parent);
            if (flush_future_opt) {
                flush_future_opt.value().get();
            }

            // move to the parent
            node = node->parent;

            // check if the parent needs to be flushed
            if (node->page->check_flush()) {
                if (!node->parent) {
                    root_to_flush = node;
                    break;
                }
            } else {
                // otherwise, update the parent's size in the cache
                boost::unique_lock cache_lock(_cache->mutex);
                _cache_update_size(node->page, cache_lock);
                break;
            }
        }

        return root_to_flush;
    }

    void
    MutableBTree::_remove_page_internal(PagePtr page,
                                        PagePtr parent)
    {
        // lock the parent's data before modifying it
        boost::unique_lock lock(parent->mutex);

        // remove the page from the parent
        parent->remove(page->prev_key, (parent == _root));

        // remove the page from the parent's children
        parent->remove_child(page->extent_id);

        // mark the page flushed
        page->flushed = true;
    }

    void
    MutableBTree::_remove_page(PagePtr page,
                               PagePtr parent)
    {
        // acquire exclusive access to the page
        boost::unique_lock lock(page->disk_mutex);

        // if the page was already flushed / removed, then do nothing
        if (page->flushed) {
            return;
        }

        // make sure the page still should be removed, there may have been another thread that
        // added an entry before the page was re-locked
        if (!page->empty()) {
            return;
        }

        // perform the removal
        _remove_page_internal(page, parent);

        // evict the page from the cache
        boost::unique_lock cache_lock(_cache->mutex);
        _cache_evict(page->extent_id);
    }

    void
    MutableBTree::_remove_root()
    {
        // note: the tree must be locked to perform this operation
        _root = nullptr;

        // evict all pages without flushing to disk
        {
            boost::unique_lock cache_lock(_cache->mutex);
            _cache_clear();
        }

        // set up an empty tree
        init_empty();
    }

    void
    MutableBTree::_lock_and_remove_page(NodePtr node)
    {
        // note: already have the parent page pinned with shared lock on disk_mutex
        PagePtr parent = node->parent->page;

        // if the parent was flushed, then the removal would have been processed during the
        // page's flush() operation
        if (parent->flushed) {
            return;
        }

        // release the shared lock on the disk_mutex to allow for exclusive access
        node->lock.unlock();

        // now that we are holding the parent, try to remove the page
        _remove_page(node->page, parent);

        // the parent would have gotten smaller, so no need to flush, but if the parent is empty
        // then we can remove it as well
        if (parent->empty()) {
            if (node->parent->parent != nullptr) {
                _lock_and_remove_page(node->parent);
            }
        }
    }

    void
    MutableBTree::_init_schemas(ExtentSchemaPtr schema,
                                const std::vector<std::string> &keys)
    {
        _leaf_schema = schema;

        // construct the field tuples for the leaf nodes
        _leaf_fields = _leaf_schema->get_mutable_fields();
        _leaf_keys = _leaf_schema->get_mutable_fields(keys);

        // construct the schema for the branches
        // note: don't need a valid sql_type for the internal nodes since they aren't exposed
        SchemaColumn child(constant::BTREE_CHILD_FIELD, 0, SchemaType::UINT64, 0, false);
        _branch_schema = _leaf_schema->create_schema(keys, { child }, keys);

        // construct the field tuples for the branch nodes
        _branch_keys = _branch_schema->get_mutable_fields(keys);
        _branch_child_f = _branch_schema->get_mutable_field(constant::BTREE_CHILD_FIELD);
    }
}
