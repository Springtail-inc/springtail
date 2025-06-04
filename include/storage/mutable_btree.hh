#pragma once

#include <boost/thread.hpp>

#include <common/constants.hh>
#include <storage/cache.hh>
#include <storage/field.hh>
#include <storage/io.hh>
#include <storage/schema.hh>

namespace springtail {
    /**
     * A b+tree constructed out of Extent objects.  Uses a FieldArray as the sort-key for the tree
     * which must be a subset of columns provided in a Schema for the BTree.  The leaf nodes of the
     * BTree contain the actual data specified by the Schema.  The branch nodes contain only the
     * sort-key columns, and an offset to the child extent.
     *
     * The MutableBTree is the object interface capable of modifying the btree through insert() and
     * remove() operations.  Note that it doesn't provide update() since that will be implemented
     * via a remove() + insert() since the full row is always provided in the replication log.  All
     * modifications are applied at a given XID which is specified via the set_xid() function.
     * Modifications must be applied in XID order to ensure that the modified extents represent a
     * valid snapshot.  Trying to apply changes from an XID that does not represent the currently
     * set XID will result in an error.  Similarly, trying to set the XID to an earlier XID will
     * also result in an error.
     *
     * It uses an internal page cache up to a specified maximum size.  Once modification operations
     * are complete, they must be flushed to disk using finalize(), which also returns the offset of
     * the new root of the tree.  After this operation, no further modifications can be made at the
     * current XID.
     *
     * Because all modifications are done copy-on-write, it is safe to have (potentially multiple)
     * read-only BTree objects constructed against the same underlying file being used concurrently
     * with the MutableBTree.
     */
    class MutableBTree {
    private:
        // forward declarations
        class Page;
        typedef std::shared_ptr<Page> PagePtr;

        struct Node;
        typedef std::shared_ptr<Node> NodePtr;
        
    public:
        /**
         * Cache of Page objects.  Works with the BTree page locks to enable thread-safe access.
         */
        class PageCache {
        public:
            /** Cache entry typedef, tuple of PagePtr, LRU iterator, usage count, page size */
            typedef std::tuple<PagePtr, typename std::list<PagePtr>::iterator, int, uint32_t> LookupEntry;

            /** The ID type for the lookup is <file, extent_id> */
            typedef std::pair<std::filesystem::path, uint64_t> LookupID;

            /** A map from extent ID to LookupEntry. */
            std::unordered_map<LookupID, LookupEntry, boost::hash<LookupID>> lookup;

            /** A list of pages in LRU order.  In-use pages are not kept in this LRU list.*/
            std::list<PagePtr> lru;

            /** The total bytes currently stored in the page cache. */
            uint64_t size;

            /** The maximum allowed bytes in the page cache. */
            uint64_t max_size;

            /** A mutex for exclusive / shared access to the cache. */
            boost::shared_mutex mutex;

        public:
            explicit PageCache(uint64_t max_size = 512)
                : size(0),
                  max_size(max_size)
            { }
        };
        typedef std::shared_ptr<PageCache> PageCachePtr;

    public:
        /**
         * Constructs an uninitialized tree at the provided XID.
         *
         * @param handle The file handle to the underlying on-disk representation.
         * @param keys A list of keys from the schema that are used to sort the entries of the tree.
         * @param cache_size The maximum size of the in-memory cache of pages.
         * @param schema The schema of the leaf entries of the tree.
         */
        MutableBTree(const std::filesystem::path &file,
                     const std::vector<std::string> &keys,
                     ExtentSchemaPtr schema,
                     uint64_t xid);

        /**
         * Initialize an empty tree.
         */
        void init_empty();

        /**
         * Initialize a tree with a given root offset.
         */
        void init(uint64_t root_offset);

        /**
         * Inserts a new entry into the tree.
         *
         * @param value The value of the entry we are about to insert into the tree.
         */
        void insert(TuplePtr value);

        /**
         * Removes the entry at the provided position.
         *
         * @param value The value of the entry we are about to remove from the tree.
         */
        void remove(TuplePtr value);

        /**
         * Truncates the btree, removing all entries from it.  In-memory dirty pages are evicted
         * without being written, meaning any non-finalized mutations are lost.
         */
        void truncate();

        /**
         * Commits all changes to disk, marking all of the written extents as being valid at the
         * current XID for the BTree -- meaning that all modifications for that XID have been
         * completed.  Further operations on the BTree without setting the XID forward should result
         * in errors.
         *
         * @return The offset of the root for the target XID we just finalized.
         */
        uint64_t finalize();

        /**
         * Helper function to return the key fields of the btree leaf extents.
         */
        // XXX is this really necessary?
        FieldArrayPtr get_key_fields()
        {
            return std::make_shared<FieldArray>(_leaf_keys->begin(), _leaf_keys->end());
        }

        /**
         * Helper function to determine if the btree is empty by checking if the root is empty.
         */
        bool empty() {
            return _root->empty();
        }

    private:
        /** The default maximum number of extents in an in-memory page before we automatically flush it to disk. */
        static const uint32_t MAX_EXTENT_COUNT = 16;

        /**
         * The Page objects represent a virtual Extent, which may hold either a single Extent
         * object, or potentially multiple Extent objects which are the result of split() operations
         * caused by modifications to the original Extent object.  Page objects may be marked as
         * dirty, indicating that they need to be written to a new location and update their parent
         * branch pointers accordingly.
         */
        class Page {
        public:
            using Iterator = StorageCache::Page::Iterator;
            using StoragePagePtr = StorageCache::SafePagePtr;

        private:
            /**
             * Checks if the extent has exceeded the max size.  If so, splits the extent in half.
             * Must have the page locked for exclusive access.
             *
             * @param pos An iterator to the extent within the extent list that we may need to split.
             */
            bool _check_split(std::vector<ExtentPtr>::iterator pos);

        public:
            /** For constructing an empty root. */
            Page(MutableBTree *btree,
                 StoragePagePtr cache_page,
                 ExtentSchemaPtr schema)
                : extent_id(constant::UNKNOWN_EXTENT),
                  type(false, true),
                  flushed(false),
                  _btree(btree),
                  _dirty(false),
                  _schema(schema),
                  _cache_page(std::move(cache_page)),
                  _parent(nullptr)
            {
                _key_fields = _schema->get_mutable_fields(_schema->get_sort_keys());
            }

            Page(MutableBTree *btree,
                 uint64_t extent_id)
                : extent_id(extent_id),
                  flushed(false),
                  _btree(btree),
                  _dirty(false)
            { }

            Page(MutableBTree *btree,
                 uint64_t extent_id,
                 ValueTuplePtr v,
                 StoragePagePtr cache_page,
                 ExtentSchemaPtr schema)
                : extent_id(extent_id),
                  prev_key(v),
                  flushed(false),
                  _btree(btree),
                  _dirty(false),
                  _schema(schema),
                  _cache_page(std::move(cache_page))
            {
                type = _cache_page->header().type;
                _key_fields = _schema->get_mutable_fields(_schema->get_sort_keys());
            }

            /** Returns an iterator to the first row in the Page. */
            Iterator begin() {
                return _cache_page->begin();
            }

            /** Returns an iterator that indicates that the Page has been fully traversed. */
            Iterator end() {
                return _cache_page->end();
            }

            /** Returns an iterator to the last valid element of the tree. */
            Iterator last() {
                return _cache_page->last();
            }

            /**
             * Returns a reference to the last row in the page.
             */
            Extent::Row back() const {
                return *(_cache_page->last());
            }

            /**
             * Returns an iterator to the first entry that has a key that is greater than or equal
             * to the provided search_key.  Returns end() if there is no such entry.
             *
             * @param search_key The key we are searching for in the page.
             */
            Iterator lower_bound(TuplePtr search_key);

            /**
             * Returns an iterator to the first entry that has a key that is greater than or equal
             * to the provided search_key.  Returns end() if there is no such entry.
             *
             * @param search_key The key we are searching for in the page.
             */
            Iterator find(TuplePtr search_key);

            /**
             * Updates the branch pointers within this page to replace an existing child page with a
             * new set of child pages.  Used to replace a child when it is flushed since pages are
             * copy-on-write.  Assumes that the page is locked for exclusive access.
             *
             * @param old_page The old child page to remove from this branch.
             * @param new_pages The new child pages to insert into this branch.
             * @param offset_f The field accessor for the child offset within a branch.
             */
            void update_branch(std::shared_ptr<Page> old_page,
                               std::vector<std::shared_ptr<Page>> new_pages,
                               MutableFieldPtr offset_f);

            /**
             * Adds an entry to this page.  Assumes that the page is locked for exclusive access.
             *
             * @param search_key The key to insert.
             * @param value The value to insert.
             * @param fields The fields in the page that the value corresponds to.
             */
            void insert(TuplePtr search_key, TuplePtr value, MutableFieldArrayPtr fields);
            
            /**
             * Remove an entry from this page.  Removes the first entry in the page with a matching
             * key.
             *
             * @param search_key The key to remove.
             */
            void remove(TuplePtr search_key, bool is_root = false);

            /**
             * Flushes the contents of the page and updates the parent with the new pages.  Assumes
             * that the caller is holding an exclusive lock on both this page and it's parent and
             * that neither are marked flushed.
             *
             * @param handle The file handle for access to write data.
             * @param xid The XID at which the page is being written.
             */
            std::vector<std::shared_ptr<Page>> flush(uint64_t xid);

            /**
             * Specialized case for flushing an empty root page.  Assumes that the caller is holding
             * an exclusive lock on the page.
             *
             * @param handle The file handle for access to write data.
             * @param xid The XID at which the page is being written.
             */
            std::vector<std::shared_ptr<Page>> flush_empty_root(uint64_t xid);

            /**
             * Set the contents of the page to the provided extent.  Stores the key of the final row
             * in the extent to use if the page is mutated and must be replaced in the parent branch
             * pointers.  Must be called when the page is empty.
             *
             * @param extent The contents of the page.
             * @param key_fields The fields that represent the key within this page.
             */
            void set_cache_page(StoragePagePtr cache_page, ExtentSchemaPtr schema);

            PageCache::LookupID
            get_lookup_id()
            {
                return PageCache::LookupID{_btree->_file, extent_id};
            }

            /**
             * Return the key fields for the page.
             */
            MutableFieldArrayPtr index_keys() const {
                return _key_fields;
            }

            /**
             * Returns the parent of the page, if one is set.  Otherwise returns nullptr.
             */
            std::shared_ptr<Page> parent() const {
                boost::unique_lock lock(_children_mutex);
                return _parent;
            }

            /**
             * Sets the parent of the page.
             *
             * @param parent The parent of the page.
             */
            void set_parent(std::shared_ptr<Page> parent) {
                boost::unique_lock lock(_children_mutex);
                _parent = parent;
            }

            /**
             * Adds a page to the list of children for this page.  Used to determine which pages
             * need to be flushed before this page can be flushed.
             *
             * @param child A child of this page.
             */
            void add_child(std::shared_ptr<Page> child) {
                boost::unique_lock lock(_children_mutex);
                _children[child->extent_id] = child;
                assert(!child->flushed);
            }

            /**
             * Removes a page from the list of children for this page.
             *
             * @param extent_id The extent ID of the page to be removed.
             */
            void remove_child(uint64_t extent_id) {
                boost::unique_lock lock(_children_mutex);
                _children.erase(extent_id);
            }

            /**
             * Checks if the page has children.
             */
            bool has_children() const {
                boost::unique_lock lock(_children_mutex);
                return !_children.empty();
            }

            /**
             * Returns a pointer to the first child of the page.  Invalid to call when the page has
             * no children.
             */
            std::shared_ptr<Page> first_child() const {
                // note: must be called when there are children
                assert(!_children.empty());

                boost::unique_lock lock(_children_mutex);
                return _children.begin()->second;
            }

            /**
             * Check if the page should be flushed based on the number of extents it contains.
             */
            bool check_flush() const {
                return (_cache_page->extent_count() > _btree->_max_extent_per_page);
            }

            /**
             * Check if the page is empty; i.e., it has a single extent with no rows in it.
             */
            bool empty() const {
                return _cache_page->empty();
            }

            /**
             * Check if the page is marked dirty.
             */
            bool is_dirty() const {
                return _dirty;
            }

            /**
             * Returns the on-disk XID of the page.
             */
            uint64_t xid() const {
                return _cache_page->header().xid;
            }

        public:
            uint64_t extent_id; ///< The offset of the previous extent this page is based on.  Won't change once the page is fully constructed.
            ExtentType type; ///< The type of this page.  Won't change once the page is fully constructed, so safe to read without holding the mutex.
            ValueTuplePtr prev_key; ///< The key in the parent of the previous extent this page is based on.  Won't change once the page is fully constructed.

            bool flushed; ///< This page has already been flushed to disk and replaced with a new page.

            mutable boost::shared_mutex disk_mutex; ///< A shared mutex that acts as a barrier for disk reads / writes.
            mutable boost::shared_mutex mutex; ///< A shared mutex for access to the in-memory data.


        private:
            MutableBTree *_btree; ///< The btree this page is associated with.
            bool _dirty; ///< Flag indicating if data has been modified since the last write.
            MutableFieldArrayPtr _key_fields; ///< The fields representing the key in the btree.

            ExtentSchemaPtr _schema; ///< The schema of the page.
            StoragePagePtr _cache_page; ///< The backing page in the cache for the contents of this BTree page.

            // the following are protected by a separate mutex, must be holding the primary mutex at least shared.
            mutable boost::shared_mutex _children_mutex; ///< A mutex to protect the map of children.  Can be acquired unique while sharing the primary mutex.
            std::map<uint64_t, std::shared_ptr<Page>> _children; ///< The set of children of this page that are part of a modified path through the tree.
            std::shared_ptr<Page> _parent; ///< Pointer to the parent page.  Won't change once the page is fully constructed.
        };

    private:
        /** The cache of pages for this MutableBTree. */
        PageCachePtr _cache;

        /** The handle to the underlying on-disk data. */
        std::shared_ptr<IOHandle> _handle;

        /** The file for the underlying on-disk data. */
        std::filesystem::path _file;

        /** The set of keys that form the sort order of the tree. */
        std::vector<std::string> _sort_keys;

        /** A mutex for protecting access to the tree. */
        mutable boost::shared_mutex _mutex;

        /** A pointer to the root of the tree. */
        PagePtr _root;

        /** The target XID of the tree. */
        uint64_t _xid;

        uint64_t _max_extent_size; ///< The maximum size of a single extent before splitting
        uint64_t _max_extent_per_page; ///< The maximum number of extents in a page before flushing

        /**
         * A flag indicating if the target XID is finalized on disk or not.  Mutations are only
         * safe when the tree is not finalized.
         */
        bool _finalized;

        /** The schema for the leaf nodes. */
        std::shared_ptr<ExtentSchema> _leaf_schema;
        MutableFieldArrayPtr _leaf_fields; ///< The fields of the leaf values.
        MutableFieldArrayPtr _leaf_keys; ///< The fields of the leaf keys columns.

        /** The schema for the branch nodes. */
        std::shared_ptr<ExtentSchema> _branch_schema;
        MutableFieldArrayPtr _branch_keys; ///< The fields of the branch keys columns.
        MutableFieldPtr _branch_child_f; ///< The field accessor for the child offset in branch rows.

    private:
        /**
         * An object to track the set of pages from the root to the current page.  Used in tree
         * traversal.
         */
        struct Node {
            const std::shared_ptr<Node> parent; ///< The parent node of this node.
            const PagePtr page; ///< The current page.
            boost::shared_lock<boost::shared_mutex> lock; ///< A shared lock on the page's disk_mutex

            /**
             * Constructor for the Node.  Automatically acquires a shared lock on the Page object's
             * disk_mutex.
             */
            Node(std::shared_ptr<Node> parent, PagePtr page)
                : parent(parent),
                  page(page),
                  lock(page->disk_mutex)
            { }

            /**
             * Constructor for the Node.  Downgrades an existing exclusive lock on the Page object's
             * disk_mutex to a shared lock.
             */
            Node(std::shared_ptr<Node> parent,
                 PagePtr page,
                 boost::unique_lock<boost::shared_mutex> &&disk_lock)
                : parent(parent),
                  page(page),
                  lock(std::move(disk_lock))
            { }
        };

    private:
        //// CACHE OPERATIONS

        /**
         * Retrieve an entry from the cache.  Increases the usage count and removes it from the LRU
         * list if not already in use.
         *
         * @param extent_id The ID of the page to retrieve.
         * @return A pointer to the page, or nullptr if no such page exists in the cache.
         */
        PagePtr _cache_get(uint64_t extent_id);

        /**
         * Releases an entry back to the cache.  Reduces the usage count and appends it to the LRU
         * list if there are no other users of the page.
         *
         * @param page The page to release back to the cache.
         */
        void _cache_release(PagePtr page);

        /**
         * Inserts an empty page entry into the cache.  Used to insert a placeholder while reading
         * the page contents from disk.  The page is marked as in-use from insertion to prevent it
         * from being evicted from the cache.
         *
         * @param page The empty page to insert into the cache.
         */
        void _cache_insert_empty(PagePtr page);

        /**
         * Inserts a populated page into the cache.  Must be called while holding an exclusive lock
         * on the cache, which may be manipulated during page insertion if eviction is necessary.
         * In most cases, this is called when a CoW page is flushed to disk and new page(s) are
         * inserted into the cache to replace it, and so the new pages can immediately be released.
         * However, when reading the root of the tree, we insert it into the cache without releasing
         * it.
         *
         * @param page The page to insert into the page.
         * @param cache_lock The exclusive lock on the cache held by the caller.
         * @param release If true then the page should immediately be released back to the cache.
         */
        void _cache_insert(PagePtr page,
                           boost::unique_lock<boost::shared_mutex> &cache_lock, bool release = true);

        /**
         * Updates the size of an entry in the cache.  This may cause an eviction to ensure that the
         * cache doesn't exceed it's maximum size, thus the exclusive lock held on the cache by the
         * caller must be provided to this function.  Also, the page being updated must have it's
         * disk_mutex held, but not it's access mutex.  Otherwise, updating the size of a parent
         * node could result in a deadlock trying to acquire the parent's lock again while evicting
         * a child.
         *
         * @param page The page to update the size of based on its size currently.
         * @param cache_lock The exclusive lock on the cache held by the caller.
         */
        void _cache_update_size(PagePtr page, boost::unique_lock<boost::shared_mutex> &cache_lock);

        /**
         * Evicts an entry from the cache.  Can only be safely called on pages that have been
         * flushed to disk.
         *
         * @param extent_id The ID of the page to evict.
         */
        void _cache_evict(uint64_t extent_id);

        /**
         * Clears the cache, dropping all pages without flushing them.  Used to implement
         * truncate().
         */
        void _cache_clear();

        //// INTERNAL OPERATIONS

        /**
         * Performs the actual disk read of the extent and populates it into the page.  Assumes that
         * the provided page is empty.
         *
         * @param page An empty page to populate from on-disk data.
         */
        void _read_page_internal(PagePtr page);

        /**
         * Read an extent from disk and store it as a Page in the cache.  When called the
         * parent_lock must be held and will be released by this function.  The leaf_lock must not
         * be held.  If the page is a leaf, the leaf lock will be acquired before returning.  If the
         * page is a branch, then the parent_lock will be replaced by a shared lock on the newly
         * read page.
         *
         * @param extent_id The ID of the page to be read from disk.
         * @param parent The parent page of the page to be read.
         */
        NodePtr _read_page(uint64_t extent_id, NodePtr parent,
                           boost::shared_lock<boost::shared_mutex> &parent_lock);

        /**
         * Find the leaf that *could* hold the provided key.  This will always return a pointer to a
         * Page since it is used for key insertion.
         *
         * note: we could (1) pass in the base Node here to allow search starting from any node and
         *       (2) use a template for the lock type... these would allow us to use this for read
         *       iteration.
         *
         * @param key The key of the entry to find a position for.
         * @param leaf_lock A lock that is acquired on the mutex of the leaf that we find.
         */
        NodePtr _find_leaf(TuplePtr key);

        /**
         * Flushes the provided page to disk, updating the parent with the corrected pointers after
         * the CoW and removes the child/parent references.  Requires that exclusive locks be held
         * on both the parent and the page being flushed.
         *
         * @param page The page being flushed.
         * @param parent The parent of the page being flushed.
         */
        std::vector<PagePtr> _flush_page_internal(PagePtr page, PagePtr parent);

        /**
         * Flushes the provided Page to disk.  Also ensures that any children of the page are
         * flushed first so that the corrected CoW extent offsets are recorded in this page.  The
         * page must *not* be locked by the caller.  The provided parent must already be locked for
         * exclusive access by the caller.
         *
         * @param page The page being flushed.
         * @param parent The parent of the page being flushed.
         */
        void _flush_page(PagePtr page, PagePtr parent);

        /**
         * Flushes the root of the tree to disk, also ensuring that any childen of the root are also
         * flushed.  The root must *not* be locked by the caller.
         *
         * @param page The root page of the tree.
         */
        PagePtr _flush_root(PagePtr page);

        /**
         * Locks the tree and flushes the root of the tree, causing the entire tree to be flushed to
         * disk.  The root must *not* be locked by the caller.  The tree itself must also *not* be
         * locked by the caller.  The function will first lock the tree for exclusive access to
         * ensure no one is accessing the root while it is being flushed.
         *
         * @param root The root page of the tree.
         */
        void _lock_and_flush_root(NodePtr root);

        /**
         * Flushes the provided Page to disk.  The page contained in the Node must not be locked by
         * the caller.  This function will first lock the parent to ensure that the provided page is
         * not accessed during flushing.
         *
         * @param node The node representing the page we want to flush.
         */
        void _lock_and_flush_page(NodePtr node);

        /**
         * Removes the page from the provided parent, including removing it from it's list of
         * branches.  Both the page and the parent must be locked for exclusive access.  The page
         * should be empty to be removed.
         *
         * @param page The empty page to remove.
         * @param parent The parent of the page.
         */
        void _remove_page_internal(PagePtr page, PagePtr parent);

        /**
         * Removes an empty page from the provided parent.  The parent must be locked for exclusive
         * access, but the page must *not* be locked.
         *
         * @param page The page to remove.
         * @param parent The parent of the page.
         */
        void _remove_page(PagePtr page, PagePtr parent);

        /**
         * Removes the root page of the tree, clears the cache and re-initializes the tree into an
         * empty state.
         */
        void _remove_root();

        /**
         * Removes an empty page from it's parent.  Both the parent and the page must *not* be
         * locked before calling this function.
         *
         * @param node The node representing the page to remove.
         */
        void _lock_and_remove_page(NodePtr node);

        /**
         * Initializes the internal schemas and field arrays used by the btree.
         *
         * @param schema The schema of the rows in the leaf extents.
         * @param keys The list of columns that make up the sort key of the tree.
         */
        void _init_schemas(ExtentSchemaPtr schema, const std::vector<std::string> &keys);

        //// ITERATOR SUPPORT
    public:
        class Iterator {
            friend MutableBTree;

        private:
            Iterator(MutableBTree *btree, NodePtr node)
                : _btree(btree),
                  _page_lock(node->page->mutex),
                  _node(node),
                  _page_i(node->page->begin())
            { }

            Iterator(MutableBTree *btree, NodePtr node, Page::Iterator &&page_i)
                : _btree(btree),
                  _page_lock(node->page->mutex),
                  _node(node),
                  _page_i(std::move(page_i))
            { }

        public:
            using iterator_category = std::forward_iterator_tag;
            using difference_type   = std::ptrdiff_t;
            using value_type        = const Extent::Row;
            using pointer           = const Extent::Row *;  // or also value_type*
            using reference         = const Extent::Row &;  // or also value_type&

            Iterator() = default;

            Iterator &operator++() {
                ++_page_i;
                if (_page_i == _node->page->end()) {
                    _node = _btree->_next_leaf(_node);
                    _page_lock = boost::shared_lock(_node->page->mutex);
                    _page_i = _node->page->begin();
                }
                return *this;
            }

            reference operator*() const {
                return *_page_i;
            }

            bool operator==(const Iterator &rhs) const {
                if (_node == nullptr && rhs._node == nullptr) {
                    return true;
                }
                return (_btree == rhs._btree && _node == rhs._node && _page_i == rhs._page_i);
            }

        private:
            MutableBTree *_btree = nullptr;
            boost::shared_lock<boost::shared_mutex> _page_lock;
            NodePtr _node;
            Page::Iterator _page_i;
        };

        Iterator begin();
        Iterator last();
        Iterator lower_bound(TuplePtr search_key, bool for_update);

        Iterator end() {
            return Iterator();
        }

    private:
        /** Iterator helper to find the next leaf node given a current node. */
        NodePtr _next_leaf(NodePtr node);
    };
    typedef std::shared_ptr<MutableBTree> MutableBTreePtr;

}
