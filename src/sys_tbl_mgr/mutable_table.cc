#include <memory>

#include <common/constants.hh>
#include <common/json.hh>
#include <common/properties.hh>

#include <sys_tbl_mgr/system_tables.hh>
#include <sys_tbl_mgr/mutable_table.hh>
#include <sys_tbl_mgr/table_mgr.hh>
#include <sys_tbl_mgr/schema_helpers.hh>

//#define SPRINGTAIL_INCLUDE_TIME_TRACES 1
#include <common/time_trace.hh>

namespace springtail {

static auto get_max_extent_size() {
    return constant::MAX_EXTENT_SIZE;
}

static auto get_max_extent_size_secondary() {
    return constant::MAX_EXTENT_SIZE_SECONDARY;
}

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
            const std::vector<std::string> &idx_cols,
            const ExtentSchemaPtr          &schema)
    {
        /* 1. Column metadata is the same for every row - fetch it once. */
        const auto key_fields   = schema->get_fields(idx_cols);

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

        LOG_DEBUG(LOG_BTREE, LOG_LEVEL_DEBUG1, "{} {} secondary rows",
            (op == IndexOperation::Insert) ? "Populated"
            : "Invalidated",
            row_id);
    }

    template <IndexOperation op>            // Operation on the index - insert/remove
    void index_mutation_handler(
            const ExtentSchemaPtr schema,
            const SecondaryIndexesCache& secondary_indexes,
            const std::map<uint64_t, Index>& index_lookup,
            const Extent::Row& row)
    {
        auto internal_row_id_f = schema->get_field(constant::INTERNAL_ROW_ID);
        auto internal_row_id = internal_row_id_f->get_uint64(&row);
        auto value_fields = std::make_shared<FieldArray>(1);

        for (auto const& [index_id, idx]: secondary_indexes) {
            auto idx_col_fields = schema->get_fields(schema->get_column_names(idx.second));
            if (index_lookup.at(index_id).index_type == constant::INDEX_TYPE_GIN) {
                auto key_fields = std::make_shared<FieldArray>(3);
                for (int i = 0; i < idx.second.size(); i++) {
                    auto&& col_field = (*idx_col_fields)[i];
                    auto pos = idx.second[i];
                    auto& column = index_lookup.at(index_id).columns[i];
                    auto&& tokens = extract_trgm_from_value(std::string(col_field->get_text(&row)), column.opclass, GIN_EXTRACTVALUE);

                    for (auto& token: tokens) {
                        // Set idx_position, token, internal_row_id
                        key_fields->at(0) = std::make_shared<ConstTypeField<uint32_t>>(pos);
                        key_fields->at(1) = std::make_shared<ConstTypeField<std::string>>(token);
                        key_fields->at(2) = std::make_shared<ConstTypeField<uint64_t>>(internal_row_id);
                        auto tuple = std::make_shared<FieldTuple>(key_fields, nullptr);
                        if constexpr (op == IndexOperation::Insert) {
                            idx.first->insert(tuple);
                        } else {
                            idx.first->remove(tuple);
                        }
                    }
                }
            } else {
                (*value_fields)[0] = std::make_shared<ConstTypeField<uint64_t>>(internal_row_id);
                auto &&svalue = std::make_shared<KeyValueTuple>(idx_col_fields, value_fields, &row);
                if constexpr (op == IndexOperation::Insert) {
                    idx.first->insert(svalue);
                } else { /* op == IndexOperation::Remove */
                    idx.first->remove(svalue);
                }
            }
        }
    }

    /* ------------------------------  PAGE  ----------------------------------- */
    void populate_index_for_page(uint64_t extent_id,
            const StorageCache::SafePagePtr &page,
            const MutableBTreePtr          &root,
            const std::vector<std::string> &idx_cols,
            const ExtentSchemaPtr          &schema)
    {
        _update_secondary_index<IndexOperation::Insert>(extent_id, page, root,
                idx_cols, schema);
    }

    void invalidate_index_for_page(uint64_t extent_id,
            const StorageCache::SafePagePtr &page,
            const MutableBTreePtr          &root,
            const std::vector<std::string> &idx_cols,
            const ExtentSchemaPtr          &schema)
    {
        _update_secondary_index<IndexOperation::Remove>(extent_id, page, root,
                idx_cols, schema);
    }

    /* -----------------------------  EXTENT  ---------------------------------- */
    void populate_index_for_extent(uint64_t extent_id,
            const std::shared_ptr<Extent>  &extent,
            const MutableBTreePtr          &root,
            const std::vector<std::string> &idx_cols,
            const ExtentSchemaPtr          &schema)
    {
        _update_secondary_index<IndexOperation::Insert>(extent_id, extent, root,
                idx_cols, schema);
    }

    void invalidate_index_for_extent(uint64_t extent_id,
            const std::shared_ptr<Extent>  &extent,
            const MutableBTreePtr          &root,
            const std::vector<std::string> &idx_cols,
            const ExtentSchemaPtr          &schema)
    {
        _update_secondary_index<IndexOperation::Remove>(extent_id, extent, root,
                idx_cols, schema);
    }

    /**
     * Struct to hold context to be passed to the callback from cache
     */
    struct IndexMutationContext
    {
        const ExtentSchemaPtr schema;
        const SecondaryIndexesCache& indexes;
        const std::map<uint64_t, Index>& index_lookup;
    };

    /**
     * Handler that does the secondary index mutations
     * @param row   Extent row holding the content
     * @param ctx   Generic pointer to an IndexMutationContext
     */
    template<IndexOperation op>
    static void mutation_handler(const Extent::Row& row, void* ctx) {
        auto* c = static_cast<const IndexMutationContext*>(ctx);
        index_mutation_handler<op>(c->schema, c->indexes, c->index_lookup, row);
    }

} // namespace indexer_helpers

    MutableTable::MutableTable(uint64_t db_id,
                               uint64_t table_id,
                               uint64_t access_xid,
                               uint64_t target_xid,
                               const std::filesystem::path &table_base,
                               const std::vector<std::string> &primary_key,
                               const std::vector<Index> &secondary,
                               const TableMetadata &metadata,
                               ExtentSchemaPtr schema,
                               ExtentSchemaPtr schema_without_table_id,
                               const ExtensionCallback &extension_callback,
                               const OpClassHandler &opclass_handler,
                               bool bypass_schema_cache)
    : _db_id(db_id),
      _id(table_id),
      _access_xid(access_xid),
      _target_xid(target_xid),
      _primary_key(primary_key),
      _bypass_schema_cache(bypass_schema_cache),
      _schema(schema),
      _schema_without_row_id(schema_without_table_id)
    {
        std::vector<TableRoot> roots;
        _snapshot_xid = metadata.snapshot_xid;
        _stats = metadata.stats;
        roots = metadata.roots;

        // construct the table's data directory
        _table_dir = table_helpers::get_table_dir(table_base, db_id, table_id, _snapshot_xid);
        _data_file = _table_dir / constant::DATA_FILE;

        // Initialize the last internal row ID in the table
        _internal_row_id = metadata.stats.last_internal_row_id;

        // make sure that the table directory exists
        std::filesystem::create_directories(_table_dir);

        // Get the singleton roots schema
        _roots_schema = schema_helpers::get_roots_schema();
        _roots_root_f = _roots_schema->get_mutable_field("root");
        _roots_index_id_f = _roots_schema->get_mutable_field("index_id");
        _roots_last_internal_row_id_f = _roots_schema->get_mutable_field("last_internal_row_id");

        if (roots.empty()) {
            if (std::filesystem::exists(_table_dir / constant::ROOTS_FILE)) {
                // read the roots from the look-aside file
                auto roots_path = std::filesystem::read_symlink(_table_dir / constant::ROOTS_FILE);
                auto root_handle = IOMgr::get_instance()->open(roots_path, IOMgr::IO_MODE::READ, true);
                auto response = root_handle->read(0);
                auto extent = std::make_shared<Extent>(response->data);
                for (auto &row : *extent) {
                    roots.push_back({_roots_index_id_f->get_uint64(&row), _roots_root_f->get_uint64(&row)});
                    _internal_row_id = _roots_last_internal_row_id_f->get_uint64(&row);
                }
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

        // construct the primary index btree
        SchemaColumn extent_c(constant::INDEX_EID_FIELD, 0, SchemaType::UINT64, 0, false);
        SchemaColumn row_c(constant::INDEX_RID_FIELD, 0, SchemaType::UINT32, 0, false);
        SchemaColumn internal_row_id(constant::INTERNAL_ROW_ID, 0, SchemaType::UINT64, 0, false);

        ExtentSchemaPtr primary_schema;
        if (primary_key.empty()) {
            std::vector<std::string> non_primary_key = { constant::INDEX_EID_FIELD };
            primary_schema = _schema->create_index_schema({}, { extent_c }, non_primary_key, extension_callback);

            _primary_index = std::make_shared<MutableBTree>(_db_id,
                                                            _table_dir / constant::INDEX_PRIMARY_FILE,
                                                            non_primary_key,
                                                            primary_schema,
                                                            _target_xid, get_max_extent_size(), extension_callback);
        } else {
            primary_schema = _schema->create_index_schema(primary_key, { extent_c }, primary_key, extension_callback);

            _primary_index = std::make_shared<MutableBTree>(_db_id,
                                                            _table_dir / constant::INDEX_PRIMARY_FILE,
                                                            primary_key,
                                                            primary_schema,
                                                            _target_xid, get_max_extent_size(), extension_callback);
        }



        // find primary index root
        auto it = std::ranges::find_if(roots, [](auto const &v) { return v.index_id == constant::INDEX_PRIMARY; });
        assert(it != roots.end());

        // initialize the primary index
        if (it->extent_id != constant::UNKNOWN_EXTENT) {
            LOG_DEBUG(LOG_BTREE, LOG_LEVEL_DEBUG1, "Primary init with root: {}", it->extent_id);
            _primary_index->init(it->extent_id);
        } else {
            LOG_DEBUG(LOG_BTREE, LOG_LEVEL_DEBUG1, "Primary init empty");
            _primary_index->init_empty();
        }
        _use_empty = _primary_index->empty();
        _primary_extent_id_f = primary_schema->get_field(constant::INDEX_EID_FIELD);

        // Set GIN Index schema to be used in constructing GIN index root
        _set_gin_index_schema(extension_callback);

        // deal with secondary indexes
        for (auto const& idx: secondary) {
            if (idx.state != static_cast<uint8_t>(sys_tbl::IndexNames::State::READY)) {
                continue;
            }
            assert(idx.id != constant::INDEX_PRIMARY);

            // Populate lookup to be used during mutation
            _index_lookup.emplace(idx.id, idx);

            // work with the index
            std::vector<uint32_t> idx_cols;
            idx_cols.reserve(idx.columns.size());
            for (auto const &col: idx.columns) {
                idx_cols.push_back(col.position);
            }
            if (!idx_cols.empty()) {

                MutableBTreePtr btree;
                if (idx.index_type == constant::INDEX_TYPE_GIN) {
                    btree = create_gin_index_root(idx.id, extension_callback, opclass_handler);
                } else {
                    btree = create_index_root(idx.id, idx_cols, extension_callback, opclass_handler, idx.index_type);
                }

                auto it = std::ranges::find_if(roots, [&](auto const &v) { return v.index_id == idx.id; });
                assert(it != roots.end());

                if (it->extent_id != constant::UNKNOWN_EXTENT) {
                    LOG_DEBUG(LOG_BTREE, LOG_LEVEL_DEBUG1, "Secondary {} of type {} init with root: {}", idx.id, idx.index_type, it->extent_id);
                    btree->init(it->extent_id);
                } else {
                    LOG_DEBUG(LOG_BTREE, LOG_LEVEL_DEBUG1, "Secondary {} of type {} init empty", idx.id, idx.index_type);
                    btree->init_empty();
                }
                assert(_secondary_indexes.find(idx.id) == _secondary_indexes.end());
                _secondary_indexes[idx.id] = {btree, idx_cols};
            }
        }

        // Look-aside exists only with secondary indexes
        auto initialize_look_aside = !_secondary_indexes.empty();

        if (initialize_look_aside) {
            // find look-aside index root
            auto la_it = std::ranges::find_if(roots, [](auto const &v) { return v.index_id == constant::INDEX_LOOK_ASIDE; });

            // initialize the look-aside index
            if (la_it != roots.end() && la_it->extent_id != constant::UNKNOWN_EXTENT) {
                create_look_aside_root(extension_callback);
                LOG_DEBUG(LOG_BTREE, LOG_LEVEL_DEBUG1, "Look-aside init with root: {}", la_it->extent_id);
                _look_aside_index->init(la_it->extent_id);
            } else {
                create_look_aside_root(extension_callback);
                LOG_DEBUG(LOG_BTREE, LOG_LEVEL_DEBUG1, "Look-aside init empty");
                _look_aside_index->init_empty();
            }
        }
    }

    void
    MutableTable::initialize_wc_schema(const ExtensionCallback& extension_callback)
    {
        // Use the table's existing schema (_schema is already set in constructor)
        auto schema = _schema_without_row_id;

        // Build sort keys with __springtail_lsn
        auto sort_keys = schema->get_sort_keys();
        sort_keys.push_back("__springtail_lsn");

        // Get column order
        auto columns = schema->column_order();

        // Create new columns for write cache
        SchemaColumn op("__springtail_op", 0, SchemaType::UINT8, 0, false);
        SchemaColumn lsn("__springtail_lsn", 0, SchemaType::UINT64, 0, false);
        std::vector<SchemaColumn> new_columns{op, lsn};

        // Create write cache schema
        _wc_schema = schema->create_schema(columns, new_columns, sort_keys, extension_callback, true);

        // Get table only fields, and then add internal_row_id for wc_fields
        _actual_table_fields = _wc_schema->get_fields(columns);
        columns.push_back(constant::INTERNAL_ROW_ID);

        // Cache field accessors
        _wc_op_field = _wc_schema->get_field("__springtail_op");
        _wc_fields = _wc_schema->get_fields(columns);
        _wc_key_fields = _wc_schema->get_fields(schema->get_sort_keys());
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

    StorageCache::SafePagePtr
    MutableTable::read_page(uint64_t extent_id)
    {
        return StorageCache::get_instance()->get(_db_id, _data_file, extent_id,
                _access_xid, _target_xid,
                get_max_extent_size(),
                false,
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
        auto orig_page = StorageCache::get_instance()->get(_db_id, _data_file, old_eid, _access_xid, constant::LATEST_XID, get_max_extent_size());


        // INVALIDATE PRIMARY INDEX
        TuplePtr pkey;
        Extent::Row row;
        StorageCache::Page::Iterator itr;

        if (_primary_key.empty()) {
            // no primary key, so use the old extent ID as the primary key
            auto pkey_fields = std::make_shared<FieldArray>(1);
            pkey_fields->at(0) = std::make_shared<ConstTypeField<uint64_t>>(orig_page->key().first);
            pkey = std::make_shared<FieldTuple>(pkey_fields, nullptr);
        } else {
            // has a primary key, get the last row of the original page for the primary index
            auto pkey_fields = _schema->get_fields(_primary_key);
            itr = orig_page->last();
            row = *itr;
            pkey = std::make_shared<FieldTuple>(pkey_fields, &row);
        }

        // remove the old primary index entry
        _primary_index->remove(pkey);

        if (_look_aside_index) {
            // Invalidate look aside index
            std::vector<std::string> look_aside_keys;
            look_aside_keys.push_back(constant::INTERNAL_ROW_ID);
            indexer_helpers::invalidate_index_for_page(orig_page->key().first, orig_page, _look_aside_index, look_aside_keys, _schema);
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
            auto new_page = StorageCache::get_instance()->get(_db_id, _data_file, extent_id, _target_xid, constant::LATEST_XID, get_max_extent_size());

            // POPULATE PRIMARY INDEX
            TuplePtr pkey;
            Extent::Row row;
            StorageCache::Page::Iterator itr;

            // create the new primary index entry
            (*value_fields)[0] = std::make_shared<ConstTypeField<uint64_t>>(extent_id);
            if (_primary_key.empty()) {
                // no primary key, use the extent ID itself as the primary key
                pkey = std::make_shared<FieldTuple>(value_fields, nullptr);
            } else {
                // has a primary key, use the primary key fields
                itr = new_page->last();
                row = *itr;
                pkey = std::make_shared<KeyValueTuple>(pkey_fields, value_fields, &row);
            }

            // insert the new primary index entry
            _primary_index->insert(pkey);

            if (_look_aside_index) {
                // Populate look aside index
                std::vector<std::string> look_aside_keys;
                look_aside_keys.push_back(constant::INTERNAL_ROW_ID);
                indexer_helpers::populate_index_for_page(extent_id, new_page, _look_aside_index, look_aside_keys, _schema);
            }
        }
    }

    std::vector<std::filesystem::path>
    MutableTable::get_table_files() const
    {
        std::vector<std::filesystem::path> r;
        r.emplace_back(_data_file);
        r.emplace_back(_primary_index->get_file_path());
        for (auto &secondary : _secondary_indexes) {
            r.emplace_back(secondary.second.first->get_file_path());
        }

        if (_id <= constant::MAX_SYSTEM_TABLE_ID) {
            r.emplace_back(_table_dir / constant::ROOTS_FILE);
        }

        return r;
    }

    void
    MutableTable::sync_data_and_indexes()
    {
        // sync the data file
        auto data_handle = IOMgr::get_instance()->open(_data_file,
                                                       IOMgr::IO_MODE::APPEND, true);

        data_handle->sync();

        // sync the indexes
        _primary_index->sync();
        for (auto &secondary : _secondary_indexes) {
            secondary.second.first->sync();
        }

        if (_id <= constant::MAX_SYSTEM_TABLE_ID) {
            // sync the roots file for sysntem tables
            auto root_handle = IOMgr::get_instance()->open(_table_dir / constant::ROOTS_FILE,
                                                           IOMgr::IO_MODE::APPEND, true);
            root_handle->sync();

            // also fsync() the directory to ensure the symlink+rename are persisted
            int fd = ::open(_table_dir.c_str(), O_RDONLY | O_DIRECTORY);
            CHECK(fd != -1) << "Failed to open directory " << _table_dir << ", error: " << strerror(errno);
            ::fsync(fd);
            ::close(fd);
        }
    }

    TableMetadata
    MutableTable::finalize(bool call_sync)
    {
        LOG_DEBUG(LOG_BTREE, LOG_LEVEL_DEBUG1, "Finalize {} {}", _id, _target_xid);

        // in the case of having an (initially) empty table, there are no invalidations... we can
        // flush the single Page and update the indexes
        if (_empty_page) {
            // if the empty page is empty, then we don't need to do anything here
            if (!(*_empty_page)->empty()) {
                _flush_and_populate_indexes(_empty_page->ptr());
                DCHECK(!(*_empty_page)->dirty());

                // this will release the page to the cache
                _empty_page.reset();
            }
        }

        // flush the dirty data pages of the table to disk
        auto end_offset = StorageCache::get_instance()->flush(_data_file);

        // now flush the indexes, capturing the roots
        TableMetadata metadata;
        metadata.roots.push_back({constant::INDEX_PRIMARY, _primary_index->finalize(false)});

        // Flush the look aside index if available
        if (_look_aside_index) {
            metadata.roots.push_back({constant::INDEX_LOOK_ASIDE, _look_aside_index->finalize()});
        }

        // now flush the indexes, capturing the roots
        for (auto &secondary : _secondary_indexes) {
            metadata.roots.emplace_back(secondary.first, secondary.second.first->finalize(false));
        }

        metadata.stats = _stats;
        metadata.snapshot_xid = _snapshot_xid;

        // Store file end offset for xid
        // to be used later to catch-up index if needed
        metadata.stats.end_offset = end_offset;
        metadata.stats.last_internal_row_id = _internal_row_id;

        // store the roots into a look-aside root file
        // Only maintain roots files for system tables (table_id <= MAX_SYSTEM_TABLE_ID)
        // User tables rely on metadata.roots stored in the system tables
        if (_id <= constant::MAX_SYSTEM_TABLE_ID) {
            auto extent = std::make_shared<Extent>(ExtentType(), _target_xid, _roots_schema->row_size(), _roots_schema->field_types());
            for (auto root : metadata.roots) {
                auto &&row = extent->append();
                _roots_root_f->set_uint64(&row, root.extent_id);
                _roots_index_id_f->set_uint64(&row, root.index_id);
                _roots_last_internal_row_id_f->set_uint64(&row, _internal_row_id);
            }
            auto filename = fmt::format(constant::ROOTS_XID_FILE, _target_xid);
            auto root_handle = IOMgr::get_instance()->open(_table_dir / filename,
                                                           IOMgr::IO_MODE::APPEND, true);
            // flush and wait for completion
            extent->async_flush(root_handle).wait();

            // swap the symlink
            std::filesystem::create_symlink(_table_dir / filename,
                                            _table_dir / constant::ROOTS_TMP_FILE);
            std::filesystem::rename(_table_dir / constant::ROOTS_TMP_FILE,
                                    _table_dir / constant::ROOTS_FILE);
        }

        if (call_sync) {
            // sync the data and indexes synchronously
            sync_data_and_indexes();
        }

        return metadata;
    }

    void
    MutableTable::_set_gin_index_schema(const ExtensionCallback& extension_callback)
    {
        SchemaColumn idx_position_c(constant::INDEX_POSITION_FIELD, 0, SchemaType::UINT32, 0, false);
        SchemaColumn idx_gin_token_c(constant::INDEX_GIN_TOKEN_FIELD, 0, SchemaType::TEXT, 0, false);
        SchemaColumn internal_row_id(constant::INTERNAL_ROW_ID, 0, SchemaType::UINT64, 0, false);

        std::vector<std::string> gin_index_keys;
        gin_index_keys.push_back(constant::INDEX_POSITION_FIELD);
        gin_index_keys.push_back(constant::INDEX_GIN_TOKEN_FIELD);
        gin_index_keys.push_back(constant::INTERNAL_ROW_ID);

        _gin_index_schema = _schema->create_index_schema({},
                { idx_position_c, idx_gin_token_c, internal_row_id },
                gin_index_keys, extension_callback);
    }

    MutableBTreePtr
    MutableTable::create_gin_index_root(uint64_t index_id,
                                        const ExtensionCallback& extension_callback,
                                        const OpClassHandler& opclass_handler)
    {
        std::vector<std::string> gin_index_keys;
        gin_index_keys.push_back(constant::INDEX_POSITION_FIELD);
        gin_index_keys.push_back(constant::INDEX_GIN_TOKEN_FIELD);
        gin_index_keys.push_back(constant::INTERNAL_ROW_ID);

        auto btree = std::make_shared<MutableBTree>(_db_id,
                _table_dir / fmt::format(constant::INDEX_FILE, index_id),
                gin_index_keys, _gin_index_schema,
                _target_xid,
                get_max_extent_size_secondary(),
                extension_callback,
                opclass_handler,
                constant::INDEX_TYPE_GIN
                );
        return btree;
    }

    MutableBTreePtr
    MutableTable::create_look_aside_root(const ExtensionCallback& extension_callback)
    {
        // Get the singleton look-aside schema
        _look_aside_schema = schema_helpers::get_look_aside_schema();

        std::vector<std::string> look_aside_keys;
        look_aside_keys.push_back(constant::INTERNAL_ROW_ID);

        _look_aside_index = std::make_shared<MutableBTree>(_db_id, _table_dir / constant::INDEX_LOOK_ASIDE_FILE,
                look_aside_keys,
                _look_aside_schema,
                _target_xid, get_max_extent_size_secondary(), extension_callback);

        return _look_aside_index;
    }

    MutableBTreePtr
    MutableTable::create_index_root(uint64_t index_id,
                                    const std::vector<uint32_t>& index_columns,
                                    const ExtensionCallback& extension_callback,
                                    const OpClassHandler& opclass_handler,
                                    const std::string_view index_type)
    {
        // Get the index schema - bypass cache if this is a snapshot table without system table metadata
        auto index_schema = _bypass_schema_cache
            ? schema_helpers::create_index_schema(_schema, index_columns, index_id, extension_callback)
            : TableMgr::get_instance()->get_index_schema(_db_id, _id, index_id, index_columns, XidLsn{_target_xid}, extension_callback);

        // get the column names for the keys
        auto &&col_names = _schema->get_column_names(index_columns);
        auto key = col_names;
        key.push_back(constant::INTERNAL_ROW_ID);

        auto btree = std::make_shared<MutableBTree>(_db_id,
                _table_dir / fmt::format(constant::INDEX_FILE, index_id),
                key, index_schema,
                _target_xid,
                index_id == constant::INDEX_PRIMARY? get_max_extent_size(): get_max_extent_size_secondary(),
                extension_callback,
                opclass_handler,
                index_type
                );
        return btree;
    }

    template <MutableTable::MutationType m_type>
    auto
    MutableTable::_mutation_wrapper(StorageCache::SafePagePtr &page,
                                    TuplePtr value)
    {

        // Create a context to passed to cache and so the same will be
        // passed back to the mutation_handler
        indexer_helpers::IndexMutationContext ctx{ _schema, _secondary_indexes, _index_lookup };

        // Find and invoke appropriate mutations in the cache
        if constexpr (m_type == MutationType::INSERT) {
            page->insert(value, _schema, &indexer_helpers::mutation_handler<indexer_helpers::IndexOperation::Insert>, &ctx);

        } else if constexpr (m_type == MutationType::APPEND) {
            page->append(value, _schema, &indexer_helpers::mutation_handler<indexer_helpers::IndexOperation::Insert>, &ctx);

        } else if constexpr (m_type == MutationType::UPDATE) {
            page->update(value, _schema, &indexer_helpers::mutation_handler<indexer_helpers::IndexOperation::Remove>,
                    &indexer_helpers::mutation_handler<indexer_helpers::IndexOperation::Insert>, &ctx);

        } else if constexpr (m_type == MutationType::UPSERT) {
            return page->upsert(value, _schema, &indexer_helpers::mutation_handler<indexer_helpers::IndexOperation::Remove>,
                    &indexer_helpers::mutation_handler<indexer_helpers::IndexOperation::Insert>, &ctx);

        } else if constexpr (m_type == MutationType::REMOVE) {
            page->remove(value, _schema, &indexer_helpers::mutation_handler<indexer_helpers::IndexOperation::Remove>, &ctx);

        } else if constexpr (m_type == MutationType::REMOVE_BY_SCAN) {
            return page->try_remove_by_scan(value, _schema, &indexer_helpers::mutation_handler<indexer_helpers::IndexOperation::Remove>, &ctx);

        } else {
            // Shouldn't reach here ideally
            CHECK(false);
        }
    }

    void
    MutableTable::_insert_direct(TuplePtr value,
                                 uint64_t extent_id)
    {
        // get the page from the cache
        auto page = StorageCache::get_instance()->get(_db_id, _data_file, extent_id, _access_xid, _target_xid,
                get_max_extent_size(),
                false,
                [this](StorageCache::PagePtr page) { return _flush_handler(page); } );

        // check if we need to convert the page contents to a new schema
        _check_convert_page(page);

        // add the row to the page
        _mutation_wrapper<MutationType::INSERT>(page, value);
    }

    void
    MutableTable::_insert_empty(TuplePtr value)
    {
        // get the page from the cache if we don't have one
        if (!_empty_page) {
            _empty_page = std::make_unique<StorageCache::SafePagePtr>(
                    StorageCache::get_instance()->get(_db_id, _data_file, constant::UNKNOWN_EXTENT, _access_xid, _target_xid, get_max_extent_size()));
        }

        // add the row to the page
        _mutation_wrapper<MutationType::INSERT>(*_empty_page, value);
    }

    void
    MutableTable::_append_empty(TuplePtr value)
    {
        // get the page from the cache if we don't have one
        if (!_empty_page) {
            _empty_page = std::make_unique<StorageCache::SafePagePtr>(
                    StorageCache::get_instance()->get(_db_id, _data_file, constant::UNKNOWN_EXTENT, _access_xid, _target_xid, get_max_extent_size()));
        }

        // add the row to the page
        _mutation_wrapper<MutationType::APPEND>(*_empty_page, value);
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
        auto page = StorageCache::get_instance()->get(_db_id, _data_file, extent_id, _access_xid, _target_xid,
                get_max_extent_size(),
                false,
                [this](StorageCache::PagePtr page) { return _flush_handler(page); } );

        // check if we need to convert the page contents to a new schema
        _check_convert_page(page);

        // append the value to the extent
        _mutation_wrapper<MutationType::APPEND>(page, value);
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
        uint64_t extent_id = _get_extent_id(search_key);

        // then we can do a direct insert
        _insert_direct(value, extent_id);
    }

    bool
    MutableTable::_upsert_direct(TuplePtr value, uint64_t extent_id)
    {
        // get the page from the cache
        auto page = StorageCache::get_instance()->get(_db_id, _data_file, extent_id, _access_xid, _target_xid,
                get_max_extent_size(),
                false,
                [this](StorageCache::PagePtr page) { return _flush_handler(page); } );

        // check if we need to convert the page contents to a new schema
        _check_convert_page(page);

        // add the row to the page
        return _mutation_wrapper<MutationType::UPSERT>(page, value);
    }

    bool
    MutableTable::_upsert_empty(TuplePtr value)
    {
        // get the page from the cache if we don't have one
        if (!_empty_page) {
            _empty_page = std::make_unique<StorageCache::SafePagePtr>(
                    StorageCache::get_instance()->get(_db_id, _data_file, constant::UNKNOWN_EXTENT, _access_xid, _target_xid, get_max_extent_size()));
        }

        // add the row to the page
        return _mutation_wrapper<MutationType::UPSERT>(*_empty_page, value);
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
        uint64_t extent_id = _get_extent_id(search_key);

        // then we can do a direct insert
        return _upsert_direct(value, extent_id);
    }

    void
    MutableTable::_remove_direct(TuplePtr value, uint64_t extent_id)
    {
        // get the page from the cache
        auto page = StorageCache::get_instance()->get(_db_id, _data_file, extent_id, _access_xid, _target_xid,
                get_max_extent_size(),
                false,
                [this](StorageCache::PagePtr page) { return _flush_handler(page); } );

        // check if we need to convert the page contents to a new schema
        _check_convert_page(page);

        // remove the row from the page
        // note: this can only be used when a primary key is present, otherwise use _remove_by_scan()
        _mutation_wrapper<MutationType::REMOVE>(page, value);
    }

    void
    MutableTable::_remove_empty(TuplePtr value)
    {
        // note: if we are performing a remove, there must be a page already
        CHECK(_empty_page != nullptr);

        // add the row to the page
        _mutation_wrapper<MutationType::REMOVE>(*_empty_page, value);
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
        uint64_t extent_id = _get_extent_id(key);

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
            _mutation_wrapper<MutationType::REMOVE_BY_SCAN>(*_empty_page, value);
            return;
        }

        // scan the index
        bool found = false;
        auto i = _primary_index->begin();
        while (!found && i != _primary_index->end()) {
            auto &&row = *i;
            // scan each extent, looking for a match
            uint64_t extent_id = _primary_extent_id_f->get_uint64(&row);

            auto page = StorageCache::get_instance()->get(_db_id, _data_file, extent_id, _access_xid, _target_xid,
                get_max_extent_size(),
                false,
                [this](StorageCache::PagePtr page) { return _flush_handler(page); } );

            // check if we need to convert the page contents to a new schema
            _check_convert_page(page);

            // pass the value tuple and the schema down to the page
            found = _mutation_wrapper<MutationType::REMOVE_BY_SCAN>(page, value);

            if (!found) {
                ++i;
            }
        }
    }

    // XXX: SPR-1082: update-only-affected-indexes-during-updates
    std::vector<uint64_t>
    MutableTable::_find_updated_secondary_indexes(Extent::Row existing_row, TuplePtr value)
    {
        auto existing_row_tuple = std::make_shared<FieldTuple>(_schema->get_fields(), &existing_row);
        std::vector<uint64_t> updated_fields;

        auto fields = existing_row_tuple->fields();
        for (uint64_t field_idx = 0; field_idx < fields->size(); field_idx++) {
            // Perform a comparison between the existing row and the new row
            if (!fields->at(field_idx)->equal(&existing_row, fields->at(field_idx), value->row())) {
                // set the index of the field that is updated
                updated_fields.push_back(field_idx + 1);
            }
        }

        std::vector<uint64_t> updated_index_ids;
        for (auto const &[index_id, idx] : _secondary_indexes) {
            std::vector<int> result;
            // do an intersection between the updates fields and the fields that are present
            // in the secondary index. If there is a match, the secondary index column is updated.
            std::set_intersection(updated_fields.begin(), updated_fields.end(),
                                  idx.second.begin(), idx.second.end(),
                                  std::back_inserter(result));

            if (!result.empty()) {
                LOG_INFO("One of the fields in the index {} is updated", index_id);
                updated_index_ids.push_back(index_id);
            }
        }

        return updated_index_ids;
    }

    void
    MutableTable::_update_direct(TuplePtr value, uint64_t extent_id)
    {
        // get the page from the cache
        auto page = StorageCache::get_instance()->get(_db_id, _data_file, extent_id, _access_xid, _target_xid,
                get_max_extent_size(),
                false,
                [this](StorageCache::PagePtr page) { return _flush_handler(page); } );

        // check if we need to convert the page contents to a new schema
        _check_convert_page(page);

        // update the row in the page
        // note: this can only be used when a primary key is present, otherwise update should have been split
        _mutation_wrapper<MutationType::UPDATE>(page, value);
    }

    void
    MutableTable::_update_empty(TuplePtr value)
    {
        // get the page from the cache if we don't have one
        if (!_empty_page) {
            _empty_page = std::make_unique<StorageCache::SafePagePtr>(
                    StorageCache::get_instance()->get(_db_id, _data_file, constant::UNKNOWN_EXTENT, _access_xid, _target_xid, get_max_extent_size()));
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
        uint64_t extent_id = _get_extent_id(search_key);

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

uint64_t
MutableTable::_get_extent_id(TuplePtr search_key) {
    auto i = _primary_index->lower_bound(search_key, true);
    auto &&row = *i;

    // if the primary index is not empty, get the target extent
    return _primary_extent_id_f->get_uint64(&row);
}

} // namespace springtail
