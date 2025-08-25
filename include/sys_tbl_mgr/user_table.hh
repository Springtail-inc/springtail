#pragma once

#include <storage/vacuumer.hh>
#include <sys_tbl_mgr/client.hh>
#include <sys_tbl_mgr/schema_mgr.hh>
#include <sys_tbl_mgr/table.hh>

namespace springtail {
    class UserTable : public Table, public std::enable_shared_from_this<UserTable> {
    public:
        using Table::schema;

        /**
         * UserTable constructor.
         */
        UserTable(uint64_t db_id,
                  uint64_t table_id,
                  uint64_t xid,
                  const std::filesystem::path &table_base,
                  const std::vector<std::string> &primary_key,
                  const std::vector<Index> &secondary,
                  const TableMetadata &metadata,
                  ExtentSchemaPtr schema) :
            Table(db_id, table_id, xid, table_base, primary_key, secondary, metadata, schema) {}

        /**
         * Retrieves the schema for the table at a given XID.
         */
        virtual ExtentSchemaPtr extent_schema() const override
        {
            return SchemaMgr::get_instance()->get_extent_schema(_db_id, _id, XidLsn(_xid));
        }

        /**
         * Get a schema for accessing an extent from this table that was written at the provided XID.
         */
        virtual SchemaPtr schema(uint64_t extent_xid) const override
        {
            return SchemaMgr::get_instance()->get_schema(_db_id, _id, XidLsn(extent_xid), XidLsn(_xid));
        }

    };

    class UserMutableTable: public MutableTable,
                            public std::enable_shared_from_this<UserMutableTable> {

    public:
        /**
         * System mutable table constructor.
         */
        UserMutableTable(uint64_t db_id,
                         uint64_t table_id,
                         uint64_t access_xid,
                         uint64_t target_xid,
                         const std::filesystem::path &table_base,
                         const std::vector<std::string> &primary_key,
                         const std::vector<Index> &secondary,
                         const TableMetadata &metadata,
                         ExtentSchemaPtr schema,
                         bool for_gc = false) :
            MutableTable(db_id, table_id, access_xid, target_xid, table_base, primary_key,
                         secondary, metadata, schema, for_gc) {}

        /**
         * Truncates the table, removing the callback of any mutated pages in the cache, clearing
         * all of the indexes, and marking the roots to be cleared in the system tables.
         */
        virtual void truncate() override
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

            // Smart vacuum if data exists
            if (std::filesystem::exists(_data_file)) {
                Vacuumer::get_instance()->expire_extent(_data_file, 0, std::filesystem::file_size(_data_file), _target_xid);
            } else {
                LOG_INFO("TRUNCATE: File: {} doesn't exist to report to vacuum", _data_file);
            }
        }
    };

} // springtail