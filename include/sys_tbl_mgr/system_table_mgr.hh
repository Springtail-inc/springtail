#pragma once

#include <common/init.hh>

#include <storage/schema.hh>
#include <sys_tbl_mgr/table.hh>

namespace springtail {

    class SystemTableMgr : public Singleton<SystemTableMgr>
    {
        friend Singleton<SystemTableMgr>;
    public:
        /**
         * Construct a system table.
         */
        TablePtr get_system_table(uint64_t db_id, uint64_t table_id, uint64_t xid);

        /**
         * Construct a mutable system table.
         */
        MutableTablePtr get_mutable_system_table(uint64_t db_id, uint64_t table_id, uint64_t access_xid, uint64_t target_xid);

        /**
         * Retrieve the schema for a given table at a given point in time.
         * @param table_id The ID of the table being requested.
         */
        std::shared_ptr<Schema> get_schema(uint64_t table_id);

        /**
         * Retrieve an ExtentSchema for a given table at a given XID that can be used for writing /
         * updating the extent. This function assumes we are retrieving the schema of the table's
         * underlying data.
         * @param table_id The table we need the schema for.
         */
        std::shared_ptr<ExtentSchema> get_extent_schema(uint64_t table_id);

    protected:

        SystemTableMgr();
        ~SystemTableMgr() = default;

        template<typename Table>
        static std::vector<Index> _get_secondary_keys();

        std::filesystem::path _table_base; ///< The base directory for individual table directories.

        /**
         * A key for the system schema cache.
         */
        struct SystemKey {
            uint64_t table_id;
            uint64_t index_id;
            bool is_leaf;

            SystemKey(uint64_t t, uint64_t i, bool l)
                : table_id(t),
                  index_id(i),
                  is_leaf(l)
            { }

            bool operator==(const SystemKey &other) const
            {
                return (table_id == other.table_id &&
                        index_id == other.index_id &&
                        is_leaf == other.is_leaf);
            }

            friend std::size_t hash_value(const SystemKey &k)
            {
                std::size_t seed = 0;

                boost::hash_combine(seed, k.table_id);
                boost::hash_combine(seed, k.index_id);
                boost::hash_combine(seed, k.is_leaf);

                return seed;
            }
        };

        /** A map of fixed system schemas.  Maps from System Table ID to the ExtentSchema for that table. */
        std::unordered_map<SystemKey, std::shared_ptr<ExtentSchema>, boost::hash<SystemKey>> _system_cache;
    };

} // springtail