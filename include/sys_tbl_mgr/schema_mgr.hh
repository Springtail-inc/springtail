#pragma once

#include <boost/thread.hpp>

#include <common/constants.hh>
#include <common/singleton.hh>
#include <common/service_register.hh>

#include <storage/schema.hh>
#include <storage/xid.hh>

namespace springtail {

    /** UserType class for holding user defined types */
    struct UserType {
        uint64_t id;                // pg type oid
        uint64_t namespace_id;      // pg namespace oid
        std::string name;           // pg type name
        nlohmann::json value_json;  // json representation of the user type value;
        bool exists;

        // for enum, it is a json array of objects with label: index pairs
        std::unordered_map<std::string, float> enum_label_map;
        std::unordered_map<float, std::string> enum_index_map;

        /** Enum type for user defined types */
        enum Type : int8_t {
            ENUM = 'E',
        } type;

        UserType(uint64_t id, bool exists=false, int8_t type=constant::USER_TYPE_ENUM)
            : id(id),
              exists(exists),
              type(static_cast<Type>(type))
        {
            DCHECK(type == constant::USER_TYPE_ENUM); // only support enum for now
        }

        UserType(uint64_t id, uint64_t namespace_id, int8_t type, const std::string &name, const std::string &value, bool exists=true)
            : id(id),
            namespace_id(namespace_id),
            name(name),
            value_json(nlohmann::json::parse(value)),
            exists(exists),
            type(static_cast<Type>(type))
        {
            DCHECK(type == constant::USER_TYPE_ENUM); // only support enum for now
            DCHECK(value_json.is_array());
            for (const auto &obj : value_json) {
                DCHECK(obj.is_object());
                auto it = obj.begin();  // Only one key-value pair per object
                float idx = it.value().get<float>();
                enum_label_map[it.key()] = idx;
                enum_index_map[idx] = it.key();
            }
        }
    };
    using UserTypePtr = std::shared_ptr<UserType>;

    class ExtentType;

    /** Interface for accessing all of the schemas for a specific table.  This includes retrieving
     *  the data schema, primary and secondary index schemas, and data in the write cache -- all at
     *  a specific XID. */
    class SchemaMgr : public Singleton<SchemaMgr>
    {
        friend class Singleton<SchemaMgr>;
    public:

        /**
         * Retrieve the column metadata for a given table at a given XID/LSN.
         * Map from column ID/column position to column metadata.
         */
        std::map<uint32_t, SchemaColumn> get_columns(uint64_t db_id, uint64_t table_id, const XidLsn &xid);

        /**
         * Retrieve the schema for a given table at a given point in time.
         * @param db_id The database ID of the table.
         * @param table_id The ID of the table being requested.
         * @param extent_xid The XID of the extent being processed.
         * @param target_xid The XID that the query is executing at.
         * @param lsn An optional LSN (logical sequence number) which tells you which schema changes within a given XID to apply up through.
         */
        std::shared_ptr<Schema> get_schema(uint64_t db_id, uint64_t table_id, const XidLsn &access_xid, const XidLsn &target_xid);

        /**
         * Retrieve an ExtentSchema for a given table at a given XID that can be used for writing /
         * updating the extent.  This function assumes we are retrieving the schema of the table's
         * underlying data.
         * @param db_id The database ID of the table.
         * @param table_id The table we need the schema for.
         * @param xid The XID/LSN that we need the schema at. Defaults to the MAX_LSN, providing the
         *            schema at the point after all changes in the XID have been applied.
         */
        std::shared_ptr<ExtentSchema> get_extent_schema(uint64_t db_id, uint64_t table_id, const XidLsn &xid);

        /**
         * @brief Get the usertype object
         * @param db_id database id
         * @param type_id user defined type id
         * @param xid The XID/LSN we need the schema at.
         * @return std::shared_ptr<UserType>
         */
        UserTypePtr get_usertype(uint64_t db_id, uint64_t type_id, const XidLsn &xid);

    private:
        /**
         * @brief Construct a new SchemaMgr object
         */
        SchemaMgr();

        ~SchemaMgr() override = default;

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

        /** Helper to convert schema column to map */
        std::map<uint32_t, SchemaColumn> _convert_columns(const std::vector<SchemaColumn> &columns);
    };

    class SchemaMgrRunner : public ServiceRunner {
    public:
        SchemaMgrRunner() : ServiceRunner("SchemaMgr") {}

        ~SchemaMgrRunner() override = default;

        bool start() override
        {
            SchemaMgr::get_instance();
            return true;
        }

        void stop() override
        {
            SchemaMgr::shutdown();
        }
    };

}
