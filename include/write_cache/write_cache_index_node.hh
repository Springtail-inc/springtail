#pragma once

#include <common/tracking_allocator.hh>
#include <common/logging.hh>

#include <storage/extent.hh>

namespace springtail {

    class WriteCacheIndexNode;
    using WriteCacheIndexNodePtr = std::shared_ptr<WriteCacheIndexNode>;

    /**
     * @brief Generic node that is used to hold an ordered set of children of same type.
     * Used to build out XID Map Tree, top level is set of XIDs, then set of Tables then set of Extents, and then Rows.
     * Also used by the EID map, to map a set of Extents
     */
    class WriteCacheIndexNode {
    public:
        /** Compare IndexNodes for sorting based on ID */
        struct ComparatorID {
            bool operator()(const WriteCacheIndexNodePtr &lhs, const WriteCacheIndexNodePtr &rhs) const {
                return (lhs->id < rhs->id);
            }
        };

        enum IndexType : uint8_t {
            INVALID=0, // for searches
            ROOT=1,
            XID=2,
            TABLE=3,
            EXTENT=4,
            EXTENT_ON_DISK=5
        };

        /** ID of entry, uint64_t for table and extent or XID, RowID for row */
        uint64_t id;

        /** For extent on disk extent offset in file */
        uint64_t data_offset{0};

        /** For extent on disk size of extent in file */
        size_t data_size{0};

        /** Data stored in node */
        ExtentPtr data;

        /** type for sanity checking */
        IndexType type;

        /** shared mutex to protect children set */
        mutable std::shared_mutex mutex;

        /** Map holding key to next level of node */
        std::set<WriteCacheIndexNodePtr, ComparatorID, TrackingAllocator<WriteCacheIndexNodePtr, TrackingAllocatorTags::TAG_WRITE_CACHE>> children;

        /** Constructor for integer id */
        WriteCacheIndexNode(uint64_t id, IndexType type=IndexType::INVALID) : id(id), type(type) {}

        /** Constructor for integer id, extent */
        WriteCacheIndexNode(uint64_t id, const ExtentPtr extent) : id(id), data(extent), type(IndexType::EXTENT) {}

        /** Constructor for integer id, extent on disk*/
        WriteCacheIndexNode(uint64_t id, uint64_t offset, size_t size) : id(id), data_offset(offset), data_size(size), type(IndexType::EXTENT_ON_DISK) {}

        /** Find child node by int id, return nullptr if not exists */
        WriteCacheIndexNodePtr find(uint64_t id) const;

        /** Find child node by passed in node, return nullptr if not exists */
        WriteCacheIndexNodePtr find(WriteCacheIndexNodePtr entry) const;

        /** Find child node by id (uint64_t), if not exists then add */
        WriteCacheIndexNodePtr findAdd(uint64_t id, IndexType type);

        /** Find child node by node ptr, if not exists then add */
        WriteCacheIndexNodePtr findAdd(WriteCacheIndexNodePtr entry);

        /** Insert child node */
        void add(WriteCacheIndexNodePtr entry);

        /** Remove child node by ID */
        WriteCacheIndexNodePtr remove(uint64_t id, uint64_t &memory_removed);

        /** Remove child node by node ptr */
        WriteCacheIndexNodePtr remove(WriteCacheIndexNodePtr entry, uint64_t &memory_removed);

        /** Remove child node if empty */
        void remove_child_if_empty(WriteCacheIndexNodePtr entry);

        /** Convert type to string for debugging */
        std::string type_to_str() const
        {
            switch(type) {
                case IndexType::ROOT: return "ROOT";
                case IndexType::XID: return "XID";
                case IndexType::TABLE: return "TABLE";
                case IndexType::EXTENT: return "EXTENT";
                case IndexType::EXTENT_ON_DISK: return "EXTENT_ON_DISK";
                default: return "INVALID";
            }
        }

        /** Dump entry as string for debugging */
        std::string dump() const
        {
            return fmt::format("{}:{}", type_to_str(), id);
        }

    private:
        /** Insert entry into children set, write lock must be held */
        WriteCacheIndexNodePtr _insert_child(WriteCacheIndexNodePtr entry);

        /** Get memory stored in the extents under this node */
        uint64_t _get_memory_size();

    };
}