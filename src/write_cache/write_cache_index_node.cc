#include <memory>
#include <mutex>

#include <common/logging.hh>

#include <write_cache/write_cache_table_set.hh>
#include <write_cache/write_cache_index.hh>
#include <write_cache/write_cache_index_node.hh>

namespace springtail {
        /** Find child node by int id, return nullptr if not exists */
    WriteCacheIndexNodePtr
    WriteCacheIndexNode::find(uint64_t id) const
    {
        auto entry = std::make_shared<WriteCacheIndexNode>(id);
        return find(entry);
    }

    /** Find child node by passed in node, return nullptr if not exists */
    WriteCacheIndexNodePtr
    WriteCacheIndexNode::find(WriteCacheIndexNodePtr entry) const
    {
        std::shared_lock<std::shared_mutex> lock{mutex};
        auto itr = children.find(entry);
        if (itr == children.end()) {
            return nullptr;
        }
        return (*itr);
    }

    /** Find child node by id (uint64_t), if not exists then add */
    WriteCacheIndexNodePtr
    WriteCacheIndexNode::findAdd(uint64_t id, IndexType type)
    {
        auto entry = std::make_shared<WriteCacheIndexNode>(id, type);
        return findAdd(entry);
    }

    /** Find child node by node ptr, if not exists then add */
    WriteCacheIndexNodePtr
    WriteCacheIndexNode::findAdd(WriteCacheIndexNodePtr entry)
    {
        // first try to obtain a write lock
        std::unique_lock<std::shared_mutex> write_lock{mutex, std::try_to_lock};
        if (write_lock.owns_lock()) {
            // got the write lock
            return _insert_child(entry);
        }

        // try lock failed, fall back to read_lock
        std::shared_lock<std::shared_mutex> read_lock{mutex};
        auto itr = children.find(entry);
        if (itr != children.end()) {
            // found entry return it
            return (*itr);
        }

        // not found, insert entry, fall back to write lock
        read_lock.unlock();

        std::unique_lock<std::shared_mutex> new_write_lock{mutex};
        return _insert_child(entry);
    }

    void
    WriteCacheIndexNode::add(WriteCacheIndexNodePtr entry)
    {
        LOG_DEBUG(LOG_WRITE_CACHE_SERVER, LOG_LEVEL_DEBUG1, "Adding child node: {} to parent: {}\n", entry->dump(), dump());
        std::unique_lock<std::shared_mutex> write_lock{mutex};
        children.insert(entry);
    }

    /** Remove child node by ID */
    WriteCacheIndexNodePtr
    WriteCacheIndexNode::remove(uint64_t id, uint64_t &memory_removed)
    {
        auto entry = std::make_shared<WriteCacheIndexNode>(id);
        return remove(entry, memory_removed);
    }

    /** Remove child node by node ptr */
    WriteCacheIndexNodePtr
    WriteCacheIndexNode::remove(WriteCacheIndexNodePtr entry, uint64_t &memory_removed)
    {
        std::unique_lock<std::shared_mutex> write_lock{mutex};
        auto itr = children.find(entry);
        if (itr == children.end()) {
            return nullptr;
        }
        WriteCacheIndexNodePtr p = (*itr);
        children.erase(itr);
        memory_removed += p->_get_memory_size();
        return p;
    }

    uint64_t
    WriteCacheIndexNode::_get_memory_size() const
    {
        uint64_t memory_size = 0;
        switch(type) {
            case EXTENT:
                memory_size += data->byte_count();
                break;
            case EXTENT_ON_DISK:
                break;
            default:
                for (auto &child: children) {
                    memory_size += child->_get_memory_size();
                }
                break;
        }
        return memory_size;
    }

    WriteCacheIndexNodePtr
    WriteCacheIndexNode::_insert_child(WriteCacheIndexNodePtr entry)
    {

        auto itr = children.find(entry);
        if (itr != children.end()) {
            // entry now exists, return it
            return (*itr);
        }
        // insert entry and return it
        children.insert(entry);
        return entry;
    }

    void
    WriteCacheIndexNode::remove_child_if_empty(WriteCacheIndexNodePtr entry)
    {
        std::unique_lock<std::shared_mutex> write_lock{mutex};
        auto itr = children.find(entry);
        if (itr == children.end()) {
            return;
        }
        if ((*itr)->children.empty()) {
            children.erase(itr);
        }
    }
}