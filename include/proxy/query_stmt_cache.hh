#pragma once

#include <string>
#include <memory>

#include <common/object_cache.hh>
#include <proxy/buffer_pool.hh>

namespace springtail {
    /**
     * @brief Cached client statement for either a prepared statement
     * or a bind portal
     */
    struct QueryStmt {
        /** Type of cached query string */
        enum Type : int8_t {
            SIMPLE = 0,     // Simple query string
            PACKET = 1,     // Packet data, e.g., bind or parse
            NOT_CACHED = 2  // Empty query string, execute against primary
        } type;

        /** The query string if SIMPLE or the packet data if PACKET */
        std::string data;
        BufferPtr buffer;

        /** Is this readonly */
        bool is_readonly;

        QueryStmt() : type (NOT_CACHED), is_readonly(false) {}

        QueryStmt(const std::string &data, bool is_readonly)
            : type(SIMPLE), data(data), is_readonly(is_readonly)
        {}

        QueryStmt(BufferPtr buffer, bool is_readonly)
            : type(PACKET), buffer(buffer), is_readonly(is_readonly)
        {}
    };
    using QueryStmtPtr = std::shared_ptr<QueryStmt>;

    class QueryStmtCache {
    public:
        static constexpr int MAX_STMT_LENGTH = 8 * 1024 * 1024; ///< 8MB largest statement we'll cache
        static constexpr char UNAMED_ID[] = "__unamed_id";

        explicit QueryStmtCache(int capacity)
            : _lru_cache(capacity)
        {}

        QueryStmtCache(const QueryStmtCache&) = delete;
        QueryStmtCache(const QueryStmtCache&&) = delete;
        QueryStmtCache& operator=(const QueryStmtCache&) = delete;

        void remove() {
            std::string key = _get_current_unamed_id();
            remove(key);
        }

        void remove(const std::string &key) {
            _lru_cache.evict(key);
        }

        void add(const std::string &key, QueryStmtPtr value) {
            _lru_cache.insert(key, value);
        }

        QueryStmtPtr get(const std::string &key) {
            if (key.size() == 0) {
                return _lru_cache.get(_get_current_unamed_id());
            }
            return _lru_cache.get(key);
        }

    private:
        uint64_t _unamed_id=0;
        LruObjectCache<std::string, QueryStmt> _lru_cache;

        std::string _generate_unamed_id() {
            return UNAMED_ID + std::to_string(_unamed_id++);
        }

        std::string _get_current_unamed_id() {
            return UNAMED_ID + std::to_string(_unamed_id);
        }
    };
    using QueryStmtCachePtr = std::shared_ptr<QueryStmtCache>;

} // namespace springtail