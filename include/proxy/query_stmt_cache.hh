#pragma once

#include <string>
#include <memory>
#include <variant>
#include <cstdint>
#include <map>

#include <fmt/format.h>
#include <xxhash.h>

#include <common/object_cache.hh>
#include <common/logging.hh>

#include <proxy/buffer_pool.hh>

namespace springtail {
    /**
     * @brief Cached client statement for either a prepared statement
     * or a bind portal
     */
    class QueryStmt {
    public:
        static constexpr int MAX_STMT_LENGTH = 8 * 1024 * 1024; ///< 8MB largest statement we'll cache

        /** Type of cached query string */
        enum Type : int8_t {
            SIMPLE = 0,     // Simple query string
            PACKET = 1,     // Packet data, e.g., bind or parse
            NOT_CACHED = 2  // Empty query string, execute against primary
        };

        using Data = std::variant<std::string, BufferPtr>;

        QueryStmt() = default;

        QueryStmt(const std::string_view data, bool is_read_safe)
            : _type(SIMPLE), _data(std::string(data)), _is_read_safe(is_read_safe)
        {}

        QueryStmt(const BufferPtr buffer, bool is_read_safe)
            : _type(PACKET), _data(buffer), _is_read_safe(is_read_safe)
        {}

        /** The query string if SIMPLE or the packet data if PACKET */
        QueryStmt(const std::string_view key,
                  const std::string_view data,
                  bool is_read_safe)
            : _type(SIMPLE), _is_read_safe(is_read_safe)
        {
            uint64_t hash = XXH64(data.data(), data.size(), 0);
            if (data.size() > MAX_STMT_LENGTH) {
                SPDLOG_DEBUG("Statement too long to cache: {}", data.size());
                _is_read_safe = false;
                _type = NOT_CACHED;
                _data = "";
            } else {
                _data = std::string(data);
            }
            _hash = fmt::format("{}:{:x}", key, hash);
        }

        QueryStmt(const std::string_view key,
                  BufferPtr buffer,
                  bool is_read_safe)
            : _type(PACKET), _data(buffer), _is_read_safe(is_read_safe)
        {
            uint64_t hash = XXH64(buffer->data(), buffer->size(), 0);
            if (buffer->size() > MAX_STMT_LENGTH) {
                SPDLOG_DEBUG("Statement too long to cache: {}", buffer->size());
                _is_read_safe = false;
                _type = NOT_CACHED;
                _data = "";
            }
            _hash = fmt::format("{}:{:x}", key, hash);
        }

        const BufferPtr get_buf() const {
            assert(_type == PACKET);
            return std::get<BufferPtr>(_data);
        }

        const std::string &get_query() const {
            assert(_type == SIMPLE);
            return std::get<std::string>(_data);
        }

        bool is_read_safe() const {
            return _is_read_safe;
        }

        const std::string &get_hash() const {
            return _hash;
        }

        Type type() const {
            return _type;
        }

    private:
        Type        _type;         ///< type of query string
        std::string _hash;         ///< hash of the query or buffer + name
        Data        _data;         ///< query string or packet data
        bool        _is_read_safe;  ///< is associated query read-only
    };
    using QueryStmtPtr = std::shared_ptr<QueryStmt>;

    /**
     * @brief Single cache for prepared and portal statements
     */
    class QueryStmtCache {
    public:
        static constexpr char UNAMED_ID[] = "__unamed";

        // Portal and prepared types, must match PostgreSQL, don't change
        static constexpr char PORTAL = 'P';   ///< Portal type
        static constexpr char PREPARED = 'S'; ///< Prepared type

        explicit QueryStmtCache(int capacity)
            : _lru_cache(capacity)
        {}

        QueryStmtCache(const QueryStmtCache&) = delete;
        QueryStmtCache(const QueryStmtCache&&) = delete;
        QueryStmtCache& operator=(const QueryStmtCache&) = delete;

        void remove(char type, const std::string_view name) {
            _lru_cache.evict(_generate_key(type, name, true));
        }

        QueryStmtPtr add(char type, const std::string_view name, BufferPtr buffer, bool is_read_safe) {
            std::string key = _generate_key(type, name, false);
            QueryStmtPtr stmt = std::make_shared<QueryStmt>(key, buffer, is_read_safe);
            _lru_cache.insert(key, stmt);
            return stmt;
        }

        QueryStmtPtr add(char type, const std::string_view name, const std::string &value, bool is_read_safe) {
            std::string key = _generate_key(type, name, false);
            QueryStmtPtr stmt = std::make_shared<QueryStmt>(key, value, is_read_safe);
            _lru_cache.insert(key.data(), stmt);
            return stmt;
        }

        QueryStmtPtr add(char type, const std::string_view name, const std::string_view value, bool is_read_safe) {
            std::string key = _generate_key(type, name, false);
            QueryStmtPtr stmt = std::make_shared<QueryStmt>(key, value, is_read_safe);
            _lru_cache.insert(key.data(), stmt);
            return stmt;
        }

        QueryStmtPtr get(char type, const std::string_view name) {
            return _lru_cache.get(_generate_key(type, name, true));
        }

    private:
        uint64_t _unamed_id=0;
        LruObjectCache<std::string, QueryStmt> _lru_cache;

        std::string _generate_unamed_id(char type) {
            return type + UNAMED_ID + std::to_string(_unamed_id++);
        }

        std::string _get_current_unamed_id(char type) {
            return type + UNAMED_ID + std::to_string(_unamed_id);
        }

        std::string _generate_key(char type,
                                  const std::string_view name,
                                  bool for_lookup) {
            if (name.size() == 0) {
                if (for_lookup) {
                    return _get_current_unamed_id(type);
                } else {
                    return _generate_unamed_id(type);
                }
            }
            return fmt::format("{}:{}", type, std::string(name));
        }
    };
    using QueryStmtCachePtr = std::shared_ptr<QueryStmtCache>;

    class SessionHistoryCache {
    public:
        /** History entry for session, excludes prepared statements and portals */
        struct HistoryEntry {
            enum Type : int8_t {
                SET = 1,         // set variable name
                RESET = 2,       // reset variable name or ALL if name empty
                PREPARE = 3,     // prepared statement name only
                DEALLOCATE = 4,  // deallocate name or ALL if name empty
                DECLARE = 5,     // open cursor/portal name only
                CLOSE = 6,       // close cursor/portal name or ALL if name empty
                FETCH = 7,       // fetch cursor/portal name (NOTE: MOVE mapped to FETCH by parser)
                DISCARD = 8,     // discard all if name is empty, otherwise not
                LISTEN = 9,      // listen for notification on channel name
                UNLISTEN = 10,   // unlisten on channel name or all channels if name empty
            };

            Type type;
            QueryStmt stmt;
            std::string name;

            HistoryEntry(Type type, const std::string_view name, BufferPtr buffer, bool is_read_safe)
                : type(type), stmt(buffer, is_read_safe), name(name)
            {}

            HistoryEntry(Type type,const std::string_view name, const std::string &value, bool is_read_safe)
                : type(type), stmt(value, is_read_safe), name(name)
            {}

            HistoryEntry(Type type, const std::string_view name)
                : type(type), name(name)
            {}
        };
        using HistoryEntryPtr = std::shared_ptr<HistoryEntry>;

        SessionHistoryCache() = default;

        void add(const std::string_view name,
                 HistoryEntry::Type type,
                 BufferPtr buffer,
                 bool is_read_safe)
        {
            HistoryEntryPtr entry = std::make_shared<HistoryEntry>(type, name, buffer, is_read_safe);
            prune(entry, name.size() == 0);
            _history[_current_idx++] = entry;
        }

        void add(const std::string_view name,
                 HistoryEntry::Type type,
                 const std::string &value,
                 bool is_read_safe)
        {
            HistoryEntryPtr entry = std::make_shared<HistoryEntry>(type, name, value, is_read_safe);
            prune(entry, name.size() == 0);
            _history[_current_idx++] = entry;
        }

        void remove(uint64_t idx) {
            _history.erase(idx);
        }

        auto begin() const {
            return _history.begin();
        }

        auto end() const {
            return _history.end();
        }

        auto find(uint64_t idx) const {
            return _history.find(idx);
        }

        HistoryEntryPtr get(uint64_t idx) const
        {
            auto it = _history.find(idx);
            if (it == _history.end()) {
                return nullptr;
            }
            return it->second;
        }

        uint64_t get_index() const {
            return _current_idx;
        }

    private:
        uint64_t _current_idx=0;
        std::map<uint64_t, HistoryEntryPtr> _history;

        void clear_by_type(HistoryEntry::Type type, const std::string &name, bool all)
        {
            for (auto it = _history.begin(); it != _history.end(); ) {
                if (it->second->type == type && (all || it->second->name == name)) {
                    it = _history.erase(it);
                } else {
                    ++it;
                }
            }
        }

        void prune(HistoryEntryPtr entry, bool all)
        {
            switch (entry->type) {
                case HistoryEntry::SET:
                case HistoryEntry::DECLARE:
                case HistoryEntry::FETCH:
                case HistoryEntry::LISTEN:
                case HistoryEntry::PREPARE:
                    return; // nothing to do
                case HistoryEntry::RESET:
                    clear_by_type(HistoryEntry::SET, entry->name, all);
                    break;
                case HistoryEntry::DEALLOCATE:
                    clear_by_type(HistoryEntry::PREPARE, entry->name, all);
                    break;
                case HistoryEntry::CLOSE:
                    clear_by_type(HistoryEntry::DECLARE, entry->name, all);
                    clear_by_type(HistoryEntry::FETCH, entry->name, all);
                    break;
                case HistoryEntry::UNLISTEN:
                    clear_by_type(HistoryEntry::LISTEN, entry->name, all);
                    break;
                case HistoryEntry::DISCARD:
                    if (all) {
                        _history.clear();
                    }
                    break;
                default:
                    break;
            }
        }
    };
    using SessionHistoryCachePtr = std::shared_ptr<SessionHistoryCache>;

} // namespace springtail