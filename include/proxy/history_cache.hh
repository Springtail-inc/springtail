#pragma once

#include <variant>
#include <map>

#include <absl/log/check.h>
#include <xxhash.h>

#include <common/object_cache.hh>

#include <proxy/buffer_pool.hh>

namespace springtail::pg_proxy {

    /**
     * @brief Encapsulates a query statement, this may be a full packet or a portion of a simple query string
     * This is used to cache prepared and portal statements, as well it is used to track history of statements
     * for a session or transaction.
     */
    struct QueryStmt {
        /** Type of statement */
        enum Type : int8_t {
            NONE = 0,           ///< no statement
            SET = 1,            ///< set variable name
            SET_LOCAL = 2,      ///< set local variable name (transaction scope)
            RESET = 3,          ///< reset variable name
            RESET_ALL = 4,      ///< reset all variables
            PREPARE = 5,        ///< prepared/parse statement name (unamed if empty name)
            DEALLOCATE = 6,     ///< deallocate name (unamed if empty name)
            DEALLOCATE_ALL = 7, ///< deallocate all prepared statements
            DECLARE_HOLD = 8,   ///< open cursor with hold cursor name
            DECLARE = 9,        ///< open (bind) cursor/portal name only (unamed if empty name)
            CLOSE = 10,         ///< close cursor/portal name (unamed if empty name)
            CLOSE_ALL = 11,     ///< close all portals
            FETCH = 12,         ///< fetch cursor/portal name (NOTE: MOVE mapped to FETCH by parser)
            DISCARD = 13,       ///< discard
            DISCARD_ALL = 14,   ///< discard all (close all; reset all, dealloate all, unlisten *)
            LISTEN = 15,        ///< listen for notification on channel name
            UNLISTEN = 16,      ///< unlisten on channel name
            UNLISTEN_ALL = 17,  ///< unlisten all channels
            SAVEPOINT = 18,     ///< savepoint name
            ROLLBACK_TO_SAVEPOINT = 19, ///< rollback to savepoint name
            RELEASE_SAVEPOINT = 20,     ///< release savepoint name
            BEGIN = 21,         ///< begin transaction
            COMMIT = 22,        ///< commit transaction
            ROLLBACK = 23,      ///< rollback transaction
            EXECUTE = 24,       ///< execute statement
            SYNC = 25,          ///< sync for extended query
            DESCRIBE = 26,      ///< describe statement
            SIMPLE_QUERY = 27,  ///< simple query statement parent (not cached)
            FUNCTION = 28,      ///< function call
            ANONYMOUS = 29,     ///< anonymous statement (no name)
        };

        /** Type of cached query string */
        enum DataType : int8_t {
            SIMPLE = 0,     ///< Simple query string
            PACKET = 1,     ///< Packet data, e.g., bind or parse
            NOT_CACHED = 2  ///< Empty query string, execute against primary
        };

        using Data = std::variant<std::string, BufferPtr>;

        QueryStmt(Type type, const Data data, bool is_read_safe, const std::string &name = {})
            : type(type), data(data), name(name), is_read_safe(is_read_safe)
        {
            if (std::holds_alternative<std::string>(data)) {
                data_type = SIMPLE;
            } else if (std::holds_alternative<BufferPtr>(data)) {
                data_type = PACKET;
            }
        }

        QueryStmt(Type type, bool is_read_safe, const std::string &name = {})
            : type(type), data_type(NOT_CACHED), name(name), is_read_safe(is_read_safe)
        {}

        /** Get buffer */
        const BufferPtr buffer() const {
            CHECK_EQ(data_type, PACKET);
            return std::get<BufferPtr>(data);
        }

        /** Get query string */
        const std::string &query() const {
            CHECK_EQ(data_type, SIMPLE);
            return std::get<std::string>(data);
        }

        /** Get hash of data */
        uint64_t hash() const {
            if (data_type == SIMPLE) {
                return XXH64(query().data(), query().size(), 0);
            } else if (data_type == PACKET) {
                return XXH64(buffer()->data(), buffer()->size(), 0);
            }
            return 0;
        }

        /** Generate a hashed name for statement */
        const std::string &get_hashed_name()
        {
            if (!hashed_name.empty()) {
                return hashed_name;
            }
            hashed_name = name + ":" + std::to_string(hash());
            return hashed_name;
        }

        bool is_extended() const {
            if (type == SIMPLE_QUERY || data_type == SIMPLE) {
                assert (extended_type == NONE);
                return false;
            }
            return true;
        }

        Type         type;          ///< type of query string
        Type         extended_type=NONE; ///< type of extended query
        DataType     data_type;     ///< type of data
        Data         data;          ///< query string or packet data
        std::string  name;          ///< name of the statement (if named, e.g., prepared, portal, savepoint)
        std::string  hashed_name;   ///< hashed name of the statement
        bool         is_read_safe;   ///< is associated query read-only
        std::shared_ptr<QueryStmt> dependency;  ///< dependent statement (e.g., bind depends on prepare)
        std::vector<std::shared_ptr<QueryStmt>> children; ///< children statements (e.g., of a simple query)
        std::vector<std::shared_ptr<QueryStmt>> set_config_calls; ///< set_config function calls
    };
    using QueryStmtPtr = std::shared_ptr<QueryStmt>;

    /** Cache of statements (used as a prepared statement cache) */
    using QueryStmtCache = LruObjectCache<std::string, QueryStmt>;
    using QueryStmtCachePtr = std::shared_ptr<QueryStmtCache>;

    /**
     * Cache of history for either a session or transaction
     * Contains state modifying commands that can be replayed against a
     * new session to bring it up-to-date with current session.
     */
    class HistoryCache {
    public:
        HistoryCache() = default;

        /**
         * @brief Lookup a history entry by type and name
         * @param name name associated with the history entry
         * @param type type of the statement
         * @return QueryStmtPtr or nullptr if not found
         */
        QueryStmtPtr lookup(const std::string_view name, QueryStmt::Type type) const;

        /**
         * @brief Clear the history cache
         */
        void clear() {
            _history.clear();
            _current_idx = 1;
        }

        /**
         * @brief Add history entry
         * @param entry history entry to add
         */
        void add(QueryStmtPtr entry);

        /**
         * @brief Compact the history cache by removing redundant entries; static for testing
         * @param replay_idx index that sessions have replayed up to
         * @param history history map to compact
         * @return compacted history map
         */
        static std::map<uint64_t, QueryStmtPtr>
        compact(uint64_t replay_idx, const std::map<uint64_t, QueryStmtPtr> &history);

        /**
         * @brief Compact the current history cache; updates _history
         * @param replay_idx index that sessions have replayed up to
         */
        void compact(uint64_t replay_idx) {
            _history = HistoryCache::compact(replay_idx, _history);
        }

        /**
         * @brief Get all entries in the statement history cache
         * @return vector of QueryStmtPtr
         */
        std::vector<QueryStmtPtr> get_all_entries() const;

        /**
         * @brief Get the size of the history cache
         * @return size_t size of the history cache
         */
        size_t get_size() const {
            return _history.size();
        }

        /**
         * @brief Is the history cache empty?
         * @return true if empty, false otherwise
         */
        bool is_empty() const {
            return _history.empty();
        }

    private:
        friend class StatementCache;

        uint64_t _current_idx{1};                      ///< current index of the history cache
        std::map<uint64_t, QueryStmtPtr> _history;     ///< history cache, map from idx to entry

        /**
         * @brief Helper to prune the history cache by type and name
         * @param type type of statement
         * @param name name of the statement
         * @param all if true, remove all entries with the same type (name is ignored)
         */
        void _clear_by_type(QueryStmt::Type type, const std::string &name, bool all);

    };
    using HistoryCachePtr = std::shared_ptr<HistoryCache>;

    /**
     * @brief The StatementCache class represents a cache for storing statement history.
     *
     * It contains three levels of caches: a statement cache, a transaction cache, and session cache.
     * The statement cache holds in progress statements that are not yet committed to the transaction.
     * The transaction cache holds statements that are part of the current transaction.
     * The session cache holds statements that are part of the current session (committed transactions).
     */
    class StatementCache {
    public:

         /**
         * @brief Interface to access internal cache state
         */
        struct CacheState {
            std::vector<QueryStmtPtr> session_history;
            bool in_error;
        };

        StatementCache() = default;

        /**
         * @brief Add a statement to the cache.
         * @param name The name of the statement.
         * @param type The type of the statement.
         * @param value The actual statement.
         * @param is_read_safe Indicates if the statement is read-safe.
         * @return A pointer to the QueryStmt object.
         */
        QueryStmtPtr add(QueryStmt::Type type,
                         const QueryStmt::Data &data,
                         bool is_read_safe,
                         const std::string &name={})
        {
            QueryStmtPtr entry = std::make_shared<QueryStmt>(type, data, is_read_safe, name);
            _statement_history.add(entry);
            return entry;
        }

        /**
         * @brief Add a query statement to the cache.
         * @param entry The statement to add.
         */
        void add(QueryStmtPtr entry) {
            _statement_history.add(entry);
        }

        /**
         * @brief Lookup a prepared statement in the cache.
         * @param name The name of the prepared statement.
         * @return A pointer to the QueryStmt object if found, nullptr otherwise;
         *         and a boolean indicating if the statement was found in the current transaction (true)
         *         or in the session history (false).
         */
        std::pair<QueryStmtPtr,bool> lookup_prepared(const std::string_view name) {
            return _lookup(name, QueryStmt::PREPARE);
        }

        /**
         * @brief Lookup a portal statement in the cache.
         * @param name The name of the prepared statement.
         * @return A pointer to the QueryStmt object if found, nullptr otherwise;
         *         and a boolean indicating if the statement was found in the current transaction (true)
         *         or in the session history (false).
         */
        std::pair<QueryStmtPtr,bool> lookup_portal(const std::string_view name) {
            return _lookup(name, QueryStmt::DECLARE);
        }

        /**
         * @brief Merge the statement into the transaction history if no error occurred.
         * NOTE: this is not necessarily a commit sql operation, it just means the current
         * statement has completed.
         * @param stmt The statement to commit
         * @param completed The number of completed sub statements
         */
        void commit_statement(QueryStmtPtr stmt, int completed, bool success=true);

        /**
         * @brief Reached a sync point; READY FOR QUERY, if not in xact then implicitly commit or rollback
         * @param xact_status The transaction status: 'I' for idle, 'T' for in transaction, 'E' for error
         */
        void sync_transaction(char xact_status);

        /**
         * @brief Clear the statement cache in preparation for a new statement
         */
        void clear_statement() {
            _statement_history.clear();
        }

       /**
         * @brief Get state of all internal caches
         * @return CacheState containing all cache contents
         */
        CacheState get_cache_state() const;

        /**
         * @brief Add a new session to the replay map
         * @param session_id The session id
         */
        void add_session(uint64_t session_id) {
            _session_replay_map[session_id] = 0;
        }

        /**
         * @brief Remove a session from the replay map
         * @param session_id The session id
         */
        void remove_session(uint64_t session_id) {
            _session_replay_map.erase(session_id);
        }

        /**
         * @brief Set the session replay idx object; for testing
         * @param session_id The session id
         * @param idx The replay index
         */
        void set_session_replay_idx(uint64_t session_id, uint64_t idx) {
            _session_replay_map[session_id] = idx;
        }

    private:
        HistoryCache _session_history;     ///< session history cache
        HistoryCache _transaction_history; ///< transaction history cache
        HistoryCache _statement_history;   ///< statement history cache

        /** Map of session id to statement index for replay; maps session id to idx */
        std::unordered_map<uint64_t, uint64_t> _session_replay_map;

        bool _in_error = false;

        /**
         * @brief Lookup a statement in the cache.
         * @param name The name of the statement.
         * @param type The type of the statement (only prepare supported for now)
         * @return A pointer to the QueryStmt object if found, nullptr otherwise,
         *         and a boolean indicating if the statement was found in the current transaction (true)
         *         or in the session history (false).
         */
        std::pair<QueryStmtPtr, bool> _lookup(const std::string_view name, QueryStmt::Type type);

        /**
         * @brief Commit a single statement to the transaction history if no error occurred.
         * @param stmt The statement to commit.
         */
        void _commit_single_stmt(QueryStmtPtr stmt);

        /**
         * @brief Commit the transaction; merge with session history.
         */
        void _commit_transaction();

        /**
         * @brief Rollback the transaction history.
         */
        void _rollback_transaction();

        /**
         * @brief Rollback transaction to a savepoint; release all statements after and including savepoint.
         * @param name The name of the savepoint.
         */
        void _rollback_to_savepoint(const std::string &name);

        /**
         * @brief Release a savepoint. Remove latest savepoint entry matching name.
         * @param name The name of the savepoint.
         */
        void _release_savepoint(const std::string &name);

        /**
         * @brief Get the min replay index across all sessions
         * @return uint64_t min replay index
         */
        uint64_t _get_replay_idx(void) const {
            uint64_t min_idx = UINT64_MAX;
            for (const auto &pair : _session_replay_map) {
                if (pair.second < min_idx) {
                    min_idx = pair.second;
                }
            }
            return min_idx;
        }
    };

} // namespace springtail::pg_proxy
