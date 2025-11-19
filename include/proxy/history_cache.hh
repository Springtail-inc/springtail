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
        enum class Type : int8_t {
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
        enum class DataType : int8_t {
            SIMPLE = 0,     ///< Simple query string
            PACKET = 1,     ///< Packet data, e.g., bind or parse
            NOT_CACHED = 2  ///< Empty query string, execute against primary
        };

        using Data = std::variant<std::string, BufferPtr>;

        QueryStmt(Type type, const Data data, bool is_read_safe, const std::string &name = {})
            : type(type), data(data), name(name), is_read_safe(is_read_safe)
        {
            if (std::holds_alternative<std::string>(data)) {
                data_type = DataType::SIMPLE;
            } else if (std::holds_alternative<BufferPtr>(data)) {
                data_type = DataType::PACKET;
            }
        }

        QueryStmt(Type type, bool is_read_safe, const std::string &name = {})
            : type(type), data_type(DataType::NOT_CACHED), name(name), is_read_safe(is_read_safe)
        {}

        /** Get buffer */
        const BufferPtr buffer() const {
            CHECK(data_type == DataType::PACKET);
            return std::get<BufferPtr>(data);
        }

        /** Get query string */
        const std::string &query() const {
            CHECK(data_type == DataType::SIMPLE);
            return std::get<std::string>(data);
        }

        bool is_extended() const {
            if (type == Type::SIMPLE_QUERY || data_type == DataType::SIMPLE) {
                assert (extended_type == Type::NONE);
                return false;
            }
            return true;
        }

        uint64_t     replay_id{0};  ///< replay id for history cache
        Type         type;          ///< type of query string
        Type         extended_type=Type::NONE; ///< type of extended query;
                                               // e.g., if this is a PARSE command, what is the underlying query type
        DataType     data_type;     ///< type of data
        Data         data;          ///< query string or packet data
        std::string  name;          ///< name of the statement (if named, e.g., prepared, portal, savepoint)
        bool         is_read_safe;  ///< is associated query read-only

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
         * This parameter is used to determine which entries can safely be removed.
         * Entries with an index less than or equal to replay_idx have already been
         * replayed by the session, so redundant entries can be removed. Some removal entries
         * with an index greater than replay_idx must be retained to ensure correct
         * behavior for future replays.
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

        /**
         * @brief Get the replay history statements above a given replay id
         * @param replay_id replay id to get statements above
         * @param read_only if true, only return read-safe statements
         * @return std::vector<QueryStmtPtr>
         */
        std::vector<QueryStmtPtr> get_replay_history(uint64_t replay_id, bool read_only) const;

    private:
        friend class StatementCache;

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
            entry->replay_id = _current_replay_id++;
            _statement_history.add(entry);
            return entry;
        }

        /**
         * @brief Add a query statement to the cache.
         * @param entry The statement to add.
         */
        void add(QueryStmtPtr entry) {
            entry->replay_id = _current_replay_id++;
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
            return _lookup(name, QueryStmt::Type::PREPARE);
        }

        /**
         * @brief Lookup a portal statement in the cache.
         * @param name The name of the prepared statement.
         * @return A pointer to the QueryStmt object if found, nullptr otherwise;
         *         and a boolean indicating if the statement was found in the current transaction (true)
         *         or in the session history (false).
         */
        std::pair<QueryStmtPtr,bool> lookup_portal(const std::string_view name) {
            return _lookup(name, QueryStmt::Type::DECLARE);
        }

        /**
         * @brief Merge the statement into the transaction history if no error occurred.
         * NOTE: this is not necessarily a commit sql operation, it just means the current
         * statement has completed.
         * @param stmt The statement to commit
         * @param completed The number of completed sub statements
         * @param session_id The session id of the server session
         * @param success true if statement completed successfully, false if error occurred
         */
        void commit_statement(QueryStmtPtr stmt, int completed, uint64_t session_id, bool success=true);

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
         * @brief Update the session replay idx object
         * @param session_id The session id
         * @param idx The replay index
         */
        void set_session_replay_idx(uint64_t session_id, uint64_t idx) {
            // make sure idx is > existing idx
            auto [it, inserted] = _session_replay_map.try_emplace(session_id, idx);
            if (!inserted && idx > it->second) {
                it->second = idx;
            }
        }

        /**
         * @brief Get the session history for a specific session
         * @param session_id The session id
         * @param read_only If true, only return read-only statements
         * @return A vector of QueryStmtPtr representing the session history
         */
        std::vector<QueryStmtPtr> get_session_history(uint64_t session_id, bool read_only) const {
            return _get_replay_history(session_id, read_only, true, false, false);
        }

        /**
         * @brief Get the transaction history for a specific session
         * @param session_id The session id
         * @param read_only If true, only return read-only statements
         * @return std::vector<QueryStmtPtr>
         */
        std::vector<QueryStmtPtr> get_transaction_history(uint64_t session_id, bool read_only) const {
            return _get_replay_history(session_id, read_only, false, true, false);
        }

        /**
         * @brief Get the statement history for a specific session
         * @param session_id The session id
         * @param read_only If true, only return read-only statements
         * @return std::vector<QueryStmtPtr>
         */
        std::vector<QueryStmtPtr> get_statement_history(uint64_t session_id, bool read_only) const {
            return _get_replay_history(session_id, read_only, false, false, true);
        }

    private:
        uint64_t _current_replay_id{1};    ///< current replay id; ordering across all caches

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
        uint64_t _get_min_replay_idx(void) const {
            uint64_t min_idx = UINT64_MAX;
            for (const auto &pair : _session_replay_map) {
                if (pair.second < min_idx) {
                    min_idx = pair.second;
                }
            }
            return min_idx;
        }

        /**
         * @brief Get query statements to replay from the session history based on last replay index
         * @param session_id The session id
         * @param read_only If true, only return read-only statements (for replica replay)
         * @param session_history If true, include session history
         * @param transaction_history If true, include transaction history
         * @param statement_history If true, include all statement history (transaction must be true)
         * @return vector of QueryStmtPtr to replay
         */
        std::vector<QueryStmtPtr> _get_replay_history(
            uint64_t session_id, bool read_only,
            bool session_history=true,
            bool transaction_history=false,
            bool statement_history=false) const;
    };

} // namespace springtail::pg_proxy
