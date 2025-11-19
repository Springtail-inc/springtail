#include <set>

#include <proxy/history_cache.hh>

namespace springtail {
namespace pg_proxy {

    void
    HistoryCache::add(QueryStmtPtr entry)
    {
        // convert FETCH to MOVE; rest of query remains the same
        if (entry->type == QueryStmt::Type::FETCH) {
            std::string query = entry->query();

            // convert query to lowercase
            std::string lower = query;
            std::transform(query.begin(), query.end(), lower.begin(),
                           [](unsigned char c){ return std::tolower(c); });

            // search for the word 'fetch' in the lower case string to find position
            size_t pos = lower.find("fetch");
            if (pos != std::string::npos) {
                // replace 'fetch' with 'move' in query string
                query.replace(pos, 5, "MOVE");
                entry->data = query;
            }
        }

        _history[entry->replay_id] = entry;
    }

    QueryStmtPtr
    HistoryCache::lookup(const std::string_view name, QueryStmt::Type type) const
    {
        for (auto it = _history.rbegin(); it != _history.rend(); ++it) {
            if (it->second->type == type && it->second->name == name) {
                return it->second;
            }
        }
        return nullptr;
    }

    // Add the new interface methods for HistoryCache
    std::vector<QueryStmtPtr>
    HistoryCache::get_all_entries() const
    {
        std::vector<QueryStmtPtr> all_entries;
        for (const auto& [index, statement_ptr] : _history) {
            all_entries.push_back(statement_ptr);
        }
        return all_entries;
    }

    void
    HistoryCache::_clear_by_type(QueryStmt::Type type, const std::string &name, bool all)
    {
        // iterate through history pruning as we go, it is inefficient but we expect
        // the history to be small...
        for (auto it = _history.begin(); it != _history.end(); ) {
            if (it->second->type == type && (all || it->second->name == name)) {
                it = _history.erase(it);
                LOG_INFO("Pruned statement from history: type={}, name='{}'", static_cast<int>(type), name);
            } else {
                ++it;
            }
        }
    }

    std::map<uint64_t, QueryStmtPtr>
    HistoryCache::compact(uint64_t replay_idx, const std::map<uint64_t, QueryStmtPtr> &history)
    {
        std::map<uint64_t, QueryStmtPtr> new_history; // hold new compacted history

        // Map to track the latest state of statements up to replay_idx
        // query type -> name -> (index, QueryStmtPtr)
        std::unordered_map<QueryStmt::Type,
            std::unordered_map<std::string, std::pair<uint64_t, QueryStmtPtr>>> statement_map;

        // Flag to indicate if we are still processing entries up to replay_idx
        // After we pass replay_idx, we cannot be sure about the state of statements
        // so we preserve the creation and deletion statements after that point.
        bool replayed = true;

        // --- Phase 1: Scan up to replay_idx ---
        for (auto const& [idx, stmt] : history) {
            if (idx > replay_idx) {
                replayed = false;
            }

            switch (stmt->type) {
                case QueryStmt::Type::SET: // set variable
                    statement_map[QueryStmt::Type::SET][stmt->name] = {idx, stmt};
                    break;

                case QueryStmt::Type::PREPARE: // add prepared statement
                    DCHECK(!stmt->name.empty()); // unnamed prepares not cached, only within transaction
                    statement_map[QueryStmt::Type::PREPARE][stmt->name] = {idx, stmt};
                    break;

                case QueryStmt::Type::DECLARE_HOLD: // add cursor/portal
                    statement_map[QueryStmt::Type::DECLARE_HOLD][stmt->name] = {idx, stmt};
                    break;

                case QueryStmt::Type::FETCH: // cursor/portal use
                    // no need to track usage, just ensure it exists
                    if (statement_map[QueryStmt::Type::DECLARE_HOLD].contains(stmt->name)) {
                        statement_map[QueryStmt::Type::FETCH][stmt->name] = {idx, stmt};
                    }
                    break;

                case QueryStmt::Type::LISTEN: // add listen
                    statement_map[QueryStmt::Type::LISTEN][stmt->name] = {idx, stmt};
                    break;

                case QueryStmt::Type::RESET: // remove variable(s)
                    statement_map[QueryStmt::Type::SET].erase(stmt->name);
                    // keep the RESET statement to reset config variables
                    new_history[idx] = stmt;
                    break;

                case QueryStmt::Type::RESET_ALL: // remove variable(s)
                    statement_map[QueryStmt::Type::SET].clear();
                    // keep the RESET ALL statement to reset config variables
                    new_history[idx] = stmt;
                    break;

                case QueryStmt::Type::DEALLOCATE: // remove prepared statement
                    if (replayed) {
                        statement_map[QueryStmt::Type::PREPARE].erase(stmt->name);
                    } else {
                        // If there are new statements after replay_idx,
                        // we cannot be sure if this DEALLOCATE affects
                        // prepared statements before or after replay_idx.
                        // So we keep it to be safe.
                        new_history[idx] = stmt;
                    }
                    break;

                case QueryStmt::Type::DEALLOCATE_ALL: // remove all prepared statements
                    statement_map[QueryStmt::Type::PREPARE].clear();
                    // keep the DEALLOCATE ALL statement
                    new_history[idx] = stmt;
                    break;

                case QueryStmt::Type::CLOSE:  // remove cursor/portal
                    if (replayed) {
                        statement_map[QueryStmt::Type::DECLARE_HOLD].erase(stmt->name);
                        statement_map[QueryStmt::Type::FETCH].erase(stmt->name);
                    } else {
                        // Similar logic as DEALLOCATE
                        new_history[idx] = stmt;
                    }
                    break;

                case QueryStmt::Type::CLOSE_ALL: // remove all cursors/portals
                    if (replayed) {
                        statement_map[QueryStmt::Type::DECLARE_HOLD].clear();
                    } else {
                        // safe to clear FETCH as the DECLARE_HOLD entries are still there
                        statement_map[QueryStmt::Type::FETCH].clear();

                        // Similar logic as DEALLOCATE
                        new_history[idx] = stmt;
                    }
                    break;

                case QueryStmt::Type::UNLISTEN: // remove listen
                    // safe to clear LISTENs no errors if channel doesn't exist
                    statement_map[QueryStmt::Type::LISTEN].erase(stmt->name);
                    // keep the UNLISTEN statement
                    new_history[idx] = stmt;
                    break;

                case QueryStmt::Type::UNLISTEN_ALL: // remove all listens
                    statement_map[QueryStmt::Type::LISTEN].clear();
                    // keep the UNLISTEN ALL statement
                    new_history[idx] = stmt;
                    break;

                case QueryStmt::Type::DISCARD_ALL: // discard all
                    statement_map.clear();
                    // keep the DISCARD ALL statement
                    new_history[idx] = stmt;
                    break;

                case QueryStmt::Type::BEGIN:
                case QueryStmt::Type::COMMIT:
                case QueryStmt::Type::SYNC:
                    // always keep transaction control statements
                    new_history[idx] = stmt;
                    break;

                default:
                    // Other statement types are not tracked for compaction
                    break;
            }
        }

        // --- Phase 2: Rebuild compacted history
        // Add retained statements from the statement_map
        for (const auto& [type, name_map] : statement_map) {
            for (const auto& [name, idx_stmt_pair] : name_map) {
                new_history[idx_stmt_pair.first] = idx_stmt_pair.second;
            }
        }

        // --- Phase 3: optimize by removing empty transaction blocks ---
        QueryStmt::Type last_type = QueryStmt::Type::NONE;
        for (auto it = new_history.begin(); it != new_history.end(); ) {
            if (it->second->type == QueryStmt::Type::COMMIT &&
                       last_type == QueryStmt::Type::BEGIN) {
                // remove both BEGIN and COMMIT as they are empty
                auto begin_it = std::prev(it);
                new_history.erase(begin_it);
                it = new_history.erase(it);

                // set last type based on new iterator position
                last_type = (it == new_history.begin()) ? QueryStmt::Type::NONE : std::prev(it)->second->type;
                continue;
            }

            // remove multiple sync statements in a row
            if (it->second->type == QueryStmt::Type::SYNC && last_type == QueryStmt::Type::SYNC) {
                // remove the current SYNC statement
                it = new_history.erase(it);
                continue;
            }

            last_type = it->second->type;
            ++it;
        }

        return new_history;
    }

    std::vector<QueryStmtPtr>
    HistoryCache::get_replay_history(uint64_t replay_idx, bool read_only) const
    {
        std::vector<QueryStmtPtr> replay_history;

        for (const auto& [idx, stmt] : _history) {
            if (idx > replay_idx && (!read_only || stmt->is_read_safe)) {
                replay_history.push_back(stmt);
            }
        }

        return replay_history;
    }

    std::vector<QueryStmtPtr>
    StatementCache::_get_replay_history(uint64_t session_id,
        bool read_only,
        bool session_history,
        bool transaction_history,
        bool statement_history) const
    {
        std::vector<QueryStmtPtr> statements_to_replay;

        // find the starting replay index for this session
        auto itr = _session_replay_map.find(session_id);
        if (itr == _session_replay_map.end()) {
            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG2,
                    "No replay index found for session {}, returning empty replay history",
                    session_id);
            return statements_to_replay;
        }

        auto starting_replay_index = itr->second;

        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG2,
                "Replaying session history for session {} starting from index {}",
                session_id, starting_replay_index);

        if (session_history) {
            return _session_history.get_replay_history(starting_replay_index, read_only);
        }

        if (transaction_history) {
            return _transaction_history.get_replay_history(starting_replay_index, read_only);
        }

        if (statement_history) {
            return _statement_history.get_replay_history(starting_replay_index, read_only);
        }

        return statements_to_replay;
    }

    std::pair<QueryStmtPtr, bool>
    StatementCache::_lookup(const std::string_view name, QueryStmt::Type type)
    {
        // check statement history first
        QueryStmtPtr qs = _statement_history.lookup(name, type);
        if (qs) {
            return {qs, true};
        }

        // then check transaction history
        qs = _transaction_history.lookup(name, type);
        if (qs) {
            return {qs, true};
        }

        // then check the session history
        qs = _session_history.lookup(name, type);
        return {qs, false};
    }

    void
    StatementCache::_rollback_to_savepoint(const std::string &name)
    {
        std::list<QueryStmtPtr> rollback_list;

        // find the savepoint in the transaction history
        auto rend = _transaction_history._history.rend();
        for (auto it = _transaction_history._history.rbegin(); it != rend; ++it) {
            if (it->second->type == QueryStmt::Type::DEALLOCATE ||
                it->second->type == QueryStmt::Type::DEALLOCATE_ALL ||
                it->second->type == QueryStmt::Type::PREPARE) {
                // want to keep these in the transaction history
                rollback_list.push_front(it->second);
            }
            if (it->second->type == QueryStmt::Type::SAVEPOINT && it->second->name == name) {
                // remove all statements after this savepoint (using reverse iterator)
                _transaction_history._history.erase(std::next(it).base(), _transaction_history._history.end());

                // add back to transaction history from rollback list
                for (auto rit = rollback_list.begin(); rit != rollback_list.end(); ++rit) {
                    _transaction_history.add(*rit);
                }
            }
        }
    }

    void
    StatementCache::_release_savepoint(const std::string &name)
    {
        // find the savepoint in the transaction history
        auto rend = _transaction_history._history.rend();
        for (auto it = _transaction_history._history.rbegin(); it != rend; ++it) {
            if (it->second->type == QueryStmt::Type::SAVEPOINT && it->second->name == name) {
                // remove this entry (using reverse iterator)
                _transaction_history._history.erase(std::next(it).base());
                return;
            }
        }
    }

    StatementCache::CacheState
    StatementCache::get_cache_state() const
    {
        CacheState cache_state;

        // Get session history cache contents only
        cache_state.session_history = _session_history.get_all_entries();

        cache_state.in_error = _in_error;

        return cache_state;
    }

    void
    StatementCache::sync_transaction(char xact_status)
    {
        // status is either: I - idle, T - in transaction, E - error
        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG3, "Syncing transaction: status: {}, in_error: {}", xact_status, _in_error);

        // finished processing statements, if we are not in a xact at this point
        // then either we saw a commit earlier and are done, or were in a
        // simple query which implicitly commits at the end
        if (xact_status == 'I') {
            // handle implicit rollback or commit
            if (_in_error) {
                // clear transaction history, rollback anything not committed
                _rollback_transaction();
            } else {
                // merge transaction history into session history
                _commit_transaction();
            }
        }

        if (xact_status != 'E') {
            _in_error = false;
        } else {
            _in_error = true;
        }

        _statement_history.clear();
    }

    void
    StatementCache::commit_statement(QueryStmtPtr stmt, int completed, uint64_t session_id, bool success)
    {
        // these statements have successfully completed, but not yet committed
        // they could still be rolled back

        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG3, "Committing statement: stmt_type: {}, completed: {}, children: {}, in_error: {}, success: {}",
                    (uint8_t)stmt->type, completed, stmt->children.size(), _in_error, success);

        if (!success) {
            _in_error = true;
        } else {
            // update replay idx for this session; marks previous history as replayed
            set_session_replay_idx(session_id, stmt->replay_id);
        }

        // if this is not a simple query statement process directly; should be only 1
        if (stmt->type != QueryStmt::Type::SIMPLE_QUERY) {
            if (completed == 1) {
                // process this single statement
                _commit_single_stmt(stmt);
            } else {
                DCHECK_EQ(completed, 0) <<
                       "Completed statements for non-simple query should be either 0 or 1";
                // no completed statements, error
                _in_error = true;
            }
            return;
        }

        // this is a simple query, we need to process each child statement
        if (stmt->children.size() == 0) {
            // no children, nothing to do (empty query)
            return;
        }

        DCHECK_GT(stmt->children.size(), 0);
        DCHECK_LE(completed, stmt->children.size());
        for (int i = 0; i < completed; i++) {
            auto qs = stmt->children[i];
            // commit any set_config function calls SELECT stmts
            for (auto &set_config_stmt : qs->set_config_calls) {
                _commit_single_stmt(set_config_stmt);
            }
            _commit_single_stmt(qs);
        }

        if (completed != stmt->children.size()) {
            // not all statements are completed
            _in_error = true;
        }

        // simple query removes unamed prepared statement from cache
        _transaction_history._clear_by_type(QueryStmt::Type::PREPARE, {}, false);
    }

    void
    StatementCache::_commit_single_stmt(QueryStmtPtr entry)
    {
        // these statements have successfully completed, but not yet committed
        // they could still be rolled back

        switch (entry->type) {
            case QueryStmt::Type::PREPARE:
                // named prepared statements don't get rolled back
                // technically, an unnamed prepared statement is valid until the next
                // unnamed prepare / PARSE message or simple query
                _transaction_history._clear_by_type(QueryStmt::Type::PREPARE, entry->name, false);
                break;
            case QueryStmt::Type::DEALLOCATE:
                // remove prepared statements
                _transaction_history._clear_by_type(QueryStmt::Type::PREPARE, entry->name, false);
                break;
            case QueryStmt::Type::DEALLOCATE_ALL:
                // clear all prepared statements
                _transaction_history._clear_by_type(QueryStmt::Type::PREPARE, {}, true);
                break;
            case QueryStmt::Type::RESET:
                // remove variable(s)
                _transaction_history._clear_by_type(QueryStmt::Type::SET, entry->name, false);
                break;
            case QueryStmt::Type::RESET_ALL:
                _transaction_history._clear_by_type(QueryStmt::Type::SET, {}, true);
                break;
            case QueryStmt::Type::CLOSE:
                // remove cursor/portal
                _transaction_history._clear_by_type(QueryStmt::Type::DECLARE_HOLD, entry->name, false);
                _transaction_history._clear_by_type(QueryStmt::Type::FETCH, entry->name, false);
                break;
            case QueryStmt::Type::CLOSE_ALL:
                // remove all cursors/portals
                _transaction_history._clear_by_type(QueryStmt::Type::DECLARE_HOLD, {}, true);
                _transaction_history._clear_by_type(QueryStmt::Type::FETCH, {}, true);
                break;
            case QueryStmt::Type::UNLISTEN:
                // remove listen(s)
                _transaction_history._clear_by_type(QueryStmt::Type::LISTEN, entry->name, false);
                break;
            case QueryStmt::Type::UNLISTEN_ALL:
                _transaction_history._clear_by_type(QueryStmt::Type::LISTEN, {}, true);
                break;
            case QueryStmt::Type::BEGIN:
                break;
            case QueryStmt::Type::SYNC:
                // removes unnamed prepared statements
                _transaction_history._clear_by_type(QueryStmt::Type::PREPARE, {}, false);
                // add to history and commit the transaction
                _transaction_history.add(entry);
                _commit_transaction();
                return;
            case QueryStmt::Type::COMMIT:
                // preserve commit ordering in transaction history
                _transaction_history.add(entry);
                // merge transaction history into session history
                _commit_transaction();
                return;
            case QueryStmt::Type::ROLLBACK:
                _rollback_transaction();
                return;
            case QueryStmt::Type::RELEASE_SAVEPOINT:
                _release_savepoint(entry->name);
                // skip adding this to transaction history
                return;
            case QueryStmt::Type::ROLLBACK_TO_SAVEPOINT:
                _rollback_to_savepoint(entry->name);
                // skip adding this to transaction history
                return;
            case QueryStmt::Type::DISCARD_ALL:
                // clear session history
                _transaction_history.clear();
                break;
            case QueryStmt::Type::ANONYMOUS:
                // anonymous statements do not affect cache
                return;
            default:
                break;
        }

        // if we got here we can add this statement to the transaction history
        _transaction_history.add(entry);
    }

    void
    StatementCache::_rollback_transaction()
    {
        bool session_history_modified = false;

        // go through and move any prepared / deallocates to the session history
        for (auto it = _transaction_history._history.begin(); it != _transaction_history._history.end(); ++it) {
            QueryStmtPtr entry = it->second;
            if (entry->type == QueryStmt::Type::PREPARE ||
                entry->type == QueryStmt::Type::DEALLOCATE) {
                // add to session history only if named
                if (!entry->name.empty()) {
                    _session_history.add(entry);
                    session_history_modified = true;
                }
            }
            if (entry->type == QueryStmt::Type::DEALLOCATE_ALL) {
                // clear all prepared statements from session history
                _session_history.add(entry);
                session_history_modified = true;
            }
        }
        _transaction_history.clear();

        if (session_history_modified) {
            // compact session history if modified
            _session_history.compact(_get_min_replay_idx());
        }
    }

    void
    StatementCache::_commit_transaction()
    {
        // map for declare hold statements
        std::map<std::string, QueryStmtPtr> declare_hold_map;

        // merge transaction history to session history
        for (auto it = _transaction_history._history.begin(); it != _transaction_history._history.end(); ++it) {
            switch (it->second->type) {
                // ignore certain transaction level statements; don't merge
                case QueryStmt::Type::SET_LOCAL:
                case QueryStmt::Type::SAVEPOINT:
                case QueryStmt::Type::ANONYMOUS:
                case QueryStmt::Type::DECLARE: // only declare hold is across transactions
                    continue;

                case QueryStmt::Type::BEGIN:
                case QueryStmt::Type::COMMIT:
                case QueryStmt::Type::SYNC:
                    break;

                case QueryStmt::Type::PREPARE:
                    // unnamed prepares do not survive transaction
                    if (it->second->name.empty()) {
                        continue;
                    }
                    break;

                    // shouldn't see these, should be handled in commit_statement()
                case QueryStmt::Type::ROLLBACK:
                case QueryStmt::Type::RELEASE_SAVEPOINT:
                case QueryStmt::Type::ROLLBACK_TO_SAVEPOINT:
                    DCHECK(false) << "Unexpected transaction control statement in commit_transaction()";
                    continue;

                // portal commands are only valid within a transaction unless they are DECLARE WITH HOLD
                // DECLARE WITH HOLD can only support SELECT or VALUES queries that are purely read-only
                case QueryStmt::Type::DECLARE_HOLD:
                    // keep track of DECLARE HOLD statements
                    declare_hold_map[it->second->name] = it->second;
                    break;

                case QueryStmt::Type::CLOSE: {
                    // if this close is for a DECLARE HOLD, remove from map
                    int rc = declare_hold_map.erase(it->second->name);
                    if (rc == 0) {
                        continue; // not a DECLARE HOLD, skip
                    }
                    break;
                }

                case QueryStmt::Type::FETCH:
                    if (!declare_hold_map.contains(it->second->name)) {
                        continue; // not a DECLARE HOLD, skip
                    }
                    break;

                default:
                    // compacting state from other removal statements is handled in compact()
                    break;
            }
            _session_history.add(it->second);
        }

        _transaction_history.clear();
        _session_history.compact(_get_min_replay_idx());
    }
} // namespace pg_proxy
} // namespace springtail
