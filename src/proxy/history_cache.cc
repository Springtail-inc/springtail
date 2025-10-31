#include <set>

#include <proxy/history_cache.hh>

namespace springtail {
namespace pg_proxy {

    void
    HistoryCache::add(QueryStmtPtr entry)
    {
        // convert FETCH to MOVE; rest of query remains the same
        if (entry->type == QueryStmt::FETCH) {
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

        _history[_current_idx++] = entry;
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
                case QueryStmt::SET: // set variable
                    statement_map[QueryStmt::SET][stmt->name] = {idx, stmt};
                    break;

                case QueryStmt::PREPARE: // add prepared statement
                    statement_map[QueryStmt::PREPARE][stmt->name] = {idx, stmt};
                    break;

                case QueryStmt::DECLARE_HOLD: // add cursor/portal
                    statement_map[QueryStmt::DECLARE_HOLD][stmt->name] = {idx, stmt};
                    break;

                case QueryStmt::FETCH: // cursor/portal use
                    // no need to track usage, just ensure it exists
                    if (statement_map[QueryStmt::DECLARE_HOLD].contains(stmt->name)) {
                        statement_map[QueryStmt::FETCH][stmt->name] = {idx, stmt};
                    }
                    break;

                case QueryStmt::LISTEN: // add listen
                    statement_map[QueryStmt::LISTEN][stmt->name] = {idx, stmt};
                    break;

                case QueryStmt::RESET: // remove variable(s)
                    if (stmt->name.empty()) { // RESET ALL
                        statement_map[QueryStmt::SET].clear();
                    } else {
                        statement_map[QueryStmt::SET].erase(stmt->name);
                    }
                    // keep the RESET statement to reset config variables
                    new_history[idx] = stmt;
                    break;

                case QueryStmt::DEALLOCATE_ALL: // remove all prepared statements
                    statement_map[QueryStmt::PREPARE].clear();
                    // keep the DEALLOCATE ALL statement
                    new_history[idx] = stmt;
                    break;

                case QueryStmt::DEALLOCATE: // remove prepared statement
                    if (replayed) {
                        statement_map[QueryStmt::PREPARE].erase(stmt->name);
                    } else {
                        // If there are new statements after replay_idx,
                        // we cannot be sure if this DEALLOCATE affects
                        // prepared statements before or after replay_idx.
                        // So we keep it to be safe.
                        new_history[idx] = stmt;
                    }
                    break;

                case QueryStmt::CLOSE:  // remove cursor/portal
                    if (replayed) {
                        statement_map[QueryStmt::DECLARE_HOLD].erase(stmt->name);
                        statement_map[QueryStmt::FETCH].erase(stmt->name);
                    } else {
                        // Similar logic as DEALLOCATE
                        new_history[idx] = stmt;
                    }
                    break;

                case QueryStmt::CLOSE_ALL: // remove all cursors/portals
                    if (replayed) {
                        statement_map[QueryStmt::DECLARE_HOLD].clear();
                    } else {
                        // safe to clear FETCH as the DECLARE_HOLD entries are still there
                        statement_map[QueryStmt::FETCH].clear();

                        // Similar logic as DEALLOCATE
                        new_history[idx] = stmt;
                    }
                    break;

                case QueryStmt::UNLISTEN: // remove listen(s)
                    // safe to clear LISTENs no errors if channel doesn't exist
                    if (stmt->name.empty()) { // UNLISTEN ALL
                        statement_map[QueryStmt::LISTEN].clear();
                    } else {
                        statement_map[QueryStmt::LISTEN].erase(stmt->name);
                    }
                    // keep the UNLISTEN statement
                    new_history[idx] = stmt;
                    break;

                case QueryStmt::DISCARD_ALL: // discard all
                    statement_map.clear();
                    // keep the DISCARD ALL statement
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

        return new_history;
    }

    void
    HistoryCache::_prune(QueryStmtPtr entry)
    {
        bool all = entry->name.empty();
        std::string empty_string = {};

        switch (entry->type) {
            // ignore portal statements (for pruning)
            case QueryStmt::FETCH:
            case QueryStmt::DECLARE_HOLD:
            case QueryStmt::PREPARE:
                return;
            // shouldn't see these:
            case QueryStmt::BEGIN:
            case QueryStmt::COMMIT:
            case QueryStmt::ROLLBACK:
            case QueryStmt::RELEASE_SAVEPOINT:
            case QueryStmt::ROLLBACK_TO_SAVEPOINT:
            case QueryStmt::SET_LOCAL:
            case QueryStmt::DECLARE:
            case QueryStmt::ANONYMOUS:
                DCHECK(false);
                return; // nothing to do

            // these override previous statements
            case QueryStmt::SET:
                _clear_by_type(QueryStmt::SET, entry->name, false);
                break;
            case QueryStmt::LISTEN:
                _clear_by_type(QueryStmt::LISTEN, entry->name, false);
                break;

            // these clear previous statements
            case QueryStmt::RESET:
                _clear_by_type(QueryStmt::SET, entry->name, all);
                break;
            case QueryStmt::DEALLOCATE:
                _clear_by_type(QueryStmt::PREPARE, entry->name, false);
                break;
            case QueryStmt::DEALLOCATE_ALL:
                _clear_by_type(QueryStmt::PREPARE, empty_string, true);
                break;
            case QueryStmt::CLOSE_ALL:
                _clear_by_type(QueryStmt::DECLARE_HOLD, empty_string, true);
                _clear_by_type(QueryStmt::FETCH, empty_string, true);
                break;
            case QueryStmt::CLOSE:
                _clear_by_type(QueryStmt::DECLARE_HOLD, entry->name, false);
                _clear_by_type(QueryStmt::FETCH, entry->name, false);
                break;
            case QueryStmt::UNLISTEN:
                _clear_by_type(QueryStmt::LISTEN, entry->name, all);
                break;
            case QueryStmt::DISCARD_ALL:
                _history.clear();
                break;
            default:
                break;
        }
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
            if (it->second->type == QueryStmt::DEALLOCATE || it->second->type == QueryStmt::DEALLOCATE_ALL) {
                // add to session history; however these are in reverse order
                rollback_list.push_front(it->second);
            }
            if (it->second->type == QueryStmt::SAVEPOINT && it->second->name == name) {
                // remove all statements after this savepoint (using reverse iterator)
                _transaction_history._history.erase(std::next(it).base(), _transaction_history._history.end());

                // add to session history from rollback list
                for (auto rit = rollback_list.begin(); rit != rollback_list.end(); ++rit) {
                    _session_history.add(*rit);
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
            if (it->second->type == QueryStmt::SAVEPOINT && it->second->name == name) {
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
    StatementCache::commit_statement(QueryStmtPtr stmt, int completed, bool success)
    {
        // these statements have successfully completed, but not yet committed
        // they could still be rolled back

        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG3, "Committing statement: stmt_type: {}, completed: {}, children: {}, in_error: {}, success: {}",
                    (uint8_t)stmt->type, completed, stmt->children.size(), _in_error, success);

        if (!success) {
            _in_error = true;
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
            _commit_single_stmt(stmt->children[i]);
        }

        if (completed != stmt->children.size()) {
            // not all statements are completed
            _in_error = true;
        }

        // simple query removes unamed prepared statement from cache
        _session_history._clear_by_type(QueryStmt::PREPARE, {}, false);
    }

    void
    StatementCache::_commit_single_stmt(QueryStmtPtr entry)
    {
        // these statements have successfully completed, but not yet committed
        // they could still be rolled back

        switch (entry->type) {
            case QueryStmt::PREPARE:
                // prepared statements don't get rolled back

                // technically, an unnamed prepared statement is valid until the next
                // unnamed prepare / PARSE message or simple query
                // we clear unamed prepared statements at the end of a simple query

                // add to session level prepared stmt cache
                _session_history.add(entry);

                // no need to add to transaction history,
                // as soon as they succeed they are visible; rollbacks don't affect them
                return;
            case QueryStmt::DEALLOCATE:
                // remove from session level prepared stmt cache
                _transaction_history._clear_by_type(QueryStmt::PREPARE, entry->name, false);
                break;
            case QueryStmt::DEALLOCATE_ALL:
                // clear all prepared statements
                _transaction_history._clear_by_type(QueryStmt::PREPARE, {}, true);
                break;
            case QueryStmt::BEGIN:
                // safe to ignore
                return;
            case QueryStmt::SYNC:
                // ignore for now, will be handled in sync_transaction()
                return;
            case QueryStmt::COMMIT:
                // merge transaction history into session history
                _commit_transaction();
                return;
            case QueryStmt::ROLLBACK:
                // clear transaction history, rollback anything not committed
                _rollback_transaction();
                return;
            case QueryStmt::RELEASE_SAVEPOINT:
                _release_savepoint(entry->name);
                // skip adding this to transaction history
                return;
            case QueryStmt::ROLLBACK_TO_SAVEPOINT:
                _rollback_to_savepoint(entry->name);
                // skip adding this to transaction history
                return;
            case QueryStmt::DISCARD_ALL:
                // clear session history
                _transaction_history.clear();
                break;
            default:
                break;
        }

        // if we got here we can add this statement to the transaction history
        _transaction_history.add(entry);
    }

    void
    StatementCache::_rollback_transaction()
    {
        // go through and move any prepared / deallocates to the session history
        for (auto it = _transaction_history._history.begin(); it != _transaction_history._history.end(); ++it) {
            QueryStmtPtr entry = it->second;
            if (entry->type == QueryStmt::PREPARE ||
                entry->type == QueryStmt::DEALLOCATE) {
                // add to session history
                _session_history.add(entry);
            }
        }
        _transaction_history.clear();
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
                case QueryStmt::SET_LOCAL:
                case QueryStmt::BEGIN:
                case QueryStmt::COMMIT:
                case QueryStmt::PREPARE:
                case QueryStmt::SAVEPOINT:
                case QueryStmt::ANONYMOUS:
                case QueryStmt::DECLARE: // only declare hold is across transactions
                    continue;
                // shouldn't see these, should be handled in commit_statement()
                case QueryStmt::ROLLBACK:
                case QueryStmt::RELEASE_SAVEPOINT:
                case QueryStmt::ROLLBACK_TO_SAVEPOINT:
                    assert(0);
                // portal commands are only valid within a transaction unless they are DECLARE WITH HOLD
                // DECLARE WITH HOLD can only support SELECT or VALUES queries that are purely read-only
                case QueryStmt::DECLARE_HOLD:
                    // keep track of DECLARE HOLD statements
                    declare_hold_map[it->second->name] = it->second;
                    break;
                case QueryStmt::CLOSE: {
                    // if this close is for a DECLARE HOLD, remove from map
                    int rc = declare_hold_map.erase(it->second->name);
                    if (rc == 0) {
                        continue; // not a DECLARE HOLD, skip
                    }
                    break;
                }
                case QueryStmt::FETCH:
                    if (!declare_hold_map.contains(it->second->name)) {
                        continue; // not a DECLARE HOLD, skip
                    }
                    break;
                case QueryStmt::CLOSE_ALL:
                    break;
                case QueryStmt::DISCARD_ALL:
                case QueryStmt::DEALLOCATE:
                case QueryStmt::DEALLOCATE_ALL:
                    // clearing the state is handled in the add() with prune
                    break;
                default:
                    break;
            }

            _session_history.add(it->second);
            _session_history.compact(_get_replay_idx());
        }
        _transaction_history.clear();
    }
} // namespace pg_proxy
} // namespace springtail
