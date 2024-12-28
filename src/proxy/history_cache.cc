#include <proxy/history_cache.hh>

#include <proxy/logging.hh>

namespace springtail {
namespace pg_proxy {

    void
    HistoryCache::add(QueryStmtPtr entry, bool prune)
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

        // prune existing history based on new entry
        // e.g., if we see a close, remove any previous matching declare
        if (prune) {
            _prune(entry, entry->name.size() == 0);
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

    void
    HistoryCache::_clear_by_type(QueryStmt::Type type, const std::string &name, bool all)
    {
        // iterate through history pruning as we go, it is inefficient but we expect
        // the history to be small...
        for (auto it = _history.begin(); it != _history.end(); ) {
            if (it->second->type == type && (all || it->second->name == name)) {
                it = _history.erase(it);
            } else {
                ++it;
            }
        }
    }

    void
    HistoryCache::_prune(QueryStmtPtr entry, bool all)
    {
        std::string empty_string = {};

        switch (entry->type) {
            // ignore portal statements (for pruning)
            case QueryStmt::FETCH:
            case QueryStmt::DECLARE_HOLD:
            case QueryStmt::DECLARE:
                return;
            // shouldn't see these:
            case QueryStmt::PREPARE:
            case QueryStmt::BEGIN:
            case QueryStmt::COMMIT:
            case QueryStmt::ROLLBACK:
            case QueryStmt::RELEASE_SAVEPOINT:
            case QueryStmt::ROLLBACK_TO_SAVEPOINT:
            case QueryStmt::SET_LOCAL:
            case QueryStmt::ANONYMOUS:
                assert(0);
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
                _clear_by_type(QueryStmt::DECLARE, empty_string, true);
                _clear_by_type(QueryStmt::FETCH, empty_string, true);
                break;
            case QueryStmt::CLOSE:
                _clear_by_type(QueryStmt::DECLARE, entry->name, false);
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
    StatementCache::_lookup(const std::string &name, QueryStmt::Type type)
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

        // then check the caches
        if (type == QueryStmt::PREPARE) {
            qs = _prepared_cache.get(name);
            return {qs, false};
        } else {
            qs = _session_history.lookup(name, type);
            return {qs, false};
        }

        return {nullptr, false};
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

    void
    StatementCache::sync_transaction(char xact_status)
    {
        // status is either: I - idle, T - in transaction, E - error
        PROXY_DEBUG(LOG_LEVEL_DEBUG3, "Syncing transaction: status: {}, in_error: {}", xact_status, _in_error);

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
        PROXY_DEBUG(LOG_LEVEL_DEBUG3, "Committing statement: stmt_type: {}, completed: {}, children: {}, in_error: {}, success: {}",
                    (uint8_t)stmt->type, completed, stmt->children.size(), _in_error, success);

        if (!success) {
            _in_error = true;
            // not sure if this is always true
            assert(completed == 0);
        }

        // if this is not a simple query statement process directly; should be only 1
        if (stmt->type != QueryStmt::Type::SIMPLE_QUERY) {
            if (completed == 1) {
                // process this single statement
                _commit_single_stmt(stmt);
            } else {
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

        assert (stmt->children.size() > 0);
        assert (completed <= stmt->children.size());
        for (int i = 0; i < completed; i++) {
            _commit_single_stmt(stmt->children[i]);
        }

        if (completed != stmt->children.size()) {
            // not all statements are completed
            _in_error = true;
        }

        // simple query removes unamed prepared statement from cache
        _prepared_cache.evict("");
    }

    void
    StatementCache::_commit_single_stmt(QueryStmtPtr entry)
    {
        switch (entry->type) {
            case QueryStmt::PREPARE:
                // prepared statements don't get rolled back

                // technically, an unnamed prepared statement is valid until the next
                // unnamed prepare / PARSE message or simple query
                // we clear unamed prepared statements at the end of a simple query

                // add to session level prepared stmt cache
                _prepared_cache.insert(entry->name, entry);

                // no need to add to transaction history,
                // as soon as they succeed they are visible; rollbacks don't affect them
                // they get replayed on demand if not already applied within the session
                return;
            case QueryStmt::DEALLOCATE:
                // remove from session level prepared stmt cache
                _prepared_cache.evict(entry->name);
                break;
            case QueryStmt::DEALLOCATE_ALL:
                // clear all prepared statements
                _prepared_cache.clear();
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
                case QueryStmt::CLOSE:
                case QueryStmt::CLOSE_ALL:
                case QueryStmt::FETCH:
                    break;
                case QueryStmt::DISCARD_ALL:
                    _prepared_cache.clear();
                    break;
                default:
                    break;
            }

            _session_history.add(it->second, true);
        }
        _transaction_history.clear();
    }
} // namespace pg_proxy
} // namespace springtail
