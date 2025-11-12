#include "merge_table.hh"

// ChangeSet::Iterator::TxnIteratorPair implementation
bool
ChangeSet::Iterator::TxnIteratorPair::operator<(const TxnIteratorPair& other) const
{
    // Priority queue is max-heap, so we reverse comparison for min-heap behavior
    // Compare the current row keys to maintain sorted order
    // Since we only add valid (non-end) iterators to the heap, we can directly compare keys
    return (*txn_it).key() > (*other.txn_it).key();
}

// ChangeSet::Iterator implementation
ChangeSet::Iterator::Iterator() : _changes(nullptr), _is_end(true), _current_rows_valid(false) {}

ChangeSet::Iterator::Iterator(const std::vector<Txn>& changes, bool is_end)
    : _changes(&changes), _is_end(is_end), _current_rows_valid(false)
{
    if (!_is_end) {
        initialize_heap();
    }
}

ChangeSet::Iterator::Iterator(const std::vector<Txn>& changes,
                              const TuplePtr& search_key,
                              bool lower_bound)
    : _changes(&changes), _is_end(false), _current_rows_valid(false)
{
    initialize_heap_with_bound(search_key, lower_bound);
}

ChangeSet::Iterator&
ChangeSet::Iterator::operator++()
{
    if (_is_end || _heap.empty()) {
        _is_end = true;
        return *this;
    }

    // Invalidate current matching rows cache
    _current_rows_valid = false;
    advance_heap();
    return *this;
}

ChangeSet::Iterator
ChangeSet::Iterator::operator++(int)
{
    Iterator tmp = *this;
    ++(*this);
    return tmp;
}

const std::vector<Extent::Row>&
ChangeSet::Iterator::operator*() const
{
    if (_is_end) {
        throw std::runtime_error("Dereferencing end iterator");
    }

    return get_matching_rows();
}

const std::vector<Extent::Row>*
ChangeSet::Iterator::operator->() const
{
    if (_is_end) {
        throw std::runtime_error("Dereferencing end iterator");
    }

    return &get_matching_rows();
}

bool
ChangeSet::Iterator::operator==(const Iterator& other) const
{
    if (_is_end && other._is_end) {
        return true;
    }
    if (_is_end != other._is_end) {
        return false;
    }

    // Compare heap states - simplified comparison
    return _heap.empty() == other._heap.empty();
}

bool
ChangeSet::Iterator::operator!=(const Iterator& other) const
{
    return !(*this == other);
}

void
ChangeSet::Iterator::initialize_heap()
{
    for (size_t xid_idx = 0; xid_idx < _changes->size(); ++xid_idx) {
        const auto& txn = (*_changes)[xid_idx];
        auto txn_it = txn.begin();
        
        // Only add to heap if the txn has rows
        if (txn_it != txn.end()) {
            TxnIteratorPair pair;
            pair.txn_it = txn_it;
            pair.xid_index = xid_idx;
            _heap.push(pair);
        }
    }

    if (_heap.empty()) {
        _is_end = true;
    }
}

void
ChangeSet::Iterator::initialize_heap_with_bound(const TuplePtr& search_key, bool lower_bound)
{
    for (size_t xid_idx = 0; xid_idx < _changes->size(); ++xid_idx) {
        const auto& txn = (*_changes)[xid_idx];
        
        // Use the Txn's bound methods to find the appropriate starting position
        Txn::Iterator txn_it;
        if (lower_bound) {
            txn_it = txn.lower_bound(search_key);
        } else {
            txn_it = txn.upper_bound(search_key);
        }
        
        // Only add to heap if we found a valid position
        if (txn_it != txn.end()) {
            TxnIteratorPair pair;
            pair.txn_it = txn_it;
            pair.xid_index = xid_idx;
            _heap.push(pair);
        }
    }

    if (_heap.empty()) {
        _is_end = true;
    }
}

void
ChangeSet::Iterator::advance_heap()
{
    // Build matching rows if not already done, which will advance the heap
    if (!_current_rows_valid) {
        build_current_matching_rows();
    } else {
        // If we already have current rows, we need to skip to the next key group
        // The heap should already be positioned at the next different key or empty
        if (_heap.empty()) {
            _is_end = true;
        }
    }
}

// Build current matching rows by consuming from heap
void
ChangeSet::Iterator::build_current_matching_rows() const
{
    _current_matching_rows.clear();
    
    if (_heap.empty()) {
        _current_rows_valid = true;
        return;
    }
    
    // Get the target key from the top of the heap
    auto target_key = (*_heap.top().txn_it).key();
    
    // Collect all rows with matching keys by pulling from heap
    while (!_heap.empty()) {
        auto current_pair = _heap.top();
        auto current_row = *current_pair.txn_it;
        auto current_row_key = current_row.key();
        
        // Check if this row's key matches our target key
        if (!current_row_key.equal_strict(target_key)) {
            break; // No more matching keys
        }
        
        // Add this row to our collection (most recent transaction first due to heap ordering)
        _current_matching_rows.push_back(current_row);
        
        // Remove from heap and advance
        auto mutable_heap = const_cast<std::priority_queue<TxnIteratorPair>*>(&_heap);
        mutable_heap->pop();
        
        // Advance this transaction's iterator
        ++current_pair.txn_it;
        
        // Get the end iterator for this transaction
        const auto& txn = (*_changes)[current_pair.xid_index];
        
        // If we still have valid data, add back to heap
        if (current_pair.txn_it != txn.end()) {
            mutable_heap->push(current_pair);
        }
    }
    
    _current_rows_valid = true;
}

// Get all rows with matching key (used internally by operators)
const std::vector<Extent::Row>&
ChangeSet::Iterator::get_matching_rows() const
{
    if (!_current_rows_valid) {
        build_current_matching_rows();
    }
    return _current_matching_rows;
}

// ChangeSet public interface implementation
ChangeSet::Iterator
ChangeSet::begin()
{
    return Iterator(_changes, false);
}

ChangeSet::Iterator
ChangeSet::end()
{
    return Iterator(_changes, true);
}

ChangeSet::Iterator
ChangeSet::lower_bound(const TuplePtr& search_key)
{
    return Iterator(_changes, search_key, true);
}

ChangeSet::Iterator
ChangeSet::upper_bound(const TuplePtr& search_key)
{
    return Iterator(_changes, search_key, false);
}

ChangeSet::Iterator
ChangeSet::inverse_lower_bound(const TuplePtr& search_key)
{
    // inverse_lower_bound finds the greatest key that is less than search_key
    // Based on btree.cc implementation: upper_bound(search_key) then decrement
    Iterator it = upper_bound(search_key);

    if (it == begin()) {
        return end();  // No element less than search_key
    }

    return --it;
}

// ChangeSet::Txn::Iterator implementation
ChangeSet::Txn::Iterator::Iterator() : _extents(nullptr), _is_end(true) {}

ChangeSet::Txn::Iterator::Iterator(const std::vector<ExtentPtr>& extents, bool is_end)
    : _extents(&extents), _is_end(is_end)
{
    if (!_is_end && !_extents->empty()) {
        _extent_it = _extents->begin();
        _extent_end = _extents->end();
        _row_it = (*_extent_it)->begin();
        advance_to_next_valid_row();
    } else {
        _extent_it = _extents->end();
        _extent_end = _extents->end();
    }
}

ChangeSet::Txn::Iterator::Iterator(const std::vector<ExtentPtr>& extents,
                                   const TuplePtr& search_key,
                                   bool lower_bound)
    : _extents(&extents), _is_end(false)
{
    if (_extents->empty()) {
        _is_end = true;
        _extent_it = _extents->end();
        _extent_end = _extents->end();
        return;
    }

    // Find the first extent that could contain rows >= search_key
    _extent_it = std::lower_bound(
        _extents->begin(), _extents->end(), *search_key,
        [](const ExtentPtr& extent, const Tuple& key) {
            auto last_row = extent->back();
            return FieldTuple(extent->get_schema()->get_sort_fields(), &last_row).less_than(key);
        });
    _extent_end = _extents->end();

    if (_extent_it != _extent_end) {
        // Find the appropriate row within this extent
        if (lower_bound) {
            _row_it = std::ranges::lower_bound(
                **_extent_it, *search_key,
                [](const Tuple& lhs, const Tuple& rhs) { return lhs.less_than(rhs); },
                [this](const Extent::Row& row) {
                    return FieldTuple((*_extent_it)->get_schema()->get_sort_fields(), &row);
                });
        } else {
            _row_it = std::ranges::upper_bound(
                **_extent_it, *search_key,
                [](const Tuple& lhs, const Tuple& rhs) { return lhs.less_than(rhs); },
                [this](const Extent::Row& row) {
                    return FieldTuple((*_extent_it)->get_schema()->get_sort_fields(), &row);
                });
        }

        advance_to_next_valid_row();
    } else {
        _is_end = true;
    }
}

ChangeSet::Txn::Iterator&
ChangeSet::Txn::Iterator::operator++()
{
    if (_is_end) {
        return *this;
    }

    ++_row_it;
    advance_to_next_valid_row();
    return *this;
}

ChangeSet::Txn::Iterator
ChangeSet::Txn::Iterator::operator++(int)
{
    Iterator tmp = *this;
    ++(*this);
    return tmp;
}

ChangeSet::Txn::Iterator&
ChangeSet::Txn::Iterator::operator--()
{
    if (_is_end) {
        // Move from end to last valid position
        if (_extents->empty()) {
            return *this;
        }
        
        _extent_it = _extents->end();
        --_extent_it;
        _extent_end = _extents->end();
        _row_it = (*_extent_it)->end();
        --_row_it;
        _is_end = false;
        return *this;
    }
    
    retreat_to_prev_valid_row();
    return *this;
}

ChangeSet::Txn::Iterator
ChangeSet::Txn::Iterator::operator--(int)
{
    Iterator tmp = *this;
    --(*this);
    return tmp;
}

Extent::Row
ChangeSet::Txn::Iterator::operator*() const
{
    if (_is_end) {
        throw std::runtime_error("Dereferencing end iterator");
    }

    return *_row_it;
}

const Extent::Row*
ChangeSet::Txn::Iterator::operator->() const
{
    if (_is_end) {
        throw std::runtime_error("Dereferencing end iterator");
    }

    return &(*_row_it);
}

bool
ChangeSet::Txn::Iterator::operator==(const Iterator& other) const
{
    if (_is_end && other._is_end) {
        return true;
    }
    if (_is_end != other._is_end) {
        return false;
    }

    return _extent_it == other._extent_it && _row_it == other._row_it;
}

bool
ChangeSet::Txn::Iterator::operator!=(const Iterator& other) const
{
    return !(*this == other);
}

void
ChangeSet::Txn::Iterator::advance_to_next_valid_row()
{
    while (_extent_it != _extent_end) {
        // If current extent has more rows, we're done
        if (_row_it != (*_extent_it)->end()) {
            return;
        }

        // Move to next extent
        ++_extent_it;

        // If we have a valid extent, start at its beginning
        if (_extent_it != _extent_end) {
            _row_it = (*_extent_it)->begin();
        }
    }

    // If we get here, we've exhausted all extents
    _is_end = true;
}

void
ChangeSet::Txn::Iterator::retreat_to_prev_valid_row()
{
    // If we're at the beginning of current extent, move to previous extent
    if (_row_it == (*_extent_it)->begin()) {
        // If we're at the first extent, we'll become begin()
        if (_extent_it == _extents->begin()) {
            // Iterator is now at position before begin() - this is undefined behavior
            // but commonly done in STL implementations
            _is_end = true;  // Mark as invalid
            return;
        }
        
        // Move to previous extent
        --_extent_it;
        _row_it = (*_extent_it)->end();
        --_row_it;  // Move to last row of previous extent
    } else {
        // Just move to previous row in current extent
        --_row_it;
    }
}

// ChangeSet::Txn public interface implementation
ChangeSet::Txn::Iterator
ChangeSet::Txn::begin()
{
    return Iterator(_extents, false);
}

ChangeSet::Txn::Iterator
ChangeSet::Txn::end()
{
    return Iterator(_extents, true);
}

ChangeSet::Txn::Iterator
ChangeSet::Txn::lower_bound(const TuplePtr& search_key)
{
    return Iterator(_extents, search_key, true);
}

ChangeSet::Txn::Iterator
ChangeSet::Txn::upper_bound(const TuplePtr& search_key)
{
    return Iterator(_extents, search_key, false);
}

ChangeSet::Txn::Iterator
ChangeSet::Txn::inverse_lower_bound(const TuplePtr& search_key)
{
    // inverse_lower_bound finds the greatest key that is less than search_key
    Iterator it = upper_bound(search_key);

    if (it == begin()) {
        return end();  // No element less than search_key
    }

    return --it;
}

// MergeTable::Iterator implementation
MergeTable::Iterator::Iterator() 
    : _is_end(true), _row_cached(false) {
}

MergeTable::Iterator::Iterator(Table::Iterator table_it, Table::Iterator table_end, 
                               ChangeSet::Iterator changes_it, ChangeSet::Iterator changes_end)
    : _table_it(table_it), _table_end(table_end), 
      _changes_it(changes_it), _changes_end(changes_end),
      _is_end(false), _row_cached(false) {
    find_next_valid_position();
}

MergeTable::Iterator&
MergeTable::Iterator::operator++()
{
    if (_is_end) {
        return *this;
    }
    
    _row_cached = false;  // Invalidate cached row
    
    // Advance based on which iterator(s) we used for the current position
    if (_table_it != _table_end && _changes_it != _changes_end) {
        const auto& change_rows = *_changes_it;
        int cmp = change_rows.empty() ? -1 : compare_keys(*_table_it, change_rows[0]);
        if (cmp <= 0) {  // Used table row (either alone or with changes)
            ++_table_it;
        }
        if (cmp >= 0) {  // Used changes row
            ++_changes_it;
        }
    } else if (_table_it != _table_end) {
        ++_table_it;
    } else if (_changes_it != _changes_end) {
        ++_changes_it;
    }
    
    find_next_valid_position();
    return *this;
}

MergeTable::Iterator
MergeTable::Iterator::operator++(int)
{
    Iterator tmp = *this;
    ++(*this);
    return tmp;
}

Extent::Row
MergeTable::Iterator::operator*() const
{
    if (_is_end) {
        throw std::runtime_error("Dereferencing end iterator");
    }
    
    if (!_row_cached) {
        merge_current_row();
        _row_cached = true;
    }
    
    return _merged_row;
}

const Extent::Row*
MergeTable::Iterator::operator->() const
{
    if (_is_end) {
        throw std::runtime_error("Dereferencing end iterator");
    }
    
    if (!_row_cached) {
        merge_current_row();
        _row_cached = true;
    }
    
    return &_merged_row;
}

bool
MergeTable::Iterator::operator==(const Iterator& other) const
{
    if (_is_end && other._is_end) {
        return true;
    }
    if (_is_end != other._is_end) {
        return false;
    }
    
    return _table_it == other._table_it && _changes_it == other._changes_it;
}

bool
MergeTable::Iterator::operator!=(const Iterator& other) const
{
    return !(*this == other);
}

void
MergeTable::Iterator::find_next_valid_position()
{
    // Check if we've reached the end of both iterators
    if (_table_it == _table_end && _changes_it == _changes_end) {
        _is_end = true;
        return;
    }

    // Assign the current row
    if (_table_it == _table_end) {
        _is_merge_row = true;
        _current_row = *_changes_it;

        // iterate through the changes until we find the next key or the end
        ++_changes_it;
        while (_changes_it != _changes_end && _get_key(_current_row) == _get_key(_changes_it->key())) {
            _current_row = *_changes_it;
            ++_changes_it;
        }
    } else if (_changes_it == _changes_end) {
        _is_merge_row = false;

        ++_table_it;
        _current_row = *_table_it;
    } else {
        // if we are on a table row, we need to move to the next 
        if (!_is_merge_row) {
            ++_table_it;
        }
    }
    
    _is_end = false;
    _row_cached = false;  // Invalidate cached row when position changes
}

void
MergeTable::Iterator::merge_current_row() const
{
    if (_table_it == _table_end && _changes_it == _changes_end) {
        throw std::runtime_error("Cannot merge at end position");
    }
    
    if (_table_it == _table_end) {
        // Only changes available - use the first (most recent) row from the vector
        const auto& change_rows = *_changes_it;
        if (change_rows.empty()) {
            throw std::runtime_error("Empty change row vector");
        }
        _merged_row = change_rows[0];  // Most recent row
    } else if (_changes_it == _changes_end) {
        // Only table available
        _merged_row = *_table_it;
    } else {
        // Both available - compare keys
        const auto& change_rows = *_changes_it;
        if (change_rows.empty()) {
            _merged_row = *_table_it;
        } else {
            int cmp = compare_keys(*_table_it, change_rows[0]);
            
            if (cmp < 0) {
                // Table row comes first
                _merged_row = *_table_it;
            } else if (cmp > 0) {
                // Changes row comes first - use most recent
                _merged_row = change_rows[0];
            } else {
                // Same key - changes override table, use most recent change
                _merged_row = change_rows[0];
            }
        }
    }
}

int
MergeTable::Iterator::compare_keys(const Extent::Row& table_row, const Extent::Row& changes_row) const
{
    auto table_key = table_row.key();
    auto changes_key = changes_row.key();
    
    if (table_key.less_than(changes_key)) {
        return -1;
    } else if (changes_key.less_than(table_key)) {
        return 1;
    } else {
        return 0;  // Keys are equal
    }
}

// MergeTable implementation
MergeTable::MergeTable(TablePtr table, ChangeSetPtr changes, uint64_t index_id)
    : _table(table), _changes(changes), _index(index_id) {
}

MergeTable::Iterator
MergeTable::begin()
{
    return Iterator(_table->begin(), _table->end(), _changes->begin(), _changes->end());
}

MergeTable::Iterator
MergeTable::end()
{
    return Iterator();
}

MergeTable::Iterator
MergeTable::lower_bound(TuplePtr search_key)
{
    return Iterator(_table->lower_bound(search_key), _table->end(), 
                    _changes->lower_bound(search_key), _changes->end(),
                    search_key, true);
}

MergeTable::Iterator
MergeTable::upper_bound(TuplePtr search_key)
{
    return Iterator(_table->upper_bound(search_key), _table->end(),
                    _changes->upper_bound(search_key), _changes->end(),
                    search_key, false);
}

MergeTable::Iterator
MergeTable::inverse_lower_bound(TuplePtr search_key)
{
    // inverse_lower_bound finds the greatest key that is less than search_key
    // This is complex for merge iterators since we need to consider the merged view
    // For now, implement a simplified version that may be less efficient
    
    Iterator it = begin();
    Iterator last_valid = end();
    
    // Scan forward to find the last element with key < search_key
    while (it != end()) {
        auto current_key = (*it).key();
        if (current_key.less_than(*search_key)) {
            last_valid = it;
            ++it;
        } else {
            break;  // Found first element >= search_key
        }
    }
    
    return last_valid;
}
