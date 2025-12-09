#include <sys_tbl_mgr/merge_table.hh>

using namespace springtail;

// MergeTable constructor
MergeTable::MergeTable(TablePtr tbl, ChangeSet changeset) :
    _table(std::move(tbl)), 
    _mutations{std::move(changeset), 
        [this](const Extent::Row& a, const Extent::Row& b) { 
            // compare row keys using the write cache schema info
            return FieldTuple(_wc_schema_info->_wc_key_fields, &a).less_than(FieldTuple(_wc_schema_info->_wc_key_fields, &b));
        }
    }
{
    auto schema = _table->extent_schema();
    _key_fields = schema->get_fields(schema->get_sort_keys());

    if (_key_fields->empty() || !_table->get_extent_schema_without_row_id().has_value()) {
        _mutations.clear();
    }

    if (!_mutations.empty()) {
        _wc_schema_info = WcSchemaInfo(*_table->get_extent_schema_without_row_id(), _table->get_extension_callback());
    }
}

// Iterator constructor
MergeTable::Iterator::Iterator(MergeTable* merge_table,
        Table::Iterator table_iter,
        Table::Iterator table_end,
        MutationIterator mutation_iter,
        MutationIterator mutation_end)

    : _merge_table(merge_table),
      _table_iter(std::move(table_iter)),
      _table_end(std::move(table_end)),
      _mutation_iter(std::move(mutation_iter)),
      _mutation_end(std::move(mutation_end))
{}

// Helper to compare rows
bool MergeTable::Iterator::compare_rows(const Extent::Row& table_row, const Extent::Row& wc_row) const {
    return FieldTuple(_merge_table->_key_fields, &table_row)
        .less_than(FieldTuple(_merge_table->_wc_schema_info->_wc_key_fields, &wc_row));
}


// Iterator operators
MergeTable::Iterator::reference MergeTable::Iterator::operator*() {
    // If table iterator is at end, return from mutation iterator
    if (_table_iter == _table_end) {
        return *_mutation_iter;
    }
    // If mutation iterator is at end, return from table iterator
    if (_mutation_iter == _mutation_end) {
        return *_table_iter;
    }
    // Both iterators are valid, compare and return the smaller
    if (compare_rows(*_table_iter, *_mutation_iter)) {
        return *_table_iter;
    } else {
        return *_mutation_iter;
    }
}

MergeTable::Iterator& MergeTable::Iterator::operator++() {
    // TODO: implement mutation handling (insert/update/delete) during iteration

    // If table iterator is at end, advance mutation iterator
    if (_table_iter == _table_end) {
        ++_mutation_iter;
        return *this;
    }
    // If mutation iterator is at end, advance table iterator
    if (_mutation_iter == _mutation_end) {
        ++_table_iter;
        return *this;
    }
    // Both iterators are valid, advance the one with the smaller value
    if (compare_rows(*_table_iter, *_mutation_iter)) {
        ++_table_iter;
    } else {
        ++_mutation_iter;
    }
    return *this;
}

MergeTable::Iterator& MergeTable::Iterator::operator--() {
    // TODO: Implement backward iteration for 2-way merge.
    --_table_iter;
    return *this;
}

// MergeTable methods
MergeTable::Iterator MergeTable::lower_bound(TuplePtr search_key, uint64_t index_id, bool index_only) {
    // TODO: Properly implement lower_bound with merge from both table and sorted_merge
    return Iterator(this,
                    _table->lower_bound(search_key, index_id, index_only),
                    _table->end(index_id, index_only),
                    _mutations.begin(),
                    _mutations.end());
}

MergeTable::Iterator MergeTable::upper_bound(TuplePtr search_key, uint64_t index_id, bool index_only) {
    // TODO: Properly implement upper_bound with merge from both table and sorted_merge
    return Iterator(this,
                    _table->upper_bound(search_key, index_id, index_only),
                    _table->end(index_id, index_only),
                    _mutations.begin(),
                    _mutations.end());
}

MergeTable::Iterator MergeTable::inverse_lower_bound(TuplePtr search_key, uint64_t index_id, bool index_only) {
    // TODO: Properly implement inverse_lower_bound with merge from both table and sorted_merge.
    return Iterator(this,
                    _table->inverse_lower_bound(search_key, index_id, index_only),
                    _table->end(index_id, index_only),
                    _mutations.begin(),
                    _mutations.end());
}

MergeTable::Iterator MergeTable::begin(uint64_t index_id, bool index_only) {
    return Iterator(this,
                    _table->begin(index_id, index_only),
                    _table->end(index_id, index_only),
                    _mutations.begin(),
                    _mutations.end());
}

MergeTable::Iterator MergeTable::end(uint64_t index_id, bool index_only) {
    return Iterator(this,
                    _table->end(index_id, index_only),
                    _table->end(index_id, index_only),
                    _mutations.end(),
                    _mutations.end());
}
