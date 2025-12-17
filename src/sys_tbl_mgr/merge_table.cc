#include <sys_tbl_mgr/merge_table.hh>
#include "common/constants.hh"

using namespace springtail;

// Implement WcRowToKey operator()
TuplePtr WcRowToKey::operator()(const Extent::Row& row) const {
    if (wc_schema_info && wc_schema_info->has_value()) {
        return std::make_shared<FieldTuple>((*wc_schema_info)->_wc_key_fields, &row);
    }
    return nullptr;
}

// Implement TuplePtrCompare operator()
bool TuplePtrCompare::operator()(const TuplePtr& a, const TuplePtr& b) const {
    if (a && b) {
        return a->less_than(*b);
    }
    return false;
}

// MergeTable constructor
MergeTable::MergeTable(TablePtr tbl, ChangeSet changeset) :
    _table(std::move(tbl)),
    _mutations{
        std::move(changeset),
        WcRowToKey(&_wc_schema_info),
        TuplePtrCompare()
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
        Table::Iterator table_begin,
        Table::Iterator table_end,
        MutationIterator mutation_iter,
        MutationIterator mutation_begin,
        MutationIterator mutation_end)

    : _merge_table(merge_table),
      _table_iter(std::move(table_iter)),
      _table_begin(std::move(table_begin)),
      _table_end(std::move(table_end)),
      _mutation_iter(std::move(mutation_iter)),
      _mutation_begin(std::move(mutation_begin)),
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
    // TODO: backward iteration is not supported when mutations are present
    // because SortedMerge only supports forward iteration

    CHECK(_mutation_begin == _mutation_end) << "Backward iteration with mutations is not supported";

    // If no mutations, just decrement table iterator
    if (_table_iter != _table_begin) {
        --_table_iter;
    }
    return *this;
}

// MergeTable methods
MergeTable::Iterator MergeTable::lower_bound(TuplePtr search_key, uint64_t index_id, bool index_only) {
    //TODO: secondary index support

    if (index_id != constant::INDEX_PRIMARY) {
        _mutations.clear();
    }

    return Iterator(this,
                    _table->lower_bound(search_key, index_id, index_only),
                    _table->begin(index_id, index_only),
                    _table->end(index_id, index_only),
                    _mutations.lower_bound(search_key),
                    _mutations.begin(),
                    _mutations.end());
}

MergeTable::Iterator MergeTable::upper_bound(TuplePtr search_key, uint64_t index_id, bool index_only) {
    //TODO: secondary index support

    if (index_id != constant::INDEX_PRIMARY) {
        _mutations.clear();
    }

    // Find upper bound in table
    auto table_ub = _table->upper_bound(search_key, index_id, index_only);

    // Find upper bound in mutations using the search_key directly
    auto mutation_ub = _mutations.upper_bound(search_key);

    return Iterator(this,
                    std::move(table_ub),
                    _table->begin(index_id, index_only),
                    _table->end(index_id, index_only),
                    std::move(mutation_ub),
                    _mutations.begin(),
                    _mutations.end());
}

MergeTable::Iterator MergeTable::begin(uint64_t index_id, bool index_only) {
    //TODO: secondary index support

    if (index_id != constant::INDEX_PRIMARY) {
        _mutations.clear();
    }

    return Iterator(this,
                    _table->begin(index_id, index_only),
                    _table->begin(index_id, index_only),
                    _table->end(index_id, index_only),
                    _mutations.begin(),
                    _mutations.begin(),
                    _mutations.end());
}

MergeTable::Iterator MergeTable::end(uint64_t index_id, bool index_only) {
    //TODO: secondary index support

    if (index_id != constant::INDEX_PRIMARY) {
        _mutations.clear();
    }

    return Iterator(this,
                    _table->end(index_id, index_only),
                    _table->begin(index_id, index_only),
                    _table->end(index_id, index_only),
                    _mutations.end(),
                    _mutations.begin(),
                    _mutations.end());
}
