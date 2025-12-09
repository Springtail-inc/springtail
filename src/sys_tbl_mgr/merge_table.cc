#include <sys_tbl_mgr/merge_table.hh>

using namespace springtail;

// MergeTable constructor
MergeTable::MergeTable(TablePtr table, ChangeSet changeset) :
    _table(std::move(table)), 
    _wc_schema_info(table->get_extent_schema_without_row_id(), table->get_extension_callback()),
    _sorted_merge{std::move(changeset), 
        [this](const Extent::Row& a, const Extent::Row& b) { 
            // compare row keys using the write cache schema info
            return FieldTuple(_wc_schema_info._wc_key_fields, &a).less_than(FieldTuple(_wc_schema_info._wc_key_fields, &b));
        }
    }
{
}

// Iterator constructor
MergeTable::Iterator::Iterator(Table::Iterator table_iter)
    : _table_iter(std::move(table_iter)) {
}

// Iterator operators
MergeTable::Iterator::reference MergeTable::Iterator::operator*() {
    return *_table_iter;
}

MergeTable::Iterator& MergeTable::Iterator::operator++() {
    ++_table_iter;
    return *this;
}

MergeTable::Iterator& MergeTable::Iterator::operator--() {
    --_table_iter;
    return *this;
}

uint64_t MergeTable::Iterator::extent_id() const {
    return _table_iter.extent_id();
}

// MergeTable methods
MergeTable::Iterator MergeTable::lower_bound(TuplePtr search_key, uint64_t index_id, bool index_only) {
    return Iterator(_table->lower_bound(search_key, index_id, index_only));
}

MergeTable::Iterator MergeTable::upper_bound(TuplePtr search_key, uint64_t index_id, bool index_only) {
    return Iterator(_table->upper_bound(search_key, index_id, index_only));
}

MergeTable::Iterator MergeTable::inverse_lower_bound(TuplePtr search_key, uint64_t index_id, bool index_only) {
    return Iterator(_table->inverse_lower_bound(search_key, index_id, index_only));
}

MergeTable::Iterator MergeTable::begin(uint64_t index_id, bool index_only) {
    return Iterator(_table->begin(index_id, index_only));
}

MergeTable::Iterator MergeTable::end(uint64_t index_id, bool index_only) {
    return Iterator(_table->end(index_id, index_only));
}
