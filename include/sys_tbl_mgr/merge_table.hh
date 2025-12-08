#pragma once

#include <sys_tbl_mgr/table.hh>
#include <common/sorted_merge.hh>


namespace springtail {

using ChangeSet = std::vector<std::vector<ExtentPtr>>;

class MergeTable {
public:
    /**
     * A forward iterator over the rows of a MergeTable object.
     */
    class Iterator {
        friend MergeTable;

    public:
        using iterator_category = std::bidirectional_iterator_tag;
        using difference_type   = std::ptrdiff_t;
        using value_type        = const Extent::Row;
        using pointer           = const Extent::Row *;
        using reference         = const Extent::Row &;

        reference operator*();
        pointer operator->() { return &*(*this); }

        /**
         * Move the iterator forward to the next row.
         */
        Iterator& operator++();

        /**
         * Move the iterator backward to the previous row.
         */
        Iterator& operator--();

        /**
         * Compares two iterators for equality.
         */
        friend bool operator==(const Iterator& a, const Iterator& b)
        {
            return a._table_iter == b._table_iter;
        }

        /**
         * Compares two iterators for inequality.
         */
        friend bool operator!= (const Iterator& a, const Iterator& b) { return !(a == b); }

        /**
         * Returns the current extent id of the iterator.
         */
        uint64_t extent_id() const;

    private:
        /** For constructing an Iterator from the MergeTable functions. */
        Iterator(Table::Iterator table_iter);
        Table::Iterator _table_iter;
    };

public:
    /**
     * MergeTable constructor.
     */
    MergeTable(TablePtr table, ChangeSet changeset);

    /**
     * Returns an iterator to the first row that is greater than or equal to the provided search key.
     */
    Iterator lower_bound(TuplePtr search_key, uint64_t index_id = constant::INDEX_PRIMARY, bool index_only = false);

    Iterator upper_bound(TuplePtr search_key, uint64_t index_id = constant::INDEX_PRIMARY, bool index_only = false);

    /**
     * Returns an iterator to the first row that is less than or equal to the provided search key.
     */
    Iterator inverse_lower_bound(TuplePtr search_key, uint64_t index_id = constant::INDEX_PRIMARY, bool index_only = false);

    /**
     * An iterator to the start of the table.
     */
    Iterator begin(uint64_t index_id = constant::INDEX_PRIMARY, bool index_only = false);

    /**
     * An iterator to the end of the table.
     */
    Iterator end(uint64_t index_id = constant::INDEX_PRIMARY, bool index_only = false);

    TablePtr table() const
    {
        return _table;
    }

private:
    TablePtr _table;
    ChangeSet _changeset;
};

}; // namespace springtail
