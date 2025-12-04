#pragma once

#include <sys_tbl_mgr/table.hh>

namespace springtail {

/**
 * Holds a set of in-memory extents that contain changes that need to be applied over an existing table while doing a scan.
 */
class ChangeSet {
public:
    /**
     * Internal helper that encapsulates a set of changes for a single table within a single
     * transaction.  The mutations are kept in one or more extents.  They are kept in primary key
     * order.
     */
    class Txn {
    public:
        explicit Txn(std::vector<ExtentPtr> extents) : _extents(std::move(extents)) {}

        /**
         * An iterator through the ChangeSet.
         */
        class Iterator {
        public:
            using iterator_category = std::bidirectional_iterator_tag;
            using value_type = Extent::Row;
            using difference_type = std::ptrdiff_t;
            using pointer = Extent::Row*;
            using reference = Extent::Row&;

            Iterator();
            explicit Iterator(const std::vector<ExtentPtr>& extents, bool is_end = false);
            Iterator(const std::vector<ExtentPtr>& extents, const TuplePtr& search_key, bool lower_bound = true);
            
            Iterator& operator++();
            Iterator operator++(int);
            Iterator& operator--();
            Iterator operator--(int);
            
            Extent::Row operator*() const;
            const Extent::Row* operator->() const;
            
            bool operator==(const Iterator& other) const;
            bool operator!=(const Iterator& other) const;

        private:
            const std::vector<ExtentPtr>* _extents;
            std::vector<ExtentPtr>::const_iterator _extent_it;
            std::vector<ExtentPtr>::const_iterator _extent_end;
            Extent::Iterator _row_it;
            bool _is_end;
            
            void advance_to_next_valid_row();
            void retreat_to_prev_valid_row();
        };

        Iterator begin();
        Iterator end();
        Iterator lower_bound(const TuplePtr& search_key);
        Iterator upper_bound(const TuplePtr& search_key);
        Iterator inverse_lower_bound(const TuplePtr& search_key);

    private:
        std::vector<ExtentPtr> _extents;
    };

    explicit ChangeSet(std::vector<Txn> changes) : _changes(std::move(changes)) {}

    /**
     * An iterator through the ChangeSet.
     */
    class Iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = std::vector<Extent::Row>;
        using difference_type = std::ptrdiff_t;
        using pointer = std::vector<Extent::Row>*;
        using reference = std::vector<Extent::Row>&;

        Iterator();
        explicit Iterator(const std::vector<Txn>& changes, bool is_end = false);
        Iterator(const std::vector<Txn>& changes, const TuplePtr& search_key, bool lower_bound = true);
        
        Iterator& operator++();
        Iterator operator++(int);
        
        const std::vector<Extent::Row>& operator*() const;
        const std::vector<Extent::Row>* operator->() const;
        
        bool operator==(const Iterator& other) const;
        bool operator!=(const Iterator& other) const;

    private:
        struct TxnIteratorPair {
            Txn::Iterator txn_it;
            size_t xid_index;
            
            bool operator<(const TxnIteratorPair& other) const;
        };
        
        const std::vector<Txn>* _changes;
        std::priority_queue<TxnIteratorPair> _heap;
        bool _is_end;
        
        // Current element state: all rows with the same key
        mutable std::vector<Extent::Row> _current_matching_rows;
        mutable bool _current_rows_valid;
        
        void initialize_heap();
        void initialize_heap_with_bound(const TuplePtr& search_key, bool lower_bound);
        void advance_heap();
        
        // Build current matching rows by consuming from heap
        void build_current_matching_rows() const;
    };

    Iterator begin();
    Iterator end();
    Iterator lower_bound(const TuplePtr& search_key);
    Iterator upper_bound(const TuplePtr& search_key);
    Iterator inverse_lower_bound(const TuplePtr& search_key);

    std::vector<Txn> _changes;
};

using ChangeSetPtr = std::shared_ptr<ChangeSet>;

/**
 * The objective of the MergeTable is to take in (1) a provided Table at a given XID and (2) a set of
 * table mutations and allow for a scan of the Table at a future XID.
 */
class MergeTable {
public:
    /**
     * An iterator for scanning the table.
     */
    class Iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = Extent::Row;
        using difference_type = std::ptrdiff_t;
        using pointer = Extent::Row*;
        using reference = Extent::Row&;

        Iterator();
        Iterator(Table::Iterator table_it, Table::Iterator table_end, ChangeSet::Iterator changes_it, ChangeSet::Iterator changes_end);
        Iterator(Table::Iterator table_it, Table::Iterator table_end, ChangeSet::Iterator changes_it, ChangeSet::Iterator changes_end, const TuplePtr& search_key, bool lower_bound = true);
        
        Iterator& operator++();
        Iterator operator++(int);
        
        Extent::Row operator*() const;
        const Extent::Row* operator->() const;
        
        bool operator==(const Iterator& other) const;
        bool operator!=(const Iterator& other) const;

    private:
        Table::Iterator _table_it;
        Table::Iterator _table_end;
        ChangeSet::Iterator _changes_it;
        ChangeSet::Iterator _changes_end;
        
        bool _is_end;
        mutable Extent::Row _merged_row;  // Cache for merged row
        mutable bool _row_cached;
        
        void find_next_valid_position();
        void merge_current_row() const;
        int compare_keys(const Extent::Row& table_row, const Extent::Row& changes_row) const;
    };

public:
    /** Construct a MergeTable from the table and a set of changes (generally provided from the write cache). */
    MergeTable(TablePtr table, ChangeSetPtr changes, uint64_t index_id = constant::INDEX_PRIMARY);

    Iterator lower_bound(TuplePtr search_key);
    Iterator upper_bound(TuplePtr search_key);
    Iterator inverse_lower_bound(TuplePtr search_key);

    Iterator begin();
    Iterator end();

private:
    uint64_t _index;
    TablePtr _table;
    ChangeSetPtr _changes;
};

};
