#pragma once

#include <cstdint>
#include <memory>

#include <storage/extent.hh>
#include <storage/field.hh>
#include <storage/schema.hh>

namespace springtail {
    /**
     * The concrete WriteStrategy (see write_strategy.hh) for a plain btree. Ports the split-point
     * rule that lives today in StorageCache::Page::_check_split() (src/storage/cache.cc:1292) --
     * byte-size threshold, split at the row midpoint -- and the generic choose-child-on-descent
     * logic that lives today in MutableBTree::_find_leaf() (src/storage/mutable_btree.cc). Both
     * move here so the extent cache underneath IndexWriter never has to know it's holding btree
     * nodes (architecture.md Sec 5.1).
     */
    class BtreeWriteStrategy {
    public:
        explicit BtreeWriteStrategy(uint64_t max_extent_size)
            : _max_extent_size(max_extent_size)
        { }

        /** Plain field-wise comparison; both tuples must already be projected onto the same sort
            keys (schema->tuple_subset()) by the caller. */
        bool less_than(TuplePtr lhs, TuplePtr rhs) const {
            return lhs->less_than(rhs);
        }

        /** Binary search over a branch extent's rows for the child to descend into, mirroring
            MutableBTree::_find_leaf()'s lower_bound-or-last() behavior. */
        Extent::Iterator choose_child(std::shared_ptr<Extent> extent,
                                      ExtentSchemaPtr schema,
                                      TuplePtr key) const;

        /** True once `extent` is at or past the byte-size threshold and has more than one row to
            split across, mirroring StorageCache::Page::_check_split()'s guard. */
        bool needs_split(std::shared_ptr<Extent> extent) const {
            return extent->byte_count() >= _max_extent_size && extent->row_count() > 1;
        }

        /** Split at the row midpoint, mirroring Extent::split()'s existing rule exactly -- this
            phase relocates *where the decision is made*, not the rule itself. */
        uint32_t split_point(std::shared_ptr<Extent> extent) const {
            return extent->row_count() / 2;
        }

    private:
        uint64_t _max_extent_size;
    };
}
