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
        /** Default occupancy threshold for underfull() (architecture.md "btree" redesign Sec
            7.1 / phase-8.md OQ5): a node below this fraction of max_extent_size is a
            structural-reclaim (merge) candidate. Picked, not derived -- see
            Vacuumer::underfull_occupancy_fraction()'s doc comment (phases/phase-8.md's Design
            notes) for why this lives on VacuumConfig as the real tunable and this is only the
            fallback for direct (non-Vacuumer-configured) construction, e.g. in tests. */
        static constexpr double DEFAULT_UNDERFULL_FRACTION = 0.25;

        explicit BtreeWriteStrategy(uint64_t max_extent_size,
                                    double underfull_fraction = DEFAULT_UNDERFULL_FRACTION)
            : _max_extent_size(max_extent_size),
              _underfull_fraction(underfull_fraction)
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

        /** True once `extent` has shrunk below `_underfull_fraction` of the split budget
            (phase 8: architecture.md Sec 7.1's "structural waste"). An empty extent trivially
            satisfies this too, but _remove_empty_leaf()'s existing fully-empty path removes
            such a node before it ever reaches a flush this method would be consulted for (see
            IndexWriter::_flush_node()'s own non-empty guard) -- not re-checked here. */
        bool underfull(std::shared_ptr<Extent> extent) const {
            return extent->byte_count() < static_cast<uint64_t>(_max_extent_size * _underfull_fraction);
        }

        /** True if `extent` and `sibling_extent`'s combined content would still fit within one
            extent -- reuses the same max-size budget needs_split()/split_point() enforce. */
        bool can_merge(std::shared_ptr<Extent> extent, std::shared_ptr<Extent> sibling_extent) const {
            return extent->byte_count() + sibling_extent->byte_count() <= _max_extent_size;
        }

    private:
        uint64_t _max_extent_size;
        double _underfull_fraction;
    };
}
