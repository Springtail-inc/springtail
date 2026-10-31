#pragma once

#include <memory>

#include <storage/extent.hh>
#include <storage/field.hh>
#include <storage/schema.hh>

namespace springtail {
    /**
     * The concrete ReadStrategy (read_strategy.hh) for a plain btree.
     *
     * less_than() duplicates BtreeWriteStrategy::less_than()'s one-line body
     * (btree_write_strategy.hh) rather than factoring it into a shared header: both are already
     * schema-generic wrappers around Tuple::less_than(), and (per read_strategy.hh/
     * write_strategy.hh's header comments) neither IndexWriter nor IndexReader actually calls
     * strategy.less_than() anywhere in their own traversal code today -- it exists on both
     * concepts only so a future strategy with a real need for a shared comparator has somewhere to
     * put one. Introducing a shared header to deduplicate one line that nothing calls would be
     * over-engineering for what this phase needs; revisit if a later strategy actually needs to
     * share more than this.
     *
     * lower_bound()/upper_bound() port BTree::lower_bound()/upper_bound()'s exact descent rule
     * (src/storage/btree.cc, today): a plain, schema-projected binary search over the extent's
     * rows, used identically whether descending a branch or searching the final leaf -- btree has
     * no separate "choose child" primitive distinct from this (see read_strategy.hh's header
     * comment).
     */
    class BtreeReadStrategy {
    public:
        BtreeReadStrategy() = default;

        bool less_than(TuplePtr lhs, TuplePtr rhs) const {
            return lhs->less_than(rhs);
        }

        /** First row of `extent` (laid out per `schema`) with a key >= `key`. */
        Extent::Iterator lower_bound(std::shared_ptr<Extent> extent,
                                     ExtentSchemaPtr schema,
                                     TuplePtr key) const;

        /** First row of `extent` (laid out per `schema`) with a key > `key`. */
        Extent::Iterator upper_bound(std::shared_ptr<Extent> extent,
                                     ExtentSchemaPtr schema,
                                     TuplePtr key) const;
    };
}
