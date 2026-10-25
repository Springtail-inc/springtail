#pragma once

#include <concepts>
#include <memory>

#include <storage/extent.hh>
#include <storage/field.hh>
#include <storage/schema.hh>

namespace springtail {
    /**
     * The ReadStrategy seam (architecture.md "btree" redesign, Sec 5.1), mirroring
     * write_strategy.hh's shape: every index-shape decision the Reader engine needs for descent is
     * supplied by a per-index-kind strategy, never decided by the extent-content provider
     * underneath (ExtentSource, extent_source.hh, is a dumb fetcher of decompressed extent content
     * with no concept of "this is a btree node" -- same spirit as IndexExtentCache on the write
     * side).
     *
     * C++ shape: IndexReader is templated on its Strategy (`IndexReader<Strategy>`), for the same
     * reason write_strategy.hh gives IndexWriter<Strategy> that shape -- arguably a stronger one
     * here: lower_bound()/upper_bound() run on every branch/leaf extent touched during descent,
     * and the Reader's descent happens per query row served from inside a Postgres backend process
     * (the FDW read path), not just per mutation the way the Writer's strategy calls are. A
     * compile-time strategy lets the compiler inline those calls instead of paying a virtual
     * dispatch per row.
     *
     * This is deliberately a different choice from ExtentSource (extent_source.hh), which *is*
     * virtual/type-erased: fetching one extent is an I/O-bound cache lookup (today's StorageCache,
     * tomorrow's phase-5 shared buffer pool), not a per-row comparison, so one virtual call per
     * extent fetch is immaterial -- and runtime polymorphism there is exactly what lets phase 5
     * swap the implementation without IndexReader's own code changing. The two seams in this phase
     * are given different C++ shapes on purpose, not by default/convenience.
     *
     * Descent: BTree::lower_bound()/upper_bound() (btree.cc, today) choose a branch's child by
     * calling plain lower_bound()/upper_bound() directly on the branch extent, projected onto the
     * branch schema -- there is no additional "which child" decision beyond that for a btree's
     * sorted array, so this concept exposes exactly those two bound operations (reused identically
     * for the final leaf search) and nothing resembling a dedicated "choose_child". GiST (phase 10)
     * will need something shaped entirely differently here -- a penalty-minimizing `consistent`
     * scan, not a sorted-array bound -- so phase 10 should expect to extend or reshape this concept
     * rather than assume lower_bound()/upper_bound() already fit, exactly the same warning
     * write_strategy.hh gives for choose_child().
     *
     * less_than() is included for symmetry with WriteStrategy, and because it is the one piece
     * small and stateless enough to be literally shared code (architecture.md Sec 5.1) if some
     * future strategy has a real use for a shared comparator -- but, like
     * WriteStrategy::less_than() today, no code in this phase actually calls strategy.less_than()
     * polymorphically; schema-generic Tuple::less_than() is used directly wherever IndexReader
     * itself needs a plain key comparison (e.g. find()'s exact-match check).
     */
    template <typename S>
    concept ReadStrategy = requires(const S strategy,
                                    TuplePtr lhs,
                                    TuplePtr rhs,
                                    std::shared_ptr<Extent> extent,
                                    ExtentSchemaPtr schema) {
        /** True if `lhs` sorts strictly before `rhs`, by whatever key this strategy's index is
            keyed on. */
        { strategy.less_than(lhs, rhs) } -> std::convertible_to<bool>;

        /** First row of `extent` (laid out per `schema`) with a key >= `lhs`, or `extent->end()`
            if none. Used both to choose a branch's child during descent and to locate the final
            leaf entry. */
        { strategy.lower_bound(extent, schema, lhs) } -> std::same_as<Extent::Iterator>;

        /** First row of `extent` (laid out per `schema`) with a key > `lhs`, or `extent->end()`
            if none. */
        { strategy.upper_bound(extent, schema, lhs) } -> std::same_as<Extent::Iterator>;
    };
}
