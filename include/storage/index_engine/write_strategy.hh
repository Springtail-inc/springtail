#pragma once

#include <concepts>
#include <memory>

#include <storage/extent.hh>
#include <storage/field.hh>
#include <storage/schema.hh>

namespace springtail {
    /**
     * The WriteStrategy seam (architecture.md "btree" redesign, Sec 5.1): every index-shape
     * decision the Writer engine needs -- how to descend into a branch, and when/where to split
     * an overflowing extent -- is supplied by a per-index-kind strategy, never decided by the
     * extent cache underneath (IndexExtentCache is a dumb extent store with no concept of "this
     * is a btree node").
     *
     * C++ shape (phase-1 OQ4, "implementer's choice of C++ idiom"): IndexWriter is templated on
     * its Strategy (`IndexWriter<Strategy>`), not built around a type-erased `WriteStrategyPtr`.
     * `less_than()`/`choose_child()` run on every row touched during tree descent and mutation --
     * the hottest path in the writer -- and a compile-time strategy lets the compiler inline
     * those calls instead of paying a virtual dispatch per row. GiST (phase 10) and GIN (phase 11)
     * each get their own concrete strategy type and their own IndexWriter<Strategy>
     * instantiation; nothing here requires them to share a vtable (or even the exact method
     * shapes below) with BtreeWriteStrategy -- only btree's needs are committed to in this phase.
     * In particular, GiST's `consistent`/`penalty`-based child selection does not reduce to a
     * sorted-array lower_bound the way `choose_child()` is shaped here, so phase 10 should expect
     * to extend or reshape this concept rather than assume it already fits.
     *
     * Plain field-wise tuple comparison (Tuple::less_than, used for ordering rows *within* one
     * extent) is already schema-generic and is deliberately *not* part of this seam -- it has
     * nothing to do with tree shape and is reused as-is by IndexWriter for any strategy with a
     * total order (btree and GIN's entry/posting trees).
     *
     * **Confirmed, not corrected, for GIN (phase 11)** -- this claim was written speculatively in
     * phase 1, before phase 10's decision 11 found the "one generic engine" framing (architecture.md
     * Sec 5.1) needed correcting for GiST (whose own split/fan-out shape genuinely doesn't fit
     * IndexWriter<Strategy>/IndexReader<Strategy>'s bodies). GIN turned out differently: both its
     * entry tree (keyed by extracted term) and every spilled posting tree (keyed by
     * internal_row_id) really are plain `IndexWriter<BtreeWriteStrategy>` instantiations, reusing
     * `Tuple::less_than()` exactly as this comment always claimed -- see
     * gin_write_strategy.hh's class comment for the full re-derivation (not an assumption) of why,
     * including the one scoped caveat it names (no seam here for a per-opclass `GIN_COMPARE`
     * comparator, which happens not to matter for any opclass this phase tests).
     */
    template <typename S>
    concept WriteStrategy = requires(const S strategy,
                                     TuplePtr lhs,
                                     TuplePtr rhs,
                                     std::shared_ptr<Extent> extent,
                                     ExtentSchemaPtr schema) {
        /** True if `lhs` sorts strictly before `rhs`, by whatever key this strategy's index is
            keyed on. For btree/GIN this is plain field comparison over the configured sort
            keys. */
        { strategy.less_than(lhs, rhs) } -> std::convertible_to<bool>;

        /** Given a branch extent's rows (laid out per `schema`) and a key being inserted or
            searched for, return an iterator to the entry whose child should be descended into.
            Btree: binary search via less_than(). GiST (phase 10) will need a penalty-minimizing
            scan instead -- this phase commits only to the signature btree needs. */
        { strategy.choose_child(extent, schema, lhs) } -> std::same_as<Extent::Iterator>;

        /** True once `extent` has grown enough that it must be split before another mutation. */
        { strategy.needs_split(extent) } -> std::convertible_to<bool>;

        /** The row index at which to split `extent`. Deciding *where* to split is the
            strategy's job (architecture.md Sec 5.1); mechanically copying the resulting row
            ranges into fresh extents is IndexExtentCache's (Sec 8) -- see
            IndexExtentCache::copy_range(). */
        { strategy.split_point(extent) } -> std::convertible_to<uint32_t>;

        /** True once `extent`'s content has shrunk enough (btree redesign Sec 7.1, phase 8:
            "structural waste") that it's a candidate for background merge with a sibling --
            never true for a *fully* empty extent, which the existing _remove_empty_leaf() path
            already handles inline, unconditionally, with no sibling coordination needed. The
            strategy owns the exact occupancy threshold, the same way it owns needs_split()'s
            overflow threshold. */
        { strategy.underfull(extent) } -> std::convertible_to<bool>;

        /** True if `extent` and `sibling_extent`'s combined content would still fit within one
            extent -- the "does a sibling have room" half of background merge. Reuses whatever
            budget needs_split()/split_point() already enforce; decides only whether this one
            candidate pair fits, never whether a merge should be attempted in the first place
            (that's underfull()'s job, plus the caller's sibling-lookup). */
        { strategy.can_merge(extent, extent) } -> std::convertible_to<bool>;
    };
}
