#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <storage/extent.hh>
#include <storage/field.hh>
#include <storage/schema.hh>

namespace springtail::gist_helpers {

    /**
     * Opclass name recognized by compress_leaf_entry()/compute_penalty()/compute_union()/
     * compute_picksplit()/consistent() as the built-in, pure-C++ reference "opclass" (defined in
     * gist_helpers.cc, not backed by PgExtnRegistry/a real Postgres .so at all) -- see
     * gist_helpers.cc's own doc comment on its test_range_ops namespace for why this phase's
     * tests need one: a confirmed, not-fully-root-caused crash in the real-opclass-interop path
     * (pg_trgm.so's gtrgm_union()/gtrgm_penalty()) on any multi-entry build, documented in
     * phases/phase-10.md's Design notes. Query bytes for this opclass are a plain, unencoded
     * string (a bare value for strategy 1 "=", or "lo\x1fhi" for strategy 2 "overlaps") -- never
     * a Postgres varlena the way a real opclass's query would be.
     */
    constexpr std::string_view TEST_RANGE_OPCLASS = "__test_range_ops";
    constexpr uint16_t TEST_RANGE_STRATEGY_EQ = 1;
    constexpr uint16_t TEST_RANGE_STRATEGY_OVERLAPS = 2;

    /** Builds a "lo\x1fhi" query for TEST_RANGE_OPCLASS's strategy-2 (overlaps) search. */
    std::string make_test_range_query(const std::string &lo, const std::string &hi);

    /**
     * One GiST entry -- a leaf's compressed indexed value(s), or a branch's union key -- adapted
     * from SPR-1036-GiST's include/storage/gist_helpers.hh (which this redesign ports, architecture.md
     * Sec 5.1/Decisions log entry 3), but reshaped around *owned* byte buffers rather than raw
     * Postgres Datums: every GIST_COMPRESS/GIST_UNION/GIST_PICKSPLIT support function this phase
     * calls into (btree_gist's var-length opclasses -- gist_text_ops, confirmed against the real
     * contrib source) returns a varlena (self-describing via VARSIZE_ANY/VARDATA_ANY, see
     * pg_ext/varatt.hh) pointing at process-local palloc'd memory whose lifetime is not ours to
     * manage; copying the bytes out immediately into `keys[i]` is what lets a GistEntry survive
     * past the one DirectFunctionCallN that produced it, and be written into/read back out of an
     * Extent row (storage::SchemaType::BINARY columns) across generations.
     *
     * Deliberate scope limit (recorded in phases/phase-10.md's Design notes): only opclasses whose
     * GIST_COMPRESS/GIST_UNION results are varlena are supported by this helper -- e.g. btree_gist's
     * scalar numeric/date opclasses (gist_int4_ops, etc.) use a fixed-size, non-varlena internal key
     * struct (gbtreekey4/8/16/32) whose true byte size isn't self-describing and isn't known to this
     * code, so copying "VARSIZE_ANY(ptr) bytes" for those would misread raw struct bytes as a varlena
     * header. Pick a varlena-keyed opclass (gist_text_ops/gist_bpchar_ops are the ones btree_gist
     * ships) until a later phase needs scalar GiST opclasses badly enough to hardcode their key-type
     * sizes.
     */
    struct GistEntry {
        /** One raw compressed/union byte buffer per indexed column, in column order. */
        std::vector<std::string> keys;

        /** Valid (and meaningful) only for leaf entries: the row's look-aside key (architecture.md
            Sec 5.5) -- never a physical location. */
        uint64_t internal_row_id = 0;

        /** Valid (and meaningful) only for branch entries: the child node's id. */
        uint64_t child_id = 0;

        bool is_leaf = true;
    };

    /**
     * Build a leaf GistEntry from a row being inserted: for each of the first `opclass_names.size()`
     * fields of `value` (laid out per `schema`), convert the stored Springtail value to a Datum and
     * run it through the opclass's GIST_COMPRESS support function if one is registered (falling back
     * to the raw, uncompressed Datum bytes if not -- mirrors real GiST's "compress is optional"
     * contract). Does *not* read `value`'s trailing internal_row_id field -- callers set
     * GistEntry::internal_row_id separately, the same way the ported branch's
     * extract_gist_entry_from_tuple() left row identity to its own caller.
     */
    GistEntry compress_leaf_entry(TuplePtr value, const std::vector<std::string> &opclass_names);

    /** Read a previously-written leaf entry back out of an on-disk row (schema: one BINARY column
        per opclass_names entry, followed by a trailing UINT64 internal_row_id column). */
    GistEntry read_leaf_entry_from_row(const Extent::Row &row, ExtentSchemaPtr schema,
                                        const std::vector<std::string> &opclass_names);

    /** Read a previously-written branch entry back out of an on-disk row (schema: one BINARY
        column per opclass_names entry, followed by a trailing UINT64 child-id column). */
    GistEntry read_branch_entry_from_row(const Extent::Row &row, ExtentSchemaPtr schema,
                                          const std::vector<std::string> &opclass_names);

    /** Write `entry` (a leaf entry) into `row`, laid out per `schema` (see read_leaf_entry_from_row
        for the expected column shape). */
    void write_leaf_row_from_entry(Extent::Row &row, const GistEntry &entry, ExtentSchemaPtr schema,
                                    const std::vector<std::string> &opclass_names);

    /** Write `key` (a branch entry's union key) plus `child_id` into `row`, laid out per `schema`. */
    void write_branch_row_from_union(Extent::Row &row, const GistEntry &key, uint64_t child_id,
                                      ExtentSchemaPtr schema, const std::vector<std::string> &opclass_names);

    /**
     * GIST_PENALTY: the cost, summed independently per indexed column, of expanding `existing`
     * (an already-materialized branch or leaf entry) to also cover `incoming`. Lower is better --
     * GistWriter's choose_subtree descent (real Postgres's gistchoose(), mirrored here) picks the
     * branch entry with the lowest total penalty, never consulting GIST_CONSISTENT at all (that
     * support function is exclusively a *search*-time predicate check -- see gist_read_strategy.hh).
     */
    double compute_penalty(const GistEntry &existing, const GistEntry &incoming,
                            const std::vector<std::string> &opclass_names);

    /** GIST_UNION: the smallest entry (independently per column) that covers every one of
        `entries`. Used both to build a fresh branch row for a newly split child, and to recompute
        an ancestor's routing key after a descendant's content changes. */
    GistEntry compute_union(const std::vector<GistEntry> &entries,
                             const std::vector<std::string> &opclass_names);

    /**
     * GIST_PICKSPLIT: partition `entries` (indices 0..entries.size()-1) into two non-empty,
     * non-overlapping (as index sets, not as key ranges -- GiST partitions are not required to be
     * key-range-disjoint, only index-disjoint) groups, `left`/`right`, that this opclass judges to
     * minimize overlap/expansion. Only the *first* indexed column's opclass drives the split
     * decision (matches real multi-column GiST, which always picksplits on column 0 and lets
     * secondary columns ride along) -- see compute_picksplit's .cc comment for why.
     */
    void compute_picksplit(const std::vector<GistEntry> &entries,
                            std::vector<size_t> &left, std::vector<size_t> &right,
                            const std::vector<std::string> &opclass_names);

    /**
     * GIST_CONSISTENT for column `col`: does `entry` (a branch or leaf entry already materialized
     * from a row) satisfy `query` (a raw, uncompressed Datum of the column's own input type, not a
     * GistEntry -- GIST_CONSISTENT's second argument is always the original scan key, never a
     * compressed one) under `strategy` (a Postgres btree-style StrategyNumber -- 1=<,2=<=,3=,4=>=,
     * 5=> for btree_gist's opclasses)? `is_leaf` must reflect whether `entry` came from a leaf or
     * branch row -- GIST_LEAF(entry) inside the opclass's own consistent function changes whether it
     * applies an exact check or (for a branch's union bounding box) a conservative overlap check.
     *
     * `recheck_out`, if non-null, receives the real GIST_CONSISTENT ABI's `recheck` out-param
     * (round-1 review of this phase, finding 3: this value used to be computed and then silently
     * discarded, with no way for any caller to ever retrieve it). `recheck=true` means the
     * opclass's compression is lossy for this entry and the match is a conservative over-
     * approximation the caller must re-verify against the real indexed value before trusting --
     * real GiST's `gistget()` unconditionally honors this; `pg_trgm`'s `gist_trgm_ops` sets it for
     * longer/more numerous inputs whose trigram set is signature-summarized. No caller in this
     * phase reads it yet (query-time routing -- the first place that would need to act on it by
     * re-checking the real row -- is phase 12's job; see phases/phase-10.md's Design notes), but
     * the value must be retrievable rather than dropped on the floor, so a later phase doesn't
     * have to rediscover that `GistReader::search()`'s results can be false positives for a lossy
     * opclass. The pure-C++ `TEST_RANGE_OPCLASS` reference opclass is never lossy (exact string
     * comparisons only) and always reports `recheck=false`.
     */
    bool consistent(const GistEntry &entry, int col, const std::string &query_bytes,
                     uint16_t strategy, bool is_leaf, const std::vector<std::string> &opclass_names,
                     bool *recheck_out = nullptr);

    /** Build a raw query Datum (varlena bytes, same convention as a compressed/union GistEntry key)
        for a text value -- the one helper this phase's own tests need to drive consistent() without
        going through a real `CREATE INDEX`/qual (query-time qual routing is phase 12's job). */
    std::string make_text_query(const std::string &value);

    /**
     * The on-disk row shape for a GiST leaf/branch extent -- one BINARY column per indexed
     * column, named "__gist_key_<i>", followed by a trailing UINT64 (internal_row_id for a leaf,
     * child-node-id for a branch). Factored out here, not duplicated in both GistWriter and
     * GistReader, because the two engines must agree on exactly this layout: GistReader reads
     * extents GistWriter wrote.
     */
    ExtentSchemaPtr build_leaf_schema(const std::vector<std::string> &opclass_names);
    ExtentSchemaPtr build_branch_schema(const std::vector<std::string> &opclass_names);

} // namespace springtail::gist_helpers
