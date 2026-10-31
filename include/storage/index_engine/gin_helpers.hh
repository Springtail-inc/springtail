#pragma once

#include <cstdint>
#include <filesystem>
#include <span>
#include <string>
#include <vector>

#include <storage/extent.hh>
#include <storage/field.hh>
#include <storage/schema.hh>

namespace springtail::gin_helpers {

    /**
     * Opclass name recognized by extract_terms() as the built-in, pure-C++ reference "opclass"
     * (defined in gin_helpers_opclass.cc, not backed by PgExtnRegistry/a real Postgres .so at
     * all) -- mirrors gist_helpers::TEST_RANGE_OPCLASS's role for GiST (phases/phase-10.md), for
     * the same reason (architecture.md "btree" redesign phase 11, Open question (e)): a real
     * opclass's own compiled support functions are a flagged-fragile dependency under the
     * multi-entry/spill conditions this phase's own tests specifically need to force, so the
     * split/intersection-correctness tests run against this reference opclass instead, keeping
     * one small, confirmed-stable end-to-end case on the real opclass (gin_trgm_ops).
     *
     * Terms: each whitespace-separated, lower-cased word of a TEXT column's value (deduplicated
     * per row, matching real GIN's own "a row contributes each distinct extracted key once"
     * contract even when extractValue returns a key more than once).
     */
    constexpr std::string_view TEST_TERM_OPCLASS = "__test_term_ops";

    /** Tag byte for an entry-tree leaf's "posting" column (Open question (b)): the value is the
        delta/varint-encoded inline posting list itself. */
    constexpr char ENTRY_VALUE_INLINE = 0;

    /** Tag byte for an entry-tree leaf's "posting" column: the value is a pointer (root extent
        id) to a separate posting tree keyed by internal_row_id. */
    constexpr char ENTRY_VALUE_POINTER = 1;

    /**
     * Decoded form of an entry-tree leaf's "posting" column (Open question (b)'s concrete
     * resolution). Exactly one of the two payloads is meaningful, selected by `is_pointer` --
     * callers never need to inspect the raw tag byte themselves.
     */
    struct DecodedEntryValue {
        bool is_pointer = false;

        /** Meaningful only if !is_pointer: the term's full posting list, sorted ascending. */
        std::vector<uint64_t> inline_ids;

        /** Meaningful only if is_pointer: the posting tree's current root extent id. The
            posting tree's *file path* is never stored here -- it is always re-derived from the
            entry's own term bytes via derive_posting_tree_path(), so this column never needs a
            second variable-length field just to name which file to open. */
        uint64_t posting_tree_root = 0;
    };

    /**
     * Posting-list byte layout (Open question (c), architecture.md Decisions log OQ6): a plain
     * varint-delta encoding of a sorted, duplicate-free run of internal_row_ids --
     *   varint(count) || varint(ids[0]) || varint(ids[1] - ids[0]) || varint(ids[2] - ids[1]) || ...
     * i.e. the first id is encoded as its own (base) value and every subsequent id as its delta
     * from the previous one; `ids` must already be sorted ascending with no duplicates. Each
     * varint is a standard unsigned LEB128 (7 payload bits per byte, high bit = "more bytes
     * follow"). Chosen over Postgres's own GIN varbyte/segment format because the architecture
     * explicitly frames this as "low risk either way... revisit only if measurement shows it
     * matters" -- this is the simplest encoding that still gets real compression for the common
     * case (many ids sharing one entry's inline list), not a byte-for-byte port of Postgres's.
     */
    std::vector<char> encode_posting_list(const std::vector<uint64_t> &sorted_unique_ids);

    /** Inverse of encode_posting_list(). `bytes` must be exactly one encoded posting list (no
        trailing/leading garbage) -- callers slice the tag byte off before calling this. */
    std::vector<uint64_t> decode_posting_list(std::span<const char> bytes);

    /** Builds an entry-tree leaf's full "posting" column value for the inline case: tag byte
        ENTRY_VALUE_INLINE followed by encode_posting_list(sorted_unique_ids). */
    std::vector<char> encode_entry_value_inline(const std::vector<uint64_t> &sorted_unique_ids);

    /** Builds an entry-tree leaf's full "posting" column value for the spilled case: tag byte
        ENTRY_VALUE_POINTER followed by `posting_tree_root` as 8 raw little-endian bytes. */
    std::vector<char> encode_entry_value_pointer(uint64_t posting_tree_root);

    /** Inverse of encode_entry_value_inline()/encode_entry_value_pointer(): reads the tag byte
        and dispatches to the matching payload decode. */
    DecodedEntryValue decode_entry_value(std::span<const char> bytes);

    /**
     * The on-disk leaf/branch row shape for a GIN entry tree -- exactly two columns, "term"
     * (BINARY: the raw extracted-term bytes, an injective but otherwise opaque encoding -- see
     * this file's own header note on why entry-tree order never needs to match any "numeric"
     * order of the opclass's real key type) and "posting" (BINARY: see DecodedEntryValue).
     * Deliberately *not* include_internal_row_id -- architecture.md Sec 5.5's look-aside
     * indirection is satisfied by construction here: a row's identity lives *inside* the
     * posting-list payload (as internal_row_ids), never as a side column on the entry itself.
     * The branch schema is derived automatically from this one by IndexWriter<Strategy>'s own
     * constructor (create_index_schema(keys, {child}, keys, ...)), exactly like any ordinary
     * secondary btree index -- GIN's entry tree needs no separate branch-schema builder.
     */
    ExtentSchemaPtr build_entry_leaf_schema();

    /** The on-disk leaf row shape for a GIN posting tree -- exactly one column, "id" (UINT64,
        the posting tree's own sort key). No payload column: see phases/phase-11.md's Design
        notes for why a posting tree's own leaf rows are one-id-per-row rather than a second,
        redundant round of run-compression on top of the tree's own paging -- the delta/varint
        encoding (OQ c) is applied where it has a real effect, at the entry's own inline list. */
    ExtentSchemaPtr build_posting_leaf_schema();

    /** Deterministically derives a spilled term's posting-tree file path from the entry tree's
        own file path and the term's raw bytes (XXH64 of the term, formatted as a fixed-width hex
        suffix) -- never stored explicitly in the entry's "posting" column, so spilling a term
        never needs a second variable-length field just to name a file. Collision between two
        distinct terms hashing to the same path is astronomically unlikely (a 64-bit hash over
        the handful of terms any one index will ever spill) and is turned into a loud, immediate
        failure rather than silent corruption wherever a *new* posting tree is first created --
        see gin_write_strategy.cc. */
    std::filesystem::path derive_posting_tree_path(const std::filesystem::path &entry_tree_file,
                                                    std::span<const char> term);

    /**
     * Term extraction (architecture.md "btree" redesign Sec 5.1 point 3, Postgres's own
     * GIN_EXTRACTVALUE access-method callback convention): `value`'s field 0 is the single
     * GIN-indexed column's raw (pre-extraction) value -- GIN's single-column scope limit mirrors
     * gist_helpers.hh's identical limit for GiST (see phases/phase-10.md); field 1 (unused here,
     * read by callers themselves) is the row's internal_row_id. Returns the row's *distinct*
     * extracted term byte-strings (real GIN's own contract: a repeated extracted key contributes
     * to the posting list only once per row).
     *
     * Declared here, defined only in gin_helpers_opclass.cc (not this header's own .cc) for the
     * identical library-layout reason gist_helpers.hh's compress/penalty/union/picksplit/
     * consistent are defined only in gist_helpers_opclass.cc: calling a real opclass's
     * GIN_EXTRACTVALUE needs pg_ext (dlopen into a real contrib .so), and pg_ext must never
     * become a transitive link dependency of springtail_pg_fdw.so -- see
     * src/storage/CMakeLists.txt's comment block. The TEST_TERM_OPCLASS reference path is
     * defined in the same file as the real-opclass path (not split out), mirroring
     * gist_helpers_opclass.cc's own "the dispatch check lives next to the real call, not in the
     * opclass-independent file" shape exactly.
     */
    std::vector<std::vector<char>> extract_terms(TuplePtr value, const std::string &opclass_name);

} // namespace springtail::gin_helpers
