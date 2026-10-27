#pragma once

#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <vector>

#include <absl/log/check.h>

#include <storage/extent.hh>
#include <storage/field.hh>
#include <storage/index_engine/btree_write_strategy.hh>
#include <storage/index_engine/gin_helpers.hh>
#include <storage/index_engine/index_extent_cache.hh>
#include <storage/index_engine/index_writer.hh>
#include <storage/schema.hh>

namespace springtail {
    /**
     * The Writer-side GIN engine (architecture.md "btree" redesign Sec 5.2, phase 11). Named
     * `GinWriteStrategy` (not `GinWriter`) to match this phase's brief/plan.md's file naming --
     * see this class's own "engine shape" note below for why, unlike GiST (phases/phase-10.md's
     * decision 11), that naming choice doesn't leave a separate orchestrating engine class
     * unaccounted for: there isn't one here, by design.
     *
     * **Resolves Open question (a), the central question of this phase:** does GIN's entry tree
     * (and posting tree) need its own concrete, non-template engine the way GiST did, or can it
     * be built as a genuine `IndexWriter<SomeStrategy>` instantiation with GIN-specific
     * term-upsert/posting-list logic layered on top using the engine's existing public API?
     *
     * **Resolved: the latter.** Both GIN's entry tree (keyed by extracted term) and every spilled
     * posting tree (keyed by internal_row_id) are literal `IndexWriter<BtreeWriteStrategy>`
     * instantiations -- the exact same class ordinary secondary btree indexes use, differing only
     * in schema/keys (gin_helpers::build_entry_leaf_schema()/build_posting_leaf_schema()). This is
     * the opposite conclusion from GiST's decision 11, for reasons specific to GIN's own shape,
     * re-derived here rather than assumed either way (per this phase's brief):
     *   - `IndexWriter<Strategy>::_split_node()` copies a *contiguous row range by position*
     *     (`IndexExtentCache::copy_range()`). GiST's `picksplit` produces an arbitrary two-way
     *     partition with no positional relationship to copy -- genuinely incompatible. GIN's
     *     entry/posting trees are both totally ordered (term bytes; internal_row_id) with a plain
     *     midpoint split -- exactly what `BtreeWriteStrategy`/`copy_range()` already assume. No
     *     new split logic is needed.
     *   - `IndexReader<Strategy>::Iterator` is a single-frame stack walking one root-to-leaf path.
     *     GiST's `consistent()`-driven search can fan out into multiple children at one branch --
     *     incompatible with a single path. GIN's own descent (exact term lookup; exact
     *     internal_row_id lookup) is single-path by construction -- no fan-out, no new iterator
     *     shape needed.
     *   - The one thing genuinely new to GIN and absent from both btree and GiST's own problems:
     *     *upserting* an existing key's value (growing a term's posting list on a repeat
     *     insertion, not just inserting a new distinct key) -- `IndexWriter::insert()` always
     *     inserts a *new* row and asserts (CHECK) no existing key collision; `remove()` asserts an
     *     exact-key match exists. Confirmed by reading `index_writer.cc`'s bodies (not just the
     *     strategy headers' comments, per this phase's own central lesson): `lower_bound(key,
     *     for_update)` is a **pure, non-mutating** descent (no `_touch()` call anywhere in its
     *     body) -- so "peek at the current value via lower_bound(), decode it, discard the
     *     iterator, then call `remove(old_key)` + `insert(new_value)`" is safe: `remove()`/
     *     `insert()` each independently re-run `_find_leaf_path()` from `_root`, so no
     *     iterator/path is ever carried across the mutation boundary, and `remove()` only needs
     *     the row's *key* fields to match (verified directly in `_remove_row()`), not the old
     *     value, to locate and delete the row. This is exactly how every upsert in this file (term
     *     insert/merge, posting-tree root pointer fixup at finalize()) is implemented.
     *
     * One caveat this decision depends on, named explicitly: `BtreeWriteStrategy::less_than()` is
     * a fixed, non-customizable `Tuple::less_than()` (plain per-field comparison) -- there is no
     * seam here for a per-opclass `GIN_COMPARE` comparator. This only works because every
     * opclass this phase's entry-tree ordering ever needs (the pure-C++ reference opclass, and
     * `gin_trgm_ops` -- confirmed via `nm -D pg_trgm.so`: no `gin_trgm_compare`/
     * `gin_trgm_compare_partial` symbol is exported, meaning pg_trgm registers no custom
     * `GIN_COMPARE` and relies on the key type's default int4 comparator) needs no ordering
     * beyond a consistent, injective byte encoding of the extracted term -- entries are only ever
     * looked up by *exact match*, never by range, so raw-byte term encoding plus plain
     * `Tuple::less_than()`'s byte-lexicographic `BINARY` comparison (see field.hh) is a valid
     * total order for correctness, even though it doesn't match the key type's own "numeric"
     * order. An opclass that *did* register a non-default `GIN_COMPARE` would need engine
     * extension this phase doesn't build -- named here as a scoped limitation, mirroring
     * gist_helpers.hh's own documented varlena-only scope limit.
     *
     * Confirms (does not correct) write_strategy.hh's own doc-comment claim that GIN's entry/
     * posting trees reuse `Tuple::less_than()` "as any strategy with a total order" -- that
     * claim turned out to need independent verification (this phase's own central lesson,
     * inherited from phase 10's decision 11), not blind trust, but it holds for GIN, unlike the
     * "one generic engine" framing that needed correcting for GiST.
     *
     * Engine-shape consequence: since no new engine body is needed, there is also no separate
     * `GinWriter` orchestrating class the way `GistWriter` wraps `GistWriteStrategy`'s opclass
     * glue -- this class *is* the orchestrator (owns the entry tree's own
     * `IndexWriter<BtreeWriteStrategy>` plus a lazily-populated map of spilled posting trees, each
     * also a plain `IndexWriter<BtreeWriteStrategy>`), named `GinWriteStrategy` only because
     * phases/phase-11.md's brief/plan.md name exactly two files (`gin_write_strategy.hh/.cc`,
     * `gin_read_strategy.hh/.cc`) for GIN's write/read sides respectively, with no
     * `gin_writer.hh`/`gin_reader.hh` named alongside them the way GiST's four files were. This is
     * fewer files than GiST's split, not a renamed copy of it -- recorded as a deliberate
     * structural deviation in phases/phase-11.md's Design notes, not a silent one.
     *
     * Single-writer, no internal locking (architecture.md Sec 5.4), same contract as every other
     * engine type in this redesign -- exclusive-access-by-caller-contract, not internally
     * synchronized; the only thing standing between that contract and silent corruption is the
     * debug-build-only ReentrancyGuard, mirroring IndexWriter's/GistWriter's identical one.
     */
    class GinWriteStrategy {
    private:
        class ReentrancyGuard {
        public:
            explicit ReentrancyGuard(bool &flag) : _flag(flag) {
#ifndef NDEBUG
                CHECK(!_flag) << "GinWriteStrategy: reentrant or concurrent call detected on a "
                                 "single-writer instance";
                _flag = true;
#endif
            }
            ~ReentrancyGuard() {
#ifndef NDEBUG
                _flag = false;
#endif
            }
            ReentrancyGuard(const ReentrancyGuard &) = delete;
            ReentrancyGuard &operator=(const ReentrancyGuard &) = delete;
        private:
            bool &_flag;
        };

        struct PostingTreeEntry {
            std::vector<char> term;
            std::shared_ptr<IndexWriter<BtreeWriteStrategy>> writer;
        };

    public:
        GinWriteStrategy(uint64_t database_id,
                         std::filesystem::path file,
                         uint64_t xid,
                         std::vector<std::string> opclass_names,
                         uint64_t max_extent_size,
                         std::shared_ptr<IndexExtentCache> cache);

        void init_empty();
        void init(uint64_t root_offset);

        /** `value`'s field 0 is the single GIN-indexed column's raw (pre-extraction) value; field
            1 is the row's internal_row_id -- the exact same calling convention
            `IndexWriter::insert()`/`GistWriter::insert()` already use for a secondary index, so
            `Indexer::_build()`'s/`_reconcile_index()`'s existing scan-and-insert/remove loops work
            against this engine unchanged (see table.hh's `AnyIndexRoot`). Extracts this row's
            terms (gin_helpers::extract_terms()) and upserts `internal_row_id` into each distinct
            term's posting list, spilling to a posting tree if the inline list would outgrow a
            page (Open question (d): the threshold is SPILL_THRESHOLD_BYTES, a quarter of
            `max_extent_size` -- see gin_write_strategy.cc). */
        void insert(TuplePtr value);

        /** Removes `value`'s internal_row_id (field 1) from every one of its extracted terms'
            posting lists -- a term whose posting list becomes empty has its entry row removed
            entirely (matching real GIN). A posting tree, once created for a term, is never
            demoted back to an inline list even if it later shrinks (see this file's own "known
            gap" note in gin_write_strategy.cc). */
        void remove(TuplePtr value);

        void truncate();

        /** Flushes every posting tree touched this generation *before* the entry tree (so each
            dirty term's entry row can be rewritten with that posting tree's real, now-known root
            extent id -- see gin_write_strategy.cc's finalize() for why a pointer entry's
            root-offset field is never trustworthy mid-generation and is fixed up exactly once,
            here, rather than on every posting-tree mutation). Returns the entry tree's own root
            extent id, exactly like `IndexWriter::finalize()`/`GistWriter::finalize()`. */
        uint64_t finalize(bool fs_sync = true);

        bool empty();

        void sync();

        std::filesystem::path get_file_path() const;

        /** The entry tree's own segments plus every posting tree *currently open this
            generation*'s segments -- not every posting tree this index has ever spilled (which
            would require eagerly opening all of them at construction; see this class's own
            lazy-open design). Mirrors `IndexWriter::get_segment_paths()`'s doc comment on why
            `get_file_path()` alone isn't enough once a tree can span multiple files. */
        std::vector<std::filesystem::path> get_segment_paths() const;

    private:
        TuplePtr _make_term_key_tuple(const std::vector<char> &term) const;
        TuplePtr _make_entry_value_tuple(const std::vector<char> &term, const std::vector<char> &posting_value) const;
        TuplePtr _make_posting_id_tuple(uint64_t id) const;

        bool _entry_exact_match(const IndexWriter<BtreeWriteStrategy>::Iterator &it,
                                const std::vector<char> &term);
        gin_helpers::DecodedEntryValue _decode_entry_at(const IndexWriter<BtreeWriteStrategy>::Iterator &it) const;

        void _upsert_term(const std::vector<char> &term, uint64_t row_id);
        void _remove_term(const std::vector<char> &term, uint64_t row_id);

        bool _posting_contains(IndexWriter<BtreeWriteStrategy> &pt, uint64_t id);

        /** Finds (lazily reopening from disk if needed, via `existing_root`) or creates
            (`existing_root == UNKNOWN_EXTENT` meaning "brand new this call") the posting tree for
            `term`, caching it in `_posting_trees` keyed by `XXH64(term)` for the rest of this
            generation. */
        IndexWriter<BtreeWriteStrategy> &_open_posting_tree(const std::vector<char> &term, uint64_t existing_root);
        IndexWriter<BtreeWriteStrategy> &_create_posting_tree(const std::vector<char> &term);

        void _mark_posting_dirty(const std::vector<char> &term);

        uint64_t _database_id;
        std::filesystem::path _file;
        uint64_t _xid;
        std::vector<std::string> _opclass_names;
        uint64_t _max_extent_size;
        uint64_t _spill_threshold_bytes;
        std::shared_ptr<IndexExtentCache> _cache;

        ExtentSchemaPtr _entry_schema;
        FieldPtr _entry_term_f;
        FieldPtr _entry_posting_f;

        ExtentSchemaPtr _posting_schema;
        FieldPtr _posting_id_f;

        IndexWriter<BtreeWriteStrategy> _entry_tree;

        std::map<uint64_t, PostingTreeEntry> _posting_trees;

        /** Terms whose posting tree was mutated this generation and whose entry row's pointer
            value therefore needs rewriting (with the posting tree's real root id) at finalize()
            -- see finalize()'s own comment for why this can't be done eagerly on every mutation. */
        std::map<uint64_t, std::vector<char>> _dirty_posting_terms;

        bool _in_call = false;
    };

    using GinWriterPtr = std::shared_ptr<GinWriteStrategy>;
}
