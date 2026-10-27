#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <vector>

#include <storage/extent.hh>
#include <storage/field.hh>
#include <storage/index_engine/btree_read_strategy.hh>
#include <storage/index_engine/extent_source.hh>
#include <storage/index_engine/gin_helpers.hh>
#include <storage/index_engine/index_reader.hh>
#include <storage/schema.hh>

namespace springtail {
    /**
     * The Reader-side GIN engine (architecture.md "btree" redesign Sec 5.2, phase 11) -- the
     * read-side counterpart of GinWriteStrategy's engine-shape decision (see that header's class
     * comment for the full reasoning, not repeated here): both the entry tree and every posting
     * tree are literal `IndexReader<BtreeReadStrategy>` instantiations, so (unlike GiST's
     * GistReader) this class needs no new engine body at all, only GIN-specific lookup/
     * intersection logic layered on top of the existing public API.
     *
     * Deliberately takes already-extracted term bytes, never a raw Postgres Datum/qual -- query-
     * time translation of a real predicate into term bytes (Postgres's own GIN_EXTRACTQUERY
     * callback) is explicitly phase 12's job (architecture.md Sec 3, this phase's "Explicitly out
     * of scope"). This is *why* this class, unlike GinWriteStrategy, never needs
     * gin_helpers::extract_terms() (which needs pg_ext to call a real opclass) and therefore
     * never needs to link the pg_ext-dependent opclass-engine library -- it lives entirely in
     * springtail_storage, consumed by both the daemon-side and FDW-side builds of `Table` the
     * same way the opclass-independent half of GistReader does (gist_reader.cc, as opposed to
     * gist_reader_search.cc). See src/storage/CMakeLists.txt.
     *
     * Immutable except for set_root_and_xid(), mirroring IndexReader's/GistReader's identical
     * rebase hook.
     */
    class GinReadStrategy {
    public:
        GinReadStrategy(uint64_t database_id,
                        std::filesystem::path file,
                        uint64_t xid,
                        uint64_t root_offset,
                        ExtentSourcePtr source);

        /** The full posting list (sorted ascending) for one extracted term, or empty if the term
            has no entry at all. */
        std::vector<uint64_t> lookup(std::span<const char> term) const;

        /** Real posting-list intersection (architecture.md Sec 5.2) across every one of `terms`
            -- not full-scan-plus-dedupe. Empty if `terms` is empty or any one term has no entry
            at all (an AND where one operand contributes zero matches is zero matches overall,
            exactly like a real multi-term GIN AND scan). */
        std::vector<uint64_t> search_and(const std::vector<std::vector<char>> &terms) const;

        bool empty() const;

        uint64_t get_xid() const {
            return _xid;
        }

        uint64_t get_root_offset() const {
            return _root_offset;
        }

        std::filesystem::path get_file_path() const {
            return _file;
        }

        /** Mirrors IndexReader::set_root_and_xid()/GistReader::set_root_and_xid() exactly. */
        void set_root_and_xid(uint64_t new_xid, uint64_t new_root_offset) {
            _xid = new_xid;
            _root_offset = new_root_offset;
            _entry_tree.set_root_and_xid(new_xid, new_root_offset);
        }

        /** Every (term, posting list) pair currently in the entry tree -- a plain unfiltered
            scan, for tests that need to see the whole structure without going through
            lookup()/search_and() (mirrors GistReader::all_leaves()'s identical role). Not a
            production read path. */
        std::vector<std::pair<std::vector<char>, std::vector<uint64_t>>> debug_all_terms() const;

        /** True if `term`'s entry currently holds a pointer to a separate posting tree rather
            than an inline posting list; false if the term has no entry at all. Lets tests verify
            promotion by inspecting the entry's leaf value directly (this phase's own acceptance
            criterion), not just by behavior. Not a production read path. */
        bool debug_is_pointer(std::span<const char> term) const;

    private:
        TuplePtr _make_term_key_tuple(std::span<const char> term) const;
        std::vector<uint64_t> _read_posting_list(const Extent::Row &entry_row, std::span<const char> term) const;

        uint64_t _database_id;
        std::filesystem::path _file;
        uint64_t _xid;
        uint64_t _root_offset;
        ExtentSourcePtr _source;

        ExtentSchemaPtr _entry_schema;
        FieldPtr _entry_term_f;
        FieldPtr _entry_posting_f;

        ExtentSchemaPtr _posting_schema;
        FieldPtr _posting_id_f;

        IndexReader<BtreeReadStrategy> _entry_tree;
    };

    using GinReaderPtr = std::shared_ptr<GinReadStrategy>;
}
