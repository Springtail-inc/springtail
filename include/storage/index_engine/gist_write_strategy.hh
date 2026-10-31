#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include <storage/extent.hh>
#include <storage/index_engine/gist_helpers.hh>
#include <storage/schema.hh>

namespace springtail {
    /**
     * GiST's WriteStrategy-equivalent (architecture.md "btree" redesign Sec 5.1; Decisions log
     * entry 3: each index kind gets its own concrete strategy interface modeled on Postgres's own
     * access-method callback names, not forced into btree's `choose_child`/`split_point` shapes).
     *
     * Deliberately does **not** implement the `WriteStrategy` concept (write_strategy.hh) and is
     * **not** the `Strategy` type parameter of `IndexWriter<Strategy>` -- phases/phase-10.md's
     * Design notes records why concretely: `IndexWriter<Strategy>`'s own (not just
     * `BtreeWriteStrategy`'s) `.cc` logic assumes a total order over leaf/branch rows
     * (`_insert_row`'s sorted-position insert via `Tuple::less_than`, `_propagate_key_increase`'s
     * "only grows" comparison) and a positional split (`_split_node`'s `copy_range(... split_at
     * ...)`). GiST has neither: leaf/branch entries are an unordered bag, and `picksplit` produces
     * an arbitrary two-way *partition* of entries, not a row index. Reusing `IndexWriter<Strategy>`
     * would mean conditionalizing most of its mutation/split bodies on "is this strategy ordered",
     * which is a bigger, riskier change to already-reviewed-clean phase 1-9 code than this phase's
     * brief scopes. GiST therefore gets its own engine, `GistWriter` (gist_writer.hh) -- this class
     * is the opclass-calling glue it drives, reusing only the mechanical primitives
     * (`IndexExtentCache`, `ExtentSource`, `SegmentSet`, the on-disk extent format) that
     * `IndexWriter<Strategy>` also reuses.
     *
     * Method names mirror Postgres's own GiST access-method callback names (`choose_subtree`,
     * `penalty`, `union_entries`, `picksplit`) rather than btree's `choose_child`/`split_point`
     * vocabulary, per Decisions log entry 3.
     */
    class GistWriteStrategy {
    public:
        /** Default byte-size split threshold, mirroring BtreeWriteStrategy's
            DEFAULT_UNDERFULL_FRACTION-adjacent convention: a tunable, not a derived value. */
        static constexpr uint64_t DEFAULT_MAX_EXTENT_SIZE = 256 * 1024;

        GistWriteStrategy(std::vector<std::string> opclass_names,
                          uint64_t max_extent_size = DEFAULT_MAX_EXTENT_SIZE)
            : _opclass_names(std::move(opclass_names)),
              _max_extent_size(max_extent_size)
        { }

        size_t num_columns() const {
            return _opclass_names.size();
        }

        const std::vector<std::string> &opclass_names() const {
            return _opclass_names;
        }

        gist_helpers::GistEntry compress_leaf(TuplePtr value) const {
            return gist_helpers::compress_leaf_entry(value, _opclass_names);
        }

        gist_helpers::GistEntry read_leaf(const Extent::Row &row, ExtentSchemaPtr schema) const {
            return gist_helpers::read_leaf_entry_from_row(row, schema, _opclass_names);
        }

        gist_helpers::GistEntry read_branch(const Extent::Row &row, ExtentSchemaPtr schema) const {
            return gist_helpers::read_branch_entry_from_row(row, schema, _opclass_names);
        }

        void write_leaf(Extent::Row &row, const gist_helpers::GistEntry &entry, ExtentSchemaPtr schema) const {
            gist_helpers::write_leaf_row_from_entry(row, entry, schema, _opclass_names);
        }

        void write_branch(Extent::Row &row, const gist_helpers::GistEntry &key, uint64_t child_id,
                          ExtentSchemaPtr schema) const {
            gist_helpers::write_branch_row_from_union(row, key, child_id, schema, _opclass_names);
        }

        /** GIST_PENALTY: cost of expanding `existing` to also cover `incoming`. Used by
            choose_subtree (real GiST's gistchoose(), not GIST_CONSISTENT -- consistent is
            exclusively a read-side search predicate, see gist_read_strategy.hh) to pick which
            branch entry to descend into: the one with the lowest penalty. */
        double penalty(const gist_helpers::GistEntry &existing, const gist_helpers::GistEntry &incoming) const {
            return gist_helpers::compute_penalty(existing, incoming, _opclass_names);
        }

        /** GIST_UNION: the smallest entry covering every one of `entries`. */
        gist_helpers::GistEntry union_entries(const std::vector<gist_helpers::GistEntry> &entries) const {
            return gist_helpers::compute_union(entries, _opclass_names);
        }

        /** GIST_PICKSPLIT: partitions `entries`' indices into `left`/`right` -- an arbitrary
            two-way partition, never a positional split point (see this class's header comment). */
        void picksplit(const std::vector<gist_helpers::GistEntry> &entries,
                       std::vector<size_t> &left, std::vector<size_t> &right) const {
            gist_helpers::compute_picksplit(entries, left, right, _opclass_names);
        }

        /** True once `extent` has grown enough that it must be split before another mutation --
            the same byte-size threshold rule BtreeWriteStrategy::needs_split() uses; GiST has no
            btree-style "split at the midpoint" rule to pair it with, only picksplit(). */
        bool needs_split(std::shared_ptr<Extent> extent) const {
            return extent->byte_count() >= _max_extent_size && extent->row_count() > 1;
        }

    private:
        std::vector<std::string> _opclass_names;
        uint64_t _max_extent_size;
    };
}
