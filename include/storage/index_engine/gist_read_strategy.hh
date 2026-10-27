#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <storage/extent.hh>
#include <storage/index_engine/gist_helpers.hh>
#include <storage/schema.hh>

namespace springtail {
    /**
     * GiST's ReadStrategy-equivalent (mirrors gist_write_strategy.hh's framing). Deliberately does
     * **not** implement the `ReadStrategy` concept (read_strategy.hh) and is not the `Strategy`
     * type parameter of `IndexReader<Strategy>` -- see gist_write_strategy.hh's header comment for
     * the full reasoning (ported here: `IndexReader<Strategy>::Iterator` assumes a single linear
     * root-to-leaf path and a total key order; GiST's `consistent`-driven predicate search can
     * legitimately fan out into *multiple* children at a branch whose overlapping bounding boxes
     * all qualify, which does not fit a single-frame-stack iterator at all). `GistReader`
     * (gist_reader.hh) is GiST's own engine, with its own recursive, fan-out-capable search.
     */
    class GistReadStrategy {
    public:
        explicit GistReadStrategy(std::vector<std::string> opclass_names)
            : _opclass_names(std::move(opclass_names))
        { }

        size_t num_columns() const {
            return _opclass_names.size();
        }

        const std::vector<std::string> &opclass_names() const {
            return _opclass_names;
        }

        gist_helpers::GistEntry read_leaf(const Extent::Row &row, ExtentSchemaPtr schema) const {
            return gist_helpers::read_leaf_entry_from_row(row, schema, _opclass_names);
        }

        gist_helpers::GistEntry read_branch(const Extent::Row &row, ExtentSchemaPtr schema) const {
            return gist_helpers::read_branch_entry_from_row(row, schema, _opclass_names);
        }

        /** GIST_CONSISTENT for indexed column `col`: does `entry` satisfy `query` under
            `strategy`? The only opclass support function the read side ever calls -- descent and
            leaf matching both reduce to this one predicate (real GiST has no separate "choose
            child" primitive for search, unlike insertion's penalty-driven choose_subtree).
            `recheck_out` forwards gist_helpers::consistent()'s own recheck out-param (round-1
            review of this phase, finding 3) -- see that function's doc comment; unused by this
            phase's own callers (GistReader::search() passes nullptr), kept so the value is
            retrievable rather than silently dropped. */
        bool consistent(const gist_helpers::GistEntry &entry, int col, const std::string &query_bytes,
                        uint16_t strategy, bool is_leaf, bool *recheck_out = nullptr) const {
            return gist_helpers::consistent(entry, col, query_bytes, strategy, is_leaf, _opclass_names, recheck_out);
        }

    private:
        std::vector<std::string> _opclass_names;
    };
}
