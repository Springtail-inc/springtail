#include <algorithm>
#include <set>

#include <absl/log/check.h>

#include <storage/index_engine/gin_read_strategy.hh>

namespace springtail {
    using gin_helpers::DecodedEntryValue;

    GinReadStrategy::GinReadStrategy(uint64_t database_id,
                                     std::filesystem::path file,
                                     uint64_t xid,
                                     uint64_t root_offset,
                                     ExtentSourcePtr source)
        : _database_id(database_id),
          _file(std::move(file)),
          _xid(xid),
          _root_offset(root_offset),
          _source(std::move(source)),
          _entry_schema(gin_helpers::build_entry_leaf_schema()),
          _posting_schema(gin_helpers::build_posting_leaf_schema()),
          _entry_tree(database_id, _file, { "__gin_term" }, _entry_schema, xid, root_offset, _source,
                     BtreeReadStrategy{})
    {
        _entry_term_f = _entry_schema->get_field("__gin_term");
        _entry_posting_f = _entry_schema->get_field("__gin_posting");
        _posting_id_f = _posting_schema->get_field("__gin_posting_id");
    }

    TuplePtr GinReadStrategy::_make_term_key_tuple(std::span<const char> term) const {
        auto key = std::make_shared<FieldArray>();
        key->push_back(std::make_shared<ConstTypeField<std::vector<char>>>(
                std::vector<char>(term.begin(), term.end())));
        auto value = std::make_shared<FieldArray>();
        return std::make_shared<KeyValueTuple>(key, value, nullptr);
    }

    std::vector<uint64_t>
    GinReadStrategy::_read_posting_list(const Extent::Row &entry_row, std::span<const char> term) const {
        auto posting_bytes = _entry_posting_f->get_binary(&entry_row);
        auto decoded = gin_helpers::decode_entry_value(
                std::span<const char>(posting_bytes.data(), posting_bytes.size()));

        if (!decoded.is_pointer) {
            return decoded.inline_ids;
        }

        auto path = gin_helpers::derive_posting_tree_path(_file, term);
        IndexReader<BtreeReadStrategy> posting_tree(_database_id, path,
                std::vector<std::string>{ "__gin_posting_id" }, _posting_schema, _xid,
                decoded.posting_tree_root, _source, BtreeReadStrategy{});

        std::vector<uint64_t> ids;
        for (auto it = posting_tree.begin(); it != posting_tree.end(); ++it) {
            ids.push_back(_posting_id_f->get_uint64(&(*it)));
        }
        return ids;
    }

    std::vector<uint64_t> GinReadStrategy::lookup(std::span<const char> term) const {
        auto key_tuple = _make_term_key_tuple(term);
        auto it = _entry_tree.find(key_tuple);
        if (it == _entry_tree.end()) {
            return {};
        }
        return _read_posting_list(*it, term);
    }

    std::vector<uint64_t>
    GinReadStrategy::search_and(const std::vector<std::vector<char>> &terms) const {
        if (terms.empty()) {
            return {};
        }

        // Start from the smallest term's posting list -- a cheap, standard merge-intersection
        // optimization (fewer candidates to re-check against the remaining terms), not required
        // for correctness.
        std::vector<std::vector<uint64_t>> lists;
        lists.reserve(terms.size());
        for (auto &term : terms) {
            auto ids = lookup(term);
            if (ids.empty()) {
                return {};
            }
            lists.push_back(std::move(ids));
        }
        std::ranges::sort(lists, [](auto &a, auto &b) { return a.size() < b.size(); });

        std::vector<uint64_t> result = lists[0];
        for (size_t i = 1; i < lists.size() && !result.empty(); ++i) {
            std::vector<uint64_t> next;
            std::ranges::set_intersection(result, lists[i], std::back_inserter(next));
            result = std::move(next);
        }
        return result;
    }

    bool GinReadStrategy::empty() const {
        return _entry_tree.empty();
    }

    bool GinReadStrategy::debug_is_pointer(std::span<const char> term) const {
        auto key_tuple = _make_term_key_tuple(term);
        auto it = _entry_tree.find(key_tuple);
        if (it == _entry_tree.end()) {
            return false;
        }
        auto posting_bytes = _entry_posting_f->get_binary(&(*it));
        auto decoded = gin_helpers::decode_entry_value(
                std::span<const char>(posting_bytes.data(), posting_bytes.size()));
        return decoded.is_pointer;
    }

    std::vector<std::pair<std::vector<char>, std::vector<uint64_t>>>
    GinReadStrategy::debug_all_terms() const {
        std::vector<std::pair<std::vector<char>, std::vector<uint64_t>>> out;
        for (auto it = _entry_tree.begin(); it != _entry_tree.end(); ++it) {
            auto term_bytes = _entry_term_f->get_binary(&(*it));
            std::vector<char> term(term_bytes.begin(), term_bytes.end());
            out.emplace_back(term, _read_posting_list(*it, term));
        }
        return out;
    }
}
