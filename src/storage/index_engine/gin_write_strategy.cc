#include <algorithm>

#include <xxhash.h>

#include <absl/log/check.h>

#include <common/constants.hh>
#include <common/exception.hh>
#include <storage/index_engine/gin_write_strategy.hh>

namespace springtail {
    using gin_helpers::DecodedEntryValue;

    namespace {
        uint64_t term_hash(const std::vector<char> &term) {
            return XXH64(term.data(), term.size(), 0);
        }
    } // namespace

    GinWriteStrategy::GinWriteStrategy(uint64_t database_id,
                                       std::filesystem::path file,
                                       uint64_t xid,
                                       std::vector<std::string> opclass_names,
                                       uint64_t max_extent_size,
                                       std::shared_ptr<IndexExtentCache> cache)
        : _database_id(database_id),
          _file(std::move(file)),
          _xid(xid),
          _opclass_names(std::move(opclass_names)),
          _max_extent_size(max_extent_size),
          _spill_threshold_bytes(max_extent_size / 4),
          _cache(std::move(cache)),
          _entry_schema(gin_helpers::build_entry_leaf_schema()),
          _posting_schema(gin_helpers::build_posting_leaf_schema()),
          _entry_tree(database_id, _file, { "__gin_term" }, _entry_schema, xid, _cache,
                     BtreeWriteStrategy{ max_extent_size })
    {
        // GIN's single-column scope limit, mirroring gist_helpers.hh's identical one for GiST
        // (see gin_write_strategy.hh's class comment): the opclass_names vector exists only to
        // keep this constructor's call shape identical to GistWriter's at the dispatch sites
        // (both take a vector sourced from _get_index_opclasses()), not because this phase
        // supports multi-column GIN. A thrown Error, not a CHECK -- this constructor runs on a
        // real DDL path (Indexer::_build(), inside its own try/catch) with no upstream guard
        // against someone issuing `CREATE INDEX ... USING gin(a, b)`; a CHECK here would crash
        // the daemon on an otherwise-ordinary (if unsupported) request, exactly the class of bug
        // this project's own "a throwing validator is only as safe as its least-protected
        // caller" lesson is about.
        if (_opclass_names.size() != 1u) {
            throw Error("GinWriteStrategy: only single-column GIN indexes are supported in this "
                        "phase, got " + std::to_string(_opclass_names.size()) + " columns");
        }

        _entry_term_f = _entry_schema->get_field("__gin_term");
        _entry_posting_f = _entry_schema->get_field("__gin_posting");
        _posting_id_f = _posting_schema->get_field("__gin_posting_id");
    }

    void GinWriteStrategy::init_empty() {
        ReentrancyGuard guard(_in_call);
        _entry_tree.init_empty();
    }

    void GinWriteStrategy::init(uint64_t root_offset) {
        ReentrancyGuard guard(_in_call);
        _entry_tree.init(root_offset);
    }

    TuplePtr GinWriteStrategy::_make_term_key_tuple(const std::vector<char> &term) const {
        auto key = std::make_shared<FieldArray>();
        key->push_back(std::make_shared<ConstTypeField<std::vector<char>>>(term));
        auto value = std::make_shared<FieldArray>();
        return std::make_shared<KeyValueTuple>(key, value, nullptr);
    }

    TuplePtr GinWriteStrategy::_make_entry_value_tuple(const std::vector<char> &term,
                                                       const std::vector<char> &posting_value) const {
        auto key = std::make_shared<FieldArray>();
        key->push_back(std::make_shared<ConstTypeField<std::vector<char>>>(term));
        auto value = std::make_shared<FieldArray>();
        value->push_back(std::make_shared<ConstTypeField<std::vector<char>>>(posting_value));
        return std::make_shared<KeyValueTuple>(key, value, nullptr);
    }

    TuplePtr GinWriteStrategy::_make_posting_id_tuple(uint64_t id) const {
        auto key = std::make_shared<FieldArray>();
        key->push_back(std::make_shared<ConstTypeField<uint64_t>>(id));
        auto value = std::make_shared<FieldArray>();
        return std::make_shared<KeyValueTuple>(key, value, nullptr);
    }

    bool GinWriteStrategy::_entry_exact_match(const IndexWriter<BtreeWriteStrategy>::Iterator &it,
                                              const std::vector<char> &term) {
        if (it == _entry_tree.end()) {
            return false;
        }
        auto bytes = _entry_term_f->get_binary(&(*it));
        return bytes.size() == term.size() && std::equal(bytes.begin(), bytes.end(), term.begin());
    }

    DecodedEntryValue GinWriteStrategy::_decode_entry_at(const IndexWriter<BtreeWriteStrategy>::Iterator &it) const {
        auto bytes = _entry_posting_f->get_binary(&(*it));
        return gin_helpers::decode_entry_value(std::span<const char>(bytes.data(), bytes.size()));
    }

    bool GinWriteStrategy::_posting_contains(IndexWriter<BtreeWriteStrategy> &pt, uint64_t id) {
        auto key = _make_posting_id_tuple(id);
        auto it = pt.lower_bound(key, false);
        if (it == pt.end()) {
            return false;
        }
        return _posting_id_f->get_uint64(&(*it)) == id;
    }

    IndexWriter<BtreeWriteStrategy> &
    GinWriteStrategy::_create_posting_tree(const std::vector<char> &term) {
        uint64_t hash = term_hash(term);
        CHECK(!_posting_trees.contains(hash)) << "gin: posting tree already open for this term";

        auto path = gin_helpers::derive_posting_tree_path(_file, term);
        CHECK(!std::filesystem::exists(path))
                << "gin: posting-tree file collision creating a new posting tree at " << path;

        auto writer = std::make_shared<IndexWriter<BtreeWriteStrategy>>(
                _database_id, path, std::vector<std::string>{ "__gin_posting_id" }, _posting_schema,
                _xid, _cache, BtreeWriteStrategy{ _max_extent_size });
        writer->init_empty();

        auto [emplaced_it, inserted] = _posting_trees.emplace(hash, PostingTreeEntry{ term, writer });
        CHECK(inserted);
        return *emplaced_it->second.writer;
    }

    IndexWriter<BtreeWriteStrategy> &
    GinWriteStrategy::_open_posting_tree(const std::vector<char> &term, uint64_t existing_root) {
        uint64_t hash = term_hash(term);
        auto found = _posting_trees.find(hash);
        if (found != _posting_trees.end()) {
            return *found->second.writer;
        }

        auto path = gin_helpers::derive_posting_tree_path(_file, term);
        auto writer = std::make_shared<IndexWriter<BtreeWriteStrategy>>(
                _database_id, path, std::vector<std::string>{ "__gin_posting_id" }, _posting_schema,
                _xid, _cache, BtreeWriteStrategy{ _max_extent_size });
        writer->init(existing_root);

        auto [emplaced_it, inserted] = _posting_trees.emplace(hash, PostingTreeEntry{ term, writer });
        CHECK(inserted);
        return *emplaced_it->second.writer;
    }

    void GinWriteStrategy::_mark_posting_dirty(const std::vector<char> &term) {
        _dirty_posting_terms.emplace(term_hash(term), term);
    }

    void GinWriteStrategy::_upsert_term(const std::vector<char> &term, uint64_t row_id) {
        auto key_tuple = _make_term_key_tuple(term);
        auto it = _entry_tree.lower_bound(key_tuple, false);

        if (!_entry_exact_match(it, term)) {
            // Brand-new term: a fresh single-id inline posting list.
            auto value = gin_helpers::encode_entry_value_inline({ row_id });
            _entry_tree.insert(_make_entry_value_tuple(term, value));
            return;
        }

        auto decoded = _decode_entry_at(it);

        if (decoded.is_pointer) {
            auto &pt = _open_posting_tree(term, decoded.posting_tree_root);
            if (!_posting_contains(pt, row_id)) {
                pt.insert(_make_posting_id_tuple(row_id));
                _mark_posting_dirty(term);
            }
            return;
        }

        auto &ids = decoded.inline_ids;
        auto pos = std::ranges::lower_bound(ids, row_id);
        if (pos != ids.end() && *pos == row_id) {
            // Already indexed for this term -- nothing to do.
            return;
        }
        ids.insert(pos, row_id);

        auto new_value = gin_helpers::encode_entry_value_inline(ids);

        // Open question (d), resolved concretely: the inline posting list spills to a posting
        // tree once the *entry row's own total encoded size* (term bytes + posting value) would
        // exceed a quarter of this tree's leaf extent size budget -- the same budget
        // BtreeWriteStrategy::needs_split() enforces for the whole extent, kept well under it so
        // several entries can still coexist on one leaf page rather than one large term forcing
        // constant single-entry splits.
        if (term.size() + new_value.size() <= _spill_threshold_bytes) {
            _entry_tree.remove(key_tuple);
            _entry_tree.insert(_make_entry_value_tuple(term, new_value));
            return;
        }

        auto &pt = _create_posting_tree(term);
        for (uint64_t id : ids) {
            pt.insert(_make_posting_id_tuple(id));
        }
        _mark_posting_dirty(term);

        // The real root offset is only known once this posting tree is flushed, which only
        // happens at finalize() -- see finalize()'s own comment. A placeholder here is never
        // read mid-generation: every lookup for this term this generation goes through
        // _posting_trees (keyed by term hash), not through decoding this row back off disk.
        _entry_tree.remove(key_tuple);
        _entry_tree.insert(_make_entry_value_tuple(term, gin_helpers::encode_entry_value_pointer(0)));
    }

    void GinWriteStrategy::_remove_term(const std::vector<char> &term, uint64_t row_id) {
        auto key_tuple = _make_term_key_tuple(term);
        auto it = _entry_tree.lower_bound(key_tuple, false);

        if (!_entry_exact_match(it, term)) {
            return;
        }

        auto decoded = _decode_entry_at(it);

        if (decoded.is_pointer) {
            auto &pt = _open_posting_tree(term, decoded.posting_tree_root);
            if (_posting_contains(pt, row_id)) {
                pt.remove(_make_posting_id_tuple(row_id));
                _mark_posting_dirty(term);
            }
            // Deliberately never demoted back to an inline list even if it shrinks (possibly to
            // empty) -- see this class's header comment/phases/phase-11.md's Design notes: real
            // GIN doesn't demote a posting tree either, and doing so here would mean reasoning
            // about an entry-tree row rewrite racing the very upsert path this removal might be
            // interleaved with across separate calls within the same generation.
            return;
        }

        auto &ids = decoded.inline_ids;
        auto pos = std::ranges::lower_bound(ids, row_id);
        if (pos == ids.end() || *pos != row_id) {
            return;
        }
        ids.erase(pos);

        _entry_tree.remove(key_tuple);
        if (!ids.empty()) {
            _entry_tree.insert(_make_entry_value_tuple(term, gin_helpers::encode_entry_value_inline(ids)));
        }
        // else: the term's posting list is now empty -- the entry row simply stays removed,
        // matching real GIN's own "an entry with no postings doesn't exist" behavior.
    }

    void GinWriteStrategy::insert(TuplePtr value) {
        ReentrancyGuard guard(_in_call);

        uint64_t row_id = value->field(1)->get_uint64(value->row());
        auto terms = gin_helpers::extract_terms(value, _opclass_names[0]);
        for (auto &term : terms) {
            _upsert_term(term, row_id);
        }
    }

    void GinWriteStrategy::remove(TuplePtr value) {
        ReentrancyGuard guard(_in_call);

        uint64_t row_id = value->field(1)->get_uint64(value->row());
        auto terms = gin_helpers::extract_terms(value, _opclass_names[0]);
        for (auto &term : terms) {
            _remove_term(term, row_id);
        }
    }

    void GinWriteStrategy::truncate() {
        ReentrancyGuard guard(_in_call);
        _entry_tree.truncate();
        // Discards every posting tree this generation touched -- their files (if newly created
        // this generation) become orphans, exactly like an interrupted ordinary IndexWriter
        // generation leaves dead bytes in its own file until a (not-built-for-GIN-in-this-phase)
        // background reclaim pass -- see phases/phase-11.md's handoff notes.
        _posting_trees.clear();
        _dirty_posting_terms.clear();
    }

    uint64_t GinWriteStrategy::finalize(bool fs_sync) {
        ReentrancyGuard guard(_in_call);

        // Posting trees must be flushed *before* the entry tree, and each dirty term's entry row
        // rewritten with that posting tree's real (now-known) root id *before* the entry tree
        // itself flushes -- a posting tree's root is only a stable, persistable number once
        // finalize() has run on it (every id before that is a TEMP_ID_BASE-range placeholder,
        // see index_writer.hh), so writing it into the entry row any earlier would persist a
        // value no future generation could resolve.
        for (auto &[hash, term] : _dirty_posting_terms) {
            auto &pt = *_posting_trees.at(hash).writer;
            uint64_t real_root = pt.finalize(fs_sync);

            auto key_tuple = _make_term_key_tuple(term);
            auto it = _entry_tree.lower_bound(key_tuple, false);
            CHECK(_entry_exact_match(it, term))
                    << "gin: dirty posting-tree term has no entry row at finalize()";

            _entry_tree.remove(key_tuple);
            _entry_tree.insert(_make_entry_value_tuple(term, gin_helpers::encode_entry_value_pointer(real_root)));
        }
        _dirty_posting_terms.clear();

        return _entry_tree.finalize(fs_sync);
    }

    bool GinWriteStrategy::empty() {
        return _entry_tree.empty();
    }

    void GinWriteStrategy::sync() {
        ReentrancyGuard guard(_in_call);
        _entry_tree.sync();
        for (auto &[hash, entry] : _posting_trees) {
            entry.writer->sync();
        }
    }

    std::filesystem::path GinWriteStrategy::get_file_path() const {
        return _entry_tree.get_file_path();
    }

    std::vector<std::filesystem::path> GinWriteStrategy::get_segment_paths() const {
        std::vector<std::filesystem::path> out = _entry_tree.get_segment_paths();
        for (auto &[hash, entry] : _posting_trees) {
            auto segs = entry.writer->get_segment_paths();
            out.insert(out.end(), segs.begin(), segs.end());
        }
        return out;
    }
}
