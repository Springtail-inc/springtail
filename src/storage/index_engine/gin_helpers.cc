#include <xxhash.h>

#include <absl/log/check.h>
#include <fmt/format.h>

#include <common/constants.hh>
#include <storage/index_engine/gin_helpers.hh>

namespace springtail::gin_helpers {

    namespace {
        void put_varint(std::vector<char> &out, uint64_t v) {
            while (v >= 0x80) {
                out.push_back(static_cast<char>((v & 0x7f) | 0x80));
                v >>= 7;
            }
            out.push_back(static_cast<char>(v));
        }

        /** Reads one varint starting at bytes[pos], advancing pos past it. */
        uint64_t get_varint(std::span<const char> bytes, size_t &pos) {
            uint64_t v = 0;
            int shift = 0;
            while (true) {
                CHECK_LT(pos, bytes.size()) << "gin_helpers: truncated varint";
                uint8_t byte = static_cast<uint8_t>(bytes[pos++]);
                v |= static_cast<uint64_t>(byte & 0x7f) << shift;
                if (!(byte & 0x80)) {
                    break;
                }
                shift += 7;
            }
            return v;
        }

        void put_u64le(std::vector<char> &out, uint64_t v) {
            for (int i = 0; i < 8; ++i) {
                out.push_back(static_cast<char>((v >> (8 * i)) & 0xff));
            }
        }

        uint64_t get_u64le(std::span<const char> bytes, size_t pos) {
            uint64_t v = 0;
            for (int i = 0; i < 8; ++i) {
                v |= static_cast<uint64_t>(static_cast<uint8_t>(bytes[pos + i])) << (8 * i);
            }
            return v;
        }
    } // namespace

    std::vector<char> encode_posting_list(const std::vector<uint64_t> &sorted_unique_ids) {
        std::vector<char> out;
        put_varint(out, sorted_unique_ids.size());

        uint64_t prev = 0;
        for (size_t i = 0; i < sorted_unique_ids.size(); ++i) {
            uint64_t id = sorted_unique_ids[i];
            CHECK(i == 0 || id > prev) << "gin_helpers: posting list must be sorted, duplicate-free";
            put_varint(out, (i == 0) ? id : (id - prev));
            prev = id;
        }
        return out;
    }

    std::vector<uint64_t> decode_posting_list(std::span<const char> bytes) {
        size_t pos = 0;
        uint64_t count = get_varint(bytes, pos);

        std::vector<uint64_t> out;
        out.reserve(count);

        uint64_t prev = 0;
        for (uint64_t i = 0; i < count; ++i) {
            uint64_t delta = get_varint(bytes, pos);
            uint64_t id = (i == 0) ? delta : (prev + delta);
            out.push_back(id);
            prev = id;
        }
        return out;
    }

    std::vector<char> encode_entry_value_inline(const std::vector<uint64_t> &sorted_unique_ids) {
        std::vector<char> out;
        out.push_back(ENTRY_VALUE_INLINE);
        auto posting = encode_posting_list(sorted_unique_ids);
        out.insert(out.end(), posting.begin(), posting.end());
        return out;
    }

    std::vector<char> encode_entry_value_pointer(uint64_t posting_tree_root) {
        std::vector<char> out;
        out.push_back(ENTRY_VALUE_POINTER);
        put_u64le(out, posting_tree_root);
        return out;
    }

    DecodedEntryValue decode_entry_value(std::span<const char> bytes) {
        CHECK(!bytes.empty()) << "gin_helpers: empty entry posting value";

        DecodedEntryValue out;
        char tag = bytes[0];
        std::span<const char> payload = bytes.subspan(1);

        if (tag == ENTRY_VALUE_POINTER) {
            out.is_pointer = true;
            CHECK_EQ(payload.size(), 8u) << "gin_helpers: malformed pointer entry value";
            out.posting_tree_root = get_u64le(payload, 0);
        } else {
            CHECK_EQ(tag, ENTRY_VALUE_INLINE) << "gin_helpers: unrecognized entry value tag";
            out.is_pointer = false;
            out.inline_ids = decode_posting_list(payload);
        }
        return out;
    }

    ExtentSchemaPtr build_entry_leaf_schema() {
        std::vector<SchemaColumn> cols;
        // pkey_position=0 marks "__gin_term" as this schema's (only) sort key -- IndexWriter<
        // BtreeWriteStrategy>::insert()/_insert_row() read ExtentSchema::get_sort_keys()/
        // get_sort_fields() directly (not just the `keys` vector passed to IndexWriter's own
        // constructor), so a schema built without this would compare/insert rows using an empty
        // sort-key set -- confirmed directly by a CHECK failure in Tuple::_equal() while writing
        // this file's own tests, not guessed.
        cols.emplace_back("__gin_term", 0, SchemaType::BINARY, 0, false, 0u);
        cols.emplace_back("__gin_posting", 1, SchemaType::BINARY, 0, false);
        // include_internal_row_id=false: see this schema's own header comment -- row identity
        // lives inside the posting payload, never as a side column here.
        return std::make_shared<ExtentSchema>(cols, ExtensionCallback{}, false, false);
    }

    ExtentSchemaPtr build_posting_leaf_schema() {
        std::vector<SchemaColumn> cols;
        // pkey_position=0: see build_entry_leaf_schema()'s identical note above.
        cols.emplace_back("__gin_posting_id", 0, SchemaType::UINT64, 0, false, 0u);
        return std::make_shared<ExtentSchema>(cols, ExtensionCallback{}, false, false);
    }

    std::filesystem::path derive_posting_tree_path(const std::filesystem::path &entry_tree_file,
                                                    std::span<const char> term) {
        uint64_t hash = XXH64(term.data(), term.size(), 0);
        auto dir = entry_tree_file.parent_path();
        auto stem = entry_tree_file.filename().string();
        return dir / fmt::format("{}.posting.{:016x}", stem, hash);
    }

} // namespace springtail::gin_helpers
