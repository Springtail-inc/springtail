#include <fmt/format.h>

#include <common/constants.hh>
#include <storage/field.hh>
#include <storage/index_engine/gist_helpers.hh>

/**
 * This file holds only the opclass-independent, pure-C++ half of gist_helpers: row <-> GistEntry
 * marshalling and schema construction. The opclass-CALLING half (compress/penalty/union/
 * picksplit/consistent, and everything pg_ext-dependent they need -- dlopen'd .so calls,
 * FmgrInfo, the fake GIST page, the pure-C++ reference opclass) lives in the sibling
 * gist_helpers_opclass.cc instead, deliberately -- see that file's header comment and
 * src/storage/CMakeLists.txt's springtail_opclass_engine comment for why: this file (and
 * gist_reader.cc's own pure half) must stay link-safe for springtail_pg_fdw.so, which dlopens
 * into a real, unmodified Postgres backend process and must never pull in pg_ext's symbols
 * (confirmed directly: doing so once broke CREATE EXTENSION springtail_fdw with a backend
 * segfault, via symbol collision with the real backend's own palloc/elog/etc).
 */
namespace springtail::gist_helpers {

    GistEntry read_leaf_entry_from_row(const Extent::Row &row, ExtentSchemaPtr schema,
                                        const std::vector<std::string> &opclass_names) {
        GistEntry out;
        out.is_leaf = true;
        out.keys.resize(opclass_names.size());

        auto fields = schema->get_fields();
        for (size_t col = 0; col < opclass_names.size(); ++col) {
            auto span = fields->at(static_cast<int>(col))->get_binary(&row);
            out.keys[col].assign(span.data(), span.size());
        }
        out.internal_row_id = fields->at(static_cast<int>(opclass_names.size()))->get_uint64(&row);

        return out;
    }

    GistEntry read_branch_entry_from_row(const Extent::Row &row, ExtentSchemaPtr schema,
                                          const std::vector<std::string> &opclass_names) {
        GistEntry out;
        out.is_leaf = false;
        out.keys.resize(opclass_names.size());

        auto fields = schema->get_fields();
        for (size_t col = 0; col < opclass_names.size(); ++col) {
            auto span = fields->at(static_cast<int>(col))->get_binary(&row);
            out.keys[col].assign(span.data(), span.size());
        }
        out.child_id = fields->at(static_cast<int>(opclass_names.size()))->get_uint64(&row);

        return out;
    }

    void write_leaf_row_from_entry(Extent::Row &row, const GistEntry &entry, ExtentSchemaPtr schema,
                                    const std::vector<std::string> &opclass_names) {
        auto fields = schema->get_mutable_fields();
        for (size_t col = 0; col < opclass_names.size(); ++col) {
            fields->at(static_cast<int>(col))->set_binary(
                    &row, std::span<const char>(entry.keys[col].data(), entry.keys[col].size()));
        }
        fields->at(static_cast<int>(opclass_names.size()))->set_uint64(&row, entry.internal_row_id);
    }

    void write_branch_row_from_union(Extent::Row &row, const GistEntry &key, uint64_t child_id,
                                      ExtentSchemaPtr schema, const std::vector<std::string> &opclass_names) {
        auto fields = schema->get_mutable_fields();
        for (size_t col = 0; col < opclass_names.size(); ++col) {
            fields->at(static_cast<int>(col))->set_binary(
                    &row, std::span<const char>(key.keys[col].data(), key.keys[col].size()));
        }
        fields->at(static_cast<int>(opclass_names.size()))->set_uint64(&row, child_id);
    }

    ExtentSchemaPtr build_leaf_schema(const std::vector<std::string> &opclass_names) {
        std::vector<SchemaColumn> cols;
        for (size_t i = 0; i < opclass_names.size(); ++i) {
            cols.emplace_back(fmt::format("__gist_key_{}", i), static_cast<int32_t>(i), SchemaType::BINARY, 0, false);
        }
        cols.emplace_back(constant::INTERNAL_ROW_ID, static_cast<int32_t>(opclass_names.size()),
                          SchemaType::UINT64, 0, false);
        // include_internal_row_id=false: the trailing UINT64 column above already is our id
        // column (named __internal_row_id for a leaf) -- adding a second one would be wrong.
        return std::make_shared<ExtentSchema>(cols, ExtensionCallback{}, false, false);
    }

    ExtentSchemaPtr build_branch_schema(const std::vector<std::string> &opclass_names) {
        std::vector<SchemaColumn> cols;
        for (size_t i = 0; i < opclass_names.size(); ++i) {
            cols.emplace_back(fmt::format("__gist_key_{}", i), static_cast<int32_t>(i), SchemaType::BINARY, 0, false);
        }
        cols.emplace_back(constant::BTREE_CHILD_FIELD, static_cast<int32_t>(opclass_names.size()),
                          SchemaType::UINT64, 0, false);
        return std::make_shared<ExtentSchema>(cols, ExtensionCallback{}, false, false);
    }

} // namespace springtail::gist_helpers
