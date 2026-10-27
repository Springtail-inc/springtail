#include <algorithm>
#include <array>
#include <cctype>
#include <cstring>
#include <mutex>
#include <set>

#include <absl/log/check.h>

#include <common/constants.hh>
#include <common/exception.hh>
#include <pg_ext/extn_registry.hh>
#include <pg_ext/fmgr.hh>
#include <pg_ext/string.hh>
#include <storage/field.hh>
#include <storage/index_engine/gin_helpers.hh>

/**
 * The opclass-CALLING half of gin_helpers (extract_terms()), split out of the sibling,
 * opclass-independent gin_helpers.cc for the exact same library-layout reason
 * gist_helpers_opclass.cc is split from gist_helpers.cc (see src/storage/CMakeLists.txt's
 * comment block, and phases/phase-10.md's Design notes for the original finding): this file
 * needs pg_ext (to dlopen and call into a real Postgres contrib opclass .so), and pg_ext must
 * never become a transitive link dependency of springtail_pg_fdw.so. extract_terms() is only
 * ever called from GinWriteStrategy (build-time term extraction); the read side
 * (GinReadStrategy) operates on already-extracted term bytes and never links this file -- see
 * gin_read_strategy.hh's own header comment.
 */
namespace springtail::gin_helpers {

    namespace {
        /**
         * Pure-C++ reference "opclass" for this phase's own spill/intersection-correctness tests
         * -- see TEST_TERM_OPCLASS's own doc comment (gin_helpers.hh) for why a real opclass is
         * deliberately not used for every test. Terms are lower-cased, alphanumeric words,
         * deduplicated per row -- a simple, independently-verifiable tokenization (easy to
         * reproduce by hand or with a plain Python/C++ reference set in a test), not an attempt
         * to mirror pg_trgm's own trigram extraction.
         */
        namespace test_term_ops {
            std::vector<std::vector<char>> extract(TuplePtr value) {
                auto field = value->field(0);
                if (field->get_type() != SchemaType::TEXT) {
                    throw Error("gin_helpers: __test_term_ops only supports TEXT columns");
                }

                std::string lower(field->get_text(value->row()));
                std::ranges::transform(lower, lower.begin(),
                                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });

                std::vector<std::vector<char>> terms;
                std::set<std::string> seen;

                size_t i = 0;
                while (i < lower.size()) {
                    while (i < lower.size() && !std::isalnum(static_cast<unsigned char>(lower[i]))) {
                        ++i;
                    }
                    size_t start = i;
                    while (i < lower.size() && std::isalnum(static_cast<unsigned char>(lower[i]))) {
                        ++i;
                    }
                    if (i > start) {
                        std::string word = lower.substr(start, i - start);
                        if (seen.insert(word).second) {
                            terms.emplace_back(word.begin(), word.end());
                        }
                    }
                }
                return terms;
            }
        } // namespace test_term_ops

        /** Mirrors gist_helpers_opclass.cc's identical helper (ScratchContext/call_with_flinfo/
            g_opclass_call_mutex) exactly, down to the reasoning in its comments -- deliberately
            duplicated rather than shared, matching that file's own self-contained, one-
            translation-unit-per-kind pattern (each kind's opclass-interop quirks/workarounds are
            independent; see gist_helpers_opclass.cc's header comment on why FmgrInfo/fn_mcxt
            must be real, not null). */
        std::mutex g_opclass_call_mutex;

        struct ScratchContext {
            void *ctx;
            ScratchContext() : ctx(AllocSetContextCreateInternal(CurrentMemoryContext, "gin_helpers_scratch", 0, 1024, 65536)) { }
            ~ScratchContext() { MemoryContextDelete(ctx); }
            ScratchContext(const ScratchContext &) = delete;
            ScratchContext &operator=(const ScratchContext &) = delete;
        };

        template <size_t N>
        Datum call_with_flinfo(PGFunction func, void *mcxt, const std::array<Datum, N> &args) {
            std::lock_guard<std::mutex> guard(g_opclass_call_mutex);

            FmgrInfo flinfo{};
            flinfo.fn_addr = func;
            flinfo.fn_mcxt = mcxt;

            LOCAL_FCINFO(fcinfo, N);
            InitFunctionCallInfoData(*fcinfo, &flinfo, N, 0, nullptr, nullptr);

            for (size_t i = 0; i < N; ++i) {
                fcinfo->args[i] = { args[i], false };
            }

            return func(fcinfo);
        }

        PGFunction opclass_func(const std::string &opclass_name, int support_number) {
            auto method = PgExtnRegistry::get_instance()->get_opclass_method_by_method_name(
                    opclass_name, support_number);
            return method.function_ptr;
        }
    } // namespace

    std::vector<std::vector<char>> extract_terms(TuplePtr value, const std::string &opclass_name) {
        if (opclass_name == TEST_TERM_OPCLASS) {
            return test_term_ops::extract(value);
        }

        auto field = value->field(0);
        if (field->get_type() != SchemaType::TEXT) {
            throw Error("gin_helpers: only TEXT-typed GIN columns are supported in this phase "
                        "(GIN_EXTRACTVALUE's returned key Datums are assumed int4-typed -- see "
                        "gin_helpers.hh/this file's own scope note)");
        }

        auto extract_fn = opclass_func(opclass_name, GIN_EXTRACTVALUE);
        if (extract_fn == nullptr) {
            throw Error("gin_helpers: no GIN_EXTRACTVALUE registered for opclass " + opclass_name);
        }

        auto text_value = field->get_text(value->row());
        text *t = cstring_to_text_with_len(text_value.data(), static_cast<int>(text_value.size()));
        Datum value_datum = PointerGetDatum(t);

        int32_t nentries = 0;
        ScratchContext scratch;
        Datum result = call_with_flinfo<2>(extract_fn, scratch.ctx,
                { value_datum, PointerGetDatum(&nentries) });

        std::vector<std::vector<char>> terms;
        if (nentries <= 0 || DatumGetPointer(result) == nullptr) {
            return terms;
        }

        // Scope limit, documented above: this phase only supports opclasses whose extracted key
        // type is int4 (true of pg_trgm's gin_trgm_ops, the only real opclass this phase tests --
        // see Open question (e)). The raw 4-byte native-endian encoding below is an injective but
        // otherwise opaque term representation -- entry-tree order never needs to match the key
        // type's own "numeric" order, since GIN only ever looks an entry up by exact match (see
        // gin_helpers.hh's header note).
        auto *entries = reinterpret_cast<Datum *>(DatumGetPointer(result));
        std::set<int32_t> seen;
        for (int32_t i = 0; i < nentries; ++i) {
            int32_t key = DatumGetInt32(entries[i]);
            if (seen.insert(key).second) {
                std::vector<char> bytes(sizeof(key));
                std::memcpy(bytes.data(), &key, sizeof(key));
                terms.push_back(std::move(bytes));
            }
        }
        return terms;
    }

} // namespace springtail::gin_helpers
