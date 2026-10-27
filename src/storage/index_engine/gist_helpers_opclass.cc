#include <algorithm>
#include <array>
#include <cstddef>
#include <cstring>
#include <mutex>

#include <absl/log/check.h>

#include <common/constants.hh>
#include <common/exception.hh>
#include <pg_ext/extn_registry.hh>
#include <pg_ext/fmgr.hh>
#include <pg_ext/string.hh>
#include <pg_ext/varatt.hh>
#include <storage/field.hh>
#include <storage/index_engine/gist_helpers.hh>

/**
 * The opclass-CALLING half of gist_helpers (compress/penalty/union/picksplit/consistent),
 * deliberately split out of the sibling gist_helpers.cc into its own translation unit, in its
 * own library (springtail_opclass_engine, see src/storage/CMakeLists.txt) -- NOT springtail_storage.
 * This file needs pg_ext (to dlopen and call into real Postgres contrib opclass .so's), and
 * pg_ext is a shared library whose symbols (palloc/elog/MemoryContextAlloc/etc) collide by name
 * with the real Postgres backend's own identically-named globals. springtail_pg_fdw.so gets
 * dlopen'd directly into a real, unmodified backend process, so it must never pull pg_ext in,
 * transitively or otherwise -- confirmed directly (not guessed): once gist_helpers.cc (then
 * undivided) became part of springtail_storage, which springtail_pg_fdw.so links, `ldd` on the
 * built springtail_fdw.so started listing libspringtail_pgext.so as a dependency for the first
 * time, and CREATE EXTENSION springtail_fdw started segfaulting the backend before even reaching
 * its own _PG_init() log line. GistReader's own consistent()-driven search() (the only caller of
 * this file reachable from Table, which springtail_pg_fdw.so's link graph does include via
 * springtail_sys_tbl_mgr_client) is deferred to phase 12 (query-time routing) and is split into
 * its own translation unit (gist_reader_search.cc) for exactly this reason -- see that file.
 */
namespace springtail::gist_helpers {

    namespace {
        /**
         * ABI-exact replicas of Postgres's own access/gist.h structs (confirmed against PG16's
         * real header, not guessed -- see phases/phase-10.md's Design notes), since this code
         * calls directly into a dlopen'd, unmodified contrib .so built against the real headers.
         * `rel` is `uintptr_t` (never dereferenced by any opclass function this phase calls), but
         * `page` genuinely needs to point at a real (fake, but ABI-correct) page buffer: confirmed
         * directly (a real SIGSEGV, not guessed) that GIST_CONSISTENT support functions (pg_trgm's
         * gtrgm_consistent(), at least) call the real GIST_LEAF(entry) macro --
         * GistPageIsLeaf(entry->page), which dereferences entry->page as a real Postgres page
         * buffer's special space -- to tell a leaf entry from a branch one, never consulting
         * GISTENTRY::leafkey for that (leafkey is an insert-time hint to compress/penalty, a
         * different thing). See make_fake_gist_page() below for the minimal fake page this
         * requires; penalty/union/picksplit never dereference `page` at all, confirmed by reading
         * their real source, so only consistent()'s entries get a real one.
         */
        struct GISTENTRY {
            Datum key;
            uintptr_t rel;
            uintptr_t page;
            uint16_t offset;
            bool leafkey;
        };

        struct GistEntryVector {
            int32_t n;
            GISTENTRY vector[1];
        };

        struct GIST_SPLITVEC {
            uint16_t *spl_left;
            int spl_nleft;
            Datum spl_ldatum;
            bool spl_ldatum_exists;

            uint16_t *spl_right;
            int spl_nright;
            Datum spl_rdatum;
            bool spl_rdatum_exists;
        };

        /**
         * A minimal, ABI-correct fake Postgres page buffer, just enough for GIST_LEAF(entry) --
         * i.e. GistPageGetOpaque(page)->flags & F_LEAF -- to read the right answer.
         * PageGetSpecialPointer() (src/include/storage/bufpage.h) is `(char*)page +
         * ((PageHeader)page)->pd_special`, and PageHeaderData's layout (same header) puts
         * pd_special at byte offset 16 (pd_lsn:8 + pd_checksum:2 + pd_flags:2 + pd_lower:2 +
         * pd_upper:2). We only need pd_special set correctly and a GISTPageOpaqueData (access/
         * gist.h: nsn:8, rightlink:4, flags:2, gist_page_id:2 = 16 bytes) at that offset with its
         * `flags` bit F_LEAF (0x01) set or clear -- every other byte can be (and is) zero.
         */
        constexpr size_t PD_SPECIAL_FIELD_OFFSET = 16; // offsetof(PageHeaderData, pd_special)
        constexpr size_t PD_SPECIAL_VALUE = 16; // where our special area starts in the fake buffer
        constexpr size_t GIST_OPAQUE_SIZE = 16;
        constexpr size_t GIST_OPAQUE_FLAGS_OFFSET = 12; // nsn(8) + rightlink(4)
        constexpr uint16_t F_LEAF = 0x01;

        const std::vector<char> &fake_gist_page(bool is_leaf) {
            static std::vector<char> leaf_page(PD_SPECIAL_VALUE + GIST_OPAQUE_SIZE, 0);
            static std::vector<char> branch_page(PD_SPECIAL_VALUE + GIST_OPAQUE_SIZE, 0);
            static bool initialized = false;
            if (!initialized) {
                uint16_t special_offset = PD_SPECIAL_VALUE;
                std::memcpy(leaf_page.data() + PD_SPECIAL_FIELD_OFFSET, &special_offset, sizeof(special_offset));
                std::memcpy(branch_page.data() + PD_SPECIAL_FIELD_OFFSET, &special_offset, sizeof(special_offset));

                uint16_t leaf_flags = F_LEAF;
                uint16_t branch_flags = 0;
                std::memcpy(leaf_page.data() + PD_SPECIAL_VALUE + GIST_OPAQUE_FLAGS_OFFSET,
                           &leaf_flags, sizeof(leaf_flags));
                std::memcpy(branch_page.data() + PD_SPECIAL_VALUE + GIST_OPAQUE_FLAGS_OFFSET,
                           &branch_flags, sizeof(branch_flags));
                initialized = true;
            }
            return is_leaf ? leaf_page : branch_page;
        }

        /**
         * A built-in, pure-C++ reference "opclass" for this phase's own split/descent/fan-out
         * tests -- entirely bypassing PgExtnRegistry/dlopen/the real Postgres ABI calling
         * convention below. Added because of a confirmed, deeply-investigated, but not fully
         * root-caused crash: calling pg_trgm.so's real gtrgm_union()/gtrgm_penalty() repeatedly
         * (i.e. on any insert sequence that actually grows a branch node, which any multi-entry
         * GiST build does) intermittently SIGSEGVs inside the .so's own compiled code, ~50-70%
         * of runs, and does *not* reproduce under AddressSanitizer across 10+ consecutive runs --
         * see phases/phase-10.md's Design notes for the full writeup of what was tried and ruled
         * out (missing FmgrInfo, SET_VARSIZE's missing bit-shift, a confirmed BlockMap key-
         * collision use-after-free in pg_ext's allocator -- all real, all fixed, none of them
         * the full story). This reference opclass lets this phase's own tests verify
         * GistWriter/GistWriteStrategy/GistReader/GistReadStrategy's own logic (choose_subtree,
         * picksplit-driven arbitrary partition, union-propagated branch keys, consistent-driven
         * fan-out search) -- this phase's actual deliverable -- independently of that separate,
         * flagged, real-opclass-interop fragility. GistStrategyTest.SingleEntryRoundTrips still
         * exercises the real opclass (pg_trgm) end to end for the one scenario confirmed stable.
         *
         * Keys are "lo<0x1f>hi" (lo/hi lexicographically bound the entries unioned so far; a
         * leaf's own key is the degenerate range [value, value]) -- the same "range opclass"
         * shape real range/spatial GiST opclasses use, just over strings instead of numbers.
         * Strategy 1 ("="): exact point lookup. Strategy 2 ("overlaps"): query is itself a
         * lo/hi range; consistent if it overlaps the entry's range -- the strategy this phase's
         * fan-out test actually drives, since an exact-match query against a *correctly*
         * partitioned (non-overlapping) range tree can only ever match one leaf by construction,
         * same as real GiST: fan-out is a property of overlap-style queries/predicates, or of a
         * lossy compression step producing false-positive overlap (pg_trgm's own reason for
         * needing `recheck`) -- not of exact lookups against a well-behaved opclass.
         */
        namespace test_range_ops {
            constexpr char SEP = '\x1f';
            constexpr uint16_t STRATEGY_EQ = 1;
            constexpr uint16_t STRATEGY_OVERLAPS = 2;

            std::string make_key(const std::string &lo, const std::string &hi) {
                return lo + SEP + hi;
            }

            std::pair<std::string, std::string> parse_key(const std::string &key) {
                auto pos = key.find(SEP);
                CHECK(pos != std::string::npos) << "malformed test_range_ops key";
                return { key.substr(0, pos), key.substr(pos + 1) };
            }

            std::string compress(const std::string &value) {
                return make_key(value, value);
            }

            double penalty(const std::string &existing_key, const std::string &incoming_key) {
                auto [lo, hi] = parse_key(existing_key);
                auto [ilo, ihi] = parse_key(incoming_key);
                double growth = 0.0;
                if (ilo < lo) growth += 1.0;
                if (ihi > hi) growth += 1.0;
                return growth;
            }

            std::string union_keys(const std::vector<std::string> &keys) {
                CHECK(!keys.empty());
                auto [lo, hi] = parse_key(keys.front());
                for (const auto &k : keys) {
                    auto [klo, khi] = parse_key(k);
                    if (klo < lo) lo = klo;
                    if (khi > hi) hi = khi;
                }
                return make_key(lo, hi);
            }

            void picksplit(const std::vector<std::string> &keys, std::vector<size_t> &left,
                           std::vector<size_t> &right) {
                std::vector<size_t> order(keys.size());
                for (size_t i = 0; i < order.size(); ++i) {
                    order[i] = i;
                }
                std::ranges::sort(order, [&](size_t a, size_t b) {
                    return parse_key(keys[a]).first < parse_key(keys[b]).first;
                });
                size_t mid = order.size() / 2;
                left.assign(order.begin(), order.begin() + static_cast<long>(mid));
                right.assign(order.begin() + static_cast<long>(mid), order.end());
            }

            bool consistent(const std::string &entry_key, const std::string &query, uint16_t strategy) {
                auto [lo, hi] = parse_key(entry_key);
                if (strategy == STRATEGY_EQ) {
                    return lo <= query && query <= hi;
                }
                CHECK_EQ(strategy, STRATEGY_OVERLAPS);
                auto [qlo, qhi] = parse_key(query);
                return lo <= qhi && qlo <= hi;
            }
        } // namespace test_range_ops

        GISTENTRY make_gistentry(Datum key, bool leafkey) {
            GISTENTRY e;
            e.key = key;
            e.rel = 0;
            e.page = 0;
            e.offset = 0;
            e.leafkey = leafkey;
            return e;
        }

        /** Copies `size` raw bytes starting at `ptr` into an owned buffer -- the one place this
            file assumes a Datum is a varlena pointer (VARSIZE_ANY-describable). See
            gist_helpers.hh's class comment on GistEntry for why this is a deliberate, documented
            scope limit rather than a general Datum-to-bytes converter. */
        std::string copy_varlena_bytes(Datum d) {
            const char *ptr = reinterpret_cast<const char *>(d);
            uint32_t size = VARSIZE_ANY(ptr);
            return std::string(ptr, size);
        }

        Datum bytes_as_datum(const std::string &bytes) {
            return PointerGetDatum(bytes.data());
        }

        /**
         * Calls `func` with a real, non-null FmgrInfo (fn_addr/fn_mcxt set, fn_extra starting
         * null). Every GiST support function this file calls turns out to need this, not just
         * GIST_CONSISTENT as first assumed: confirmed directly (two separate real SIGSEGVs, not
         * guessed) that both pg_trgm's gtrgm_consistent() (caching extracted query trigrams) and
         * gtrgm_penalty() (caching a converted-to-signature copy of a small/array-form `newval`,
         * "if (ISARRKEY(newval))") unconditionally dereference `fcinfo->flinfo->fn_extra`/
         * `fn_mcxt` with no null check -- DirectFunctionCallN's own flinfo=nullptr crashes both.
         * A fresh FmgrInfo per call (fn_extra starts null every time, rather than persisting
         * across calls the way a real index scan's single long-lived FmgrInfo would) means
         * these functions' own caches are never actually warm -- functionally harmless (they
         * recompute instead of reusing a cached result), just not as fast as a real scan; this
         * phase's own tests are nowhere near where that would matter. Used for every opclass call
         * in this file (not just the ones confirmed to need it) since there's no reason to expect
         * compress()/union()/picksplit() are exempt from this same pattern for other opclasses.
         *
         * Serialized by `g_opclass_call_mutex`: `pg_ext`'s global MemoryContext/palloc
         * implementation (src/pg_ext/memory.cc, TopMemoryContext/CurrentMemoryContext) has no
         * internal locking at all, unlike every other shared, cross-thread structure in this
         * codebase (architecture.md Sec 5.4's own framing: the engine's extent caches are
         * explicitly "ordinary, cheap in-process thread-safety", never assumed free). An
         * intermittent (confirmed, reproduced repeatedly), ASLR-correlated SIGSEGV inside a real
         * opclass .so's own palloc-caching logic (gtrgm_penalty/gtrgm_union, via
         * MemoryContextAlloc) went away under every AddressSanitizer run but kept recurring
         * natively -- consistent with a racing, unsynchronized allocator rather than a fixed
         * out-of-bounds access ASan would have caught directly. This mutex is the narrow,
         * external serialization point (the same shape architecture.md Sec 5.4 already requires
         * elsewhere for shared mutable state) rather than adding locking inside pg_ext itself,
         * which is shared, unowned infrastructure well outside this phase's scope to rearchitect.
         */
        std::mutex g_opclass_call_mutex;

        /**
         * A short-lived scratch MemoryContext, one per opclass call, instead of handing every
         * call fn_mcxt=CurrentMemoryContext (TopMemoryContext) directly. Belt-and-suspenders on
         * top of the BlockMap->multimap fix (pg_ext/memory.hh) for the same underlying class of
         * bug: TopMemoryContext is a single, process-lifetime-long-lived bump allocator every
         * opclass call would otherwise share and never reset, so its _blocks bookkeeping
         * accumulates entries across an entire test run or daemon lifetime; a fresh child
         * context bounds the blast radius of any _blocks-tracking bug (found or not-yet-found)
         * to just one call's own small allocations. Callers must keep the ScratchContext alive
         * until they've copied every result they need out of it (copy_varlena_bytes() et al.) --
         * destroying it (via MemoryContextDelete, which clears and detaches the child) frees
         * everything the call allocated, including the function's own return value if it's a
         * pointer into this context (true of compress/union/picksplit's results).
         */
        struct ScratchContext {
            void *ctx;
            ScratchContext() : ctx(AllocSetContextCreateInternal(CurrentMemoryContext, "gist_helpers_scratch", 0, 1024, 65536)) { }
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

        /** Convert the raw table-column value at `field`/`row` into a Datum of its own (pre-
            compression) input type. Scope-limited to TEXT -- see gist_helpers.hh. */
        Datum field_to_datum(const FieldPtr &field, const void *row) {
            if (field->get_type() != SchemaType::TEXT) {
                throw Error("gist_helpers: only TEXT-typed GiST columns are supported in this phase "
                            "(opclass compress/union results must be varlena-describable -- see "
                            "gist_helpers.hh)");
            }
            auto text_value = field->get_text(row);
            text *t = cstring_to_text_with_len(text_value.data(), static_cast<int>(text_value.size()));
            return PointerGetDatum(t);
        }
    } // namespace

    GistEntry compress_leaf_entry(TuplePtr value, const std::vector<std::string> &opclass_names) {
        GistEntry out;
        out.is_leaf = true;
        out.keys.resize(opclass_names.size());

        for (size_t col = 0; col < opclass_names.size(); ++col) {
            if (opclass_names[col] == TEST_RANGE_OPCLASS) {
                auto field = value->field(static_cast<int>(col));
                out.keys[col] = test_range_ops::compress(std::string(field->get_text(value->row())));
                continue;
            }

            Datum raw = field_to_datum(value->field(static_cast<int>(col)), value->row());

            auto compress = opclass_func(opclass_names[col], GIST_COMPRESS);
            if (compress) {
                GISTENTRY entry = make_gistentry(raw, true);
                ScratchContext scratch;
                Datum compressed = call_with_flinfo<1>(compress, scratch.ctx, { PointerGetDatum(&entry) });
                auto *retval = reinterpret_cast<GISTENTRY *>(DatumGetPointer(compressed));
                out.keys[col] = copy_varlena_bytes(retval->key);
            } else {
                out.keys[col] = copy_varlena_bytes(raw);
            }
        }

        return out;
    }

    double compute_penalty(const GistEntry &existing, const GistEntry &incoming,
                            const std::vector<std::string> &opclass_names) {
        double total = 0.0;

        for (size_t col = 0; col < opclass_names.size(); ++col) {
            if (opclass_names[col] == TEST_RANGE_OPCLASS) {
                total += test_range_ops::penalty(existing.keys[col], incoming.keys[col]);
                continue;
            }

            auto penalty_fn = opclass_func(opclass_names[col], GIST_PENALTY);
            if (!penalty_fn) {
                continue;
            }

            GISTENTRY o = make_gistentry(bytes_as_datum(existing.keys[col]), existing.is_leaf);
            GISTENTRY n = make_gistentry(bytes_as_datum(incoming.keys[col]), incoming.is_leaf);
            float result = 0.0f;

            ScratchContext scratch;
            call_with_flinfo<3>(penalty_fn, scratch.ctx, { PointerGetDatum(&o), PointerGetDatum(&n),
                                                          PointerGetDatum(&result) });
            total += result;
        }

        return total;
    }

    GistEntry compute_union(const std::vector<GistEntry> &entries,
                             const std::vector<std::string> &opclass_names) {
        CHECK(!entries.empty());

        GistEntry out;
        out.is_leaf = false;
        out.keys.resize(opclass_names.size());

        for (size_t col = 0; col < opclass_names.size(); ++col) {
            if (opclass_names[col] == TEST_RANGE_OPCLASS) {
                std::vector<std::string> keys;
                keys.reserve(entries.size());
                for (auto &e : entries) {
                    keys.push_back(e.keys[col]);
                }
                out.keys[col] = test_range_ops::union_keys(keys);
                continue;
            }

            auto union_fn = opclass_func(opclass_names[col], GIST_UNION);
            if (!union_fn) {
                // No union support function -- fall back to the first entry's own key, mirroring
                // real GiST's behavior for an opclass that declares union optional (none of
                // btree_gist's do; this is defensive, not expected to be exercised).
                out.keys[col] = entries.front().keys[col];
                continue;
            }

            size_t n = entries.size();
            std::vector<char> buffer(offsetof(GistEntryVector, vector) + n * sizeof(GISTENTRY));
            auto *vec = reinterpret_cast<GistEntryVector *>(buffer.data());
            vec->n = static_cast<int32_t>(n);
            for (size_t i = 0; i < n; ++i) {
                vec->vector[i] = make_gistentry(bytes_as_datum(entries[i].keys[col]), entries[i].is_leaf);
            }

            int32_t out_size = 0;
            ScratchContext scratch;
            Datum result = call_with_flinfo<2>(union_fn, scratch.ctx, { PointerGetDatum(vec), PointerGetDatum(&out_size) });
            out.keys[col] = copy_varlena_bytes(result); // copy before `scratch` destructs
        }

        return out;
    }

    void compute_picksplit(const std::vector<GistEntry> &entries,
                            std::vector<size_t> &left, std::vector<size_t> &right,
                            const std::vector<std::string> &opclass_names) {
        CHECK(entries.size() >= 2);
        left.clear();
        right.clear();

        if (opclass_names[0] == TEST_RANGE_OPCLASS) {
            std::vector<std::string> keys;
            keys.reserve(entries.size());
            for (auto &e : entries) {
                keys.push_back(e.keys[0]);
            }
            test_range_ops::picksplit(keys, left, right);
            return;
        }

        // Only column 0's opclass drives the split -- matches real multi-column GiST: the core
        // gistSplit() always picksplits on the first column and lets the remaining columns'
        // union keys ride along for whichever side each entry landed on (gist.c's own
        // "secondary split" machinery, spl_ldatum_exists/spl_rdatum_exists, exists precisely to
        // let later columns refine an already-decided split further -- not needed here, since
        // this phase only exercises single-column GiST indexes).
        auto picksplit_fn = opclass_func(opclass_names[0], GIST_PICKSPLIT);
        CHECK(picksplit_fn != nullptr) << "opclass " << opclass_names[0] << " has no GIST_PICKSPLIT";

        size_t n = entries.size();
        // Postgres's own gistSplit() builds this 1-based (vector[0] unused, real entries at
        // 1..n) -- picksplit's returned spl_left/spl_right offsets are 1-based OffsetNumbers
        // into exactly this layout, confirmed against the real contrib/btree_gist source read
        // for this phase (see gist_helpers.hh's header comment).
        std::vector<char> buffer(offsetof(GistEntryVector, vector) + (n + 1) * sizeof(GISTENTRY));
        auto *vec = reinterpret_cast<GistEntryVector *>(buffer.data());
        vec->n = static_cast<int32_t>(n + 1);
        vec->vector[0] = make_gistentry(0, entries[0].is_leaf);
        for (size_t i = 0; i < n; ++i) {
            vec->vector[i + 1] = make_gistentry(bytes_as_datum(entries[i].keys[0]), entries[i].is_leaf);
        }

        GIST_SPLITVEC sv;
        std::memset(&sv, 0, sizeof(sv));

        ScratchContext scratch;
        call_with_flinfo<2>(picksplit_fn, scratch.ctx, { PointerGetDatum(vec), PointerGetDatum(&sv) });

        CHECK_GT(sv.spl_nleft, 0);
        CHECK_GT(sv.spl_nright, 0);

        // sv.spl_left/spl_right are themselves pointers picksplit allocated in `scratch` --
        // read them out (into our own owned vectors) before `scratch` destructs below.
        for (int i = 0; i < sv.spl_nleft; ++i) {
            left.push_back(static_cast<size_t>(sv.spl_left[i]) - 1);
        }
        for (int i = 0; i < sv.spl_nright; ++i) {
            right.push_back(static_cast<size_t>(sv.spl_right[i]) - 1);
        }
    }

    bool consistent(const GistEntry &entry, int col, const std::string &query_bytes,
                     uint16_t strategy, bool is_leaf, const std::vector<std::string> &opclass_names,
                     bool *recheck_out) {
        if (opclass_names[col] == TEST_RANGE_OPCLASS) {
            if (recheck_out) {
                *recheck_out = false; // exact string comparisons only -- never lossy.
            }
            return test_range_ops::consistent(entry.keys[col], query_bytes, strategy);
        }

        auto consistent_fn = opclass_func(opclass_names[col], GIST_CONSISTENT);
        CHECK(consistent_fn != nullptr) << "opclass " << opclass_names[col] << " has no GIST_CONSISTENT";

        GISTENTRY gentry = make_gistentry(bytes_as_datum(entry.keys[col]), is_leaf);
        // GIST_LEAF(entry) (see this file's GISTENTRY comment) reads entry->page, not
        // entry->leafkey -- give it a real (fake) page so it reports the right answer.
        gentry.page = reinterpret_cast<uintptr_t>(fake_gist_page(is_leaf).data());
        bool recheck = false;

        ScratchContext scratch;
        Datum result = call_with_flinfo<5>(consistent_fn, scratch.ctx, {
                PointerGetDatum(&gentry),
                bytes_as_datum(query_bytes),
                static_cast<Datum>(strategy),
                ObjectIdGetDatum(0),
                PointerGetDatum(&recheck) });
        if (recheck_out) {
            *recheck_out = recheck;
        }
        return DatumGetBool(result);
    }

    std::string make_text_query(const std::string &value) {
        text *t = cstring_to_text_with_len(value.data(), static_cast<int>(value.size()));
        return copy_varlena_bytes(PointerGetDatum(t));
    }

    std::string make_test_range_query(const std::string &lo, const std::string &hi) {
        return test_range_ops::make_key(lo, hi);
    }

} // namespace springtail::gist_helpers
