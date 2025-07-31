#include <iostream>
#include <dlfcn.h>
#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <pg_ext/fmgr.hh>
#include <pg_ext/string.hh>
#include <stdint.h>
#include <string.h>

// ---------- Unbuffer stdout/stderr for immediate logs ----------
static void init_io_unbuffered() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    setvbuf(stderr, nullptr, _IONBF, 0);
}

// ---------- VarlenA helpers (1B or 4B header aware) ----------
typedef struct {
    int32_t vl_len_;   // only valid for 4B representation
    char    vl_dat[1];
} text;

// ---------- Builders: 4B, 1B and AUTO ----------
static void* cstring_to_text_4b(const char *s) {
    size_t n = std::strlen(s);
    size_t tot = VARHDRSZ_LONG + n;
    unsigned char *p = (unsigned char*)std::malloc(tot);
    if (!p) { std::perror("malloc"); std::exit(1); }
    int32_t len = (int32_t)tot;
    std::memcpy(p, &len, 4);         // total length incl header
    std::memcpy(p + 4, s, n);
    return (void*)p;
}

static void* cstring_to_text_1b(const char *s) {
    size_t n = std::strlen(s);
    if (n > 127) {
        // fall back to 4B when too large for 1B
        return cstring_to_text_4b(s);
    }
    size_t tot = VARHDRSZ_SHORT + n;
    unsigned char *p = (unsigned char*)std::malloc(tot);
    if (!p) { std::perror("malloc"); std::exit(1); }
    p[0] = (unsigned char)(0x80 | (unsigned char)n);   // 1B header: high bit set, 7 bits = payload len
    std::memcpy(p + 1, s, n);
    return (void*)p;
}

static void* cstring_to_text_auto(const char *s) {
    size_t n = std::strlen(s);
    return (n <= 127) ? cstring_to_text_1b(s) : cstring_to_text_4b(s);
}

// Allow forcing via env var: PGTEST_VARLENA=1B|4B
static void* build_text_from_env(const char* s) {
    const char* mode = std::getenv("PGTEST_VARLENA");
    if (!mode) return cstring_to_text_auto(s);
    if (std::strcmp(mode, "1B") == 0 || std::strcmp(mode, "1b") == 0) return cstring_to_text_1b(s);
    if (std::strcmp(mode, "4B") == 0 || std::strcmp(mode, "4b") == 0) return cstring_to_text_4b(s);
    // default
    return cstring_to_text_auto(s);
}

// ---------- Datum helpers ----------
static inline Datum PointerGetDatum(const void* p) { return (Datum)(uintptr_t)p; }
static inline void* DatumGetPointer(Datum d) { return (void*)(uintptr_t)d; }
static inline float DatumGetFloat4(Datum d) {
    float f;
    std::memcpy(&f, &d, sizeof(float));
    return f;
}

// ---------- Dump helpers ----------
static void dump_bytes(const char* label, const void* ptr, size_t len) {
    std::fprintf(stderr, "%s %p len=%zu :", label, ptr, len);
    auto *p = reinterpret_cast<const unsigned char*>(ptr);
    for (size_t i = 0; i < len; ++i) std::fprintf(stderr, " %02X", p[i]);
    std::fprintf(stderr, "\n");
}

static void dump_text_hdr(const char* tag, const void* tptr) {
    int32_t tot = VARSIZE_ANY(tptr);
    int32_t pay = VARSIZE_ANY_EXHDR(tptr);
    const char* data = VARDATA_ANY_CONST(tptr);
    std::fprintf(stderr, "%s @%p: total=%d is1b=%d payload=%d data='%.*s'\n",
                 tag, tptr, tot, VARATT_IS_1B(tptr), pay, pay, data);
}

#define PG_DETOAST_DATUM_PACKED(datum) \
    pg_detoast_datum_packed((struct varlena *) DatumGetPointer(datum))
#define DatumGetTextPP(X) ((text *) PG_DETOAST_DATUM_PACKED((X)))
#define PG_GETARG_DATUM(n)    (fcinfo->args[(n)].value)
#define PG_GETARG_TEXT_PP(n)  DatumGetTextPP(PG_GETARG_DATUM(n))

void test_similarity_function(void* pgtrgm)
{
    init_io_unbuffered();

    // Resolve similarity and print where it comes from
    PGFunction similarity = (PGFunction)dlsym(pgtrgm, "similarity");
    if (!similarity) {
        std::cerr << "dlsym(similarity) failed: " << dlerror() << "\n";
        return;
    }

    void* t1 = build_text_from_env("Hello");
    void* t2 = build_text_from_env("Hallo");

    // dump_text_hdr("[host] t1", t1);
    // dump_text_hdr("[host] t2", t2);

    // Prepare fcinfo using your shim’s macros/types
    LOCAL_FCINFO(fcinfo, 2);
    InitFunctionCallInfoData(*fcinfo, nullptr, 2, 0 /*collation*/, nullptr, nullptr);

    fcinfo->args[0].value  = PointerGetDatum(t1);
    fcinfo->args[0].isnull = false;
    fcinfo->args[1].value  = PointerGetDatum(t2);
    fcinfo->args[1].isnull = false;

    // Sanity check with OUR header-aware macros (what the caller sees)
    // text* debug_t1 = PG_GETARG_TEXT_PP(0);
    // text* debug_t2 = PG_GETARG_TEXT_PP(1);

    // dump_bytes("[host] arg0", t1, VARSIZE_ANY(t1));
    // dump_bytes("[host] arg1", t2, VARSIZE_ANY(t2));
    // dump_text_hdr("[host] arg0", debug_t1);
    // dump_text_hdr("[host] arg1", debug_t2);

    std::cout << "Testing similarity()..." << std::endl;
    Datum d = similarity(fcinfo);

    float sim = DatumGetFloat4(d);
    std::cout << "Similarity: " << sim << std::endl;

    pfree(t1);
    pfree(t2);
}

void test_word_similarity_function(void* pgtrgm){
    // Load word_similarity() function
    PGFunction word_similarity = (PGFunction)dlsym(pgtrgm, "word_similarity");
    if (!word_similarity) {
        std::cerr << "Failed to find word_similarity: " << dlerror() << std::endl;
    }

    void* t1 = build_text_from_env("Hello");
    void* t2 = build_text_from_env("Hallo");

    dump_text_hdr("[host] t1", t1);
    dump_text_hdr("[host] t2", t2);

    // Prepare fcinfo using your shim’s macros/types
    LOCAL_FCINFO(fcinfo, 2);
    InitFunctionCallInfoData(*fcinfo, nullptr, 2, 0 /*collation*/, nullptr, nullptr);

    fcinfo->args[0].value  = PointerGetDatum(t1);
    fcinfo->args[0].isnull = false;
    fcinfo->args[1].value  = PointerGetDatum(t2);
    fcinfo->args[1].isnull = false;

    // Sanity check with OUR header-aware macros (what the caller sees)
    text* debug_t1 = PG_GETARG_TEXT_PP(0);
    text* debug_t2 = PG_GETARG_TEXT_PP(1);

    dump_bytes("[host] arg0", t1, VARSIZE_ANY(t1));
    dump_bytes("[host] arg1", t2, VARSIZE_ANY(t2));
    dump_text_hdr("[host] arg0", debug_t1);
    dump_text_hdr("[host] arg1", debug_t2);

    std::cout << "Calling word_similarity()..." << std::endl;
    Datum result = word_similarity(fcinfo);

    float sim = DatumGetFloat4(result);
    std::cout << "Word Similarity: " << sim << std::endl;

    pfree(t1);
    pfree(t2);
}

void test_strict_word_similarity_function(void* pgtrgm){
    // Load strict_word_similarity() function
    PGFunction strict_word_similarity = (PGFunction)dlsym(pgtrgm, "strict_word_similarity");
    if (!strict_word_similarity) {
        std::cerr << "Failed to find strict_word_similarity: " << dlerror() << std::endl;
    }

    void* t1 = build_text_from_env("Hello");
    void* t2 = build_text_from_env("Hallo");

    dump_text_hdr("[host] t1", t1);
    dump_text_hdr("[host] t2", t2);

    // Prepare fcinfo using your shim’s macros/types
    LOCAL_FCINFO(fcinfo, 2);
    InitFunctionCallInfoData(*fcinfo, nullptr, 2, 0 /*collation*/, nullptr, nullptr);

    fcinfo->args[0].value  = PointerGetDatum(t1);
    fcinfo->args[0].isnull = false;
    fcinfo->args[1].value  = PointerGetDatum(t2);
    fcinfo->args[1].isnull = false;

    // Sanity check with OUR header-aware macros (what the caller sees)
    text* debug_t1 = PG_GETARG_TEXT_PP(0);
    text* debug_t2 = PG_GETARG_TEXT_PP(1);

    dump_bytes("[host] arg0", t1, VARSIZE_ANY(t1));
    dump_bytes("[host] arg1", t2, VARSIZE_ANY(t2));
    dump_text_hdr("[host] arg0", debug_t1);
    dump_text_hdr("[host] arg1", debug_t2);

    std::cout << "Calling strict_word_similarity()..." << std::endl;
    Datum result = strict_word_similarity(fcinfo);

    float sim = DatumGetFloat4(result);
    std::cout << "Strict Word Similarity: " << sim << std::endl;

    pfree(t1);
    pfree(t2);
}

void call_test_function(void* so, const char* name) {
    init_io_unbuffered();

    PGFunction similarity = (PGFunction)dlsym(so, name);
    if (!similarity) {
        std::cerr << "dlsym(" << name << ") failed: " << dlerror() << "\n";
        return;
    }

    void* t1 = build_text_from_env("Hello");
    void* t2 = build_text_from_env("Hallo");

    LOCAL_FCINFO(fcinfo, 2);
    InitFunctionCallInfoData(*fcinfo, nullptr, 2, 0 /*collation*/, nullptr, nullptr);

    fcinfo->args[0].value  = PointerGetDatum(t1);
    fcinfo->args[0].isnull = false;
    fcinfo->args[1].value  = PointerGetDatum(t2);
    fcinfo->args[1].isnull = false;

    std::cout << "Testing " << name << "()" << std::endl;
    Datum d = similarity(fcinfo);

    float sim = DatumGetFloat4(d);
    std::cout << "Result for " << name << ": " << sim << std::endl;

    pfree(t1);
    pfree(t2);
    pfree(fcinfo);
}

int main() {
    // Load shim library first
    void* shims = dlopen("libpgext.so", RTLD_NOW | RTLD_GLOBAL);
    if (!shims) {
        std::cerr << "Failed to load shims: " << dlerror() << std::endl;
        return 1;
    }

    // Load pg_trgm
    void* pgtrgm = dlopen("/tmp/pg_trgm.so", RTLD_NOW | RTLD_GLOBAL);
    // void* pgtrgm = dlopen("/usr/lib/postgresql/16/lib/pg_trgm.so", RTLD_NOW | RTLD_GLOBAL);

    if (!pgtrgm) {
        std::cerr << "Failed to load pg_trgm: " << dlerror() << std::endl;
        return 1;
    }

    call_test_function(pgtrgm, "similarity");
    call_test_function(pgtrgm, "word_similarity");
    call_test_function(pgtrgm, "strict_word_similarity");

    // test_similarity_function(pgtrgm);

    return 0;
}
