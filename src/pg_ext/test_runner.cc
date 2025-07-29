#include <iostream>
#include <dlfcn.h>
#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <pg_ext/fmgr.hh>

using similarity_fn = Datum (*)(FunctionCallInfoData*);
inline Datum PointerGetDatum(const void* ptr) {
    return reinterpret_cast<Datum>(ptr);
}

// Redefine basic Postgres-compatible text type layout
typedef struct {
    int32_t vl_len_;  // Length of the structure including itself
    char data[];      // Flexible array member for the actual string
} text;

#define DatumGetFloat4(X)  (*((float *) &(X)))
#define VARHDRSZ ((int32_t)sizeof(int32_t))

#define SET_VARSIZE(PTR, len) (((text *)(PTR))->vl_len_ = (len))
#define VARDATA(PTR) (((text *)(PTR))->data)

static text* cstring_to_text(const char* cstr) {
    int len = strlen(cstr);
    text* t = (text*) malloc(VARHDRSZ + len);
    SET_VARSIZE(t, VARHDRSZ + len);
    memcpy(VARDATA(t), cstr, len);
    return t;
}

void dump_bytes(const char* label, void* ptr, size_t len) {
    std::cout << label << " @ " << ptr << ": ";
    auto* p = reinterpret_cast<unsigned char*>(ptr);
    for (size_t i = 0; i < len; ++i)
        std::printf("%02x ", p[i]);
    std::cout << std::endl;
}

void test_singularity_function(void* pgtrgm){
    // Load similarity() function
    PGFunction similarity = (PGFunction)dlsym(pgtrgm, "similarity");
    if (!similarity) {
        std::cerr << "Failed to find similarity: " << dlerror() << std::endl;
    }

    Datum d1 = PointerGetDatum(cstring_to_text("Hello"));
    Datum d2 = PointerGetDatum(cstring_to_text("Hello"));

    LOCAL_FCINFO(fcinfo, 2);

    InitFunctionCallInfoData(*fcinfo, NULL, 2, 0, NULL, NULL);

    fcinfo->args[0].value = d1;
    fcinfo->args[0].isnull = false;
    fcinfo->args[1].value = d2;
    fcinfo->args[1].isnull = false;

    std::cout << "Calling similarity()..." << std::endl;
    Datum result = similarity(fcinfo);

    float sim = DatumGetFloat4(result);
    std::cout << "Similarity: " << sim << std::endl;
}

void test_word_similarity_function(void* pgtrgm){
    // Load word_similarity() function
    PGFunction word_similarity = (PGFunction)dlsym(pgtrgm, "word_similarity");
    if (!word_similarity) {
        std::cerr << "Failed to find word_similarity: " << dlerror() << std::endl;
    }

    Datum d1 = PointerGetDatum(cstring_to_text("Hello"));
    Datum d2 = PointerGetDatum(cstring_to_text("Hello"));

    LOCAL_FCINFO(fcinfo, 2);

    InitFunctionCallInfoData(*fcinfo, NULL, 2, 0, NULL, NULL);

    fcinfo->args[0].value = d1;
    fcinfo->args[0].isnull = false;
    fcinfo->args[1].value = d2;
    fcinfo->args[1].isnull = false;

    std::cout << "Calling word_similarity()..." << std::endl;
    Datum result = word_similarity(fcinfo);

    float sim = DatumGetFloat4(result);
    std::cout << "Word Similarity: " << sim << std::endl;
}

void test_strict_word_similarity_function(void* pgtrgm){
    // Load strict_word_similarity() function
    PGFunction strict_word_similarity = (PGFunction)dlsym(pgtrgm, "strict_word_similarity");
    if (!strict_word_similarity) {
        std::cerr << "Failed to find strict_word_similarity: " << dlerror() << std::endl;
    }

    Datum d1 = PointerGetDatum(cstring_to_text("Hello"));
    Datum d2 = PointerGetDatum(cstring_to_text("Hello"));

    LOCAL_FCINFO(fcinfo, 2);

    InitFunctionCallInfoData(*fcinfo, NULL, 2, 0, NULL, NULL);

    fcinfo->args[0].value = d1;
    fcinfo->args[0].isnull = false;
    fcinfo->args[1].value = d2;
    fcinfo->args[1].isnull = false;

    std::cout << "Calling strict_word_similarity()..." << std::endl;
    Datum result = strict_word_similarity(fcinfo);

    float sim = DatumGetFloat4(result);
    std::cout << "Strict Word Similarity: " << sim << std::endl;
}


int main() {
    // Load shim library first
    void* shims = dlopen("libpgext.so", RTLD_NOW | RTLD_GLOBAL);
    if (!shims) {
        std::cerr << "Failed to load shims: " << dlerror() << std::endl;
        return 1;
    }

    // Load pg_trgm
    void* pgtrgm = dlopen("/usr/lib/postgresql/16/lib/pg_trgm.so", RTLD_NOW | RTLD_GLOBAL);
    // void* pgtrgm = dlopen("/tmp/pg_trgm.so", RTLD_NOW | RTLD_GLOBAL);

    if (!pgtrgm) {
        std::cerr << "Failed to load pg_trgm: " << dlerror() << std::endl;
        return 1;
    }

    test_singularity_function(pgtrgm);
    test_word_similarity_function(pgtrgm);
    test_strict_word_similarity_function(pgtrgm);

    return 0;
}
