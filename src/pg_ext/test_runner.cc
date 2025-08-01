#include <iostream>
#include <dlfcn.h>
#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <pg_ext/fmgr.hh>
#include <pg_ext/string.hh>
#include <stdint.h>

Datum
PointerGetDatum(const void *X)
{
	return (Datum) X;
}

float DatumGetFloat4(Datum d) {
    float f;
    std::memcpy(&f, &d, sizeof(float));
    return f;
}

static void* cstring_to_text_4b(const char *s) {
    size_t n = std::strlen(s);
    size_t tot = VARHDRSZ + n;
    unsigned char *p = (unsigned char*)std::malloc(tot);
    if (!p) { std::perror("malloc"); std::exit(1); }
    int32_t len = (int32_t)tot;
    std::memcpy(p, &len, 4);         // total length incl header
    std::memcpy(p + 4, s, n);
    return (void*)p;
}

static void* cstring_to_text_1b(const char *s) {
    size_t data_len = std::strlen(s);
    if (data_len > 127) {
        // fall back to 4B when too large for 1B
        return cstring_to_text_4b(s);
    }
    size_t tot = VARHDRSZ + data_len;
    char *p = (char*)std::malloc(tot);
    if (!p) {
        std::perror("malloc");
        std::exit(1);
    }
    SET_VARSIZE_1B(p, data_len + VARHDRSZ_SHORT);
    std::memcpy(p + VARHDRSZ_SHORT, s, data_len);
    return (void*)p;
}

static void* cstring_to_text_auto(const char *s) {
    size_t n = std::strlen(s);
    return (n <= 127) ? cstring_to_text_1b(s) : cstring_to_text_4b(s);
}

void call_test_function(void* so, const char* name) {
    PGFunction test_function = (PGFunction)dlsym(so, name);
    if (!test_function) {
        std::cerr << "Failed to find function " << name << ": " << dlerror() << std::endl;
        return;
    }

    // Create text values with proper varlena headers
    void* t1 = cstring_to_text_auto("Hello there");
    void* t2 = cstring_to_text_auto("Hallo dear");

    LOCAL_FCINFO(fcinfo, 2);
    InitFunctionCallInfoData(*fcinfo, nullptr, 2, 0 /*collation*/, nullptr, nullptr);

    fcinfo->args[0].value  = PointerGetDatum(t1);
    fcinfo->args[0].isnull = false;
    fcinfo->args[1].value  = PointerGetDatum(t2);
    fcinfo->args[1].isnull = false;

    std::cout << "Testing " << name << "()" << std::endl;
    Datum d = test_function(fcinfo);

    float sim = DatumGetFloat4(d);
    std::cout << "Result for " << name << ": " << sim << std::endl;

    std::free(t1);
    std::free(t2);
}

int main() {
    // Load shim library first
    void* shims = dlopen("libpgext.so", RTLD_NOW | RTLD_GLOBAL);
    if (!shims) {
        std::cerr << "Failed to load shims: " << dlerror() << std::endl;
        return 1;
    }

    // Load pg_trgm
    // void* pgtrgm = dlopen("/tmp/pg_trgm.so", RTLD_NOW | RTLD_GLOBAL);
    void* pgtrgm = dlopen("/usr/lib/postgresql/16/lib/pg_trgm.so", RTLD_NOW | RTLD_GLOBAL);

    if (!pgtrgm) {
        std::cerr << "Failed to load pg_trgm: " << dlerror() << std::endl;
        dlclose(shims);
        return 1;
    }

    call_test_function(pgtrgm, "similarity");
    call_test_function(pgtrgm, "word_similarity");
    call_test_function(pgtrgm, "strict_word_similarity");

    // Clean up
    dlclose(pgtrgm);
    dlclose(shims);

    return 0;
}
