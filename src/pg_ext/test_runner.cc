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

void dump_bytes(const char* label, void* ptr, size_t len) {
    std::cout << label << " @ " << ptr << ": ";
    auto* p = reinterpret_cast<unsigned char*>(ptr);
    for (size_t i = 0; i < len; ++i)
        std::printf("%02x ", p[i]);
    std::cout << std::endl;
}

int main() {
    // Load shim library first
    void* shims = dlopen("src/pg_ext/libpgext.so", RTLD_NOW | RTLD_GLOBAL);
    if (!shims) {
        std::cerr << "Failed to load shims: " << dlerror() << std::endl;
        return 1;
    }

    // Load pg_trgm
    void* pgtrgm = dlopen("/usr/lib/postgresql/16/lib/pg_trgm.so", RTLD_NOW | RTLD_GLOBAL);
    if (!pgtrgm) {
        std::cerr << "Failed to load pg_trgm: " << dlerror() << std::endl;
        return 1;
    }

    // Load similarity() function
    PGFunction similarity = (PGFunction)dlsym(pgtrgm, "similarity");
    if (!similarity) {
        std::cerr << "Failed to find similarity: " << dlerror() << std::endl;
        return 1;
    }

    Datum d1 = PointerGetDatum("Hello");
    Datum d2 = PointerGetDatum("Hallo");

    LOCAL_FCINFO(fcinfo, 2);

    InitFunctionCallInfoData(*fcinfo, NULL, 2, 0, NULL, NULL);

    fcinfo->args[0].value = d1;
    fcinfo->args[0].isnull = false;
    fcinfo->args[1].value = d2;
    fcinfo->args[1].isnull = false;

    std::cout << "Calling similarity()..." << std::endl;
    Datum result = similarity(fcinfo);

    float sim;
    memcpy(&sim, &result, sizeof(float));
    std::cout << "Similarity: " << sim << std::endl;

    return 0;
}
