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

uint32_t
DatumGetInt32(Datum X)
{
	return (uint32_t) X;
}

float
DatumGetFloat4(Datum X)
{
	union
	{
		uint32_t	value;
		float		retval;
	} myunion;

	myunion.value = DatumGetInt32(X);
	return myunion.retval;
}

char *
DatumGetCString(Datum X)
{
	return (char *) (X);
}

void*
cstring_to_text_4b(const char *s) {
    size_t data_len = std::strlen(s);
    size_t tot = VARHDRSZ + data_len;
    char *p = (char*)std::malloc(tot);
    if (!p) {
        std::perror("malloc");
        std::exit(1);
    }
    SET_VARSIZE_4B(p, data_len + VARHDRSZ);
    std::memcpy(p + VARHDRSZ, s, data_len);
    return (void*)p;
}

void*
cstring_to_text_1b(const char *s) {
    size_t data_len = std::strlen(s);
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

void*
cstring_to_text_auto(const char *s) {
    size_t n = std::strlen(s);
    return (n <= 127) ? cstring_to_text_1b(s) : cstring_to_text_4b(s);
}

void
call_test_function(void* so, const char* name, const char* text1, const char* text2) {
    PGFunction test_function = (PGFunction)dlsym(so, name);
    if (!test_function) {
        std::cerr << "Failed to find function " << name << ": " << dlerror() << std::endl;
        return;
    }

    Datum d;
    LOCAL_FCINFO(fcinfo, 2);
    if ( text1 || text2 ) {
        void* t1 = cstring_to_text_auto(text1);

        InitFunctionCallInfoData(*fcinfo, nullptr, 2, 0, nullptr, nullptr);

        if ( text1 ) {
            fcinfo->args[0].value  = PointerGetDatum(t1);
            fcinfo->args[0].isnull = false;
        }

        if ( text2 ) {
            void* t2 = cstring_to_text_auto(text2);
            fcinfo->args[1].value  = PointerGetDatum(t2);
            fcinfo->args[1].isnull = false;
        }
    }

    d = test_function(fcinfo);
    if (strcmp(name, "similarity") == 0 ||
    strcmp(name, "word_similarity") == 0 ||
    strcmp(name, "strict_word_similarity") == 0) {
        std::cout << name << "('" << text1 << "', '" << text2 << "') = " << DatumGetFloat4(d) << std::endl;
    }
    else if (strcmp(name, "show_limit") == 0) {
        std::cout << name << "() = " << DatumGetFloat4(d) << std::endl;
    }
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

    // 1B Tests
    const char* t1 = "Hello there";
    const char* t2 = "Hallo dear";
    call_test_function(pgtrgm, "similarity", t1, t2);
    call_test_function(pgtrgm, "word_similarity", t1, t2);
    call_test_function(pgtrgm, "strict_word_similarity", t1, t2);

    // 4B Tests
    const char* t3 = "This is a length string that is determined to exceed the 127 byte limit to make use of the varlena extended header and it is not that very longer than the other string";
    const char* t4 = "This is a length string that is determined to exceed the 127 byte limit to make use of the varlena extended header and it is very very longer than the other string";
    call_test_function(pgtrgm, "similarity", t3, t4);
    call_test_function(pgtrgm, "word_similarity", t3, t4);
    call_test_function(pgtrgm, "strict_word_similarity", t3, t4);

    // // Test show_trgm and show_limit
    // call_test_function(pgtrgm, "show_trgm", "Hello there", nullptr);
    call_test_function(pgtrgm, "show_limit", nullptr, nullptr);

    // Clean up
    dlclose(pgtrgm);
    dlclose(shims);

    return 0;
}
