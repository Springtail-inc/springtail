#include <iostream>
#include <dlfcn.h>
#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <iomanip>
#include <pg_ext/fmgr.hh>
#include <pg_ext/string.hh>
#include <pg_ext/array.hh>
#include "pg_ext/guc.hh"

typedef char GinTernaryValue;


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

void print_bytea_hex(Datum bytea_datum) {
    struct varlena* v = (struct varlena*)DatumGetPointer(bytea_datum);
    if (!v) {
        std::cout << "NULL";
        return;
    }

    size_t len = VARSIZE_ANY_EXHDR(v);
    char* data = VARDATA_ANY(v);

    std::cout << "\\x";
    for (size_t i = 0; i < len; i++) {
        std::cout << std::hex << std::setw(2) << std::setfill('0')
                 << (static_cast<int>(data[i]) & 0xff);
    }
    std::cout << std::dec;
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

    // Initialize function call info
    if (text1 || text2) {
        void* t1 = text1 ? cstring_to_text_auto(text1) : nullptr;
        void* t2 = text2 ? cstring_to_text_auto(text2) : nullptr;

        InitFunctionCallInfoData(*fcinfo, nullptr, 2, 0, nullptr, nullptr);

        if (text1) {
            fcinfo->args[0].value = PointerGetDatum(t1);
            fcinfo->args[0].isnull = false;
        }

        if (text2) {
            fcinfo->args[1].value = PointerGetDatum(t2);
            fcinfo->args[1].isnull = false;
        }
    } else {
        // For functions with no arguments
        InitFunctionCallInfoData(*fcinfo, nullptr, 0, 0, nullptr, nullptr);
    }

    d = test_function(fcinfo);

    // Handle different function types based on name patterns
    if (strstr(name, "similarity") != nullptr) {
        // All similarity functions return float4
        std::cout << name << "('";
        if (text1) std::cout << text1;
        std::cout << "'";
        if (text2) std::cout << ", '" << text2 << "'";
        std::cout << ") = " << DatumGetFloat4(d) << std::endl;
    }
    else if (strcmp(name, "show_limit") == 0) {
        std::cout << name << "() = " << DatumGetFloat4(d) << std::endl;
    }
    else if (strcmp(name, "show_trgm") == 0 && text1) {
        std::cout << name << "('" << text1 << "') = ";
        // print_trgm_array(d);
        std::cout << std::endl;
    }
    else if (strstr(name, "set_limit") != nullptr) {
        std::cout << name << "() called successfully" << std::endl;
    }
    else if (strncmp(name, "gtrgm_consistent", 16) == 0) {
        std::cout << name << "() = " << (DatumGetBool(d) ? "true" : "false") << std::endl;
    }
    else if (strncmp(name, "gtrgm_compress", 14) == 0 ||
             strncmp(name, "gtrgm_decompress", 16) == 0 ||
             strncmp(name, "gtrgm_picksplit", 15) == 0 ||
             strncmp(name, "gtrgm_union", 11) == 0 ||
             strncmp(name, "gtrgm_same", 10) == 0) {
        std::cout << name << "() = ";
        print_bytea_hex(d);
        std::cout << std::endl;
    }
    else if (strncmp(name, "gtrgm_distance", 14) == 0) {
        std::cout << name << "() = " << DatumGetFloat8(d) << std::endl;
    }
    else if (strncmp(name, "gtrgm_inet_consistent", 20) == 0) {
        std::cout << name << "() = " << (DatumGetBool(d) ? "true" : "false") << std::endl;
    }
    else {
        // Default output for other functions
        std::cout << name << " called successfully" << std::endl;
    }
}

void
test_gin_extract_value_trgm(void* pgtrgm, const char* text1){
    PGFunction test_function = (PGFunction)dlsym(pgtrgm, "gin_extract_value_trgm");
    if (!test_function) {
        std::cerr << "Failed to find function gin_extract_value_trgm" << std::endl;
        return;
    }

    Datum d;
    LOCAL_FCINFO(fcinfo, 2);

    const char* text2 = "10";
    void* t1 = cstring_to_text_auto(text1);
    void* t2 = cstring_to_text_auto(text2);

    InitFunctionCallInfoData(*fcinfo, nullptr, 2, 0, nullptr, nullptr);

    fcinfo->args[0].value = PointerGetDatum(t1);
    fcinfo->args[0].isnull = false;

    fcinfo->args[1].value = PointerGetDatum(t2);
    fcinfo->args[1].isnull = false;

    d = test_function(fcinfo);

    std::cout << DatumGetFloat4(d) << std::endl;
}

void test_gtrgm_functions(void* pgtrgm, const char* text1, const char* text2) {
    std::cout << "\n=== Testing GIN/GiST Support Functions ===\n";

    // Test with both text arguments
    call_test_function(pgtrgm, "gtrgm_consistent", text1, text2);
    call_test_function(pgtrgm, "gtrgm_compress", text1, nullptr);
    call_test_function(pgtrgm, "gtrgm_decompress", text1, nullptr);
    call_test_function(pgtrgm, "gtrgm_union", text1, text2);
    call_test_function(pgtrgm, "gtrgm_same", text1, text1);  // Same text
    call_test_function(pgtrgm, "gtrgm_same", text1, text2);  // Different text
    call_test_function(pgtrgm, "gtrgm_distance", text1, text2);
    call_test_function(pgtrgm, "gtrgm_picksplit", text1, text2);
    call_test_function(pgtrgm, "gtrgm_inet_consistent", text1, text2);

    // Test with empty string
    std::cout << "\n=== Testing with Empty String ===\n";
    call_test_function(pgtrgm, "gtrgm_consistent", "", text2);
    call_test_function(pgtrgm, "gtrgm_compress", "", nullptr);

    // Test with NULL inputs (where applicable)
    std::cout << "\n=== Testing with NULL Input ===\n";
    call_test_function(pgtrgm, "gtrgm_consistent", nullptr, text2);
}

void init_trgm(double similarity_threshold, double word_similarity_threshold, double strict_word_similarity_threshold){

    DefineCustomRealVariable("pg_trgm.similarity_threshold",
        "Sets the threshold used by the % operator.",
        "Valid range is 0.0 .. 1.0.",
        &similarity_threshold,
        0.3f,
        0.0,
        1.0,
        pgext::PGC_USERSET,
        0,
        NULL,
        NULL,
        NULL);
    DefineCustomRealVariable("pg_trgm.word_similarity_threshold",
            "Sets the threshold used by the <% operator.",
            "Valid range is 0.0 .. 1.0.",
            &word_similarity_threshold,
            0.6f,
            0.0,
            1.0,
            pgext::PGC_USERSET,
            0,
            NULL,
            NULL,
            NULL);
    DefineCustomRealVariable("pg_trgm.strict_word_similarity_threshold",
            "Sets the threshold used by the <<% operator.",
            "Valid range is 0.0 .. 1.0.",
            &strict_word_similarity_threshold,
            0.5f,
            0.0,
            1.0,
            pgext::PGC_USERSET,
            0,
            NULL,
            NULL,
            NULL);

    MarkGUCPrefixReserved("pg_trgm");
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
    if (!pgtrgm) {
        std::cerr << "Failed to load pg_trgm: " << dlerror() << std::endl;
        dlclose(shims);
        return 1;
    }

    // Test strings
    const char* t1 = "Hello there";
    const char* t2 = "Hallo dear";
    const char* t3 = "This is a length string that is determined to exceed the 127 byte limit to make use of the varlena extended header and it is not that very longer than the other string";
    const char* t4 = "This is a length string that is determined to exceed the 127 byte limit to make use of the varlena extended header and it is very very longer than the other string";

    double		similarity_threshold = 0.3f;
    double		word_similarity_threshold = 0.6f;
    double		strict_word_similarity_threshold = 0.5f;
    init_trgm(similarity_threshold, word_similarity_threshold, strict_word_similarity_threshold);

    call_test_function(pgtrgm, "similarity", t1, t2);

    // test_gin_extract_value_trgm(pgtrgm, t1);

    // 1. Test similarity functions
    std::cout << "\n=== Testing Similarity Functions (1B strings) ===\n";
    call_test_function(pgtrgm, "similarity", t1, t2);
    call_test_function(pgtrgm, "similarity_op", t1, t2);
    call_test_function(pgtrgm, "similarity_dist", t1, t2);

    // 2. Test word similarity functions
    std::cout << "\n=== Testing Word Similarity Functions (1B strings) ===\n";
    call_test_function(pgtrgm, "word_similarity", t1, t2);
    call_test_function(pgtrgm, "word_similarity_op", t1, t2);
    call_test_function(pgtrgm, "word_similarity_commutator_op", t1, t2);
    call_test_function(pgtrgm, "word_similarity_dist_op", t1, t2);
    call_test_function(pgtrgm, "word_similarity_dist_commutator_op", t1, t2);

    // 3. Test strict word similarity functions
    std::cout << "\n=== Testing Strict Word Similarity Functions (1B strings) ===\n";
    call_test_function(pgtrgm, "strict_word_similarity", t1, t2);
    call_test_function(pgtrgm, "strict_word_similarity_op", t1, t2);
    call_test_function(pgtrgm, "strict_word_similarity_commutator_op", t1, t2);
    call_test_function(pgtrgm, "strict_word_similarity_dist_op", t1, t2);
    call_test_function(pgtrgm, "strict_word_similarity_dist_commutator_op", t1, t2);

    // 4. Test with 4B strings
    std::cout << "\n=== Testing with 4B Strings ===\n";
    call_test_function(pgtrgm, "similarity", t3, t4);
    call_test_function(pgtrgm, "word_similarity", t3, t4);
    call_test_function(pgtrgm, "strict_word_similarity", t3, t4);

    // 5. Test show functions
    std::cout << "\n=== Testing Show Functions ===\n";
    call_test_function(pgtrgm, "show_trgm", t1, nullptr);
    call_test_function(pgtrgm, "show_limit", nullptr, nullptr);


    // 6. Test set_limit
    std::cout << "\n=== Testing Set Limit ===\n";
    call_test_function(pgtrgm, "set_limit", "0.3", nullptr);
    call_test_function(pgtrgm, "show_limit", nullptr, nullptr);
    call_test_function(pgtrgm, "set_limit", "0.7", nullptr);
    call_test_function(pgtrgm, "show_limit", nullptr, nullptr);

    // 7. Test GIN/GiST support functions with different inputs
    // test_gtrgm_functions(pgtrgm, t1, t2);

    // 8. Test with 4B strings
    // test_gtrgm_functions(pgtrgm, t3, t4);

    // Clean up
    dlclose(pgtrgm);
    dlclose(shims);

    return 0;
}
