#pragma once

namespace pgext {

enum GucContext : int {
    PGC_INTERNAL,
    PGC_POSTMASTER,
    PGC_SIGHUP,
    PGC_SUSET,
    PGC_USERSET
};

enum GucSource : int {
    PGC_S_DEFAULT,
    PGC_S_DYNAMIC_DEFAULT,
    PGC_S_ENV_VAR,
    PGC_S_FILE,
    PGC_S_ARGV,
    PGC_S_GLOBAL,
    PGC_S_DATABASE,
    PGC_S_USER,
    PGC_S_DATABASE_USER,
    PGC_S_CLIENT,
    PGC_S_OVERRIDE,
    PGC_S_INTERACTIVE,
    PGC_S_TEST,
    PGC_S_SESSION
};

class GucManager {
public:
    void define_real(std::string_view name, double *value_addr);
    void set_option();
    void reserve_prefix(std::string_view prefix);

private:
    absl::flat_hash_map<>;
};

}

extern "C" PGEXT_API void DefineCustomRealVariable(const char *name,
                                                   const char *short_desc,
                                                   const char *long_desc,
                                                   double *valueAddr,
                                                   double defaultVal,
                                                   double minVal,
                                                   double maxVal,
                                                   int context,
                                                   int flags,
                                                   void *check_hook, // GucRealCheckHook
                                                   void *assign_hook, // GucRealAssignHook
                                                   void *show_hook); // GucShowHook
extern "C" PGEXT_API void SetConfigOption(const char *name,
                                          const char *value,
                                          int context,
                                          int source);
extern "C" PGEXT_API void MarkGUCPrefixReserved(const char *prefix);
