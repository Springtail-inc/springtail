#pragma once

#include <pg_ext/export.hh>

#include <string>
#include <string_view>

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

namespace pgext {

enum class GucContext : int {
    PGC_INTERNAL,
    PGC_POSTMASTER,
    PGC_SIGHUP,
    PGC_SUSET,
    PGC_USERSET
};

enum class GucSource : int {
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

/**
 * Simplified implementation of Postgres Grand-Unified-Configuration (GUC)
 */
class GucManager {
public:
    enum class Type : int {
        REAL = 0
    };

    void define_real(std::string_view name, double *value_addr);
    void set_option(std::string_view name, std::string_view value, GucContext context = PGC_USERSET);
    void reserve_prefix(std::string_view prefix);

private:
    struct Variable {
        Type type;
        void* value_addr;
    };

    absl::flat_hash_map<std::string, Variable> _vars;
    absl::flat_hash_set<std::string> _prefixes;
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
