#include <pg_ext/export.hh>
#include <string>

struct IntReloption {
    std::string name;
    std::string description;
    int default_val;
    int min_val;
    int max_val;
};

extern "C" PGEXT_API void add_local_int_reloption(const char *name, const char *desc, int default_val,
    int min_val, int max_val);

extern "C" PGEXT_API IntReloption* get_local_int_reloption(const char *name);
extern "C" PGEXT_API void init_local_reloptions(void);
