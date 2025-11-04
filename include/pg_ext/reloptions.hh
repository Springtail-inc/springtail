#include <pg_ext/export.hh>
#include <pg_ext/list.hh>
#include <string>
#include <mutex>

struct IntReloption {
    std::string name;
    std::string description;
    int default_val;
    int min_val;
    int max_val;
};

struct local_relopts {
    List *options;           /* list of local_relopt definitions */
    List *validators;        /* list of relopts_validator callbacks */
    Size relopt_struct_size; /* size of parsed bytea structure */
};

extern "C" PGEXT_API void add_local_int_reloption(const char *name, const char *desc, int default_val, int min_val, int max_val);
extern "C" PGEXT_API IntReloption* get_local_int_reloption(const char *name);
extern "C" PGEXT_API void init_local_reloptions(local_relopts *relopts, Size relopt_struct_size);
