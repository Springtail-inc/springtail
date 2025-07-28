#include <pg_ext/reloptions.hh>
#include <vector>
#include <mutex>

static std::vector<IntReloption> reloptions_registry;
static std::mutex registry_mutex;

void add_local_int_reloption(const char *name, const char *desc, int default_val,
                             int min_val, int max_val) {
    std::lock_guard<std::mutex> guard(registry_mutex);
    reloptions_registry.push_back(IntReloption{
        .name = name,
        .description = desc,
        .default_val = default_val,
        .min_val = min_val,
        .max_val = max_val
    });
}

IntReloption* get_local_int_reloption(const char *name) {
    std::lock_guard<std::mutex> guard(registry_mutex);
    for (auto& opt : reloptions_registry) {
        if (opt.name == std::string(name))
            return &opt;
    }
    return nullptr;
}

void init_local_reloptions(void) {
    // XXX Stubbed for now
}
