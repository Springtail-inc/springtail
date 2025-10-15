#include <pg_ext/reloptions.hh>
#include <vector>
#include <mutex>

std::vector<IntReloption> _reloptions_registry;
std::mutex _registry_mutex;

void add_local_int_reloption(const char *name, const char *desc, int default_val,
                             int min_val, int max_val) {
    std::lock_guard guard(_registry_mutex);
    _reloptions_registry.push_back(IntReloption{
        .name = name,
        .description = desc,
        .default_val = default_val,
        .min_val = min_val,
        .max_val = max_val
    });
}

IntReloption* get_local_int_reloption(const char *name) {
    std::lock_guard guard(_registry_mutex);
    for (auto& opt : _reloptions_registry) {
        if (opt.name == std::string(name))
            return &opt;
    }
    return nullptr;
}

void
init_local_reloptions(local_relopts *relopts, Size relopt_struct_size)
{
    relopts->options = nullptr;
    relopts->validators = nullptr;
    relopts->relopt_struct_size = relopt_struct_size;
}
