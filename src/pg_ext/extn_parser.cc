#include <fmt/format.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <dlfcn.h>

#include <common/logging.hh>

const std::vector<std::string> WHITELIST_EXTNS = {"pg_trgm","cube", "hstore"};

int
main()
{
    for ( auto extn : WHITELIST_EXTNS ) {
        auto lib_path = fmt::format("/usr/lib/postgresql/16/lib/{}.so", extn);

        LOG_INFO("Loading library: {}", lib_path);

        void* library = dlopen(lib_path.data(), RTLD_NOW | RTLD_GLOBAL);
        if (!library) {
            LOG_ERROR("Failed to load library: {}", dlerror());
            return 1;
        }
    }

    return 0;
}
