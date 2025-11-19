#pragma once

#include <pg_ext/string.hh>
#include <pg_ext/extn_registry.hh>

namespace springtail {
    std::string unpack_trigram_int_to_string(uint32_t v);

    std::vector<std::string> extract_trgm_from_value(const std::string& value,
            const std::string& opclass, int method_strategy_number);
}
