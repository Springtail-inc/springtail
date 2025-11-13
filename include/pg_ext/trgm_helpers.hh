#pragma once

#include <pg_ext/varatt.hh>
#include <pg_ext/extn_registry.hh>
#include <common/constants.hh>

namespace springtail {
    std::string unpack_trigram_int_to_string(uint32_t v);

    void* cstring_to_text_4b(const char *s);

    void* cstring_to_text_1b(const char *s);

    void* cstring_to_text_auto(const char *s);

    std::vector<std::string> extract_trgm_from_value(const std::string& value,
            const std::string& opclass, int method_strategy_number);
}
