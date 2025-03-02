#pragma once

#include <string>
#include <cctype>
#include <iostream>
#include <regex>

#include <common/common.hh>

namespace springtail::pg_proxy::util {

    /**
     * @brief Is the key a valid PostgreSQL (SET) key
     * @param key key to check
     * @return true if valid
     * @return false if invalid
     */
    static inline bool
    is_valid_postgres_key(const std::string& key)
    {
        // Regex for valid PostgreSQL key names
        // XXX Currently ignoring quoted identifiers
        std::regex valid_key_regex(R"(^[a-zA-Z_][a-zA-Z0-9_.]{0,62}$)");
        return std::regex_match(key, valid_key_regex);
    }

    /**
     * @brief Is the value a valid PostgreSQL (SET) value
     * @param value value to check
     * @return true if valid
     * @return false if invalid
     */
    static inline bool
    is_valid_postgres_value(const std::string& value)
    {
        // Regex for general PostgreSQL SET values
        std::regex valid_value_regex(R"(^([a-zA-Z0-9_.+-]+|'([^']|'')*'|".*")$)");
        return std::regex_match(value, valid_value_regex);
    }

    /**
     * @brief Quote a value for PostgreSQL (SET), tries to detect if quoting is necessary
     * @param value value to quote
     * @return std::string quoted value
     */
    static inline std::string
    quote_postgres_value(const std::string& value)
    {
        // Check if the value is safe as an unquoted identifier (alphanumeric and underscores)
        bool is_safe_unquoted = !value.empty() &&
                                (std::isalpha(value[0]) || value[0] == '_') &&
                                std::all_of(value.begin() + 1, value.end(), [](unsigned char c) {
                                    return std::isalnum(c) || c == '_';
                                });

        // Return the value as-is if it doesn't require quoting
        if (is_safe_unquoted || is_valid_postgres_value(value)) {
            return value;
        }

        // Otherwise, add single quotes and escape any existing single quotes in the value
        std::string quoted_value = "'";
        for (char c : value) {
            if (c == '\'') {
                quoted_value += "''"; // Escape single quote by doubling it
            } else {
                quoted_value += c;
            }
        }
        quoted_value += "'";

        return quoted_value;
    }

    /**
     * @brief Parse options from a string
     * Used in postgres startup (e.g., -c key1=value1 -c key2=value2)
     * @param input input string
     * @return std::map<std::string, std::string> map of options
     */
    std::map<std::string, std::string>
    parse_options(const std::string& input)
    {
        std::map<std::string, std::string> options;
        size_t pos = 0;
        size_t next_pos;

        while ((next_pos = input.find("-c", pos)) != std::string::npos) {
            size_t start = next_pos + 2; // Move past "-c"
            // Skip any leading whitespace after "-c"
            while (start < input.size() && input[start] == ' ') {
                ++start;
            }

            // Find the end of this key-value pair (next space or end of string)
            size_t end = input.find("-c", start);
            std::string key_value = input.substr(start, end - start);

            // Find the '=' in the key-value pair
            size_t equal_pos = key_value.find('=');
            if (equal_pos != std::string::npos) {
                // Extract key and value using substr
                std::string key = common::trim(key_value.substr(0, equal_pos));
                std::string value = common::trim(key_value.substr(equal_pos + 1));

                // Add to the map
                options[key] = quote_postgres_value(value);
            }

            // Move to the next part of the string
            pos = end;
        }

        return options;
    }

} // namespace springtail::pg_proxy::util
