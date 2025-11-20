#pragma once

#include <optional>

#include <nlohmann/json.hpp>
#include <fmt/core.h>

#include <common/exception.hh>
#include <common/logging.hh>

namespace springtail {
    /**
     * @brief Helper json class to extract json values
     */
    class Json {
    public:
        /**
         * @brief Get value from json blob, default value assigned if no value exists or value is null
         * @tparam T type of value
         * @param json input json blob
         * @param key  json key to lookup
         * @param def_value default value if key doesn't exist or is assigned null
         * @return value (from json, otherwise def_value) of type T. On json type mistamtch,
         * then it will be a default-constructed T.
         */
        template<typename T> static inline T
        get_or(const nlohmann::json &json, const std::string &key, const T &def_value)
        {
            if (json.is_null() || !json.contains(key) || json[key].is_null()) {
                return def_value;
            }

            T result;
            _get_to_helper(json[key], result);
            return result;
        }

        /**
         * @brief Get value from json blob, no default value
         * @tparam T type of value
         * @param json input json blob
         * @param key  json key to lookup
         * @param result reference to result
         * @return true a value is assigned to result
         * @return false a value of null was stored in the json; or key not found
         */
        template<typename T> static inline bool
        get_to(const nlohmann::json &json, const std::string &key, T &result)
        {
            if (json.is_null() || !json.contains(key) || json[key].is_null()) {
                return false;
            }

            _get_to_helper(json[key], result);
            return true;
        }

        /**
         * @brief Get value from json blob, no default value
         * @tparam T type of value
         * @param json input json blob
         * @param key  json key to lookup
         * @return optional of type T
         */
        template<typename T> static inline std::optional<T>
        get(const nlohmann::json &json, const std::string &key)
        {
            if (json.is_null() || !json.contains(key) || json[key].is_null()) {
                return std::nullopt;
            }

            try {
                return json[key].get<T>();
            } catch (const nlohmann::json::type_error& e) {
                LOG_WARN("Bad conversion for key '{}' - {}", key, e.what());
                return std::nullopt;  // Return nullopt on failed conversion
            }
        }

        /**
         * @brief Get the vector from json
         *
         * @tparam T - type of vector element
         * @param json - json array
         * @return std::vector<T> - vector from json array
         */
        template<typename T> static inline std::vector<T>
        get_vector(const nlohmann::json &json)
        {
            DCHECK(json.is_array());
            if (!json.is_array()) {
                return {};
            }

            return json.get<std::vector<T>>();
        }

    private:
        template<typename T> static inline void
        _get_to_helper(const nlohmann::json &json, T &result) {
            if constexpr (std::is_same_v<T, bool>) {
                if (json.is_string()) {
                    std::string val;
                    json.get_to(val);
                    result = (val == "true" || val == "1");
                } else {
                    result = json.get<bool>();
                }
                return;
            }

            if constexpr (std::is_same_v<T, char>) {
                if (json.is_string()) {
                    std::string val;
                    json.get_to(val);
                    result = val[0];
                } else {
                    result = json.get<char>();
                }
                return;
            }

            if constexpr(std::is_integral_v<T>) {
                if (json.is_string()) {
                    std::string val;
                    json.get_to(val);
                    result = std::stoll(val);
                } else {
                    json.get_to(result);
                }
                return;
            }

            json.get_to(result);
        }
    };
}
