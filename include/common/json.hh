#pragma once

#include <nlohmann/json.hpp>
#include <fmt/core.h>

#include <common/exception.hh>

namespace springtail {
    /**
     * @brief Helper json class to extract json values
     */
    class Json {
    public:
        /**
         * @brief Get value from json blob, default value assigned if no value exists
         * @tparam T type of value
         * @param json input json blob
         * @param key  json key to lookup
         * @param result reference to result
         * @param def_value default value if key doesn't exist; if key is null false is returned
         * @return true a value is assigned to result (default or otherwise)
         * @return false a value of null was stored in the json
         */
        template<typename T> static inline bool
        get(const nlohmann::json &json, const std::string &key, T &result, const T &def_value)
        {
            if (json.is_null() || !json.contains(key)) {
                result = def_value;
                return true;
            }

            if (json[key].is_null()) {
                return false;
            }

            json[key].get_to(result);
            return true;
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
        get(const nlohmann::json &json, const std::string &key, T &result)
        {
            if (json.is_null() || !json.contains(key) || json[key].is_null()) {
                return false;
            }

            json[key].get_to(result);
            return true;
        }
    };
}