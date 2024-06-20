#include <string>
#include <fstream>
#include <stdexcept>

#include <nlohmann/json.hpp>

#include <common/exception.hh>
#include <common/properties.hh>

namespace springtail {

    Properties* Properties::_instance {nullptr};

    nlohmann::json 
    Properties::get(const std::string &key)
    {
        if (_instance == nullptr) {
            throw Error("Error Properties instance not initialized");
        }
        return _instance->_json[key];
    }

    Properties::Properties(const std::string &file)
    {
        // read the configuration file
        std::ifstream ifs(file);
        _json = nlohmann::json::parse(ifs);

        // check for overrides in the environment variables
        const char *var = std::getenv(SPRINGTAIL_ENV_VARIABLE);
        if (var == nullptr) {
            return; // no overrides, exit
        }

        std::string_view props(var);
        std::size_t pos = 0;
        while (pos < props.size()) {
            auto item = _json;
            std::string_view key, val;
            std::size_t start = pos;
            std::string next_token = ".=";
            do {
                std::size_t token = props.find_first_of(next_token, start);
                if (token == std::string_view::npos) {
                    token = props.size();
                }

                if (token == props.size() || props[token] == ';') {
                    assert(key.size());
                    val = props.substr(start, token - start);

                    // set the overridden value
                    // note: all override values are stored as strings
                    item[key] = val;
                } else if (props[token] == '=') {
                    key = props.substr(start, token - start);
                    next_token = ";";
                } else {
                    assert(props[token] == '.');
                    key = props.substr(start, token - start);
                    item = item[key];
                }

                start = token + 1;
            } while (val.empty() && start < props.size());

            // set the position to the start of the next entry
            pos = start;
        }
    }

    void
    Properties::init(const std::string &file)
    {
        if (_instance == nullptr) {
            _instance = new Properties(file);
        }
    }
}
