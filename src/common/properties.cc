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
        std::ifstream ifs(file);
        _json = nlohmann::json::parse(ifs);
    }

    void
    Properties::init(const std::string &file)
    {
        if (_instance == nullptr) {
            _instance = new Properties(file);
        }
    }
}