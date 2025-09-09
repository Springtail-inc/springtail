#include <nlohmann/json.hpp>
#include <pg_ext/extn_registry.hh>
#include <pg_repl/exception.hh>

#include <common/properties.hh>
#include <common/json.hh>

#include <dlfcn.h>

namespace springtail {

PGFunction
PgExtnRegistry::_load_extn_function(void* library, const std::string_view func_name)
{
    PGFunction extn_function = (PGFunction)dlsym(library, func_name.data());
    if (!extn_function) {
        LOG_ERROR("Failed to find function PGFunction {}", func_name);
        return nullptr;
    }
    return extn_function;
}

void
PgExtnRegistry::add_type(uint32_t oid, const std::string& typinput, const std::string& typoutput, const std::string& typreceive, const std::string& typsend)
{
    void* library = _load_library("/usr/lib/postgresql/16/lib/cube.so");

    _type_func_name_to_func.insert({typreceive, _load_extn_function(library, typreceive)});
    _type_func_name_to_func.insert({typsend, _load_extn_function(library, typsend)});
    _type_func_name_to_func.insert({typinput, _load_extn_function(library, typinput)});
    _type_func_name_to_func.insert({typoutput, _load_extn_function(library, typoutput)});

    _type_oid_to_type[oid] = {oid, typinput, typoutput, typreceive, typsend};
}

void
PgExtnRegistry::add_operator(uint32_t oid, const std::string& oper_name, const std::string& proc_name)
{
    void* library = _load_library("/usr/lib/postgresql/16/lib/cube.so");

    auto extn_function = _load_extn_function(library, proc_name);
    _oper_name_to_func.insert({oper_name, extn_function});
    _proc_name_to_func.insert({proc_name, extn_function});

    _oper_oid_to_name.insert({oid, oper_name});
    _proc_oid_to_name.insert({oid, proc_name});
}

PGFunction
PgExtnRegistry::get_operator_func_by_oid(uint32_t oid) const
{
    auto it = _oper_oid_to_name.find(oid);
    if (it == _oper_oid_to_name.end()) {
        LOG_ERROR("Failed to find operator function by oid: {}", oid);
        return nullptr;
    }
    return _oper_name_to_func.at(it->second);
}

PGFunction
PgExtnRegistry::get_operator_func_by_oper_name(const std::string& oper_name) const
{
    auto it = _oper_name_to_func.find(oper_name);
    if (it == _oper_name_to_func.end()) {
        LOG_ERROR("Failed to find operator function by oper name: {}", oper_name);
        return nullptr;
    }
    return it->second;
}

PGFunction
PgExtnRegistry::get_operator_func_by_proc_name(const std::string& proc_name) const
{
    auto it = _proc_name_to_func.find(proc_name);
    if (it == _proc_name_to_func.end()) {
        LOG_ERROR("Failed to find operator function by proc name: {}", proc_name);
        return nullptr;
    }
    return it->second;
}

PGFunction
PgExtnRegistry::get_type_func_by_type_name(const std::string& type_name) const
{
    auto it = _type_func_name_to_func.find(type_name);
    if (it == _type_func_name_to_func.end()) {
        LOG_ERROR("Failed to find type function by type name: {}", type_name);
        return nullptr;
    }
    return it->second;
}

PgType
PgExtnRegistry::get_type_by_oid(uint32_t oid) const
{
    auto it = _type_oid_to_type.find(oid);
    if (it == _type_oid_to_type.end()) {
        LOG_ERROR("Failed to find type by oid: {}", oid);
        return PgType();
    }
    return it->second;
}
void*
PgExtnRegistry::_load_library(const std::string_view lib_path)
{
    LOG_INFO("Loading library: {}", lib_path);

    void* library = dlopen(lib_path.data(), RTLD_NOW | RTLD_GLOBAL);
    if (!library) {
        LOG_ERROR("Failed to load library: {}", dlerror());
        return nullptr;
    }
    return library;
}
}  // namespace springtail
