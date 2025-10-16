#include <nlohmann/json.hpp>
#include <pg_ext/fmgr.hh>
#include <pg_ext/string.hh>
#include <pg_ext/extn_registry.hh>
#include <pg_repl/exception.hh>

#include <common/properties.hh>
#include <common/json.hh>

#include <dlfcn.h>

namespace springtail {
void
PgExtnRegistry::add_type(const std::string& extension, uint32_t oid, const std::string& typinput, const std::string& typoutput, const std::string& typreceive, const std::string& typsend)
{
    auto library = _library_map.at(extension);

    _type_func_name_to_func.try_emplace(typreceive, _load_extn_function(library, typreceive));
    _type_func_name_to_func.try_emplace(typsend, _load_extn_function(library, typsend));
    _type_func_name_to_func.try_emplace(typinput, _load_extn_function(library, typinput));
    _type_func_name_to_func.try_emplace(typoutput, _load_extn_function(library, typoutput));

    _type_oid_to_type[oid] = {oid, typinput, typoutput, typreceive, typsend};
}

void
PgExtnRegistry::add_operator(const std::string& extension, uint32_t oid, const std::string& oper_name, const std::string& proc_name)
{
    auto library = _library_map.at(extension);

    auto extn_function = _load_extn_function(library, proc_name);
    _oper_name_to_func.try_emplace(oper_name, extn_function);
    _proc_name_to_func.try_emplace(proc_name, extn_function);

    _oper_oid_to_name.try_emplace(oid, oper_name);
    _proc_oid_to_name.try_emplace(oid, proc_name);
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
PgExtnRegistry::get_operator_func_by_oper_name(const char* oper_name) const
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

void
PgExtnRegistry::init_libraries(uint64_t db_id,
                               const std::string& extension,
                               const std::string& extension_lib_path)
{
    LOG_DEBUG(LOG_FDW, LOG_LEVEL_DEBUG2, "Creating extension: {} from lib_path: {} for db_id: {}", extension, extension_lib_path, db_id);

    auto library = _load_library(extension_lib_path);

    _library_map.try_emplace(extension, library);
}

bool
PgExtnRegistry::comparator_func(const ComparatorContext* context,
                                const std::span<const char> &lhval,
                                const std::span<const char> &rhval)
{
    auto type_oid = context->type_oid;
    auto op_str = context->op_str;

    auto extn_registry = PgExtnRegistry::get_instance();

    auto type = extn_registry->get_type_by_oid(type_oid);

    Datum leftDatum = extn_registry->binary_to_datum(lhval, type_oid, -1);
    Datum rightDatum = extn_registry->binary_to_datum(rhval, type_oid, -1);

    auto operator_func = extn_registry->get_operator_func_by_oper_name(op_str);

    Datum result = DirectFunctionCall3(operator_func, leftDatum, rightDatum, ObjectIdGetDatum(0));

    auto leftDatumString = extn_registry->datum_to_string(leftDatum, type_oid);
    auto rightDatumString = extn_registry->datum_to_string(rightDatum, type_oid);
    bool comparatorResult = DatumGetBool(result);

    LOG_DEBUG(LOG_COMMON, LOG_LEVEL_DEBUG3, "Operator = Result: {} {} {} = {}", leftDatumString,
              op_str, rightDatumString, comparatorResult);

    return comparatorResult;
}

std::string
PgExtnRegistry::datum_to_string(Datum value, Oid pg_oid) const
{
    auto type = get_type_by_oid(pg_oid);
    auto typoutput = get_type_func_by_type_name(type.typoutput);

    DCHECK(typoutput);

    // call the output function
    Datum result = DirectFunctionCall1(typoutput, value);
    const char* str = DatumGetCString(result);

    return std::string(str);
}

Datum
PgExtnRegistry::binary_to_datum(const std::span<const char> &value,
                                Oid pg_oid,
                                int32_t atttypmod) const
{
    auto type = get_type_by_oid(pg_oid);
    auto typreceive = get_type_func_by_type_name(type.typreceive);

    DCHECK(typreceive);

    StringInfoData string;
    initStringInfo(&string);

    appendBinaryStringInfoNT(&string, value.data(), value.size());
    Datum datum = PointerGetDatum(&string);

    // call the receive function
    Datum result = DirectFunctionCall3(typreceive, datum, ObjectIdGetDatum(0), Int32GetDatum(atttypmod));
    return result;
};

PGFunction
PgExtnRegistry::_load_extn_function(void* library, const std::string_view func_name)
{
    auto extn_function = (PGFunction)dlsym(library, func_name.data());
    if (!extn_function) {
        LOG_ERROR("Failed to find function PGFunction {}", func_name);
        return nullptr;
    }
    return extn_function;
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
