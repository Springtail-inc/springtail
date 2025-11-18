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

void
PgExtnRegistry::add_opclass(const std::string& extension, PgOpsClass opclass, PgOpsClassMethod method)
{
    auto library = _library_map.at(extension);

    // Load the opclass method
    auto extn_function = _load_extn_function(library, method.function_name);
    method.function_ptr = extn_function;

    // Update the name map
    _opclass_function_map[opclass.name][method.support_number] = method;
}

void*
PgExtnRegistry::get_operator_func_by_oid(uint32_t oid) const
{
    auto it = _oper_oid_to_name.find(oid);
    if (it == _oper_oid_to_name.end()) {
        LOG_ERROR("Failed to find operator function by oid: {}", oid);
        return nullptr;
    }
    return _oper_name_to_func.at(it->second);
}

void*
PgExtnRegistry::get_operator_func_by_oper_name(const char* oper_name) const
{
    auto it = _oper_name_to_func.find(oper_name);
    if (it == _oper_name_to_func.end()) {
        LOG_ERROR("Failed to find operator function by oper name: {}", oper_name);
        return nullptr;
    }
    return it->second;
}

void*
PgExtnRegistry::get_operator_func_by_proc_name(const std::string& proc_name) const
{
    auto it = _proc_name_to_func.find(proc_name);
    if (it == _proc_name_to_func.end()) {
        LOG_ERROR("Failed to find operator function by proc name: {}", proc_name);
        return nullptr;
    }
    return it->second;
}

void*
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

    // Ensure library is loaded
    DCHECK(library);

    _library_map.try_emplace(extension, library);
}

PgOpsClassMethod
PgExtnRegistry::get_opclass_method_by_method_name(const std::string& opclass_name, int support_number)
{
    // Find the opclass by name
    auto opclass_it = _opclass_function_map.find(opclass_name);
    if (opclass_it == _opclass_function_map.end()) {
        LOG_ERROR("Failed to find opclass by opclass name: {}", opclass_name);
        return PgOpsClassMethod();
    }

    // Find the method by support number
    auto method_it = opclass_it->second.find(support_number);
    if (method_it == opclass_it->second.end()) {
        LOG_ERROR("Failed to find opclass method by opclass name: {} and support number: {}", opclass_name, support_number);
        return PgOpsClassMethod();
    }

    // Check if the function pointer is valid
    if (method_it->second.function_ptr == nullptr) {
        LOG_ERROR("Failed to find opclass method function by opclass name: {} and support number: {}", opclass_name, support_number);
        return PgOpsClassMethod();
    }

    // Return the method
    return method_it->second;
}

void*
PgExtnRegistry::get_opclass_method_func_ptr_by_method_name(const std::string& opclass_name, int support_number)
{
    auto extn_registry = PgExtnRegistry::get_instance();

    auto method = extn_registry->get_opclass_method_by_method_name(opclass_name, support_number);

    // Check if the function pointer is valid
    if (method.function_ptr == nullptr) {
        LOG_ERROR("Failed to find opclass method function by opclass name: {} and support number: {}", opclass_name, support_number);
        return nullptr;
    }

    // Return the function pointer
    return method.function_ptr;
}

Datum
PgExtnRegistry::invoke_opclass_method(const std::string& opclass_name, int support_number, Datum value)
{
    auto extn_registry = PgExtnRegistry::get_instance();

    auto method = extn_registry->get_opclass_method_by_method_name(opclass_name, support_number);
    if (method.function_ptr == nullptr) {
        LOG_ERROR("Failed to find opclass method function by opclass name: {} and support number: {}", opclass_name, support_number);
        return Datum();
    }

    LOG_INFO("Invoking opclass method: {} with datum: {}", method.function_name, value);
    PGFunction operator_func_ptr = (PGFunction)method.function_ptr;

    Datum result = DirectFunctionCall1(operator_func_ptr, value);
    return result;
}

bool
PgExtnRegistry::comparator_func(const ExtensionContext* context,
                                const std::span<const char> &lhval,
                                const std::span<const char> &rhval)
{
    auto type_oid = context->type_oid;
    auto op_str = context->op_str;

    auto extn_registry = PgExtnRegistry::get_instance();

    auto type = extn_registry->get_type_by_oid(type_oid);

    Datum left_datum = extn_registry->binary_to_datum(lhval, type_oid, -1);
    Datum right_datum = extn_registry->binary_to_datum(rhval, type_oid, -1);

    auto operator_func = extn_registry->get_operator_func_by_oper_name(op_str);

    PGFunction operator_func_ptr = (PGFunction)operator_func;
    Datum result = DirectFunctionCall3(operator_func_ptr, left_datum, right_datum, ObjectIdGetDatum(0));

    auto left_datum_string = extn_registry->datum_to_string(left_datum, type_oid);
    auto right_datum_string = extn_registry->datum_to_string(right_datum, type_oid);
    bool comparator_result = DatumGetBool(result);

    LOG_DEBUG(LOG_COMMON, LOG_LEVEL_DEBUG3, "Operator = Result: {} {} {} = {}", left_datum_string,
              op_str, right_datum_string, comparator_result);

    return comparator_result;
}

std::string
PgExtnRegistry::datum_to_string(Datum value, Oid pg_oid) const
{
    auto type = get_type_by_oid(pg_oid);
    auto typoutput = get_type_func_by_type_name(type.typoutput);

    DCHECK(typoutput);

    // call the output function
    PGFunction typoutput_func = (PGFunction)typoutput;
    Datum result = DirectFunctionCall1(typoutput_func, value);
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
    PGFunction typreceive_func = (PGFunction)typreceive;
    Datum result = DirectFunctionCall3(typreceive_func, datum, ObjectIdGetDatum(0), Int32GetDatum(atttypmod));
    return result;
};

void*
PgExtnRegistry::_load_extn_function(void* library, const std::string_view func_name)
{
    auto extn_function = (void*)dlsym(library, func_name.data());
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
