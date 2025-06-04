#include <charconv>
#include <stdexcept>

#include <pg_ext/guc.hh>

namespace pgext {

void
GucManager::define_real(std::string_view name,
                        double *value_addr)
{
    // Check if the name is already defined
    if (_vars.contains(std::string(name))) {
        throw std::runtime_error("GUC variable already defined: " + std::string(name));
    }

    // Store the variable
    Variable var;
    var.type = Type::REAL;
    var.value_addr = value_addr;
    _vars[std::string(name)] = var;
}

void
GucManager::set_option(std::string_view name,
                       std::string_view value,
                       GucContext context)
{
    // Find the variable
    auto it = _vars.find(std::string(name));
    if (it == _vars.end()) {
        throw std::runtime_error("Unknown GUC variable: " + std::string(name));
    }

    // Parse and set the value
    double new_value;
    auto [ptr, ec] = std::from_chars(value.data(), value.data() + value.size(), new_value);
    if (ec != std::errc()) {
        throw std::runtime_error("Invalid value for GUC variable: " + std::string(name));
    }

    // Update the value based on type
    if (it->second.type == Type::REAL) {
        *static_cast<double *>(it->second.value_addr) = new_value;
    } else {
        throw std::runtime_error("Unsupported GUC variable type");
    }
}

void
GucManager::reserve_prefix(std::string_view prefix)
{
    // Check if prefix is already reserved
    if (_prefixes.contains(std::string(prefix))) {
        throw std::runtime_error("Prefix already reserved: " + std::string(prefix));
    }

    // Add the prefix to the reserved set
    _prefixes.insert(std::string(prefix));
}

}  // namespace pgext

// Global GucManager instance
static pgext::GucManager g_guc_manager;

void
DefineCustomRealVariable(const char *name,
                         const char *short_desc,
                         const char *long_desc,
                         double *valueAddr,
                         double defaultVal,
                         double minVal,
                         double maxVal,
                         int context,
                         int flags,
                         void *check_hook,
                         void *assign_hook,
                         void *show_hook)
{
    try {
        // Set the default value
        *valueAddr = defaultVal;

        // Define the variable
        g_guc_manager.define_real(name, valueAddr);
    } catch (const std::exception &e) {
        // TODO: Proper error handling
        throw;
    }
}

void
SetConfigOption(const char *name, const char *value, int context, int source)
{
    try {
        g_guc_manager.set_option(name, value, static_cast<pgext::GucContext>(context));
    } catch (const std::exception &e) {
        // TODO: Proper error handling
        throw;
    }
}

void
MarkGUCPrefixReserved(const char *prefix)
{
    try {
        g_guc_manager.reserve_prefix(prefix);
    } catch (const std::exception &e) {
        // TODO: Proper error handling
        throw;
    }
}
