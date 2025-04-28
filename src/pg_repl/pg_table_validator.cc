#include <pg_repl/pg_table_validator.hh>

namespace springtail
{
    void
    TableValidator::mark_invalid(uint64_t table_oid, const nlohmann::json& table_info)
    {
        _cache.hset(table_oid, table_info.dump());
    }

    bool
    TableValidator::check_invalid(uint64_t table_oid)
    {
        return _cache.hget(table_oid).has_value();
    }

    void
    TableValidator::mark_valid(uint64_t table_oid)
    {
        _cache.hdel(table_oid);
    }

    nlohmann::json
    TableValidator::get_invalid_columns(uint64_t table_oid)
    {
        if ( _cache.hget(table_oid).has_value()) {
            return _cache.hget(table_oid).value();
        }
        return nlohmann::json();
    }
} // namespace springtail
