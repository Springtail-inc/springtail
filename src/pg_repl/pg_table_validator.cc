#include <pg_repl/pg_table_validator.hh>

namespace springtail
{
    void
    TableValidator::populate_invalid_tables_in_redis(uint64_t table_oid, const nlohmann::json& table_info)
    {
        auto field_key = fmt::format("{}", table_oid);
        _cache.hset(field_key, table_info.dump());
    }

    bool
    TableValidator::check_if_table_is_invalid_in_redis(uint64_t table_oid)
    {
        auto field_key = fmt::format("{}", table_oid);
        auto table_info = _cache.hget(field_key);
        return table_info.has_value();
    }

    void
    TableValidator::clear_invalid_table_in_redis(uint64_t table_oid)
    {
        auto field_key = fmt::format("{}", table_oid);
        _cache.hdel(field_key);
    }
} // namespace springtail
