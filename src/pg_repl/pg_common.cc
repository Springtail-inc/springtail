#include <pg_repl/pg_common.hh>

extern "C" {
    #include <postgres.h>
    #include <catalog/pg_type.h>
}

namespace springtail
{
    SchemaType
    convert_pg_type(int32_t pg_type)
    {
        switch (pg_type) {
            case INT4OID:
            case DATEOID:
                return SchemaType::INT32;

            case TEXTOID:
            case VARCHAROID:
            case BPCHAROID:
                return SchemaType::TEXT;

            case INT8OID:
            case TIMESTAMPOID:
            case TIMESTAMPTZOID:
            case TIMEOID:
            case TIMETZOID:
            case MONEYOID:
                return SchemaType::INT64;

            case BOOLOID:
                return SchemaType::BOOLEAN;

            case INT2OID:
                return SchemaType::INT16;

            case FLOAT4OID:
                return SchemaType::FLOAT32;

            case FLOAT8OID:
                return SchemaType::FLOAT64;

            case CHAROID:
                return SchemaType::INT8;

            default:
                // put all other types into BINARY data for now
                return SchemaType::BINARY;
        }
    }

    void
    _populate_invalid_tables_in_redis(uint64_t db_id, uint64_t table_oid, const nlohmann::json& table_info)
    {
        auto &&key = fmt::format(redis::HASH_INVALID_TABLES, Properties::get_db_instance_id(), db_id);
        auto redis = RedisMgr::get_instance()->get_client();
        auto field_key = fmt::format("{}", table_oid);

        redis->hset(key, field_key, table_info.dump());
    }

    bool
    _check_if_table_is_invalid_in_redis(uint64_t db_id, uint64_t table_oid)
    {
        auto &&key = fmt::format(redis::HASH_INVALID_TABLES, Properties::get_db_instance_id(), db_id);
        auto redis = RedisMgr::get_instance()->get_client();
        auto field_key = fmt::format("{}", table_oid);

        auto table_info = redis->hget(key, field_key);
        if (table_info.has_value()) {
            return true;
        }
        return false;
    }

    void _clear_invalid_table_in_redis(uint64_t db_id,
                                      uint64_t table_oid)
    {
        auto &&key = fmt::format(redis::HASH_INVALID_TABLES, Properties::get_db_instance_id(), db_id);
        auto redis = RedisMgr::get_instance()->get_client();
        auto field_key = fmt::format("{}", table_oid);

        redis->hdel(key, field_key);
    }
}
