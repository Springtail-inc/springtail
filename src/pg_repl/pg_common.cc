#include <pg_repl/pg_common.hh>
#include <utility>

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

    class InvalidTableCache {
        public:
            explicit InvalidTableCache(RedisClientPtr redis) : redis(std::move(redis)) {}

            // Get value from cache or fallback to Redis
            std::optional<std::string> get(const std::string& key) {
                std::lock_guard<std::mutex> lock(cache_mutex);

                // Check in-memory cache
                auto it = cache.find(key);
                if (it != cache.end()) {
                    return it->second;
                }

                // If not in cache, check Redis
                auto redis_value = redis->hget(redis::HASH_INVALID_TABLES, key);
                if (redis_value) {
                    // Store in cache for next time
                    cache[key] = *redis_value;
                }

                return redis_value; // std::nullopt if key not found
            }

            void hset(const std::string& key, const std::string& value) {
                std::lock_guard<std::mutex> lock(cache_mutex);

                // Update in-memory cache
                cache[key] = value;

                // Write-through to Redis
                redis->hset(redis::HASH_INVALID_TABLES, key, value);
            }
        private:
            std::unordered_map<std::string, std::string> cache;
            std::mutex cache_mutex;
            RedisClientPtr redis;
    };

    void
    TableValidator::populate_invalid_tables_in_redis(uint64_t table_oid, const nlohmann::json& table_info)
    {
        auto redis = RedisMgr::get_instance()->get_client();
        auto field_key = fmt::format("{}", table_oid);

        redis->hset(redis::HASH_INVALID_TABLES, field_key, table_info.dump());
    }

    bool
    TableValidator::check_if_table_is_invalid_in_redis(uint64_t table_oid)
    {
        auto redis = RedisMgr::get_instance()->get_client();
        auto field_key = fmt::format("{}", table_oid);

        auto table_info = redis->hget(redis::HASH_INVALID_TABLES, field_key);

        return table_info.has_value();
    }

    void
    TableValidator::clear_invalid_table_in_redis(uint64_t table_oid)
    {
        auto redis = RedisMgr::get_instance()->get_client();
        auto field_key = fmt::format("{}", table_oid);

        redis->hdel(redis::HASH_INVALID_TABLES, field_key);
    }
}
