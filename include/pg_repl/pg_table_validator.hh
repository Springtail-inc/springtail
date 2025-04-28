#include <boost/functional/hash.hpp>

#include <nlohmann/json.hpp>

#include <fmt/format.h>

#include <common/properties.hh>
#include <common/redis.hh>
#include <common/redis_types.hh>
#include <common/singleton.hh>

#include <pg_repl/pg_repl_msg.hh>

namespace springtail
{
    class InvalidTableCache {
        public:
            explicit InvalidTableCache(RedisClientPtr redis) : redis(std::move(redis)), redis_key(fmt::format(redis::HASH_INVALID_TABLES, Properties::get_db_instance_id())) {}

            /**
             * @brief Gets a value from Cache, if its not found in the In memory cache
             *        then find the value from Redis
             *
             * @param key Key that needs to be fetched
             * @return std::optional<std::string> Value if found, otherwise std::nullopt
             */
            std::optional<std::string> hget(uint64_t key) {
                std::lock_guard lock(cache_mutex);

                // Check in-memory cache
                auto cache_iterator = cache.find(key);
                if (cache_iterator != cache.end()) {
                    // Found in the in-memory cache
                    return cache_iterator->second;
                }

                // If not in cache, check Redis
                auto redis_value = redis->hget(redis_key, std::to_string(key));
                // Even if value isn't present, update the cache
                cache[key] = redis_value;

                return redis_value; // std::nullopt if key not found
            }

            /**
             * @brief Sets the value for a key in cache and write through to redis
             *
             * @param key Key that needs to be set
             * @param value Value that needs to be set
             */
            void hset(uint64_t key, const std::string& value) {
                std::lock_guard lock(cache_mutex);

                // Update in-memory cache
                cache[key] = value;

                // Write-through to Redis
                redis->hset(redis_key, std::to_string(key), value);
            }

            /**
             * @brief Delete a key from the cache and redis
             *
             * @param key Key that needs to be deleted
             */
            void hdel(uint64_t key) {
                std::lock_guard lock(cache_mutex);

                // Update in-memory cache
                cache.erase(key);

                // Write-through to Redis
                redis->hdel(redis_key, std::to_string(key));
            }
        private:
            std::unordered_map<uint64_t, std::optional<std::string>, boost::hash<uint64_t>> cache;
            std::mutex cache_mutex;
            RedisClientPtr redis;
            const std::string redis_key;
    };

    class TableValidator : public Singleton<TableValidator> {
        friend class Singleton<TableValidator>;
        public:
            /**
            * @brief Populates the redis cache with the information about invalid tables. This is used to
            *        validate the invalid tables in the systems which are skipped for replication and also
            *        can be exposed to the users to understand why certain tables aren't replicated
            *
            * @param table_oid The table oid which has the invalid columns
            * @param table_info JSON field containing meta info about the invalid columns
            */
            void mark_invalid(uint64_t table_oid, const nlohmann::json &table_info);

            /**
            * @brief Check if the table has any invalid columns
            *
            * @param table_oid The table oid which has the invalid columns
            * @return true/false based on whether the table is invalid
            */
            bool check_invalid(uint64_t table_oid);

            /**
            * @brief Clears the table in Redis, making it valid again
            *
            * @param table_oid The table oid which has the invalid columns
            */
            void mark_valid(uint64_t table_oid);

            /**
            * @brief Validate the DDL operation and get the list of invalid columns
            *
            * @param columns Vector of original columns as part of the DDL
            * @return True if columns are all valid, false otherwise
            */
            template<class T>
            nlohmann::json
            validate_columns(const std::vector<T> &columns)
            {
                auto invalid_columns = nlohmann::json::array();

                // Validate if the table has an invalid column
                for (const auto& column : columns) {
                    if (column.is_generated || column.is_non_standard_collation ||
                        !column.is_user_defined_type) {
                        invalid_columns.push_back({{"name", column.name},
                                                   {"type_name", column.type_name},
                                                   {"collation", column.collation}});
                        LOG_DEBUG(LOG_PG_REPL, "VALIDATE_DDL: Invalid column: name={}",
                                  column.name);
                    }
                }

                return invalid_columns;
            }

        private:
            /** Private constructor */
            TableValidator() : _cache(RedisMgr::get_instance()->get_client()) {}
            /** Private destructor */
            ~TableValidator() noexcept = default;

            InvalidTableCache _cache;
    };
} // namespace springtail
