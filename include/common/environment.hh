#pragma once

#include <tuple>

namespace springtail::environment {
    enum Type {
        STR,
        UINT32,
        UINT64,
        BOOL
    };

    /** Environment variable name for property overrides */
    static constexpr char ENV_OVERRIDE[]           = "SPRINGTAIL_PROPERTIES";

    /** Environment variable name for the properties file,
     * overrides reading config from redis
     */
    static constexpr char PROPERTIES_FILE_OVERRIDE[] = "SPRINGTAIL_PROPERTIES_FILE";

    /** Environment variable name for the properties file
     * env_varname, type, json object, json_key
     */
    static const std::tuple<const char *, Type, const char *, const char *> Variables[] =
    {
        {"REDIS_HOSTNAME", STR, "redis", "host"},
        {"REDIS_USER", STR, "redis", "user"},
        {"REDIS_PASSWORD", STR, "redis", "password"},
        {"REDIS_USER_DATABASE_ID", UINT32, "redis", "db"},
        {"REDIS_CONFIG_DATABASE_ID", UINT32, "redis", "config_db"},
        {"REDIS_PORT", UINT32, "redis", "port"},
        {"REDIS_SSL", BOOL, "redis", "ssl"},
        {"ORGANIZATION_ID", STR, "org", "organization_id"},
        {"ACCOUNT_ID", STR, "org", "account_id"},
        {"DATABASE_INSTANCE_ID", UINT64, "org", "db_instance_id"},
        {"LUSTRE_DNS_NAME", STR, "fs", "dns_name"},
        {"LUSTRE_MOUNT_NAME", STR, "fs", "mount_name"},
        {"MOUNT_POINT", STR, "fs", "mount_point"},
        {"FDW_ID", STR, "org", "fdw_id"},
        {"REPLICATION_USER_PASSWORD", STR, "org", "replication_user_password"},
        {"FDW_USER_PASSWORD", STR, "org", "fdw_user_password"}
    };
} // namespace springtail::environment
