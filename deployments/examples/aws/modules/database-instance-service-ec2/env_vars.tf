# For built-in environment variables
locals {
  # We have some system-wide env vars that are "built-in". So we set them here.
  builtin_env_vars = {
    AWS_DEFAULT_REGION         = local.dbi_region
    AWS_REGION                 = local.dbi_region
    ORGANIZATION_ID            = var.service_config.organization_id
    ACCOUNT_ID                 = var.service_config.account_id
    DATABASE_INSTANCE_ID       = var.service_config.database_instance_id
    MOUNT_POINT                = "/fsx"
    REDIS_HOSTNAME             = var.service_config.shard.redis.cluster_endpoint
    REDIS_HOST                 = var.service_config.shard.redis.cluster_endpoint
    REDIS_PORT                 = var.service_config.shard.redis.cluster_port
    REDIS_USER                 = var.service_config.shard.redis.user
    REDIS_PASSWORD_KEY         = var.service_config.shard.redis.password_key
    PRIMARY_DB_PASSWORD_KEY    = var.primary_db_password_key
    REDIS_SSL                  = 1
    REDIS_CONFIG_DATABASE_ID   = 0
    REDIS_USER_DATABASE_ID     = 1
    SHARD                      = var.service_config.shard.shard
    LOGGING_VOLUME_MOUNT_POINT = var.logging_volume != null ? var.logging_volume.mount_point : ""
  }
  combined_env_vars = merge(
    local.builtin_env_vars,
    # Additional env vars
    var.env_vars,
  )
}
