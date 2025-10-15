from .models import *  # noqa: F403
from .redis_proxies import *  # noqa: F403


class Config:
    def __init__(
        self,
        instance_id: int,
        redis_client: redis.Redis,
    ):
        """
        Sample layout in Redis:

            4149:coordinator_state (hm)
            4149:db_config (hm)
            4149:fdw (hm)
            4149:instance_config (hm)
            4149:instance_state (hm)
            4149:fdw_ids (set)

        :param instance_id: The DB instance ID to bind the configuration manager to.
        :param redis_client:
        :param system_settings_template:
        """
        self.instance_id = instance_id
        self.client = redis_client
        self.k_instance_config = f"{self.instance_id}:instance_config"
        self.k_instance_state = f"{self.instance_id}:instance_state"
        self.k_db_config = f"{self.instance_id}:db_config"
        self.k_fdw = f"{self.instance_id}:fdw"
        self.k_fdw_ids = f"{self.instance_id}:fdw_ids"
        self.k_coordinator_state = f"{self.instance_id}:coordinator_state"

    @property
    def instance_config(self) -> HProxy[InstanceConfig]:
        return bind_config_hashmap(
            InstanceConfig, lambda: self.k_instance_config, self.client
        )

    @property
    def instance_state(self) -> HProxy[InstanceState]:
        return bind_config_hashmap(
            InstanceState, lambda: self.k_instance_state, self.client
        )

    @property
    def coordinator_state(self) -> HProxy[CoordinatorState]:
        return bind_config_hashmap(
            CoordinatorState, lambda: self.k_coordinator_state, self.client
        )

    @property
    def db_config(self) -> HProxy[DBConfig]:
        return bind_config_hashmap(DBConfig, lambda: self.k_db_config, self.client)

    @property
    def fdw(self) -> HProxy[FDW]:
        return bind_config_hashmap(FDW, lambda: self.k_fdw, self.client)

    @property
    def fdw_ids(self) -> SProxy[str]:
        return bind_config_set(str, lambda: self.k_fdw_ids, self.client)
