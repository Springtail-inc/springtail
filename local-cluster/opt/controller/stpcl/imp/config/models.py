from dataclasses import dataclass, field
from bidict import bidict
from enum import Enum


class ServiceName(Enum):
    INGESTION = "ingestion"
    PROXY = "proxy"
    FDW = "fdw"


@dataclass
class PrimaryDB:
    host: str
    port: int


@dataclass
class IncludeFilterEntry:
    schema: str
    table: str


@dataclass
class IncludeFilter:
    schemas: list[str] = field(default_factory=list)
    tables: list[IncludeFilterEntry] | None = None


@dataclass
class DBConfigEntry:
    id: int
    name: str
    replication_slot: str
    publication_name: str
    include: IncludeFilter


@dataclass
class FDWConfigEntry:
    host: str
    port: int
    state: str
    sync_seconds: int


DBConfig = dict[int, DBConfigEntry]
FDW = dict[str, FDWConfigEntry]
DBState = dict[int, str]
FDWIDs = set[str]
CoordinatorState = dict[str, str]
InstanceState = dict[str, str]


@dataclass
class InstanceConfig:
    id: int
    primary_db: PrimaryDB
    database_ids: list[int]
    system_settings: dict[str, any]
    system_settings_gitsha: str
    hostname_proxy: str
    hostname_ingestion: str

    __key_map__ = bidict(
        {
            # Mapping between actual keys and dataclass attribute names
            "hostname_proxy": "hostname:proxy",
            "hostname_ingestion": "hostname:ingestion",
        }
    )


@dataclass
class AllConfigs:
    """
    Sample layout in Redis

    4149:coordinator_state (hm)
    4149:db_config (hm)
    4149:fdw (hm)
    4149:instance_config (hm)
    4149:instance_state (hm)
    4149:fdw_ids (set)

    """

    db_instance_config: InstanceConfig
    db_config: DBConfig
    fdw: FDW
    fdw_ids: FDWIDs
    db_state: DBState
    coordinator_state: CoordinatorState
