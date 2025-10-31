import click
import logging
import copy
import orjson as json
from ..cli import (
    config,
    DEPLOYMENT_ENV_LOCAL,
)
from ...imp.utils import run_psql_files

logger = logging.Logger("stpcl.config.init")


@config.command("init")
@click.option(
    "--primary-db-host",
    type=str,
    required=True,
)
@click.option(
    "--primary-db-port",
    type=int,
    required=False,
    default=5432,
)
@click.option(
    "--system-settings-gitsha",
    type=str,
    required=True,
)
@click.option(
    "--system-settings",
    type=click.File(mode="r"),
    required=False,
    default="/etc/default-system-settings.json",
    help="Path to system settings JSON file. Will generate a default copy if omitted.",
)
@click.option(
    "--hostname-proxy",
    type=str,
    required=False,
    default="proxy",
    help="Hostname for the proxy service. Default: 'proxy'.",
)
@click.option(
    "--hostname-ingestion",
    type=str,
    required=False,
    default="ingestion",
    help="Hostname for the ingestion service. Default: 'ingestion'.",
)
@click.option(
    "--db-info",
    type=(int, str, str, str),
    multiple=True,
    required=True,
    help="List of database (ID, name, publication_name, replication_slot) to initialize. Use multiple times for multiple IDs. E.g., --database-ids db1 --database-ids db2",
)
@click.option(
    "--proxy-instance-key",
    type=str,
    required=False,
    default="default",
    help="Proxy instance key. Default: 'default'.",
)
@click.option(
    "--ingestion-instance-key",
    type=str,
    required=False,
    default="default",
    help="Ingestion instance key. Default: 'default'.",
)
@click.option(
    "--fdw-info",
    type=(str, str),
    multiple=True,
    required=True,
    help="List of FDW (instance_key, host) to initialize. Use multiple times for multiple FDWs. E.g., --fdw-info fdw1 host1 --fdw-info fdw2 host2",
)
@click.option(
    "--disable-ssl",
    is_flag=True,
    type=bool,
    default=False,
    help="Use SSL for all connections (except for Redis). Default: False",
)
@click.option(
    "--enable-otel",
    is_flag=True,
    type=bool,
    default=False,
    help="Use SSL for all connections. Default: True",
)
@click.option(
    "--replication-cred",
    type=(str, str, str),
    multiple=False,
    required=True,
    help="Initial replication user credentials as (username, password, type). Use space between each value."
         " Default: ('postgres', 'postgres', 'text')",
)
@click.option(
    "--database-cred",
    type=(str, str, str),
    multiple=False,
    required=False,
    help="Initial database user credentials as (username, password, type). Use space between each value. "
         "If not provided, will use the replication user credentials."
         " Default: None",
    default=None,
)
@click.option(
    "--fdw-superuser-cred",
    type=(str, str, str),
    multiple=False,
    required=True,
    help="Initial FDW superuser credentials as (username, password, type). Use space between each value. "
         "If not provided, will use the replication user credentials.",
)
@click.option(
    "--proxy-to-fdw-cred",
    type=(str, str, str),
    multiple=False,
    required=False,
    help="Initial proxy to FDW user credentials as (username, password, type). Use space between each value. "
         "If not provided, will use the FDW Superuser credentials."
         "Default: None",
    default=None,
)
@click.option(
    "--force-set",
    is_flag=True,
    type=bool,
    default=False,
    help="Force set the initial secrets even if they already exist. Default: False",
)
@click.pass_obj
def init(
        obj,
        primary_db_host: str,
        primary_db_port: int,
        system_settings_gitsha: str,
        system_settings: click.File | None,
        hostname_proxy: str,
        hostname_ingestion: str,
        db_info: list[str],
        proxy_instance_key: str,
        ingestion_instance_key: str,
        fdw_info: list[str],
        disable_ssl: bool,
        enable_otel: bool,
        replication_cred: tuple[str, str, str],
        database_cred: tuple[str, str, str] | None,
        fdw_superuser_cred: tuple[str, str, str],
        proxy_to_fdw_cred: tuple[str, str, str],
        force_set: bool,
):
    """Load a configuration file into Redis."""
    from stpcl.imp.config import (
        Config,
        PrimaryDB,
        DBConfigEntry,
        IncludeFilter,
        FDWConfigEntry,
    )
    from stpcl.imp.secrets import (
        Secrets,
        PostgresCred,
        PostgresCredType,
        PostgresUserRole,
    )
    from stpcl.imp.utils import run_psql

    deployment_env = obj["deployment_env"]
    if deployment_env != DEPLOYMENT_ENV_LOCAL:
        raise NotImplementedError(
            "Only 'local' deployment environment is supported for config 'init'."
        )

    redis = obj["redis"]

    instance_id = obj["instance_id"]

    cfg = Config(instance_id, redis)

    # Instance config
    instance_config = cfg.instance_config
    instance_config.primary_db = PrimaryDB(host=primary_db_host, port=primary_db_port)
    instance_config.id = instance_id
    instance_config.system_settings_gitsha = system_settings_gitsha

    if not system_settings:
        logger.warning("No system settings file provided. Generating a default copy.")
        s = click.open_file(f"default_system_settings.json", mode="r")
    else:
        s = click.open_file(system_settings.name, mode="r")
    logger.info("SSL disabled: %s", disable_ssl)
    raw_system_settings = json.loads(s.read())
    instance_config.system_settings = massage_system_settings(raw_system_settings, not disable_ssl, enable_otel)
    s.close()

    instance_config.hostname_proxy = hostname_proxy
    instance_config.hostname_ingestion = hostname_ingestion
    instance_config.database_ids = [db_id for db_id, _, _, _ in db_info]

    # Instance state and db config
    instance_state = cfg.instance_state
    db_config = cfg.db_config
    for db_id, name, publication_name, replication_slot in db_info:
        instance_state[db_id] = "initialize"
        db_config[db_id] = DBConfigEntry(
            id=db_id,
            name=name,
            publication_name=publication_name,
            replication_slot=replication_slot,
            include=IncludeFilter(schemas=["*"]),
        )

    # Coordinator state
    coordinator_state = cfg.coordinator_state
    coordinator_state[f"proxy:{proxy_instance_key}"] = "startup"
    coordinator_state[f"ingestion:{ingestion_instance_key}"] = "startup"
    for fdw_instance_key, _ in fdw_info:
        coordinator_state[f"fdw:{fdw_instance_key}"] = "startup"

    # FDW and FDW IDs
    fdw = cfg.fdw
    fdw_ids = cfg.fdw_ids
    for fdw_instance_key, host in fdw_info:
        fdw[fdw_instance_key] = FDWConfigEntry(
            host=host,
            port=5432,
            state="initialize",
            sync_seconds=30,
        )
        fdw_ids += fdw_instance_key

    # Secrets
    aws_secretsmanager_client = obj["aws_secretsmanager_client"]

    organization_id = obj["organization_id"]
    account_id = obj["account_id"]
    instance_id = obj["instance_id"]
    redis = obj["redis"]

    secrets = Secrets(
        organization_id, account_id, instance_id, aws_secretsmanager_client
    )
    cfg = Config(instance_id, redis)
    # Set initial secrets
    cred = secrets.primary_db_credentials
    if not cred() or force_set:
        r = PostgresCred(
            username=replication_cred[0],
            password=replication_cred[1],
            type=PostgresCredType(replication_cred[2]),
            role=PostgresUserRole.REPLICATION,
        )
        if database_cred:
            d = PostgresCred(
                username=database_cred[0],
                password=database_cred[1],
                type=PostgresCredType(database_cred[2]),
                role=PostgresUserRole.DATABASE,
            )
        else:
            d = copy.deepcopy(r)
            d.role = PostgresUserRole.DATABASE

        f = PostgresCred(
            username=fdw_superuser_cred[0],
            password=fdw_superuser_cred[1],
            type=PostgresCredType(fdw_superuser_cred[2]),
            role=PostgresUserRole.FDW_SUPERUSER,
        )
        if proxy_to_fdw_cred:
            p = PostgresCred(
                username=proxy_to_fdw_cred[0],
                password=proxy_to_fdw_cred[1],
                type=PostgresCredType(proxy_to_fdw_cred[2]),
                role=PostgresUserRole.PROXY_TO_FDW,
            )
        else:
            p = copy.deepcopy(f)
            p.role = PostgresUserRole.PROXY_TO_FDW
        # Save!
        cred.commit([r, d, f, p])
        # Save another copy into the Redis Config, system_settings
        logger.info("Primary DB credentials initialized and set.")
        instance_config = cfg.instance_config
        s = copy.deepcopy(instance_config.system_settings)
        s['aws_users_override'] = [r, d, f, p]
        instance_config.system_settings = s
    else:
        logger.info("Primary DB credentials already set. Use --force-set to override.")
        for item in cred():
            if item.role == PostgresUserRole.REPLICATION:
                r = item
                break
        else:
            raise ValueError("Replication credentials not found in existing secrets.")
    # Initial setup of publication and replication slots
    # (dbname, script)
    db_sql_list = []
    for _, p in db_config().items():
        d = json.loads(p)
        db_sql_list.append(
            (
                d["name"], get_publication_and_replication_slot_sql(
                    publication_name=d["publication_name"],
                    replication_slot=d["replication_slot"],
                ),
            )
        )

    for dbname, sql in db_sql_list:
        logger.info("Ensuring publication and replication slot in database '%s'.", dbname)
        run_psql(
            host=primary_db_host,
            port=primary_db_port,
            user=r.username,
            password=r.password,
            dbname=dbname,
            sql=sql,
        )
        run_psql_files(
            host=primary_db_host,
            port=primary_db_port,
            user=r.username,
            password=r.password,
            dbname=dbname,
            files=[
                "/app/scripts/triggers.sql",
                "/app/scripts/roles.sql",
                "/app/scripts/role_members.sql",
                "/app/scripts/policy.sql",
                "/app/scripts/table_owners.sql",
            ]
        )


def get_publication_and_replication_slot_sql(publication_name: str, replication_slot: str) -> str:
    """Generate SQL to create publication and replication slot if not exists."""
    return f"""
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = '{publication_name}') THEN
            CREATE PUBLICATION {publication_name} FOR ALL TABLES;
        END IF;
    END $$;
    
    SELECT format($sql$
      SELECT pg_create_logical_replication_slot(%L, 'pgoutput')$sql$, 
      '{replication_slot}')
        WHERE NOT EXISTS (
            SELECT 1 FROM pg_replication_slots WHERE slot_name = '{replication_slot}'
        );
    """


def massage_system_settings(s: dict, enable_ssl: bool, enable_otel: bool) -> dict:
    """Massage system settings to ensure correct flags are set.
    - Disable all SSL
    - Disable OTEL
    """
    # Always disable SSL for local deployments for Redis
    s["redis"]["ssl"] = False
    s["proxy"]["mode"] = "normal"

    for k in ("write_cache", "sys_tbl_mgr", "log_mgr"):
        s[k]["rpc_config"]["ssl"] = enable_ssl
        s[k]["rpc_config"]["server_cert"] = "/certs/server_cert.pem"
        s[k]["rpc_config"]["server_key"] = "/certs/server_key.pem"
        s[k]["rpc_config"]["server_trusted"] = "/certs/ca_cert.pem"
        s[k]["rpc_config"]["client_cert"] = "/certs/client_cert.pem"
        s[k]["rpc_config"]["client_key"] = "/certs/client_key.pem"
        s[k]["rpc_config"]["client_trusted"] = "/certs/ca_cert.pem"
    s["proxy"]["enable_ssl"] = enable_ssl
    s["proxy"]["cert_path"] = "/certs/server_cert.pem"
    s["proxy"]["key_path"] = "/certs/server_key.pem"

    s["otel"]["enabled"] = enable_otel

    return s
