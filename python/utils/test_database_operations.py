import argparse
from enum import StrEnum
import io
import json
import logging
import os
from pathlib import Path
import psycopg2
import socket
import sys
import threading
import time
from typing import List

import pprint

# Get the parent directory of the current script directory (i.e., the project root directory)
# project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
project_root = Path(__file__).resolve().parents[1]

# Add the /shared directory to the Python path
sys.path.append(os.path.join(project_root, 'shared'))
sys.path.append(os.path.join(project_root, 'controller'))

# Now import the Properties class
from properties import Properties
from common import (
    connect_db,
    execute_sql,
    execute_sql_select,
    execute_sql_script
)

class DatabaseOperation(StrEnum):
    """
    Database operation enum that is used for command line option '--action'
    """
    ADD_DB = 'add_db'
    REMOVE_DB = 'remove_db'
    REMOVE_DB_UNDER_LOAD = 'remove_db_under_load'
    CLEANUP_DB = 'cleanup_db'

class DatabaseTester():
    """
    This is the class that performs all database operations.
    """

    def __init__(self):
        """
        Initialize DatabaseTester object.
        """
        self.props = Properties()

        self.logger = logging.getLogger('springtail')
        self.pp_buffer = io.StringIO()
        self.pretty_printer = pprint.PrettyPrinter(4, 120, None, self.pp_buffer, compact=True, underscore_numbers=True)
        self.config_redis = self.props.get_config_redis()
        self.instance_id = self.props.get_db_instance_id()
        self.preload_table_name = "test_preload"
        self.replicate_table_name = "test_replicate"
        self.fdw_list = []
        self.primary_postgres_conn = None
        self.primary_conn = None
        self.primary_ip = None
        self.proxy_conn = None
        self.fdw_conn = {}
        self.fdw_ip = {}
        self.fdw_user = os.environ.get("FDW_USER")
        self.fdw_user_password = os.environ.get("FDW_USER_PASSWORD")
        self.stop_event = threading.Event()
        self.ready_event = threading.Event()
        self.thread = None

    def log_data(self, level: int, msg: str, data) -> None:
        """
        This method logs the message together with pretty printed data
        """
        self.pretty_printer.pprint(data)
        self.logger.log(level, f"{msg}:\n{self.pp_buffer.getvalue()}")
        self.pp_buffer.seek(0)
        self.pp_buffer.truncate(0)

    def _verify_cluster_up(self):
        """
        This method verifies that the cluster is up. It reads the coordinator state from redis for all containers and verifies
        that the state of each container is 'running'.
        """
        container_list = ['fdw', 'ingestion', 'proxy']
        coordinator_hash_name = f"{self.instance_id}:coordinator_state"
        states = self.config_redis.hgetall(coordinator_hash_name)
        if len(states) == 0:
            raise SystemExit(f"FAILURE: No containers found")
        containers_found = []
        for container, state in states.items():
            container_name, container_instance = container.split(":")
            if container_name not in container_list:
                raise SystemExit(f"FAILURE: Unknown container '{container_name}' and instance '{container_instance}'")
            if state != 'running':
                raise SystemExit(f"FAILURE: Invalid state '{state}' for container '{container}:{container_instance}'; unable to proceed")

            # store all FDW instance keys, we would need them later
            if container_name == 'fdw':
                self.fdw_list.append(container_instance)
            containers_found.append(container_name)

        containers_found = sorted(set(containers_found))

        if container_list != containers_found:
            raise SystemExit(f"FAILURE: Did not find all expected containers")

        self.logger.debug("Cluster is up")

    def _verify_database_exists(self, conn: psycopg2.extensions.connection, database_name: str) -> bool:
        """
        This method verifies that the database with the given name exists.
        """
        result = execute_sql_select(conn, f"SELECT datname FROM pg_database")
        db_names = [row[0] for row in result]
        self.log_data(logging.INFO, "Found databases", db_names)
        return (database_name in db_names)

    def _get_database_ip(self, conn: psycopg2.extensions.connection) -> str:
        """
        This method gets IP address of the host that database connection is connected to.
        """
        result = execute_sql_select(conn, f"SELECT inet_server_addr()")
        ip = [row[0] for row in result][0]
        self.log_data(logging.INFO, "Found database server address ", ip)
        return ip

    def _create_database(self, database_name: str, replication_slot: str, publication_name: str) -> None:
        """
        This method creates database
        """

        self.primary_postgres_conn = connect_db("postgres", "postgres", "postgres", "primary", 5432)

        # verify if database already exist
        if self._verify_database_exists(self.primary_postgres_conn, database_name):
            self.logger.info(f"Database '{database_name}' already exists")
        else:
            execute_sql(self.primary_postgres_conn, f"CREATE DATABASE {database_name}")
            self.logger.info(f"Created database '{database_name}'")

        self.primary_ip = socket.gethostbyname("primary")
        self.logger.info("Creating primary connection; ip {self.primary_ip}")
        self.primary_conn = connect_db(database_name, "postgres", "postgres", "primary", 5432)

        self._create_table(self.preload_table_name)

        # set up replication slot
        out = execute_sql_select(self.primary_conn,
                f"SELECT pg_create_logical_replication_slot('{replication_slot}', 'pgoutput')")
        self.log_data(logging.INFO, "Create replication slot result", out)

        # create publication name
        execute_sql(self.primary_conn,
                    f"CREATE PUBLICATION {publication_name} FOR ALL TABLES")

        # add triggers
        script_files = [
            "triggers.sql",
            "roles.sql",
            "role_members.sql",
            "policy.sql",
            "table_owners.sql",
        ]
        parent_dir = Path(__file__).resolve().parents[1]

        for sf in script_files:
            script_file = parent_dir / "scripts" / sf
            self.logger.info(f"Executing script file: {str(script_file)}")
            execute_sql_script(self.primary_conn, str(script_file))

    def _create_table(self, table_name: str) -> None:
        """
        This method creates a table in the primary database.
        """
        self.logger.info(f"Creating table '{table_name}'")
        execute_sql(self.primary_conn,
                    f"""CREATE TABLE {table_name} (
                        id SERIAL PRIMARY KEY,
                        name TEXT NOT NULL,
                        age INT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )""")
        execute_sql(self.primary_conn,
                    f"""INSERT INTO {table_name} (name, age) VALUES
                        ('Alice', 30),
                        ('Bob', 25),
                        ('Charlie', 35)"""
                    )
        self.logger.info(f"Created table '{table_name}'")

    def _get_test_data(self, conn: psycopg2.extensions.connection, table_name: str) -> List:
        """
        This method gets all the data from the given table.
        """
        return execute_sql_select(conn, f"SELECT * FROM {table_name} ORDER BY id")

    def _add_database_to_redis(self, database_name: str, database_id: int, replication_slot: str, publication_name: str) -> None:
        # verify that this database id is not already in use
        db_configs = self.props.get_db_configs()
        for db in db_configs:
            if int(db['id']) == database_id:
                raise SystemExit(f"FAILURE: Database id '{database_id}' is already i use")
            if db['name'] == database_name:
                raise SystemExit(f"FAILURE: Database name '{database_name}' is already i use")

        db_id_str = str(database_id)

        # add the database config
        key = self.instance_id + ':db_config'
        self.config_redis.hset(key, db_id_str, json.dumps({
            'id': database_id,
            'name': database_name,
            'replication_slot': replication_slot,
            'publication_name': publication_name,
            'include': {
                'schemas': ['*'],
                'tables': None
            }
        }))
        self.logger.info(f"Added database '{database_name}' to redis hash '{key}'")

        # create initial state entry for this database
        state_key = self.instance_id + ':instance_state'
        self.config_redis.hset(state_key, db_id_str, 'initialize')
        self.logger.info(f"Added database '{database_name}' to redis hash '{state_key}'")

        # add database id to the list of databases
        instance_key = self.instance_id + ':instance_config'
        db_ids = json.loads(self.config_redis.hget(instance_key, 'database_ids'))
        db_ids.append(database_id)
        self.config_redis.hset(instance_key, 'database_ids', json.dumps(db_ids))
        self.logger.info(f"Added database id '{database_id}' to list of database ids in redis hash '{instance_key}:database_ids'")

    def _create_connections(self, database_name: str) -> None:
        """
        This method creates proxy and fdw connections for the given database
        """
        self.logger.info("Creating proxy connection")
        self.proxy_conn = connect_db(database_name, "postgres", "postgres", "proxy", 5432)
        self.log_data(logging.INFO, "Creating FDW connections for", self.fdw_list)
        for fdw_instance in self.fdw_list:
            ip = socket.gethostbyname(fdw_instance)
            self.fdw_ip[ip] = fdw_instance
            self.fdw_conn[fdw_instance] = connect_db(database_name, self.fdw_user, self.fdw_user_password, fdw_instance, 5432)

        # verify that proxy is connected to one of FDWs
        server_ip = self._get_database_ip(self.proxy_conn)
        if server_ip == self.primary_ip:
            self.logger.error(f"Proxy connection is connected to primary")
            raise SystemExit(f"FAILURE: Proxy is connected to primary database")
        if server_ip in self.fdw_ip:
            self.logger.info(f"Proxy connection is connected to {self.fdw_ip[server_ip]}")

    def _verify_proxy_primary_connection(self):
        server_ip = self._get_database_ip(self.proxy_conn)
        if server_ip != self.primary_ip:
            self.logger.error(f"Proxy connection is not connected to primary")
            raise SystemExit(f"FAILURE: Proxy is not connected to primary database")
        self.logger.info(f"Proxy connection is connected to IP: {server_ip}")


    def _verify_table(self, table_name: str, no_fdw: bool = False) -> None:
        self.logger.info(f"Verifying table '{table_name}'")
        primary_data = self._get_test_data(self.primary_conn, table_name)
        self.log_data(logging.DEBUG, "Primary data", primary_data)

        proxy_data = self._get_test_data(self.proxy_conn, table_name)
        self.log_data(logging.DEBUG, "Proxy data", primary_data)
        if primary_data != proxy_data:
            raise SystemExit(f"FAILURE: Primary and proxy data do not match")

        if no_fdw:
            self.logger.info(f"Verified table '{table_name}' has correct data on primary and proxy connections")
            return

        for fdw_id, fdw_conn in self.fdw_conn.items():
            fdw_data = self._get_test_data(fdw_conn, table_name)
            self.log_data(logging.DEBUG, f"FDW ({fdw_id}) data", fdw_data)
            if primary_data != fdw_data:
                raise SystemExit(f"FAILURE: Primary and fdw '{fdw_id}' data do not match")

        self.logger.info(f"Verified table '{table_name}' has correct data on all connections")

    def _create_all_connections(self, database_name: str) -> None:
        """
        This method creates all connection for the given database, which now includes primary database connection,
        and also creates a connection to 'postgres' database on primary.
        """
        self.primary_postgres_conn = connect_db("postgres", "postgres", "postgres", "primary", 5432)
        self.primary_ip = socket.gethostbyname("primary")
        self.primary_conn = connect_db(database_name, "postgres", "postgres", "primary", 5432)
        self._create_connections(database_name)

    def _verify_database_query(self, conn: psycopg2.extensions.connection, database_name: str) -> None:
        """
        This method verifies connection to the given database by doing database name lookup query.
        """
        param = db_name = conn.get_dsn_parameters()
        db_name = param['dbname']
        host = param['host']
        port = param['port']
        user = param['user']

        self.logger.info(f"Verifying connection: db - {db_name}, host - {host}, port - {port}, user - {user}")
        if not self._verify_database_exists(conn, database_name):
            raise SystemExit(f"FAILURE: Failed to query database '{database_name}'")

    def _verify_all_connections(self, database_name: str) -> None:
        """
        This method verifies all connections.
        """
        self._verify_database_query(self.primary_postgres_conn, database_name)
        self._verify_database_query(self.primary_conn, database_name)

        self._verify_database_query(self.proxy_conn, database_name)
        for fdw_id, fdw_conn in self.fdw_conn.items():
            self._verify_database_query(fdw_conn, database_name)

    def _remove_database_from_redis(self, database_name: str, database_id: int) -> None:
        """
        This method removed database related data from redis.
        """
        # verify that this database exists
        db_configs = self.props.get_db_configs()
        found = False
        for db in db_configs:
            if int(db['id']) == database_id and db['name'] == database_name:
                found = True
                break
        if not found:
            self.logger.error(f"Database '{database_name}' with id '{database_id}' is not known")

        db_id_str = str(database_id)

        # remove database id from the list of databases
        instance_key = self.instance_id + ':instance_config'
        db_ids = json.loads(self.config_redis.hget(instance_key, 'database_ids'))
        self.log_data(logging.DEBUG, "Found database ids", db_ids)
        # pprint.pprint(json.dumps(db_ids))
        if database_id not in db_ids:
            self.logger.error(f"Database id '{database_id}' is not found in instance config")
        else:
            db_ids.remove(database_id)
            self.config_redis.hset(instance_key, 'database_ids', json.dumps(db_ids))

        # remove state entry for this database
        self.config_redis.hdel(self.instance_id + ':instance_state', db_id_str)

        # remove the database config
        key = self.instance_id + ':db_config'
        self.config_redis.hdel(key, db_id_str)

    def _verify_connection_alive(self, conn: psycopg2.extensions.connection) -> bool:
        """
        This method verifies that the given database connection is alive by running a very simple query through it.
        """
        if conn is None or conn.closed != 0:
            return False
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
            return True
        except (psycopg2.InterfaceError, psycopg2.OperationalError):
            return False

    def _verify_connections_fail(self, database_name: str) -> None:
        """
        This method verifies that all FDW connections fail, but proxy connection is still alive.
        """
        # NOTE: proxy connection to the database should remain up for now
        if not self._verify_connection_alive(self.proxy_conn):
            raise SystemExit(f"FAILURE: Proxy connection to database '{database_name}' should remain alive")

        for fdw_id, fdw_conn in self.fdw_conn.items():
            if self._verify_connection_alive(fdw_conn):
                raise SystemExit(f"FAILURE: FDW '{fdw_id}' connection to database '{database_name}' is still alive")
            fdw_conn.close()
        self.fdw_conn = {}

    def _remove_database(self, database_name: str, replication_slot: str, publication_name: str) -> None:
        """
        This method remover database.
        """
        # remove publication
        execute_sql(self.primary_conn, f"DROP PUBLICATION IF EXISTS {publication_name}")

        try:
            # remove up replication slot
            out = execute_sql_select(self.primary_conn,
                    f"SELECT pg_drop_replication_slot('{replication_slot}')")
            self.log_data(logging.INFO, "Drop replication slot result", out)
        except Exception as e:
            self.logger.error(f"Failed to remove replication slot '{replication_slot}': '{e}'")

        execute_sql(self.primary_postgres_conn, f"DROP DATABASE {database_name} WITH (FORCE)")
        self.logger.info(f"Removed database '{database_name}'")

        if self._verify_connection_alive(self.primary_conn):
            raise SystemExit(f"FAILURE: Primary connection to database '{database_name}' is still alive")
        self.logger.info(f"Primary connection to database '{database_name}' is no longer alive")
        self.primary_conn.close()
        self.primary_conn = None

        if self._verify_connection_alive(self.proxy_conn):
            raise SystemExit(f"FAILURE: Proxy connection to database '{database_name}' is still alive")
        self.logger.info(f"Proxy connection to database '{database_name}' is no longer alive")
        self.proxy_conn.close()
        self.proxy_conn = None

    def _writer_thread(self, database_name: str, table_name: str):
        """
        This method implements actions performed by the writer thread that keeps writing data to the primary database while
        database is being removed from redis.
        """
        self.logger.info("Creating primary connection for background thread")
        primary_conn = connect_db(database_name, "postgres", "postgres", "primary", 5432)
        counter = 0
        cur = primary_conn.cursor()
        # Signal that we are ready
        self.ready_event.set()

        while not self.stop_event.is_set():
            name = f"user_{counter}"
            age = counter % 100
            cur.execute(
                f"INSERT INTO {table_name} (name, age) VALUES (%s, %s)",
                (name, age),
            )
            counter += 1
            time.sleep(0.05)

        primary_conn.close()

    def run_test(self, database: str, database_id: int, operation: DatabaseOperation) -> None:
        """
        This method performs all database test operations.
        """
        replication_slot = f"springtail_replication_slot_{database_id}"
        publication_name = f"springtail_pub_{database_id}"

        # 1. Verify that all containers are up and running (verify coordinator state in redis)
        self._verify_cluster_up()

        match operation:
            # --- add case
            case DatabaseOperation.ADD_DB:
                # 2. Create database in postgres (primary) and populate it with a test table
                self._create_database(database, replication_slot, publication_name)

                # 3. Access database data through proxy (verify that it is possible to access database through
                #    proxy that has not been added yet) - can't connect, skip this step, connect later
                primary_data = self._get_test_data(self.primary_conn, self.preload_table_name)
                self.logger.info("10 seconds break")
                time.sleep(10)

                # 4. Add database in redis
                self._add_database_to_redis(database, database_id, replication_slot, publication_name)

                # 5. Wait for running state
                self.props.wait_for_state('running', database_id, "", 30)
                self.logger.info("20 seconds break")
                time.sleep(20)

                # 6. Establish proxy and FDW connections to this database
                self._create_connections(database)

                # 7. Verify content of preload table
                self._verify_table(self.preload_table_name)

                # 8. Add a table with some data through primary connection
                self._create_table(self.replicate_table_name)
                time.sleep(5)

                # 9. Verify content of replicate table
                self._verify_table(self.replicate_table_name)

            # --- remove case or remove under load case
            case DatabaseOperation.REMOVE_DB | DatabaseOperation.REMOVE_DB_UNDER_LOAD:
                remove_under_load = (operation == DatabaseOperation.REMOVE_DB_UNDER_LOAD)

                # 2. Create all postgres connections
                self._create_all_connections(database)

                # 3. Verify database exists on all connection
                self._verify_all_connections(database)

                if remove_under_load:
                    # 3.5. Start background thread
                    self.thread = threading.Thread(target=self._writer_thread, args=(database, self.replicate_table_name, ))
                    self.thread.start()
                    self.ready_event.wait()
                    self.logger.info("Load writer thread has started, 2 seconds break")
                    time.sleep(5)

                # 4. Remove database from redis
                self._remove_database_from_redis(database, database_id)
                time.sleep(5)

                # 5. Verify that proxy connection is now connected to primary database
                self._verify_proxy_primary_connection()

                # 6. Verify proxy and FDW connections fail
                self._verify_connections_fail(database)

                # 7. Verify content of preload table on primary and proxy connections
                self._verify_table(self.preload_table_name, True)

                if remove_under_load:
                    # 8. Stop writing thread
                    self.stop_event.set()
                    self.thread.join()
                else:
                    # 8. Verify content of replicate table on primary and proxy connections
                    self._verify_table(self.replicate_table_name, True)

                # 9. Remove database from postgres
                self._remove_database(database, replication_slot, publication_name)

            # --- cleanup case
            case DatabaseOperation.CLEANUP_DB:
                self.primary_postgres_conn = connect_db("postgres", "postgres", "postgres", "primary", 5432)
                self.primary_conn = connect_db(database, "postgres", "postgres", "primary", 5432)
                # just cleanup database without any verification
                self._remove_database_from_redis(database, database_id)
                time.sleep(5)
                self._remove_database(database, replication_slot, publication_name)


def parse_arguments() -> argparse.Namespace:
    """
    Parse the command line arguments.
    """
    # Create the argument parser
    parser = argparse.ArgumentParser(description="Tool for testing database add and remove operations.")

    parser.add_argument('-d', '--database', type=str, required=True,
                        help='Specify database name.')
    parser.add_argument('-i', '--database_id', type=int, required=True,
                        help='Specify database id.')
    parser.add_argument('-a', '--action', type=DatabaseOperation, required=True, choices=list(DatabaseOperation),
                        help="Specify database test action.")
    args = parser.parse_args()

    return args

class CustomFormatter(logging.Formatter):
    """
    This is a custom formatter class for logging.
    """

    def __init__(self, fmt = None, datefmt = None, style = "%", validate = True, *, defaults = None):
        """
        This is custom formatter initializer method.
        """
        super().__init__(fmt, datefmt, style, validate, defaults=defaults)

        grey = "\x1b[38;20m"
        green = "\x1b[32;1m"
        yellow = "\x1b[33;20m"
        red = "\x1b[31;20m"
        bold_red = "\x1b[31;1m"
        reset = "\x1b[0m"

        # self.format = '%(asctime)s.%(msecs)03d %(levelname)-5s [%(filename)s:%(lineno)d] %(message)s'
        self.base_format = '%(asctime)s.%(msecs)03d %(levelname)-5s [%(filename)s:%(lineno)d] %(message)s'
        self.FORMATS = {
            logging.DEBUG: grey + self.base_format + reset,
            logging.INFO: green + self.base_format + reset,
            logging.WARNING: yellow + self.base_format + reset,
            logging.ERROR: red + self.base_format + reset,
            logging.CRITICAL: bold_red + self.base_format + reset
        }
        self.formatters = {
            level: logging.Formatter(fmt) for level, fmt in self.FORMATS.items()
        }

    def format(self, record):
        """
        Formatter format method. It uses appropriate formatter based on the log level of the record.
        """
        formatter = self.formatters.get(record.levelno)
        return formatter.format(record)

def setup_logging() -> None:
    """
    This method performs logging setup.
    """
    logger = logging.getLogger("springtail")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    # setup console handler
    console_handler = logging.StreamHandler(sys.stdout)
    # Show debug logs in console
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(CustomFormatter())
    logger.addHandler(console_handler)


def main() -> None:
    """
    Main function to database testing tool.
    """
    # Parse command line arguments
    args = parse_arguments()
    setup_logging()

    tester = DatabaseTester()
    tester.run_test(args.database, args.database_id, args.action)

if __name__ == "__main__":
    main()
