import json
import sys
import os
import time
import logging
from redis import Redis
from typing import Optional
from common import parse_bool
from aws import AwsHelper

DB_USERS_SECRET = "sk/{}/{}/aws/dbi/{}/primary_db_password"

class Properties:
    def __init__(self, config_file=None, load_redis=False) -> None:
        """Initialize the properties object."""
        self.init(config_file)

        if load_redis:
            try:
                self._load_redis(config_file)
            except KeyError as e:
                raise Exception(f'JSON key error while loading redis, missing key: {e}')

    def init(self, config_file=None) -> None:
        """Initialize the properties object."""
        self.cache = {}

        if config_file is None and 'SPRINGTAIL_PROPERTIES_FILE' in os.environ:
            config_file = os.environ.get('SPRINGTAIL_PROPERTIES_FILE')

        if config_file:
            # remove the environment variable; prevents daemons from reloading redis
            os.environ.pop('SPRINGTAIL_PROPERTIES_FILE', None)

            with open(config_file) as file:
                system_json = json.load(file)

            if system_json['redis'] is None:
                print('system.json.settings does not have redis settings', file=sys.stderr)
                sys.exit(1)

            try:
                self.redis_host = system_json['redis']['host']
                self.redis_port = system_json['redis']['port']
                self.redis_ssl = system_json['redis']['ssl']
                self.redis_user = system_json['redis']['user']
                self.redis_password = system_json['redis']['password'] if system_json['redis']['password'] else None
                self.redis_data_db = system_json['redis']['db']
                self.redis_config_db = system_json['redis']['config_db'] if 'config_db' in system_json['redis'] else 0
                self.redis_ssl = system_json['redis']['ssl'] if 'ssl' in system_json['redis'] else False
                self.db_instance_id = str(system_json['org']['db_instance_id'])
                self.fdw_id = system_json['org']['fdw_id']
                self.fdw_user_password = system_json['org']['fdw_user_password']

                # for test env, replication user and password are set in system.json
                self.replication_user = system_json['org']['replication_user']
                self.replication_user_password = system_json['org']['replication_user_password']

                # not in config file, but will be set in production env
                self.instance_key = None
                self.service_name = None

                # set the environment variables
                env_vars = {
                    'ORGANIZATION_ID': system_json['org']['organization_id'],
                    'ACCOUNT_ID': system_json['org']['account_id'],
                    'FDW_ID': system_json['org']['fdw_id'],
                    'DATABASE_INSTANCE_ID': str(self.db_instance_id),
                    'REDIS_HOSTNAME': self.redis_host,
                    'REDIS_PORT': str(self.redis_port),
                    'REDIS_USER': self.redis_user,
                    'REDIS_PASSWORD': self.redis_password if self.redis_password else '',
                    'REDIS_USER_DATABASE_ID': str(self.redis_data_db),
                    'REDIS_CONFIG_DATABASE_ID': str(self.redis_config_db),
                    'REDIS_SSL': '1' if self.redis_ssl else '0',
                    'MOUNT_POINT': system_json['fs']['mount_point'],
                    'LUSTRE_MOUNT_NAME': system_json['fs']['mount_name'],
                    'LUSTRE_DNS_NAME': system_json['fs']['dns_name'],
                    'FDW_USER_PASSWORD': self.fdw_user_password,
                    'REPLICATION_USER_PASSWORD': self.replication_user_password
                }

                for (key, value) in env_vars.items():
                    if value is not None:
                        os.environ[key] = value

            except KeyError as e:
                raise Exception(f'JSON key error, missing key: {e}')
        else:
            # otherwise, read in redis settings from environment
            self.redis_host = os.environ.get('REDIS_HOST', 'localhost')
            self.redis_port = int(os.environ.get('REDIS_PORT', 6379))
            self.redis_user = os.environ.get('REDIS_USER', 'default')
            self.redis_ssl = parse_bool(os.environ.get('REDIS_SSL', '0'))
            self.redis_password = os.environ.get('REDIS_PASSWORD', None)
            self.redis_data_db = int(os.environ.get('REDIS_USER_DATABASE_ID', 1))
            self.redis_config_db = int(os.environ.get('REDIS_CONFIG_DATABASE_ID', 0))
            self.redis_ssl = parse_bool(os.environ.get('REDIS_SSL', 'false'))
            self.db_instance_id = os.environ.get('DATABASE_INSTANCE_ID', None)
            self.fdw_user_password = os.environ.get('FDW_USER_PASSWORD', None)
            self.fdw_id = os.environ.get('FDW_ID', None)
            self.org_id = os.environ.get('ORGANIZATION_ID', None)
            self.account_id = os.environ.get('ACCOUNT_ID', None)

            # not in config file, but will be set in production env
            self.instance_key = os.environ.get('INSTANCE_KEY', None)
            self.service_name = os.environ.get('SERVICE_NAME', None)

            # fetch replication user from aws secrets manager
            self.aws = AwsHelper()
            (self.replication_user, self.replication_user_password) = self._get_replication_user_from_aws()

        self.redis = Redis(host=self.redis_host, port=self.redis_port, db=self.redis_config_db, ssl=self.redis_ssl,
                           username=self.redis_user, password=self.redis_password, encoding="utf-8", decode_responses=True)

    def _get_replication_user_from_aws(self) -> tuple[str, str]:
        """Get the replication user and password from AWS Secrets Manager."""
        secret = DB_USERS_SECRET.format(self.org_id, self.account_id, self.db_instance_id)

        secret_data = self.aws.get_secret(secret)
        if secret_data is None:
            raise Exception(f"Failed to get secret {secret}")

        for user in secret_data:
            if user['role'] == 'replication':
                return user['username'], user['password']

        raise Exception("Replication user not found in AWS secrets.")

    def get_db_configs(self) -> list[dict]:
        """Return a json array of database instance id:name pairs.
           return: [{"id":, "name":, "replication_slot":, "publication_name": }, ...]
        """
        key = str(self.db_instance_id) + ':instance_config'
        if 'db_configs' in self.cache:
            return self.cache['db_configs']

        ids = json.loads(self.redis.hget(key, 'database_ids'))
        dbs = []
        for id in ids:
            db_key = str(self.db_instance_id) + ':db_config'
            db = json.loads(self.redis.hget(db_key, str(id)))

            dbs.append({"id": id,
                        "name": db['name'],
                        "replication_slot": db['replication_slot'],
                        "publication_name": db['publication_name'],
                        "include": db['include']})

        self.cache[key] = dbs

        return dbs

    def get_db_instance_config(self) -> dict:
        """Return the primary db instance configuration as an object.
        return: {"host":, "port":, "replication_user":, "password":}
        """
        key = str(self.db_instance_id) + ':instance_config'
        if 'db_instance_config' in self.cache:
            return self.cache['db_instance_config']

        config = json.loads(self.redis.hget(key, 'primary_db'))
        config['password'] = self.replication_user_password
        config['replication_user'] = self.replication_user
        self.cache[key] = config

        return config

    def get_fdw_config(self, nocache : bool = False) -> dict:
        """Return a config object for foreign data wrapper configuration."""
        key = str(self.db_instance_id) + ':fdw'
        if 'fdw_config' in self.cache and not nocache:
            return self.cache['fdw_config']

        if not self.fdw_id:
            return {}

        config = json.loads(self.redis.hget(key, self.fdw_id))
        config['password'] = self.fdw_user_password
        self.cache[key] = config

        return config

    def get_proxy_config(self) -> dict:
        """Return the proxy configuration as an object."""
        key = str(self.db_instance_id) + ':instance_config'
        if 'proxy_config' in self.cache:
            return self.cache['proxy_config']

        config = json.loads(self.redis.hget(key, 'system_settings'))
        proxy_config = config['proxy']
        self.cache['proxy_config'] = proxy_config

        return proxy_config

    def get_system_config(self) -> dict:
        """Return the system configuration as an object."""
        key = str(self.db_instance_id) + ':instance_config'
        if 'system_config' in self.cache:
            return self.cache['system_config']

        config = json.loads(self.redis.hget(key, 'system_settings'))
        self.cache[key] = config

        return config

    def get_otel_config(self) -> dict:
        """Return the OpenTelemetry configuration."""
        system_config = self.get_system_config()
        if 'otel' not in system_config:
            raise Exception('otel not found in system settings')
        otel = system_config['otel']
        return otel

    def get_mount_path(self) -> str:
        """Return the mount point for the file system."""
        return os.environ.get('MOUNT_POINT')

    def get_fdw_id(self) -> str:
        """Return the foreign data wrapper id."""
        return self.fdw_id

    def get_db_instance_id(self) -> str:
        """Return the database instance id."""
        return self.db_instance_id

    def get_liveness_hash(self) -> str:
        """Return the liveness hash key."""
        # see common/redis_types.hh HASH_LIVENESS
        return self.db_instance_id + ':hash:liveness'

    def get_liveness_notification_pubsub(self) -> str:
        """Return the liveness notification pubsub channel."""
        # see common/redis_types.hh PUBSUB_LIVENESS_NOTIFY
        return self.db_instance_id + ':pubsub:liveness_notify'

    def _load_redis(self, config_file=None) -> None:
        """Load redis based on a system.json file.
        :param config_file: the system.json file to load, if None
        then use SPRINGTAIL_PROPERTIES_FILE
        """
        if config_file is None and 'SPRINGTAIL_PROPERTIES_FILE' in os.environ:
            config_file = os.environ.get('SPRINGTAIL_PROPERTIES_FILE')

        if config_file is None:
            print('No config file provided')
            sys.exit(1)

        # re-initialize the properties
        self.init(config_file)

        # connect to the Redis config server
        self.redis_data = Redis(host=self.redis_host, port=self.redis_port, db=self.redis_data_db,
                                username=self.redis_user, password=self.redis_password,
                                encoding="utf-8", decode_responses=True, ssl=self.redis_ssl)

        # load the system settings into Redis
        with open(config_file) as f:
            system_json = json.load(f)

        # clear the Redis data and config databases
        self.redis_data.flushdb()
        self.redis.flushdb()

        db_instance_id = system_json['org']['db_instance_id']
        db_instance_key = str(db_instance_id) + ':instance_config'
        self.redis.hset(db_instance_key, 'id', db_instance_id)

        db_instance_json = system_json['db_instances'][str(db_instance_id)]
        self.redis.hset(db_instance_key, 'primary_db', json.dumps(db_instance_json['primary_db']))

        # set the hostnames for ingestion and proxy instances
        self.redis.hset(db_instance_key, 'hostname:ingestion', db_instance_json['hostname:ingestion'])
        self.redis.hset(db_instance_key, 'hostname:proxy', db_instance_json['hostname:proxy'])

        db_ids = json.dumps(db_instance_json['database_ids'])
        self.redis.hset(db_instance_key, 'database_ids', db_ids)

        # extract system config
        sys_config_json = {}
        sys_config_json['logging'] = system_json['logging']
        sys_config_json['iopool'] = system_json['iopool']
        sys_config_json['write_cache'] = system_json['write_cache']
        sys_config_json['xid_mgr'] = system_json['xid_mgr']
        sys_config_json['storage'] = system_json['storage']
        sys_config_json['redis'] = system_json['redis']
        sys_config_json['log_mgr'] = system_json['log_mgr']
        sys_config_json['sys_tbl_mgr'] = system_json['sys_tbl_mgr']
        sys_config_json['proxy'] = system_json['proxy']
        sys_config_json['otel'] = system_json['otel']

        self.redis.hset(db_instance_key, 'system_settings', json.dumps(sys_config_json))

        # setup db_config
        for db_id in db_instance_json['database_ids']:
            db_json = system_json['databases'][str(db_id)]
            # set db_config
            db_key = str(db_instance_id) + ':db_config'
            self.redis.hset(db_key, str(db_id), json.dumps(db_json))

            #set state; default to initialize
            db_state_key = str(db_instance_id) + ':instance_state'
            self.redis.hset(db_state_key, str(db_id), 'initialize')

        # create hset for fdw configs, and set the fdw ids
        fdw_key = str(db_instance_id) + ':fdw'
        for fdw_id in system_json['fdws']:
            fdw_json_str = json.dumps(system_json['fdws'][fdw_id])
            self.redis.hset(fdw_key, fdw_id, fdw_json_str)
            self.redis.sadd(fdw_key + '_ids', fdw_id)

    def wait_for_state(self, state : str, id : int, error_state : str = "", timeout : int = 600) -> None:
        """Wait for the database state to reach the desired state.
        :param state: the state to wait for
        :param id: the database id to check
        :param timeout: the maximum time to wait in seconds (0 wait forever)
        """
        key = self.db_instance_id + ':instance_state'
        start = time.time()
        while True:
            current_state = self.redis.hget(key, str(id))
            if current_state == state:
                return
            if error_state != "" and current_state == error_state:
                raise Exception(f"Database {id} entered error state {error_state}")
            time.sleep(1)
            if time.time() - start > timeout:
                break

        if timeout != 0:
            raise TimeoutError('Timed out waiting for state')

    def get_pid_path(self) -> str:
        """Return the path to the pid file."""
        system_config = self.get_system_config()
        if 'pid_path' not in system_config['logging']:
            raise Exception('pid_path not found in system settings')
        pid_path = system_config['logging']['pid_path']
        return pid_path

    def get_log_path(self) -> str:
        """Return the path to the log file."""
        system_config = self.get_system_config()
        if 'log_path' not in system_config['logging']:
            raise Exception('log_path not found in system settings')

        log_path = system_config['logging']['log_path']

        # check if the log path is a file; if so return the directory
        if os.path.isfile(log_path):
            log_path = os.path.dirname(system_config['logging']['log_path'])

        return log_path

    def get_data_redis(self) -> Redis:
        """Return the data redis object."""
        return Redis(host=self.redis_host, port=self.redis_port, db=self.redis_data_db,
                     username=self.redis_user, password=self.redis_password,
                     encoding="utf-8", decode_responses=True, ssl=self.redis_ssl)

    def get_config_redis(self) -> Redis:
        """Return the config redis object."""
        return Redis(host=self.redis_host, port=self.redis_port, db=self.redis_config_db,
                     username=self.redis_user, password=self.redis_password,
                     encoding="utf-8", decode_responses=True, ssl=self.redis_ssl)

    def get_hostname(self, type : str) -> str:
        """Return the hostname for the given type."""
        key = self.db_instance_id + ':instance_config'
        return self.redis.hget(key, f'hostname:{type}')

    def get_db_states(self) -> dict:
        """Return a dictionary of database states."""
        key = self.db_instance_id + ':instance_state'
        return self.redis.hgetall(key)

    def set_db_state(self, dbname : str, state :str) -> None:
        """Set the state of a database, use cautiously."""
        db_configs = self.get_db_configs()
        for db in db_configs:
            if db['name'] == dbname:
                key = self.db_instance_id + ':instance_state'
                self.redis.hset(key, db['id'], state)
                self.redis.publish(self.db_instance_id + ':pubsub:db_state_changes', str(db['id']) + ':' + state)
                return
        logging.error(f"Database {dbname} not found, setting state failed")

    def add_database(self, dbname : str) -> None:
        """Add a database to the database instance."""
        # check if the database already exists
        db_configs = self.get_db_configs()
        max_id = 0
        for db in db_configs:
            if int(db['id']) > max_id:
                max_id = int(db['id'])
            if db['name'] == dbname:
                return

        # create a new database id
        new_id = str(max_id+1)

        # add the database config
        key = self.db_instance_id + ':db_config'
        self.redis.hset(key, new_id, json.dumps({
            'name': dbname,
            'replication_slot': dbname + '_slot',
            'publication_name': dbname + '_pub',
            'include': {
                'schemas': ['*']
            }
        }))

        # clear the cache
        self.cache.pop('db_configs', None)

        # add the database to the database instance
        instance_key = self.db_instance_id + ':instance_config'
        db_ids = json.loads(self.redis.hget(instance_key, 'database_ids'))
        db_ids.append(new_id)
        self.redis.hset(instance_key, 'database_ids', json.dumps(db_ids))
        self.cache.pop('db_instance_config', None)

        # set the state to initialize
        self.redis.hset(self.db_instance_id + ':instance_state', new_id, 'initialize')

    def get_coordinator_state(self) -> str:
        """Return the coordinator state."""
        key = self.db_instance_id + ':coordinator_state'
        if not self.service_name or not self.instance_key:
            return 'running' # for test env.
        field_key = self.service_name + ':' + self.instance_key
        return self.redis.hget(key, field_key)

    def set_coordinator_state(self, state: str) -> None:
        """Set the coordinator state."""
        if not self.service_name or not self.instance_key:
            return # for test env.
        key = self.db_instance_id + ':coordinator_state'
        field_key = self.service_name + ':' + self.instance_key
        self.redis.hset(key, field_key, state)

def main():
    # init logging for console output
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

    # call the get functions and print the results
    props = Properties()
    print(f"db_configs: {props.get_db_configs()}")
    print(f"db_instance_config: {props.get_db_instance_config()}")
    if props.get_fdw_id():
        print(f"fdw_config: {props.get_fdw_config()}")
    print(f"proxy_config: {props.get_proxy_config()}")
    print(f"system_config: {props.get_system_config()}")
    print(f"mount_path: {props.get_mount_path()}")
    print(f"fdw_id: {props.get_fdw_id()}")
    print(f"db_instance_id: {props.get_db_instance_id()}")
    print(f"liveness_hash: {props.get_liveness_hash()}")
    print(f"liveness_notification_pubsub: {props.get_liveness_notification_pubsub()}")
    print(f"pid_path: {props.get_pid_path()}")
    print(f"log_path: {props.get_log_path()}")
    print(f"ingest_host: {props.get_hostname('ingestion')}")
    print(f"proxy_host: {props.get_hostname('proxy')}")
    print(f"coordinator_state: {props.get_coordinator_state()}")

if __name__ == "__main__":
    main()
