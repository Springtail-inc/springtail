# python file to load system.json into redis
import json
import os
import redis
import sys

# extract filename for system.json
if len(sys.argv) > 1:
    file = sys.argv[1]
else:
    file = 'system.json.test'

# Load the system.json.test file as json
with open(file, 'r') as f:
    system_json = json.load(f)

if system_json['redis'] is None:
    print('system.json.settings does not have redis settings', file=sys.stderr)
    sys.exit(1)

if system_json['redis']['db'] == 0:
    print('system.json.settings redis db must not be 0 (0 is reserved for config db)', file=sys.stderr)
    sys.exit(1)

# Connect to the Redis server
redis_host = system_json['redis']['host']
redis_port = system_json['redis']['port']
redis_user = system_json['redis']['user']
redis_password = system_json['redis']['password'] if system_json['redis']['password'] else None
redis_db = 0 # 0 reserved for config db
redis_user_db = system_json['redis']['db']

# Clear the Redis user database
r = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_user_db, password=redis_password, encoding="utf-8", decode_responses=True)
r.flushdb()

# Clear the Redis config database
r = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db, password=redis_password, encoding="utf-8", decode_responses=True)
r.flushdb()

# Load the system settings into Redis
# create hset for db instance
db_instance_id = system_json['org']['db_instance_id']
db_instance_key = 'instance_config:' + str(db_instance_id)
r.hset(db_instance_key, 'id', db_instance_id)

db_instance_json = system_json['db_instances'][str(db_instance_id)]
r.hset(db_instance_key, 'primary_db', json.dumps(db_instance_json['primary_db']))

db_ids = json.dumps(db_instance_json['database_ids'])
r.hset(db_instance_key, 'database_ids', db_ids)

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

r.hset(db_instance_key, 'system_settings', json.dumps(sys_config_json))

# setup db_config
for db_id in db_instance_json['database_ids']:
    db_json = system_json['databases'][str(db_id)]
    db_key = 'db_config:' + str(db_instance_id) + ':' + str(db_id)
    r.set(db_key, json.dumps(db_json))

# create hset for fdws
fdw_key = 'fdw:' + str(db_instance_id)
for fdw_id in system_json['fdws']:
    fdw_json_str = json.dumps(system_json['fdws'][fdw_id])
    r.hset(fdw_key, fdw_id, fdw_json_str)

# generate environment variables
env_vars = {
    'ORGANIZATION_ID': system_json['org']['organization_id'],
    'ACCOUNT_ID': system_json['org']['account_id'],
    'FDW_ID': system_json['org']['fdw_id'],
    'DATABASE_INSTANCE_ID': str(db_instance_id),
    'REDIS_HOSTNAME': redis_host,
    'REDIS_PORT': str(redis_port),
    'REDIS_USER': redis_user,
    'REDIS_PASSWORD': redis_password if redis_password else '',
    'REDIS_USER_DATABASE_ID': system_json['redis']['db'],
    'MOUNT_POINT': system_json['fs']['mount_point'],
}

print ("Loaded redis with contents from {}".format(file))

print ("\nEnvironment variables (please set the following):")
print ("unset SPRINGTAIL_PROPERTIES_FILE")
for e in env_vars:
    print(f"export {e}={env_vars[e]}")



