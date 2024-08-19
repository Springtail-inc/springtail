import sys
import os
import json
import redis
import psycopg2
import subprocess

FDW_SERVER_NAME = 'springtail_fdw_server'
FDW_WRAPPER = 'springtail_fdw'
FDW_SYSTEM_CATALOG = '__springtail_catalog'

# get the xid client path
XID_CLIENT_RELATIVE_PATH = 'debug/src/xid_mgr/xid_client'
SPRINGTAIL_SRC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../')
XID_CLIENT = os.path.join(SPRINGTAIL_SRC_DIR, XID_CLIENT_RELATIVE_PATH)

def execute_sql(conn, sql, args=None):
    """Execute the given sql statement with the given arguments on the given connection."""
    print(f"Executing sql: {sql} with args: {args}")
    try:
        cur = conn.cursor()
        cur.execute(sql, args)
        conn.commit()
    except Exception as e:
        print(f"Error executing sql: {e}")
        conn.rollback()

    return

def run_command(command, args):
    """Run the given command with the given arguments and return the last line of the output."""
    command_with_args = [command] + args
    # Run the external command
    result = subprocess.run(command_with_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=False)

    # Check if the command was successful
    if result.returncode != 0:
        raise Exception(f"Command failed with error: {result.stderr}")

    # Split the output into lines and get the last line
    output_lines = result.stdout.strip().split('\n')
    last_line = output_lines[-1] if output_lines else None

    return last_line

# if using system.json.settings file then get redis settings from there
sys_file = os.environ.get('SPRINGTAIL_PROPERTIES_FILE', None)
if sys_file is not None:
    print(f"Reading redis settings from {sys_file}")
    with open(sys_file) as file:
        system_json = json.load(file)

    if system_json['redis'] is None:
        print('system.json.settings does not have redis settings', file=sys.stderr)
        sys.exit(1)

    redis_host = system_json['redis']['host']
    redis_port = system_json['redis']['port']
    redis_user = system_json['redis']['user']
    redis_password = system_json['redis']['password'] if system_json['redis']['password'] else None
    redis_user_db = system_json['redis']['db']
    db_instance_id = system_json['org']['db_instance_id']
    fdw_id = system_json['org']['fdw_id']
else:
    # otherwise, read in redis settings from environment
    redis_host = os.environ.get('REDIS_HOST', 'localhost')
    redis_port = os.environ.get('REDIS_PORT', 6379)
    redis_user = os.environ.get('REDIS_USER', 'default')
    redis_password = os.environ.get('REDIS_PASSWORD', None)
    redis_user_db = os.environ.get('REDIS_USER_DATABASE_ID', 0)
    db_instance_id = os.environ.get('DATABASE_INSTANCE_ID', None)
    fdw_id = os.environ.get('FDW_ID', None)

# Connect to the Redis server
r = redis.StrictRedis(host=redis_host, port=redis_port, db=0, password=redis_password, encoding="utf-8", decode_responses=True)

# fetch the database ids for the db instance
db_instance_key = 'instance_config:' + str(db_instance_id)
ids = r.hget(db_instance_key, 'database_ids')
print(db_instance_key, ids)
db_ids = json.loads(r.hget(db_instance_key, 'database_ids'))

# dict of db name to list of schemas
db_schemas = {}

# fetch the db name and schemas for each database
for db_id in db_ids:
    db_key = 'db_config:' + str(db_instance_id) + ':' + str(db_id)
    db = json.loads(r.get(db_key))
    dbname = db['name']
    schemas = db['schemas'].keys()
    db_schemas[dbname] = {'id': db_id, 'schemas': schemas }

print(db_schemas)

# fetch the fdw settings for this fdw
fdw = json.loads(r.hget('fdw:' + str(db_instance_id), fdw_id))

# create databases first
conn = psycopg2.connect(dbname='postgres', user=fdw['user'], password=fdw['password'], host='localhost', port=['port'])
for db_name in db_schemas:
    execute_sql(conn, "CREATE DATABASE IF NOT EXISTS %s;", (db_name))

# get the current xid from xid mgr
xid = run_command(XID_CLIENT, ['-g'])

print(f"Using xid: {xid}")

# connect to database
for db_name in db_schemas.keys():
    conn = psycopg2.connect(dbname=dbname, user=fdw['user'], password=fdw['password'], host='localhost', port=['port'])

    # generate the create server and import foreign schema commands
    db_json = db_schemas[db_name]
    db_id = str(db_json['id'])

    execute_sql(conn, "DROP SERVER %s IF EXISTS;", (FDW_SERVER_NAME))
    execute_sql(conn, "CREATE SERVER %s FOREIGN DATA WRAPPER %s OPTIONS (id %s, db_id %s, db_name %s, schema_xid %s);", (fdw_id, FDW_SERVER_NAME, FDW_WRAPPER, db_id, db_name, xid))

    for schema in db_json['schemas']:
        execute_sql(conn, "IMPORT FOREIGN SCHEMA %s FROM SERVER %s INTO %s;", (schema, FDW_SERVER_NAME, schema))

    # import the system catalog
    execute_sql(conn, "IMPORT FOREIGN SCHEMA %s FROM SERVER %s INTO %s;", (FDW_SYSTEM_CATALOG, FDW_SERVER_NAME, FDW_SYSTEM_CATALOG))

    conn.close()

# notify fdw that the import is complete
conn = psycopg2.connect(dbname='postgres', user=fdw['user'], password=fdw['password'], host='localhost', port=['port'])
execute_sql(conn, "SELECT springtail_fdw_function('startup');")
conn.close()
