import os
import sys
import shutil
import argparse
import traceback
from psycopg2.extensions import quote_ident

# Get the parent directory of the current script (i.e., the project root directory)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the /shared directory to the Python path
sys.path.append(os.path.join(project_root, 'shared'))

# Now import the Properties class
from properties import Properties

from common import (
    connect_db,
    execute_sql,
    execute_sql_select,
    execute_sql_script,
    run_command)

from sysutils import (
    stop_daemons,
    clean_fs,
    check_postgres_running,
    start_postgres,
    stop_postgres,
    start_daemons,
    running_daemons,
    is_linux)

# Constants
FDW_SERVER_NAME = 'springtail_fdw_server'
FDW_WRAPPER = 'springtail_fdw'
FDW_SYSTEM_CATALOG = '__pg_springtail_catalog'

def get_lib_ext():
    """Get the library extension for the current platform."""
    if is_linux:
        return 'so'
    return 'dylib'


def cleanup_filesystem(props):
    """Clear the file system data at the given mount path."""
    # Get the mount path
    mount_path = props.get_mount_path()
    sys_config = props.get_system_config()
    log_path = sys_config['logging']['log_path']

    clean_fs(mount_path, log_path)


def connect_db_instance(props, db_name='postgres'):
    """Connect to the database instance and return connection."""
    # Get the database instance configuration
    db_instance_config = props.get_db_instance_config()
    db_host = db_instance_config['host']
    db_port = db_instance_config['port']
    db_user = db_instance_config['replication_user']
    db_password = db_instance_config['password']

    # Connect to the database
    conn = connect_db(db_name, db_user, db_password, db_host, db_port)

    return conn


def connect_fdw_instance(props, db_name='postgres'):
    """Connect to the foreign data wrapper instance and return connection."""
    # Get the fdw configuration
    fdw_config = props.get_fdw_config()
    fdw_host = fdw_config['host']
    fdw_port = fdw_config['port']
    fdw_user = fdw_config['fdw_user']
    fdw_password = fdw_config['password']

    # Connect to the database
    conn = connect_db(db_name, fdw_user, fdw_password, fdw_host, fdw_port)

    return conn


def cleanup_db_instance(props):
    """Cleanup the database instance.
       Drop and recreate the db and execute cleanup SQL statements.
    """
    # connect to the db instance
    db_config = props.get_db_configs()[0]
    db_name = db_config['name']
    slot_name = db_config['replication_slot']
    pub_name = db_config['publication_name']

    if not check_postgres_running():
        start_postgres()

    # Connect to the database ("postgres" database)
    conn = connect_db_instance(props)

    # Drop and recreate the database
    execute_sql(conn, f"DROP DATABASE IF EXISTS {quote_ident(db_name, conn)};")
    execute_sql(conn, f"CREATE DATABASE {quote_ident(db_name, conn)};")

    conn.close()

    # Connect to the database
    conn = connect_db_instance(props, db_name)

    # Execute the cleanup SQL statements
    execute_sql(conn, "DROP EVENT TRIGGER IF EXISTS springtail_event_trigger_for_drops;")
    execute_sql(conn, "DROP EVENT TRIGGER IF EXISTS springtail_event_trigger_for_ddl;")
    execute_sql(conn, "DROP FUNCTION IF EXISTS springtail_add_replica_identity_full;")
    execute_sql(conn, "DROP FUNCTION IF EXISTS springtail_event_trigger_for_drops;")
    execute_sql(conn, "DROP FUNCTION IF EXISTS springtail_event_trigger_for_ddl;")

    slot_exists = execute_sql_select(conn, "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s;", slot_name)
    if slot_exists:
        execute_sql(conn, "SELECT pg_drop_replication_slot(%s);", slot_name)

    execute_sql(conn, f"DROP PUBLICATION IF EXISTS {quote_ident(pub_name, conn)};")

    # Close the database connection
    conn.close()


def install_fdw(build_dir):
    """Install the foreign data wrapper extension."""
    # Get the share and lib directories
    share_dir = run_command('pg_config', ['--sharedir'])
    lib_dir = run_command('pg_config', ['--pkglibdir'])

    build_dir = os.path.abspath(build_dir)
    print (f"Build dir: {build_dir}")

    # stop postgres
    stop_postgres()

    # copy the extension files to the share directory
    share_dir = os.path.join(share_dir.strip(), 'extension')
    print(f"Copying extension files to the share directory: {share_dir}")
    # get parent directory of build_dir
    parent_dir = os.path.dirname(build_dir)

    if is_linux:
        run_command('sudo', ['cp', str(os.path.join(parent_dir, 'src/pg_fdw/springtail_fdw--1.0.sql')), share_dir])
        run_command('sudo', ['cp', str(os.path.join(parent_dir, 'src/pg_fdw/springtail_fdw.control')), share_dir])
    else:
        shutil.copy(os.path.join(parent_dir, 'src/pg_fdw/springtail_fdw--1.0.sql'), share_dir)
        shutil.copy(os.path.join(parent_dir, 'src/pg_fdw/springtail_fdw.control'), share_dir)

    # copy the shared library to the lib directory
    lib_dir = os.path.join(lib_dir.strip(), 'springtail_fdw.' + get_lib_ext())
    print(f"Copying shared library to the lib directory: {lib_dir}")

    if is_linux:
        run_command('sudo', ['cp', os.path.join(build_dir, 'src/pg_fdw/libspringtail_pg_fdw.so'), lib_dir])
    else:
    shutil.copy(os.path.join(build_dir, 'src/pg_fdw/libspringtail_pg_fdw.dylib'), lib_dir)
    shutil.copy(os.path.join(build_dir, 'src/pg_fdw/libspringtail_pg_fdw.dylib'), lib_dir)

        shutil.copy(os.path.join(build_dir, 'src/pg_fdw/libspringtail_pg_fdw.dylib'), lib_dir)

        # create the debug symbols
        print(f"Creating debug symbols for the shared library: {lib_dir}")
        run_command('dsymutil', [lib_dir])

    # start postgres
    print("Starting postgres...")
    start_postgres()


def start_replication(props, build_dir):
    """Start the replication process."""
    # Get db config
    db_config = props.get_db_configs()[0]
    db_name = db_config['name']
    slot_name = db_config['replication_slot']
    pub_name = db_config['publication_name']

    # Connect to the primary database
    conn = connect_db_instance(props, db_name)

    # Create the publication
    execute_sql(conn, f"CREATE PUBLICATION {quote_ident(pub_name, conn)} FOR ALL TABLES;")

    # Create the replication slot
    execute_sql(conn, "SELECT pg_create_logical_replication_slot(%s, 'pgoutput');", slot_name)

    # Trigger scripts
    parent_dir = os.path.dirname(build_dir)
    trigger_sql = os.path.join(parent_dir, 'scripts/triggers.sql')
    execute_sql_script(conn, trigger_sql)

    # Close the connection
    conn.close()


def fetch_schemas(props, db_name):
    """Fetch the schemas for the given database."""
    # Connect to the database
    conn = connect_fdw_instance(props, db_name)

    # Get the schemas
    schemas = []
    rows = execute_sql_select(conn, "SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT LIKE 'pg_%' AND schema_name <> 'information_schema';")
    for row in rows:
        schemas.append(row[0])

    # Close the connection
    conn.close()

    return schemas


def fdw_import(props, build_dir, config_file):
    """Import the foreign data wrapper schemas."""
    fdw_config = props.get_fdw_config()
    db_configs = props.get_db_configs()
    mount_path = props.get_mount_path()

    print(f"Copying config file to mount path: {os.path.join(mount_path, 'system.json')}")
    shutil.copy(config_file, os.path.join(mount_path, 'system.json'))
    config_file = os.path.join(mount_path, 'system.json')

    # hash of db names to set of schemas
    dbs = {}
    for db_config in db_configs:
        includes = db_config['include']

        # check for schemas in includes, if so fetch them
        if '*' in includes['schemas']:
            schemas = fetch_schemas(props, db_config['name'])
        else:
            schemas = includes['schemas']

        # check for tables in includes, if so add their schemas
        if 'tables' in includes:
            for table in includes['tables']:
                schemas.append(table['schema'])

        if 'db_prefix' in fdw_config:
            db_name = fdw_config['db_prefix'] + db_config['name']
        else:
            db_name = db_config['name']

        dbs[db_name] = {'id': db_config['id'], 'schemas': set(schemas)}

    # Connect to the foreign data wrapper instance
    conn = connect_fdw_instance(props)

    rows = execute_sql_select(conn, "SHOW \"springtail_fdw.config_file_path\";")
    if len(rows) == 0 or rows[0][0] != config_file:
        # Set the config file option 'springtail_fdw.config_file_path' to the config file path and reload
        execute_sql(conn, "ALTER SYSTEM SET \"springtail_fdw.config_file_path\" TO %s;", config_file)
        execute_sql(conn, "SELECT pg_reload_conf();")

        conn.close()

        # Re-connect to the foreign data wrapper instance, not sure why but
        # the reload doesn't seem to take effect immediately
        conn = connect_fdw_instance(props)

        # verify the config file path
        rows = execute_sql_select(conn, "SHOW \"springtail_fdw.config_file_path\";")
        if len(rows) == 0 or rows[0][0] != config_file:
            raise Exception("Failed to set the config file path")

    # drop and create the databases
    print("Creating databases on FDW...")
    for db_name in dbs:
        execute_sql(conn, f"DROP DATABASE IF EXISTS {quote_ident(db_name, conn)};")
        execute_sql(conn, f"CREATE DATABASE {quote_ident(db_name, conn)};")

    # setup primary db
    execute_sql(conn, f"DROP EXTENSION IF EXISTS {quote_ident(FDW_WRAPPER, conn)};")
    execute_sql(conn, f"DROP FUNCTION IF EXISTS springtail_fdw_startup;")
    execute_sql(conn, f"CREATE EXTENSION IF NOT EXISTS {quote_ident(FDW_WRAPPER, conn)};")

    conn.close()

    # get the fdw id
    fdw_id = props.get_fdw_id()

    # connect to each database, create the foreign server and import the foreign schema
    for db in dbs:
        db_name = db
        db_id = dbs[db]['id']
        schemas = dbs[db]['schemas']

        # get the current xid from xid mgr for this database
        xid = run_command(os.path.join(build_dir, 'src/xid_mgr/xid_client'), ['-g', '-d', db_id])

        # Connect to the database
        conn = connect_fdw_instance(props, db_name)

        # Create the foreign server
        execute_sql(conn, f"DROP EXTENSION IF EXISTS {quote_ident(FDW_WRAPPER, conn)};")
        execute_sql(conn, f"DROP FUNCTION IF EXISTS springtail_fdw_startup;")
        execute_sql(conn, f"CREATE EXTENSION IF NOT EXISTS {quote_ident(FDW_WRAPPER, conn)};")
        execute_sql(conn, f"DROP SERVER IF EXISTS {quote_ident(FDW_SERVER_NAME, conn)} CASCADE;")
        execute_sql(conn, "CREATE SERVER {} FOREIGN DATA WRAPPER {} OPTIONS (id %s, db_id %s, db_name %s, schema_xid %s);".format(quote_ident(FDW_SERVER_NAME, conn), quote_ident(FDW_WRAPPER, conn)), (fdw_id, db_id, db_name, xid))

        # Import the foreign schema
        for schema in schemas:
            execute_sql(conn, f"CREATE SCHEMA IF NOT EXISTS {quote_ident(schema, conn)};")
            execute_sql(conn, f"IMPORT FOREIGN SCHEMA {quote_ident(schema, conn)} FROM SERVER {quote_ident(FDW_SERVER_NAME, conn)} INTO {quote_ident(schema, conn)}")

        # import the system catalog
        execute_sql(conn, f"CREATE SCHEMA IF NOT EXISTS {quote_ident(FDW_SYSTEM_CATALOG, conn)};")
        execute_sql(conn, f"IMPORT FOREIGN SCHEMA {quote_ident(FDW_SYSTEM_CATALOG, conn)} FROM SERVER {quote_ident(FDW_SERVER_NAME, conn)} INTO {quote_ident(FDW_SYSTEM_CATALOG, conn)}")

        # Close the connection
        conn.close()

    # Connect to the foreign data wrapper instance and execute the startup function
    conn = connect_fdw_instance(props)
    execute_sql(conn, "SELECT springtail_fdw_startup(%s)", fdw_id)
    conn.close()


def wait_for_running(props):
    """Wait for the system to be in a running state."""
    # Wait for the system to be in a running state
    db_config = props.get_db_configs()[0]
    id = db_config['id']
    props.wait_for_state('running', id)


def execute_startup_sql(props, sql_file):
    """Execute the startup SQL file."""
    # Connect to the primary database
    db_name = props.get_db_configs()[0]['name']
    conn = connect_db_instance(props, db_name)

    # Execute the startup SQL file
    execute_sql_script(conn, sql_file)

    # Close the connection
    conn.close()


def print_sys_props(props, config_file):
    """Print the system properties."""
    db_config = props.get_db_configs()[0]

    print("\nSystem properties:")
    print(f"  Config file    : {config_file}")
    print(f"  Mount path     : {props.get_mount_path()}")
    print(f"  Pid path       : {props.get_pid_path()}")
    print(f"  DB instance ID : {props.get_db_instance_id()}")
    print(f"  Primary DB name: {db_config['name']}")
    print(f"  Primary DB ID  : {db_config['id']}")
    print(f"  FDW ID         : {props.get_fdw_id()}")


def check_config(props):
    """Check the system configuration for invalid settings."""
    db_instance_config = props.get_db_instance_config()
    fdw_config = props.get_fdw_config()

    db_host = db_instance_config['host']
    db_port = db_instance_config['port']

    fdw_host = fdw_config['host']
    fdw_port = fdw_config['port']
    fdw_prefix = fdw_config['db_prefix'] if 'db_prefix' in fdw_config else None

    # Check that the primary DB and FDW are not on the same host and port
    # if fdw_prefix is not set
    if (fdw_prefix is None or fdw_prefix == '') and (db_host == fdw_host and db_port == fdw_port):
        raise Exception("Primary DB and FDW cannot be on the same host and port.  Please set the 'db_prefix' in the FDW configuration.")

    # Check that the pid path exists; if not try to create it
    pid_path = props.get_pid_path()
    if not os.path.exists(pid_path):
        try:
            run_command('sudo', ['mkdir', '-p', pid_path])
        except Exception as e:
            raise Exception(f"Failed to create pid path: {pid_path}, please create it manually.")


def start(args):
    """Main function to start the Springtail system."""
    # Get the config file and build directory from the command line arguments
    config_file = args.config_file
    build_dir = args.build_dir
    sql_file = args.sql_file

    # get absolute path for config_file
    config_file = os.path.abspath(config_file)

    # Load the system properties from the system.json file
    # also does a load redis from the system file
    props = Properties(config_file, True)

    # Print the system properties
    print_sys_props(props, config_file)

    # Check the configuration
    check_config(props)

    # Clear file system data
    print("\nClearing file system data...")
    cleanup_filesystem(props)

    # Stop the daemons
    print("\nStopping daemons...")
    stop_daemons(props.get_pid_path())

    # cleanup db instance
    print("\nCleaning up database instance...")
    cleanup_db_instance(props)

    if sql_file:
        # execute startup sql
        print(f"\nExecuting startup SQL file: {sql_file}")
        execute_startup_sql(props, sql_file)

    # install fdw
    print("\nInstalling foreign data wrapper...")
    install_fdw(build_dir)

    # start replication on db instance
    print("\nStarting replication on database instance...")
    start_replication(props, build_dir)

    # start daemons
    print("\nStarting daemons...")
    start_daemons(build_dir)

    # wait for running state
    print("\nWaiting for running state...")
    wait_for_running(props)

    # import the fdw schemas
    print("\nImporting foreign data wrapper schemas...")
    fdw_import(props, build_dir, config_file)

    print("\nSpringtail system started successfully.")


def status():
    print("Checking status...")

    procs = running_daemons()
    if len(procs) == 0:
        print("No daemons running.")
    else:
        print("Daemons running:")
        for proc in procs:
            print(f"  {proc['name']} with PID: {proc['pid']}")

    if check_postgres_running():
        print("Postgres is running.")
    else:
        print("Postgres is not running.")


def stop():
    """Function to stop the Springtail system."""
    # Get the config file and build directory from the command line arguments
    config_file = args.config_file

    # get absolute path for config_file
    config_file = os.path.abspath(config_file)

    # Load the system properties from the system.json file
    props = Properties(config_file, False)

    # Stop the daemons
    print("\nStopping daemons...")
    stop_daemons(props.get_pid_path())


def parse_arguments():
    # Create the argument parser
    parser = argparse.ArgumentParser(description="Process command-line arguments for config file and build directory.")

    # Add arguments -f for config file and -b for build directory
    parser.add_argument('-f', '--config-file', type=str, required=True, help='Path to the configuration file')
    parser.add_argument('-b', '--build-dir', type=str, required=True, help='Path to the build directory')
    parser.add_argument('-s', '--sql-file', type=str, required=False, help='Path to a sql file to execute prior to startup')
    parser.add_argument('--start', action=argparse.BooleanOptionalAction, help="Start the Springtail system")
    parser.add_argument('--status', action=argparse.BooleanOptionalAction, help="Check the status of the Springtail daemons")
    parser.add_argument('--kill', action=argparse.BooleanOptionalAction, help="Kill the Springtail daemons")
    parser.add_argument('--debug', action=argparse.BooleanOptionalAction, help="Print debug information on error")

    # Parse the arguments and return them
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    # Parse command line arguments
    args = parse_arguments()

    if not args.start and not args.status and not args.kill:
        print("No action specified. Use --start, --status, or --kill.")
        sys.exit(1)

    try:
        if args.status:
            status()
            sys.exit(0)

        if args.kill:
            stop()
            sys.exit(0)

        if args.start:
            start(args)

    except Exception as e:
        print(f"Caught error: {e}")
        if (args.debug):
            traceback.print_exc()
        sys.exit(1)
