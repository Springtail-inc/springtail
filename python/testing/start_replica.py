import glob
import os
import sys
import shutil
import glob
import argparse
import traceback
import tempfile
import time
import logging
from typing import Dict, List, Optional
import psycopg2
from psycopg2.extensions import quote_ident

# Get the parent directory of the current script (i.e., the project root directory)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the /shared directory to the Python path
sys.path.append(os.path.join(project_root, 'shared'))
sys.path.append(os.path.join(project_root, 'grpc'))

# Now import the Properties class
from properties import Properties
from xid_mgr import XidMgrClient

from common import (
    connect_db,
    execute_sql,
    execute_sql_select,
    execute_sql_script,
    run_command,
    set_dir_writable,
    is_linux,
    makedir
)

from sysutils import (
    clean_fs,
    check_postgres_running,
    start_postgres,
    stop_postgres,
    stop_daemons,
    start_daemons,
    check_daemons_running,
    check_backtrace,
    extract_backtrace
)

from linear import Linear

# Constants
FDW_SERVER_NAME = 'springtail_fdw_server'
FDW_WRAPPER = 'springtail_fdw'
FDW_SYSTEM_CATALOG = '__pg_springtail_catalog'

# List of daemons to start tuple: (name, path, args)
CORE_DAEMONS = [
    ('xid_mgr_daemon', 'src/xid_mgr/xid_mgr_daemon', '-x,10'),
    ('sys_tbl_mgr_daemon', 'src/sys_tbl_mgr/sys_tbl_mgr_daemon'),
    ('pg_log_mgr_daemon', 'src/pg_log_mgr/pg_log_mgr_daemon')
]

FDW_DAEMONS = [
    ('pg_ddl_daemon', 'src/pg_fdw/pg_ddl_daemon', '-s,/var/run/postgresql'),
    ('pg_xid_subscriber_daemon', 'src/pg_fdw/pg_xid_subscriber_daemon')
]

PROXY_DAEMONS = [
    ('proxy', 'src/proxy/proxy')
]

ALL_DAEMONS = CORE_DAEMONS + FDW_DAEMONS + PROXY_DAEMONS

ALL_DAEMONS_NAMES = [name[0] for name in ALL_DAEMONS]

def get_lib_ext() -> str:
    """Get the library extension for the current platform."""
    if is_linux():
        return 'so'
    return 'dylib'

def check_log_writable(props : Properties) -> None:
    """Check the log path is writable."""
    # Get the log path and check the parent directory is writable
    log_path = props.get_log_path()

    print(f"Checking log path {log_path} is writable...")
    set_dir_writable(log_path)


def fixup_log_perms(props : Properties) -> None:
    """Fix up permissions for the given mount path."""
    # Fix up permissions for the given mount path
    if is_linux():
        log_path = props.get_log_path()
        for log in glob.glob(os.path.join(log_path, '*.log')):
            run_command('sudo', ['chmod', 'a+r', log])

def cleanup_filesystem(props : Properties) -> None:
    """Clear the file system data at the given mount path."""
    # Get the mount path
    mount_path = props.get_mount_path()
    log_path = props.get_log_path()
    pid_path = props.get_pid_path()

    clean_fs(mount_path, log_path)

    # Create log path if it doesn't exist
    makedir(log_path, '777')

    # Create the mount path if it doesn't exist
    makedir(mount_path, '755')

    # Check that the pid path exists; if not try to create it
    makedir(pid_path, '755')


def connect_db_instance(props : Properties, db_name : str ='postgres') -> psycopg2.extensions.connection:
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

def connect_fdw_instance(props : Properties, db_name : str ='postgres') -> psycopg2.extensions.connection:
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

def install_fdw(build_dir : str) -> None:
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

    if is_linux():
        run_command('sudo', ['cp', str(os.path.join(parent_dir, 'src/pg_fdw/springtail_fdw--1.0.sql')), share_dir])
        run_command('sudo', ['cp', str(os.path.join(parent_dir, 'src/pg_fdw/springtail_fdw.control')), share_dir])
    else:
        shutil.copy(os.path.join(parent_dir, 'src/pg_fdw/springtail_fdw--1.0.sql'), share_dir)
        shutil.copy(os.path.join(parent_dir, 'src/pg_fdw/springtail_fdw.control'), share_dir)

    # copy the shared library to the lib directory
    lib_dir = os.path.join(lib_dir.strip(), 'springtail_fdw.' + get_lib_ext())
    print(f"Copying shared library to the lib directory: {lib_dir}")

    if is_linux():
        run_command('sudo', ['cp', os.path.join(build_dir, 'src/pg_fdw/libspringtail_pg_fdw.so'), lib_dir])
    else:
        shutil.copy(os.path.join(build_dir, 'src/pg_fdw/libspringtail_pg_fdw.dylib'), lib_dir)

        # create the debug symbols
        print(f"Creating debug symbols for the shared library: {lib_dir}")
        run_command('dsymutil', [lib_dir])

    # copy the dependent springtail libraries to the lib directory
    shlib_dir = '/usr/lib/springtail'
    print(f"Copying dependent libraries to the shared lib directory: {shlib_dir}")

    run_command('sudo', ['mkdir', '-p', shlib_dir])
    run_command('sudo', ['cp', '-a'] + glob.glob(os.path.join(parent_dir, 'shared-lib', '*')) + [shlib_dir])

    # install the environment file
    # XXX we should consider calling 'SHOW config_file;' from within
    #     psql to get the correct directory location
    env_file = os.path.join(parent_dir, 'src', 'pg_fdw', 'environment')
    print(f"Installing environment file: {env_file}")
    if is_linux():
        run_command('sudo', ['cp', env_file, '/etc/postgresql/16/main/'])
    else:
        run_command('sudo', ['cp', env_file, '/opt/homebrew/var/postgresql@16/'])


    # start postgres
    print("Starting postgres...")
    start_postgres()

def start_replication(props : Properties, build_dir : str) -> None:
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

    # Create the replication slot;
    # NOTE: it the slot name needs to be globally unique
    execute_sql(conn, "SELECT pg_create_logical_replication_slot(%s, 'pgoutput');", slot_name)

    # Trigger scripts
    parent_dir = os.path.dirname(build_dir)
    trigger_sql = os.path.join(parent_dir, 'scripts/triggers.sql')
    print(f"Init triggers: {trigger_sql}")
    execute_sql_script(conn, trigger_sql)

    # Close the connection
    conn.close()


def start_fdw_daemons(props : Properties,
                      build_dir : str,
                      config_file : str = None) -> None:
    """Import the foreign data wrapper schemas."""
    fdw_config = props.get_fdw_config()
    db_configs = props.get_db_configs()
    mount_path = props.get_mount_path()

    if config_file is not None:
        print(f"Copying config file to mount path: {os.path.join(mount_path, 'system.json')}")
        shutil.copy(config_file, os.path.join(mount_path, 'system.json'))
        config_file = os.path.join(mount_path, 'system.json')

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

    # startup pg_ddl_daemon; schema import done by pg_ddl_daemon
    print("Starting pg_ddl_daemon...")

    # a little ugly, but need to add args for username password for fdw daemon for ddl user
    daemons = []
    for d in FDW_DAEMONS:
        if d[0] == 'pg_ddl_daemon':
            s = d[2]
            daemons.append((d[0], d[1], s + f',-u,{fdw_config["fdw_user"]},-p,{fdw_config["password"]}'))
        else:
            daemons.append(d)
    start_daemons(build_dir, daemons)


def start_proxy(props : Properties, build_dir : str, restart: bool = False) -> None:
    """Start the proxy."""
    # Start the proxy
    print("Starting proxy...")
    start_daemons(build_dir, PROXY_DAEMONS, restart)

def wait_for_running(props : Properties) -> None:
    """Wait for the system to be in a running state."""
    # Wait for the system to be in a running state
    db_config = props.get_db_configs()[0]
    id = db_config['id']
    props.wait_for_state('running', id, 'failed')

def print_sys_props(props : Properties, config_file : str) -> None:
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


def check_config(props : Properties) -> None:
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

    # Get the mount path
    mount_path = props.get_mount_path()
    log_path = props.get_log_path()
    pid_path = props.get_pid_path()

    # Create log path if it doesn't exist
    makedir(log_path, '777')

    # Create the mount path if it doesn't exist
    makedir(mount_path, '755')

    # Check that the pid path exists; if not try to create it
    makedir(pid_path, '755')

def start(config_file: str,
          build_dir: str) -> None:
    # get absolute path for config_file
    config_file = os.path.abspath(config_file)

    # Load the system properties from the system.json file
    # also does a load redis from the system file if do_cleanup is True
    props = Properties(config_file, False)

    # Stop the daemons
    print("\nStopping daemons...")
    stop_daemons(props.get_pid_path(), ALL_DAEMONS_NAMES)

    # Print the system properties
    print_sys_props(props, config_file)

    # Check the configuration
    check_config(props)

    #print("\nClearing file system data...")
    #cleanup_filesystem(props)

    print("\nInstalling foreign data wrapper...")
    install_fdw(build_dir)

    # start replication on db instance
    #print("\nStarting replication on database instance...")
    #check_log_writable(props)
    #start_replication(props, build_dir)

    start_daemons(build_dir, CORE_DAEMONS)

    # wait for running state
    print("\nWaiting for running state...")
    wait_for_running(props)

    # start the fdw daemons (e.g., ddl manager to import schemas)
    print("\nStarting FDW daemons...")
    start_fdw_daemons(props, build_dir, config_file)
    fixup_log_perms(props)

    # start the proxy
    start_proxy(props, build_dir)

    print("\nSpringtail system started successfully.")


def parse_arguments() -> argparse.Namespace:
    """Parse the command line arguments."""
    # Create the argument parser
    parser = argparse.ArgumentParser(description="Process command-line arguments for config file and build directory.")

    # Add arguments -f for config file and -b for build directory
    parser.add_argument('-f', '--config-file', type=str, required=True, help='Path to the configuration file')
    parser.add_argument('-b', '--build-dir', type=str, required=True, help='Path to the build directory')
    parser.add_argument('--start', action=argparse.BooleanOptionalAction, help="Start the Springtail system")

    # Parse the arguments and return them
    args = parser.parse_args()
    return args

if __name__ == "__main__":
    """Main function to run the Springtail system."""
    # Parse command line arguments
    args = parse_arguments()

    if not args.start:
        print("No action specified. Use --start.")
        sys.exit(1)

    if not is_linux():
        print("This script only supports running on Linux.")
        sys.exit(1)

    try:
        start(args.config_file, args.build_dir)

    except Exception as e:
        print(f"Caught error: {e}")
        traceback.print_exc()
        sys.exit(1)

