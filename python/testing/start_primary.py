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
    check_backtrace,
    extract_backtrace
)

from linear import Linear

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


def execute_startup_sql(props : Properties, sql_file : str) -> None:
    """Execute the startup SQL file."""
    # Connect to the primary database
    db_name = props.get_db_configs()[0]['name']
    conn = connect_db_instance(props, db_name)

    # Execute the startup SQL file
    execute_sql_script(conn, sql_file)

    # Close the connection
    conn.close()

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
    execute_sql_script(conn, trigger_sql)

    # Close the connection
    conn.close()

def check_log_writable(props : Properties) -> None:
    """Check the log path is writable."""
    # Get the log path and check the parent directory is writable
    log_path = props.get_log_path()

    print(f"Checking log path {log_path} is writable...")
    set_dir_writable(log_path)

def start(config_file: str,
          build_dir: str,
          sql_file: str = None,
          do_cleanup: bool = True) -> None:
    """Main function to start the Springtail system."""
    # must do init if we are performing cleanup
    do_init = False
    if do_cleanup:
        do_init = True

    # get absolute path for config_file
    config_file = os.path.abspath(config_file)

    # Load the system properties from the system.json file
    # also does a load redis from the system file if do_cleanup is True
    props = Properties(config_file, do_init)

    # Print the system properties
    print_sys_props(props, config_file)

    # Check the configuration
    check_config(props)

    print("\nClearing file system data...")
    cleanup_filesystem(props)

    # get parent directory of build_dir
    parent_dir = os.path.dirname(build_dir)

    env_file = os.path.join(parent_dir, 'src', 'pg_fdw', 'environment')
    print(f"Installing environment file: {env_file}")
    if is_linux():
        run_command('sudo', ['cp', env_file, '/etc/postgresql/16/main/'])
    else:
        run_command('sudo', ['cp', env_file, '/opt/homebrew/var/postgresql@16/'])

    # start postgres
    print("Starting postgres...")
    start_postgres()

    # start replication on db instance
    print("\nStarting replication on database instance...")
    check_log_writable(props)
    start_replication(props, build_dir)

    if sql_file:
        # execute startup sql
        print(f"\nExecuting startup SQL file: {sql_file}")
        execute_startup_sql(props, sql_file)

def parse_arguments() -> argparse.Namespace:
    """Parse the command line arguments."""
    # Create the argument parser
    parser = argparse.ArgumentParser(description="Process command-line arguments for config file and build directory.")

    # Add arguments -f for config file and -b for build directory
    parser.add_argument('-f', '--config-file', type=str, required=True, help='Path to the configuration file')
    parser.add_argument('-b', '--build-dir', type=str, required=True, help='Path to the build directory')
    parser.add_argument('-s', '--sql-file', type=str, required=False, help='Path to a sql file to execute prior to startup')
    parser.add_argument('-c', '--csv-file', type=str, required=False, help='Path to a sql file to execute prior to startup')
    parser.add_argument('--cleanup', action='store_true', help="Start without reinitializing the system")
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
        start(args.config_file, args.build_dir, args.sql_file, do_cleanup=args.cleanup)

    except Exception as e:
        print(f"Caught error: {e}")
        traceback.print_exc()
        sys.exit(1)
