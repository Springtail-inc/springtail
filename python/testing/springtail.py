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
    stop_daemons,
    clean_fs,
    check_postgres_running,
    start_postgres,
    stop_postgres,
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
    ('pg_ddl_daemon', 'src/pg_fdw/pg_ddl_daemon', '-s,/var/run/postgresql')
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


def cleanup_db_instance(props : Properties) -> None:
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

    # see if the replication slot exists and drop it on the target database
    # if we don't do this and the slot exists, we can't drop the database
    try:
        # Connect to the database, may fail if it doesn't exist
        conn = connect_db_instance(props, db_name)
        slot_exists = execute_sql_select(conn, "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s;", slot_name)
        if slot_exists:
            execute_sql(conn, "SELECT pg_drop_replication_slot(%s);", slot_name)
    except Exception as e:
        pass

    # Connect to the database ("postgres" database)
    conn = connect_db_instance(props)

    # Drop and recreate the database
    execute_sql(conn, f"DROP DATABASE IF EXISTS {quote_ident(db_name, conn)} WITH (FORCE);")
    execute_sql(conn, f"CREATE DATABASE {quote_ident(db_name, conn)};")

    conn.close()

    # Connect to the database
    conn = connect_db_instance(props, db_name)

    # Cleanup trigger functions
    execute_sql(conn, "DROP SCHEMA IF EXISTS __pg_springtail_triggers CASCADE;")

    slot_exists = execute_sql_select(conn, "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s;", slot_name)
    if slot_exists:
        execute_sql(conn, "SELECT pg_drop_replication_slot(%s);", slot_name)

    execute_sql(conn, f"DROP PUBLICATION IF EXISTS {quote_ident(pub_name, conn)};")

    # Close the database connection
    conn.close()


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


def start_proxy(props : Properties, build_dir : str) -> None:
    """Start the proxy."""
    # Start the proxy
    print("Starting proxy...")
    start_daemons(build_dir, PROXY_DAEMONS)


def wait_for_running(props : Properties) -> None:
    """Wait for the system to be in a running state."""
    # Wait for the system to be in a running state
    db_config = props.get_db_configs()[0]
    id = db_config['id']
    props.wait_for_state('running', id, 'failed')


def execute_startup_sql(props : Properties, sql_file : str) -> None:
    """Execute the startup SQL file."""
    # Connect to the primary database
    db_name = props.get_db_configs()[0]['name']
    conn = connect_db_instance(props, db_name)

    # Execute the startup SQL file
    execute_sql_script(conn, sql_file)

    # Close the connection
    conn.close()


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


def gen_dump_tarball(props : Properties, build_dir : str) -> str:
    """
    Generate a tarball of the log files.
    Returns the path to the tarball.
    """
    # get paths
    mount_path = props.get_mount_path()
    log_path = props.get_log_path()
    fixup_log_perms(props)
    # create a temp directory
    with tempfile.TemporaryDirectory() as tmp_dir:
        # generate a tarball of the log files
        # create a logs directory in the temp directory
        tmp_logs_dir = os.path.join(tmp_dir, 'logs')
        os.mkdir(tmp_logs_dir)

        # create tarball filename including timestamp
        timestr = time.strftime("%Y%m%d-%H%M%S")
        tarfile = 'dump-' + timestr + '.tar.gz'
        tarball = os.path.join(tmp_dir, tarfile)

        # copy the log files to the temp directory
        logs = os.path.join(log_path, '*.log')
        for log in glob.glob(logs):
            shutil.copy(log, tmp_logs_dir)

        # dump the system tables
        run_command(os.path.join(build_dir, 'src/storage/dump_system_tables'), ['1'], os.path.join(tmp_logs_dir, 'system_table.dump'));

        # create the tarball
        run_command('tar', ['-czf', tarball, '-C', tmp_dir, 'logs'])

        # copy the tarball to the mount path
        # create a dumps dir if not exists
        dumps_dir = os.path.join(mount_path, 'dumps')
        if not os.path.exists(dumps_dir):
            os.mkdir(dumps_dir)

        # copy the tarball to the dumps dir
        shutil.copy(tarball, dumps_dir)

        print(f"Log files dumped to tarball: {os.path.join(dumps_dir, tarfile)}")

        return os.path.join(dumps_dir, tarfile)


def current_xid(props: Properties, db_id: int) -> int:
    config = props.get_system_config()
    rpc_config = config['xid_mgr']['rpc_config']

    hostname = props.get_hostname('ingestion')
    port = rpc_config['server_port']
    if not rpc_config['ssl']:
        client = XidMgrClient(hostname, port)
    else:
        client = XidMgrClient(hostname, port, rpc_config['client_trusted'],
                              rpc_config['client_key'], rpc_config['client_cert'])
    return client.get_committed_xid(db_id)


def restart(props: Properties,
            build_dir: str,
            start_xid: int = None) -> None:
    # Stop the daemons
    print("\nStopping daemons...")
    stop_daemons(props.get_pid_path(), ALL_DAEMONS_NAMES)

    # start daemons with XID if specified
    print("\nStarting daemons...")
    if start_xid is not None:
        # Modify xid_mgr_daemon args to include starting XID
        modified_daemons = []
        for daemon in CORE_DAEMONS:
            if daemon[0] == 'xid_mgr_daemon':
                modified_daemons.append((daemon[0], daemon[1], f'-x,{start_xid}'))
            else:
                modified_daemons.append(daemon)
        start_daemons(build_dir, modified_daemons)
    else:
        start_daemons(build_dir, CORE_DAEMONS)

    # wait for running state
    print("\nWaiting for running state...")
    wait_for_running(props)

    # start the fdw daemons (e.g., ddl manager to import schemas)
    print("\nStarting FDW daemons...")
    start_fdw_daemons(props, build_dir)
    fixup_log_perms(props)

    print("\nSpringtail system restarted successfully.")


def start(config_file: str,
          build_dir: str,
          sql_file: str = None,
          do_cleanup: bool = True,
          do_init: bool = True,
          start_xid: int = None) -> None:
    """Main function to start the Springtail system."""
    # must do init if we are performing cleanup
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

    # Stop the daemons
    print("\nStopping daemons...")
    stop_daemons(props.get_pid_path(), ALL_DAEMONS_NAMES)

    if do_cleanup:
        # Clear file system data
        print("\nClearing file system data...")
        cleanup_filesystem(props)

        # cleanup db instance
        print("\nCleaning up database instance...")
        cleanup_db_instance(props)

    if sql_file:
        # execute startup sql
        print(f"\nExecuting startup SQL file: {sql_file}")
        execute_startup_sql(props, sql_file)

    if do_init:
        # install fdw
        print("\nInstalling foreign data wrapper...")
        install_fdw(build_dir)

        # start replication on db instance
        print("\nStarting replication on database instance...")
        check_log_writable(props)
        start_replication(props, build_dir)

    # start daemons with XID if specified
    print("\nStarting daemons...")
    if start_xid is not None:
        # Modify xid_mgr_daemon args to include starting XID
        modified_daemons = []
        for daemon in CORE_DAEMONS:
            if daemon[0] == 'xid_mgr_daemon':
                modified_daemons.append((daemon[0], daemon[1], f'-x,{start_xid}'))
            else:
                modified_daemons.append(daemon)
        start_daemons(build_dir, modified_daemons)
    else:
        start_daemons(build_dir, CORE_DAEMONS)

    # wait for running state
    print("\nWaiting for running state...")
    wait_for_running(props)

    # start the fdw daemons (e.g., ddl manager to import schemas)
    print("\nStarting FDW daemons...")
    start_fdw_daemons(props, build_dir, config_file)
    fixup_log_perms(props)

    print("\nSpringtail system started successfully.")


def status() -> None:
    """Function to check the status of the Springtail system."""
    print("Checking status...")
    (all_running, not_running) = check_daemons_running(ALL_DAEMONS_NAMES)

    if all_running:
        print("All daemons are running.")
    else:
        for name in not_running:
            print(f"Daemon {name} is not running.")

    if check_postgres_running():
        print("Postgres is running.")
    else:
        print("Postgres is not running.")


def stop(config_file: str, do_cleanup: bool = False) -> None:
    """Function to stop the Springtail system."""
    # get absolute path for config_file
    config_file = os.path.abspath(config_file)

    # Load the system properties from the system.json file
    props = Properties(config_file, False)

    # Stop the daemons
    print("\nStopping daemons...")
    stop_daemons(props.get_pid_path(), ALL_DAEMONS_NAMES)

    if do_cleanup:
        # Clear file system data
        print("\nClearing file system data...")
        cleanup_filesystem(props)

        # Cleanup db instance
        print("\nCleaning up database instance...")
        cleanup_db_instance(props)


def check_logs(config_file: str) -> List[str]:
    """Check the logs for errors."""
    # Load the system properties from the system.json file
    props = Properties(os.path.abspath(config_file), False)
    log_path = props.get_log_path()

    fixup_log_perms(props)

    # Check the logs for errors
    error_logs = check_backtrace(log_path)
    if error_logs:
        print(f"Found the following logs with errors: {error_logs}")
    else:
        print("No errors found in the log files.")

    # Extract the backtrace from the error logs
    for log in error_logs:
        print(f"Extracting backtrace from log: {log}")
        bt = extract_backtrace(log)
        print(f"\nBacktrace: {"".join(bt)}")

    return error_logs


def dump_logs(config_file: str, build_dir: str) -> None:
    """Dump the log files into a tarball."""
    # Load the system properties from the system.json file
    props = Properties(os.path.abspath(config_file), False)

    gen_dump_tarball(props, build_dir)


def generate_report(props : Properties, build_dir : str, title :str, description : str) -> str:
    """
    Generate a bug report programmatically.
    Returns issue url.
    """
    log_path = props.get_log_path()
    error_logs = check_backtrace(log_path)

    if error_logs:
        description += "\n\nErrors found in log files:\n"
        for log in error_logs:
            description += f"\n{log}\n"
            bt = extract_backtrace(log)
            description += f"\nBacktrace:\n{''.join(bt)}\n"

    # Generate a tarball of the log files
    tarball = gen_dump_tarball(props, build_dir)
    linear = Linear()
    linear.set_team('Springtail')
    issue = linear.create_issue_with_file(title, description, tarball)

    return issue['url']


def create_report(config_file: str, build_dir: str) -> None:
    """Create a new bug report."""
    props = Properties(os.path.abspath(config_file), False)

    linear = Linear()
    linear.set_team('Springtail')

    # Prompt for the bug title
    title = input("Enter the title of the bug: ")

    # Prompt for the bug description
    description = input("Enter a detailed description of the bug: ")

    # Prompt for the label
    label = input("Enter label: 'Bug', or 'Test Error Report'.  Hit enter for default 'Bug': ")
    if label == '':
        label = 'Bug'

    while label not in ['Bug', 'Test Error Report']:
        print("Invalid label, please enter 'Bug' or 'Test Error Report'.")
        label = input("Enter label: 'Bug', or 'Test Error Report'.  Hit enter for default 'Bug': ")
        if label == '':
            label = 'Bug'

    # Prompt for whether to upload log files
    upload_logs = input("Do you want to extract and upload log files? (y/n): ").strip().lower()

    # Validate input for upload logs
    while upload_logs not in ['y', 'n', 'yes', 'no']:
        upload_logs = input("Do you want to extract and upload log files? (y/n): ").strip().lower()

    tarball = None
    if upload_logs in ['y', 'yes']:
        # Generate a tarball of the log files
        print("Generating tarball of log files...")
        tarball = gen_dump_tarball(props, build_dir)

    # Create the bug report
    print("Creating bug report...")
    issue = None
    if tarball is not None:
        issue = linear.create_issue_with_file(title, description, tarball, label)
    else:
        issue = linear.create_issue_with_label(description, title, label)

    print(f"Bug report created: {issue['url']}")


def parse_arguments() -> argparse.Namespace:
    """Parse the command line arguments."""
    # Create the argument parser
    parser = argparse.ArgumentParser(description="Process command-line arguments for config file and build directory.")

    # Add arguments -f for config file and -b for build directory
    parser.add_argument('-f', '--config-file', type=str, required=True, help='Path to the configuration file')
    parser.add_argument('-b', '--build-dir', type=str, required=True, help='Path to the build directory')
    parser.add_argument('-s', '--sql-file', type=str, required=False, help='Path to a sql file to execute prior to startup')
    parser.add_argument('-x', '--start-xid', type=int, required=False, help='Start the system at a specific XID')
    parser.add_argument('--no-cleanup', action='store_true', help="Start without reinitializing the system")
    parser.add_argument('--start', action=argparse.BooleanOptionalAction, help="Start the Springtail system")
    parser.add_argument('--status', action=argparse.BooleanOptionalAction, help="Check the status of the Springtail daemons")
    parser.add_argument('--kill', action=argparse.BooleanOptionalAction, help="Kill the Springtail daemons")
    parser.add_argument('--debug', action=argparse.BooleanOptionalAction, help="Print debug information on error")
    parser.add_argument('--check', action=argparse.BooleanOptionalAction, help="Check for backtrace on error")
    parser.add_argument('--dump', action=argparse.BooleanOptionalAction, help="Dump the log files into a tarball")
    parser.add_argument('--report', action=argparse.BooleanOptionalAction, help="Create a new bug report")
    parser.add_argument('--load-redis', action=argparse.BooleanOptionalAction, help="Load redis from the system file")

    # Parse the arguments and return them
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    """Main function to run the Springtail system."""
    # Parse command line arguments
    args = parse_arguments()

    if not args.start and not args.status and not args.kill and not args.dump and not args.check and not args.report and not args.load_redis:
        print("No action specified. Use --start, --status, --dump, --check, --report, --load-redis or --kill.")
        sys.exit(1)

    if not is_linux():
        print("This script only supports running on Linux.")
        sys.exit(1)

    if args.debug:
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s.%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                            datefmt='%Y-%m-%d:%H:%M:%S',
                            handlers=logging.StreamHandler(sys.stdout))

    try:
        if args.status:
            status()
            sys.exit(0)

        if args.kill:
            stop(args.config_file, do_cleanup=not args.no_cleanup)
            sys.exit(0)

        if args.check:
            check_logs(args.config_file)
            sys.exit(0)

        if args.dump:
            dump_logs(args.config_file, args.build_dir)
            sys.exit(0)

        if args.report:
            create_report(args.config_file, args.build_dir)
            sys.exit(0)

        if args.load_redis:
            props = Properties(args.config_file, True)
            print("Redis loaded from the system settings file.")
            sys.exit(0)

        if args.start:
            start(args.config_file, args.build_dir, args.sql_file,
                  do_cleanup=not args.no_cleanup, do_init=not args.no_cleanup, start_xid=args.start_xid)

    except Exception as e:
        print(f"Caught error: {e}")
        if (args.debug):
            traceback.print_exc()
        sys.exit(1)
