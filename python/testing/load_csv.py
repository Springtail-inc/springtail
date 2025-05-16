import argparse
import botocore
import glob
import logging
import os
import psycopg2
from psycopg2.extensions import quote_ident
import sys
import shutil
import traceback
import tempfile
import time
from typing import Dict, List, Optional

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the /shared directory to the Python path
sys.path.append(os.path.join(project_root, 'shared'))
sys.path.append(os.path.join(project_root, 'grpc'))

from aws import AwsHelper

from properties import Properties

from common import (
    connect_db,
    makedir,
    is_linux
)

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

def parse_arguments() -> argparse.Namespace:
    """Parse the command line arguments."""
    # Create the argument parser
    parser = argparse.ArgumentParser(description="Process command-line arguments.")

    # Add arguments -f for config file and -b for build directory
    parser.add_argument('-f', '--config-file', type=str, required=True, help='Path to the configuration file')
    parser.add_argument('-c', '--csv-file', type=str, required=True, help='Path to a .csv file')
    parser.add_argument('-t', '--table-name', type=str, required=True, help='Table name to load to')

    # Parse the arguments and return them
    args = parser.parse_args()
    return args

def _load_csv(cursor: psycopg2.extensions.cursor, filename: str, table: str) -> None:
    with open(filename, 'r') as f:
        f.readline() # skip the header
        cursor.copy_from(f, table, sep=',', null='')

if __name__ == "__main__":
    """Main function to run the Springtail system."""
    # Parse command line arguments
    args = parse_arguments()

    if not is_linux():
        print("This script only supports running on Linux.")
        sys.exit(1)

    try:
        # get absolute path for config_file
        config_file = os.path.abspath(args.config_file)

        # Load the system properties from the system.json file
        # also does a load redis from the system file if do_cleanup is True
        props = Properties(config_file, False)

        # Print the system properties
        print_sys_props(props, config_file)

        # Check the configuration
        check_config(props)

        # sync the benchmark data files from S3
        helper = AwsHelper(config=botocore.config.Config(signature_version=botocore.UNSIGNED),
                           region="us-east-1")
        helper.sync_s3_data('bench_data', s3_path='bench_files')

        # Connect to the primary database
        db_name = props.get_db_configs()[0]['name']
        print(f"Connecting database: {db_name}...")

        conn = connect_db_instance(props, db_name)

        print(f"Loading {args.csv_file} into {args.table_name}...")
        with conn.cursor() as cursor:
            # Insert sentinel value to track replication
            _load_csv(cursor, args.csv_file, args.table_name)
        conn.commit()

    except Exception as e:
        print(f"Caught error: {e}")
        traceback.print_exc()
        sys.exit(1)

