import os
import sys
import random
import argparse
import traceback
import time
import psycopg2

# Get the parent directory of the current script (i.e., the project root directory)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the /shared directory to the Python path
sys.path.append(os.path.join(project_root, 'shared'))

# Now import the Properties class
from properties import Properties

from common import (
    connect_db,
    execute_sql
)

# Load simulation parameters
NUM_SCHEMAS = 10
NUM_TABLES_PER_SCHEMA = 25
NUM_INSERTS = 5000
RUN_TIME_SECONDS = 60

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

def create_schema_and_tables(conn):
    schema_name = f"test_schema_{random.randint(1000, 9999)}"
    execute_sql(conn, f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    print(f"[+] Created schema: {schema_name}")

    for i in range(NUM_TABLES_PER_SCHEMA):
        table_name = f"table_{random.randint(1000, 9999)}"
        execute_sql(conn, f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                id SERIAL PRIMARY KEY,
                name TEXT,
                value INT,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        print(f"[+] Created table: {schema_name}.{table_name}")

        # Create an index
        execute_sql(conn, f"CREATE INDEX IF NOT EXISTS idx_{table_name}_value ON {schema_name}.{table_name} (value)")
        print(f"[+] Created index on: {schema_name}.{table_name}(value)")
        insert_data(conn, schema_name, table_name)
    return schema_name


def insert_data(conn, schema_name, table_name):
    for _ in range(NUM_INSERTS):
        execute_sql(conn, f"""
            INSERT INTO {schema_name}.{table_name} (name, value)
            VALUES (%s, %s)
        """, (f"name_{random.randint(1, 100)}", random.randint(1, 1000)))
    print(f"[+] Inserted {NUM_INSERTS} rows into {schema_name}.{table_name}")


def drop_schema(conn, schema_name):
    execute_sql(conn, f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")
    print(f"[-] Dropped schema: {schema_name}")

def timed_query(config_file: str) -> None:
    config_file = os.path.abspath(config_file)

    props = Properties(config_file, True)

    # Print the system properties
    print_sys_props(props, config_file)

    conn = connect_db_instance(props, "springtail")

    start_time = time.time()
    execute_sql(conn, f"""INSERT INTO table_with_default_types (col1, col2, col3, col4, col5, col6, col7, col8, col9)
SELECT (RANDOM() * 1000)::INT, 'Col-' || i, CURRENT_DATE, (RANDOM() * 1000), TRUE, 'Col-' || i, CURRENT_TIMESTAMP, 'A', (RANDOM() * 1000)::FLOAT
FROM generate_series(1, 10000) AS i;""")
    end_time = time.time()
    print(f"[+] Inserted 10000 rows into table_with_default_types in {(end_time - start_time)*1000:.2f} ms")

def load_data(config_file: str) -> None:
    config_file = os.path.abspath(config_file)

    props = Properties(config_file, True)

    # Print the system properties
    print_sys_props(props, config_file)

    conn = connect_db_instance(props, "springtail")

    start_time = time.time()
    created_schemas = []

    while time.time() - start_time < RUN_TIME_SECONDS:
        schema = create_schema_and_tables(conn)

    conn.close()

def parse_arguments() -> argparse.Namespace:
    """Parse the command line arguments."""
    # Create the argument parser
    parser = argparse.ArgumentParser(description="Process command-line arguments for config file and build directory.")

    # Add arguments -f for config file and -b for build directory
    parser.add_argument('-f', '--config-file', type=str, required=True, help='Path to the configuration file')

    # Parse the arguments and return them
    args = parser.parse_args()
    return args

if __name__ == "__main__":
    """Main function to run the Springtail system."""
    # Parse command line arguments
    args = parse_arguments()

    try:
        load_data(args.config_file)
    except Exception as e:
        print(f"Caught error: {e}")
        sys.exit(1)
