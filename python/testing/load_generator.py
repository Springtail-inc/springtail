import os
import sys
import random
import argparse
import traceback
import time
import csv
import psycopg2
from datetime import datetime

# Get the parent directory of the current script (i.e., the project root directory)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the /shared directory to the Python path
sys.path.append(os.path.join(project_root, 'shared'))

# Now import the Properties class
from properties import Properties

from common import (
    connect_db,
)

# Load simulation parameters
NUM_SCHEMAS = 10
NUM_TABLES_PER_SCHEMA = 25
NUM_INSERTS = 5
RUN_TIME_SECONDS = 5

def connect_db_instance(props: Properties, db_name: str = 'postgres') -> psycopg2.extensions.connection:
    db_instance_config = props.get_db_instance_config()
    db_host = db_instance_config['host']
    db_port = db_instance_config['port']
    db_user = db_instance_config['replication_user']
    db_password = db_instance_config['password']
    return connect_db(db_name, db_user, db_password, db_host, db_port)

def print_sys_props(props: Properties, config_file: str) -> None:
    db_config = props.get_db_configs()[0]
    print("\nSystem properties:")
    print(f"  Config file    : {config_file}")
    print(f"  Mount path     : {props.get_mount_path()}")
    print(f"  Pid path       : {props.get_pid_path()}")
    print(f"  DB instance ID : {props.get_db_instance_id()}")
    print(f"  Primary DB name: {db_config['name']}")
    print(f"  Primary DB ID  : {db_config['id']}")

def write_metrics_to_csv(csv_file: str, duration_ms: float, txid: int, pg_ts: str) -> None:
    with open(csv_file, mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([txid, duration_ms, pg_ts])

def time_and_log_query(conn, query: str, csv_file: str, params=None) -> None:
    start = time.time()
    with conn.cursor() as cur:
        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)
    conn.commit()
    end = time.time()
    duration_ms = round((end - start) * 1000, 2)
    with conn.cursor() as cur:
        cur.execute("SELECT txid_current(), FLOOR(EXTRACT(EPOCH FROM now()) * 1000) AS pg_ts_epoch_ms")
        txid, pg_ts = cur.fetchone()
    write_metrics_to_csv(csv_file, duration_ms, txid, pg_ts)

def create_schema_and_tables(conn, csv_file: str):
    schema_name = f"test_schema_{random.randint(1000, 9999)}"
    time_and_log_query(conn, f"CREATE SCHEMA IF NOT EXISTS {schema_name}", csv_file)
    print(f"[+] Created schema: {schema_name}")

    for i in range(NUM_TABLES_PER_SCHEMA):
        table_name = f"table_{random.randint(1000, 9999)}"
        time_and_log_query(conn, f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                id SERIAL PRIMARY KEY,
                name TEXT,
                value INT,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """, csv_file)
        print(f"[+] Created table: {schema_name}.{table_name}")

        time_and_log_query(conn, f"CREATE INDEX IF NOT EXISTS idx_{table_name}_value ON {schema_name}.{table_name} (value)", csv_file)
        print(f"[+] Created index on: {schema_name}.{table_name}(value)")

        insert_data(conn, schema_name, table_name, csv_file)

    return schema_name

def insert_data(conn, schema_name: str, table_name: str, csv_file: str):
    for _ in range(NUM_INSERTS):
        name = f"name_{random.randint(1, 100)}"
        value = random.randint(1, 1000)
        time_and_log_query(conn, f"""
            INSERT INTO {schema_name}.{table_name} (name, value)
            VALUES (%s, %s)
        """, csv_file, (name, value))
    print(f"[+] Inserted {NUM_INSERTS} rows into {schema_name}.{table_name}")

def drop_schema(conn, schema_name):
    time_and_log_query(conn, f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")
    print(f"[-] Dropped schema: {schema_name}")

def load_data(config_file: str, csv_file: str) -> None:
    config_file = os.path.abspath(config_file)
    props = Properties(config_file, True)
    print_sys_props(props, config_file)

    with open(csv_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["txid", "duration_ms", "pg_timestamp"])

    conn = connect_db_instance(props, "springtail")
    start_time = time.time()

    while time.time() - start_time < RUN_TIME_SECONDS:
        schema = create_schema_and_tables(conn, csv_file)

    conn.close()

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run ingestion metrics logger")
    parser.add_argument('-f', '--config-file', type=str, required=True, help='Path to the configuration file')
    parser.add_argument('-o', '--output-file', type=str, required=True, help='Path to the CSV output file')

    # Add arguments for custom parameters
    parser.add_argument('--num-schemas', type=int, default=NUM_SCHEMAS, help='Number of schemas to create')
    parser.add_argument('--num-tables-per-schema', type=int, default=NUM_TABLES_PER_SCHEMA, help='Number of tables per schema')
    parser.add_argument('--num-inserts', type=int, default=NUM_INSERTS, help='Number of inserts per table')
    parser.add_argument('--run-time-seconds', type=int, default=RUN_TIME_SECONDS, help='Duration to run the data load (seconds)')

    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()

    # Update global variables with parsed arguments
    NUM_SCHEMAS = args.num_schemas
    NUM_TABLES_PER_SCHEMA = args.num_tables_per_schema
    NUM_INSERTS = args.num_inserts
    RUN_TIME_SECONDS = args.run_time_seconds

    try:
        load_data(args.config_file, args.output_file)
    except Exception as e:
        print(f"Caught error: {e}")
        sys.exit(1)
