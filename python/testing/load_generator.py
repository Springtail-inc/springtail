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
NUM_UPDATES = 5
NUM_DELETES = 2
BATCHED_INSERTS = False
OPERATIONS = 'TIAUD'
RUN_TS = int(time.time())
NUM_COLUMNS = '3-10'
NUM_INDEXES = '1-2'

# Global dictionary to store table column information
table_columns = {}
def print_table_columns_to_csv(file: str) -> None:
    max_columns = 0
    max_indexes = 0
    for table_name in table_columns:
        max_columns = max(max_columns, len(table_columns[table_name]['columns']))
        max_indexes = max(max_indexes, len(table_columns[table_name]['indexes']))

    with open(file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        header = ['table_name']
        header.extend(['column_{:03d}'.format(i) for i in range(1, max_columns+1)])
        header.extend(['index_{:03d}'.format(i) for i in range(1, max_indexes+1)])
        writer.writerow(header)

        for table_name in table_columns:
            columns = [table_name]
            columns.extend([column[0] for column in table_columns[table_name]['columns'][:max_columns]])
            columns.extend([''] * (max_columns - len(table_columns[table_name]['columns'])))
            columns.extend([column for column in table_columns[table_name]['indexes'][:max_indexes]])
            columns.extend([''] * (max_indexes - len(table_columns[table_name]['indexes'])))
            writer.writerow(columns)

def connect_db_instance(props: Properties, db_name: str = 'postgres') -> psycopg2.extensions.connection:
    db_instance_config = props.get_db_instance_config()
    db_host = db_instance_config['host']
    db_port = db_instance_config['port']
    db_user = db_instance_config['replication_user']
    db_password = db_instance_config['password']
    return connect_db(db_name, db_user, db_password, db_host, db_port, False)

def print_sys_props(props: Properties, config_file: str) -> None:
    db_config = props.get_db_configs()[0]
    print("\nSystem properties:")
    print(f"  Config file       : {config_file}")
    print(f"  Mount path        : {props.get_mount_path()}")
    print(f"  Pid path          : {props.get_pid_path()}")
    print(f"  DB instance ID    : {props.get_db_instance_id()}")
    print(f"  Primary DB name   : {db_config['name']}")
    print(f"  Primary DB ID     : {db_config['id']}")

def print_run_config_to_csv(file: str) -> None:
    with open(file, mode='w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Parameter", "Value"])
        writer.writerow(["Number of schemas", NUM_SCHEMAS])
        writer.writerow(["Number of tables per schema", NUM_TABLES_PER_SCHEMA])
        writer.writerow(["Number of inserts", NUM_INSERTS])
        writer.writerow(["Number of updates", NUM_UPDATES])
        writer.writerow(["Number of deletes", NUM_DELETES])
        writer.writerow(["Batched inserts", BATCHED_INSERTS])
        writer.writerow(["Operations", parse_operations(True)])
        writer.writerow(["Number of columns", NUM_COLUMNS])
        writer.writerow(["Number of indexes", NUM_INDEXES])

def write_metrics_to_csv(csv_file: str, _type: str, duration_ms: float, txid: int, pg_ts: str, rows: int, full_table_name: str = None) -> None:
    with open(csv_file, mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([_type, txid, duration_ms, pg_ts, rows, full_table_name])

def time_and_log_query(conn, _type: str, query: str, csv_file: str, params=None, full_table_name: str = None) -> int:
    start = time.time()

    with conn.cursor() as cur:
        cur.execute("BEGIN;")

        if params:
            cur.executemany(query, params)
        else:
            cur.execute(query)
        # Get the number of rows affected
        rows_affected = cur.rowcount

        cur.execute("SELECT txid_current(), FLOOR(EXTRACT(EPOCH FROM now()) * 1000)")
        txid, pg_ts_epoch_ms = cur.fetchone()

        cur.execute("COMMIT;")

    end = time.time()
    duration_ms = round((end - start) * 1000, 2)
    write_metrics_to_csv(csv_file, _type, duration_ms, txid, pg_ts_epoch_ms, rows_affected, full_table_name)
    return rows_affected

def create_table(conn, schema_name: str, table_name: str, csv_file: str):
    full_table_name = f"{schema_name}.{table_name}"
    # List of PostgreSQL data types to choose from
    # XXX Need to add other possible types too
    column_types = [
        "TEXT",
        "INT",
        "BIGINT",
        "FLOAT",
        "DOUBLE PRECISION",
        "BOOLEAN",
        "DATE",
        "TIME",
        "VARCHAR(255)",
        "CHAR(10)",
        "NUMERIC(10,2)"
    ]

    # Always include id as SERIAL PRIMARY KEY and created_at as TIMESTAMP
    columns = [
        "id SERIAL PRIMARY KEY",
        "created_at TIMESTAMP DEFAULT NOW()"
    ]

    # Add NUM_COLUMNS random columns with random types
    min_columns, max_columns = map(int, NUM_COLUMNS.split('-'))
    num_columns = random.randint(min_columns, max_columns)
    for i in range(num_columns):
        column_type = random.choice(column_types)
        col_type = column_type.replace(" ", "_").replace("(", "").replace(")", "").replace(",", "").lower().replace("__", "_")
        column_name = f"col_{i}_{col_type}"
        columns.append(f"{column_name} {column_type}")

    # Store column information in the global dictionary
    table_columns[full_table_name] = {
        "columns": [],
        "indexes": []
    }
    for col in columns:
        if col.startswith("id") or col.startswith("created_at"):
            continue

        col_name, col_type = col.split()[0], col.split()[1]
        # Handle special cases like VARCHAR(255)
        if '(' in col_type:
            col_type = col_type.split('(')[0]

        table_columns[full_table_name]["columns"].append((col_name, col_type.lower()))

    # Create the CREATE TABLE statement
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
            {',\n            '.join(columns)}
        )
    """

    time_and_log_query(conn, "create_table", create_table_sql, csv_file, full_table_name=full_table_name)
    print(f"[+] Created table: {full_table_name} with {num_columns} random columns")

def create_index(conn, schema_name: str, table_name: str, csv_file: str):
    full_table_name = f"{schema_name}.{table_name}"
    if full_table_name not in table_columns:
        print(f"[!] No column information found for {full_table_name}")
        return

    columns = table_columns[full_table_name]["columns"]
    if not columns:
        print(f"[!] No suitable columns found for indexing in {full_table_name}")
        return

    # Filter out columns that are not suitable for indexing
    indexable_columns = [col[0] for col in columns if col[1] not in ['jsonb', 'bytea', 'uuid']]

    if not indexable_columns:
        print(f"[!] No indexable columns found in {full_table_name}")
        return

    # Create a random number of indexes with a random number of columns
    min_indexes, max_indexes = map(int, NUM_INDEXES.split('-'))
    num_indexes = random.randint(min_indexes, max_indexes)
    for i in range(num_indexes):
        index_columns = random.sample(indexable_columns, random.randint(1, 2))
        index_name = f"idx_{table_name}_{i}_{('_'.join(index_columns))}"

        create_index_sql = f"""
            CREATE INDEX IF NOT EXISTS {index_name}
            ON {full_table_name} ({', '.join(index_columns)})
        """
        time_and_log_query(conn, "create_index", create_index_sql, csv_file, full_table_name=full_table_name)
        print(f"[+] Created index on: {full_table_name}({index_name})")

        table_columns[full_table_name]["indexes"].append(index_name)

def generate_values_list(columns: list, batch_size: int = 1) -> list:
    values_list = []

    for _ in range(batch_size):
        values = []
        for col_name, col_type in columns:
            if col_type in ['text', 'varchar', 'char']:
                values.append(f"value_{random.randint(1, 100)}")
            elif col_type in ['int', 'bigint']:
                values.append(random.randint(1, 1000))
            elif col_type in ['numeric', 'double', 'float']:
                values.append(random.random() * 1000)
            elif col_type == 'boolean':
                values.append(random.choice([True, False]))
            elif col_type == 'date':
                values.append(f"{random.randint(2000, 2025)}-0{random.randint(1, 9)}-0{random.randint(1, 9)}")
            elif col_type == 'time':
                values.append(f"{random.randint(0, 23)}:{random.randint(0, 59)}:{random.randint(0, 59)}")
            else:
                values.append(None)
        values_list.append(tuple(values))

    return values_list

def insert_data(conn, schema_name: str, table_name: str, csv_file: str):
    full_table_name = f"{schema_name}.{table_name}"
    if full_table_name not in table_columns:
        print(f"[!] No column information found for {full_table_name}")
        return

    columns = table_columns[full_table_name]["columns"]
    if not columns:
        print(f"[!] No columns found in {full_table_name}")
        return

    # Generate INSERT statement with all columns
    column_names = [col[0] for col in columns]
    placeholders = ['%s'] * len(column_names)

    insert_sql = f"""
        INSERT INTO {full_table_name} ({', '.join(column_names)})
        VALUES ({', '.join(placeholders)})
    """

    if BATCHED_INSERTS:
        remaining_inserts = NUM_INSERTS
        while remaining_inserts > 0:
            if remaining_inserts <= 10:  # For small remaining, just insert them all
                batch_size = remaining_inserts
            else:
                # Generate random batch size between 5% and 70% of remaining inserts
                batch_size = random.randint(max(1, int(remaining_inserts * 0.05)),
                                        min(remaining_inserts, int(remaining_inserts * 0.7)))

            values_list = generate_values_list(columns, batch_size)

            time_and_log_query(conn, "insert_data", insert_sql, csv_file, values_list, full_table_name=full_table_name)

            remaining_inserts -= batch_size
            print(f"[+] Inserted batch of {batch_size} rows into {full_table_name} (remaining: {remaining_inserts})")
    else:
        # Generate random batch sizes that add up to NUM_INSERTS
        values_list = generate_values_list(columns, NUM_INSERTS)
        time_and_log_query(conn, "insert_data", insert_sql, csv_file, values_list, full_table_name=full_table_name)

    print(f"[+] Total {NUM_INSERTS} rows inserted into {full_table_name}")

def update_data(conn, schema_name: str, table_name: str, csv_file: str):
    full_table_name = f"{schema_name}.{table_name}"
    if full_table_name not in table_columns:
        print(f"[!] No column information found for {full_table_name}")
        return

    columns = table_columns[full_table_name]["columns"]
    if not columns:
        print(f"[!] No columns found in {full_table_name}")
        return

    # Generate UPDATE statement with 1-3 random columns
    num_columns_to_update = random.randint(1, min(3, len(columns)))
    columns_to_update = random.sample(columns, num_columns_to_update)
    column_names = [col[0] for col in columns_to_update]
    placeholders = ['%s'] * len(column_names)
    order_by_column = random.choice(columns)[0]
    update_sql = f"""
        UPDATE {full_table_name}
        SET {', '.join(f'{col[0]} = %s' for col in columns_to_update)}
        WHERE id IN (SELECT id FROM {full_table_name} ORDER BY {order_by_column} LIMIT {NUM_UPDATES})
    """

    value_columns = columns_to_update
    values_list = generate_values_list(value_columns)
    out = time_and_log_query(conn, "update_data", update_sql, csv_file, values_list, full_table_name=full_table_name)

    print(f"[+] Updated {out} rows in {full_table_name}")

def delete_data(conn, schema_name: str, table_name: str, csv_file: str):
    full_table_name = f"{schema_name}.{table_name}"
    if full_table_name not in table_columns:
        print(f"[!] No column information found for {full_table_name}")
        return

    columns = table_columns[full_table_name]["columns"]
    if not columns:
        print(f"[!] No columns found in {full_table_name}")
        return

    delete_column = random.choice(columns)

    values_list = generate_values_list([delete_column], 1)
    order_by_column = random.choice(columns)[0]

    delete_sql = f"""
        DELETE FROM {full_table_name}
        WHERE id IN (SELECT id FROM {full_table_name} ORDER BY {order_by_column} LIMIT {NUM_DELETES})
    """

    out = time_and_log_query(conn, "delete_data", delete_sql, csv_file, values_list, full_table_name=full_table_name)

    print(f"[+] Deleted {out} rows from {full_table_name}")

def create_schema_and_tables(conn, csv_file: str, schema_name: str):
    time_and_log_query(conn, "create_schema", f"CREATE SCHEMA IF NOT EXISTS {schema_name}", csv_file, full_table_name=schema_name)
    print(f"[+] Created schema: {schema_name}")

    for i in range(NUM_TABLES_PER_SCHEMA):
        table_name = f"table_{i}_{random.randint(1000, 9999)}"

        for func in parse_operations():
            try:
                func(conn, schema_name, table_name, csv_file)
            except Exception as e:
                print(f"[-] Got an error while {func.__name__} {schema_name}.{table_name}: {e}")

    return schema_name

def parse_operations(parse_names: bool = False):
    operations = []
    for char in OPERATIONS:
        if char == 'T':
            operations.append(create_table)
        elif char == 'I':
            operations.append(create_index)
        elif char == 'A':
            operations.append(insert_data)
        elif char == 'U':
            operations.append(update_data)
        elif char == 'D':
            operations.append(delete_data)
    if parse_names:
        return ",".join([func.__name__ for func in operations])
    return operations

def load_data(config_file: str, csv_file: str) -> None:
    config_file = os.path.abspath(config_file)
    props = Properties(config_file)
    print_sys_props(props, config_file)

    with open(csv_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["query_type", "pg_xid", "duration_ms", "pg_timestamp", "rows_affected", "table_name"])

    conn = connect_db_instance(props, "springtail")
    start_time = time.time()

    # Create NUM_SCHEMAS schemas
    for schema_idx in range(NUM_SCHEMAS):
        schema_name = f"test_schema_{schema_idx}_{RUN_TS}"
        create_schema_and_tables(conn, csv_file, schema_name)

    print_table_columns_to_csv('/tmp/table_columns.csv')
    print_run_config_to_csv('/tmp/run_config.csv')
    conn.close()

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run ingestion metrics logger")
    parser.add_argument('-f', '--config-file', type=str, required=True, help='Path to the configuration file')
    parser.add_argument('-o', '--output-file', type=str, required=True, help='Path to the CSV output file')

    # Add arguments for custom parameters
    parser.add_argument('--num-schemas', type=int, default=NUM_SCHEMAS, help='Number of schemas to create')
    parser.add_argument('--num-tables-per-schema', type=int, default=NUM_TABLES_PER_SCHEMA, help='Number of tables per schema')
    parser.add_argument('--num-inserts', type=int, default=NUM_INSERTS, help='Number of inserts per table')
    parser.add_argument('--num-updates', type=int, default=NUM_UPDATES, help='Number of updates per table')
    parser.add_argument('--num-deletes', type=int, default=NUM_DELETES, help='Number of deletes per table')

    parser.add_argument('--num-columns', type=str, default=NUM_COLUMNS, help='Number of columns per table')
    parser.add_argument('--num-indexes', type=str, default=NUM_INDEXES, help='Number of indexes per table')

    parser.add_argument('--operations', type=str, default=OPERATIONS, help='Use batched inserts')
    parser.add_argument('--batched_inserts', type=bool, default=BATCHED_INSERTS, help='Use batched inserts')
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()

    # Update global variables with parsed arguments
    NUM_SCHEMAS = args.num_schemas
    NUM_TABLES_PER_SCHEMA = args.num_tables_per_schema

    # DML counts
    NUM_INSERTS = args.num_inserts
    if NUM_INSERTS <= 0:
        print(f"[-] NUM_INSERTS should be greater than 0")
        sys.exit(1)
    NUM_UPDATES = min(args.num_updates, NUM_INSERTS // 2)
    NUM_DELETES = min(args.num_deletes, NUM_INSERTS // 2)
    OPERATIONS = args.operations
    BATCHED_INSERTS = args.batched_inserts

    # Table values
    NUM_COLUMNS = args.num_columns
    NUM_INDEXES = args.num_indexes

    parsed_operations = parse_operations()
    # If no updates or deletes are specified, set them to 0
    if ( 'U' not in parsed_operations ):
        NUM_UPDATES = 0
    if ( 'D' not in parsed_operations ):
        NUM_DELETES = 0

    try:
        load_data(args.config_file, args.output_file)
    except Exception as e:
        print(f"Caught error: {e}")
        sys.exit(1)
