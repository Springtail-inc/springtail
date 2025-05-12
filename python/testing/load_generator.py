"""
Springtail Load Generator
=========================

This module generates database load by creating schemas, tables, indexes, and performing
various database operations (inserts, updates, deletes) according to configurable parameters.

Usage:
------
python load_generator.py -f config.yaml -c load_config.yaml
"""

import os
import sys
import random
import argparse
import traceback
import time
import csv
import yaml
import psycopg2
import json
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
RUN_TS = int(time.time())

# Global dictionary to store table column information
table_columns = {}

# Global dictionary to store the run configuration
run_config = {}

def print_table_columns_to_csv(file: str) -> None:
    """
    Write table column information to a CSV file.

    Args:
        file (str): Path to the output CSV file

    The CSV file will contain:
        - Table name
        - Column names (up to max_columns)
        - Index names (up to max_indexes)

    Notes:
        - Empty cells are filled with empty strings
        - Column names are padded to max_columns
        - Index names are padded to max_indexes
    """
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
    """
    Connect to a PostgreSQL database instance.

    Args:
        props (Properties): Properties object containing database configuration
        db_name (str): Name of the database to connect to (default: 'postgres')

    Returns:
        psycopg2.extensions.connection: Database connection object

    Raises:
        psycopg2.Error: If connection fails

    Notes:
        - Uses replication user credentials from properties
        - Connection is not autocommit
    """
    db_instance_config = props.get_db_instance_config()
    db_host = db_instance_config['host']
    db_port = db_instance_config['port']
    db_user = db_instance_config['replication_user']
    db_password = db_instance_config['password']
    return connect_db(db_name, db_user, db_password, db_host, db_port, False)

def print_sys_props(props: Properties, config_file: str) -> None:
    """
    Print system properties to the console.

    Args:
        props (Properties): Properties object containing system configuration
        config_file (str): Path to the configuration file

    Prints:
        - Config file path
        - Mount path
        - Pid path
        - DB instance ID
        - Primary DB name and ID

    Notes:
        - Uses first database configuration from props
    """
    db_config = props.get_db_configs()[0]
    print("\nSystem properties:")
    print(f"  Config file       : {config_file}")
    print(f"  Mount path        : {props.get_mount_path()}")
    print(f"  Pid path          : {props.get_pid_path()}")
    print(f"  DB instance ID    : {props.get_db_instance_id()}")
    print(f"  Primary DB name   : {db_config['name']}")
    print(f"  Primary DB ID     : {db_config['id']}")

def print_run_config_to_csv(file: str) -> None:
    """
    Write run configuration parameters to a CSV file.

    Args:
        file (str): Path to the output CSV file

    The CSV file will contain:
        - Number of schemas
        - Number of tables per schema
        - Number of inserts/updates/deletes
        - Batched inserts setting
        - Operations
        - Column/index configuration

    Notes:
        - Overwrites existing file
        - Uses global configuration variables
    """
    with open(file, mode='w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Parameter", "Value"])
        writer.writerow(["Number of schemas", run_config['num_schemas']])
        writer.writerow(["Number of tables per schema", run_config['num_tables']])
        writer.writerow(["Number of inserts", run_config['num_inserts']])
        writer.writerow(["Number of updates", run_config['num_updates']])
        writer.writerow(["Number of deletes", run_config['num_deletes']])
        writer.writerow(["Batched inserts", run_config['batched_inserts']])
        writer.writerow(["Operations", parse_operations(run_config['operations'], True)])
        writer.writerow(["Number of columns", run_config['num_columns']])
        writer.writerow(["Number of indexes", run_config['num_indexes']])
        writer.writerow(["Running with existing table set", run_config['use_existing_config']])

def write_sql_to_txt(sql_file: str, sql_str: str):
    """
    Write DDL SQL to a text file.

    Args:
        sql_file (str): Path to the output SQL file
        sql_str (str): SQL string to write
    """
    with open(sql_file, mode='a', newline='') as file:
        file.write(sql_str)

def write_metrics_to_csv(_type: str, duration_ms: float, txid: int, pg_ts: str, rows: int, full_table_name: str = None) -> None:
    """
    Write query metrics to a CSV file.

    Args:
        _type (str): Type of operation (e.g., 'create_table', 'insert_data')
        duration_ms (float): Duration of the operation in milliseconds
        txid (int): PostgreSQL transaction ID
        pg_ts (str): PostgreSQL timestamp
        rows (int): Number of rows affected
        full_table_name (str, optional): Full table name in format schema.table

    Notes:
        - Appends to existing file
        - Creates file if it doesn't exist
    """
    with open(run_config['output_file'], mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([_type, txid, duration_ms, pg_ts, rows, full_table_name])

def time_and_log_query(conn, _type: str, query: str, params=None, full_table_name: str = None) -> int:
    """
    Execute a query with timing and logging.

    Args:
        conn: Database connection object
        _type (str): Type of operation (e.g., 'create_table', 'insert_data')
        query (str): SQL query to execute
        params (list, optional): Parameters for SQL query
        full_table_name (str, optional): Full table name in format schema.table

    Returns:
        int: Number of rows affected by the query

    Raises:
        psycopg2.Error: If query execution fails

    Notes:
        - Automatically wraps query in transaction
        - Logs metrics to CSV file
        - Returns affected row count
    """
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
    write_metrics_to_csv(_type, duration_ms, txid, pg_ts_epoch_ms, rows_affected, full_table_name)
    return rows_affected

def generate_random_table_data(full_table_name: str):
    """
    Generate random table data.

    Returns:
        list: List of column definitions
    """
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

    # Add NUM_COLUMNS random columns with random types
    min_columns, max_columns = map(int, run_config['num_columns'].split('-'))
    num_columns = random.randint(min_columns, max_columns)
    for i in range(num_columns):
        column_type = random.choice(column_types)
        col_type = column_type.replace(" ", "_").replace("(", "").replace(")", "").replace(",", "").lower().replace("__", "_")
        column_name = f"col_{i}_{col_type}"
        columns.append(f"{column_name} {column_type}")

    # Store column information in the global dictionary
    schema_name, table_name = full_table_name.split('.')
    if schema_name not in table_columns:
        table_columns[schema_name] = {}
    if table_name not in table_columns[schema_name]:
        table_columns[schema_name][table_name] = {
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

        table_columns[schema_name][table_name]["columns"].append((col_name, col_type.lower()))

    return columns

def create_table(conn, schema_name: str, table_name: str):
    """
    Create a new table with random columns and store column information.

    Args:
        conn: Database connection object
        schema_name (str): Name of the schema
        table_name (str): Name of the table

    The table will have:
        - id SERIAL PRIMARY KEY
        - created_at TIMESTAMP
        - Random number of columns with random types

    Notes:
        - Stores column information in global table_columns dict (or)
        - Retrieves the column information from the JSON file if it exists
        - Uses time_and_log_query for execution and metrics
    """
    full_table_name = f"{schema_name}.{table_name}"

    # Always include id as SERIAL PRIMARY KEY and created_at as TIMESTAMP
    columns = [
        "id SERIAL PRIMARY KEY",
        "created_at TIMESTAMP DEFAULT NOW()"
    ]
    if run_config['use_existing_config']:
        columns.extend([f"{col_name} {col_type}" for col_name, col_type in table_columns[schema_name][table_name]["columns"]])
    else:
        columns.extend(generate_random_table_data(full_table_name))

    # Create the CREATE TABLE statement
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
            {',\n            '.join(columns)}
        )
    """

    time_and_log_query(conn, "create_table", create_table_sql, full_table_name=full_table_name)
    write_sql_to_txt(run_config['sql_file'], create_table_sql)

    print(f"[+] Created table: {full_table_name} with {len(columns)} random columns")

def create_index(conn, schema_name: str, table_name: str):
    """
    Create random indexes on the table.

    Args:
        conn: Database connection object
        schema_name (str): Name of the schema
        table_name (str): Name of the table

    Creates:
        - Random number of indexes (1-2)
        - Each index with random number of columns (1-2)

    Notes:
        - Skips non-indexable columns (id, created_at, jsonb, bytea, uuid)
        - Updates global table_columns dict with index information
    """
    full_table_name = f"{schema_name}.{table_name}"

    columns = table_columns[schema_name][table_name]["columns"]

    # Filter out columns that are not suitable for indexing
    indexable_columns = [col_name for col_name, col_type in columns if col_name not in ['id', 'created_at'] and col_type not in ['jsonb', 'bytea', 'uuid']]

    if not run_config['use_existing_config']:
        # If the prev run flag isn't set, create new indexes
        if not indexable_columns:
            print(f"[!] No indexable columns found in {full_table_name}")
            return

        # Create a random number of indexes with a random number of columns
        min_indexes, max_indexes = map(int, run_config['num_indexes'].split('-'))
        num_indexes = random.randint(min_indexes, max_indexes)
        min_cols_per_index, max_cols_per_index = map(int, run_config['num_columns_per_index'].split('-'))
        for i in range(num_indexes):
            index_columns = random.sample(indexable_columns, random.randint(min_cols_per_index, max_cols_per_index))
            index_name = f"idx_{table_name}_{i}_{('_'.join(index_columns))}"
            table_columns[schema_name][table_name]["indexes"].append([index_name, index_columns])

    for index_name, index_columns in table_columns[schema_name][table_name]["indexes"]:
        create_index_sql = f"""
            CREATE INDEX IF NOT EXISTS {index_name}
            ON {full_table_name} ({', '.join(index_columns)})
        """
        time_and_log_query(conn, "create_index", create_index_sql, full_table_name=full_table_name)
        write_sql_to_txt(run_config['sql_file'], create_index_sql)

        print(f"[+] Created index on: {full_table_name}({index_name})")

def generate_values_list(columns: list, batch_size: int = 1) -> list:
    """
    Generate random values for table columns.

    Args:
        columns (list): List of (column_name, column_type) tuples
        batch_size (int): Number of value sets to generate

    Returns:
        list: List of tuples containing random values for each column

    Value generation rules:
        - TEXT/VARCHAR/CHAR: Random string "value_[1-100]"
        - INT/BIGINT: Random integer 1-1000
        - NUMERIC/DOUBLE/FLOAT: Random float 0-1000
        - BOOLEAN: Random boolean
        - DATE: Random date between 2000-2025
        - TIME: Random time
        - Other types: None
    """
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

def insert_data(conn, schema_name: str, table_name: str):
    """
    Insert data into a table.

    Args:
        conn: Database connection object
        schema_name (str): Name of the schema
        table_name (str): Name of the table

    Insert behavior:
        - If BATCHED_INSERTS is True:
            - Inserts data in random batch sizes
            - Batch size ranges from 5% to 70% of remaining inserts
            - Minimum batch size is 1, maximum is remaining inserts
        - Otherwise:
            - Inserts all data in a single batch

    Notes:
        - Uses generate_values_list for value generation
        - Uses time_and_log_query for execution and metrics
    """
    full_table_name = f"{schema_name}.{table_name}"
    if schema_name not in table_columns or table_name not in table_columns[schema_name]:
        print(f"[!] No column information found for {full_table_name}")
        return

    columns = table_columns[schema_name][table_name]["columns"]
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

    if run_config['batched_inserts']:
        remaining_inserts = run_config['num_inserts']
        while remaining_inserts > 0:
            if remaining_inserts <= 10:  # For small remaining, just insert them all
                batch_size = remaining_inserts
            else:
                # Generate random batch size between 5% and 70% of remaining inserts
                batch_size = random.randint(max(1, int(remaining_inserts * 0.05)),
                                        min(remaining_inserts, int(remaining_inserts * 0.7)))

            values_list = generate_values_list(columns, batch_size)

            time_and_log_query(conn, "insert_data", insert_sql, values_list, full_table_name=full_table_name)
            # write_sql_to_txt(run_config['sql_file'], insert_sql)

            remaining_inserts -= batch_size
            print(f"[+] Inserted batch of {batch_size} rows into {full_table_name} (remaining: {remaining_inserts})")
    else:
        # Generate random batch sizes that add up to NUM_INSERTS
        values_list = generate_values_list(columns, run_config['num_inserts'])
        time_and_log_query(conn, "insert_data", insert_sql, values_list, full_table_name=full_table_name)
        # write_sql_to_txt(run_config['sql_file'], insert_sql)

    print(f"[+] Total {run_config['num_inserts']} rows inserted into {full_table_name}")

def update_data(conn, schema_name: str, table_name: str):
    """
    Update random rows in the table.

    Args:
        conn: Database connection object
        schema_name (str): Name of the schema
        table_name (str): Name of the table

    Update behavior:
        - Randomly selects 1-3 columns to update
        - Updates run_config['num_updates'] rows
        - Uses ORDER BY with random column
        - Uses generate_values_list for new values

    Notes:
        - Uses time_and_log_query for execution and metrics
        - Skips if no columns found
    """
    full_table_name = f"{schema_name}.{table_name}"
    if schema_name not in table_columns or table_name not in table_columns[schema_name]:
        print(f"[!] No column information found for {full_table_name}")
        return

    columns = table_columns[schema_name][table_name]["columns"]
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
        WHERE id IN (SELECT id FROM {full_table_name} ORDER BY {order_by_column} LIMIT {run_config['num_updates']})
    """

    value_columns = columns_to_update
    values_list = generate_values_list(value_columns)
    out = time_and_log_query(conn, "update_data", update_sql, values_list, full_table_name=full_table_name)
    # write_sql_to_txt(run_config['sql_file'], update_sql)

    print(f"[+] Updated {out} rows in {full_table_name}")

def delete_data(conn, schema_name: str, table_name: str):
    """
    Delete random rows from the table.

    Args:
        conn: Database connection object
        schema_name (str): Name of the schema
        table_name (str): Name of the table

    Delete behavior:
        - Deletes run_config['num_deletes'] rows
        - Uses ORDER BY with random column

    Notes:
        - Uses time_and_log_query for execution and metrics
        - Skips if no columns found
    """
    full_table_name = f"{schema_name}.{table_name}"
    if schema_name not in table_columns or table_name not in table_columns[schema_name]:
        print(f"[!] No column information found for {full_table_name}")
        return

    columns = table_columns[schema_name][table_name]["columns"]
    if not columns:
        print(f"[!] No columns found in {full_table_name}")
        return

    delete_column = random.choice(columns)

    values_list = generate_values_list([delete_column], 1)
    order_by_column = random.choice(columns)[0]

    delete_sql = f"""
        DELETE FROM {full_table_name}
        WHERE id IN (SELECT id FROM {full_table_name} ORDER BY {order_by_column} LIMIT {run_config['num_deletes']})
    """

    out = time_and_log_query(conn, "delete_data", delete_sql, values_list, full_table_name=full_table_name)
    # write_sql_to_txt(run_config['sql_file'], delete_sql)

    print(f"[+] Deleted {out} rows from {full_table_name}")

def create_schema_and_tables(conn, schema_name: str):
    """
    Create a schema and its tables.

    Args:
        conn: Database connection object
        schema_name (str): Name of the schema

    Process:
        1. Creates the schema if it doesn't exist
        2. Creates run_config['num_tables'] tables
        3. For each table:
            - Applies operations from run_config['operations'] string
            - Handles errors gracefully

    Returns:
        str: Name of the created schema

    Notes:
        - Uses time_and_log_query for schema creation
        - Logs errors but continues with next table
    """
    # Create the main schema
    create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"
    time_and_log_query(conn, "create_schema", create_schema_sql, full_table_name=schema_name)
    write_sql_to_txt(run_config['sql_file'], create_schema_sql)

    print(f"[+] Created schema: {schema_name}")

    # Create the tables under the schema
    table_names = []
    if run_config['use_existing_config']:
        table_names = table_columns.get(schema_name)
    else:
        table_names = [f"table_{table_idx}_{random.randint(1000, 9999)}" for table_idx in range(run_config['num_tables'])]

    for table_name in table_names:
        for func in parse_operations(run_config['operations']):
            try:
                func(conn, schema_name, table_name)
            except Exception as e:
                print(f"[-] Got an error while {func.__name__} {schema_name}.{table_name}: {e}")

    return schema_name

def parse_operations(operations: str, parse_names: bool = False):
    """
    Parse operations string into function list.

    Args:
        operations (str): String of operations to parse
        parse_names (bool): If True, return comma-separated function names

    Operations mapping:
        'T' -> create_table
        'I' -> create_index
        'A' -> insert_data
        'U' -> update_data
        'D' -> delete_data

    Returns:
        list: List of operation functions or string of function names

    Notes:
        - Validates operation characters
        - Returns empty list if no valid operations found
    """
    ops = []
    for char in operations:
        if char == 'T':
            ops.append(create_table)
        elif char == 'I':
            ops.append(create_index)
        elif char == 'A':
            ops.append(insert_data)
        elif char == 'U':
            ops.append(update_data)
        elif char == 'D':
            ops.append(delete_data)
    if parse_names:
        return ",".join([func.__name__ for func in ops])
    return ops

def load_data(system_config_file: str, run_config: dict) -> None:
    """
    Load data into the database according to configuration.

    Args:
        system_config_file (str): Path to system configuration file
        run_config (dict): Run configuration

    Process:
        1. Parse configuration
        2. Create database connection
        3. Create run_config['num_schemas'] schemas
        4. For each schema:
            - Create run_config['num_tables'] tables
            - Perform operations based on run_config['operations'] string
        5. Generate metrics files

    Notes:
        - Creates metrics files in /tmp/
        - Closes database connection on completion
    """
    config_file = os.path.abspath(system_config_file)
    props = Properties(config_file)
    print_sys_props(props, config_file)

    with open(run_config['output_file'], mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["query_type", "pg_xid", "duration_ms", "pg_timestamp", "rows_affected", "table_name"])

    conn = connect_db_instance(props, "springtail")
    start_time = time.time()

    global table_columns
    if run_config['use_existing_config']:
        with open(run_config['table_columns_file'], 'r') as table_columns_file:
            table_columns = json.load(table_columns_file)

    # Create the schema and the tables
    schema_names = []
    if run_config['use_existing_config']:
        schema_names = table_columns.keys()
    else:
        schema_names = [f"test_schema_{schema_idx}_{RUN_TS}" for schema_idx in range(run_config['num_schemas'])]

    for schema_name in schema_names:
        create_schema_and_tables(conn, schema_name)

    # Dumping table columns into JSON file
    with open(run_config['table_columns_file'], 'w') as file:
        json.dump(table_columns, file, indent=4)

    # Dumping run config to CSV file
    print_run_config_to_csv(run_config['run_config_file'])
    conn.close()

def parse_arguments() -> argparse.Namespace:
    """
    Parse command line arguments.

    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(description="Run ingestion metrics logger")
    parser.add_argument('-f', '--system-config-file', type=str, required=True, help='Path to the system configuration file')
    parser.add_argument('-c', '--load-config-file', type=str, required=True, help='Path to the load configuration file')

    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()
    with open(args.load_config_file) as f:
        run_config = yaml.safe_load(f)

    try:
        load_data(args.system_config_file, run_config)
    except Exception as e:
        print(f"Caught error: {e}")
        sys.exit(1)
