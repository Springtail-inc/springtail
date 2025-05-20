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

class LoadGenerator:
    def __init__(self, run_config: dict):
        self.run_config = run_config
        self.table_columns = {}
        self.query_info_file = run_config['file_configuration']['output_files']['query_info']
        self.load_sql_file = run_config['file_configuration']['meta_files']['load_sql']
        self.table_columns_json = run_config['file_configuration']['meta_files']['table_columns']
        self.table_columns_csv = run_config['file_configuration']['meta_files']['table_columns_csv']
        self.run_config_csv = run_config['file_configuration']['meta_files']['run_config']

    def print_table_columns_to_csv(self) -> None:
        """
        Write table column information to a CSV file.

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
        for schema_name in self.table_columns:
            for table_name in self.table_columns[schema_name]:
                max_columns = max(max_columns, len(self.table_columns[schema_name][table_name]['columns']))
                max_indexes = max(max_indexes, len(self.table_columns[schema_name][table_name]['indexes']))

        with open(self.table_columns_csv, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            header = ['schema_name', 'table_name']
            header.extend(['column_{:03d}'.format(i) for i in range(1, max_columns+1)])
            header.extend(['index_{:03d}'.format(i) for i in range(1, max_indexes+1)])
            writer.writerow(header)

            for schema_name in self.table_columns:
                for table_name in self.table_columns[schema_name]:
                    columns = [schema_name, table_name]
                    columns.extend([column[0] for column in self.table_columns[schema_name][table_name]['columns'][:max_columns]])
                    columns.extend([''] * (max_columns - len(self.table_columns[schema_name][table_name]['columns'])))
                    columns.extend([column for column in self.table_columns[schema_name][table_name]['indexes'][:max_indexes]])
                    columns.extend([''] * (max_indexes - len(self.table_columns[schema_name][table_name]['indexes'])))
                    writer.writerow(columns)

    def connect_db_instance(self, db_name: str = 'postgres') -> psycopg2.extensions.connection:
        """
        Connect to a PostgreSQL database instance.

        Args:
            db_name (str): Name of the database to connect to (default: 'postgres')

        Returns:
            psycopg2.extensions.connection: Database connection object

        Raises:
            psycopg2.Error: If connection fails

        Notes:
            - Uses replication user credentials from properties
            - Connection is not autocommit
        """
        db_instance_config = self.props.get_db_instance_config()
        db_host = db_instance_config['host']
        db_port = db_instance_config['port']
        db_user = db_instance_config['replication_user']
        db_password = db_instance_config['password']
        return connect_db(db_name, db_user, db_password, db_host, db_port, False)

    def parse_operations(self, operations: list, parse_names: bool = False):
        """
        Parse operations string into function list.

        Args:
            operations (list): List of operations to parse
            parse_names (bool): If True, return comma-separated function names

        Returns:
            list: List of operation functions or string of function names

        Notes:
            - Validates operation characters
            - Returns empty list if no valid operations found
        """
        ops = []
        for op in operations:
            if op == 'create_table':
                ops.append(self.create_table)
            elif op == 'create_index':
                ops.append(self.create_index)
            elif op == 'insert_data':
                ops.append(self.insert_data)
            elif op == 'update_data':
                ops.append(self.update_data)
            elif op == 'delete_data':
                ops.append(self.delete_data)
        if parse_names:
            return ",".join([func.__name__ for func in ops])
        return ops

    def print_sys_props(self, config_file: str) -> None:
        """
        Print system properties to the console.

        Args:
            config_file (str): Path to the configuration file
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
        db_config = self.props.get_db_configs()[0]
        print("\nSystem properties:")
        print(f"  Config file       : {config_file}")
        print(f"  Mount path        : {self.props.get_mount_path()}")
        print(f"  Pid path          : {self.props.get_pid_path()}")
        print(f"  DB instance ID    : {self.props.get_db_instance_id()}")
        print(f"  Primary DB name   : {db_config['name']}")
        print(f"  Primary DB ID     : {db_config['id']}")

    def print_run_config_to_csv(self) -> None:
        """
        Write run configuration parameters to a CSV file.

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
        with open(self.run_config_csv, mode='w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["Parameter", "Value"])
            writer.writerow(["Number of schemas", self.run_config['num_schemas']])
            writer.writerow(["Number of tables per schema", self.run_config['table_configuration']['num_tables']])
            writer.writerow(["Number of inserts", self.run_config['load_configuration']['num_inserts']])
            writer.writerow(["Number of updates", self.run_config['load_configuration']['num_updates']])
            writer.writerow(["Number of deletes", self.run_config['load_configuration']['num_deletes']])
            writer.writerow(["Batched inserts", self.run_config['batched_inserts']])
            writer.writerow(["Operations", self.parse_operations(self.run_config['operations'], True)])
            writer.writerow(["Number of columns (min)", self.run_config['table_configuration']['min_columns']])
            writer.writerow(["Number of columns (max)", self.run_config['table_configuration']['max_columns']])
            writer.writerow(["Number of indexes (min)", self.run_config['index_configuration']['min_indexes']])
            writer.writerow(["Number of indexes (max)", self.run_config['index_configuration']['max_indexes']])
            writer.writerow(["Number of columns per index (min)", self.run_config['index_configuration']['min_columns_per_index']])
            writer.writerow(["Number of columns per index (max)", self.run_config['index_configuration']['max_columns_per_index']])
            writer.writerow(["Running with existing table set", self.run_config['use_existing_config']])

    def write_sql_to_txt(self, query: str, params: list = None):
        """
        Write SQL to a text file.

        Args:
            query (str): SQL string to write
            params (list, optional): Parameters for SQL query
        """
        # Write the SQL query to the file
        if params:
            # Format the query with None values represented as NULL
            formatted_query = query
            for param in params:
                if param is None:
                    formatted_query = formatted_query.replace('%s', 'NULL', 1)
                else:
                    formatted_query = formatted_query.replace('%s', str(param), 1)
            with open(self.load_sql_file, mode='a', newline='') as file:
                file.write(formatted_query + "\n")
        else:
            with open(self.load_sql_file, mode='a', newline='') as file:
                file.write(query + "\n")

    def write_metrics_to_csv(self, _type: str, duration_ms: float, txid: int, pg_ts: str, rows: int, full_table_name: str = None) -> None:
        """
        Write query metrics to a CSV file.

        Args:
            _type (str): Type of operation (e.g., 'create_table', 'insert_data', 'update_data', 'delete_data')
            duration_ms (float): Duration of the operation in milliseconds
            txid (int): PostgreSQL transaction ID
            pg_ts (str): PostgreSQL timestamp
            rows (int): Number of rows affected
            full_table_name (str, optional): Full table name in format schema.table

        Notes:
            - Appends to existing file
            - Creates file if it doesn't exist
        """
        with open(self.query_info_file, mode='a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([_type, txid, duration_ms, pg_ts, rows, full_table_name])

    def time_and_log_query(self, conn, _type: str, query: str, params=None, full_table_name: str = None) -> int:
        """
        Execute a query with timing and logging.

        Args:
            conn: Database connection object
            _type (str): Type of operation (e.g., 'create_table', 'insert_data', 'update_data', 'delete_data')
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

        # Write metrics to CSV
        self.write_metrics_to_csv(_type, duration_ms, txid, pg_ts_epoch_ms, rows_affected, full_table_name)

        # Write the SQL query to the file
        self.write_sql_to_txt(query, params)

        return rows_affected

    def generate_random_table_data(self, full_table_name: str):
        """
        Generate random table data.

        Returns:
            list: List of column definitions
        """
        columns = []  # Initialize the columns list

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
        table_configuration = self.run_config['table_configuration']
        min_columns, max_columns = table_configuration['min_columns'], table_configuration['max_columns']
        num_columns = random.randint(min_columns, max_columns)
        for i in range(num_columns):
            column_type = random.choice(column_types)
            col_type = column_type.replace(" ", "_").replace("(", "").replace(")", "").replace(",", "").lower().replace("__", "_")
            column_name = f"col_{i}_{col_type}"
            columns.append(f"{column_name} {column_type}")

        # Store column information in the global dictionary
        schema_name, table_name = full_table_name.split('.')
        if schema_name not in self.table_columns:
            self.table_columns[schema_name] = {}
        if table_name not in self.table_columns[schema_name]:
            self.table_columns[schema_name][table_name] = {
                "columns": [],
                "indexes": []
            }

        for col in columns:
            if col.startswith("id") or col.startswith("created_at") or col.startswith("updated_at"):
                continue

            # Extract the column data and store it in the table_columns array
            col_data = col.split()
            col_name = col_data[0]
            col_type = ' '.join(col_data[1:])

            self.table_columns[schema_name][table_name]["columns"].append((col_name, col_type))

        return columns

    def create_table(self, conn, schema_name: str, table_name: str):
        """
        Create a new table with random columns and store column information.

        Args:
            conn: Database connection object
            schema_name (str): Name of the schema
            table_name (str): Name of the table

        The table will have:
            - id SERIAL PRIMARY KEY
            - created_at TIMESTAMP
            - updated_at TIMESTAMP
            - Random number of columns with random types

        Notes:
            - Stores column information in global table_columns dict (or)
            - Retrieves the column information from the JSON file if it exists
            - Uses time_and_log_query for execution and metrics
        """
        full_table_name = f"{schema_name}.{table_name}"

        # Always include id as SERIAL PRIMARY KEY and created_at as TIMESTAMP and updated_at as TIMESTAMP
        columns = [
            "id SERIAL PRIMARY KEY",
            "created_at TIMESTAMP DEFAULT NOW()",
            "updated_at TIMESTAMP DEFAULT NOW()"
        ]
        if self.run_config['use_existing_config']:
            columns.extend([f"{col_name} {col_type}" for col_name, col_type in self.table_columns[schema_name][table_name]["columns"]])
        else:
            columns.extend(self.generate_random_table_data(full_table_name))

        # Create the CREATE TABLE statement
        create_table_sql = f"""CREATE TABLE IF NOT EXISTS {full_table_name} ({', '.join(columns)});"""

        self.time_and_log_query(conn, "create_table", create_table_sql, full_table_name=full_table_name)

        print(f"[+] Created table: {full_table_name} with {len(columns)} random columns")

    def create_index(self, conn, schema_name: str, table_name: str):
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
            - Skips non-indexable columns (id, created_at, updated_at, jsonb, bytea, uuid)
            - Updates global table_columns dict with index information
        """
        full_table_name = f"{schema_name}.{table_name}"

        columns = self.table_columns[schema_name][table_name]["columns"]

        # Filter out columns that are not suitable for indexing
        indexable_columns = [col_name for col_name, col_type in columns if col_name not in ['id', 'created_at', 'updated_at'] and col_type not in ['jsonb', 'bytea', 'uuid']]

        if not self.run_config['use_existing_config']:
            # If the prev run flag isn't set, create new indexes
            if not indexable_columns:
                print(f"[!] No indexable columns found in {full_table_name}")
                return

            indexes_config = self.run_config['index_configuration']
            # Create a random number of indexes with a random number of columns
            min_indexes, max_indexes = indexes_config['min_indexes'], indexes_config['max_indexes']
            num_indexes = random.randint(min_indexes, max_indexes)
            min_cols_per_index, max_cols_per_index = indexes_config['min_columns_per_index'], indexes_config['max_columns_per_index']
            for i in range(num_indexes):
                index_columns = random.sample(indexable_columns, random.randint(min_cols_per_index, max_cols_per_index))
                index_name = f"idx_{table_name}_{i}_{('_'.join(index_columns))}"
                self.table_columns[schema_name][table_name]["indexes"].append([index_name, index_columns])

        for index_name, index_columns in self.table_columns[schema_name][table_name]["indexes"]:
            create_index_sql = f"""CREATE INDEX IF NOT EXISTS {index_name} ON {full_table_name} ({', '.join(index_columns)});"""
            self.time_and_log_query(conn, "create_index", create_index_sql, full_table_name=full_table_name)

            print(f"[+] Created index on: {full_table_name}({index_name})")

    def generate_values_list(self, columns: list, batch_size: int = 1) -> list:
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

        allow_nulls = self.run_config['load_configuration']['allow_nulls']
        null_row_indices = []
        if allow_nulls.get("rows", False):
            # Randomly select a percentage of rows to be null
            null_rows = random.randint(int(batch_size * 0.1), int(batch_size * 0.15))
            # Generate NULL rows first
            null_row_indices = random.sample(range(batch_size), min(null_rows, batch_size))

        for i in range(batch_size):
            values = []

            if i in null_row_indices:
                values_list.append(tuple([None] * len(columns)))
                continue

            null_col_indices = []
            if allow_nulls.get("cols", False):
                # Out of all the columns, select the indices of 10-15% of columns for setting the value as null
                num_null_cols = random.randint(int(len(columns) * 0.1), int(len(columns) * 0.15))
                # Get indices of columns to be NULL
                null_col_indices = random.sample(range(len(columns)), num_null_cols)

            for col_idx, (col_name, col_type) in enumerate(columns):
                if col_idx in null_col_indices:
                    values.append(None)
                elif col_type in ['TEXT', 'VARCHAR(255)', 'CHAR(10)']:
                    values.append(f"value_{random.randint(1, 100)}")
                elif col_type in ['INT', 'BIGINT']:
                    values.append(random.randint(1, 1000))
                elif col_type in ['NUMERIC(10,2)', 'DOUBLE PRECISION', 'FLOAT']:
                    values.append(random.random() * 1000)
                elif col_type == 'BOOLEAN':
                    values.append(random.choice([True, False]))
                elif col_type == 'DATE':
                    values.append(f"{random.randint(2000, 2025)}-0{random.randint(1, 9)}-0{random.randint(1, 9)}")
                elif col_type == 'TIME':
                    values.append(f"{random.randint(0, 23)}:{random.randint(0, 59)}:{random.randint(0, 59)}")
                else:
                    values.append(None)
            values_list.append(tuple(values))

        return values_list

    def insert_data(self, conn, schema_name: str, table_name: str):
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
        if schema_name not in self.table_columns or table_name not in self.table_columns[schema_name]:
            print(f"[!] No column information found for {full_table_name}")
            return

        columns = self.table_columns[schema_name][table_name]["columns"]
        if not columns:
            print(f"[!] No columns found in {full_table_name}")
            return

        # Generate INSERT statement with all columns
        column_names = [col[0] for col in columns]
        placeholders = ['%s'] * len(column_names)

        insert_sql = f"""INSERT INTO {full_table_name} ({', '.join(column_names)})
        VALUES ({', '.join(placeholders)});"""

        if self.run_config['batched_inserts']:
            remaining_inserts = self.run_config['load_configuration']['num_inserts']
            while remaining_inserts > 0:
                if remaining_inserts <= 10:  # For small remaining, just insert them all
                    batch_size = remaining_inserts
                else:
                    # Generate random batch size between 5% and 70% of remaining inserts
                    batch_size = random.randint(max(1, int(remaining_inserts * 0.05)),
                                            min(remaining_inserts, int(remaining_inserts * 0.7)))

                values_list = self.generate_values_list(columns, batch_size)

                self.time_and_log_query(conn, "insert_data", insert_sql, values_list, full_table_name=full_table_name)

                remaining_inserts -= batch_size
                print(f"[+] Inserted batch of {batch_size} rows into {full_table_name} (remaining: {remaining_inserts})")
        else:
            # Generate random batch sizes that add up to run_config['load_configuration']['num_inserts']
            values_list = self.generate_values_list(columns, self.run_config['load_configuration']['num_inserts'])
            self.time_and_log_query(conn, "insert_data", insert_sql, values_list, full_table_name=full_table_name)

        print(f"[+] Total {self.run_config['load_configuration']['num_inserts']} rows inserted into {full_table_name}")

    def update_data(self, conn, schema_name: str, table_name: str):
        """
        Update random rows in the table.

        Args:
            conn: Database connection object
            schema_name (str): Name of the schema
            table_name (str): Name of the table

        Update behavior:
            - Randomly selects 1-3 columns to update
            - Updates run_config['load_configuration']['num_updates'] rows
            - Uses ORDER BY with random column
            - Uses generate_values_list for new values

        Notes:
            - Uses time_and_log_query for execution and metrics
            - Skips if no columns found
        """
        full_table_name = f"{schema_name}.{table_name}"
        if schema_name not in self.table_columns or table_name not in self.table_columns[schema_name]:
            print(f"[!] No column information found for {full_table_name}")
            return

        columns = self.table_columns[schema_name][table_name]["columns"]
        if not columns:
            print(f"[!] No columns found in {full_table_name}")
            return

        # Generate UPDATE statement with 1-3 random columns
        num_columns_to_update = random.randint(1, min(3, len(columns)))
        columns_to_update = random.sample(columns, num_columns_to_update)
        column_names = [col[0] for col in columns_to_update]
        placeholders = ['%s'] * len(column_names)
        order_by_column = random.choice(columns)[0]
        update_sql = f"""UPDATE {full_table_name}
        SET {', '.join(f'{col[0]} = %s' for col in columns_to_update)}
        WHERE id IN
        (SELECT id FROM {full_table_name}
        ORDER BY {order_by_column}
        LIMIT {self.run_config['load_configuration']['num_updates']})"""

        value_columns = columns_to_update
        values_list = self.generate_values_list(value_columns)
        out = self.time_and_log_query(conn, "update_data", update_sql, values_list, full_table_name=full_table_name)

        print(f"[+] Updated {out} rows in {full_table_name}")

    def delete_data(self, conn, schema_name: str, table_name: str):
        """
        Delete random rows from the table.

        Args:
            conn: Database connection object
            schema_name (str): Name of the schema
            table_name (str): Name of the table

        Delete behavior:
            - Deletes run_config['load_configuration']['num_deletes'] rows
            - Uses ORDER BY with random column

        Notes:
            - Uses time_and_log_query for execution and metrics
            - Skips if no columns found
        """
        full_table_name = f"{schema_name}.{table_name}"
        if schema_name not in self.table_columns or table_name not in self.table_columns[schema_name]:
            print(f"[!] No column information found for {full_table_name}")
            return

        columns = self.table_columns[schema_name][table_name]["columns"]
        if not columns:
            print(f"[!] No columns found in {full_table_name}")
            return

        delete_column = random.choice(columns)

        values_list = self.generate_values_list([delete_column], 1)
        order_by_column = random.choice(columns)[0]

        delete_sql = f"""DELETE FROM {full_table_name}
        WHERE id IN
        (SELECT id FROM {full_table_name}
        ORDER BY {order_by_column}
        LIMIT {self.run_config['load_configuration']['num_deletes']})
        """

        out = self.time_and_log_query(conn, "delete_data", delete_sql, values_list, full_table_name=full_table_name)

        print(f"[+] Deleted {out} rows from {full_table_name}")

    def create_schema_and_tables(self, conn, schema_name: str):
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
        create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
        self.time_and_log_query(conn, "create_schema", create_schema_sql, full_table_name=schema_name)

        print(f"[+] Created schema: {schema_name}")

        # Create the tables under the schema
        table_names = []
        if self.run_config['use_existing_config']:
            table_names = self.table_columns.get(schema_name)
        else:
            table_names = [f"table_{table_idx}_{random.randint(1000, 9999)}" for table_idx in range(self.run_config['table_configuration']['num_tables'])]

        for table_name in table_names:
            for func in self.parse_operations(self.run_config['operations']):
                try:
                    func(conn, schema_name, table_name)
                except Exception as e:
                    print(f"[-] Got an error while {func.__name__} {schema_name}.{table_name}: {e}")
                    bt = traceback.format_exc()
                    print(bt)

        return schema_name

    def load_data(self) -> None:
        """
        Load data into the database according to configuration.

        Args:
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
        config_file = os.path.abspath(self.run_config['system_json_path'])
        self.props = Properties(config_file)
        self.print_sys_props(config_file)

        self.query_info_file = self.run_config['file_configuration']['output_files']['query_info']
        self.load_sql_file = self.run_config['file_configuration']['meta_files']['load_sql']
        self.table_columns_json = self.run_config['file_configuration']['meta_files']['table_columns']
        self.table_columns_csv = self.run_config['file_configuration']['meta_files']['table_columns_csv']
        self.run_config_csv = self.run_config['file_configuration']['meta_files']['run_config']

        with open(self.query_info_file, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["query_type", "pg_xid", "duration_ms", "pg_timestamp", "rows_affected", "table_name"])

        conn = self.connect_db_instance("springtail")
        start_time = time.time()

        # Clean the previous SQL file
        with open(self.load_sql_file, mode='w', newline='') as file:
            file.write("")

        if self.run_config['use_existing_config']:
            if os.path.exists(self.table_columns_json):
                with open(self.table_columns_json, 'r') as table_columns_file:
                    self.table_columns = json.load(table_columns_file)
            else:
                print(f"[!] Warning: {self.table_columns_json} does not exist. Using empty table_columns.")
                self.table_columns = {}
                self.run_config['use_existing_config'] = False

        # Create the schema and the tables
        schema_names = []
        if self.run_config['use_existing_config']:
            schema_names = self.table_columns.keys()
        else:
            schema_names = [f"test_schema_{schema_idx}_{RUN_TS}" for schema_idx in range(self.run_config['num_schemas'])]

        for schema_name in schema_names:
            self.create_schema_and_tables(conn, schema_name)

        # Dumping table columns into JSON file
        with open(self.table_columns_json, 'w') as file:
            json.dump(self.table_columns, file, indent=4)

        # Dumping run config to CSV file
        self.print_run_config_to_csv()
        # Dumping table columns to CSV file
        self.print_table_columns_to_csv()

        conn.close()

def parse_arguments() -> argparse.Namespace:
    """
    Parse command line arguments.

    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(description="Run ingestion metrics logger")
    parser.add_argument('-c', '--load-config-file', type=str, default="load_config.yaml", help='Path to the load configuration file')

    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()
    with open(args.load_config_file) as f:
        run_config = yaml.safe_load(f)

    try:
        LoadGenerator(run_config).load_data()
    except Exception as e:
        print(f"Caught error: {e}")
        sys.exit(1)
