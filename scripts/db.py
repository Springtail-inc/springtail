import argparse
import psycopg2
from psycopg2 import sql

def get_database_info(conn) -> dict:
    """
    Gathers information about the database including:
    - Number of tables
    - Size of the database
    - Top 10 largest tables
    - Loaded extensions
    - Custom triggers, functions, and types
    """
    database_info = {}

    # Query 1: Number of tables
    cursor = conn.cursor()
    cursor.execute("""
        SELECT count(*)
        FROM information_schema.tables
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema');
    """)
    num_tables = cursor.fetchone()[0]
    database_info['num_tables'] = num_tables

    # Query 2: Database size in GB
    cursor.execute("""
        SELECT pg_size_pretty(pg_database_size(current_database()));
    """)
    db_size = cursor.fetchone()[0]
    database_info['db_size'] = db_size

    # Query 3: Top 5 largest tables and their sizes
    cursor.execute("""
        SELECT
            table_name,
            pg_size_pretty(pg_total_relation_size(quote_ident(table_schema) || '.' || quote_ident(table_name))) AS size
        FROM
            information_schema.tables
        WHERE
            table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY
            pg_total_relation_size(quote_ident(table_schema) || '.' || quote_ident(table_name)) DESC
        LIMIT 5;
    """)
    tables_size = cursor.fetchall()
    database_info['top_tables'] = tables_size

    # Query 4: Loaded extensions
    cursor.execute("""
        SELECT extname FROM pg_extension;
    """)
    extensions = cursor.fetchall()
    database_info['extensions'] = [ext[0] for ext in extensions]

    # Query 5: Custom triggers
    cursor.execute("""
        SELECT trigger_name
        FROM information_schema.triggers
        WHERE trigger_schema NOT IN ('pg_catalog', 'information_schema')
    """)
    triggers = cursor.fetchall()
    database_info['custom_triggers'] = [trigger[0] for trigger in triggers]

    # Query 6: Custom functions
    cursor.execute("""
        SELECT DISTINCT quote_ident(p.proname) as function
        FROM   pg_catalog.pg_proc p
        JOIN   pg_catalog.pg_namespace n ON n.oid = p.pronamespace
        WHERE  n.nspname NOT IN ('pg_catalog', 'information_schema')
  """)
    functions = cursor.fetchall()
    database_info['custom_functions'] = [func[0] for func in functions]

    # Query 7: Custom types \dT
    cursor.execute("""
        SELECT DISTINCT t.typname as type
        FROM pg_type t
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
        WHERE (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid))
        AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid)
        AND n.nspname NOT IN ('pg_catalog', 'information_schema');
    """)
    types = cursor.fetchall()
    database_info['custom_types'] = [type[0] for type in types]

    # Query 8: Get active connections for this db
    cursor.execute("""
        SELECT count(*)
        FROM pg_stat_activity
        WHERE datname = current_database();
    """)
    num_connections = cursor.fetchone()[0]
    database_info['num_connections'] = num_connections

    # Query 9: Get connections grouped by user
    cursor.execute("""
        SELECT usename, application_name, count(*)
        FROM pg_stat_activity
        WHERE datname = current_database()
        GROUP BY usename, application_name
        ORDER BY count(*) DESC;
    """)
    connections_by_user = cursor.fetchall()
    database_info['connections_by_user'] = connections_by_user

    # Query 10: Get tables using custom types
    cursor.execute("""
        SELECT COUNT(DISTINCT c.oid) AS num_tables_with_custom_types
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_attribute a ON a.attrelid = c.oid
        JOIN pg_type t ON t.oid = a.atttypid
        JOIN pg_namespace tn ON tn.oid = t.typnamespace
        WHERE c.relkind = 'r'  -- only regular tables
        AND a.attnum > 0       -- skip system columns
        AND tn.nspname NOT IN ('pg_catalog', 'information_schema')
        AND n.nspname NOT IN ('pg_catalog', 'information_schema');
    """)
    custom_type_tables = cursor.fetchone()[0]
    database_info['custom_type_tables'] = custom_type_tables

    # Query 11: Tables with no primary key index
    cursor.execute("""
        SELECT COUNT(*) AS tables_without_primary_key
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'r'  -- only regular tables
        AND n.nspname NOT IN ('pg_catalog', 'information_schema')
        AND NOT EXISTS (
            SELECT 1
            FROM pg_index i
            WHERE i.indrelid = c.oid
            AND i.indisprimary
        );
    """)
    tables_without_primary_key = cursor.fetchone()[0]
    database_info['tables_without_primary_key'] = tables_without_primary_key

    # Query 12: Database encoding, collation, and ctype
    cursor.execute("""
        SELECT pg_encoding_to_char(encoding), datcollate, datctype
        FROM pg_database
        WHERE datname = current_database();
    """)
    encoding_info = cursor.fetchone()
    database_info['encoding'] = encoding_info[0]
    database_info['collation'] = encoding_info[1]
    database_info['ctype'] = encoding_info[2]

    cursor.close()
    return database_info

def get_db_options(conn) -> dict:
    """
    Get database options
    """
    db_options = {}
    cursor = conn.cursor()

    # get number of users
    cursor.execute("""
        SELECT count(*)
        FROM pg_roles
        WHERE rolname NOT LIKE 'pg_%' AND rolname NOT LIKE 'rds%'
          AND rolname NOT IN ('awsadmin', 'rds_superuser');
    """)
    num_users = cursor.fetchone()[0]
    db_options['num_users'] = num_users

    # get config options
    cursor.execute("""
        SELECT name, setting
        FROM pg_settings
        WHERE name IN ('server_version', 'max_connections', 'max_slot_wal_keep_size',
            'max_replication_slots', , 'wal_level', 'server_encoding', 'ssl,
            'max_logical_replication_workers', 'max_worker_processes', 'max_wal_senders');
    """)
    config_options = cursor.fetchall()
    db_options['config_options'] = {name: setting for name, setting in config_options}

    # get active/inactive replication slot counts
    cursor.execute("""
        SELECT
            active AS is_active,
            COUNT(*) AS slot_count
        FROM
            pg_replication_slots
        GROUP BY
            active
        ORDER BY
            is_active DESC;
    """)
    db_options['active_replication_slots'] = 0
    db_options['inactive_replication_slots'] = 0
    for row in cursor.fetchall():
        if row[0]:
            db_options['active_replication_slots'] = row[1]
        else:
            db_options['inactive_replication_slots'] = row[1]


    cursor.close()
    return db_options

def parseargs():
    """
    Parse command line arguments.
    """
    parser = argparse.ArgumentParser(description="Gather database information.")
    parser.add_argument("-d", "--database", type=str, default='postgres', help="Database name to connect to.")
    parser.add_argument("-u", "--user", type=str, help="Database user.")
    parser.add_argument("-p", "--password", type=str, help="Database password.")
    parser.add_argument("-H", "--host", type=str, default="localhost", help="Database host.")
    parser.add_argument("-P", "--port", type=int, default=5432, help="Database port.")
    return parser.parse_args()

def main():
    """
    Main function to gather database information.
    """
    args = parseargs()
    # Connect to the 'postgres' database to fetch all databases
    conn = psycopg2.connect(dbname=args.database, user=args.user, password=args.password, host=args.host, port=args.port)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT datname
        FROM pg_database d
        WHERE datistemplate = false
        AND has_database_privilege(current_user, d.datname, 'CONNECT');
    """)
    databases = cursor.fetchall()
    cursor.close()

    db_options = get_db_options(conn)

    # Iterate through each database and gather information
    totals = {
        'num_dbs': len(databases),
        'num_tables': 0,
        'db_size': 0,
    }

    for db in databases:
        dbname = db[0]
        print(f"\nGathering info for database: {dbname}")

        # Connect to the individual database
        db_conn = psycopg2.connect(dbname=dbname, user=args.user, password=args.password, host=args.host, port=args.port)

        # Get database info
        db_info = get_database_info(db_conn)

        # Update totals
        totals['num_tables'] += db_info['num_tables']
        # convert size to bytes for summation
        (db_size, db_unit) = db_info['db_size'].split()
        if db_unit == 'GB':
            db_size_bytes = int(db_size) * (1024 ** 3)
        elif db_unit == 'MB':
            db_size_bytes = int(db_size) * (1024 ** 2)
        elif db_unit == 'kB':
            db_size_bytes = int(db_size) * 1024
        else:
            db_size_bytes = int(db_size)

        totals['db_size'] += db_size_bytes

        # Print the gathered information
        print(f"Database size: {db_info['db_size']}")
        print(f"Database encoding: {db_info['encoding']}, collation: {db_info['collation']}, ctype: {db_info['ctype']}")
        print(f"Number of tables: {db_info['num_tables']}")
        print(f"Tables without primary key: {db_info['tables_without_primary_key']}")
        print("Largest tables:")
        for table, size in db_info['top_tables']:
            print(f"  {table}: {size}")

        print("Active connections: ", db_info['num_connections'])
        print("Connections by user:")
        for user, app_name, count in db_info['connections_by_user']:
            print(f"  {user}:{app_name}: {count}")

        print(f"Tables using custom types: {db_info['custom_type_tables']}")

        print("Extensions loaded:")
        for ext in db_info['extensions']:
            print(f"  {ext}")

        print("Custom Triggers:")
        for trigger in db_info['custom_triggers']:
            print(f"  {trigger}")

        print("Custom Functions:")
        for function in db_info['custom_functions']:
            print(f"  {function}")

        print("Custom Types:")
        for custom_type in db_info['custom_types']:
            print(f"  {custom_type}")

        # Close the connection to the current database
        db_conn.close()

    # Close the connection to the 'postgres' database
    conn.close()

    # Print the totals
    print("\nSummary across all databases:")

    print(f"Total databases: {totals['num_dbs']}")
    print(f"Total tables: {totals['num_tables']}")
    # Convert total size to a human-readable format
    if totals['db_size'] >= (1024 ** 3):
        print(f"Total database size: {totals['db_size'] / (1024 ** 3):.2f} GB")
    elif totals['db_size'] >= (1024 ** 2):
        print(f"Total database size: {totals['db_size'] / (1024 ** 2):.2f} MB")
    elif totals['db_size'] >= 1024:
        print(f"Total database size: {totals['db_size'] / 1024:.2f} KB")
    else:
        print(f"Total database size: {totals['db_size']} bytes")

    print(f"Number of users: {db_options['num_users']}")

    print("Replication slots:")
    print(f"  Active: {db_options['active_replication_slots']}")
    print(f"  Inactive: {db_options['inactive_replication_slots']}")

    print("\nConfiguration options:")
    for name, setting in db_options['config_options'].items():
        print(f"  {name}: {setting}")


if __name__ == "__main__":
    main()
