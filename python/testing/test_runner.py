import psycopg2
import logging
import os
import time
import select
from psycopg2 import OperationalError

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Increase default timeout
DEFAULT_TIMEOUT = 60  # 60 seconds

def check_db_connection(db_config):
    """Check if the database is accessible."""
    try:
        with psycopg2.connect(db_config, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        return True
    except psycopg2.Error:
        return False

def connect_with_retry(db_config, max_retries=3, retry_delay=5):
    """Attempt to connect to the database with retries."""
    for attempt in range(max_retries):
        try:
            return psycopg2.connect(db_config)
        except psycopg2.Error as e:
            if attempt == max_retries - 1:
                raise
            logging.warning(f"Failed to connect to database (attempt {attempt + 1}): {e}")
            time.sleep(retry_delay)

def check_replication_status(conn):
    """Check if the replication subscription is active."""
    cur = conn.cursor()
    cur.execute("SELECT * FROM pg_stat_subscription WHERE subname = 'springtail_sub';")
    replication_status = cur.fetchone()
    logging.info(f"Replication status: {replication_status}")
    return replication_status

def insert_test_state(test_name, cur_main):
    """Insert the current test state into the test_state table."""
    logging.info(f"Inserting test state for: {test_name}")
    cur_main.execute(f"INSERT INTO test_state (test_name, status) VALUES (%s, %s) ON CONFLICT (test_name) DO NOTHING;", (test_name, 'running'))
    cur_main.connection.commit()

def check_table_locks(cursor, table_name):
    """Check for any locks on the specified table."""
    cursor.execute(f"""
        SELECT pid, mode, granted
        FROM pg_locks l
        JOIN pg_class t ON l.relation = t.oid
        WHERE t.relkind = 'r' AND t.relname = '{table_name}'
    """)
    locks = cursor.fetchall()
    logging.info(f"Active locks on {table_name}: {locks}")

def close_other_connections(cursor, db_name):
    """Attempt to close other connections to the database."""
    cursor.execute(f"""
        SELECT pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE datname = '{db_name}'
        AND pid <> pg_backend_pid()
    """)
    logging.info("Attempted to close other connections to the database.")

def check_long_running_queries(cursor):
    """Check for any long-running queries that might be holding locks."""
    cursor.execute("""
        SELECT pid, now() - pg_stat_activity.query_start AS duration, query
        FROM pg_stat_activity
        WHERE (now() - pg_stat_activity.query_start) > interval '5 seconds'
    """)
    long_running_queries = cursor.fetchall()
    for query in long_running_queries:
        logging.warning(f"Long-running query detected: PID={query[0]}, Duration={query[1]}, Query={query[2]}")

def drop_table_with_lock(cursor, table_name):
    """Attempt to acquire an exclusive lock before dropping the table."""
    try:
        cursor.execute(f"LOCK TABLE {table_name} IN ACCESS EXCLUSIVE MODE NOWAIT")
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        logging.info(f"Table {table_name} dropped successfully")
    except psycopg2.Error as e:
        logging.error(f"Failed to acquire lock or drop table {table_name}: {e}")

def execute_sql_section(cursor, section_sql, max_retries=3, retry_delay=1, timeout=DEFAULT_TIMEOUT):
    """Execute a block of SQL (setup, test, verify, or cleanup) with retries and timeout."""
    for statement in section_sql.strip().split(';'):
        if statement.strip():
            retries = 0
            while retries < max_retries:
                try:
                    logging.debug(f"Preparing to execute SQL: {statement}")
                    if statement.lower().startswith("drop table"):
                        drop_timeout = 120  # 2 minutes for DROP TABLE
                        logging.info(f"Starting DROP TABLE operation: {statement}")
                        start_time = time.time()
                        cursor.execute(statement)
                        end_time = time.time()
                        logging.info(f"DROP TABLE operation completed in {end_time - start_time:.2f} seconds")
                    else:
                        drop_timeout = timeout
                        cursor.execute(statement)
                    while True:
                        if select.select([cursor.connection], [], [], drop_timeout) == ([], [], []):
                            logging.warning(f"Operation timed out after {drop_timeout} seconds: {statement}")
                            break
                        if cursor.connection.notifies:
                            cursor.connection.notifies.pop()
                        else:
                            break
                    logging.debug(f"SQL executed successfully: {statement}")
                    cursor.connection.commit()
                    logging.debug(f"Transaction committed for: {statement}")
                    break
                except OperationalError as e:
                    retries += 1
                    logging.warning(f"Error executing SQL (attempt {retries}): {e}")
                    if retries == max_retries:
                        raise
                    time.sleep(retry_delay)
                except Exception as e:
                    logging.error(f"Error executing SQL: {statement}")
                    logging.error(f"Exception: {e}")
                    raise

def run_test_case(test_file, db_main, db_replica, run_sections=None):
    """Run a test case from a SQL file on both main and replica databases."""
    with open(test_file, 'r') as f:
        content = f.read()

    # Split the file into sections based on ## markers
    sections = content.split('##')
    sections = {section.strip().split()[0].lower(): section.split('\n', 1)[1] for section in sections if section.strip()}

    # Connect to both main and replica databases with retry
    with connect_with_retry(db_main) as conn_main, connect_with_retry(db_replica) as conn_replica:
        conn_main.autocommit = False
        conn_replica.autocommit = False
        cur_main = conn_main.cursor()
        cur_replica = conn_replica.cursor()

        try:
            # Insert the test name into test_state for tracking
            test_name = os.path.basename(test_file).replace('.sql', '')
            insert_test_state(test_name, cur_main)

            for section in ['setup', 'test', 'verify', 'cleanup']:
                if run_sections and section not in run_sections:
                    continue
                if section in sections:
                    logging.info(f"Running {section.upper()} for {test_name}")
                    
                    if section == 'cleanup':
                        # Check for table locks before cleanup
                        check_table_locks(cur_main, 'test1')  # Assuming the table name is always 'test1'
                        close_other_connections(cur_main, 'springtail')  # Replace 'springtail' with your actual database name
                        check_long_running_queries(cur_main)
                        drop_table_with_lock(cur_main, 'test1')
                    else:
                        execute_sql_section(cur_main, sections[section])
                    
                    conn_main.commit()
                    logging.info(f"{section.upper()} phase for {test_name} completed")

                    if section == 'verify':
                        # Execute verify on replica as well
                        cur_main.execute(sections['verify'].strip())
                        result_main = cur_main.fetchall()
                        logging.info(f"Main DB result: {result_main}")

                        cur_replica.execute(sections['verify'].strip())
                        result_replica = cur_replica.fetchall()
                        logging.info(f"Replica DB result: {result_replica}")

                        # Compare the results between main and replica
                        if result_main != result_replica:
                            logging.error(f"Verification failed for {test_name}: Main DB result {result_main}, Replica DB result {result_replica}")
                        else:
                            logging.info(f"Verification passed for {test_name}")

        except Exception as e:
            logging.error(f"Error in test case {test_name}: {e}")
            conn_main.rollback()
            conn_replica.rollback()
        finally:
            cur_main.close()
            cur_replica.close()

    # Check if replica is still accessible after the test
    if not check_db_connection(db_replica):
        logging.error("Replica database became inaccessible after the test")

def run_all_tests(test_folder, db_main, db_replica):
    """Run all test case files in the specified folder."""
    logging.info(f"Running all test cases from folder: {test_folder}")
    for file in sorted(os.listdir(test_folder)):
        if file.endswith('.sql'):
            logging.info(f"Running test case: {file}")
            run_test_case(os.path.join(test_folder, file), db_main, db_replica)
            
            # Check if databases are still accessible after each test
            if not check_db_connection(db_main):
                logging.error("Main database became inaccessible")
                break
            if not check_db_connection(db_replica):
                logging.error("Replica database became inaccessible")
                break

# Example usage of running all test cases
if __name__ == "__main__":
    test_folder = 'test_cases'  # Folder with test case files
    db_main = 'dbname=springtail user=springtail host=localhost connect_timeout=10'
    db_replica = 'dbname=springtail user=springtail host=localhost connect_timeout=10'

    run_all_tests(test_folder, db_main, db_replica)

    # Uncomment the following lines to run a specific test case
    # specific_test = os.path.join(test_folder, 'test_case1.sql')
    # run_test_case(specific_test, db_main, db_replica)

    # Uncomment the following line to run specific sections of a test case
    # run_test_case(specific_test, db_main, db_replica, run_sections=['setup', 'test'])