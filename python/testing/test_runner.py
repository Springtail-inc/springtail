import psycopg2
import logging
import os
import yaml
import time
from jinja2 import Template
import argparse
from springtail import connect_db_instance, connect_fdw_instance, Properties

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class TestResult:
    """Class to store the results of test cases."""

    def __init__(self):
        """Initialize the test result counters."""
        self.passed = 0
        self.failed = 0
        self.errors = []
        self.test_cases = []

class TestCase:
    """Class to represent a single test case result."""

    def __init__(self, name, status, duration, error=None):
        """
        Initialize the test case with its name, status, duration, and error (if any).
        
        Args:
            name (str): Name of the test case.
            status (str): Status of the test case (e.g., PASSED, FAILED).
            duration (float): Duration of the test case execution.
            error (str, optional): Error message if the test failed.
        """
        self.name = name
        self.status = status
        self.duration = duration
        self.error = error

def setup(conn):
    """
    Set up the database by creating a log table for test executions.

    Args:
        conn (psycopg2.connection): Connection to the main database.
    """
    logging.info("Running global setup")
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS test_execution_log (
            id SERIAL PRIMARY KEY,
            test_name TEXT,
            execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status TEXT
        )
        """)

def log_test_execution(conn, test_name, status):
    """
    Log the execution status of a test case to the database.

    Args:
        conn (psycopg2.connection): Connection to the main database.
        test_name (str): Name of the test case.
        status (str): Status of the test case (e.g., PASSED, FAILED).
    """
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO test_execution_log (test_name, status)
        VALUES (%s, %s)
        """, (test_name, status))

def execute_sql(cursor, sql):
    """
    Execute an SQL statement and log its progress.

    Args:
        cursor (psycopg2.cursor): Cursor to execute the SQL statement.
        sql (str): SQL query to execute.
    """
    logging.debug(f"Executing SQL: {sql}")
    cursor.execute(sql)
    logging.debug("SQL executed successfully")

def run_test_case(test_file, main_conn, replica_conn, results):
    start_time = time.time()
    with open(test_file, 'r') as f:
        content = f.read()

    sections = content.split('##')
    sections = {section.strip().split()[0].lower(): section.split('\n', 1)[1].strip() for section in sections if section.strip()}

    main_conn.autocommit = True
    replica_conn.autocommit = True
    main_cur = main_conn.cursor()
    replica_cur = replica_conn.cursor()

    try:
        for section in ['setup', 'test', 'verify']:
            if section in sections:
                logging.info(f"Running {section.upper()} for {test_file}")
                sql_statements = sections[section].split(';')

                for sql in sql_statements:
                    if sql.strip():
                        if section in ['setup', 'test']:
                            execute_sql(main_cur, sql)
                        elif section == 'verify':
                            if sql.strip().lower().startswith('select'):
                                time.sleep(1)  # Allow time for replication
                                logging.info(f"Verifying: {sql.strip()}")

                                main_cur.execute(sql)
                                main_result = main_cur.fetchall()
                                replica_cur.execute(sql)
                                replica_result = replica_cur.fetchall()

                                if main_result != replica_result:
                                    raise AssertionError(
                                        f"Verification failed for {test_file}: "
                                        f"Main DB: {main_result}, Replica DB: {replica_result}"
                                    )
                                logging.info("Verification passed")
                            else:
                                logging.info(f"Executing non-SELECT verification: {sql.strip()}")
                                execute_sql(main_cur, sql)

        # Handle cleanup gracefully
        if 'cleanup' in sections:
            cleanup_content = sections['cleanup'].strip()
            if not cleanup_content or all(line.startswith('--') for line in cleanup_content.split('\n')):
                logging.info(f"Skipping empty or comment-only cleanup in {test_file}")
            else:
                logging.info(f"Running CLEANUP for {test_file}")
                cleanup_statements = cleanup_content.split(';')
                for sql in cleanup_statements:
                    if sql.strip():
                        execute_sql(main_cur, sql)

        results.passed += 1
        status = "PASSED"
        logging.info(f"Test case {test_file} PASSED")

    except (psycopg2.Error, AssertionError, Exception) as e:
        results.failed += 1
        status = "FAILED"
        error_msg = str(e)
        results.errors.append(error_msg)
        logging.error(f"Test case {test_file} FAILED: {error_msg}")

    finally:
        log_test_execution(main_conn, test_file, status)

    duration = time.time() - start_time
    results.test_cases.append(TestCase(test_file, status, duration, error_msg if status == "FAILED" else None))


def run_all_tests(test_folder, main_conn, replica_conn):
    """
    Run all test cases in the specified folder.

    Args:
        test_folder (str): Path to the folder containing SQL test cases.
        main_conn (psycopg2.connection): Connection to the primary database.
        replica_conn (psycopg2.connection): Connection to the replica database.
    """
    logging.info(f"Running all test cases from folder: {test_folder}")
    results = TestResult()

    setup(main_conn)

    for file in sorted(os.listdir(test_folder)):
        if file.endswith('.sql'):
            run_test_case(os.path.join(test_folder, file), main_conn, replica_conn, results)

    generate_report(results)

    logging.info("\n--- Test Summary ---")
    logging.info(f"Total tests run: {results.passed + results.failed}")
    logging.info(f"Tests passed: {results.passed}")
    logging.info(f"Tests failed: {results.failed}")
    if results.errors:
        logging.info("\nErrors:")
        for error in results.errors:
            logging.error(error)

def generate_report(results):
    """
    Generate an HTML report for the test results.

    Args:
        results (TestResult): Object containing the test results.
    """
    template = Template('''
    <html>
    <head>
        <title>Test Report</title>
        <style>
            body { font-family: Arial, sans-serif; }
            table { border-collapse: collapse; width: 100%; }
            th, td { border: 1px solid #ddd; padding: 8px; }
            tr:nth-child(even) { background-color: #f2f2f2; }
            .passed { color: green; }
            .failed { color: red; }
        </style>
    </head>
    <body>
        <h1>Test Report</h1>
        <p>Total tests: {{ total_tests }}</p>
        <p>Passed: {{ passed_tests }}</p>
        <p>Failed: {{ failed_tests }}</p>
        <table>
            <tr>
                <th>Test Case</th>
                <th>Status</th>
                <th>Duration (s)</th>
                <th>Error</th>
            </tr>
            {% for test_case in test_cases %}
            <tr>
                <td>{{ test_case.name }}</td>
                <td class="{{ test_case.status.lower() }}">{{ test_case.status }}</td>
                <td>{{ "%.2f"|format(test_case.duration) }}</td>
                <td>{{ test_case.error or '' }}</td>
            </tr>
            {% endfor %}
        </table>
    </body>
    </html>
    ''')

    report = template.render(
        total_tests=len(results.test_cases),
        passed_tests=results.passed,
        failed_tests=results.failed,
        test_cases=results.test_cases
    )

    os.makedirs('reports', exist_ok=True)
    report_file = os.path.join('reports', 'test_report.html')
    with open(report_file, 'w') as f:
        f.write(report)

    logging.info(f"Test report generated: {report_file}")

def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Run Springtail tests")
    parser.add_argument('-c', '--config', type=str, required=True, help='Path to the configuration file')
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()

    with open(args.config, 'r') as f:
        yaml_config = yaml.safe_load(f)

    test_folder = yaml_config['test_folder']
    system_json_path = yaml_config.get('system_json_path')

    if not system_json_path:
        raise ValueError("'system_json_path' is missing in the YAML configuration")

    props = Properties(os.path.abspath(system_json_path))
    main_conn = connect_db_instance(props)
    replica_conn = connect_fdw_instance(props)

    run_all_tests(test_folder, main_conn, replica_conn)

    main_conn.close()
    replica_conn.close()
