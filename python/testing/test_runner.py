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
        self.error_logs = []  # New field to store error logs

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

def check_backtrace(log_path):
    """
    Check log files for backtraces indicating errors.
    
    Args:
        log_path (str): Path to directory containing log files
        
    Returns:
        list: List of log files containing backtraces
    """
    error_logs = []
    for file in os.listdir(log_path):
        if file.endswith('.log'):
            log_file = os.path.join(log_path, file)
            try:
                with open(log_file, 'r') as f:
                    content = f.read()
                    if 'Backtrace:' in content:
                        error_logs.append(log_file)
            except Exception as e:
                logging.error(f"Error reading log file {log_file}: {str(e)}")
    return error_logs

def extract_backtrace(log_file):
    """
    Extract backtrace from a log file.
    
    Args:
        log_file (str): Path to log file
        
    Returns:
        list: Lines containing the backtrace
    """
    backtrace = []
    in_backtrace = False
    try:
        with open(log_file, 'r') as f:
            for line in f:
                if 'Backtrace:' in line:
                    in_backtrace = True
                elif in_backtrace and line.strip() and not line.startswith('---'):
                    backtrace.append(line)
                elif in_backtrace and (line.startswith('---') or not line.strip()):
                    in_backtrace = False
    except Exception as e:
        logging.error(f"Error extracting backtrace from {log_file}: {str(e)}")
    return backtrace

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

def execute_sql(cursor, sql, fetch_results=False):
    """
    Execute an SQL statement and log its progress.

    Args:
        cursor (psycopg2.cursor): Cursor to execute the SQL statement.
        sql (str): SQL query to execute.
        fetch_results (bool): Whether to fetch and return results.

    Returns:
        list: Query results if fetch_results is True, None otherwise.
    """
    logging.debug(f"Executing SQL statement:\n{sql}")
    cursor.execute(sql)
    if fetch_results:
        return cursor.fetchall()
    logging.debug("SQL executed successfully")
    return None

def split_sql_statements(sql_content):
    """
    Split SQL content into individual statements.
    
    Args:
        sql_content (str): SQL content containing one or more statements.
        
    Returns:
        list: List of individual SQL statements.
    """
    statements = []
    current_statement = []
    
    for line in sql_content.split('\n'):
        line = line.strip()
        if not line or line.startswith('--'):
            continue
            
        current_statement.append(line)
        
        if line.endswith(';'):
            statements.append(' '.join(current_statement))
            current_statement = []
            
    if current_statement:  # Handle last statement if it doesn't end with semicolon
        statements.append(' '.join(current_statement))
        
    return statements

def run_test_case(test_file, main_conn, replica_conn, results):
    """
    Execute a test case and verify its results across primary and replica databases.
    """
    start_time = time.time()
    with open(test_file, 'r') as f:
        content = f.read()

    sections = content.split('##')
    sections = {section.strip().split()[0].lower(): section.split('\n', 1)[1].strip() 
               for section in sections if section.strip()}

    main_conn.autocommit = True
    replica_conn.autocommit = True
    main_cur = main_conn.cursor()
    replica_cur = replica_conn.cursor()

    error_msg = None
    try:
        for section in ['setup', 'test', 'verify']:
            if section in sections:
                logging.info(f"Running {section.upper()} for {test_file}")
                sql_statements = split_sql_statements(sections[section])
                
                for sql in sql_statements:
                    if section in ['setup', 'test']:
                        execute_sql(main_cur, sql)
                    elif section == 'verify':
                        time.sleep(1)  # Allow time for replication to catch up
                        logging.info(f"Verifying: {sql}")
                        
                        main_result = execute_sql(main_cur, sql, fetch_results=True)
                        replica_result = execute_sql(replica_cur, sql, fetch_results=True)
                        
                        if main_result != replica_result:
                            raise AssertionError(
                                f"Verification failed for {test_file}:\n"
                                f"Statement: {sql}\n"
                                f"Main DB: {main_result}\n"
                                f"Replica DB: {replica_result}"
                            )
                        logging.info(f"Verification passed for: {sql}")

        if 'cleanup' in sections and sections['cleanup'].strip():
            logging.info(f"Running CLEANUP for {test_file}")
            cleanup_statements = split_sql_statements(sections['cleanup'])
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
    results.test_cases.append(TestCase(test_file, status, duration, error_msg))

def check_logs(props, results):
    """
    Check logs for errors and update test results.
    
    Args:
        props (Properties): System properties object
        results (TestResult): Test results object to update
    """
    log_path = props.get_log_path()
    error_logs = check_backtrace(log_path)
    
    if error_logs:
        logging.error(f"Found errors in logs: {error_logs}")
        results.error_logs = error_logs
        
        for log in error_logs:
            backtrace = extract_backtrace(log)
            if backtrace:
                error_msg = f"Error in {os.path.basename(log)}:\n{''.join(backtrace)}"
                results.errors.append(error_msg)

def generate_report(results):
    """Generate an HTML report for the test results."""
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
            .error-logs { margin-top: 20px; }
        </style>
    </head>
    <body>
        <h1>Test Report</h1>
        <p>Total tests: {{ total_tests }}</p>
        <p>Passed: {{ passed_tests }}</p>
        <p>Failed: {{ failed_tests }}</p>
        
        <h2>Test Cases</h2>
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
        
        {% if error_logs %}
        <div class="error-logs">
            <h2>Log File Errors</h2>
            <pre>
            {% for error in errors %}
{{ error }}
            {% endfor %}
            </pre>
        </div>
        {% endif %}
    </body>
    </html>
    ''')

    report = template.render(
        total_tests=len(results.test_cases),
        passed_tests=results.passed,
        failed_tests=results.failed,
        test_cases=results.test_cases,
        error_logs=results.error_logs,
        errors=results.errors
    )

    os.makedirs('reports', exist_ok=True)
    report_file = os.path.join('reports', 'test_report.html')
    with open(report_file, 'w') as f:
        f.write(report)

    logging.info(f"Test report generated: {report_file}")

def run_all_tests(test_folder, main_conn, replica_conn, props):
    """
    Run all test cases in the specified folder.

    Args:
        test_folder (str): Path to the folder containing SQL test cases.
        main_conn (psycopg2.connection): Connection to the primary database.
        replica_conn (psycopg2.connection): Connection to the replica database.
        props (Properties): System properties object.
    """
    logging.info(f"Running all test cases from folder: {test_folder}")
    results = TestResult()

    setup(main_conn)

    for file in sorted(os.listdir(test_folder)):
        if file.endswith('.sql'):
            run_test_case(os.path.join(test_folder, file), main_conn, replica_conn, results)

    # Check logs for errors after all tests have completed
    check_logs(props, results)
    
    generate_report(results)

    logging.info("\n--- Test Summary ---")
    logging.info(f"Total tests run: {results.passed + results.failed}")
    logging.info(f"Tests passed: {results.passed}")
    logging.info(f"Tests failed: {results.failed}")
    
    if results.errors:
        logging.info("\nErrors:")
        for error in results.errors:
            logging.error(error)

def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Run Springtail tests")
    parser.add_argument('-c', '--config', type=str, required=True, help='Path to the configuration file')
    parser.add_argument('--check', action='store_true', help='Check logs for errors after tests complete')
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

    run_all_tests(test_folder, main_conn, replica_conn, props)

    main_conn.close()
    replica_conn.close()