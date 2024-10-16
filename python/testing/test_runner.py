import psycopg2
import logging
import os
import yaml
import time
from jinja2 import Template
from springtail import connect_db_instance, connect_fdw_instance, Properties

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class TestResult:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.errors = []
        self.test_cases = []

class TestCase:
    def __init__(self, name, status, duration, error=None):
        self.name = name
        self.status = status
        self.duration = duration
        self.error = error

def setup(db_config):
    """Initialize the database for logging test executions."""
    logging.info("Running global setup.")
    with psycopg2.connect(**db_config) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS test_execution_log (
                id SERIAL PRIMARY KEY,
                test_name TEXT,
                execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT
            );
            """)

def log_test_execution(db_config, test_name, status):
    """Log the status of each test case execution."""
    logging.info(f"Logging execution of test case: {test_name} with status: {status}")
    with psycopg2.connect(**db_config) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO test_execution_log (test_name, status)
            VALUES (%s, %s);
            """, (test_name, status))

def execute_sql(cursor, sql, section):
    """Execute SQL statements with logging."""
    logging.debug(f"[{section}] Executing SQL: {sql.strip()}")
    cursor.execute(sql)
    logging.debug(f"[{section}] SQL executed successfully.")

def log_data_state(cursor, sql, db_name):
    """Log the data state after executing SQL queries."""
    logging.info(f"Fetching data from {db_name} with query: {sql.strip()}")
    cursor.execute(sql)
    data = cursor.fetchall()
    logging.info(f"Data in {db_name}: {data}")
    return data

def run_test_case(test_file, main_db_config, replica_db_config, results):
    """Run a test case and verify replication."""
    logging.info(f"Running test case: {test_file}")
    start_time = time.time()

    with open(test_file, 'r') as f:
        content = f.read()

    sections = {section.split()[0].lower(): section.split('\n', 1)[1]
                for section in content.split('##') if section.strip()}

    with psycopg2.connect(**main_db_config) as main_conn, \
         psycopg2.connect(**replica_db_config) as replica_conn:
        main_conn.autocommit = True
        replica_conn.autocommit = True
        main_cur = main_conn.cursor()
        replica_cur = replica_conn.cursor()

        try:
            for section in ['setup', 'test', 'verify']:
                if section in sections:
                    logging.info(f"Running {section.upper()} section for {test_file}.")
                    sql_statements = sections[section].strip().split(';')
                    for sql in sql_statements:
                        if sql.strip():
                            if section in ['setup', 'test']:
                                execute_sql(main_cur, sql, section)
                                log_data_state(main_cur, "SELECT * FROM test_execution_log", "Primary DB")
                            elif section == 'verify':
                                time.sleep(1)  # Delay for replication
                                logging.info(f"Verifying with query: {sql.strip()}")
                                main_result = log_data_state(main_cur, sql, "Primary DB")
                                replica_result = log_data_state(replica_cur, sql, "Replica DB")
                                if main_result != replica_result:
                                    raise AssertionError(
                                        f"Verification failed: Main: {main_result}, Replica: {replica_result}"
                                    )
                                logging.info("Verification passed.")

            if 'cleanup' in sections:
                logging.info(f"Running CLEANUP section for {test_file}.")
                for sql in sections['cleanup'].strip().split(';'):
                    if sql.strip():
                        execute_sql(main_cur, sql, "cleanup")

            results.passed += 1
            status = "PASSED"
            logging.info(f"Test case {test_file} PASSED.")
        except (psycopg2.Error, AssertionError, Exception) as e:
            status = "FAILED"
            results.failed += 1
            error_msg = f"Error in {test_file}: {str(e)}"
            results.errors.append(error_msg)
            logging.error(error_msg)
        finally:
            log_test_execution(main_db_config, test_file, status)
            duration = time.time() - start_time
            results.test_cases.append(TestCase(test_file, status, duration, error_msg if status == "FAILED" else None))

def run_all_tests(test_folder, main_db_config, replica_db_config):
    """Run all test cases from the given folder."""
    logging.info(f"Running all test cases from folder: {test_folder}.")
    results = TestResult()
    setup(main_db_config)

    for file in sorted(os.listdir(test_folder)):
        if file.endswith('.sql'):
            run_test_case(os.path.join(test_folder, file), main_db_config, replica_db_config, results)

    generate_report(results)
    logging.info("\n--- Test Summary ---")
    logging.info(f"Total tests run: {results.passed + results.failed}")
    logging.info(f"Tests passed: {results.passed}")
    logging.info(f"Tests failed: {results.failed}")

    if results.errors:
        logging.error("\nErrors encountered:")
        for error in results.errors:
            logging.error(error)

def generate_report(results):
    """Generate a test report in HTML format."""
    template = Template('''
    <html>
    <head><title>Test Report</title></head>
    <body>
        <h1>Test Report</h1>
        <p>Total tests: {{ total_tests }}</p>
        <p>Passed: {{ passed_tests }}</p>
        <p>Failed: {{ failed_tests }}</p>
        <table border="1">
            <tr><th>Test Case</th><th>Status</th><th>Duration (s)</th><th>Error</th></tr>
            {% for test_case in test_cases %}
            <tr>
                <td>{{ test_case.name }}</td>
                <td>{{ test_case.status }}</td>
                <td>{{ "%.2f"|format(test_case.duration) }}</td>
                <td>{{ test_case.error or '' }}</td>
            </tr>
            {% endfor %}
        </table>
    </body>
    </html>
    ''')

    report_content = template.render(
        total_tests=len(results.test_cases),
        passed_tests=results.passed,
        failed_tests=results.failed,
        test_cases=results.test_cases
    )

    os.makedirs('reports', exist_ok=True)
    report_file = 'reports/test_report.html'
    with open(report_file, 'w') as f:
        f.write(report_content)

    logging.info(f"Test report generated: {report_file}")

if __name__ == "__main__":
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)

    test_folder = config['test_folder']
    props = Properties(load_config=False)
    main_db_config = connect_db_instance(props)
    replica_db_config = connect_fdw_instance(props)

    run_all_tests(test_folder, main_db_config, replica_db_config)
