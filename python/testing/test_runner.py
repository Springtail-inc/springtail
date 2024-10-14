import psycopg2
import logging
import os
import yaml
import time
from jinja2 import Template

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
    logging.info("Running global setup")
    # Connect to the postgres database on the primary
    postgres_config = db_config.copy()
    postgres_config['dbname'] = 'postgres'
    with psycopg2.connect(**postgres_config) as conn:
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

def log_test_execution(db_config, test_name, status):
    postgres_config = db_config.copy()
    postgres_config['dbname'] = 'postgres'
    with psycopg2.connect(**postgres_config) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO test_execution_log (test_name, status)
            VALUES (%s, %s)
            """, (test_name, status))

def execute_sql(cursor, sql):
    logging.debug(f"Executing SQL: {sql}")
    cursor.execute(sql)
    logging.debug("SQL executed successfully")

def run_test_case(test_file, primary_db_config, replica_db_config, results):
    start_time = time.time()
    with open(test_file, 'r') as f:
        content = f.read()

    sections = content.split('##')
    sections = {section.strip().split()[0].lower(): section.split('\n', 1)[1] for section in sections if section.strip()}

    with psycopg2.connect(**primary_db_config) as primary_conn, \
         psycopg2.connect(**replica_db_config) as replica_conn:
        primary_conn.autocommit = True
        replica_conn.autocommit = True
        primary_cur = primary_conn.cursor()
        replica_cur = replica_conn.cursor()

        try:
            for section in ['setup', 'test']:
                if section in sections:
                    logging.info(f"Running {section.upper()} for {test_file}")
                    sql_statements = sections[section].strip().split(';')
                    for sql in sql_statements:
                        if sql.strip():
                            execute_sql(primary_cur, sql)
            
            if 'verify' in sections:
                logging.info(f"Running VERIFY for {test_file}")
                # Add a delay before verification to allow replication to catch up
                time.sleep(5)  # Adjust this delay as needed
                sql_statements = sections['verify'].strip().split(';')
                for sql in sql_statements:
                    if sql.strip():
                        logging.info(f"Executing verification SQL: {sql.strip()}")
                        primary_cur.execute(sql)
                        primary_result = primary_cur.fetchall()
                        replica_cur.execute(sql)
                        replica_result = replica_cur.fetchall()
                        if primary_result != replica_result:
                            raise AssertionError(f"Verification failed for {test_file}: "
                                                 f"Primary DB: {primary_result}, "
                                                 f"Replica DB: {replica_result}")
                        logging.info("Verification passed")

            if 'cleanup' in sections:
                logging.info(f"Running CLEANUP for {test_file}")
                sql_statements = sections['cleanup'].strip().split(';')
                for sql in sql_statements:
                    if sql.strip():
                        execute_sql(primary_cur, sql)

            results.passed += 1
            status = "PASSED"
            logging.info(f"Test case {test_file} PASSED")
        except psycopg2.Error as e:
            results.failed += 1
            status = "FAILED"
            error_msg = f"SQL Error in test case {test_file}: {str(e)}"
            results.errors.append(error_msg)
            logging.error(error_msg)
        except AssertionError as e:
            results.failed += 1
            status = "FAILED"
            error_msg = str(e)
            results.errors.append(error_msg)
            logging.error(error_msg)
        except Exception as e:
            results.failed += 1
            status = "FAILED"
            error_msg = f"Unexpected error in test case {test_file}: {str(e)}"
            results.errors.append(error_msg)
            logging.error(error_msg)
        finally:
            log_test_execution(primary_db_config, test_file, status)

        duration = time.time() - start_time
        results.test_cases.append(TestCase(test_file, status, duration, error_msg if status == "FAILED" else None))

def run_all_tests(test_folder, primary_db_config, replica_db_config):
    logging.info(f"Running all test cases from folder: {test_folder}")
    results = TestResult()
    
    setup(primary_db_config)
    
    for file in sorted(os.listdir(test_folder)):
        if file.endswith('.sql'):
            run_test_case(os.path.join(test_folder, file), primary_db_config, replica_db_config, results)

    generate_report(results)

    # Print summary statistics
    logging.info("\n--- Test Summary ---")
    logging.info(f"Total tests run: {results.passed + results.failed}")
    logging.info(f"Tests passed: {results.passed}")
    logging.info(f"Tests failed: {results.failed}")
    if results.errors:
        logging.info("\nErrors:")
        for error in results.errors:
            logging.error(error)

def generate_report(results):
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

if __name__ == "__main__":
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    test_folder = config['test_folder']

    # Base configuration for both databases
    base_db_config = {
        'user': 'springtail',
        'password': 'springtail',
        'host': 'localhost',
        'port': 5432
    }

    # Main database configuration
    main_db_config = base_db_config.copy()
    main_db_config['dbname'] = 'springtail'

    # Replica database configuration
    replica_db_config = base_db_config.copy()
    replica_db_config['dbname'] = 'replica_springtail'

    run_all_tests(test_folder, main_db_config, replica_db_config)