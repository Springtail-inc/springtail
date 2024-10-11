import psycopg2
import logging
import os
import yaml
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from jinja2 import Template
import re

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
    with psycopg2.connect(**db_config) as conn:
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

def teardown(db_config):
    logging.info("Running global teardown")
    with psycopg2.connect(**db_config) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE test_execution_log")

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
                            execute_sql(replica_cur, sql)
            
            # Separate handling for the 'verify' section
            if 'verify' in sections:
                logging.info(f"Running VERIFY for {test_file}")
                verify_statements = re.findall(r'SELECT.+?;', sections['verify'], re.DOTALL | re.IGNORECASE)
                for sql in verify_statements:
                    # Execute the query on the primary database
                    logging.info(f"Running SELECT on primary DB: {sql.strip()}")
                    primary_cur.execute(sql)
                    primary_result = primary_cur.fetchall()
                    
                    # Execute the same query on the replica database
                    logging.info(f"Running SELECT on replica DB: {sql.strip()}")
                    replica_cur.execute(sql)
                    replica_result = replica_cur.fetchall()
                    
                    # Compare the results between primary and replica
                    if primary_result != replica_result:
                        error_msg = (f"Verification failed for {test_file}: "
                                     f"Primary DB: {primary_result}, "
                                     f"Replica DB: {replica_result}")
                        raise AssertionError(error_msg)

                    logging.info(f"Verification passed: {sql.strip()}")

            if 'cleanup' in sections:
                logging.info(f"Running CLEANUP for {test_file}")
                sql_statements = sections['cleanup'].strip().split(';')
                for sql in sql_statements:
                    if sql.strip():
                        execute_sql(primary_cur, sql)
                        execute_sql(replica_cur, sql)

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
            primary_cur.close()
            replica_cur.close()

        duration = time.time() - start_time
        results.test_cases.append(TestCase(test_file, status, duration, error_msg if status == "FAILED" else None))


def run_all_tests(test_folder, primary_db_config, replica_db_config, max_workers):
    logging.info(f"Running all test cases from folder: {test_folder}")
    results = TestResult()
    
    setup(primary_db_config)
    setup(replica_db_config)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_test = {executor.submit(run_test_case, os.path.join(test_folder, file), primary_db_config, replica_db_config, results): file 
                          for file in sorted(os.listdir(test_folder)) if file.endswith('.sql')}
        for future in as_completed(future_to_test):
            test = future_to_test[future]
            try:
                future.result()
            except Exception as exc:
                print(f'{test} generated an exception: {exc}')

    teardown(primary_db_config)
    teardown(replica_db_config)
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
    max_workers = config['max_workers']

    # Base configuration for both databases
    base_db_config = {
        'dbname': 'springtail',
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

    run_all_tests(test_folder, main_db_config, replica_db_config, max_workers)