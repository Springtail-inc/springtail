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

def setup(conn):
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

def run_test_case(test_file, main_conn, replica_conn, results):
    start_time = time.time()
    with open(test_file, 'r') as f:
        content = f.read()

    sections = content.split('##')
    sections = {section.strip().split()[0].lower(): section.split('\n', 1)[1] for section in sections if section.strip()}

    main_conn.autocommit = True
    replica_conn.autocommit = True
    main_cur = main_conn.cursor()
    replica_cur = replica_conn.cursor()

    try:
        for section in ['setup', 'test', 'verify']:
            if section in sections:
                logging.info(f"Running {section.upper()} for {test_file}")
                sql_statements = sections[section].strip().split(';')
                for sql in sql_statements:
                    if sql.strip():
                        if section in ['setup', 'test']:
                            execute_sql(main_cur, sql)
                        elif section == 'verify':
                            # Add a delay before verification to allow replication to catch up
                            time.sleep(1)  # Adjust this delay as needed
                            logging.info(f"Verifying: {sql.strip()}")
                            main_cur.execute(sql)
                            main_result = main_cur.fetchall()
                            replica_cur.execute(sql)
                            replica_result = replica_cur.fetchall()
                            if main_result != replica_result:
                                raise AssertionError(f"Verification failed for {test_file}: "
                                                     f"Main DB: {main_result}, "
                                                     f"Replica DB: {replica_result}")
                            logging.info("Verification passed")

        if 'cleanup' in sections:
            logging.info(f"Running CLEANUP for {test_file}")
            sql_statements = sections['cleanup'].strip().split(';')
            for sql in sql_statements:
                if sql.strip():
                    execute_sql(main_cur, sql)

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
        log_test_execution(main_conn, test_file, status)

    duration = time.time() - start_time
    results.test_cases.append(TestCase(test_file, status, duration, error_msg if status == "FAILED" else None))

def run_all_tests(test_folder, main_conn, replica_conn):
    logging.info(f"Running all test cases from folder: {test_folder}")
    results = TestResult()
    
    setup(main_conn)
    
    for file in sorted(os.listdir(test_folder)):
        if file.endswith('.sql'):
            run_test_case(os.path.join(test_folder, file), main_conn, replica_conn, results)

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

def parse_arguments():
    parser = argparse.ArgumentParser(description="Run Springtail tests")
    parser.add_argument('-c', '--config', type=str, required=True, help='Path to the configuration file')
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()

    # Load the YAML configuration
    with open(args.config, 'r') as f:
        yaml_config = yaml.safe_load(f)
    
    test_folder = yaml_config['test_folder']

    # Check if 'system_json_path' is in the YAML config
    if 'system_json_path' not in yaml_config:
        raise ValueError("'system_json_path' is missing in the YAML configuration")

    system_json_path = yaml_config['system_json_path']

    # Get the absolute path of the system.json file
    current_dir = os.path.dirname(os.path.abspath(args.config))
    absolute_system_json_path = os.path.abspath(os.path.join(current_dir, system_json_path))

    logging.info(f"Using system JSON file: {absolute_system_json_path}")

    # Initialize Properties with the absolute path to the system.json file
    props = Properties(absolute_system_json_path)

    # Get database connections
    main_conn = connect_db_instance(props)
    replica_conn = connect_fdw_instance(props)

    run_all_tests(test_folder, main_conn, replica_conn)

    # Close the connections
    main_conn.close()
    replica_conn.close()