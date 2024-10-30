import argparse
import jinja2
import logging
import os
import psycopg2
import springtail
import time
import yaml

from test_case import TestCase

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class TestResult:
    """Class to store the results of test cases."""

    def __init__(self):
        """Initialize the test result counters."""
        self.passed = 0
        self.failed = 0
        self.errors = []
        self.test_cases = []
        self.error_logs = []  # New field


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


def run_all_tests(test_folder: str, props: springtail.Properties, debug_mode: bool, check_logs: bool) -> None:
    """
    Run all test cases in the specified folder.

    Args:
        test_folder (str): Path to the folder containing SQL test cases.
        props (Properties): System properties object.
    """
    logging.info(f"Running all test cases from folder: {test_folder}")
    results = TestResult()

    # parse and prepare all of the test cases
    test_cases = []
    for test_file in sorted(os.listdir(test_folder)):
        # test files must be of the form "<name>.sql"
        if not test_file.endswith('.sql'):
            logging.warning(f'skipped test file {test_file} -- must have the ".sql" extension')
            continue

        test_case = TestCase(os.path.join(test_folder, test_file), props, debug_mode)
        test_cases.append(test_case)

    # run the test cases
    stop_tests = False
    for test_case in test_cases:
        # run the actual test
        try:
            test_case.setup()
            test_case.test()
            test_case.verify()

        except Exception as e:
            logging.error(f'Error: {e}')
            stop_tests = True # stop running tests

        # try to perform cleanup even on failure
        try:
            test_case.cleanup()
        except Exception as e:
            logging.error(f'Error on cleanup: {e}')

        # if requested, check the logs
        if check_logs:
            test_case.check_logs()

        # if we should stop the tests, break the loop
        if stop_tests:
            break

    # generate a report of the test run
    generate_report(test_cases)


def generate_report(test_cases: list) -> None:
    """
    Generate an HTML report for the test results.

    Args:
        results (TestResult): Object containing the test results.
    """
    template = jinja2.Template('''
    <html>
    <head>
        <title>Test Report</title>
        <style>
            body { font-family: Arial, sans-serif; }
            table { border-collapse: collapse; width: 100%; }
            th, td { border: 1px solid #ddd; padding: 8px; }
            tr:nth-child(even) { background-color: #f2f2f2; }
            .success { color: green; }
            .failed { color: red; }
            .unknown { color: gray; }
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
                <th>Result</th>
                <th>Duration (s)</th>
                <th>Error</th>
            </tr>
            {% for test_case in test_cases %}
            <tr>
                <td>{{ test_case['name'] }}</td>
                <td class="{{ test_case['result'].lower() }}">{{ test_case['result'] }}</td>
                <td>{{ "%.2f ms"|format(test_case['duration'] * 1000) }}</td>
                <td>{{ test_case['error'] }}</td>
            </tr>
            {% endfor %}
        </table>
        
    </body>
    </html>
    ''')

#         {% if error_logs %}
#         <div class="error-logs">
#             <h2>Log File Errors</h2>
#             <pre>
#             {% for error in errors %}
# {{ error }}
#             {% endfor %}
#             </pre>
#         </div>
#         {% endif %}


    results = [ c.get_result() for c in test_cases ]

    total_tests = len(results)
    passed_tests = sum(1 for r in results if r['result'] == 'SUCCESS')
    failed_tests = sum(1 for r in results if r['result'] == 'FAILED')
    
    report = template.render(
        total_tests=total_tests,
        passed_tests=passed_tests,
        failed_tests=failed_tests,
        test_cases=results
    )

    os.makedirs('reports', exist_ok=True)
    report_file = os.path.join('reports', 'test_report.html')
    with open(report_file, 'w') as f:
        f.write(report)

    logging.info(f"Test report generated: {report_file}")

    logging.info("\n--- Test Summary ---")
    logging.info(f"Total tests found: {total_tests}")
    logging.info(f"Total tests run: {passed_tests + failed_tests}")
    logging.info(f"Tests passed: {passed_tests}")
    logging.info(f"Tests failed: {failed_tests}")
    logging.info(f"Tests details:")
    for result in results:
        if result['result'] == 'SUCCESS':
            logging.info(f'Duration: {result["duration"]}')
        if result['error']:
            logging.info(f'Errors: {result["error"]}')


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Run Springtail tests")
    parser.add_argument('-d', '--debug', action='store_true',
                        help='Set this flag to run in debugging mode (does not execute SQL)')
    parser.add_argument('-c', '--config', type=str, required=True, help='Path to the configuration file')
    parser.add_argument('--check', action='store_true', help='Check logs for errors after tests complete')
    return parser.parse_args()

## main()
if __name__ == "__main__":
    args = parse_arguments()

    with open(args.config, 'r') as f:
        yaml_config = yaml.safe_load(f)

    test_folder = yaml_config['test_folder']
    system_json_path = yaml_config.get('system_json_path')

    if not system_json_path:
        raise ValueError("'system_json_path' is missing in the YAML configuration")

    props = springtail.Properties(os.path.abspath(system_json_path))

    run_all_tests(test_folder, props, args.debug, args.check)
