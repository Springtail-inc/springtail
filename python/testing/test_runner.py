import argparse
import jinja2
import logging
from lxml import etree
import os
import sys
import yaml

from test_case import TestCase
from test_set import TestSet

def gen_test_cases(test_set: str,
                   test_files: list,
                   config_file: str,
                   build_dir: str) -> list[TestSet]:
    """Generate a test set with specific test cases."""
    test = TestSet(test_set, config_file, build_dir, test_files)
    return [ test ]


def gen_test_set(test_set: str,
                 config_file: str,
                 build_dir: str) -> list[TestSet]:
    """Generate all of the test cases in a specific test set."""
    test = TestSet(test_set, config_file, build_dir)
    return [ test ]


def gen_all_tests(test_folder: str,
                  config_file: str,
                  build_dir: str,
                  test_dirs: list[str]) -> list[TestSet]:
    """Generate all test sets in the test folder.

    Args:
        test_folder (str): Path to the folder containing the test set directories.
        props (Properties): System properties object.

    Returns True if all test cases pass.
    """
    logging.info(f"Running all test cases from folder: {test_folder}")

    if test_dirs is None:
        test_dirs = sorted(os.listdir(test_folder))

    # parse and prepare all of the test cases
    test_sets = []
    for test_set in test_dirs:
        if not os.path.isfile(os.path.join(test_folder, test_set, '__config.sql')):
            print(f'Skipping test set {test_set} -- missing __config.sql')
            continue
        test_sets.append(TestSet(os.path.join(test_folder, test_set), config_file, build_dir))

    return test_sets


def try_generate_junit(junit_file: str, test_sets: list[TestSet]) -> None:
    """Optionally generates a JUnit XML file with the test results if
    an output file was specified.

    """
    if not junit_file:
        return

    suites = etree.Element('testsuites')
    for test_set in test_sets:
        suites.append(test_set.junit())

    with open(junit_file, 'wb') as f:
        tree = etree.ElementTree(suites)
        tree.write(f, pretty_print=True, xml_declaration=True, encoding='UTF-8')


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
    parser.add_argument('-c', '--config', type=str, default='config.yaml', help='Path to the test configuration file')
    parser.add_argument('-j', '--junit', type=str, help='Output test results to the specified JUnit XML file')
    parser.add_argument('--check', action='store_true', help='Check logs for errors after tests complete')
    parser.add_argument('test_set', type=str, nargs='?', help='Limit to a specific test set')
    parser.add_argument('test_case', type=str, nargs='*', help='Limit to a specific test case from the test set')
    return parser.parse_args()

## main()
if __name__ == "__main__":
    # parse the command line arguments
    args = parse_arguments()

    # parse the test configuration
    with open(args.config, 'r') as f:
        yaml_config = yaml.safe_load(f)

    test_folder = yaml_config['test_folder']

    system_json_path = yaml_config.get('system_json_path')
    if not system_json_path:
        raise ValueError('"system_json_path" is missing in the YAML configuration')

    build_dir = yaml_config.get('build_dir')
    if not build_dir:
        raise ValueError('"build_dir" is missing in the YAML configuration')

    default_test_sets = yaml_config.get('default_test_sets')

    # set the log level and format
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

    # build the test sets
    if args.test_set is None:
        tests = gen_all_tests(test_folder, system_json_path, build_dir, default_test_sets)
    else:
        if args.test_case is None:
            tests = gen_test_set(os.path.join(test_folder, args.test_set),
                                 system_json_path, build_dir)
        else:
            tests = gen_test_cases(os.path.join(test_folder, args.test_set), args.test_case,
                                   system_json_path, build_dir)

    # run the tests
    for test in tests:
        success = test.run()
        if not success:
            break

    # generate the JUnit report, if requested
    try_generate_junit(args.junit, tests)

    # print a report for each test set
    success = all([ test.report() for test in tests ])

    # exit with error on failure
    if not success:
        sys.exit(-1)
