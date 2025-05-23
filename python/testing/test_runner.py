import argparse
import botocore
import jinja2
import json
import logging
from lxml import etree
import os
import sys
import yaml

from test_case import TestCase
from test_set import TestSet

# Get the parent directory of the current script (i.e., the project root directory)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(project_root, 'shared')) # Add the /shared directory to the Python path

from aws import AwsHelper
from common import merge_json


def gen_test_cases(test_set: str,
                   test_files: list,
                   config_file: str,
                   build_dir: str,
                   test_params: dict) -> list[TestSet]:
    """Generate a test set with specific test cases."""
    test = TestSet(test_set, config_file, build_dir, test_params, test_files)
    return [ test ]


def gen_test_set(test_set: str,
                 config_file: str,
                 build_dir: str,
                 test_params: dict) -> list[TestSet]:
    """Generate all of the test cases in a specific test set."""
    test = TestSet(test_set, config_file, build_dir, test_params)
    return [ test ]


def gen_all_tests(test_folder: str,
                  config_file: str,
                  build_dir: str,
                  test_params: dict,
                  test_dirs: list[str]) -> list[TestSet]:
    """Generate all test sets in the test folder.

    Args:
        test_folder (str): Path to the folder containing the test set directories.
        props (Properties): System properties object.

    Returns True if all test cases pass.
    """
    logging.info(f"Scanning all test cases from folder: {test_folder}")

    if test_dirs is None:
        test_dirs = sorted(os.listdir(test_folder))

    # parse and prepare all of the test cases
    test_sets = []
    for test_set in test_dirs:
        if not os.path.isfile(os.path.join(test_folder, test_set, '__config.sql')):
            print(f'Skipping test set {test_set} -- missing __config.sql')
            continue
        print(f'Processing test set {test_set}')
        test_sets.append(TestSet(os.path.join(test_folder, test_set), config_file, build_dir, test_params))

    return test_sets


def create_configurations(tmp_config_dir: str,
                          default_config_file: str,
                          test_config: dict) -> None:
    """Creates Springtail configuration files for the default config
    as well as all overlays into a temporary directory.

    """
    # Clear or create the temporary config directory
    if os.path.exists(tmp_config_dir):
        for f in os.listdir(tmp_config_dir):
            os.remove(os.path.join(tmp_config_dir, f))
    else:
        os.makedirs(tmp_config_dir)

    # load the default configuration
    with open(system_json_path, 'r') as f:
        default_config = json.load(f)

    # write out the default configuration
    with open(os.path.join(tmp_config_dir, 'default.json'), 'w') as f:
        json.dump(default_config, f, indent=2)

    if not test_config.get('overlays'):
        return

    # write out a full configuration for each overlay
    for overlay_name in yaml_config['overlays']:
        overlay_file = os.path.join('overlays', overlay_name + '.json')
        with open(overlay_file) as f:
            overlay_data = json.load(f)
        merged_config = merge_json(default_config, overlay_data)
        with open(os.path.join(tmp_config_dir, f'{overlay_name}.json'), 'w') as f:
            json.dump(merged_config, f, indent=2)


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


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Run Springtail tests")
    parser.add_argument('-a', '--run-all', action='store_true', help='Run all test sets regardless of the default_test_sets')
    parser.add_argument('-c', '--config', type=str, default='config.yaml', help='Path to the test configuration file')
    parser.add_argument('-j', '--junit', type=str, help='Output test results to the specified JUnit XML file')
    parser.add_argument('-o', '--overlay', type=str, help='Run using a specific overlay config')
    parser.add_argument('--skip-downloads', action='store_true', help='Skip downloading the test files from S3')
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
    if args.run_all:
        default_test_sets = None

    # set the log level and format
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

    # prepare the configuration(s)
    tmp_config_dir = 'tmp_configs'
    create_configurations(tmp_config_dir, system_json_path, yaml_config)

    # Process overlay configurations
    if args.overlay:
        if (yaml_config.get('overlays') is None) or (args.overlay not in yaml_config['overlays']):
            raise ValueError(f'overlay "{args.overlay}" does not exist in the configuration')
        overlay_config_file = os.path.join(tmp_config_dir, f'{args.overlay}.json')

        # Extract the overlay configuration and the params specific to the current overlay
        overlay_config = yaml_config['overlays'][args.overlay]
        overlay_test_params = overlay_config.get('params', {})
        # build the test sets for the requested overlay
        if args.test_set is None:
            overlay_test_sets = overlay_config['test_sets']
            tests = gen_all_tests(test_folder, overlay_config_file, build_dir, overlay_test_params, overlay_test_sets)
        else:
            if args.test_case is None:
                tests = gen_test_set(os.path.join(test_folder, args.test_set),
                                     overlay_config_file, build_dir, overlay_test_params)
            else:
                tests = gen_test_cases(os.path.join(test_folder, args.test_set), args.test_case,
                                       overlay_config_file, build_dir, overlay_test_params)

    else:
        default_config_file = os.path.join(tmp_config_dir, 'default.json')
        if args.test_set is None:
            tests = gen_all_tests(test_folder, default_config_file, build_dir, {}, default_test_sets)

            if yaml_config.get('overlays'):
                for overlay_name in yaml_config['overlays']:
                    overlay_config_file = os.path.join(tmp_config_dir, f'{overlay_name}.json')
                    overlay_test_sets = yaml_config['overlays'][overlay_name]['test_sets']
                    tests += gen_all_tests(test_folder, overlay_config_file,
                                           build_dir, {}, overlay_test_sets)
        else:
            if args.test_case is None:
                tests = gen_test_set(os.path.join(test_folder, args.test_set),
                                     default_config_file, build_dir, {})
            else:
                tests = gen_test_cases(os.path.join(test_folder, args.test_set), args.test_case,
                                       default_config_file, build_dir, {})

    # sync the test data files
    if not args.skip_downloads:
        helper = AwsHelper(config=botocore.config.Config(signature_version=botocore.UNSIGNED),
                           region="us-east-1")
        helper.sync_s3_data('test_data', s3_path='test_files')

    # run the tests
    test_failure = False
    for test in tests:
        success = test.run()
        if not success:
            test_failure = True
            break

    # generate the JUnit report, if requested
    try_generate_junit(args.junit, tests)

    # print a report for each test set
    [ test.report() for test in tests ]

    # exit with error on failure
    if test_failure:
        sys.exit(-1)

    # if we succeeded, clean up the configurations
    for f in os.listdir(tmp_config_dir):
        os.remove(os.path.join(tmp_config_dir, f))
