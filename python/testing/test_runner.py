import argparse
import botocore
import json
import logging
from lxml import etree
import os
import shutil
import sys
import yaml
import gzip
import shutil

from test_set import TestSet

from aws import AwsHelper
from common import merge_json

def generate_tests(test_folder: str,
                   test_set: None | list[str],
                   test_files: list[str],
                   config_file: str,
                   build_dir: str,
                   test_params: None | dict,
                   overlay: None | str) -> list[TestSet]:
    """
    Generates a list of TestSet objects based on the provided configuration and test inputs.

    Args:
        test_folder (str): Path to the directory containing all the test sets.
        test_set (list[str] | None): Specific test set names to include. If None, all test sets are included.
        test_files (list[str]): List of test file names to process. If empty, all files will be included.
        config_file (str): Path to overlay specific springtail config file.
        build_dir (str): Top level build directory.
        test_params (dict | None): Optional test parameters for modifying test environment.
        overlay (str | None): Optional overlay name used for the tests.

    Returns:
        list[TestSet]: A list of TestSet objects ready for execution.
    """

    logging.info(
        f'Generate test: test_folder: {test_folder}, '
        f'test_set: {"ALL" if test_set is None else test_set}, '
        f'test_files: {", ".join(test_files) if len(test_files) != 0 else "ALL"}'
    )

    test_sets = []
    if test_set is None:
        test_sets = sorted(os.listdir(test_folder))
    else:
        for t in test_set:
            test_set_file_path = os.path.join(test_folder, t)
            if not os.path.exists(test_set_file_path):
                logging.error(f"Test set directory {test_set_file_path} does not exists")
                raise ValueError(f"Test set directory {test_set_file_path} does not exists")
            test_sets.append(t)

    tests = []
    for ts in test_sets:
        if not os.path.isfile(os.path.join(test_folder, ts, '__config.sql')):
            logging.warning(f'Skipping test set {ts} -- missing __config.sql')
            continue

        if test_files is not None:
            for tf in test_files:
                test_file_path = os.path.join(test_folder, ts, tf)
                if not os.path.exists(test_file_path):
                    logging.error(f"Test file {test_file_path} does not exists")
                    raise ValueError(f"Test file {test_file_path} does not exists")

        logging.info(f'Processing test set {ts}')
        test = TestSet(os.path.join(test_folder, ts), config_file, build_dir, test_params, overlay, test_files)
        if test.skip():
            logging.warning(f'Skipping test set {ts}')
        else:
            tests.append(test)

    return tests

def generate_tests_for_overlay(test_folder: str,
                               build_dir: str,
                               system_json_path: str,
                               tmp_config_dir: str,
                               overlays_config: dict,
                               test_set: None | list[str],
                               test_files: list[str],
                               default_config: dict,
                               overlay: None | str) -> list[TestSet]:
    """
    Generates a list of TestSet objects using overlay-specific configurations.

    This function prepares and configures tests based on a given overlay, combining
    default and overlay-specific settings to produce tailored test sets.

    Args:
        test_folder (str): Path to the directory containing all the test sets.
        build_dir (str): Top level build directory.
        system_json_path (str): Path to springtail system config file.
        tmp_config_dir (str): Temporary directory for storing generated configuration files.
        overlays_config (dict): Dictionary containing overlay configuration.
        test_set (list[str] | None): Specific test set names to include. If None, all test sets are included.
        test_files (list[str]): List of test file names to process. If empty, all files will be included.
        default_config (dict): Default configuration stored in system_json_path.
        overlay (str | None): Optional overlay name used for the tests.

    Returns:
        list[TestSet]: A list of TestSet objects configured with the specified overlay.
    """

    config_json_path = system_json_path

    overlay_params = {}

    # process overlay if specified
    if overlay is not None:
        if overlay not in list(overlays_config.keys()):
            logging.error(f'overlay "{overlay}" does not exist in the configuration')
            raise ValueError(f'overlay "{overlay}" does not exist in the configuration')

        # get overlay parameters
        overlay_params = overlays_config.get(overlay).get('params')
        if overlay_params is None:
            overlay_params = {}

        # create temporary directory if it does not exist
        if not os.path.exists(tmp_config_dir):
            os.makedirs(tmp_config_dir)

        # read overlay system json file
        overlay_file = os.path.join('overlays', overlay + '.json')
        with open(overlay_file) as f:
            overlay_data = json.load(f)

        # merge overlay config with default config
        merged_config = merge_json(default_config, overlay_data)

        # create new system json file for running tests for this overlay
        config_json_path = os.path.join(tmp_config_dir, f'{overlay}.json')
        with open(config_json_path, 'w') as f:
            json.dump(merged_config, f, indent=2)

    # generate tests
    return generate_tests(test_folder, test_set, test_files, config_json_path, build_dir, overlay_params, overlay)


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
    parser.add_argument('-c', '--config', type=str, help='Run specific test sets as specified in the config file for given configuration')
    parser.add_argument('-j', '--junit', type=str, help='Output test results to the specified JUnit XML file')
    parser.add_argument('-o', '--overlay', type=str, help='Run using a specific overlay config')
    parser.add_argument('--skip-downloads', action='store_true', help='Skip downloading the test files from S3')
    parser.add_argument('test_set', type=str, nargs='?', help='Limit to a specific test set')
    parser.add_argument('test_case', type=str, nargs='*', help='Limit to specific test cases from the test set')
    args = parser.parse_args()

    # Manual mutual exclusion check
    if args.config and (args.overlay or args.test_set or args.test_case):
        parser.error("Argument -c/--config is mutually exclusive with -o/--overlay, test_set, and test_case")

    return args

## main()
if __name__ == "__main__":
    # set the log level and format
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

    # parse the command line arguments
    args = parse_arguments()

    config_file = "test_runner_config.yaml"
    # parse the test configuration
    with open(config_file, 'r') as f:
        yaml_config = yaml.safe_load(f)

    test_folder = yaml_config.get('test_folder')

    system_json_path = yaml_config.get('system_json_path')
    if not system_json_path:
        logging.error('"system_json_path" is missing in the YAML configuration')
        raise ValueError('"system_json_path" is missing in the YAML configuration')

    # load the default configuration
    with open(system_json_path, 'r') as f:
        default_config = json.load(f)

    # verify build directory
    build_dir = yaml_config.get('build_dir')
    if not build_dir:
        logging.error('"build_dir" is missing in the YAML configuration')
        raise ValueError('"build_dir" is missing in the YAML configuration')

    # get temp directory
    tmp_config_dir = yaml_config.get('tmp_config_dir')

    # get overlay configuration
    overlays_config = yaml_config.get('overlay_config')

    tests = []
    # positional arguments test_set and test_case and -o option will be ignored when -c option is specified
    if args.config:
        configs = yaml_config.get('configs')
        if args.config not in list(configs.keys()):
            logging.error(f'Configuration "{args.config}" is not defined in the configuration file "{config_file}"')
            raise ValueError(f'Configuration "{args.config}" is not defined in the configuration file "{config_file}"')

        overlay_specs = configs.get(args.config)
        for overlay_item in overlay_specs:
            overlay = overlay_item.get('overlay')
            test_sets = overlay_item.get('test_sets')
            if test_sets == 'all':
                test_sets = None
            else:
                if not isinstance(test_sets, list):
                    logging.error(f'Configuration "{args.config}": test_sets is not a list')
                    raise ValueError(f'Configuration "{args.config}" test_sets is not a list')
                if len(test_sets) == 0:
                    logging.error(f'Configuration "{args.config}": test_sets is empty')
                    raise ValueError(f'Configuration "{args.config}" test_sets is empty')

            tests += generate_tests_for_overlay(test_folder, build_dir, system_json_path, tmp_config_dir, overlays_config, test_sets, [], default_config, overlay)
    else:
        test_sets = [args.test_set] if args.test_set is not None else None
        tests += generate_tests_for_overlay(test_folder, build_dir, system_json_path, tmp_config_dir, overlays_config, test_sets, args.test_case, default_config, None)

    # sync the test data files
    helper = AwsHelper(config=botocore.config.Config(signature_version=botocore.UNSIGNED),
                       region="us-east-1")
    if not args.skip_downloads:
        helper.sync_s3_data('test_data', s3_path='test_files')
    else:
        # CI sets skip_downloads to true but
        # to run performance regressions we use customer.csv
        # so we download it anyway
        csv_file = "customers.csv" 
        os.makedirs("test_data", exist_ok=True)
        helper.s3.download_file('public-share.springtail.io',
                                f"test_files/{csv_file}.gz",
                                f"test_data/{csv_file}.gz")
        with gzip.open(f"test_data/{csv_file}.gz", 'rb') as f_in:
            with open(f"test_data/{csv_file}", 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
            os.remove(f"test_data/{csv_file}.gz")


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
    if os.path.exists(tmp_config_dir):
        shutil.rmtree(tmp_config_dir)

