import logging
import os
import springtail
import sysutils

from test_case import TestCase

_GLOBAL_CONFIG_FILE = '__config.sql'

class TestSet:
    """Class to manage a set of tests.  A test set is composed of a
    set of test files and a global "config" file, all placed together
    into a directory.  The global config is a specialized test case
    that consists of a "metadata" section which defines global
    parameters for the tests in this set, a "setup" section which
    performs operations on the primary database prior to starting the
    Springtail replica, and a "cleanup" section which performs
    operations on the primary database after stopping the Springtail
    replica.

    The test files have a "metadata", "test", "verify" and "cleanup"
    sections which are run sequentially for each test case.  The test
    cases are also run sequentially based on the lexical sort order of
    the files in the directory.

    """
    def __init__(self,
                 directory: str,
                 config_file: str,
                 build_dir: str) -> None:
        """Initialize the test set"""
        self._directory = directory
        self._config_file = config_file
        self._build_dir = build_dir
        self._props = springtail.Properties(config_file, True)

        # constuct the special "config" test case for global setup and cleanup
        self._config = TestCase(os.path.join(directory, _GLOBAL_CONFIG_FILE), self._props, ['setup', 'cleanup'])

        # collect and parse the test cases from the directory
        self._test_files = [ ]
        self._tests = { }
        for test_file in sorted(os.listdir(directory)):
            if test_file == _GLOBAL_CONFIG_FILE:
                continue

            # test files must be of the form "<name>.sql"
            if not test_file.endswith('.sql'):
                logging.warning(f'skipped test file {test_file} -- must have the ".sql" extension')
                continue

            self._test_files.append(test_file)
            self._tests[test_file] = TestCase(os.path.join(directory, test_file), self._props)


    def run(self,
            test_files: list = [],
            check_logs: bool = False,
            shutdown_on_fail: bool = False) -> bool:
        """Runs one or more of the test cases in the test set in the
        provided order.  If no test cases are provided then it runs
        all of the tests in lexographical order.

        Returns True if the tests all succeed, False otherwise

        """
        # make sure Springtail is stopped
        logging.debug('Stopping any existing Springtail instance')
        springtail.stop(self._config_file, do_cleanup=True)

        # perform the primary db setup
        logging.debug('Perform the global setup()')
        self._config.setup()

        # start Springtail
        logging.debug('Starting the Springtail instance')
        springtail.start(self._config_file, self._build_dir, do_cleanup=False)

        # run the tests
        if not test_files:
            test_files = self._test_files
        logging.info(f'Run the tests: {test_files}')

        test_failed = False
        for test_file in test_files:
            if test_file not in self._tests:
                logging.warning(f'unable to find test: {test_file}')
                continue
            logging.debug(f'Running the test: {test_file}')

            # run the actual test
            try:
                self._tests[test_file].test()
                self._tests[test_file].verify()
            except Exception as e:
                logging.error(f'Error: {e}')
                test_failed = True

            # if we should stop the tests, break the loop
            if test_failed and not shutdown_on_fail:
                if check_logs:
                    test_case.check_logs()
                break

            # try to perform cleanup
            try:
                self._tests[test_file].cleanup()
            except Exception as e:
                logging.error(f'Error on cleanup: {e}')

            # check here if the test failed nad we need to check the logs
            if test_failed and check_logs:
                test_case.check_logs()
                break

        # if a test failed and we don't shutdown on failure, return immediately
        if test_failed and not shutdown_on_fail:
            return False

        # shutdown Springtail
        logging.debug('Stopping the Springtail instance')
        springtail.stop(self._config_file)

        # perform the primary db cleanup
        logging.debug('Perform the global cleanup()')
        self._config.cleanup()

        return not test_failed


    def report(self) -> None:
        """Generates a report about the test set"""
        results = [ self._tests[t].get_result() for t in self._tests ]
        passed_tests = sum(1 for r in results if r['result'] == 'SUCCESS')
        failed_tests = sum(1 for r in results if r['result'] == 'FAILED')

        print('\n')
        print(f'--- Test Summary: {os.path.basename(self._directory)} ---')
        print(f'Total tests found: {len(self._tests)}')
        print(f'Total tests run: {passed_tests + failed_tests}')
        print(f'Tests passed: {passed_tests}')
        print(f'Tests failed: {failed_tests}')

        print('Test durations:')
        for result in results:
            if result['result'] == 'SUCCESS':
                print(f'\t{result["name"]}: {round(result["duration"],2)}s')

        print('Test errors:')
        for result in results:
            if result['error']:
                print(f'\t{result["name"]}: {result["error"]}')
