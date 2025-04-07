import logging
from lxml import etree
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
                 build_dir: str,
                 test_params: dict,
                 test_files: list[str] = []) -> None:
        """Initialize the test set"""
        self._directory = directory
        self._config_file = config_file
        self._build_dir = build_dir
        self._props = springtail.Properties(config_file, True)
        self._test_params = test_params
        self._name = os.path.splitext(os.path.basename(self._config_file))[0] + ' - ' + os.path.basename(self._directory)

        # constuct the special "config" test case for global setup and cleanup
        self._config = TestCase(os.path.join(directory, _GLOBAL_CONFIG_FILE), self._props, self._build_dir, self._test_params, ['setup', 'cleanup'])
        self._config.parse_file()

        # collect and parse the test cases from the directory
        self._test_files = [ ]
        self._tests = { }
        for test_file in sorted(os.listdir(directory)):
            # skip the test set configuration file
            if test_file == _GLOBAL_CONFIG_FILE:
                continue
            logging.info(f'Processing test file {test_file}')

            # test files must be of the form "<name>.sql"
            if not test_file.endswith('.sql'):
                logging.warning(f'skipped test file {test_file} -- must have the ".sql" extension')
                continue

            try:
                # parse the test
                self._tests[test_file] = TestCase(os.path.join(directory, test_file), self._props, self._build_dir, self._test_params)
                self._tests[test_file].parse_file()

                # if only a subset of test cases was requsted, limit them here
                if test_files and test_file not in test_files:
                    logging.warning(f'skipping test file {test_file} -- not in the requested tests')
                    self._tests[test_file].skip()
                else:
                    self._test_files.append(test_file)

            except Exception as e:
                logging.error(f'Error parsing test -- {e}')
                pass # this test was recorded as an error and we continue


    def _apply_replica_full(self) -> None:
        table_sql = """SELECT nspname::text, relname::text
                         FROM pg_catalog.pg_class
                         JOIN pg_catalog.pg_namespace ON relnamespace=pg_namespace.oid
                         LEFT OUTER JOIN pg_catalog.pg_index ON indrelid=pg_class.oid
                        WHERE relkind = 'r'
                          AND nspname NOT LIKE 'pg_%'
                          AND nspname != 'information_schema'
                          AND pg_index.indexrelid IS NULL
                        ORDER BY pg_class.oid"""
        primary_name = self._props.get_db_configs()[0]['name']
        connection = springtail.connect_db_instance(self._props, primary_name)
        with connection.cursor() as cursor:
            # retrieve the list of tables without primary keys
            cursor.execute(table_sql)
            results = cursor.fetchall()

            # apply REPLICA IDENITFY FULL to each
            for row in results:
                logging.debug(f'ALTER TABLE "{row[0]}"."{row[1]}" REPLICA IDENTITY FULL')
                cursor.execute(f'ALTER TABLE "{row[0]}"."{row[1]}" REPLICA IDENTITY FULL')
        connection.commit()
        connection.close()


    def run(self,
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

        # apply the REPLICA IDENTITY FULL to any tables without primary keys
        self._apply_replica_full()

        # start Springtail
        logging.debug('Starting the Springtail instance')
        springtail.start(self._config_file, self._build_dir, do_cleanup=False, do_init=True)

        # run the tests
        logging.info(f'Run the tests: {self._test_files}')

        test_failed = False
        for test_file in self._test_files:
            if test_file not in self._tests:
                logging.warning(f'unable to find test: {test_file}')
                continue
            logging.debug(f'Running the test: {test_file}')

            # start capturing the logs
            self._tests[test_file].start_capture()

            # run the actual test
            try:
                self._tests[test_file].test()
                self._tests[test_file].verify()

            except Exception as e:
                logging.error(f'Error: exception: [{e}] result: {self._tests[test_file].get_result()["result"]}')
                if self._tests[test_file].get_result()['result'] == 'FAILED':
                    test_failed = True
                else:
                    logging.info(f'Skipping the test: {test_file}')
                    self._tests[test_file].skip()

            # save the logs
            self._tests[test_file].stop_capture()

            # if we should stop the tests, break the loop
            if test_failed and not shutdown_on_fail:
                springtail.check_logs(self._config_file)
                break

            # try to perform cleanup
            try:
                self._tests[test_file].cleanup()
            except Exception as e:
                logging.error(f'Error on cleanup: {e}')

            # check here if the test failed nad we need to check the logs
            if test_failed:
                springtail.check_logs(self._config_file)
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


    def report(self) -> bool:
        """Generates a report about the test set"""
        results = [ self._tests[t].get_result() for t in self._tests ]
        passed_tests = sum(1 for r in results if r['result'] == 'SUCCESS')
        failed_tests = sum(1 for r in results if r['result'] == 'FAILED' or r['result'] == 'ERROR')
        skipped_tests = sum(1 for r in results if r['result'] == 'SKIPPED')

        print('\n')
        print(f'--- Test Summary: {self._name} ---')
        print(f'Total tests found: {len(self._tests)}')
        print(f'Total tests run: {passed_tests + failed_tests}')
        print(f'Tests passed: {passed_tests}')
        print(f'Tests failed: {failed_tests}')
        print(f'Tests skipped: {skipped_tests}')

        print('Test durations:')
        for result in results:
            if result['result'] == 'SUCCESS':
                print(f'\t{result["name"]}: {round(result["duration"],2)}s')

        print('Test errors:')
        for result in results:
            if result['error']:
                print(f'\t{result["name"]}: {result["error"]}')

        # returns True if no failed tests
        return (failed_tests == 0)


    def junit(self) -> etree.Element:
        """Generate a JUnit <testsuite> report and return it."""
        results = [ self._tests[t].get_result() for t in self._tests ]
        passed_tests = sum(1 for r in results if r['result'] == 'SUCCESS')
        failed_tests = sum(1 for r in results if r['result'] == 'FAILED')

        suite = etree.Element('testsuite',
                              name=self._name,
                              tests=f'{len(results)}',
                              failures=f'{failed_tests}')

        for test_file in self._tests:
            suite.append(self._tests[test_file].junit())

        return suite
