import logging
import springtail
import sysutils

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
    def __init__(self, directory: str, config_file: str, build_dir: str) -> None:
        """Initialize the test set"""
        self._directory = directory
        self._config_file = config_file
        self._build_dir = build_dir
        self._props = springtail.Properties(config_file, True)

        # constuct the special "config" test case for global setup and cleanup
        self._config = TestCase(os.path.join(directory, "config"), self._props, ['setup', 'cleanup'])

        # collect and parse the test cases from the directory
        self._test_files = sorted(os.listdir(directory))
        self._tests = { }
        for test_file in self._test_files:
            if test_file == "config":
                continue

            # test files must be of the form "<name>.sql"
            if not test_file.endswith('.sql'):
                logging.warning(f'skipped test file {test_file} -- must have the ".sql" extension')
                continue

            self._tests[test_file] = TestCase(os.path.join(directory, test_file), self._props)


    def run(self, test_files: list = None) -> None:
        """Runs one or more of the test cases in the test set in the
        provided order.  If no test cases are provided then it runs
        all of the tests in lexographical order.

        """
        # make sure Springtail is stopped
        logging.debug('Stopping any existing Springtail instance')
        sysutils.stop_daemons(self._props.get_pid_path(), springtail.ALL_DAEMONS_NAMES)
        springtail.cleanup_db_instance(self._props)

        # perform the primary db setup
        logging.debug('Perform the global setup()')
        self._config.setup()

        # start Springtail
        logging.debug('Starting the Springtail instance')
        springtail.start_replication(self._props, self._build_dir)
        sysutils.start_daemons(self._build_dir, springtail.CORE_DAEMONS)
        springtail.wait_for_running(self._props)
        springtail.fdw_import(self._props, self._build_dir, self._config_file)
        springtail.fixup_log_perms(self._props)

        # run the tests
        logging.debug('Run the tests')
        if test_files is None:
            test_files = self._test_files

        stop_tests = False
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
                stop_tests = True

            # try to perform cleanup
            try:
                self._tests[test_file].cleanup()
            except Exception as e:
                logging.error(f'Error on cleanup: {e}')

            # if requested, check the logs
            if check_logs:
                test_case.check_logs()

            # if we should stop the tests, break the loop
            if stop_tests:
                break

        # shutdown Springtail
        logging.debug('Stopping the Springtail instance')
        sysutils.stop_daemons(self._props.get_pid_path(), springtail.ALL_DAEMONS_NAMES)
        # note: maybe don't clean up in case we need to debug anything
        # springtail.cleanup_db_instance(self._props)

        # perform the primary db cleanup
        logging.debug('Perform the global cleanup()')
        self._config.cleanup()


    def report(self) -> dict:
        """Generates a report about the test set"""
        pass
