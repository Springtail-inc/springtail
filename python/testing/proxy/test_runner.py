import logging
import os
import sys
import yaml
import argparse
import glob
import shutil
import datetime
import time
import subprocess
import threading

# Get the parent directory of the current script (i.e., the project root directory)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the /shared directory to the Python path
sys.path.append(os.path.join(project_root, '../shared'))
sys.path.append(os.path.join(project_root, '../coordinator'))
sys.path.append(os.path.join(project_root, './'))

import springtail

from properties import Properties
from common import (
    run_command,
    makedir,
    execute_sql,
    execute_sql_script,
    running_pids
)
from sysutils import (
    restart_postgres
)

from component_factory import ComponentFactory
from component import Component

PG_REGRESS_PATH = 'pgxs/src/test/regress/pg_regress'

REGRESSION_DB = 'regression'

class Test:
    def __init__(self,
                 config_file: str,
                 install_dir: str,
                 external_dir: str):
        """Initialize the test runner"""
        self._config_file = config_file
        self._install_dir = os.path.abspath(install_dir)
        self._external_dir = os.path.abspath(external_dir)

        self._props = springtail.Properties(config_file, True)

        # get the primary db info
        db_configs = self._props.get_db_configs()
        self._primary_dbname = db_configs[0]['name']

        db_instance = self._props.get_db_instance_config()
        self._primary_user = db_instance['replication_user']
        self._primary_pass = db_instance['password']
        self._primary_port = db_instance['port']
        self._primary_host = db_instance['host']

        # get the proxy configuration
        self._proxy_config = self._props.get_proxy_config()

        # add the regression database to redis
        self._props.add_database(REGRESSION_DB)
        # set db state to running so it can be used by the proxy
        self._props.set_db_state(REGRESSION_DB, 'running')

        # get the path to the pg_regress binary
        # pkglibdir looks like: /usr/lib/postgresql/16/lib
        pkg_libdir = run_command('/usr/bin/pg_config', ['--pkglibdir']).strip()
        self._pg_regress = os.path.join(pkg_libdir, PG_REGRESS_PATH)

        # copy the libregress.so from fdw build dir to pkg_libdir
        libregress_so = os.path.join(self._install_dir, 'lib/libregress.so')
        run_command('sudo', ['cp', libregress_so, os.path.join(pkg_libdir, 'regress.so')])

    def allocate_regress_dir(self) -> None:
        """Allocate the regression directory"""
        # create the regression directory
        current_date = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        self._regress_path = os.path.join('/tmp', f'regress_{current_date}')
        makedir(self._regress_path, '777')

        logging.info(f'Created regression directory: {self._regress_path}')

        # set the other paths based on the regress path
        self._sql_path = os.path.join(self._regress_path, 'sql')
        self._expected_path = os.path.join(self._regress_path, 'expected')
        self._data_path = os.path.join(self._regress_path, 'data')
        self._result_path = os.path.join(self._regress_path, 'results')


    def setup_regress_files(self) -> None:
        """Setup the regression files.
        This function will copy the sql, expected and data files from the external libpq build.
        """
        # create the sql, expected and data directories
        makedir(os.path.join(self._sql_path))
        makedir(os.path.join(self._expected_path))
        makedir(os.path.join(self._data_path))

        # create the results directory
        makedir(self._result_path, '777');

        # fetch the sql, expected and data files from the external libpq build
        sql_files = glob.glob(os.path.join(os.getcwd(), 'tests/sql/*.sql'))
        expected_files = glob.glob(os.path.join(os.getcwd(), 'tests/sql/*.out'))
        data_files = glob.glob(os.path.join(os.getcwd(), 'tests/data/*.data'))

        # create symlinks to the sql and expected files
        for file in sql_files:
            shutil.copy(file, os.path.join(self._sql_path, os.path.basename(file)))

        for file in expected_files:
            shutil.copy(file, os.path.join(self._expected_path, os.path.basename(file)))

        # copy the data files
        for file in data_files:
            shutil.copy(file, os.path.join(self._data_path, os.path.basename(file)))

        # copy the schedule files
        schedule_files = glob.glob(os.path.join(os.getcwd(), 'tests/schedules/*'))
        for f in schedule_files:
            shutil.copy(f, os.path.join(self._regress_path, os.path.basename(f)))


    def reset_db(self) -> None:
        """Reset the database"""
        restart_postgres()

        # connect and drop the tablespace
        logging.debug('Connecting to the primary db')
        primary_conn = springtail.connect_db_instance(self._props, "postgres")
        execute_sql(primary_conn, f'DROP TABLESPACE IF EXISTS regress_tblspace')
        execute_sql(primary_conn, f'DROP DATABASE IF EXISTS {REGRESSION_DB} WITH (FORCE)')
        execute_sql(primary_conn, f'CREATE DATABASE {REGRESSION_DB}')
        primary_conn.close()

        # load the trigger functions
        primary_conn = springtail.connect_db_instance(self._props, REGRESSION_DB)
        parent_dir = os.path.dirname(self._install_dir)
        trigger_sql = os.path.join(parent_dir, 'scripts/triggers.sql')
        execute_sql_script(primary_conn, trigger_sql)
        primary_conn.close()

        primary_conn = springtail.connect_db_instance(self._props, "springtail")
        parent_dir = os.path.dirname(self._install_dir)
        trigger_sql = os.path.join(parent_dir, 'scripts/triggers.sql')
        execute_sql_script(primary_conn, trigger_sql)
        primary_conn.close()


    def extract_options(self, schedule : str) -> dict:
        """Extract the options from the schedule file"""
        options = {}
        with open(schedule, 'r') as f:
            line = f.readline()
            if line.startswith('# options: '):
                line = line.replace('# options: ', '')
                options = { x.split('=')[0].strip() : x.split('=')[1].strip() for x in line.split(',') }

        if 'timeout' in options:
            try:
                options['timeout'] = float(options['timeout'])
            except ValueError as e:
                options['timeout'] = 60
                pass

        return options


    def run_regress_cmd(self, port : int,
                        schedule : str,
                        test_files : list[str],
                        suffix : str,
                        stop_on_error : bool = True) -> None:
        """Run the regression test"""
        def monitor(process, timeout):
            process.wait(timeout=timeout)
            if process.poll() is None:  # Process is still running
                print(f"Timeout reached ({timeout}s). Terminating process...")
                process.terminate()

        # remove all files in result directory
        for f in os.listdir(self._result_path):
            os.remove(os.path.join(self._result_path, f))

        # remove old regression files
        for f in ['regression.out', 'regression.diffs']:
            if os.path.exists(os.path.join(self._regress_path, f)):
                os.remove(os.path.join(self._regress_path, f))

        # extract options from the schedule file
        options = {}
        if schedule:
            schedule_path = os.path.join(self._regress_path, schedule)
            options = self.extract_options(schedule_path)

        if self._notimeout:
            timeout = None
        elif 'timeout' in options:
            timeout = options['timeout']
        else:
            timeout = 60

        # set up the run
        os.environ['PGPASSWORD'] = self._primary_pass
        args = [self._pg_regress,
                f'--dbname={REGRESSION_DB}',
                f'--inputdir={self._regress_path}',
                f'--host=localhost',
                f'--port={port}',
                f'--user={self._primary_user}',
                '--max-connections=1',
                '--use-existing']

        if schedule:
            args += [f'--schedule={schedule_path}']

        args += test_files

        logging.info(self._pg_regress + ' ' + ' '.join(args))
        logging.info(f"Timeout: {timeout} seconds")

        # run the regression tests
        not_ok_count = 0
        ok_count = 0
        process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, cwd=self._regress_path)
        monitor_thread = threading.Thread(target=monitor, args=(process, timeout))
        monitor_thread.start()

        for line in iter(process.stdout.readline, ''):
            print(line, end='')
            if 'not ok' in line:
                not_ok_count += 1
                if stop_on_error:
                    process.terminate()
                    break

            elif 'ok' in line:
                ok_count += 1

        monitor_thread.join()

        return_code = process.returncode

        if return_code == 0:  # all tests passed, return
            logging.info(f'Regression tests completed successfully: {ok_count}')
            return

        if len(os.listdir(self._result_path)) == 0:
            logging.error(f'No result files found in the results directory')
            raise ValueError("No result files found in the results directory")

        # check for errors from logs
        logging.info(f'Regression tests failed: {not_ok_count} / {(not_ok_count + ok_count)}')
        logging.info(f'Results can be found in {os.path.join(self._regress_path, 'regression.diff.proxy.out')}')

        # rename regression output files
        os.rename(os.path.join(self._regress_path, 'regression.out'), os.path.join(self._regress_path, 'regression.out.' + suffix))
        os.rename(os.path.join(self._regress_path, 'regression.diffs'), os.path.join(self._regress_path, 'regression.diff.' + suffix))


    def start_proxy(self, manual_proxy : bool = False) -> None:
        """Start the proxy"""
        # start the proxy
        logging.debug('Starting the proxy')

        # override the proxy type to 'primary'
        os.environ['SPRINGTAIL_PROPERTIES'] = 'proxy.mode=primary'

        # remove logs from previous runs
        if os.path.exists(os.path.join(self._props.get_log_path(), 'proxy.log')):
            os.remove(os.path.join(self._props.get_log_path(), 'proxy.log'))

        # start proxy
        if manual_proxy:
            # wait for user input before continuing
            while True:
                print('\nPress enter once proxy is started and running:')
                input()
                (pids, not_running) = running_pids(['proxy'])
                if not pids:
                    print("Can't find running proxy process")
                    break
                else:
                    break
        else:
            # create proxy component
            factory = ComponentFactory(os.path.join(self._install_dir, 'bin/system'), self._props.get_pid_path())
            proxy = factory.create_proxy()

            if proxy.is_running():
                if not proxy.shutdown():
                    raise ValueError("Failed to stop the proxy")

            if not proxy.start():
                raise ValueError("Failed to start the proxy")

            # wait for the proxy to start
            time.sleep(2)


    def run_regress(self,
                    schedule: str,
                    test_files: list[str] = [],
                    manual_proxy: bool = False,
                    notimeout: bool = False) -> None:
        """Run the regression tests"""
        # connect and drop the tablespace
        self.reset_db()

        # allocate the regression directory and set the paths
        self.allocate_regress_dir()

        # setup the regression files
        self.setup_regress_files()

        self._notimeout = notimeout

        # run the regression tests first against normal postgres
        self.run_regress_cmd(self._primary_port, schedule, test_files, 'pg.out', False)

        # rename the expected dir
        os.rename(self._expected_path, self._expected_path + '.pg')

        # rename the results dir to the expected dir
        os.rename(self._result_path, self._expected_path)

        # recreate the results dir
        makedir(self._result_path, '777')

        # reset the database
        self.reset_db()

        # start the proxy
        self.start_proxy(manual_proxy)

        # run the regression tests against the proxy
        logging.info('Running the regression tests against the proxy')

        self.run_regress_cmd(self._proxy_config['port'], schedule, test_files, 'proxy.out', True)


    def cleanup(self):
        """Cleanup the regression directory"""
        logging.info(f'Cleaning up regression directory: {self._regress_path}')
        run_command('sudo', ['rm', '-rf', self._regress_path])


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Run Springtail Proxy tests")
    parser.add_argument('-c', '--config', type=str, default='config.yaml', help='Path to the test configuration file')
    parser.add_argument('-m', '--manual', action='store_true', default=False, help='Run the proxy manually')
    parser.add_argument('-s', '--schedule', type=str, default=None, help='Path to the schedule file')
    parser.add_argument('-l', '--list', action='store_true', default=False, help='List all schedules')
    parser.add_argument('-t', '--notimeout', action='store_true', default=False, help='Disable timeouts')
    parser.add_argument('test_files', nargs="*", help="Individual test sql files (without .sql).")

    return parser.parse_args()

## main()
if __name__ == "__main__":
    # parse the command line arguments
    args = parse_arguments()

    if args.list:
        # list all the schedules
        schedules = glob.glob(os.path.join(os.getcwd(), 'tests/schedules/*'))
        for s in schedules:
            print(os.path.basename(s))
        sys.exit(0)

    if not args.schedule and not args.test_files:
        raise ValueError("No schedule or test files specified")

    if args.schedule and not os.path.exists(os.path.join(os.getcwd(), f'tests/schedules/{args.schedule}')):
        raise ValueError(f"Schedule file not found: {args.schedule}")

    # set the log level and format
    handlers = []
    handlers.append(logging.StreamHandler(sys.stdout))
    handlers.append(logging.FileHandler(os.path.join(os.getcwd(), 'proxy_regress.log')))

    logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                        datefmt='%Y-%m-%d:%H:%M:%S',
                        level=logging.DEBUG,
                        handlers=handlers)

    # parse the test configuration
    with open(args.config, 'r') as f:
        yaml_config = yaml.safe_load(f)

    test = Test(yaml_config['system_json_path'], yaml_config['install_dir'], yaml_config['external_dir'])

    try:
        test.run_regress(args.schedule, args.test_files, args.manual, args.notimeout)
    except Exception as e:
        logging.error(f'Failed to run the regression tests: {e}')
        # cleanup the regression tmp dir on exception
        test.cleanup()
        raise e



