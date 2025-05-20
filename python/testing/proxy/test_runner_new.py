import json
import logging
import os
import sys
import yaml
import argparse
import glob
import shutil
import datetime
import subprocess
import threading

# Get the parent directory of the current script (i.e., the project root directory)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the /shared directory to the Python path
sys.path.append(os.path.join(project_root, './'))

import springtail

from properties import Properties
from common import (
    run_command,
    makedir,
    execute_sql,
    execute_sql_script
)

PG_REGRESS_PATH = 'pgxs/src/test/regress/pg_regress'

REGRESSION_DB = 'regression'

class Test:
    def __init__(self,
                 config_file: str,
                 build_dir: str) -> None:
        """Initialize the test runner"""
        self._config_file = config_file
        self._build_dir = os.path.abspath(build_dir)
        self._config_file_copy = config_file + ".copy"

        # read config json
        try:
            with open(config_file, 'r') as f:
                self._config = json.load(f)
        except FileNotFoundError:
            logging.error(f"Config file not found: {config_file}")
            raise
        except json.JSONDecodeError as e:
            logging.error(f"Invalid JSON in config file: {e}")
            raise

        # add regression database
        self._config["db_instances"]["1234"]["database_ids"] = ["1", "2"]
        self._config["databases"]["2"] = {
            "name": REGRESSION_DB,
            "replication_slot": REGRESSION_DB + "_slot",
            "publication_name": REGRESSION_DB + "_pub",
            "include": {
                "schemas": ["*"]
            }
        }

        # Write modified config to copy file
        with open(self._config_file_copy, 'w') as f:
            json.dump(self._config, f, indent=4)

        # get properties
        self._props = springtail.Properties(self._config_file_copy, False)

        # get instance config
        db_instance = self._props.get_db_instance_config()
        self._primary_user = db_instance['replication_user']
        self._primary_pass = db_instance['password']
        self._primary_port = db_instance['port']
        self._primary_host = db_instance['host']

        # get the proxy configuration
        self._proxy_config = self._props.get_proxy_config()

        self.restart()

    def restart(self) -> None:
        # stop daemon processes
        logging.debug("Stopping springtail processes")
        springtail.stop_daemons(self._props.get_pid_path(), springtail.ALL_DAEMONS_NAMES)

        # get the path to the pg_regress binary
        # pkglibdir looks like: /usr/lib/postgresql/16/lib
        logging.debug("Setting up path to pg_regress")
        pkg_libdir = run_command('/usr/bin/pg_config', ['--pkglibdir']).strip()
        self._pg_regress = os.path.join(pkg_libdir, PG_REGRESS_PATH)
        logging.debug("Path to pg_regress is %s", self._pg_regress)

        # copy the libregress.so from fdw build dir to pkg_libdir
        logging.debug("Copy pg_regress.so to %s", pkg_libdir)
        libregress_so = os.path.join(self._build_dir, 'src/pg_fdw/libregress.so')
        self._regress_lib_path = os.path.join(pkg_libdir, 'regress.so')
        run_command('sudo', ['cp', libregress_so, self._regress_lib_path])

        logging.debug('Starting the fdw instance')
        springtail.install_fdw(self._build_dir)

        logging.debug('Set up regression database')
        self.setup_regress_db()

        logging.debug('Starting the Springtail instance')
        springtail.start(self._config_file_copy, self._build_dir, do_cleanup=True, do_init=True, postgres_only=False, do_fdw_install=False)


    def setup_regress_db(self) -> None:
        """Setup database for regression testing"""

        # connect and drop the tablespace
        logging.debug('Connecting to the primary db')
        primary_conn = springtail.connect_db_instance(self._props, "postgres")
        execute_sql(primary_conn, f'DROP TABLESPACE IF EXISTS regress_tblspace')
        execute_sql(primary_conn, f'DROP DATABASE IF EXISTS {REGRESSION_DB} WITH (FORCE)')
        execute_sql(primary_conn, f'CREATE DATABASE {REGRESSION_DB}')
        primary_conn.close()

        # load the trigger functions
        primary_conn = springtail.connect_db_instance(self._props, REGRESSION_DB)
        parent_dir = os.path.dirname(self._build_dir)
        trigger_sql = os.path.join(parent_dir, 'scripts/triggers.sql')
        execute_sql_script(primary_conn, trigger_sql)
        primary_conn.close()

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
            timeout = 600

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

        logging.info(' '.join(args))
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
        logging.info(f'Results can be found in {os.path.join(self._regress_path, 'regression.diff.' + suffix)}')

        # rename regression output files
        os.rename(os.path.join(self._regress_path, 'regression.out'), os.path.join(self._regress_path, 'regression.out.' + suffix))
        os.rename(os.path.join(self._regress_path, 'regression.diffs'), os.path.join(self._regress_path, 'regression.diff.' + suffix))


    def run_diff(self, dir1, dir2, output_file):
        """
        Compare two directories and write differences to output file
        
        Args:
            dir1 (str): Path to first directory
            dir2 (str): Path to second directory
            output_file (str): Path to output file for diff results
        """
        logging.info(f'Comparing directories: {dir1} and {dir2}')

        # Check if directories exist
        if not os.path.exists(dir1):
            raise FileNotFoundError(f"Directory not found: {dir1}")
        if not os.path.exists(dir2):
            raise FileNotFoundError(f"Directory not found: {dir2}")

        # Run diff and capture both stdout and stderr
        result = subprocess.run(['diff', '-r', dir1, dir2],
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)

        # Log according to the return code
        if result.returncode == 0:
            logging.info(f'Directories {dir1} and {dir2} are identical')
        elif result.returncode == 1:
            logging.info(f'Directories {dir1} and {dir2} differ; diff output file: {output_file}')
            # Directories differ; write the output.
            output_lines = []
            if result.stderr:
                output_lines.append(result.stderr.decode())
            if result.stdout:
                output_lines.append(result.stdout.decode())
            full_output = "\n".join(output_lines)

            with open(output_file, 'w') as out:
                out.write(full_output)
                out.close()
        else:
            logging.error(f"Error running diff: {result.stderr.decode()}")
            error_msg = result.stderr or "Unknown error"
            raise subprocess.SubprocessError(f"diff command failed: {error_msg}")

    def run_regress(self,
                    schedule: str,
                    test_files: list[str] = [],
                    notimeout: bool = False) -> None:

        # allocate the regression directory and set the paths
        self.allocate_regress_dir()

        # setup the regression files
        self.setup_regress_files()

        self._notimeout = notimeout

        # run the regression tests first against normal postgres
        logging.info('Running the regression tests against postgres')
        self.run_regress_cmd(self._primary_port, schedule, test_files, 'pg.out', False)

        # check the log files
        check_logs_result = springtail.check_logs(self._config_file_copy)
        if len(check_logs_result) != 0:
            logging.error("There were some failures; exiting")
            sys.exit(1)

        # move result path
        pg_out_dir = self._result_path + ".pg.out"
        logging.info(f"Renaming {self._result_path} to {pg_out_dir}")
        os.rename(self._result_path, pg_out_dir)

        # recreate the results dir
        makedir(self._result_path, '777')

        # restart database and all the processes
        self.restart()

        # run the regression tests against the proxy
        logging.info('Running the regression tests against the proxy')
        self.run_regress_cmd(self._proxy_config['port'], schedule, test_files, 'proxy.out', False)

        # check the log files
        springtail.check_logs(self._config_file_copy)
        if len(check_logs_result) != 0:
            logging.error("There were some failures; exiting")
            sys.exit(1)

        # move result path
        proxy_out_dir = self._result_path + ".proxy.out"
        logging.info(f"Renaming {self._result_path} to {proxy_out_dir}")
        os.rename(self._result_path, proxy_out_dir)

        # Ensure file operations are complete by syncing the filesystem
        os.sync()

        # compare postgress and proxy runs
        out_path = os.path.join(self._regress_path, 'regression_result.diff.out')
        self.run_diff(pg_out_dir, proxy_out_dir, out_path)

    def cleanup(self):
        """Cleanup the regression directory"""
        logging.info(f'Cleaning up regression directory: {self._regress_path}')
        run_command('sudo', ['rm', '-rf', self._regress_path])
        logging.info(f'Cleaning up regression library: {self._regress_lib_path}')
        run_command('sudo', ['rm', self._regress_lib_path])


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Run Springtail Proxy tests")
    parser.add_argument('-c', '--config', type=str, default='config.yaml', help='Path to the test configuration file')
    parser.add_argument('-b', '--build-dir', type=str, required=True, help='Path to the build directory')
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

    test = Test(yaml_config['system_json_path'], args.build_dir)

    try:
        test.run_regress(args.schedule, args.test_files, args.notimeout)
    except Exception as e:
        logging.error(f'Failed to run the regression tests: {e}')
        # cleanup the regression tmp dir on exception
        test.cleanup()
        raise e
