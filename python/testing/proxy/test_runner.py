import logging
import os
import sys
import yaml
import argparse
import glob
import shutil

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
    execute_sql
)
from sysutils import (
    check_postgres_running,
    start_postgres,
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
        self._install_dir = install_dir
        self._external_dir = external_dir

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
        springtail.add_db_to_redis(self._props, REGRESSION_DB)

        # get the path to the pg_regress binary
        # pkglibdir looks like: /usr/lib/postgresql/16/lib
        pkg_libdir = run_command('/usr/bin/pg_config', ['--pkglibdir']).strip()
        self._pg_regress = os.path.join(pkg_libdir, PG_REGRESS_PATH)

        # copy the libregress.so from fdw build dir to pkg_libdir
        libregress_so = os.path.join(self._install_dir, 'lib/libregress.so')
        run_command('sudo', ['cp', libregress_so, os.path.join(pkg_libdir, 'regress.so')])

        # fetch the sql, expected and data files from the external libpq build
        sql_files = glob.glob(os.path.join(external_dir, 'vcpkg/buildtrees/libpq/src/*/src/test/regress/sql/*.sql'))
        expected_files = glob.glob(os.path.join(external_dir, 'vcpkg/buildtrees/libpq/src/*/src/test/regress/expected/*.out'))
        data_files = glob.glob(os.path.join(external_dir, 'vcpkg/buildtrees/libpq/src/*/src/test/regress/data/*.data'))

        # remove any existing regress directory
        run_command('sudo', ['rm', '-rf', 'regress'])

        # create the sql, expected and data directories
        self._expected_path = os.path.join(os.getcwd(), 'regress/expected')
        makedir(os.path.join(os.getcwd(), 'regress/sql'))
        makedir(os.path.join(self._expected_path))
        makedir(os.path.join(os.getcwd(), 'regress/data'))

        # create the results directory
        self._result_path = os.path.join(os.getcwd(), 'regress/results')
        makedir(self._result_path, '777');

        # remove any existing symlinks
        for f in os.listdir('regress/sql'):
            os.remove(os.path.join('sql', f))

        for f in os.listdir('regress/expected'):
            os.remove(os.path.join('expected', f))

        for f in os.listdir('regress/data'):
            os.remove(os.path.join('data', f))

        # create symlinks to the sql and expected files
        for file in sql_files:
            os.symlink(file, os.path.join('regress/sql', os.path.basename(file)))

        for file in expected_files:
            os.symlink(file, os.path.join('regress/expected', os.path.basename(file)))

        # copy the data files
        for file in data_files:
            shutil.copy(file, os.path.join('regress/data', os.path.basename(file)))

        # copy the resultmap and the schedule files
        schedule_file = glob.glob(os.path.join(external_dir, 'vcpkg/buildtrees/libpq/src/*/src/test/regress/parallel_schedule'))
        resultmap_file = glob.glob(os.path.join(external_dir, 'vcpkg/buildtrees/libpq/src/*/src/test/regress/resultmap'))

        for f in schedule_file:
            os.symlink(f, os.path.join('regress', os.path.basename(f)))

        for f in resultmap_file:
            os.symlink(f, os.path.join('regress', os.path.basename(f)))

        # set the environment variables expected by pg_regress sql files
        self._regress_path = os.path.join(os.getcwd(), 'regress')
        os.environ['PG_ABS_SRCDIR'] = self._regress_path
        os.environ['PG_ABS_BUILDDIR'] = self._regress_path
        # PG_LIBDIR, PG_DLSUFFIX


    def reset_db(self) -> None:
        """Reset the database"""
        # connect and drop the tablespace
        logging.debug('Connecting to the primary db')
        primary_conn = springtail.connect_db_instance(self._props, "postgres")
        execute_sql(primary_conn, f'DROP TABLESPACE IF EXISTS regress_tblspace')
        execute_sql(primary_conn, f'DROP DATABASE IF EXISTS {REGRESSION_DB}')
        execute_sql(primary_conn, f'CREATE DATABASE {REGRESSION_DB}')
        primary_conn.close()


    def run_regress_cmd(self, port : int, file_out : str) -> None:
        """Run the regression test"""
        # run the regression tests first against normal postgres
        logging.info('Running the regression tests: output to ' + file_out)

        # remove all files in result directory
        for f in os.listdir(self._result_path):
            os.remove(os.path.join(self._result_path, f))

        # remove old regression files
        for f in ['regression.out', 'regression.diffs']:
            os.remove(os.path.join(self._regress_path, f))

        os.environ['PGPASSWORD'] = self._primary_pass
        run_command(self._pg_regress, [f'--dbname={REGRESSION_DB}',
                                       f'--inputdir={self._regress_path}',
                                       f'--host=localhost',
                                       f'--port={port}',
                                       f'--user={self._primary_user}',
                                       f'--schedule={os.path.join(self._regress_path, 'parallel_schedule')}',
                                       '--max-connections=5',
                                       '--use-existing'],
                    os.path.join(self._regress_path, file_out))


    def start_proxy(self) -> None:
        """Start the proxy"""
        # start the proxy
        logging.debug('Starting the proxy')

        # override the proxy type to 'primary'
        os.environ['SPRINGTAIL_PROPERTIES', 'proxy.mode=primary']

        factory = ComponentFactory(self._install_dir, self._props.get_pid_path())
        proxy = factory.create_proxy()
        if proxy.is_running():
            if not proxy.shutdown():
                raise ValueError("Failed to stop the proxy")
        if not proxy.start():
            raise ValueError("Failed to start the proxy")


    def run_regress(self) -> None:
        """Run the regression tests"""

        # make sure postgres is running
        if not check_postgres_running():
            start_postgres(self._props)
            if not check_postgres_running():
                raise ValueError("Failed to start postgres")

        # connect and drop the tablespace
        self.reset_db()

        # run the regression tests first against normal postgres
        self.run_regress_cmd(self._primary_port, 'pg.out')

        # rename the expected dir
        os.rename(self._expected_path, self._expected_path + '.pg')

        # rename the results dir to the expected dir
        os.rename(self._result_path, self._expected_path)

        # rename regression output files
        os.rename(os.path.join(self._regress_path, 'regression.out'), os.path.join(self._regress_path, 'regression.out.pg'))
        os.rename(os.path.join(self._regress_path, 'regression.diff'), os.path.join(self._regress_path, 'regression.diff.pg'))

        # recreate the results dir
        makedir(self._result_path, '777')

        # reset the database
        self.reset_db()

        # start the proxy
        # self.start_proxy()

        # run the regression tests against the proxy
        logging.info('Running the regression tests against the proxy')
        self.run_regress(self._primary_port, 'proxy.out')
        # self.run_regress(self._proxy_config['port'], 'proxy.out')


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Run Springtail Proxy tests")
    parser.add_argument('-c', '--config', type=str, default='config.yaml', help='Path to the test configuration file')
    return parser.parse_args()

## main()
if __name__ == "__main__":
    # parse the command line arguments
    args = parse_arguments()

    # parse the test configuration
    with open(args.config, 'r') as f:
        yaml_config = yaml.safe_load(f)

    test = Test(yaml_config['system_json_path'], yaml_config['install_dir'], yaml_config['external_dir'])

    test.run_regress()



