import logging
import os
import sys
import yaml
import argparse
import glob

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
    makedir
)
from component import Component
from component_factory import ComponentFactory

PG_REGRESS_PATH = 'test/regress/pg_regress'

class Test:
    def __init__(self,
                 config_file: str,
                 build_dir: str,
                 bin_dir: str,
                 external_dir: str):
        """Initialize the test runner"""
        self._config_file = config_file
        self._build_dir = build_dir
        self._bin_dir = bin_dir
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

        # get the path to the pg_regress binary
        # pgxs looks like: /usr/lib/postgresql/12/lib/pgxs/src/makefiles/pgxs.mk
        pgxs = run_command('/usr/bin/pg_config', ['--pgxs'])
        # remove last two components of the path
        pgxs = os.path.join(*os.path.split(pgxs)[:-2])
        self._pg_regress = os.path.join(pgxs, PG_REGRESS_PATH)

        sql_files = glob.glob(os.path.join(external_dir, 'vcpkg/buildtrees/libpq/src/*/src/test/regress/sql/*.sql'))

        expected_files = glob.glob(os.path.join(external_dir, 'vcpkg/buildtrees/libpq/src/*/src/test/regress/expected/*.out'))

        data_files = glob.glob(os.path.join(external_dir, 'vcpkg/buildtrees/libpq/src/*/src/test/regress/data/*.data'))

        # create the sql, expected and data directories
        makedir('regress/sql')
        makedir('regress/expected')
        makedir('regress/data')

        # create the results directory
        self._result_path = os.path.join(os.getcwd(), 'regress/results')
        makedir(self._result_path);

        # remove any existing symlinks
        for f in os.listdir('regress/sql'):
            os.remove(os.path.join('sql', f))

        for f in os.listdir('regress/expected'):
            os.remove(os.path.join('expected', f))

        for f in os.listdir('regress/data'):
            os.remove(os.path.join('data', f))

        # create symlinks to the sql, expected and data files
        for file in sql_files:
            os.symlink(file, os.path.join('regress/sql', os.path.basename(file)))

        for file in expected_files:
            os.symlink(file, os.path.join('regress/expected', os.path.basename(file)))

        for file in data_files:
            os.symlink(file, os.path.join('regress/data', os.path.basename(file)))

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

    def run_regress(self):
        """Run the regression tests"""
        # remove all files in result directory
        for f in os.listdir(self._result_path):
            os.remove(os.path.join(self._result_path, f))

        # make sure Springtail is stopped
        logging.debug('Stopping any existing Springtail instance')
        springtail.stop(self._config_file, do_cleanup=True)

        # start Springtail
        logging.debug('Starting the Springtail instance')
        springtail.start(self._config_file, self._build_dir, do_cleanup=False)

        # start the proxy
        logging.debug('Starting the proxy')
        factory = ComponentFactory(self._bin_dir, self._props.get_pid_path())
        proxy = factory.create_proxy()
        if proxy.is_running():
            if not proxy.shutdown():
                raise ValueError("Failed to stop the proxy")
        if not proxy.start():
            raise ValueError("Failed to start the proxy")

        # run the regression tests
        logging.info('Running the regression tests')
        os.environ['PGPASSWORD'] = self._primary_pass
        run_command(self._pg_regress, [f'--dbname={self._primary_dbname}',
                                       f'--expecteddir={self._regress_path}/expected',
                                       f'--inputdir={self._regress_path}/sql',
                                       f'--host=localhost',
                                       f'--port={self._proxy_config["port"]}',
                                       f'--user={self._primary_user}',
                                       f'--schedule={self._regress_path}/parallel_schedule',
                                       '--max-connections=1',
                                       '--use-existing'])


def parse_arguments():
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

    test = Test(yaml_config['system_json_path'], yaml_config['build_dir'], yaml_config['external_dir'])

