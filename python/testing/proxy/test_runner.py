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
    def __init__(self, config_file: str, build_dir: str, external_dir: str):
        self._config_file = config_file
        self._build_dir = build_dir
        self._external_dir = external_dir

        self._props = springtail.Properties(config_file, True)

        fdw_config = self._props.get_fdw_config()
        db_configs = self._props.get_db_configs()
        self._primary_name = db_configs[0]['name']
        if 'db_prefix' in fdw_config:
            self._replica_name = fdw_config['db_prefix'] + self._primary_name
        else:
            self._replica_name = self._primary_name

        self._proxy_config = self._props.get_proxy_config()

        # get the path to the pg_regress binary
        # pgxs looks like: /usr/lib/postgresql/12/lib/pgxs/src/makefiles/pgxs.mk
        pgxs = run_command('/usr/bin/pg_config', ['--pgxs'])
        # remove last two components of the path
        pgxs = os.path.join(*os.path.split(pgxs)[:-2])
        self._pg_regress = os.path.join(pgxs, PG_REGRESS_PATH)

        sql_files = glob.glob(os.path.join(external_dir, 'vcpkg/buildtrees/libpq/src/*/src/test/regress/sql/*.sql'))

        expected_files = glob.glob(os.path.join(external_dir, 'vcpkg/buildtrees/libpq/src/*/src/test/regress/expected/*.out'))

        # create the sql directory and the expected directory
        makedir('sql')
        makedir('expected')

        # remove any existing symlinks
        for f in os.listdir('sql'):
            os.remove(os.path.join('sql', f))

        for f in os.listdir('expected'):
            os.remove(os.path.join('expected', f))

        for sql_file in sql_files:
            os.symlink(sql_file, os.path.join('sql', os.path.basename(sql_file)))

    def run(self):
        # make sure Springtail is stopped
        logging.debug('Stopping any existing Springtail instance')
        springtail.stop(self._config_file, do_cleanup=True)

        # start Springtail
        logging.debug('Starting the Springtail instance')
        springtail.start(self._config_file, self._build_dir, do_cleanup=False)




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

