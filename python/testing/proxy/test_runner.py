import logging
import os
import sys
import yaml
import argparse

# Get the parent directory of the current script (i.e., the project root directory)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the /shared directory to the Python path
sys.path.append(os.path.join(project_root, '../shared', '../'))

import springtail

class Test:
    def __init__(self, config_file: str, build_dir: str):
        self._config_file = config_file
        self._build_dir = build_dir
        self._result = None

        self._props = springtail.Properties(config_file, True)

        fdw_config = self._props.get_fdw_config()
        db_configs = self._props.get_db_configs()
        self._primary_name = db_configs[0]['name']
        if 'db_prefix' in fdw_config:
            self._replica_name = fdw_config['db_prefix'] + self._primary_name
        else:
            self._replica_name = self._primary_name

    def run(self):
        # make sure Springtail is stopped
        logging.debug('Stopping any existing Springtail instance')
        springtail.stop(self._config_file, do_cleanup=True)




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

    test = Test(yaml_config['system_json_path'], yaml_config['build_dir'])

