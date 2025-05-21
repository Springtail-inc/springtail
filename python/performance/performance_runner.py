import sys
import os
import yaml
import argparse
import traceback
import json

# Get the parent directory of the current script (i.e., the project root directory)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the /shared directory to the Python path
sys.path.append(os.path.join(project_root, 'shared'))
# Add the /testing directory to the Python path
sys.path.append(os.path.join(project_root, 'testing'))

import springtail
from common import merge_json

from run_performance_benchmark import run_performance_benchmark

def parse_arguments() -> argparse.Namespace:
    """
    Parse command line arguments.

    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(description="Run performance benchmark")
    parser.add_argument('-s', '--suite', type=str, default="basic", help='Specific performance suite to run')
    parser.add_argument('-c', '--config-file', type=str, default="performance_config.yaml", help='Path to the performance configuration file')

    return parser.parse_args()

def run_performance_suite(config_file: str):
    config = yaml.safe_load(open(config_file))
    sys_config_file = config['system_json_path']
    base_config_file = config['base_config_path']
    build_dir = config['build_dir']
    cases = config['cases']

    for case in cases:
        config_file = os.path.join('cases', case, 'load_config.yaml')

        with open(base_config_file, 'r') as f:
            default_config = yaml.safe_load(f)

        with open(config_file, 'r') as f:
            case_config = yaml.safe_load(f)

        merged_config = merge_json(default_config, case_config)

        print(f"Running performance with config_file: {config_file}")

        # make sure Springtail is stopped
        print('Stopping any existing Springtail instance')
        springtail.stop(sys_config_file, do_cleanup=True)

        # start Springtail
        print('Starting the Springtail instance')
        springtail.start(sys_config_file, build_dir, do_cleanup=False, do_init=True, postgres_only=False, do_fdw_install=False)

        # Run the performance suite
        run_performance_benchmark(merged_config)

        # stop Springtail
        print('Stopping the Springtail instance')
        springtail.stop(sys_config_file)

if __name__ == "__main__":
    args = parse_arguments()
    run_performance_suite(args.config_file)
