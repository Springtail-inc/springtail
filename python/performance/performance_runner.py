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
    parser.add_argument('-d', '--do-cleanup', action='store_true', help='Clean up previous run data')

    return parser.parse_args()

def run_performance_suite(config_file: str, do_cleanup: bool = False):
    """
    Run the performance suite.

    Args:
        config_file (str): Path to the performance configuration file
        do_cleanup (bool): Whether to clean up previous run data
    """
    config = yaml.safe_load(open(config_file))
    sys_config_file = config['system_json_path']
    base_config_file = config['base_config_path']
    build_dir = config['build_dir']
    cases = config['cases']

    print(f"Running performance with config_file: {config_file}")
    print(f"Using system_json_path: {sys_config_file}")
    print(f"Using build_dir: {build_dir}")

    # Override the log rotation enabled to false
    log_rotation_enabled_orig = True
    with open(sys_config_file, 'r') as f:
        system_config = json.load(f)
        log_rotation_enabled_orig = system_config['logging']['log_rotation_enabled']
        system_config['logging']['log_rotation_enabled'] = False

    with open(sys_config_file, 'w') as f:
        json.dump(system_config, f, indent=2)

    try:
        for case in cases:
            config_file = os.path.join('cases', case, 'load_config.yaml')

            with open(base_config_file, 'r') as f:
                default_config = yaml.safe_load(f)

            with open(config_file, 'r') as f:
                case_config = yaml.safe_load(f)

            merged_config = merge_json(default_config, case_config)

            # make sure Springtail is stopped
            print('Stopping any existing Springtail instance')
            springtail.stop(sys_config_file, do_cleanup=True)

            # start Springtail
            print('Starting the Springtail instance')
            springtail.start(sys_config_file, build_dir, do_cleanup=False, do_init=True, postgres_only=False, do_fdw_install=True)

            # Run the performance suite
            run_performance_benchmark(merged_config, do_cleanup)

            # stop Springtail
            print('Stopping the Springtail instance')
            springtail.stop(sys_config_file)
    except Exception as e:
        print(f"Error running performance suite: {e}")
        raise
    finally:
        # Restore the log rotation enabled
        with open(sys_config_file, 'r') as f:
            system_config = json.load(f)
            system_config['logging']['log_rotation_enabled'] = log_rotation_enabled_orig

        with open(sys_config_file, 'w') as f:
            json.dump(system_config, f, indent=2)

if __name__ == "__main__":
    args = parse_arguments()
    run_performance_suite(args.config_file, args.do_cleanup)
