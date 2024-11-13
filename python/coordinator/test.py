import sys
import os
import argparse
import logging
import yaml
import string
from random import SystemRandom

from component import Component
from component_factory import ComponentFactory

# Get the parent directory of the current script (i.e., the project root directory)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the /shared directory to the Python path
sys.path.append(os.path.join(project_root, 'shared'))

# Now import the Properties class
from properties import Properties

def parse_arguments():
    """Parse the command line arguments."""
    # Create the argument parser
    parser = argparse.ArgumentParser(description="Process command-line arguments for config file and build directory.")

    # Add arguments -f for config file and -b for build directory
    parser.add_argument('-c', '--config-file', type=str, default='config.yaml', help='Path to the configuration file')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')

    # Parse the arguments and return them
    args = parser.parse_args()
    return args

def test(component : Component) -> None:
    """Test if the component is running"""
    if component.is_running():
        print(f"Component {component.name} is running")
        if not component.shutdown():
            print(f"Failed to stop {component.name}")
            return

    print(f"Component {component.name} is not running")
    if not component.start():
        print(f"Failed to start {component.name}")
        return
    print(f"Component {component.name} should be running")
    if not component.is_running():
        print(f"Failed to start {component.name}")
        return
    print(f"Component {component.name} is running")
    if not component.shutdown():
        print(f"Failed to stop {component.name}, trying kill")
        if not component.kill():
            print(f"Failed to kill {component.name}")
            return
    else:
        print(f"Component {component.name} should be stopped")
        if component.is_running():
            print(f"Failed to stop {component.name}")
            if not component.kill():
                print(f"Failed to kill {component.name}")
                return

    print(f"Component {component.name} should now be stopped")
    if component.is_running():
        print(f"Component {component.name} is still running")
        return
    print(f"SUCCESS Component {component.name} is stopped")


def gen_random_string(length: int) -> str:
    """Generate a random string of the specified length."""
    return ''.join(SystemRandom().choice(string.ascii_letters + string.digits) for _ in range(length))


if __name__ == "__main__":
    # Configure logging
    args = parse_arguments()

    logging.basicConfig(level=logging.DEBUG)

    if not os.path.exists(args.config_file):
        raise ValueError(f"Config file not found: {args.config_file}")

    with open(args.config_file, 'r') as f:
        yaml_config = yaml.safe_load(f)

    # Load properties from config file if provided;
    # otherwise assume environment variables
    props = None
    config_file = yaml_config.get('system_json_path')
    if not config_file or not os.path.exists(config_file):
        raise ValueError(f"System JSON file not found: {config_file}")
    props = Properties(config_file)

    # Sanity check the properties
    if not props:
        raise ValueError("Failed to load properties")

    # Create component factory
    bin_dir = os.path.join(yaml_config.get('install_dir'), 'bin/system')
    if not os.path.exists(bin_dir):
        raise ValueError(f"Invalid binary directory: {bin_dir}")
    factory = ComponentFactory(bin_dir, props.get_pid_path())

    # Create components and test if they are running
    xid_mgr_daemon = factory.create_xid_mgr_daemon()
    test(xid_mgr_daemon)
    assert not xid_mgr_daemon.is_running()

    xid_mgr_daemon.start()
    assert xid_mgr_daemon.is_running()

    write_cache_daemon = factory.create_write_cache_daemon()
    test(write_cache_daemon)
    assert not write_cache_daemon.is_running()

    sys_tbl_mgr_daemon = factory.create_sys_tbl_mgr_daemon()
    test(sys_tbl_mgr_daemon)
    assert not sys_tbl_mgr_daemon.is_running()

    gc_daemon = factory.create_gc_daemon()
    test(gc_daemon)
    assert not gc_daemon.is_running()

    postgres = factory.create_postgres()
    test(postgres)
    assert not postgres.is_running()

    postgres.start()
    assert postgres.is_running()
    postgres.create_user('test_user', 'test_password', True, True)

    ddl_daemon = factory.create_ddl_daemon('test_user', 'test_password')
    test(ddl_daemon)
    assert not ddl_daemon.is_running()

