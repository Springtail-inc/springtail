# NOTE THIS IS CURRENTLY BROKEN

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
    parser = argparse.ArgumentParser(description="Run tests, or start/stop/kill components. Component names: xid_mgr_daemon, sys_tbl_mgr_daemon, gc_daemon, postgres, ddl_daemon")

    # Add arguments -f for config file and -b for build directory
    parser.add_argument('-c', '--config-file', type=str, default='config.yaml', help='Path to the configuration file')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--start', type=str, help='Start the component <name>')
    parser.add_argument('--shutdown', type=str, help='Shutdown the component <name>')
    parser.add_argument('--isrunning', type=str, help='Is the component running <name>')
    parser.add_argument('--kill', type=str, help='Kill the component <name>')
    parser.add_argument('--test', action='store_true', help='Run tests <name>')
    parser.add_argument('--stopall', action='store_true', help='Stop all ingest components (except postgres)')

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


def run_tests(factory: ComponentFactory) -> None:
    """Run tests for all components"""
    # Create components and test if they are running

    pg_xid_subscriber_daemon = factory.create_pg_xid_subscriber_daemon()
    test(pg_xid_subscriber_daemon)
    assert not sys_tbl_mgr_daemon.is_running()

    sys_tbl_mgr_daemon = factory.create_sys_tbl_mgr_daemon()
    test(sys_tbl_mgr_daemon)
    assert not sys_tbl_mgr_daemon.is_running()


    postgres = factory.create_postgres()
    test(postgres)
    assert not postgres.is_running()

    postgres.start()
    assert postgres.is_running()
    postgres.create_user('test_user', 'test_password', True, True)

    ddl_daemon = factory.create_ddl_daemon('test_user', 'test_password')
    test(ddl_daemon)
    assert not ddl_daemon.is_running()

    xid_mgr_daemon.kill()
    assert not xid_mgr_daemon.is_running()


if __name__ == "__main__":
    # Configure logging
    args = parse_arguments()

    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s.%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                        datefmt='%Y-%m-%d:%H:%M:%S',)

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

    # Run tests
    if args.test:
        run_tests(factory)
        sys.exit(0)

    if args.stopall:
        # Stop all components
        sys_tbl_mgr_daemon = factory.create_sys_tbl_mgr_daemon()
        if not sys_tbl_mgr_daemon.shutdown() and not sys_tbl_mgr_daemon.kill():
            raise ValueError("Failed to stop sys_tbl_mgr_daemon")
        pg_xid_subscriber_daemon = factory.create_pg_xid_subscriber_daemon()
        if not pg_xid_subscriber_daemon.shutdown() and not pg_xid_subscriber_daemon.kill():
            raise ValueError("Failed to stop pg_xid_subscriber_daemon")
        ddl_daemon = factory.create_ddl_daemon('test_user', 'test_password')
        if not ddl_daemon.shutdown() and not ddl_daemon.kill():
            raise ValueError("Failed to stop ddl_daemon")
        xid_mgr_daemon = factory.create_xid_mgr_daemon()
        if not xid_mgr_daemon.shutdown() and not xid_mgr_daemon.kill():
            raise ValueError("Failed to stop xid_mgr_daemon")
        sys.exit(0)

    # Start/stop/kill components
    # Get the component name
    component_name = None
    if args.start or args.shutdown or args.isrunning or args.kill:
        component_name = args.start or args.shutdown or args.isrunning or args.kill

    if component_name is None:
        raise ValueError("Component name not provided")

    # Create the component
    component = None
    if component_name == 'xid_mgr_daemon':
        component = factory.create_xid_mgr_daemon()
    elif component_name == 'sys_tbl_mgr_daemon':
        component = factory.create_sys_tbl_mgr_daemon()
    elif component_name == 'postgres':
        component = factory.create_postgres()
    elif component_name == 'ddl_daemon':
        component = factory.create_ddl_daemon('test_user', 'test_password')
    elif component_name == 'proxy':
        component = factory.create_proxy()
    else:
        raise ValueError(f"Invalid component name: {component_name}")

    # Perform the requested action
    if args.start:
        if not component.start():
            raise ValueError(f"Failed to start {component_name}")
    elif args.shutdown:
        if not component.shutdown():
            raise ValueError(f"Failed to shutdown {component_name}")
    elif args.isrunning:
        if component.is_running():
            print(f"{component_name} is running")
        else:
            print(f"{component_name} is not running")
    elif args.kill:
        if not component.kill():
            raise ValueError(f"Failed to kill {component_name}")
