import os
import sys
import yaml
import logging
import argparse
import string
from random import SystemRandom

# Get the parent directory of the current script (i.e., the project root directory)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the /shared directory to the Python path
sys.path.append(os.path.join(project_root, 'shared'))

# import the Properties class
from properties import Properties

# import the ComponentFactory class and the Scheduler class
from component_factory import ComponentFactory
from scheduler import Scheduler

def check_properties(props: Properties) -> None:
    """Check the properties; check paths exist."""
    mount_path = props.get_mount_path()
    log_path = props.get_log_path()

    # check mount path exists
    if not mount_path or not os.path.exists(mount_path):
        raise ValueError(f"Invalid mount path: {mount_path}")

    # check log path exists
    if not log_path or not os.path.exists(log_path):
        raise ValueError(f"Invalid log path: {log_path}")

    # check log path is writable
    if not ((os.stat(log_path).st_mode & 0o777) & 0o002):
        raise ValueError(f"Log path is not writable: {log_path}")


def parse_arguments():
    """Parse the command line arguments."""
    # Create the argument parser
    parser = argparse.ArgumentParser(description="Process command-line arguments for config file and build directory.")

    # Add arguments -f for config file and -b for build directory
    parser.add_argument('-c', '--config-file', type=str, default='config.yaml', help='Path to the configuration file')
    parser.add_argument('-s', '--service', type=str, required=False, help='Name of the service: ingestion, fdw, or proxy')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--env', action='store_true', help='Use environment variables instead of config file')

    # Parse the arguments and return them
    args = parser.parse_args()
    return args


def gen_random_string(length: int) -> str:
    """Generate a random string of the specified length."""
    return ''.join(SystemRandom().choice(string.ascii_letters + string.digits) for _ in range(length))


if __name__ == "__main__":
    # Configure logging
    args = parse_arguments()

    if not os.path.exists(args.config_file):
        raise ValueError(f"Config file not found: {args.config_file}")

    with open(args.config_file, 'r') as f:
        yaml_config = yaml.safe_load(f)

    # Load properties from config file if provided;
    # otherwise assume environment variables
    props = None
    if args.env:
        props = Properties()
    else:
        config_file = yaml_config.get('system_json_path')
        if not config_file or not os.path.exists(config_file):
            raise ValueError(f"System JSON file not found: {config_file}")
        props = Properties(config_file)

    # Sanity check the properties
    if not props:
        raise ValueError("Failed to load properties")
    check_properties(props)

    # Configure logging
    log_path = props.get_log_path()
    logging.basicConfig(filename=os.path.join(log_path, 'coordinator.log'),
                        level=logging.DEBUG if args.debug else logging.INFO)

    # Get the service type
    service_type = args.service
    if service_type is None:
        service_type = os.environ.get('SERVICE_TYPE')

    # Create scheduler
    scheduler = Scheduler(props)

    # Create component factory
    bin_dir = os.path.join(yaml_config.get('install_dir'), 'bin/system')
    if not os.path.exists(bin_dir):
        raise ValueError(f"Invalid binary directory: {bin_dir}")
    factory = ComponentFactory(bin_dir, props.get_pid_path())

    # Register components
    if service_type == "ingestion":
        scheduler.register_component(factory.create_xid_mgr_daemon(), 1)
        scheduler.register_component(factory.create_write_cache_daemon(), 2)
        scheduler.register_component(factory.create_sys_tbl_mgr_daemon(), 3)
        scheduler.register_component(factory.create_gc_daemon(), 4)
        scheduler.register_component(factory.create_log_mgr_daemon(), 5)
    elif service_type == "fdw":
        # startup postgres if not running
        postgres = factory.create_postgres()
        if not postgres.is_running():
            postgres.start()

        # create the ddl user
        ddl_password = gen_random_string(16)
        postgres.create_user('ddl_user', ddl_password, True, True)

        scheduler.register_component(postgres, 1)
        scheduler.register_component(factory.create_ddl_daemon('ddl_user', ddl_password), 2)
    elif service_type == "proxy":
        scheduler.register_component(factory.create_proxy(), 1)
    else:
        raise ValueError(f"Invalid service type: {service_type}; must be one of: ingestion, fdw, proxy")

    # Start all components
    if scheduler.start_all():
        print("All components started successfully")
    else:
        print("Failed to start all components")

    # Monitor for timeouts (this could be in a separate thread)
    scheduler.monitor_timeouts()
