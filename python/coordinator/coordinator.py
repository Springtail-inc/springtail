import os
import sys
import yaml
import logging
import argparse
import string
import signal
import time
from typing import Optional
from random import SystemRandom

# Get the parent directory of the current script (i.e., the project root directory)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the /shared directory to the Python path
sys.path.append(os.path.join(project_root, 'shared'))
sys.path.append(os.path.join(project_root, 'grpc'))

# import the Properties class
from properties import Properties

# import the ComponentFactory class and the Scheduler class
from component_factory import ComponentFactory
from scheduler import Scheduler

# import the xid_mgr_client
from xid_mgr import XidMgrClient
from sys_tbl_mgr import SysTblMgrClient

# import production utils
from production import (
    install_binaries,
    install_pgfdw,
    send_sns
)

def check_properties(props: Properties) -> None:
    """
    Check the properties; check paths exist.
    Arguments:
        props -- the properties object
    """
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

    # Parse the arguments and return them
    args = parser.parse_args()
    return args


def gen_random_string(length: int) -> str:
    """
    Generate a random string of the specified length.
    Arguments:
        length -- the length of the string
    Returns:
        a random string of the specified length
    """
    return ''.join(SystemRandom().choice(string.ascii_letters + string.digits) for _ in range(length))


def setup_props(yaml_config: dict) -> Properties:
    """
    Load properties from the config file or environment variables.
    Arguments:
        yaml_config -- the YAML configuration
    """
    # Load properties from config file if provided;
    # otherwise assume environment variables
    props = None
    config_file = yaml_config.get('system_json_path')
    if config_file is None:
        # default to using environment variables
        props = Properties()
    else:
        if not config_file or not os.path.exists(config_file):
            raise ValueError(f"System JSON file not found: {config_file}")
        props = Properties(config_file)

    # Sanity check the properties
    if not props:
        raise ValueError("Failed to load properties")

    check_properties(props)

    return props


def wait_for_ingestion(props: Properties) -> None:
    """
    Wait for the ingestion service to be ready.
    """
    host = None
    while True:
        host = props.get_hostname('ingestion')
        if host is not None:
            break
        time.sleep(1)

    system_config = props.get_system_config()
    xid_port = system_config['xid_mgr']['rpc_config']['server_port']
    sys_tbl_port = system_config['sys_tbl_mgr']['rpc_config']['server_port']

    waiting = True
    while waiting:
        try:
            with XidMgrClient(host, xid_port) as client:
                client.ping()
                logger.info("XidManager is ready")
                waiting = False
        except Exception as e:
            continue

    waiting = True
    while waiting:
        try:
            with SysTblMgrClient(host, sys_tbl_port) as client:
                client.ping()
                logger.info("SysTblManager is ready")
                waiting = False
        except Exception as e:
            continue


if __name__ == "__main__":
    """Main entry point for the coordinator script."""
    # Parse the command line arguments
    args = parse_arguments()

    if not os.path.exists(args.config_file):
        raise ValueError(f"Config file not found: {args.config_file}")

    # Load the yaml configuration file
    with open(args.config_file, 'r') as f:
        yaml_config = yaml.safe_load(f)

    # Load properties from the config file
    props = setup_props(yaml_config)

    # Configure logging
    log_path = props.get_log_path()

    handlers = []
    # Add a file handler to the logger for stdout and file output
    if args.debug:
        handlers.append(logging.StreamHandler(sys.stdout))
    handlers.append(logging.FileHandler(os.path.join(log_path, 'coordinator.log')))

    logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                        datefmt='%Y-%m-%d:%H:%M:%S',
                        level=logging.DEBUG if args.debug else logging.INFO,
                        handlers=handlers)

    logger = logging.getLogger("Coordinator")

    # Get the service type
    service_name = args.service
    if not service_name:
        service_name = os.environ.get('SERVICE_NAME')
        if not service_name:
            raise ValueError("Service type not provided")

    send_sns('startup')

    # get install path
    install_path = yaml_config.get('install_dir')

    # Check the properties for production
    production = False
    if yaml_config.get('production'):
        logger.debug("Checking properties for production")
        production = True

        # Install binaries
        try:
            install_binaries(install_path)
        except Exception as e:
            raise ValueError("Failed to install binaries: " + str(e))


    # Create scheduler
    logger.debug("Starting scheduler")
    scheduler = Scheduler(props, service_name, production)

    # Set up signal handlers
    def signal_handler(signum, frame):
        if scheduler:
            logger.info(f"Received signal {signum}, shutting down...")
            scheduler.shutdown()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Create component factory
    bin_dir = os.path.join(yaml_config.get('install_dir'), 'bin/system')
    if not os.path.exists(bin_dir):
        raise ValueError(f"Invalid binary directory: {bin_dir}")

    logger.debug(f"Creating component factory with bin_dir={bin_dir}")
    factory = ComponentFactory(bin_dir, props.get_pid_path())

    # Register components
    logger.debug(f"Starting {service_name} service")

    if service_name == "ingestion":
        scheduler.register_component(factory.create_xid_mgr_daemon(), 1)
        scheduler.register_component(factory.create_sys_tbl_mgr_daemon(), 2)
        scheduler.register_component(factory.create_gc_daemon(), 3)
        scheduler.register_component(factory.create_log_mgr_daemon(), 4)

    elif service_name == "fdw":
        try:
            install_pgfdw(install_path)
        except Exception as e:
            raise ValueError("Failed to install postgres_fdw: " + str(e))

        # startup postgres if not running
        postgres = factory.create_postgres()
        if not postgres.is_running():
            postgres.start()

        # create the ddl user
        ddl_password = gen_random_string(16)
        postgres.create_user('ddl_user', ddl_password, True, True)

        # wait for ingestion to be ready
        wait_for_ingestion(props)

        # For testing uncomment lines below since they are needed for ddl daemon
        # but in production they should be running elsewhere
        # scheduler.register_component(factory.create_xid_mgr_daemon(), 1)
        # scheduler.register_component(factory.create_sys_tbl_mgr_daemon(), 2)
        scheduler.register_component(postgres, 3)
        scheduler.register_component(factory.create_ddl_daemon('ddl_user', ddl_password), 4)

    elif service_name == "proxy":
        scheduler.register_component(factory.create_proxy(), 1)

    else:
        raise ValueError(f"Invalid service type: {service_name}; must be one of: ingestion, fdw, proxy")

    # Start all components
    if not scheduler.start_all():
        logger.error("Failed to start all components")
        send_sns('shutdown')
        raise ValueError("Failed to start all components")

    logger.info("All components started successfully")

    # Monitor for timeouts (this could be in a separate thread)
    # this will exit on a SIGINT or SIGTERM
    logger.debug("Scheduler entering monitor loop")
    scheduler.monitor_timeouts()

    # shutdown all components
    logger.debug("Shutting down all components")
    scheduler.shutdown_all()

    send_sns('shutdown')