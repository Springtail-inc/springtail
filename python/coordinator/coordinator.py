import os
import sys
import yaml
import logging
import argparse
import string
import signal
import time
import threading
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
from scheduler import Scheduler, CoordinatorState
from production import Production

# import the xid_mgr_client
from xid_mgr import XidMgrClient
from sys_tbl_mgr import SysTblMgrClient

from otel_logger import init_logging

class Coordinator:
    """The Coordinator class to manage the components of the system."""

    def __init__(self,
                 props: Properties,
                 debug: bool,
                 is_production: bool,
                 install_path: str,
                 service_name: str):
        """
        Initialize the Coordinator.
        Arguments:
            props -- the properties object
            debug -- the debug flag
            is_production -- the production flag
            install_path -- the installation path
            service_name -- the name of the service
        """
        self.props = props
        self._check_properties(props)
        self.shutdown_event = threading.Event()

        # Configure logging
        init_logging(props.get_otel_config(), props.get_log_path(), debug)
        self.logger = logging.getLogger("coordinator")

        # Get the service type
        self.service_name = service_name
        if not self.service_name:
            self.service_name = os.environ.get('SERVICE_NAME')
            if not self.service_name:
                self.logger.error("Service name not provided")
                raise ValueError("Service name not provided")

        # Check the service name
        if not self.service_name in ['ingestion', 'fdw', 'proxy']:
            self.logger.error(f"Invalid service name: {self.service_name}")
            raise ValueError(f"Invalid service name: {self.service_name}")

        # Check the install path
        if not install_path or not os.path.exists(install_path):
            self.logger.error(f"Invalid install path: {install_path}")
            raise ValueError(f"Invalid install path: {install_path}")

        self.install_path = install_path

        # Check the properties for production
        self.production = None
        if is_production:
            self.logger.debug("Checking properties for production")
            self.production = Production(self.install_path)


    def startup(self):
        """
        Start the coordinator.

        Blocks while running.
        """
        state = self.props.get_coordinator_state()
        self.logger.info(f"Coordinator state: {state}")

        if self.production:
            # Send SNS message
            self.production.send_sns('startup')

            # Install binaries
            try:
                if state == CoordinatorState.STARTUP:
                    self.logger.debug("Installing binaries")
                    self.production.install_binaries()
            except Exception as e:
                raise ValueError("Failed to install binaries: " + str(e))

        # Get the installation path and setup bin dir
        self.bin_dir = os.path.join(self.install_path, 'bin/system')
        if not os.path.exists(self.bin_dir):
            self.logger.error(f"Invalid binary directory: {self.bin_dir}")
            raise ValueError(f"Invalid binary directory: {self.bin_dir}")

        # Create scheduler
        self.logger.debug("Starting scheduler")
        self.scheduler = Scheduler(self.props, self.service_name, self.production)

        # Create component factory
        self.logger.debug(f"Creating component factory with bin_dir={self.bin_dir}")
        factory = ComponentFactory(self.bin_dir, props.get_pid_path())

        # Register components
        self.logger.info(f"Starting {self.service_name} service")

        match self.service_name:
            case "ingestion":
                self.scheduler.register_component(factory.create_xid_mgr_daemon(), 1)
                self.scheduler.register_component(factory.create_sys_tbl_mgr_daemon(), 2)
                self.scheduler.register_component(factory.create_log_mgr_daemon(), 3)

            case "fdw":
                try:
                    if self.production:
                        self.production.install_pgfdw()
                except Exception as e:
                    raise ValueError("Failed to install postgres_fdw: " + str(e))

                # startup postgres if not running
                postgres = factory.create_postgres()
                if not postgres.is_running():
                    postgres.start()

                # create the ddl user
                ddl_password = self._gen_random_string(16)
                postgres.create_user('ddl_user', ddl_password, True, True)

                # in test startup ingestion services
                if not self.production:
                    self.xid_mgr_component = factory.create_xid_mgr_daemon()
                    self.sys_tlb_mgr_component = factory.create_sys_tbl_mgr_daemon()
                    self.xid_mgr_component.start()
                    self.sys_tlb_mgr_component.start()

                # wait for ingestion to be ready
                self._wait_for_ingestion(self.props)

                self.scheduler.register_component(postgres, 3)
                self.scheduler.register_component(factory.create_ddl_daemon('ddl_user', ddl_password), 4)

            case "proxy":
                self.scheduler.register_component(factory.create_proxy(), 1)

            case _:
                self.logger.error(f"Invalid service type: {self.service_name}")
                if self.production:
                    self.production.send_sns('shutdown')
                raise ValueError(f"Invalid service type: {self.service_name}; must be one of: ingestion, fdw, proxy")

        # Start all components
        if not self.scheduler.start_all():
            self.logger.error("Failed to start all components")
            if self.production:
                self.production.send_sns('shutdown')
            raise ValueError("Failed to start all components")

        self.logger.info("All components started successfully")

        # Monitor for timeouts (this could be in a separate thread)
        # this will exit on a SIGINT or SIGTERM
        self.logger.debug("Scheduler entering monitor loop")
        self.scheduler.monitor_timeouts()

        # shutdown all components
        self.logger.info("Shutting down all components")
        self.scheduler.shutdown()

        if self.production:
            self.production.send_sns('shutdown')

    def shutdown(self, signum: int):
        """
        Shutdown the coordinator.
        """
        # set shutdown flag
        self.shutdown_event.set()

        # shutdown scheduler
        if self.scheduler:
            self.logger.info(f"Received signal {signum}, shutting down...")
            self.scheduler.shutdown()

        # if not in production, shutdown the xid_mgr and sys_tbl_mgr
        if not self.production and self.service_name == 'fdw':
            if self.xid_mgr_component:
                self.xid_mgr_component.shutdown()
            if self.sys_tlb_mgr_component:
                self.sys_tlb_mgr_component.shutdown()

    def _check_properties(self, props: Properties) -> None:
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


    def _gen_random_string(self, length: int) -> str:
        """
        Generate a random string of the specified length.
        Arguments:
            length -- the length of the string
        Returns:
            a random string of the specified length
        """
        return ''.join(SystemRandom().choice(string.ascii_letters + string.digits) for _ in range(length))

    def _wait_for_ingestion(self, props: Properties) -> None:
        """
        Wait for the ingestion service to be ready.
        """
        self.logger.debug("Waiting for ingestion service to be ready")
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
        while waiting and not self.shutdown_event.is_set():
            try:
                with XidMgrClient(host, xid_port) as client:
                    client.ping()
                    self.logger.info("XidManager is ready")
                    waiting = False
            except Exception as e:
                continue

        waiting = True
        while waiting and not self.shutdown_event.is_set():
            try:
                with SysTblMgrClient(host, sys_tbl_port) as client:
                    client.ping()
                    self.logger.info("SysTblManager is ready")
                    waiting = False
            except Exception as e:
                continue
        self.logger.info("Ingestion service is ready")

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

    return props


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

    coordinator = Coordinator(props, args.debug, yaml_config.get('production'), yaml_config.get('install_dir'), args.service)

    # Set up signal handlers
    def signal_handler(signum, frame):
        coordinator.shutdown(signum)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    coordinator.startup()

