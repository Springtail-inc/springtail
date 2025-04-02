import os
import sys
import logging
import multiprocessing

# Get the parent directory of the current script (i.e., the project root directory)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the /shared directory to the Python path
sys.path.append(os.path.join(project_root, 'shared'))

# import the Properties class
from properties import Properties
from common import (
    run_command
)
from production import Production
from scheduler import CoordinatorState
from otel_logger import init_logging

class Loader(multiprocessing.Process):
    """
    This class is responsible for reloading the coordinator python files and restaring the coordinator.
    """

    def __init__(self, install_path: str, project_root: str):
        """
        Initialize the Loader with properties.
        """
        super().__init__(daemon=False)
        self.project_root = project_root
        self.install_path = install_path

    def run(self):
        """
        Reload the coordinator and restart the coordinator, called from start().
        """
        # re-init properties and logging
        props = Properties(self.install_path)
        init_logging(props.get_otel_config(), props.get_log_path());
        logger = logging.getLogger('springtail')
        prod = Production(self.install_path)

        # set state to reload
        props.set_coordinator_state(CoordinatorState.RELOADING)

        # make sure that python code exists in the install dir
        if not os.path.exists(os.path.join(self.install_path, 'python')):
            logger.error("Python code does not exist in the install directory.")
            prod.send_sns('coordinator_reload_failed')
            return

        logger.info("Re-installing coordinator")

        # try to reload the coordinator, recovery[] is a list of commands to run in case of failure
        recovery = []
        try:
            logger.info("Stopping coordinator...")
            run_command("sudo", ["systemctl", "stop", "springtail-coordinator"])
            recovery.append(["sudo", ["systemctl", "start", "springtail-coordinator"]])

            logger.info("Backing up coordinator files...")
            run_command("sudo", ["mv", self.project_root, self.project_root + ".bak"])
            recovery.append(["sudo", ["mv", self.project_root + ".bak", self.project_root]])

            logger.info("Copying coordinator files...")
            run_command("sudo", ["cp", "-r", os.path.join(self.install_path, 'python'), self.project_root])
            recovery.append(["sudo", ["rm -rf", self.project_root]])

            logger.info("Restarting coordinator...")
            run_command("sudo", ["systemctl", "start", "springtail-coordinator"])

            logger.info("Coordinator reloaded successfully. Cleaning up...")
            run_command("sudo", ["rm", "-rf", self.project_root + ".bak"])

            logger.info("Cleanup completed successfully.")

        except Exception as e:
            logger.error(f"Error during reloading coordinator: {e}")
            prod.send_sns('coordinator_reload_failed')

            # try and recover from the error
            for command in reversed(recovery):
                try:
                    logger.info(f"Recovering from error: {command}")
                    run_command(*command)
                except Exception as e:
                    logger.error(f"Error during recovery: {e}")

        props.wait_for_coordinator_state(CoordinatorState.RUNNING)