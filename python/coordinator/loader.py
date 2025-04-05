import os
import sys
import logging
import multiprocessing
import shutil
import tempfile
from typing import Optional

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

BACKUP_FILE = 'stc_py_backup.tgz'
NEW_FILE = 'stc_py_new.tgz'

class Loader(multiprocessing.Process):
    """
    This class is responsible for reloading the coordinator python files and restaring the coordinator.
    """

    def __init__(self, install_path: str, project_root: str):
        """
        Initialize the Loader

        Args:
            install_path (str): Path to the installation directory.
            project_root (str): Path to the project root directory.
        """
        super().__init__()
        self.daemon = False
        self.project_root = project_root
        self.install_path = install_path
        print(f"Loader initialized with install path: {self.install_path}, pid: {os.getpid()}")


    def find_and_copy_file(self, src_dir: str, file_name: str, dest_dir: str) -> Optional[tuple[str, str]]:
        """
        Recursively searches for a file in src_dir, copies it to dest_dir.

        Args:
            src_dir (str): Source directory to search in.
            file_name (str): Name of the file to search for.
            dest_dir (str): Destination directory to copy the file to.
        Returns:
            Optional[tuple[str, str]]: Tuple of original path and destination path if found, else None.
        """
        for root, _, files in os.walk(src_dir):
            if file_name in files:
                original_path = os.path.join(root, file_name)
                dest_path = os.path.join(dest_dir, file_name)

                # Ensure destination directory exists
                os.makedirs(dest_dir, exist_ok=True)

                # Copy the file
                shutil.copy2(original_path, dest_path)

                return (original_path, dest_path)  # Return original path, destination path

        return None  # Return None if file not found


    def run(self):
        """
        Reload the coordinator and restart the coordinator, called from start().
        """
        print(f"In run {os.getpid()}")
        # re-init properties and logging
        props = Properties()
        init_logging(props.get_otel_config(), props.get_log_path());
        logger = logging.getLogger('springtail')
        prod = Production(self.install_path)

        logger.info("Re-installing coordinator")

        # set state to reload
        props.set_coordinator_state(CoordinatorState.RELOADING)

        # make sure that python code exists in the install dir
        if not os.path.exists(os.path.join(self.install_path, 'python')):
            logger.error("Python code does not exist in the install directory.")
            prod.send_sns('coordinator_reload_failed')
            return

        # try to reload the coordinator, recovery[] is a list of commands to run in case of failure
        recovery = []
        try:
            # make a temp directory to copy the python code to
            logger.info("Creating temporary directory for python code...")
            temp_dir = tempfile.mkdtemp()
            recovery.append(["sudo", ["rm", "-rf", temp_dir]])

            # archive new python code
            logger.info("Archiving new python code...")
            new_file = os.path.join(temp_dir, NEW_FILE)
            run_command("sudo", ["tar", "cfz", new_file, "-C", os.path.join(self.install_path, 'python'), "."])

            # find config.yaml and copy it to tmp
            logger.info("Finding and copying config.yaml...")
            config_files = self.find_and_copy_file(self.project_root, 'config.yaml', temp_dir)

            if not config_files:
                raise FileNotFoundError("config.yaml not found in the project root.")

            logger.info("Stopping coordinator...")
            run_command("sudo", ["systemctl", "stop", "springtail-coordinator"])
            recovery.append(["sudo", ["systemctl", "start", "springtail-coordinator"]])

            logger.info("Backing up coordinator files...")
            backup_file = os.path.join(temp_dir, BACKUP_FILE)
            run_command("sudo", ["tar", "cfz", backup_file, self.project_root])

            logger.info("Deleting old coordinator files...")
            run_command("sudo", ["rm", "-rf", self.project_root])
            recovery.append(["sudo", ["tar", "xfz", backup_file, '-C', '/']])

            logger.info("Copying coordinator files...")
            run_command("sudo", ["mkdir", "-p", self.project_root])
            run_command("sudo", ["tar", "xfz", new_file, "-C", self.project_root])
            recovery.append(["sudo", ["rm", "-rf", self.project_root]])

            logger.info("Copying config.yaml back to the coordinator directory...")
            run_command("sudo", ["cp", config_files[1], config_files[0]])

            logger.info("Restarting coordinator...")
            run_command("sudo", ["systemctl", "start", "springtail-coordinator"])

            logger.info("Coordinator reloaded successfully. Cleaning up...")
            run_command("sudo", ["rm", "-rf", temp_dir])

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

        try:
            props.wait_for_coordinator_state(CoordinatorState.RUNNING)
            logger.info("Coordinator started successfully.")
        except Exception as e:
            logger.error(f"Error waiting for coordinator to start: {e}")
            prod.send_sns('coordinator_reload_failed')


def main():
    """
    Main function to start the Loader process.
    """
    install_path = '/opt/springtail'
    project_root = '/home/ubuntu/stc'

    loader = Loader(install_path, project_root)
    loader.start()


if __name__ == "__main__":
    main()