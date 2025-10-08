import os
import sys
import logging
import shutil
import tempfile
import argparse
import subprocess
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

class Loader:
    """
    This class is responsible for reloading the coordinator python files and restaring the coordinator.
    """

    def __init__(self, install_path: str, project_root: str, postgres_pid_file: Optional[str] = None) -> None:
        """
        Initialize the Loader

        Args:
            install_path (str): Path to the installation directory.
            project_root (str): Path to the project root directory.
        """
        self.project_root = project_root
        self.install_path = install_path
        print(f"Loader initialized with install path: {self.install_path}, project_root: {self.project_root}, pid: {os.getpid()},"
              f" postgres_pid_file: {postgres_pid_file}")

        self.props = Properties()
        init_logging(self.props.get_otel_config(), self.props.get_log_path())
        self.logger = logging.getLogger('springtail')
        self.prod = Production(self.install_path, postgres_pid_file)


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


    def run(self) -> None:
        """
        Reload the coordinator and restart the coordinator, called from start().
        """
        self.logger.info("Re-installing coordinator")

        # set state to reload
        if (self.props.get_coordinator_state() != CoordinatorState.RELOADING):
            self.logger.info("Setting coordinator state to RELOADING")

        # make sure that python code exists in the install dir
        if not os.path.exists(os.path.join(self.install_path, 'python')):
            self.logger.error("Python code does not exist in the install directory.")
            self.prod.send_sns('coordinator_reload_failed')
            return

        # try to reload the coordinator, recovery[] is a list of commands to run in case of failure
        recovery = []
        try:
            # make a temp directory to copy the python code to
            self.logger.info("Creating temporary directory for python code...")
            temp_dir = tempfile.mkdtemp()
            recovery.append(["sudo", ["rm", "-rf", temp_dir]])

            # archive new python code
            self.logger.info("Archiving new python code...")
            new_file = os.path.join(temp_dir, NEW_FILE)
            run_command("sudo", ["tar", "cfz", new_file, "-C", os.path.join(self.install_path, 'python'), "."])

            # find config.yaml and copy it to tmp
            self.logger.info("Finding and copying config.yaml...")
            config_files = self.find_and_copy_file(self.project_root, 'config.yaml', temp_dir)

            if not config_files:
                raise FileNotFoundError("config.yaml not found in the project root.")

            self.logger.info("Stopping coordinator...")
            run_command("sudo", ["systemctl", "stop", "springtail-coordinator"])
            recovery.append(["sudo", ["systemctl", "start", "springtail-coordinator"]])

            self.logger.info("Backing up coordinator files...")
            backup_file = os.path.join(temp_dir, BACKUP_FILE)
            run_command("sudo", ["tar", "cfz", backup_file, self.project_root])

            self.logger.info("Deleting old coordinator files...")
            run_command("sudo", ["rm", "-rf", self.project_root + "/*"])
            recovery.append(["sudo", ["tar", "xfz", backup_file, '-C', '/']])

            self.logger.info("Copying coordinator files...")
            run_command("sudo", ["mkdir", "-p", self.project_root])
            run_command("sudo", ["tar", "xfz", new_file, "-C", self.project_root])
            recovery.append(["sudo", ["rm", "-rf", self.project_root]])

            self.logger.info("Copying config.yaml back to the coordinator directory...")
            run_command("sudo", ["cp", config_files[1], config_files[0]])

            self.logger.info("Restarting coordinator...")
            run_command("sudo", ["systemctl", "start", "springtail-coordinator"])

            self.logger.info("Coordinator reloaded successfully. Cleaning up...")
            run_command("sudo", ["rm", "-rf", temp_dir])

            self.logger.info("Cleanup completed successfully.")

            # wait for the coordinator to start; throws an exception if it fails
            self.props.wait_for_coordinator_state(CoordinatorState.RUNNING)
            self.logger.info("Coordinator started successfully.")

            return
        except Exception as e:
            self.logger.error(f"Error during reloading coordinator: {e}")
            self.prod.send_sns('coordinator_reload_failed')

            # try and recover from the error
            try:
                for command in reversed(recovery):
                    self.logger.info(f"Recovering from error: {command}")
                    run_command(*command)

                self.props.wait_for_coordinator_state(CoordinatorState.RUNNING)
                self.logger.info("Coordinator started successfully after rollback.")

                return
            except Exception as e:
                self.logger.error(f"Error during recovery: {e}")


def startup(install_path : str, project_root: str, postgres_pid_file: Optional[str] = None) -> None:
    """
    'Static' function to start the loader process.
    """
    script_path = os.path.join(project_root, 'coordinator', 'loader.py')
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"Install path does not exist: {install_path}")

    cmds = [sys.executable, script_path, '-i', install_path, '-p', project_root]
    if postgres_pid_file:
        cmds += ['-f', postgres_pid_file]
    subprocess.Popen(
        cmds,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True
    )


def parse_args():
    """
    Parse command line arguments.
    """
    parser = argparse.ArgumentParser(description="Springtail Loader")
    parser.add_argument(
        "-i", "--install_path",
        type=str,
        default='/opt/springtail',
        help="Path to the installation directory."
    )
    parser.add_argument(
        "-p", "--project_root",
        type=str,
        default='/home/ubuntu/stc',
        help="Path to the project root directory."
    )
    parser.add_argument(
        '-f', '--postgres-pid-file',
        type=str,
        default=None,
        help='Full path name to the Postgres PID file (for fdw service only)'
    )


    return parser.parse_args()


def main():
    """
    Main function to start the Loader process.
    """
    args = parse_args()

    log_path = os.path.join(args.install_path, 'logs')
    if os.path.exists(log_path):
        sys.stderr = open(os.path.join(log_path, 'loader_stderr.log'), 'a+')
        sys.stdout = open(os.path.join(log_path, 'loader_stdout.log'), 'a+')

    print(f"Loader started with pid: {os.getpid()}")
    loader = Loader(args.install_path, args.project_root, args.postgres_pid_file)
    loader.run()


if __name__ == "__main__":
    main()