import os
import sys
import signal
import time
import logging
import psutil
from typing import Dict, List, Optional
from enum import Enum

# Get the parent directory of the current script (i.e., the project root directory)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the /shared directory to the Python path
sys.path.append(os.path.join(project_root, 'shared'))

from common import run_command

class ComponentState(Enum):
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    FAILED = "failed"

class ShutdownSignal(Enum):
    SIGINT = signal.SIGINT
    SIGTERM = signal.SIGTERM
    SIGKILL = signal.SIGKILL

class Component:
    """Component class to represent a single component"""

    def __init__(self, name: str,
                 id: str,
                 path: str,
                 pid_path: str,
                 args: List[str] = [],
                 force_shutdown: bool = True,
                 shutdown_signal: ShutdownSignal = ShutdownSignal.SIGTERM):
        """Initialize a new component

        Args:
            name: The name of the component
            id: The ID of the component
            path: The path to the component binary
            pid_path: The path to the PID file
            args: Additional arguments to pass to the component
            force_shutdown: Forcefully shutdown the component on startup if running
            shutdown_signal: The signal to send to the component on shutdown
        """
        self.name = name
        self.id = id
        self.path = os.path.join(path, name)
        self.args = args
        self.shutdown_signal = shutdown_signal
        self.startup_timeout :int = 30 # seconds
        self.shutdown_timeout : int = 30 # seconds
        self.startup_order: int = 0
        self.pid_path = pid_path
        self.state : ComponentState = ComponentState.STOPPED
        self.process: Optional[psutil.Process] = None
        self.pid: Optional[int] = None
        self.logger = logging.getLogger('springtail')

        if not os.path.exists(self.path):
            raise ValueError(f"Component path not found: {self.path}")

        # check if component is already running
        pid = self._check_running(self.pid)
        if pid:
            self.pid = pid
            self.process = psutil.Process(self.pid)
            self.state = ComponentState.RUNNING

            if force_shutdown:
                self.logger.info(f"Found running component {self.name}: state={self.state}, pid={self.pid}; shutting it down for startup")
                self.shutdown()
        else:
            self.pid = None
            self.process = None
            self.state = ComponentState.STOPPED

        self.logger.info(f"Initialized component {self.name}: state={self.state}, pid={self.pid}")

    def _check_running(self, pid : Optional[int]) -> Optional[int]:
        """
        Check if the component is already running.
        Returns:
            The PID of the running component if found, None otherwise
        """
        # check if component is already running
        # first see if a pid is passed in and it exists
        if pid and psutil.pid_exists(pid):
            return pid

        # next check if a pid file exists and pid is running
        pid = self._pid_from_file()
        if pid:
            try:
                process = psutil.Process(pid)
                if process.is_running() and process.status() != psutil.STATUS_ZOMBIE:
                    return pid
            except Exception as e:
                return None

        # finally check if the process is running without a pidfile
        for proc in psutil.process_iter(['pid', 'name', 'status']):
            if proc.info['name'] == self.name:
                if not proc.is_running() or proc.info['status'] == psutil.STATUS_ZOMBIE:
                    return None

                pid = proc.info['pid']

                # create pid file
                with open(self.pid_path, 'w') as f:
                    f.write(str(self.pid))

                return pid

        return None

    def _pid_from_file(self) -> Optional[int]:
        """
        Read the PID from the PID file (protected method)
        Returns:
            The PID as an integer if successful, None otherwise
        """
        try:
            # use sudo head to read the first line of the pid file
            # for postgres we may not have permissions to read the file directly
            pid_str = run_command('sudo', ['head', '-n', '1', self.pid_path]).strip()
            pid = int(pid_str)
            if psutil.pid_exists(pid):
                # check if the process is running
                process = psutil.Process(pid)
                if process.is_running() and process.status() != psutil.STATUS_ZOMBIE:
                    # process is running, mark it as such
                    self.logger.info(f"Found running PID for component {self.name}: {pid}")
                    return pid

        except Exception as e:
            pass

        # remove pid file if not correct
        try:
            os.remove(self.pid_path)
        except Exception as e:
            pass

        return None

    def get_id(self) -> str:
        """
        Get the ID of the component
        Returns:
            The ID of the component as a string
        """
        return self.id

    def get_name(self) -> str:
        """
        Get the name of the component
        Returns:
            The name of the component as a string
        """
        return self.name

    def set_timeout(self, startup_timeout: int = 30, shutdown_timeout: int = 30) -> None:
        """Set the startup and shutdown timeouts for the component"""
        self.startup_timeout = startup_timeout
        self.shutdown_timeout = shutdown_timeout

    def set_startup_order(self, startup_order: int) -> None:
        """Set the startup order of the component"""
        self.startup_order = startup_order

    def start(self) -> bool:
        """
        Start the component process
        Returns:
            True if successful, False otherwise
        """
        self.logger.debug(f"Starting component {self.name}")

        if self.is_running():
            self.logger.info(f"Component {self.name} already running")
            return True

        try:
            self.state = ComponentState.STARTING
            self.process = None

            # start the process, daemonize it
            run_command(self.path, self.args)

            # Wait for process to start
            timeout = time.time() + self.startup_timeout
            while time.time() < timeout:
                # check for pid file
                if not os.path.exists(self.pid_path):
                    time.sleep(0.1)
                    continue

                # read pid from file
                pid = self._pid_from_file()
                if pid and psutil.pid_exists(pid):
                    self.process = psutil.Process(pid)
                    break

                time.sleep(0.1)

            # Check if process started (or timedout)
            if not self.process:
                self.state = ComponentState.FAILED
                self.logger.error(f"Failed to start component {self.name} within timeout")
                return False

            self.pid = self.process.pid
            self.state = ComponentState.RUNNING
            self.logger.info(f"Started component {self.name} with PID {self.pid}")

            return True
        except Exception as e:
            self.state = ComponentState.FAILED
            self.logger.error(f"Failed to start component {self.name}: {str(e)}")
            return False

    def kill(self) -> Optional[bool]:
        """
        Kill the component process
        Returns:
            True if successful, False otherwise
        """
        self.logger.debug(f"Killing component: {self.name}")
        if self.pid:
            try:
                os.kill(self.pid, signal.SIGKILL)
                return self._wait_shutdown()
            except Exception as e:
                self.logger.error(f"Error killing component {self.name}: {str(e)}")

        # iterate over processes and kill the one with the name
        for proc in psutil.process_iter(['pid', 'name']):
            if proc.info['name'] == self.name:
                try:
                    proc.kill()
                    return self._wait_shutdown()
                except Exception as e:
                    self.logger.error(f"Error killing component {self.name}: {str(e)}")

        return False

    def _wait_shutdown(self) -> bool:
        """Wait for the process to shutdown"""
        # Wait for process to terminate
        shutdown_timeout = time.time() + self.shutdown_timeout
        while time.time() < shutdown_timeout:
            if self.process is not None and self.process.is_running():
                time.sleep(0.1)
                continue

            # Move to stopped state
            self.pid = None
            self.process = None
            self.state = ComponentState.STOPPED

            # remove pid file
            if os.path.exists(self.pid_path):
                os.remove(self.pid_path)

            self.logger.debug(f"Component {self.name} shutdown")

            return True

        self.state = ComponentState.FAILED
        self.logger.error(f"Failed to shutdown component {self.name} within timeout")

        return False

    def shutdown(self, sig: Optional[int] = None) -> bool:
        """
        Shutdown the component process
        Returns:
            True if successful, False otherwise
        """
        self.logger.debug(f"Shutting down component: {self.name}")
        if sig is None:
            sig = self.shutdown_signal.value

        if not self.is_running():
            return True

        try:
            self.state = ComponentState.STOPPING
            if self.pid is not None:
                os.kill(self.pid, sig)

            return self._wait_shutdown()

        except ProcessLookupError:
            # Process already terminated
            self.state = ComponentState.STOPPED
            self.pid = None
            self.process = None
            return True
        except Exception as e:
            self.state = ComponentState.FAILED
            self.logger.error(f"Error shutting down component {self.name}: {str(e)}")
            return False

    def is_running(self) -> bool:
        """
        Check if the process is still running
        Returns:
            True if running, False otherwise
        """
        if not self.pid:
            self.logger.debug(f"Component {self.name} has no PID")
            return False

        try:
            if not self.process or self.process.pid != self.pid:
                self.process = psutil.Process(self.pid)

            running = self.process.is_running()
            self.logger.debug(f"Process {self.name}, {self.pid} {'is' if running else 'is not'} running")

            return running

        except psutil.NoSuchProcess:
            self.logger.warning(f"Process {self.name}, {self.pid} not found")
            return False
        except Exception:
            self.logger.warning(f"Exception, Process {self.name}, {self.pid} not found")
            return False

    def is_alive(self) -> bool:
        """
        Check if the process is still alive
        Returns:
            True if alive, False otherwise
        """
        self.logger.debug(f"Checking if component {self.name} is alive")
        isrunning = self.is_running()
        self.logger.debug(f"Component {self.name} {'is' if isrunning else 'is not'} alive")
        return isrunning

