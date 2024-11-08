import os
import signal
import psutil
import time
from component import Component, ComponentState

from common import (
    run_command,
    running_pids
)

class PostgresComponent(Component):
    """PostgresComponent class to represent a single Postgres component"""

    def __init__(self,
                 id: str,
                 pid_path: str,
                 name: str = "postgres") :
        """Initialize a new PostgresComponent"""
        super().__init__(name, id, "", pid_path)

    def start(self) -> bool:
        """
        Start the Postgres process.
        Returns:
            True if successful, False otherwise
        """
        run_command('sudo', ['service', 'postgresql', 'start'])

        # Wait for process to start
        timeout = time.time() + self.startup_timeout
        while time.time() < timeout:
            if self.is_running():
                if os.path.exists(self.pid_path):
                    self.pid = self.__pid_from_file()
                    self.state = ComponentState.RUNNING
                    self.process = psutil.Process(self.pid)
                    return True
            time.sleep(0.5)

        return True

    def kill(self) -> bool:
        """
        Kill the Postgres process
        Returns:
            True if successful, False otherwise
        """
        if self.pid:
            super().kill()
        else:
            pids = running_pids(['postgres'])[0]
            for pid in pids:
                os.kill(pid, signal.SIGKILL)

        # Wait for process to terminate
        timeout = time.time() + self.shutdown_timeout
        while time.time() < timeout:
            if not self.is_running():
                return True
            time.sleep(0.1)

        return False

    def shutdown(self, sig: int = None) -> bool:
        """
        Shutdown the Postgres process
        Returns:
            True if successful, False otherwise
        """
        run_command('sudo', ['service', 'postgresql', 'stop'])

        # Wait for process to terminate
        timeout = time.time() + self.shutdown_timeout
        while time.time() < timeout:
            if not self.is_running():
                return True
            time.sleep(0.1)

        return False

    def is_running(self) -> bool:
        """
        Check if the Postgres process is running
        Returns:
            True if running, False otherwise
        """
        if self.pid:
            return super().is_running()

        pids = running_pids(['postgres'])[0]
        return len(pids) > 0

    def is_alive(self) -> bool:
        """
        Check if the Postgres process is still alive
        Returns:
            True if alive, False otherwise
        """
        if not self.is_running():
            return False

        # try connecting to the database and running select 1
        try:
            run_command('sudo' ['-u', 'postgres', 'psql', '-c', 'select 1'])
        except Exception as e:
            return False

        return True
