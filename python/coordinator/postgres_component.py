import os
import signal
import psutil
import time
import logging
from typing import Dict, List, Optional

from component import Component, ComponentState

from common import (
    run_command,
    running_pids
)

class PostgresComponent(Component):
    """PostgresComponent class to represent a single Postgres component"""

    def __init__(self,
                 id: str,
                 path: str,    # Path is unused but needs to be valid for the parent class
                 pid_path: str,
                 name: str = "postgres") :
        """Initialize a new PostgresComponent"""
        self.logger = logging.getLogger('springtail')

        version_str = run_command('pg_config', ['--version']).strip()
        self.version = version_str.split(' ')[1].split('.')[0]

        environment = os.environ.get('DEPLOYMENT_ENV', 'development')
        self.is_production = False
        if environment == 'production':
            self.is_production = True
            fdw_user = os.environ.get('FDW_USER', 'springtail')
            self.service_name = f'postgresql-{fdw_user}.service'

        super().__init__(name, id, path, pid_path)

    def start(self) -> bool:
        """
        Start the Postgres process.
        Returns:
            True if successful, False otherwise
        """
        # since we are restarting, clear the pid
        self.pid = None
        self.process = None
        self.state = ComponentState.STARTING

        self.logger.debug("Re-starting Postgres")
        if self.is_production:
            run_command('sudo', ['systemctl', 'stop', self.service_name])
            run_command('sudo', ['systemctl', 'start', self.service_name])
        else:
            run_command('sudo', ['service', 'postgresql', 'restart'])

        # Wait for process to start
        timeout = time.time() + self.startup_timeout
        while time.time() < timeout:
            # Check if the process has started
            if os.path.exists(self.pid_path):
                self.pid = super()._pid_from_file()
                self.process = psutil.Process(self.pid)

            if self.is_running():
                self.state = ComponentState.RUNNING
                return True

            self.pid = None
            self.process = None

            time.sleep(0.5)

        return False

    def kill(self) -> bool:
        """
        Kill the Postgres process
        Returns:
            True if successful, False otherwise
        """
        self.logger.debug("Killing Postgres")
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

    def shutdown(self, sig: Optional[int] = None) -> bool:
        """
        Shutdown the Postgres process
        Returns:
            True if successful, False otherwise
        """
        self.logger.debug("Shutting down Postgres")
        if self.is_production:
            run_command('sudo', ['systemctl', 'stop', f'postgresql@{self.version}-main'])
        else:
            run_command('sudo', ['service', 'postgresql', 'stop'])

        # Wait for process to terminate
        timeout = time.time() + self.shutdown_timeout
        while time.time() < timeout:
            if not self.is_running():
                self.pid = None
                self.state = ComponentState.STOPPED
                self.process = None
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
            run_command('sudo', ['-u', 'postgres', 'psql', '-c', 'select 1'])
        except Exception as e:
            self.logger.error(f"Failed to connect to Postgres: {str(e)}")
            return False

        return True