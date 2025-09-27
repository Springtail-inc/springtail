import os
from typing import Optional
from component import Component
from postgres_component import PostgresComponent
from common import run_command
from properties import Properties

class ComponentFactory:
    """
    Component factory class to create new components
    """
    # NOTE: IDs must match the IDs used in the Redis queue defined
    # in src/common/coordinator.hh enum DaemonType
    LOG_MGR_ID = "1"
    DDL_MGR_ID = "4"
    SYS_TBL_MGR_ID = "6"
    PROXY_ID = "7"
    XID_SUBSCRIBER_ID = "9"
    POSTGRES = "10"

    def __init__(self, install_dir : str, props: Properties):
        """Initialize the component factory"""
        self.install_dir = install_dir
        self.pid_dir = props.get_pid_path()
        self.props = props

    def create_log_mgr_daemon(self) -> Component:
        """Create a new log manager component."""
        return Component(
            name="pg_log_mgr_daemon",
            id=self.LOG_MGR_ID,
            args=["--daemonize"],
            path=self.install_dir,
            pid_path=os.path.join(self.pid_dir, 'pg_log_mgr_daemon.pid')
        )

    def create_sys_tbl_mgr_daemon(self) -> Component:
        """Create a new sys table mgr component."""
        return Component(
            name="sys_tbl_mgr_daemon",
            id=self.SYS_TBL_MGR_ID,
            args=["--daemonize"],
            path=self.install_dir,
            pid_path=os.path.join(self.pid_dir, 'sys_tbl_mgr_daemon.pid')
        )

    def create_ddl_daemon(self) -> Component:
        """Create a new write cache component."""
        return Component(
            name="pg_ddl_daemon",
            id=self.DDL_MGR_ID,
            args=["--daemonize", "-s", "/var/run/postgresql"],
            path=self.install_dir,
            pid_path=os.path.join(self.pid_dir, 'pg_ddl_daemon.pid')
        )

    def create_proxy(self) -> Component:
        """Create a new proxy component."""
        return Component(
            name="proxy",
            id=self.PROXY_ID,
            args=["--daemonize"],
            path=self.install_dir,
            pid_path=os.path.join(self.pid_dir, 'proxy.pid')
        )

    def create_postgres(self, pid_path: Optional[str]) -> PostgresComponent:
        """Create a new postgres component."""
        # get the path to the postgres binary, and the version
        bindir = run_command('pg_config', ['--bindir']).strip()

        if not pid_path:
            # if pid_path is not provided, use the default path based on the version
            # version string is like: 'PostgreSQL 16.4 (Ubuntu 16.4-0ubuntu0.24.04.2)'
            version_str = run_command('pg_config', ['--version']).strip()
            version = version_str.split(' ')[1].split('.')[0]
            pid_path = f'/var/run/postgresql/{version}-main.pid'

        return PostgresComponent(
            name="postgres",
            id=self.POSTGRES,
            path=bindir,
            pid_path=pid_path,
            props=self.props
        )

    def create_xid_subscriber_daemon(self) -> Component:
        """Create a new XID subscriber component."""
        return Component(
            name="pg_xid_subscriber_daemon",
            id=self.XID_SUBSCRIBER_ID,
            args=["--daemon"],
            path=self.install_dir,
            pid_path=os.path.join(self.pid_dir, 'pg_xid_subscriber.pid')
        )
