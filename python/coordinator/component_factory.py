import os
from component import Component
from postgres_component import PostgresComponent
from common import run_command

class ComponentFactory:
    """
    Component factory class to create new components
    """
    # NOTE: IDs must match the IDs used in the Redis queue defined
    # in src/common/coordinator.hh enum DaemonType
    LOG_MGR_ID = "1"
    WRITE_CACHE_ID = "2"
    XID_MGR_ID = "3"
    DDL_ID = "4"
    GC_ID = "5"
    SYS_TBL_MGR_ID = "6"
    PROXY_ID = "7"
    POSTGRES = "10"

    def __init__(self, install_dir : str, pid_dir : str):
        """Initialize the component factory"""
        self.install_dir = install_dir
        self.pid_dir = pid_dir

    def create_log_mgr_daemon(self) -> Component:
        """Create a new log manager component."""
        return Component(
            name="pg_log_mgr_daemon",
            id=self.LOG_MGR_ID,
            args=["--daemon"],
            path=self.install_dir,
            pid_path=os.path.join(self.pid_dir, 'pg_log_mgr.pid')
        )

    def create_gc_daemon(self) -> Component:
        """Create a new garbage collector component."""
        return Component(
            name="gc_daemon",
            id=self.GC_ID,
            args=["--daemon"],
            path=self.install_dir,
            pid_path=os.path.join(self.pid_dir, 'gc.pid')
        )

    def create_xid_mgr_daemon(self) -> Component:
        """Create a new xid mgr component."""
        return Component(
            name="xid_mgr_daemon",
            id=self.XID_MGR_ID,
            args=["--daemon"],
            path=self.install_dir,
            pid_path=os.path.join(self.pid_dir, 'xid_mgr.pid')
        )

    def create_sys_tbl_mgr_daemon(self) -> Component:
        """Create a new sys table mgr component."""
        return Component(
            name="sys_tbl_mgr_daemon",
            id=self.SYS_TBL_MGR_ID,
            args=["--daemon"],
            path=self.install_dir,
            pid_path=os.path.join(self.pid_dir, 'sys_tbl_mgr.pid')
        )

    def create_write_cache_daemon(self) -> Component:
        """Create a new write cache component."""
        return Component(
            name="write_cache_daemon",
            id=self.WRITE_CACHE_ID,
            args=["--daemon"],
            path=self.install_dir,
            pid_path=os.path.join(self.pid_dir, 'write_cache.pid')
        )

    def create_ddl_daemon(self, user: str, password: str) -> Component:
        """Create a new write cache component."""
        return Component(
            name="pg_ddl_daemon",
            id=self.DDL_ID,
            args=["--daemon", "--username", user, "--password", password],
            path=self.install_dir,
            pid_path=os.path.join(self.pid_dir, 'pg_ddl_mgr.pid')
        )

    def create_proxy(self) -> Component:
        """Create a new proxy component."""
        return Component(
            name="proxy",
            id=self.PROXY_ID,
            args=["--daemon"],
            path=self.install_dir,
            pid_path=os.path.join(self.pid_dir, 'proxy.pid')
        )

    def create_postgres(self) -> Component:
        """Create a new postgres component."""
        # get the path to the postgres binary, and the version
        bindir = run_command('pg_config', ['--bindir']).strip()
        version_str = run_command('pg_config', ['--version']).strip()
        # version string is like: 'PostgreSQL 16.4 (Ubuntu 16.4-0ubuntu0.24.04.2)'
        version = version_str.split(' ')[1].split('.')[0]
        return PostgresComponent(
            name="postgres",
            id=self.POSTGRES,
            path=bindir,
            pid_path=f'/var/run/postgresql/{version}-main.pid'
        )