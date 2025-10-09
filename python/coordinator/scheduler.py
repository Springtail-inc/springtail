import sys
import os
import time
import logging
import redis
import threading
import traceback
from enum import Enum, StrEnum
from typing import Dict, Optional

from component import Component
from exceptions import ComponentFailureException

# Get the parent directory of the current script (i.e., the project root directory)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the /shared directory to the Python path
sys.path.append(os.path.join(project_root, 'shared'))

# Now import the Properties class
from properties import Properties

from production import Production

class CoordinatorState(StrEnum):
    """Coordinator state class"""
    SHUTDOWN = 'shutdown'
    STARTUP = 'startup'
    RUNNING = 'running'
    DEAD = 'dead'
    RELOAD = 'reload'
    RELOADING = 'reloading'

# Note: this must be kept in sync with the C++ DaemonType in coordinator.hh
# Mapping to service names must also be updated below
class LivenessDaemonType(Enum):
    """Enumeration for Liveness Daemon types"""
    LOG_MGR = 1
    WRITE_CACHE = 2
    XID_MGR = 3
    DDL_MGR = 4
    GC_MGR = 5
    SYS_TBL_MGR = 6
    PROXY = 7
    FDW = 8
    XID_SUBSCRIBER = 9

# Mapping from LivenessDaemonType to service names
LIVENESS_DAEMON_TO_SERVICE = {
    LivenessDaemonType.LOG_MGR: 'ingestion',
    LivenessDaemonType.WRITE_CACHE: 'ingestion',
    LivenessDaemonType.XID_MGR: 'ingestion',
    LivenessDaemonType.DDL_MGR: 'fdw',
    LivenessDaemonType.GC_MGR: 'ingestion',
    LivenessDaemonType.SYS_TBL_MGR: 'ingestion',
    LivenessDaemonType.PROXY: 'proxy',
    LivenessDaemonType.FDW: 'fdw',
    LivenessDaemonType.XID_SUBSCRIBER: 'fdw',
}

# Number of failures before a component is considered failed
MAX_FAILURES = 5

# Time between consecutive failures before considering a component failed
FAILURE_WINDOW_THRESHOLD = 5 # seconds

# Wait time between max failures before retrying
RETRY_WAITTIME_BETWEEN_MAX_FAILURES = 5 # minutes

# Wait time between connection count queries
RETRY_WAITTIME_CONNECTION_COUNT = 5 # seconds

# Sleep interval in idle mode, when all services are down, but coordinator is up
IDLE_SLEEP_INTERVAL = 5 # seconds

class Scheduler:
    """Scheduler class to manage the lifecycle of components"""

    def __init__(self,
        props: Properties,
        service_name : str,
        production : Optional[Production] = None,
        allowed_timeout_secs: int = 40
    ):
        """
        Initialize a new scheduler
        Arguments:
            props -- the properties object
            service_name -- the name of the service
            production -- the Production object managing service environments
            allowed_timeout -- the allowed timeout for components in seconds
                NOTE: see constants.hh for the default keep alive timeout
        """
        self.props = props
        self.service_name = service_name.lower()
        self.production = production
        self.redis = props.get_data_redis()
        self.liveness_hash = props.get_liveness_hash()
        self.liveness_pubsub = props.get_liveness_notification_pubsub()
        self.components: Dict[str, Component] = {}
        self.logger = logging.getLogger('springtail')
        self.timeouts = {}
        self.allowed_timeout = allowed_timeout_secs * 1000
        self.db_states = {}
        # Map of component_name to {count, time} where count is the number of failures and time is the last time it failed
        self.last_fail_time = {}
        self.postgres_component = None
        self.services_stopped = False

        # Setup pubsub for liveness notifications
        self.pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
        self.pubsub.subscribe(self.liveness_pubsub)

        # Create list of daemon ids for this service (for daemon checks)
        self.liveness_ids = [str(daemon.value) for daemon, service in LIVENESS_DAEMON_TO_SERVICE.items() if service.lower() == self.service_name]

        self.shutdown_event = threading.Event()

    def register_component(self, component: Component, startup_order: int) -> None:
        """Register a new component with the scheduler"""
        self.components[component.get_id()] = component
        component.set_startup_order(startup_order)
        self.logger.info(f"Scheduler registered component {component.name}")
        if component.name == 'postgres':
            self.postgres_component = component

    def start_all(self) -> bool:
        """
        Start all components in order of their startup_order
        Returns:
            True if all components started successfully
        """
        # Clear timeout tracking
        self._init_timeouts()

        # Sort components by startup order
        sorted_components = sorted(
            self.components.values(),
            key=lambda x: x.startup_order
        )

        for component in sorted_components:
            # Start component
            if not component.start():
                self.logger.error(f"Failed to start {component.name}")
                return False

            # Wait for startup timeout
            start_time = time.time()
            while time.time() - start_time < component.startup_timeout:
                if component.is_running():
                    break
                time.sleep(0.1)
            else:
                self.logger.error(f"Component {component.name} failed to start within timeout")
                return False

        self.props.set_coordinator_state(CoordinatorState.RUNNING)

        return True

    def shutdown_all(self) -> bool:
        """
        Shutdown all components in reverse order of startup_order
        Returns:
            True if all components shutdown successfully
        """
        # Sort components in reverse startup order
        sorted_components = sorted(
            self.components.values(),
            key=lambda x: x.startup_order,
            reverse=True
        )

        success = True
        for component in sorted_components:
            if not component.shutdown():
                if not component.kill():
                    success = False
                    self.logger.error(f"Failed to shutdown {component.name}")

        return success

    def restart_all(self) -> bool:
        """
        Restart all components
        Returns:
            True if all components restarted successfully
        """
        self.logger.info("Restarting all components")
        if not self.shutdown_all():
            return False

        return self.start_all()

    def _track_last_failures(self, name: str, threshold : int = FAILURE_WINDOW_THRESHOLD, fatal : bool = True) -> None:
        """
        Track last failures for components

        Args:
            name: the name of the component

        If we have reached the maximum failures and the last failure was less than FAILURE_WINDOW_THRESHOLD seconds ago, raise an exception
        Otherwise, reset the failure count
        """
        if name not in self.last_fail_time:
            # Initialize failure tracking
            self.last_fail_time[name] = {'count': 1, 'time': time.time()}
        else:
            # If the last fail time is not within the failure window threshold, reset the failure count
            if time.time() - self.last_fail_time[name]['time'] > threshold:
                self.last_fail_time[name]['count'] = 1
                self.last_fail_time[name]['time'] = time.time()

        if self.last_fail_time[name]['count'] < MAX_FAILURES:
            # Increment failure count
            self.last_fail_time[name]['count'] += 1
            self.last_fail_time[name]['time'] = time.time()
            return

        # If we have reached the maximum failures and the last failure was less than threshold seconds ago, raise an exception
        if time.time() - self.last_fail_time[name]['time'] < threshold:
            if fatal:
                # fatal error, raise an exception
                self.logger.error(f"Component {name} has failed {MAX_FAILURES} times within {threshold} seconds, aborting")
                raise ComponentFailureException(f"Component {name} has failed {MAX_FAILURES} times within {threshold} seconds, aborting", name)
            else:
                # non-fatal error, just log a warning, send sns message if in production
                self.logger.warning(f"Component {name} has failed {MAX_FAILURES} times within {threshold} seconds, continuing")
                if self.production:
                    self.production.send_sns('warning', component=name, attrs={'warning': 'max_timeouts_reached',
                                                                               'count': self.last_fail_time[name]['count'],
                                                                               'threshold_secs': threshold})

        # reset count and timeout
        self.last_fail_time[name]['count'] = 1
        self.last_fail_time[name]['time'] = time.time()

    def _track_db_state_changes(self) -> None:
        """
        Track database state changes
        """
        states = self.props.get_db_states()
        for id, state in states.items():
            if id not in self.db_states or self.db_states[id] != state:
                self.logger.info(f"DB state changed for {id}: {state}", extra={'db_id': id})
                attrs = {'db_id': id, 'new_state': state, 'old_state': self.db_states.get(id, 'service_startup')}

                if self.production:
                    self.production.send_sns('db_state_change', attrs=attrs)

                self.db_states[id] = state

    def _check_pubsub(self) -> bool:
        """
        Check for messages on the liveness pubsub channel
        Returns:
            True if any component has failed.
        """
        try:
            # get message from pubsub channel
            msg = self.pubsub.get_message(timeout=1)

            # process message if there is one
            if msg and msg['type'] == 'message':
                self.logger.debug(f"Received message: {msg['data']}")
                id, _ = msg['data'].split(':')
                if id not in self.components:
                    return False
                component_name = self.components[id].get_name()
                self.logger.error(f"PubSub: Error for component: {id}: {component_name}")
                self._track_last_failures(component_name, threshold=60*30, fatal=False)
                return True

        except redis.TimeoutError:
            return False
        return False

    def _check_timeouts(self) -> bool:
        """
        Check the liveness of all components
        Returns:
            True if any components has failed.
            False if no components have failed.
        """
        # Check for timeout messages in Redis
        data = self.redis.hgetall(self.liveness_hash)
        if not data:
            self.logger.debug(f"No timeout data for hash: {self.liveness_hash}")
            return False

        # Go through and merge time values for the same id, taking minimum
        # key format: daemon_id:thread_id
        # value format: timestamp (epoch ms)
        timeouts: Dict[str, int] = {}
        for key, value in data.items():
            id = key.split(':')[0]
            if id not in self.liveness_ids:
                continue
            if id not in timeouts:
                timeouts[id] = int(value)
            else:
                timeouts[id] = min(timeouts[id], int(value))
            self.logger.debug(f"Got timeout data: {key}:{value}")

        if not timeouts:
            return False

        # Merge with existing timeouts
        self.timeouts.update(timeouts)

        # Check for timeouts
        min_time = time.time() * 1000 - self.allowed_timeout
        self.logger.debug(f"Checking timeouts: allowed: {self.allowed_timeout}, min_time: {min_time}")
        for id, timestamp in self.timeouts.items():
            if timestamp < min_time:
                name = LivenessDaemonType(int(id)).name
                self._track_last_failures(name)
                self.logger.error(f"Timeout for component: {name}, {timestamp} < {min_time} {time.time() * 1000 - timestamp}")
                self.logger.error(f"Raw data:\n{data}")
                if self.production:
                    self.production.send_sns('failure', name)
                return True

        return False

    def _check_components(self) -> bool:
        """
        Check the liveness of all components
        Returns:
            True if any components has failed.
        """
        failed = False
        for id, component in self.components.items():
            if not component.is_alive():
                self.logger.error(f"Component {component.get_name()} is not running")
                self._track_last_failures(component.get_name())
                if self.production:
                    self.production.send_sns('failure', component=component.get_name())
                failed = True

        return failed

    def shutdown(self) -> None:
        """
        Shutdown the scheduler
        """
        self.shutdown_event.set()

    def _init_timeouts(self) -> None:
        """
        Initialize timeouts for all registered components, clear any existing timeouts from redis
        """
        self.timeouts.clear()

        keys = self.redis.hkeys(self.liveness_hash)
        for key in keys:
            id = key.split(':')[0]
            if id in self.liveness_ids:
                self.redis.hdel(self.liveness_hash, key)

    def _check_coordinator_state(self) -> None:
        """
        Check the coordinator state
        """
        if not self.production:
            return

        coordinator_state = self.props.get_coordinator_state()

        match coordinator_state:
            case CoordinatorState.RUNNING:
                return

            case CoordinatorState.SHUTDOWN:
                self.logger.info("Coordinator state is shutdown, exiting")
                self.shutdown()
                return

            case CoordinatorState.STARTUP:
                self.logger.info("Coordinator state is startup, setting running state")
                self.props.set_coordinator_state(CoordinatorState.RUNNING)
                return

            case CoordinatorState.RELOAD:
                self.logger.info("Coordinator state is reload, shutting down services")
                self.shutdown_all()
                self.logger.info("Shutting down services complete, installing binaries")

                if self.production:
                    config_gitsha = self.props.get_config_gitsha()
                    self.production.install_binaries(config_gitsha)
                    if self.service_name == 'fdw':
                        self.logger.info("Installing postgres_fdw")
                        self.production.install_pgfdw(self.props)

                self.logger.info("Restarting services")
                self.restart_all()

                self.props.set_coordinator_state(CoordinatorState.RUNNING)
                self.logger.info("Restarting services complete")
                return

            case _:  # default case
                self.logger.error(f"Unknown coordinator state: {coordinator_state}")
                return

    def _process_fdw_state_change(self):
        """
        Check if there was FDW state change to 'draining' and there was, process it accordingly.
        """
        # get FDW state
        fdw_config = self.props.get_fdw_config(True)
        fdw_state = fdw_config['state']
        self.logger.debug(f"FDW state = '{fdw_state}'")
        if fdw_state != 'draining':
            return

        self.logger.debug(f"Draining state detected, waiting for connections to drain")
        # wait for all proxy connections to drain
        while True:
            connection_count = self.postgres_component.get_connection_count()
            self.logger.debug(f"Remaining number of connections: {connection_count}")
            if connection_count == 0:
                break
            time.sleep(RETRY_WAITTIME_CONNECTION_COUNT)

        # bring all components down
        self.shutdown_all()

        try:
            # wait for stopped state
            self.logger.debug(f"Waiting for FDW state change to 'stopped'")
            self.props.wait_for_fdw_state('stopped', 30)
            self.logger.debug(f"FDW state was set to 'stopped', performing cleanup")
        except TimeoutError as e:
            self.logger.error(f"Timed out waiting for state change: {str(e)}")
            self.props.set_fdw_state('stopped')

        # clean up data db
        # <db instance id>:queue:ddl:fdw:<fdw id>
        # <db instance_id>:hash:ddl:fdw  key: <fdw_id>
        instance_id = self.props.get_db_instance_id()
        fdw_id = self.props.get_fdw_id()

        redis = self.props.get_data_redis()
        redis.delete(f"{instance_id}:queue:ddl:fdw:{fdw_id}")

        ddl_hash_name = f"{instance_id}:hash:ddl:fdw"
        # Get all matching keys of the hash
        keys = [k for k in redis.hkeys(ddl_hash_name) if k.endswith(f":{fdw_id}")]
        # Remove matching keys
        if keys:
            redis.hdel(ddl_hash_name, *keys)

        fdw_min_xids_hash_name = f"{instance_id}:fdw_min_xids"
        # Get all matching keys of the hash
        keys = [k for k in redis.hkeys(fdw_min_xids_hash_name) if k.startswith(f"{fdw_id}:")]
        # Remove matching keys
        if keys:
            redis.hdel(fdw_min_xids_hash_name, *keys)

        fdw_pids_set_name = f"{instance_id}:fdw_pids"
        # Get all matching members of the set
        members = [m for m in redis.smembers(fdw_pids_set_name) if m.startswith(f"{fdw_id}:")]
        # Remove matching members
        if members:
            redis.srem(fdw_pids_set_name, *members)

        # set a flag indicating that the services are stopped while coordinator
        # is still running
        self.services_stopped = True
        self.logger.info("FDW is now stopped")

    def monitor_timeouts(self) -> None:
        """
        Monitor Redis queue for timeout events
        """
        # Initialize timeouts for registered components
        # This will clear any existing timeouts from Redis
        self._init_timeouts()

        while not self.shutdown_event.is_set():
            try:
                self.logger.debug("Scheduler monitoring timeouts")

                # get the coordinator state
                self._check_coordinator_state()

                if self.services_stopped:
                    self.logger.debug("All services are stopped")
                    time.sleep(IDLE_SLEEP_INTERVAL)
                    continue

                # track database state changes
                self._track_db_state_changes()

                # check for timeouts; don't error, just issue a warning
                self._check_timeouts()

                # check for pubsub messages, timeouts, or component failures
                if (
                   self._check_pubsub() or    # check for pubsub ; blocks 1 second if no messages
                   self._check_components()   # check for component failures
                ):
                    # restart all components
                    self.logger.warning("Restarting all components")

                    if self.logger.isEnabledFor(logging.DEBUG):
                        sys.exit(1)

                    if not self.restart_all():
                        # XXX handle
                        if self.production:
                            self.production.send_sns('failure', component='all')
                        self.logger.error("Failed to restart all components")
                        break

                # handle 'draining' state for FDW
                if self.service_name == 'fdw':
                    self._process_fdw_state_change()

            except redis.RedisError as e:
                # XXX handle
                self.logger.error(f"Redis error: {str(e)}")
                continue
            except ComponentFailureException as e:
                error_message = traceback.format_exc()
                self.logger.error(error_message)
                if self.production:
                    self.production.send_sns('max_retries_failed', component=e.component_name)
                # sleep for RETRY_WAITTIME_BETWEEN_MAX_FAILURES minutes before retrying
                time.sleep(RETRY_WAITTIME_BETWEEN_MAX_FAILURES * 60)
                continue
            except Exception as e:
                # XXX handle
                error_message = traceback.format_exc()
                self.logger.error(f"Unexpected error in monitor_timeouts: {str(e)}")
                self.logger.error(error_message)
                continue

        # end of while loop

        # shutdown all components
        self.shutdown_all()

        # close the pubsub
        self.pubsub.close()

        # close the redis connection
        self.redis.close()

        # set dead state
        self.props.set_coordinator_state(CoordinatorState.DEAD)
