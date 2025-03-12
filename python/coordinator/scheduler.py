import sys
import os
import time
import logging
import redis
import threading
import traceback
from typing import Dict, Optional

from component import Component

# Get the parent directory of the current script (i.e., the project root directory)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the /shared directory to the Python path
sys.path.append(os.path.join(project_root, 'shared'))

# Now import the Properties class
from properties import Properties

from production import Production

class CoordinatorState:
    """Coordinator state class"""
    SHUTDOWN = 'shutdown'
    STARTUP = 'startup'
    RUNNING = 'running'
    DEAD = 'dead'
    RELOAD = 'reload'

class Scheduler:
    """Scheduler class to manage the lifecycle of components"""

    def __init__(self,
        props: Properties,
        service_name : Optional[str],
        production : Optional[Production] = None,
        allowed_timeout_secs: int = 15
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
        self.service_name = service_name
        self.production = production
        self.redis = props.get_data_redis()
        self.liveness_hash = props.get_liveness_hash()
        self.liveness_pubsub = props.get_liveness_notification_pubsub()
        self.components: Dict[str, Component] = {}
        self.logger = logging.getLogger("coordinator")
        self.timeouts = {}
        self.allowed_timeout = allowed_timeout_secs * 1000
        self.db_states = {}

        # Setup pubsub for liveness notifications
        self.pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
        self.pubsub.subscribe(self.liveness_pubsub)

        self.shutdown_event = threading.Event()

    def register_component(self, component: Component, startup_order: int) -> None:
        """Register a new component with the scheduler"""
        self.components[component.get_id()] = component
        component.set_startup_order(startup_order)
        self.logger.info(f"Scheduler registered component {component.name}")

    def start_all(self) -> bool:
        """
        Start all components in order of their startup_order
        Returns:
            True if all components started successfully
        """
        # Clear timeout tracking
        self.timeouts.clear()

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

                self.logger.error(f"PubSub: Error for component: {id}: {self.components[id].get_name()}")
                if self.production:
                    self.production.send_sns('failure', component=self.components[id].get_name())
                return True

        except redis.TimeoutError:
            return False
        return False

    def _check_timeouts(self) -> bool:
        """
        Check the liveness of all components
        Returns:
            True if any components has failed.
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
            self.logger.debug(f"Got timeout data: {key}:{value}")
            id = key.split(':')[0]
            if id not in self.components:
                continue
            if id not in timeouts:
                timeouts[id] = int(value)
            else:
                timeouts[id] = min(timeouts[id], int(value))

        # Merge with existing timeouts
        self.timeouts.update(timeouts)

        # Check for timeouts
        min_time = time.time() * 1000 - self.allowed_timeout
        self.logger.debug(f"Checking timeouts: allowed: {self.allowed_timeout}, min_time: {min_time}")
        for id, timestamp in self.timeouts.items():
            if timestamp < min_time:
                self.logger.error(f"Timeout for component: {self.components[id].get_name()}, {timestamp} < {min_time} {time.time() * 1000 - timestamp}")
                if id in self.components:  # this should always be true
                    if self.production:
                        self.production.send_sns('failure', self.components[id].get_name())
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
        keys = self.redis.hkeys(self.liveness_hash)
        for key in keys:
            id = key.split(':')[0]
            if id in self.components:
                self.redis.hdel(self.liveness_hash, key)

    def _check_coordinator_state(self) -> None:
        """
        Check the coordinator state
        """
        if not self.production:
            return

        coordinator_state = self.props.get_coordinator_state()

        if coordinator_state == CoordinatorState.RUNNING:
            return

        if coordinator_state == CoordinatorState.SHUTDOWN:
            self.logger.info("Coordinator state is shutdown, exiting")
            self.shutdown()

        if coordinator_state == CoordinatorState.STARTUP:
            self.logger.info("Coordinator state is startup, setting running state")
            self.props.set_coordinator_state(CoordinatorState.RUNNING)
            return

        if coordinator_state == CoordinatorState.RELOAD:
            self.logger.info("Coordinator state is reload, shutting down services")
            self.shutdown_all()
            self.logger.info("Shutting down services complete, installing binaries")
            if self.production:
                self.production.install_binaries()
            if self.service_name == 'fdw':
                self.logger.info("Installing postgres_fdw")
                if self.production:
                    self.production.install_pgfdw()
            self.logger.info("Restarting services")
            self.restart_all()
            self.props.set_coordinator_state(CoordinatorState.RUNNING)
            self.logger.info("Restarting services complete")
            return;

    def monitor_timeouts(self) -> None:
        """
        Monitor Redis queue for timeout events
        """
        # Initialize timeouts for registered components
        # This will clear any existing timeouts from Redis
        self._init_timeouts()

        while not self.shutdown_event.is_set():
            try:
                self.logger.debug("Sheduler monitoring timeouts")

                # get the coordinator state
                self._check_coordinator_state()

                # track database state changes
                self._track_db_state_changes()

                # check for pubsub messages, timeouts, or component failures
                if (
                   self._check_pubsub() or    # check for pubsub ; blocks 1 second if no messages
                   self._check_timeouts() or  # check for timeouts
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

            except redis.RedisError as e:
                # XXX handle
                self.logger.error(f"Redis error: {str(e)}")
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
