import sys
import os
import time
import logging
import redis
import threading
import traceback
from typing import Dict

from component import Component

# Get the parent directory of the current script (i.e., the project root directory)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the /shared directory to the Python path
sys.path.append(os.path.join(project_root, 'shared'))

# Now import the Properties class
from properties import Properties


class Scheduler:
    """Scheduler class to manage the lifecycle of components"""

    def __init__(self, props: Properties, allowed_timeout_secs: int = 5):
        """
        Initialize a new scheduler
        Arguments:
            props -- the properties object
            allowed_timeout -- the allowed timeout for components in seconds
        """
        self.props = props
        self.redis = props.get_data_redis()
        self.liveness_hash = props.get_liveness_hash()
        self.liveness_pubsub = props.get_liveness_notification_pubsub()
        self.components: Dict[int, Component] = {}
        self.logger = logging.getLogger("Scheduler")
        self.timeouts = {}
        self.allowed_timeout = allowed_timeout_secs * 1000

        # Setup pubsub for liveness notifications
        self.pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
        self.pubsub.subscribe(self.liveness_pubsub)

        self.shutdown_event = threading.Event()

    def register_component(self, component: Component, startup_order: int) -> None:
        """Register a new component with the scheduler"""
        self.components[component.id] = component
        component.set_startup_order(startup_order)
        self.logger.info(f"Registered component {component.name}")

    def start_all(self) -> bool:
        """
        Start all components in order of their startup_order
        Returns:
            True if all components started successfully
        """
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

    def __check_pubsub(self) -> bool:
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

                self.logger.error(f"PubSub: Error for component: {id}: {self.components[id].name}")
                return True

        except redis.TimeoutError:
            return False
        return False

    def __check_timeouts(self) -> bool:
        """
        Check the liveness of all components
        Returns:
            True if any components has failed.
        """
        # Check for timeout messages in Redis
        data = self.redis.hgetall(self.liveness_hash)
        if not data:
            return False

        # Go through and merge time values for the same id, taking minimum
        # key format: daemon_id:thread_id
        # value format: timestamp (epoch ms)
        timeouts: Dict[str, int] = {}
        for key, value in data.items():
            id, _ = key.split(':')
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
        for id, timestamp in self.timeouts.items():
            if timestamp < min_time:
                self.logger.error(f"Timeout for component: {id}: {self.components[id].name}")
                if id in self.components:  # this should always be true
                    return True

        return False

    def __check_components(self) -> bool:
        """
        Check the liveness of all components
        Returns:
            True if any components has failed.
        """
        failed = False
        for id, component in self.components.items():
            if not component.is_alive():
                self.logger.error(f"Component {component.name} is not running")
                failed = True

        return failed

    def shutdown(self) -> None:
        """
        Shutdown the scheduler
        """
        self.shutdown_event.set()

    def monitor_timeouts(self) -> None:
        """
        Monitor Redis queue for timeout events
        """
        while not self.shutdown_event.is_set():
            try:
                if (
                   self.__check_pubsub() or    # check for messages on the pubsub channel
                   self.__check_timeouts() or  # check for timeouts
                   self.__check_components()   # check for component failures
                ):
                    # restart all components
                    self.logger.warning("Restarting all components")
                    if not self.restart_all():
                        # XXX handle
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

        # end of loop, continue
        self.pubsub.close()
        self.redis.close()