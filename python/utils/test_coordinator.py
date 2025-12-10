import argparse
import json
import os
from redis import Redis
import time
from typing import Optional
import sys
from enum import StrEnum

# Get the parent directory of the current script (i.e., the project root directory)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the /shared directory to the Python path
sys.path.append(os.path.join(project_root, 'shared'))
sys.path.append(os.path.join(project_root, 'controller'))

# Now import the Properties class
from properties import Properties
from stpcl.imp.utils import DockerCli

class CoordinatorState(StrEnum):
    """Coordinator state class"""
    SHUTDOWN = 'shutdown'
    STARTUP = 'startup'
    RUNNING = 'running'
    DEAD = 'dead'
    RELOAD = 'reload'
    RELOADING = 'reloading'


class CoordinatorTester:
    def __init__(self,
                 container_name: str,
                 instance_key: str):
        print("Initializing CoordinatorTester")
        self.props = Properties()

        self.redis = self.props.get_config_redis()
        self.instance_id = self.props.get_db_instance_id()

        self.container_name = container_name
        self.instance_key = instance_key

        self.docker_cli = DockerCli()

    def _get_container_state(self) -> Optional[CoordinatorState]:
        """Get container state from redis."""
        coordinator_hash_name = f"{self.instance_id}:coordinator_state"
        coordinator_key = f"{self.container_name}:{self.instance_key}"
        state = self.redis.hget(coordinator_hash_name, coordinator_key)
        print(f"Coordinator hash '{coordinator_hash_name}', coordinator_key '{coordinator_key}', state '{state}'")
        if state is not None:
            return CoordinatorState(state)
        return None

    def _set_container_state(self, state: CoordinatorState) -> None:
        """Set container state in redis."""
        coordinator_hash_name = f"{self.instance_id}:coordinator_state"
        coordinator_key = f"{self.container_name}:{self.instance_key}"
        self.redis.hset(coordinator_hash_name, coordinator_key, state.value)

    def _get_fdw_state(self) -> Optional[str]:
        """Get FDW state in redis for specific instance key."""
        fdw_key = self.instance_id + ':fdw'
        config_str = self.redis.hget(fdw_key, self.instance_key)
        if config_str is None:
            raise SystemExit(f"FDW configuration is not found in redis")
        current_config = json.loads(config_str)
        return current_config['state']

    def _set_fdw_state(self, state: str) -> None:
        """Set FDW state in redis for specific instance key."""
        fdw_key = self.instance_id + ':fdw'
        config_str = self.redis.hget(fdw_key, self.instance_key)
        if config_str is None:
            raise SystemExit(f"FDW configuration is not found in redis")
        current_config = json.loads(config_str)
        current_config['state'] = state
        self.redis.hset(fdw_key, self.instance_key, json.dumps(current_config))

    def _wait_for_fdw_state(self, state: str, timeout: int = 60) -> None:
        """Wait for FDW state to reach the desired state.
        :param state: the state to wait for
        :param timeout: the maximum time to wait in seconds
        """
        fdw_key = self.instance_id + ':fdw'
        start = time.time()
        while True:
            config_str = self.redis.hget(fdw_key, self.instance_key)
            if config_str is None:
                raise SystemExit(f"FDW configuration is not found in redis")
            current_config = json.loads(config_str)
            print(f"current FDW state = '{current_config['state']}'")
            if current_config['state'] == state:
                return
            time.sleep(1)
            if timeout != 0 and time.time() - start > timeout:
                break

        if timeout != 0:
            raise TimeoutError(f'Timed out waiting for FDW state: {state}')


    def _wait_for_container_state(self, state: CoordinatorState, timeout: int = 60) -> None:
        """Wait for the container state to reach the desired state.
        :param state: the state to wait for
        :param timeout: the maximum time to wait in seconds
        """
        coordinator_hash_name = f"{self.instance_id}:coordinator_state"
        coordinator_key = f"{self.container_name}:{self.instance_key}"
        start = time.time()
        while True:
            current_state = self.redis.hget(coordinator_hash_name, coordinator_key)
            if current_state is None:
                raise SystemExit(f"Failed to get container '{self.container_name}' state")
            print(f"current container state = '{current_state}'")
            if CoordinatorState(current_state) == state:
                return
            time.sleep(1)
            if timeout != 0 and time.time() - start > timeout:
                break

        if timeout != 0:
            raise TimeoutError(f'Timed out waiting for container state: {state.value}')

    def _start_container(self):
        container_name = self.container_name
        if self.instance_key != 'default':
            container_name = self.instance_key
        out = self.docker_cli.exec_in_container(container_name, ['systemctl', 'start', 'springtail-coordinator'])
        out = out.strip()
        if out != 'springtail-coordinator: started':
            raise SystemExit(f"Failed to start container '{self.container_name}': '{out}'")
        print(f"Started container returned: '{out}'")
        self._wait_for_container_state(CoordinatorState.RUNNING)

    def _stop_container(self):
        self._set_container_state(CoordinatorState.SHUTDOWN)
        self._wait_for_container_state(CoordinatorState.DEAD)

    def run_test(self, action: str):
        state = self._get_container_state()

        match action:
            case "reload":
                if state != CoordinatorState.RUNNING:
                    raise SystemExit(f"Unable to reload container that is not in the 'running' state")
                self._set_container_state(CoordinatorState.RELOAD)
                self._wait_for_container_state(CoordinatorState.RUNNING)

            case "shutdown":
                if state != CoordinatorState.RUNNING:
                    raise SystemExit(f"Unable to shutdown container that is not in the 'running' state")

                self._stop_container()

            case "start":
                if state != CoordinatorState.STARTUP:
                    raise SystemExit(f"Unable to start container that is not in the 'startup' state")

                self._start_container()

            case "restart":
                if state != CoordinatorState.RUNNING:
                    raise SystemExit(f"Unable to shutdown container that is not in the 'running' state")

                self._stop_container()

                self._set_container_state(CoordinatorState.STARTUP)

                self._start_container()

            case "drain":
                if state != CoordinatorState.RUNNING:
                    raise SystemExit(f"Unable to drain container that is not in the 'running' state")

                fdw_state = self._get_fdw_state()
                if state != "running":
                    raise SystemExit(f"Unable to drain container when FDW is not in the 'running' state")

                self._set_fdw_state('draining')
                self._wait_for_fdw_state('stopped')
                state = self._get_container_state()
                if state != CoordinatorState.RUNNING:
                    raise SystemExit(f"Unexpected container state change to '{state.value}'")


def parse_arguments() -> argparse.Namespace:
    """Parse the command line arguments."""
    # Create the argument parser
    parser = argparse.ArgumentParser(description="Coordinator testing tool.")

    parser.add_argument('-c', '--container', type=str, required=True, choices=['ingestion', 'fdw', 'proxy'],
                        help='Specify which container to test.')
    parser.add_argument('-i', '--instance_key', type=str, required=True,
                        help="Specify container instance key.")
    parser.add_argument('-a', '--action', type=str, required=True, choices=['reload', 'shutdown', 'start', 'restart', 'drain'],
                        help="Specify container test action. Action 'drain' can only be used with 'fdw' container.")
    args = parser.parse_args()

    if args.action == 'drain' and args.container != 'fdw':
        raise SystemExit(f"Can't use action '{args.action}' with '{args.container}' container")

    return args

def main() -> None:
    """Main function to admin console tool."""
    # Parse command line arguments
    args = parse_arguments()

    tester = CoordinatorTester(args.container, args.instance_key)

    tester.run_test(args.action)

if __name__ == "__main__":
    main()
