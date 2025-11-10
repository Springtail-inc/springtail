import argparse
from enum import StrEnum
import logging
import os
import sys
from test_database_operations import setup_logging, DatabaseTester, DatabaseOperation
import time

# Get the parent directory of the current script (i.e., the project root directory)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the /shared directory to the Python path
sys.path.append(os.path.join(project_root, 'shared'))
sys.path.append(os.path.join(project_root, 'controller'))

# Now import the classes
from properties import Properties
from stpcl.imp.utils import DockerCli

class TestAction(StrEnum):
    """Test Action Enumeration"""
    TEST_ADD_REMOVE_DB = 'add_remove_db'

class ContainerType(StrEnum):
    """Container Type Enumeration"""
    INGESTION = "ingestion"
    FDW = "fdw"
    PROXY = "proxy"

class CoordinatorState(StrEnum):
    """Coordinator State Enumeration"""
    STARTUP = "startup"
    RELOADING = "reloading"
    RUNNING = "running"
    DEAD = "dead"

class ClusterTester:
    """
    This class that performs all cluster tests.
    """

    def __init__(self):
        """
        Initialize ClusterTester object.
        """
        self.logger = logging.getLogger('springtail')
        self.props = Properties()

        self.redis = self.props.get_config_redis()
        self.instance_id = self.props.get_db_instance_id()

        self._read_containers()

        self.docker_cli = DockerCli()
        self.database_tester = DatabaseTester()

        pass

    def _read_containers(self):
        """
        Read the list of all containers from redis.
        """
        coordinator_hash_name = f"{self.instance_id}:coordinator_state"
        cluster_containers = self.redis.hgetall(coordinator_hash_name)
        self.containers = {}
        for key, value in cluster_containers.items():
            key_parts = key.split(':')
            if len(key_parts) == 2:
                container_type = key_parts[0]
                instance_key = key_parts[1]

                # Organize containers by type
                if container_type not in self.containers:
                    self.containers[container_type] = []

                host = container_type
                if container_type == ContainerType.FDW:
                    host = instance_key
                self.containers[container_type].append({
                    'instance_key': instance_key,
                    'host': host,
                    'state': value
                })

    def _wait_for_state(self, container_type: ContainerType, state: CoordinatorState):
        """
        Wait for all containers of the given type to change state to the given state.
        """
        completed_count = 0
        coordinator_hash_name = f"{self.instance_id}:coordinator_state"
        while completed_count < len(self.containers[container_type]):
            completed_count = 0
            time.sleep(1)
            for container in self.containers[container_type]:
                key = f"{container_type}:{container['instance_key']}"
                container_state = self.redis.hget(coordinator_hash_name, key)
                self.logger.debug(f"Container '{container['instance_key']}' state '{container_state}' ")
                if container_state == state:
                    completed_count += 1

    def _start_containers(self, container_type: ContainerType):
        """
        Start all containers of the given type.
        """
        self.logger.info(f"Starting containers of type '{container_type}'")
        for container in self.containers[container_type]:
            out = self.docker_cli.exec_in_container(f'{container['host']}', ['systemctl', 'start', 'springtail-coordinator'])
            out = out.strip()
            if out != 'springtail-coordinator: started':
                raise SystemExit(f"Failed to start container '{container}': '{out}'")

        self._wait_for_state(container_type, CoordinatorState.RUNNING)
        self.logger.info(f"Started all containers of type '{container_type}'")

    def _stop_containers(self, container_type: ContainerType):
        """
        Stop all containers of the given type.
        """
        for container in self.containers[container_type]:
            out = self.docker_cli.exec_in_container(f'{container['host']}', ['systemctl', 'stop', 'springtail-coordinator'])
            out = out.strip()
            if out != 'springtail-coordinator: stopped':
                raise SystemExit(f"Failed to stop container '{self.container_name}': '{out}'")

        self._wait_for_state(container_type, CoordinatorState.DEAD)
        self.logger.info(f"Stopped all containers of type '{container_type}'")

    def _start_cluster(self):
        """
        Start all springtail containers in the cluster.
        """
        self._start_containers(ContainerType.INGESTION)
        self._start_containers(ContainerType.FDW)
        self._start_containers(ContainerType.PROXY)

    def _stop_cluster(self):
        """
        Stop all springtail containers in the cluster.
        """
        self._stop_containers(ContainerType.PROXY)
        self._stop_containers(ContainerType.FDW)
        self._stop_containers(ContainerType.INGESTION)

    def run_test(self, action: TestAction, database_name: str, database_id: int):
        """
        Run specific test.
        """
        match action:
            case TestAction.TEST_ADD_REMOVE_DB:
                # 1. Start all containers
                self._start_cluster()

                # 2. Add database
                self.database_tester.run_test(database_name, database_id, DatabaseOperation.ADD_DB)

                # 3. Remove database
                self.database_tester.run_test(database_name, database_id, DatabaseOperation.REMOVE_DB)

                # 4. Add database
                self.database_tester.run_test(database_name, database_id, DatabaseOperation.ADD_DB)

                # 5. Remove database under load
                self.database_tester.run_test(database_name, database_id, DatabaseOperation.REMOVE_DB_UNDER_LOAD)

                # 6. Stop all containers
                self._stop_cluster()


def parse_arguments() -> argparse.Namespace:
    """
    Parse the command line arguments.
    """
    # Create the argument parser
    parser = argparse.ArgumentParser(description="Tool for testing database add and remove operations.")
    parser.add_argument('-a', '--action', type=TestAction, required=True, choices=list(TestAction),
                        help="Specify cluster test to perform.")
    parser.add_argument('-d', '--database', type=str, required=True,
                        help='Specify database name.')
    parser.add_argument('-i', '--database_id', type=int, required=True,
                        help='Specify database id.')

    args = parser.parse_args()
    return args

def main() -> None:
    """
    Main function to database testing tool.
    """
    # Parse command line arguments
    args = parse_arguments()
    setup_logging()

    tester = ClusterTester()
    tester.run_test(args.action, args.database, args.database_id)

if __name__ == "__main__":
    main()
