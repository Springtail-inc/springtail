import threading
import time
import os
import signal
import logging
import psycopg2
import sys
import argparse
import yaml
import traceback
from typing import Optional

# Get the parent directory of the current script (i.e., the project root directory)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the /shared directory to the Python path
sys.path.append(os.path.join(project_root, 'shared'))
sys.path.append(os.path.join(project_root, 'grpc'))

# Now import the Properties class
from properties import Properties

from springtail import (
    start,
    connect_db_instance,
    check_logs,
    CORE_DAEMONS
)

from sysutils import (
    start_daemons,
    check_daemons_running,
)

def insert_worker(db_name : str, props : Properties, stop_event : threading.Event, batch_size : int = 10):
    """Thread worker that connects to the primary DB and issues batches of inserts."""
    conn : Optional[psycopg2.extensions.connection] = None
    cursor : Optional[psycopg2.extensions.cursor] = None

    print(f"Insert worker started for database: {db_name} with batch size: {batch_size}")

    itr = 0
    while not stop_event.is_set():
        try:
            if cursor is None or conn is None or conn.closed:
                conn = connect_db_instance(props, db_name)
                cursor = conn.cursor()
            cursor.execute("BEGIN")
            cursor.execute(
                f"""INSERT INTO test_table (email, name, age)
                    SELECT
                        md5(random()::text) || '@example.com',
                        md5(random()::text),
                        (random() * 52 + 18)::BIGINT
                    FROM generate_series(1, {batch_size}) AS s;"""
            )
            cursor.execute("COMMIT")
            itr += 1
            print(f"Inserted batch {itr} into test_table")
            time.sleep(0.2)

        except Exception as e:
            logging.error(f"Insert worker error: {e}")
            if cursor:
                cursor.close()
            if conn:
                conn.close()
            conn = None
            cursor = None
            time.sleep(5)
    if cursor:
        cursor.close()
    if conn:
        conn.close()


def kill_core_daemons(pid_path):
    """Send SIGKILL to the core daemons."""
    pids = []
    for fname in os.listdir(pid_path):
        if fname in ['pg_log_mgr.pid', 'sys_tbl_mgr.pid']:
            with open(os.path.join(pid_path, fname), 'r') as f:
                pids.append(int(f.read().strip()))
    if len(pids) == len(CORE_DAEMONS):
        for pid in pids:
            print(f"Killing core daemons with PID {pid}")
            os.kill(pid, signal.SIGKILL)
    else:
        raise Exception("Core daemons PIDs not found")

def restart_core_daemons(build_dir):
    """Restart the core daemons."""
    for daemon in CORE_DAEMONS:
        start_daemons(build_dir, [daemon])
        print(f"{daemon[0]} restarted")

def parse_arguments() -> argparse.Namespace:
    """Parse the command line arguments."""
    # Create the argument parser
    parser = argparse.ArgumentParser(description="Process command-line arguments for config file and build directory.")

    # Add arguments -f for config file
    parser.add_argument('-f', '--config-file', type=str, required=False, help='Path to the configuration file')
    parser.add_argument('--repeat', type=int, default=1, help='Number of types log_mgr to be restarted')
    parser.add_argument('-b', '--batch-size', type=int, default=10, help='Number of rows to insert in each batch')

    args = parser.parse_args()
    if not args.config_file:
        args.config_file = os.path.join(os.getcwd(), "config.yaml")

    return args


def main():
    """Main function to run the test."""
    # Set up logging
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)
    logging.getLogger('grpc').setLevel(logging.WARNING)
    logging.basicConfig(level=logging.INFO)

    # Parse command line arguments
    args = parse_arguments()
    repeat_restarts = args.repeat

    # Load the configuration file
    config_file = args.config_file
    if not os.path.isfile(args.config_file):
        raise FileNotFoundError(f"Config file does not exist: {args.config_file}")
    with open(config_file, 'r') as f:
        config_data = yaml.safe_load(f)
    build_dir = os.path.abspath(os.path.join(os.path.dirname(config_file), config_data['build_dir']))
    system_json_path = os.path.abspath(os.path.join(os.path.dirname(config_file), config_data['system_json_path']))

    props = Properties(system_json_path, True)

    db_configs = props.get_db_configs()
    db_config = next(iter(db_configs), None)
    if not db_config:
        raise ValueError("No database configuration found in properties")

    db_name = db_config.get("name")
    if not db_name:
        raise ValueError("No database name found in database configuration")

    # Start up the system
    print("Starting Springtail system...")
    start(system_json_path, build_dir, do_cleanup=True, do_init=True)

    # Wait for daemons to be running
    print("Waiting for daemons to be running...")
    all_running, not_running = check_daemons_running([d[0] for d in CORE_DAEMONS])
    if not all_running:
        raise Exception(f"Not all core daemons running: {not_running}")

    # Check logs for any issues
    if check_logs(system_json_path):
        raise Exception("Issues found in logs")

    # Prepare test table
    conn = connect_db_instance(props, db_name)
    cursor = conn.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS test_table
                      (id SERIAL PRIMARY KEY, email TEXT, name TEXT, age BIGINT);""")
    conn.commit()
    cursor.close()
    conn.close()

    # Start insert thread
    stop_event = threading.Event()
    insert_thread = threading.Thread(target=insert_worker, args=(db_name, props, stop_event, args.batch_size))
    insert_thread.start()

    # Let the insert thread run for 5 seconds
    time.sleep(5)

    # Check logs for any issues
    if check_logs(system_json_path):
        raise Exception("Issues found in logs")

    while repeat_restarts > 0:
        # Kill the core daemons with SIGKILL
        pid_path = props.get_pid_path()
        kill_core_daemons(pid_path)

        # Wait a moment to ensure process is dead
        time.sleep(2)

        # Restart the core daemons
        restart_core_daemons(build_dir)

        print("Waiting for daemons to be running...")
        all_running, not_running = check_daemons_running([d[0] for d in CORE_DAEMONS])
        if not all_running:
            check_logs(system_json_path)
            raise Exception(f"Not all core daemons running: {not_running}")
        repeat_restarts -= 1;

    stop_event.set()
    insert_thread.join(timeout=2)

    # Check logs for any issues
    if check_logs(system_json_path):
        raise Exception("Issues found in logs")

    print("Test complete.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"Error occurred: {e}")
        logging.error(traceback.format_exc())
        sys.exit(1)

    sys.exit(0)
