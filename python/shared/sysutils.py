import os
import shutil
import time
from typing import Dict, List, Optional

from common import (
    run_command,
    kill_processes,
    running_pids,
    grep_file,
    search_and_capture,
    is_linux
)

POSTGRES = 'postgresql@16'

def stop_daemons(pid_path : str, daemons : List[tuple] = []) -> None:
    """Stop all daemons."""
    # Stop the daemons
    if not os.path.exists(pid_path):
        raise Exception(f"PID path not found: {pid_path}")

    found_pids = []

    # for each .pid file in the pid_path run kill -2 (SIGINT) on the pid in that file
    for file in os.listdir(pid_path):
        daemon_name = file.split('.')[0]
        file = os.path.join(pid_path, file)
        if file.endswith(".pid"):
            with open(file, 'r') as f:
                pid = f.read().strip()
                found_pids.append(pid)
                print(f"Stopping daemon {daemon_name} with pid: {pid}")
                try:
                    run_command('kill', ['-s', 'TERM', pid])
                except Exception as e:
                    # most likely the process is already dead
                    pass
                run_command('rm', [file])

    # kill any lingering daemons after waiting a bit
    retry = 20
    while (retry > 0):
        (pids, _) = running_pids(daemons, found_pids)
        print(f"Waiting for daemons with pids: {pids}")
        if pids is None or len(pids) == 0:
            return
        time.sleep(1)
        retry = retry - 1

    kill_processes(daemons)


def check_daemons_running(daemons : List[tuple]) -> tuple:
    """Check if the daemons are running."""
    # Check if the daemons are running
    (pids, not_running) = running_pids(daemons)
    return (len(pids) == len(daemons), not_running)


def clean_fs(mount_path : str, log_path : str) -> None:
    """Clear the file system data at the given mount path."""
    # Get the mount path
    print(f"Clearing file system data at mount path: {mount_path}")

    # Run the external command to clear the file system data
    run_command('sudo', ['rm', '-rf', mount_path])

    # remove log files
    print(f"Clearing log files at directory: {log_path}")
    run_command('sudo', ['rm', '-rf', log_path])


def move_files(source_dir, destination_dir):
    """Move all files from source directory to destination directory.

    Args:
        source_dir (str): Path to the source directory
        destination_dir (str): Path to the destination directory

    """
    # Create destination directory if it doesn't exist
    if not os.path.exists(destination_dir):
        raise Exception(f'Destination does not exist: {destination_dir}')

    # Get all items in the source directory
    items = os.listdir(source_dir)

    # Move each file to the destination directory
    for item in items:
        item_path = os.path.join(source_dir, item)

        # Check if it's a file (not a directory)
        if os.path.isfile(item_path):
            shutil.move(item_path, os.path.join(destination_dir, item))


def check_postgres_running() -> bool:
    """Check if the postgres process is running and start it if it is not."""
    # check if postgres is running
    pids = running_pids(['postgres'])[0]
    return len(pids) > 0


def restart_postgres() -> None:
    """Restart the postgres process."""
    print("Restarting the postgres process...")

    if is_linux():
        run_command('sudo', ['service', 'postgresql', 'restart'])
    else:
        run_command('brew', ['services', 'restart', POSTGRES])

    iters = 0
    pids = []
    while (len(pids) == 0 and iters < 10):
        time.sleep(1)
        pids = running_pids(['postgres'])[0]
        iters = iters + 1

    if iters == 10 and len(pids) == 0:
        raise Exception("Postgres failed to start, timedout")


def start_postgres() -> None:
    """Start the postgres process using brew on mac."""
    print("Postgres is not running, starting it...")

    # start the postgres process, detect platform
    if is_linux():
        run_command('sudo', ['service', 'postgresql', 'start'])
    else:
        run_command('brew', ['services', 'start', POSTGRES])

    iters = 0
    pids = []
    while (len(pids) == 0 and iters < 10):
        time.sleep(1)
        pids = running_pids(['postgres'])[0]
        iters = iters + 1

    if iters == 10 and len(pids) == 0:
        raise Exception("Postgres failed to start, timedout")


def stop_postgres() -> None:
    """Stop the postgres process using brew."""
    print("Stopping the postgres process...")

    # stop the postgres process, detect platform
    if is_linux():
        run_command('sudo', ['service', 'postgresql', 'stop'])
    else:
        run_command('brew', ['services', 'stop', POSTGRES])

    iters = 0
    pids = []
    while (len(pids) > 0 and iters < 10):
        time.sleep(1)
        pids = running_pids(['postgres'])[0]
        iters = iters + 1

    if len(pids) > 0:
        print("Failed to stop the postgres cleanly, killing the process...")
        kill_processes(['postgres'])
        time.sleep(1)
        pids = running_pids(['postgres'])[0]
        if len(pids) > 0:
            raise Exception("Failed to stop the postgres process, timedout")


def start_daemons(build_dir : str, daemons : List[tuple], restart : bool = True) -> None:
    """Start the daemons."""
    # Check if the daemons are already running
    daemon_names = [daemon[0] for daemon in daemons]
    if check_daemons_running(daemon_names)[0]:
        if restart:
            print("Stopping all daemons...")
            stop_daemons()
        else:
            print("All daemons are already running.")
            return

    # start the daemons
    # daemon is a tuple of (name, path, args)
    for daemon in daemons:
        cmd_dir = os.path.join(build_dir, daemon[1])

        if not os.path.exists(cmd_dir):
            raise Exception(f"Daemon {daemon[0]} not found: {cmd_dir}")

        args = ['--daemonize']
        if len(daemon) > 2:
            args += daemon[2].split(',')

        print(f"Starting daemon: {daemon[0]} with args {args}")
        run_command(cmd_dir, args)

        time.sleep(2)

    # check if all daemons are running
    (all_running, not_running) = check_daemons_running(daemon_names)
    if not all_running:
        for name in not_running:
            print(f"Daemon {name} failed to start")
        raise Exception("Failed to start all daemons")


def running_daemons(daemons : List[tuple]) -> List[Dict]:
    """Return a list of process IDs for the running daemons."""
    return running_pids(daemons)[0]


def check_backtrace(log_path : str) -> List[str]:
    """Check if any daemon log files contain a backtrace."""
    failed_files = []

    # iterate over files in mount_path
    for f in os.listdir(log_path):
        f = os.path.join(log_path, f)
        if os.path.isfile(f) and f.endswith('.log'):
            # check if the file contains a backtrace
            if grep_file(f, ['backtrace_handler', 'Stack trace']):
                failed_files.append(f)

    return failed_files


def extract_backtrace(file: str) -> List[str]:
    """Extract the backtrace from the given file."""
    lines = search_and_capture(file, ['backtrace_handler', 'Stack trace'])
    for i in range(len(lines)-1, 0, -1):
        if lines[i][0] != '#':
            lines.pop()
        else:
            break
    return lines
