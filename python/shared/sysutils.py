import os
import shutil
import time
import socket
import http.client
import subprocess
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


def restart_container(container_name: str) -> bool:
    """Restart a Docker container using the Docker socket."""
    docker_sock = "/var/run/docker.sock"
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

    try:
        sock.connect(docker_sock)
        conn = http.client.HTTPConnection('localhost')
        conn.sock = sock  # Directly assign the UNIX socket

        path = f"/containers/{container_name}/restart"
        conn.request("POST", path)

        response = conn.getresponse()
        status = response.status
        response.read()  # Read to clean up the socket

        if status == 204:
            time.sleep(1)  # Give some time for the container to restart
            print(f"Restarted container '{container_name}' successfully.")
            return True
        else:
            print(f"Failed to restart container: HTTP {status}")
            return False
    except Exception as e:
        print(f"Error: {e}")
        return False
    finally:
        sock.close()


def stop_daemons(pid_path : str, daemons : List[str] = []) -> None:
    """Stop all daemons in the order specified by the daemons list, waiting for each to shutdown before proceeding."""
    # Stop the daemons
    if not os.path.exists(pid_path):
        print(f"PID path: {pid_path} does not exist")
        return

    all_found_pids = []

    # Stop daemons in the order specified by the daemons list, waiting for each to complete
    for daemon in daemons:

        pid_file = os.path.join(pid_path, f"{daemon}.pid")

        if os.path.exists(pid_file):
            with open(pid_file, 'r') as f:
                pid = f.read().strip()
                all_found_pids.append(pid)
                print(f"Stopping daemon {daemon} with pid: {pid}")
                try:
                    run_command('kill', ['-s', 'TERM', pid])
                except Exception as e:
                    # most likely the process is already dead
                    pass
                run_command('rm', [pid_file])

                # Wait for this specific daemon to shutdown before proceeding to next
                retry = 20
                while retry > 0:
                    (pids, _) = running_pids([daemon], [pid])
                    if pids is None or len(pids) == 0:
                        print(f"Daemon {daemon} has stopped")
                        break
                    time.sleep(1)
                    retry -= 1

                # If daemon didn't stop gracefully, force kill it before proceeding
                if retry == 0:
                    print(f"Force killing daemon {daemon}")
                    kill_processes([daemon])
        else:
            print("Failed to find pid file: ", pid_file)

    # Final check for any remaining processes
    (remaining_pids, _) = running_pids(daemons, all_found_pids)
    if remaining_pids and len(remaining_pids) > 0:
        print(f"Force killing remaining daemons with pids: {remaining_pids}")
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


def start_daemons(build_dir : str,
                  daemons : List[tuple],
                  restart : bool = True,
                  valgrind_daemons: List[str] = [],
                  pid_path: Optional[str] = None,
                  log_path: Optional[str] = None) -> None:
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

    # Track valgrind parent PIDs
    valgrind_pids = {}

    # start the daemons
    # daemon is a tuple of (name, path, args)
    for daemon in daemons:
        daemon_name = daemon[0]
        cmd_dir = os.path.join(build_dir, daemon[1])

        if not os.path.exists(cmd_dir):
            raise Exception(f"Daemon {daemon_name} not found: {cmd_dir}")

        # Check if this daemon should be run with valgrind
        use_valgrind = daemon_name in valgrind_daemons

        if use_valgrind:
            if not pid_path or not log_path:
                raise Exception(f"Cannot start {daemon_name} with valgrind: pid_path and log_path must be provided")

            # Build valgrind command
            valgrind_log = os.path.join(log_path, f'{daemon_name}.valgrind.log')
            cmd = ['valgrind', '--tool=memcheck', '--leak-check=no', '--track-origins=yes',
                   f'--log-file={valgrind_log}', cmd_dir]

            # Add daemon-specific args (but not --daemonize)
            if len(daemon) > 2:
                cmd += daemon[2].split(',')

            print(f"Starting daemon: {daemon_name} with valgrind, log: {valgrind_log}")

            # Start process in background and capture PID
            proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

            # Store the valgrind PID for verification
            valgrind_pids[daemon_name] = proc.pid

            # Write PID file with the valgrind PID (for cleanup)
            pid_file = os.path.join(pid_path, f'{daemon_name}.pid')
            with open(pid_file, 'w') as f:
                f.write(str(proc.pid))

            print(f"Started {daemon_name} with valgrind, PID: {proc.pid}")
        else:
            # Normal daemon start with --daemonize
            args = ['--daemonize']
            if len(daemon) > 2:
                args += daemon[2].split(',')

            print(f"Starting daemon: {daemon_name} with args {args}")
            run_command(cmd_dir, args)

        time.sleep(2)

    # Check if all daemons are running
    # For valgrind daemons, check by PID; for normal daemons, check by name
    non_valgrind_daemons = [name for name in daemon_names if name not in valgrind_daemons]

    # Check non-valgrind daemons by name
    if non_valgrind_daemons:
        (all_running, not_running) = check_daemons_running(non_valgrind_daemons)
        if not all_running:
            for name in not_running:
                print(f"Daemon {name} failed to start")
            raise Exception("Failed to start all daemons")

    # Check valgrind daemons by verifying the PID is still running
    import psutil
    for daemon_name, pid in valgrind_pids.items():
        try:
            proc = psutil.Process(pid)
            if proc.is_running():
                print(f"Daemon {daemon_name} confirmed running under valgrind (PID: {pid})")
            else:
                print(f"Daemon {daemon_name} failed to start - valgrind process not running")
                raise Exception(f"Daemon {daemon_name} failed to start")
        except psutil.NoSuchProcess:
            print(f"Daemon {daemon_name} failed to start - valgrind process {pid} not found")
            raise Exception(f"Daemon {daemon_name} failed to start")


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
