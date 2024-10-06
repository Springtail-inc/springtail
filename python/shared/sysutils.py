import os
import platform
import time

from common import run_command, kill_processes, running_pids

DAEMONS = [
    'xid_mgr_daemon',
    'sys_tbl_mgr_daemon',
    'write_cache_daemon',
    'gc_daemon',
    'pg_log_mgr_daemon'
]

POSTGRES = 'postgresql@16'

def stop_daemons(pid_path):
    """Stop the daemons."""
    # Stop the daemons
    # for each .pid file in the pid_path run kill -9 on the pid in that file
    for file in os.listdir(pid_path):
        file = os.path.join(pid_path, file)
        if file.endswith(".pid"):
            print(f"Stopping daemon with pid file: {file}")
            with open(file, 'r') as f:
                pid = f.read()
                try:
                    run_command('kill', ['-9', pid])
                except Exception as e:
                    # most likely the process is already dead
                    pass
                run_command('rm', [file])

    # kill any lingering daemons
    kill_processes(DAEMONS)


def check_daemons_running():
    """Check if the daemons are running."""
    # Check if the daemons are running
    pids = running_pids(DAEMONS)
    return len(pids) == len(DAEMONS)


def clean_fs(mount_path, log_path):
    """Clear the file system data at the given mount path."""
    # Get the mount path
    print(f"Clearing file system data at mount path: {mount_path}")

    # Run the external command to clear the file system data
    run_command('rm', ['-rf', mount_path])
    log_dir = os.path.dirname(log_path)

    # remove log files
    print(f"Clearing log files at directory: {log_dir}")
    run_command('rm', ['-f', log_dir + '/*.log'])


def check_postgres_running():
    """Check if the postgres process is running and start it if it is not."""
    # check if postgres is running
    pids = running_pids(['postgres'])
    return len(pids) > 0


def start_postgres():
    """Start the postgres process using brew on mac."""
    print("Postgres is not running, starting it...")

    # start the postgres process, detect platform
    system = platform.system()
    if system == 'Darwin':
        run_command('brew', ['services', 'start', POSTGRES])
    elif system == 'Linux':
        run_command('sudo', ['service', 'postgresql', 'start'])

    iters = 0
    pids = []
    while (len(pids) == 0 and iters < 10):
        time.sleep(1)
        pids = running_pids(['postgres'])
        iters = iters + 1

    if iters == 10 and len(pids) == 0:
        raise Exception("Postgres failed to start, timedout")


def stop_postgres():
    """Stop the postgres process using brew."""
    print("Stopping the postgres process...")

    # stop the postgres process, detect platform
    system = platform.system()
    if system == 'Darwin':
        run_command('brew', ['services', 'stop', POSTGRES])
    elif system == 'Linux':
        run_command('sudo', ['service', 'postgresql', 'stop'])

    pids = running_pids(['postgres'])
    if len(pids) > 0:
        print("Failed to stop the postgres cleanly, killing the process...")
        kill_processes(['postgres'])

    pids = running_pids(['postgres'])
    if len(pids) > 0:
        raise Exception("Failed to stop the postgres process, timedout")


def start_daemons(build_dir, restart=True):
    """Start the daemons."""
    # Check if the daemons are already running
    if check_daemons_running():
        if restart:
            print("Stopping all daemons...")
            stop_daemons()
        else:
            print("All daemons are already running.")
            return

    # Start the springtail xid manager daemon
    print("Starting the Springtail xid manager daemon...")
    xid_bin = os.path.join(build_dir, 'src/xid_mgr/xid_mgr_daemon')
    run_command(xid_bin, ['-x', '10', '--daemon'])

    time.sleep(1)

    # Start the sys table mgr daemon
    print("Starting the Springtail system table manager daemon...")
    sys_tbl_mgr_bin = os.path.join(build_dir, 'src/sys_tbl_mgr/sys_tbl_mgr_daemon')
    run_command(sys_tbl_mgr_bin, ['--daemon'])

    time.sleep(1)

    # Start the springtail write cache daemon
    print("Starting the Springtail write cache daemon...")
    write_cache_bin = os.path.join(build_dir, 'src/write_cache/write_cache_daemon')
    run_command(write_cache_bin, ['--daemon'])

    time.sleep(1)

    # Start the garbage collector daemon
    print("Starting the Springtail garbage collector daemon...")
    gc_bin = os.path.join(build_dir, 'src/garbage_collector/gc_daemon')
    run_command(gc_bin, ['--daemon'])

    time.sleep(1)

    # Start the springtail log mgr daemon
    print("Starting the Springtail log mgr daemon...")
    log_mgr_bin = os.path.join(build_dir, 'src/pg_log_mgr/pg_log_mgr_daemon')
    run_command(log_mgr_bin, ['--daemon'])

    time.sleep(1)

    # check if all daemons are running
    if not check_daemons_running():
        raise Exception("Failed to start all daemons")

def running_daemons():
    """Return a list of process IDs for the running daemons."""
    return running_pids(DAEMONS)