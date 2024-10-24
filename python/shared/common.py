import os
import psutil
import subprocess
import psycopg2
import platform
import re
from typing import Dict, List, Optional

def connect_db(dbname : str, username : str, password :str, host : str, port : int, autocommit : bool=True) -> psycopg2.extensions.connection:
    """Connect to the given database with the given username, password, host, and port."""
    print(f"Connecting to database: {dbname} with username: {username} and host: {host}")
    try:
        conn = psycopg2.connect(dbname=dbname, user=username, password=password, host=host, port=port)
        if autocommit:
            conn.autocommit = True
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise e

    return conn


def execute_sql(conn, sql : str, args=None) -> None:
    """Execute the given sql statement with the given arguments on the given connection."""
    print(f"Executing sql: {sql} with args: {args}")
    try:
        if isinstance(args, str):
            args = (args,)
        cur = conn.cursor()
        cur.execute(sql, args)
    except Exception as e:
        print(f"Error executing sql: {e}")
        raise e

    return


def execute_sql_script(conn, script_file : str) -> None:
    """Execute the given sql script file on the given connection."""
    print(f"Executing sql script: {script_file}")
    if not os.path.exists(script_file):
        print(f"Script sql file not found: {script_file}")
        raise Exception(f"Script sql file not found: {script_file}")
    try:
        cur = conn.cursor()
        with open(script_file, 'r') as f:
            cur.execute(f.read())
    except Exception as e:
        print(f"Error executing sql script {script_file}: {e}")
        raise e

    return


# execute a sql select statement and return the result
def execute_sql_select(conn, sql : str, args=None) -> List:
    """Execute the given sql select statement with the given arguments on the given connection and return the result."""
    print(f"Executing sql select: {sql} with args: {args}")
    try:
        if isinstance(args, str):
            args = (args,)
        cur = conn.cursor()
        cur.execute(sql, args)
        result = cur.fetchall()
    except Exception as e:
        print(f"Error executing sql: {e}")
        raise e

    return result


def running_pids(names : List[str]) -> tuple:
    """Return a list of process IDs for the given process names."""
    pids = []
    not_running = []
    running_names = {}
    for proc in psutil.process_iter(['pid', 'name']):
        if proc.info['name'] in names:
            pids.append({'name':proc.info['name'], 'pid':proc.info['pid']})
            running_names[proc.info['name']] = True

    for name in names:
        if name not in running_names:
            not_running.append(name)

    return (pids, not_running)


def kill_processes(names : List[str]):
    """Kill the processes with the given names."""
    for proc in psutil.process_iter(['pid', 'name']):
        if proc.info['name'] in names:
            print(f"Killing process {proc.info['name']} with PID: {proc.info['pid']}")
            proc.kill()


def run_command(command, args : List[str], outfile : str = None) -> Optional[str]:
    """Run the given command with the given arguments and return the last line of the output."""
    command_with_args = [command] + args

    # Run the external command
    result = None
    if outfile:
        with open(outfile, 'w') as f:
            result = subprocess.run(command_with_args, stdout=f, stderr=subprocess.PIPE, text=True, shell=False)
    else:
        result = subprocess.run(command_with_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=False)

    # Check if the command was successful
    if result.returncode != 0:
        raise Exception(f"Command failed with error: {result.stderr}")

    if outfile:
        return None

    # Split the output into lines and get the last line
    output_lines = result.stdout.strip().split('\n')
    last_line = output_lines[-1] if output_lines else None

    return last_line


def grep_file(file_path: str, search_strings: List[str]) -> bool:
    """Search for a line in a file.
    Arguments:
    file_path -- the path to the file to search
    search_lines -- a list of strings to search for
    Returns:
    True if any of the search strings are found in the file, False otherwise
    """
    try:
        with open(file_path, 'r') as file:
            for line in file:
                for search_line in search_strings:
                    if search_line in line:
                        return True
        return False
    except FileNotFoundError:
        print(f"The file {file_path} was not found.")
        return False


def remove_ansi_escapes(text):
    """Remove ANSI escape sequences from a string."""
    # Regular expression pattern to match ANSI escape sequences
    ansi_escape_pattern = r'\x1B\[\d{1,2}m'
    return re.sub(ansi_escape_pattern, '', text)


def search_and_capture(filename: str, search_strings: List[str]) -> List[str]:
    """Search for a string in a file and capture all lines after the string is found."""
    found_string = False
    captured_output = []

    # Open the file and read line by line
    with open(filename, 'r') as file:
        for line in file:
            if found_string:
                # If the search string is found, start capturing the output
                captured_output.append(remove_ansi_escapes(line))
            else:
                for search_line in search_strings:
                    if search_line in line:
                        found_string = True
                        captured_output.append(remove_ansi_escapes(line))

    # return the captured output as a list of lines
    return captured_output


def set_dir_writable(dir) -> None:
    """Check the log path is writable."""
    if not os.path.exists(dir):
        # make directory writable by owner, group, and others
        os.makedirs(dir, mode=0o777)
        return

    if not os.access(dir, os.W_OK) or not (os.stat(dir).st_mode & 0o0007 == 0o0007):
        # set writable by owner, group, and others
        os.chmod(dir, 0o777)


def is_linux() -> bool:
    """
    Check if the system is Linux.
    Returns: True if the system is Linux, False otherwise.
    """
    return platform.system() == 'Linux'