import os
import psutil
import subprocess
import psycopg2


def connect_db(dbname, username, password, host, port, autocommit=True):
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


def execute_sql(conn, sql, args=None):
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


def execute_sql_script(conn, script_file):
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
def execute_sql_select(conn, sql, args=None):
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


def running_pids(names):
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


def kill_processes(names):
    """Kill the processes with the given names."""
    for proc in psutil.process_iter(['pid', 'name']):
        if proc.info['name'] in names:
            print(f"Killing process {proc.info['name']} with PID: {proc.info['pid']}")
            proc.kill()


def run_command(command, args):
    """Run the given command with the given arguments and return the last line of the output."""
    command_with_args = [command] + args
    # Run the external command
    result = subprocess.run(command_with_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=False)

    # Check if the command was successful
    if result.returncode != 0:
        raise Exception(f"Command failed with error: {result.stderr}")

    # Split the output into lines and get the last line
    output_lines = result.stdout.strip().split('\n')
    last_line = output_lines[-1] if output_lines else None

    return last_line