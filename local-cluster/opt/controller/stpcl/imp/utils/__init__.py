import subprocess
import os

def run_psql(host: str, port: int, user: str, password: str, dbname: str, sql: str) -> None:

    env = {
        **os.environ,
        "PGPASSWORD": password,
    }

    cmd = ["psql", "-h", host, "-p", str(port), "-U", user, "-d", dbname,
           "-c", sql]

    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Error running psql command: {result.stderr}")
    print(result.stdout)


def run_psql_files(host: str, port: int, user: str, password: str, dbname: str, files: list[str]) -> None:

    env = {
        **os.environ,
        "PGPASSWORD": password,
    }
    if not files:
        raise ValueError("No SQL files provided to run_psql_files")
    cmd = ["psql", "-h", host, "-p", str(port), "-U", user, "-d", dbname]
    for file in files:
        cmd.append("-f")
        cmd.append(file)

    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Error running psql file {files}: {result.stderr}")
    print(result.stdout)
