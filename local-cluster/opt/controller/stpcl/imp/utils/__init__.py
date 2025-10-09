import subprocess
import os
import docker


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


class DockerCli:
    def __init__(self):
        self.client = docker.from_env()
        self.containers = {
            cont.labels["com.docker.compose.service"]: cont
            for cont in self.client.containers.list(
                filters={
                    "label": ["com.docker.compose.project=springtail_local_cluster"],
                }
            )
        }
        print(1)

    def restart_container(self, container_name: str) -> None:
        if container_name not in self.containers:
            raise ValueError(f"Container {container_name} not found")
        container = self.containers[container_name]
        container.restart()

    def exec_in_container(self, container_name: str, cmd: list[str]) -> str:
        if container_name not in self.containers:
            raise ValueError(f"Container {container_name} not found")
        container = self.containers[container_name]
        result = container.exec_run(cmd)
        if result.exit_code != 0:
            raise Exception(f"Error running command {cmd} in container {container_name}: {result.output.decode()}")
        return result.output.decode()

    def exec_systemctl_in_container(self, container_name: str, action: str, service: str) -> str:
        return self.exec_in_container(container_name, ["systemctl", action, service])
