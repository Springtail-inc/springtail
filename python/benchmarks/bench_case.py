import os
import springtail
import time
import common
import yaml
import subprocess
import signal
import logging
from typing import Any
from psycopg2.extensions import connection as psycopg2_connection

def parse_sql_commands(sql_content: str) -> str:
    """Parse and process SQL commands marked with ##"""
    lines = sql_content.splitlines()
    processed_lines = []
    repeat_count = 0
    repeat_buffer = []
    in_repeat = False

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("##"):
            cmd = stripped[2:].strip()
            parts = cmd.split()
            if parts and parts[0] == "repeat":
                if in_repeat:
                    raise ValueError("Nested repeats are not allowed")
                try:
                    repeat_count = int(cmd.split()[1])
                    if repeat_count <= 0:
                        raise ValueError(
                            f"Repeat count must be positive: {repeat_count}"
                        )
                    in_repeat = True
                except (IndexError, ValueError):
                    raise ValueError(f"Invalid repeat command: {cmd}")
            elif cmd == "endrepeat":
                if not in_repeat:
                    raise ValueError("endrepeat without matching repeat")
                # Add the repeated content
                processed_lines.extend(repeat_buffer * repeat_count)
                repeat_buffer = []
                in_repeat = False
            else:
                raise ValueError(f"Unrecognized command: {cmd}")
        else:
            if in_repeat:
                repeat_buffer.append(line)
            else:
                processed_lines.append(line)

    if in_repeat:
        raise ValueError("Unclosed repeat block")

    return "\n".join(processed_lines)

class BenchCase:
    """Class to run a single benchmark case"""

    def __init__(self, props: springtail.Properties, name: str, filename: str, test_sql: str, build_dir: str, enable_perf: bool = False) -> None:
        self.filename = filename
        self.name = filename
        self.build_dir = build_dir
        self.props = props
        self.test_sql = test_sql
        self.enable_perf = enable_perf

        # Get database names
        db_configs = self.props.get_db_configs()
        fdw_config = self.props.get_fdw_config()
        self.primary_name = db_configs[0]["name"]
        if "db_prefix" in fdw_config:
            self.replica_name = fdw_config["db_prefix"] + self.primary_name
        else:
            self.replica_name = self.primary_name

    def _start_perf(self, replica_conn: psycopg2_connection) -> tuple[subprocess.Popen[bytes] | None, str | None, int | None]:
        """Start perf profiling on the replica postgres backend.

        Returns:
            tuple: (perf_process, perf_output_file, backend_pid) or (None, None, None) if perf cannot be started
        """
        if not self.enable_perf:
            return None, None, None

        logging.info("=" * 60)
        logging.info("PERF PROFILING ENABLED")
        logging.info("=" * 60)

        try:
            # Get the backend PID for the replica connection
            with replica_conn.cursor() as cursor:
                cursor.execute("SELECT pg_backend_pid()")
                result = cursor.fetchone()
                if not result:
                    logging.error("Could not get backend PID for replica connection")
                    return None, None, None
                backend_pid = result[0]

            logging.info(f"Replica postgres backend PID: {backend_pid}")

            # Generate output filename with timestamp
            timestamp = time.strftime("%Y%m%d-%H%M%S")
            # Use benchmark name from filename (directory name)
            bench_name = os.path.basename(os.path.dirname(self.filename))
            perf_output = f"perf-{bench_name}-{timestamp}.data"

            logging.info(f"Perf output file: {perf_output}")

            # Start perf recording with sudo
            perf_cmd = [
                "sudo", "perf", "record",
                "-p", str(backend_pid),
                "-g",  # Enable call graph recording
                "-o", perf_output,
                "--", "sleep", "999999"  # Sleep indefinitely until we stop it
            ]

            logging.info(f"Perf command: {' '.join(perf_cmd)}")

            perf_process = subprocess.Popen(
                perf_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )

            logging.info(f"Perf process started with PID: {perf_process.pid}")

            # Give perf a moment to initialize
            time.sleep(0.5)

            # Check if perf started successfully
            if perf_process.poll() is not None:
                stdout = perf_process.stdout.read().decode()
                stderr = perf_process.stderr.read().decode()
                logging.error(f"perf failed to start (exit code: {perf_process.returncode})")
                if stdout:
                    logging.error(f"STDOUT: {stdout}")
                if stderr:
                    logging.error(f"STDERR: {stderr}")
                return None, None, None

            logging.info("Perf is running and attached to postgres backend")
            logging.info("=" * 60)

            return perf_process, perf_output, backend_pid

        except FileNotFoundError as e:
            logging.error(f"Command not found: {e}")
            return None, None, None
        except Exception as e:
            logging.error(f"Failed to start perf: {e}")
            return None, None, None

    def _stop_perf(self, perf_process: subprocess.Popen[bytes] | None, perf_output: str | None, backend_pid: int | None) -> None:
        """Stop perf profiling and finalize the output file."""
        if perf_process is None:
            return

        try:
            # Send SIGINT to gracefully stop perf and finalize the data file
            perf_process.send_signal(signal.SIGINT)

            # Wait for perf to finish (with timeout)
            try:
                perf_process.wait(timeout=5)
                logging.info(f"perf profiling complete: {perf_output}")
                print(f"  Perf data saved to: {perf_output}")
            except subprocess.TimeoutExpired:
                logging.warning("perf did not terminate gracefully, killing it")
                perf_process.kill()
                perf_process.wait()
        except Exception as e:
            logging.warning(f"Error stopping perf: {e}")
            try:
                perf_process.kill()
            except:
                pass

    def _run_benchmark(self, primary_conn: psycopg2_connection, replica_conn: psycopg2_connection, setup_timeout: int | float) -> dict[str, Any]:
        """Run the benchmark with given connections"""

        # test root
        root = os.path.dirname(self.filename)

        def get_sql(n: str) -> str | None:
            p = config.get(n)
            if not p:
                return None
            sql_path = os.path.join(root, p)
            if not os.path.exists(sql_path):
                raise ValueError(f"File not found: {sql_path}")
            with open(sql_path) as f:
                sql_content = f.read()
            return parse_sql_commands(sql_content)

        setup_sql = None

        if not self.test_sql:
            with open(self.filename) as f:
                config = yaml.safe_load(f)

            if not config:
                print("Ignoring...")
                print(f"The config file is empty: {self.filename}")
                return {}

            setup_sql = get_sql("setup")
            self.test_sql = get_sql("test")

        if not setup_sql and not self.test_sql:
            print(f"Bad config format: {self.filename}")
            return {}

        result = {}
        if setup_sql:
            # Time postgres write and total springtail time
            start = time.time()
            with primary_conn.cursor() as cursor:
                cursor.execute(setup_sql)
                # Insert sentinel value to track replication
                cursor.execute(
                    "INSERT INTO benchmark_state (key, state) VALUES (%s, %s) "
                    "ON CONFLICT (key) DO UPDATE SET state = EXCLUDED.state",
                    (f"benchcase_{self.name}", self.name),
                )
            primary_conn.commit()
            postgres_time = time.time() - start

            # Wait for springtail sync
            common.wait_for_replica_condition(
                replica_conn,
                f"SELECT state FROM benchmark_state WHERE key = 'benchcase_{self.name}'",
                (self.name,),
                setup_timeout,
            )

            result["Primary setup time"] = postgres_time
            result["Setup sync time"] = time.time() - start

        if self.test_sql:
            with primary_conn.cursor() as cursor:
                cursor.execute("DISCARD ALL")

            start = time.time()
            with primary_conn.cursor() as cursor:
                cursor.execute(self.test_sql)
            primary_conn.commit()
            postgres_time = time.time() - start

            # Start perf profiling before replica queries
            perf_process, perf_output, backend_pid = self._start_perf(replica_conn)

            try:
                with replica_conn.cursor() as cursor:
                    cursor.execute("DISCARD ALL")

                start = time.time()
                with replica_conn.cursor() as cursor:
                    cursor.execute(self.test_sql)
                replica_conn.commit()
                replica_time = time.time() - start

                result["First run: primary test time"] = postgres_time
                result["First run: replica test time"] = replica_time

                start = time.time()
                with replica_conn.cursor() as cursor:
                    cursor.execute(self.test_sql)
                replica_conn.commit()
                replica_time = time.time() - start

                result["Second run: primary test time"] = postgres_time
                result["Second run: replica test time"] = replica_time

            finally:
                # Stop perf profiling after replica queries complete
                self._stop_perf(perf_process, perf_output, backend_pid)

        # Clean up after benchmark (outside of timing)
        cleanup_path = os.path.join(os.path.dirname(__file__), "benchmark_cleanup.sql")
        with open(cleanup_path) as f:
            cleanup_sql = f.read()
        with primary_conn.cursor() as cursor:
            cursor.execute(cleanup_sql)
        primary_conn.commit()
        # Wait for cleanup to sync
        common.wait_for_replica_condition(
            replica_conn,
            "SELECT COUNT(*) FROM benchmark_data",
            (0,),
        )

        return result

    def run(self, setup_timeout: int | float) -> dict[str, Any]:
        """Run the benchmark and return timing results"""
        # Connect to databases
        primary_conn = springtail.connect_db_instance(self.props, self.primary_name)
        primary_conn.autocommit = True
        replica_conn = springtail.connect_fdw_instance(self.props, self.replica_name)

        try:
            return self._run_benchmark(primary_conn, replica_conn, setup_timeout)
        except Exception as e:
            raise RuntimeError(f"Error running benchmark {self.name}") from e
        finally:
            primary_conn.close()
            replica_conn.close()
