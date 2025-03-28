import os
import springtail
import time
import common
import yaml

class BenchCase:
    """Class to run a single benchmark case"""

    def __init__(self, props: springtail.Properties, name: str, filename: str, config_file: str, build_dir: str):
        self.filename = filename
        self.name = filename
        self.config_file = config_file
        self.build_dir = build_dir
        self.props = props 

        # Get database names
        db_configs = self.props.get_db_configs()
        fdw_config = self.props.get_fdw_config()
        self.primary_name = db_configs[0]["name"]
        if "db_prefix" in fdw_config:
            self.replica_name = fdw_config["db_prefix"] + self.primary_name
        else:
            self.replica_name = self.primary_name

    def _run_benchmark(self, primary_conn, replica_conn, setup_timeout) -> dict:
        """Run the benchmark with given connections"""

        # test root
        root = os.path.dirname(self.filename)

        with open(self.filename) as f:
            config = yaml.safe_load(f)

        if not config:
            print("Ignoring...")
            print(f"The config file is empty: {self.filename}")
            return {}

        setup_sql = None
        test_sql = None

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

        def get_sql(n):
            p = config.get(n)
            if not p:
                return None
            sql_path = os.path.join(root, p)
            if not os.path.exists(sql_path):
                raise ValueError(f"File not found: {sql_path}")
            with open(sql_path) as f:
                sql_content = f.read()
            return parse_sql_commands(sql_content)

        setup_sql = get_sql("setup")
        test_sql = get_sql("test")

        if not setup_sql and not test_sql:
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

        if test_sql:
            with primary_conn.cursor() as cursor:
                cursor.execute("DISCARD ALL")

            start = time.time()
            with primary_conn.cursor() as cursor:
                cursor.execute(test_sql)
            primary_conn.commit()
            postgres_time = time.time() - start

            with replica_conn.cursor() as cursor:
                cursor.execute("DISCARD ALL")

            start = time.time()
            with replica_conn.cursor() as cursor:
                cursor.execute(test_sql)
            replica_conn.commit()
            replica_time = time.time() - start

            result["Primary test time"] = postgres_time
            result["Replica test time"] = replica_time

        # Clean up after benchmark (outside of timing)
        cleanup_path = os.path.join(os.path.dirname(__file__), "benchmark_cleanup.sql")
        with open(cleanup_path) as f:
            cleanup_sql = f.read()
        with primary_conn.cursor() as cursor:
            cursor.execute(cleanup_sql)
        primary_conn.commit()
        # Wait for cleanup to sync
        # common.wait_for_replica_condition(
         #    replica_conn,
          #   "SELECT COUNT(*) FROM benchmark_data",
           #  (0,),
        # )

        return result

    def run(self, setup_timeout) -> dict:
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
