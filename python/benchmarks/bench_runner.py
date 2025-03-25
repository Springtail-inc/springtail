import argparse
import logging
import os
import sys
import yaml
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../testing")))

import springtail
import common

from bench_case import BenchCase


def wait_for_springtail_ready(props: springtail.Properties) -> None:
    """Wait for Springtail to be ready by checking sentinel value"""
    # Get database names
    db_configs = props.get_db_configs()
    fdw_config = props.get_fdw_config()
    primary_name = db_configs[0]["name"]
    replica_name = fdw_config.get("db_prefix", "") + primary_name

    # Connect to databases
    primary_conn = springtail.connect_db_instance(props, primary_name)
    replica_conn = springtail.connect_fdw_instance(props, replica_name)

    try:
        # Execute setup script on primary
        setup_path = os.path.join(os.path.dirname(__file__), "benchmark_setup.sql")
        with open(setup_path) as f:
            setup_sql = f.read()

        with primary_conn.cursor() as cursor:
            cursor.execute(setup_sql)
        primary_conn.commit()

        # Wait for sentinel value in replica
        logging.info("Waiting for Springtail to be ready...")

        # Wait for table to be created in replica
        common.wait_for_replica_condition(
            replica_conn,
            "SELECT 1 FROM pg_class WHERE relname = 'benchmark_state'",
            (1,),
        )

        # Wait for sentinel value
        sync_time = common.wait_for_replica_condition(
            replica_conn,
            "SELECT state FROM benchmark_state WHERE key = 'common_benchmark_setup'",
            ("ready",),
        )
        logging.info(f"Springtail is ready (took {sync_time:.3f}s)")

    finally:
        primary_conn.close()
        replica_conn.close()


def run_benchmark(
    bench_dir: str,
    config_file: str,
    build_dir: str,
    setup_timeout: int,
    bench_name: str = None,
    start_only: bool = False,
    stop_only: bool = False,
    nostartstop: bool = False,
) -> None:
    """Run benchmarks from the specified directory.

    Args:
        bench_dir: Directory containing benchmark cases
        config_file: Path to system config file
        build_dir: Path to build directory
        bench_name: Optional specific benchmark to run
        start_only: Only start Springtail and wait for ready
        stop_only: Only stop Springtail
        nostartstop: Don't start/stop Springtail
    """
    logging.info(f"Running benchmarks from: {bench_dir}")

    props = springtail.Properties(config_file, True)

    # Handle startup control flags
    if stop_only:
        springtail.stop(config_file, do_cleanup=True)
        return

    if not nostartstop:
        springtail.stop(config_file, do_cleanup=True)
        springtail.start(config_file, build_dir, do_cleanup=False)
    wait_for_springtail_ready(props)

    if start_only:
        return

    try:
        if bench_name:
            # Run single benchmark
            bench_path = os.path.join(bench_dir, bench_name)
            if not os.path.exists(bench_path):
                raise ValueError(f"Benchmark not found: {bench_path}")
            benchmarks = {os.path.basename(os.path.dirname(bench_path)): bench_path}
        else:
            # Run all benchmarks
            test_files = [os.path.join(root, f) for root, _, files in os.walk(bench_dir)
                          for f in files if f.endswith(".yaml")]
            benchmarks = dict(sorted({os.path.basename(os.path.dirname(key)): key for key in test_files}.items()))

        print("\nRunning benchmarks:")
        for n, f in benchmarks.items():
            print(f" {n}:   {f}")

        for n, f in benchmarks.items():
            print(f"\nBenchmark: {n}")
            bench = BenchCase(props, n, f, config_file, build_dir)
            result = bench.run(setup_timeout)
            for d, t in result.items():
                print(f"   {d}: {t}")

    finally:
        if not nostartstop:
            springtail.stop(config_file)


def parse_args():
    parser = argparse.ArgumentParser(description="Run Springtail benchmarks")
    parser.add_argument(
        "-c", "--config", type=str, default="config.yaml", help="Path to config file"
    )
    parser.add_argument("-b", "--benchmark", type=str, help="Run specific benchmark")

    # Add startup control group
    startup_group = parser.add_mutually_exclusive_group()
    startup_group.add_argument(
        "--start-only",
        action="store_true",
        help="Only start Springtail and wait for ready, then exit",
    )
    startup_group.add_argument(
        "--stop-only", action="store_true", help="Only stop Springtail, then exit"
    )
    startup_group.add_argument(
        "--nostartstop",
        action="store_true",
        help="Don't start/stop Springtail (assume it's already running)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    bench_dir = config.get("benchmark_folder", "cases")
    build_dir = config.get("build_dir")
    system_config = config.get("system_json_path")
    setup_timeout = config.get("setup_timeout")
    if not setup_timeout:
        setup_timeout = 30

    if not build_dir or not system_config:
        raise ValueError("Missing required config: build_dir or system_json_path")

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    run_benchmark(
        bench_dir,
        system_config,
        build_dir,
        setup_timeout,
        args.benchmark,
        args.start_only,
        args.stop_only,
        args.nostartstop,
    )
