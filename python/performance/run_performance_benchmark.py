import sys
import yaml
import argparse
import traceback
import os
import csv

from load_generator import LoadGenerator
from generate_xid_traces import generate_xid_traces
from generate_final_report import generate_final_report

def read_csv_to_dict(file_path):
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        return {row['Label']: row['Value'] for row in reader}

def compare_csv_values(prev_run_file, current_run_file, threshold_percent=5.0):
    prev_run_data = read_csv_to_dict(prev_run_file)
    current_run_data = read_csv_to_dict(current_run_file)

    all_keys = set(prev_run_data) | set(current_run_data)
    threshold_fraction = threshold_percent / 100.0

    for key in sorted(all_keys):
        prev_run_value = float(prev_run_data.get(key))
        current_run_value = float(current_run_data.get(key))

        diff = current_run_value - prev_run_value
        rel_change = abs(diff) / abs(prev_run_value)

        if rel_change <= threshold_fraction:
            print(f"[~] {key}: No significant change (from {prev_run_value} to {current_run_value})")
        elif diff > 0:
            print(f"[↑] {key}: Increased from {prev_run_value} to {current_run_value} (+{rel_change * 100:.2f}%)")
        else:
            print(f"[↓] {key}: Decreased from {prev_run_value} to {current_run_value} (-{rel_change * 100:.2f}%)")

def compare_report_with_previous_run(run_config: dict):
    prev_aggregates_file = '/tmp/PREV_PERF_final_aggregates.csv'
    final_aggregates_file = '/tmp/PERF_final_aggregates.csv'

    compare_csv_values(prev_aggregates_file, final_aggregates_file)

def backup_prev_run_files():
    # Move all PERF_ files to a different folder
    for file in os.listdir("/tmp"):
        if file.startswith("PERF_"):
            os.rename(f"/tmp/{file}", f"/tmp/PREV_{file}")

def parse_arguments() -> argparse.Namespace:
    """
    Parse command line arguments.

    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(description="Run performance benchmark")
    parser.add_argument('-c', '--load-config-file', type=str, default="load_config.yaml", help='Path to the load configuration file')

    return parser.parse_args()

def run_performance_benchmark(config_file: str):
    with open(config_file) as f:
        run_config = yaml.safe_load(f)

    try:
        print("[*] Backing up previous run files...")
        backup_prev_run_files()

        print("[*] Loading data...")
        LoadGenerator(run_config).load_data()

        print("[*] Generating XID traces...")
        generate_xid_traces()

        print("[*] Generating final report...")
        generate_final_report()

        print("[*] Comparing final report with previous run...")
        compare_report_with_previous_run(run_config)
    except Exception as e:
        print(f"Caught error: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    print("[*] Running performance benchmark...")
    args = parse_arguments()
    run_performance_benchmark(args.load_config_file)
    print("[*] Performance benchmark completed successfully!")
