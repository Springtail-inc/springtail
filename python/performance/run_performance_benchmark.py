import sys
import yaml
import argparse
import traceback
import os
import csv
import json
import sys

from load_generator import LoadGenerator
from generate_xid_traces import generate_xid_traces
from generate_final_report import generate_final_report
from utils import get_file_path, zip_folder, unzip_file, print_banner

# Get the parent directory of the current script (i.e., the project root directory)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the /shared directory to the Python path
sys.path.append(os.path.join(project_root, 'shared'))
# Add the /testing directory to the Python path
sys.path.append(os.path.join(project_root, 'testing'))

from aws import AwsHelper
PERFORMANCE_TEST_BUCKET = "performance-test-output"

def read_csv_to_dict(file_path):
    """
    Read a CSV file and return a dictionary of key-value pairs.

    Args:
        file_path (str): The path to the CSV file

    Returns:
        dict: A dictionary of key-value pairs
    """
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        return {row['Label']: row['Value'] for row in reader}

def get_comparision_details(key, prev_run_value, current_run_value, threshold_fraction, results):
    """
    Get the comparison details

    Args:
        key (str): Key to compare
        prev_run_value (float): Previous run value
        current_run_value (float): Current run value
        threshold_fraction (float): Threshold fraction
        results (list): List of results
    """
    diff = current_run_value - prev_run_value
    base = prev_run_value if prev_run_value != 0 else 1e-9
    rel_change = abs(diff) / abs(base)

    if rel_change <= threshold_fraction:
        description = f"No significant change (from {prev_run_value} to {current_run_value})"
        improvement = None
    elif diff > 0:
        description = f"Increased from {prev_run_value} to {current_run_value} (+{rel_change * 100:.2f}%)"
        improvement = False
    else:
        description = f"Decreased from {prev_run_value} to {current_run_value} (-{rel_change * 100:.2f}%)"
        improvement = True

    results.append({
        "key": key,
        "improvement": improvement,
        "description": description
    })

def compare_csv_values(run_config: dict, prev_run_file, current_run_file):
    """
    Compare the previous run with the current run

    Args:
        run_config (dict): Run configuration
        prev_run_file (str): Previous run file
        current_run_file (str): Current run file
    """
    prev_run_data = read_csv_to_dict(prev_run_file)
    current_run_data = read_csv_to_dict(current_run_file)

    all_keys = set(prev_run_data) | set(current_run_data)
    threshold_fraction = run_config['comparison_threshold'] / 100.0

    results = []
    for key in sorted(all_keys):
        prev_run_value = float(prev_run_data.get(key)) if prev_run_data.get(key) else 0
        current_run_value = float(current_run_data.get(key)) if current_run_data.get(key) else 0

        get_comparision_details(key, prev_run_value, current_run_value, threshold_fraction, results)

    json_output = json.dumps(results, indent=2)
    print(json_output)

    # If at-least one failure, fail the step
    if any(result['improvement'] == False for result in results):
        print_banner("Performance test failed")
        sys.exit(1)

    # At-least one of the tests improved
    if any(result['improvement'] == True for result in results):
        # If all tests pass, upload the file to S3
        write_performance_data_to_s3(run_config)

def write_performance_data_to_s3(run_config: dict):
    """
    Upload the final aggregates to S3

    Args:
        run_config (dict): Run configuration
    """
    base_dir = run_config['file_configuration']['base_dir']
    zip_path = os.path.join('/tmp', "final_aggregates.zip")
    zip_folder(base_dir, zip_path)
    print(f"[*] Zipped contents from {base_dir} to {zip_path}")

    AwsHelper().s3_upload(PERFORMANCE_TEST_BUCKET, base_dir, zip_path, "final_aggregates.zip")
    print(f"[*] Uploaded performance data to S3")

def get_current_run_details(run_config: dict):
    """
    Get the current run details and compare with previous run

    Args:
        run_config (dict): Run configuration
    """
    print("[*] Writing performance data to S3")
    final_aggregates_file = get_file_path(run_config, "final_aggregates")
    csv_reader = csv.DictReader(open(final_aggregates_file))
    results = []
    for row in csv_reader:
        get_comparision_details(row['Label'], 0, float(row['Value']), run_config['comparison_threshold'] / 100.0, results)
    json_output = json.dumps(results, indent=2)
    print(json_output)

    write_performance_data_to_s3(run_config)

def download_performance_data_from_s3(run_config: dict):
    """
    Download the final aggregates from S3

    Args:
        run_config (dict): Run configuration

    Returns:
        bool: True if successful, False otherwise
    """
    base_dir = run_config['file_configuration']['base_dir']
    prev_run_dir = run_config['file_configuration']['prev_run_dir']

    download_path = os.path.join('/tmp/', base_dir)
    os.makedirs(download_path, exist_ok=True)
    extract_path = os.path.join(prev_run_dir)
    os.makedirs(extract_path, exist_ok=True)

    # Download file to /tmp
    filename = AwsHelper().s3_download(PERFORMANCE_TEST_BUCKET, base_dir, download_path, 'final_aggregates.zip')
    if filename and os.path.exists(filename):
        return unzip_file(filename, extract_path)
    else:
        print("[*] No previous run found")
        return False

def compare_report_with_previous_run(run_config: dict):
    """
    Compare the current run with the previous run

    Args:
        run_config (dict): Run configuration
    """
    # Try and download the prev run details from S3
    if not download_performance_data_from_s3(run_config):
        print("[-] No previous run found. Skipping comparison.")
        get_current_run_details(run_config)
        return
    else:
        print("[+] Previous run found. Comparing...")
        prev_aggregates_file = get_file_path(run_config, "final_aggregates", use_dir="prev_run_dir")
        final_aggregates_file = get_file_path(run_config, "final_aggregates")
        compare_csv_values(run_config, prev_aggregates_file, final_aggregates_file)

def parse_arguments() -> argparse.Namespace:
    """
    Parse command line arguments.

    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(description="Run performance benchmark")
    parser.add_argument('-c', '--load-config-file', type=str, default="load_config.yaml", help='Path to the load configuration file')

    return parser.parse_args()

def run_performance_benchmark(run_config: dict):
    """
    Run the performance benchmark

    Args:
        run_config (dict): Run configuration
    """
    try:
        print("[*] Downloading previous run data...")
        download_performance_data_from_s3(run_config)

        print("[*] Generating data...")
        LoadGenerator(run_config).load_data()

        print("[*] Generating XID traces...")
        generate_xid_traces(run_config)

        print("[*] Generating final report...")
        generate_final_report(run_config)

        print("[*] Comparing final report with previous run...")
        compare_report_with_previous_run(run_config)
    except Exception as e:
        print(f"Caught error: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    print("[*] Running performance benchmark...")
    args = parse_arguments()

    with open(args.load_config_file) as f:
        run_config = yaml.safe_load(f)

    # Run the performance benchmark
    run_performance_benchmark(run_config)

    print("[*] Performance benchmark completed successfully!")
