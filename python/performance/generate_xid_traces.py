import re
import csv
import sys
import time
from pathlib import Path
import tempfile
import yaml
import os
import argparse

from utils import get_file_path

def parse_xid_traces(logfile):
    """Parse XID traces from the log file."""
    with open(logfile, 'r') as f:
        traces = []
        for line in f:
            match = re.search(r'\[TRACE\] \[(.*?)-xid_([0-9]+),(.*?)\]', line)
            if match:
                func, xid, stats = match.groups()
                # Clean up stats by removing ms/us suffixes
                stats = re.sub(r'([0-9]+)(ms|us)', r'\1', stats)
                traces.append((func, xid, stats))
    return traces

def parse_pgxid_traces(logfile):
    """Parse PG-XID traces from the log file."""
    with open(logfile, 'r') as f:
        traces = []
        for line in f:
            match = re.search(r'\[TRACE\] \[(.*?)-pgxid_([0-9]+),(.*?)\]', line)
            if match:
                func, pgxid, stats = match.groups()
                # Clean up stats by removing ms/us suffixes
                stats = re.sub(r'([0-9]+)(ms|us)', r'\1', stats)
                traces.append((func, pgxid, stats))
    return traces

def parse_xid_mapping(logfile):
    """Parse XID to PG-XID mapping from the log file."""
    with open(logfile, 'r') as f:
        mappings = []
        for line in f:
            match = re.search(r'Committing PG XID: ([0-9]+) -> XID: ([0-9]+)', line)
            if match:
                pgxid, xid = match.groups()
                mappings.append((pgxid, xid))
    return mappings

def write_traces_to_csv(traces, filename, prefix):
    """Write parsed traces to CSV file."""
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        header = ['function', f'{prefix}xid', 'totalms', 'totalmicros', 'counter',
                 'averagemicros', 'averagems']
        writer.writerow(header)

        for func, xid, stats in traces:
            # Split stats into key-value pairs
            kv_pairs = stats.split(',')
            kv_dict = {k: v for k, v in (pair.split('=') for pair in kv_pairs)}

            # Write row with all values
            writer.writerow([
                func, xid,
                kv_dict.get('totalms', ''), kv_dict.get('totalmicros', ''),
                kv_dict.get('counter', ''), kv_dict.get('averagemicros', ''),
                kv_dict.get('averagems', '')
            ])

def merge_traces(xid_pgxid_mapping_csv, xid_traces_csv, pgxid_traces_csv, merged_traces_csv, final_traces_csv, query_info_file):
    # Load xid to pg_xid mapping
    xid_to_pg_xid_mapping = {}
    pg_xid_to_xid_mapping = {}
    with open(xid_pgxid_mapping_csv, newline='') as mapfile:
        reader = csv.DictReader(mapfile)
        for row in reader:
            xid_to_pg_xid_mapping[row['xid']] = row['pg_xid']
            pg_xid_to_xid_mapping[row['pg_xid']] = row['xid']

    # Step 1: Copy xid_traces_csv to merged_traces_csv
    with open(xid_traces_csv, newline='') as xidfile, open(merged_traces_csv, 'w', newline='') as tracescsvfile:
        reader = csv.reader(xidfile)
        writer = csv.writer(tracescsvfile)

        for row in reader:
            writer.writerow(row)

    # Step 2: Rewrite pgxid → xid and save to merged_traces_csv
    with open(pgxid_traces_csv, newline='') as pgxidfile, open(merged_traces_csv, 'a', newline='') as tracescsvfile:
        reader = csv.reader(pgxidfile)
        writer = csv.writer(tracescsvfile)
        next(reader) # Skip header since the original header comes from xid_traces_csv

        for row in reader:
            if len(row) >= 2 and row[1] in pg_xid_to_xid_mapping:
                row[1] = pg_xid_to_xid_mapping[row[1]].strip()
            writer.writerow(row)

    # Step 3: Load query info mapping by pg_xid
    pg_xid_to_query_info = {}
    with open(query_info_file, newline='') as queryfile:
        reader = csv.DictReader(queryfile)
        for row in reader:
            pg_xid_to_query_info[row['pg_xid']] = {
                "query_type": row.get("query_type", ""),
                "duration_ms": row.get("duration_ms", ""),
                "pg_timestamp": row.get("pg_timestamp", ""),
                "rows_affected": row.get("rows_affected", ""),
                "table_name": row.get("table_name", "")
            }

    # Step 4: Merge traces
    with open(merged_traces_csv, newline='') as tracesfile, open(final_traces_csv, 'w', newline='') as outputfile:
        reader = csv.DictReader(tracesfile)
        fieldnames = (
            ['pg_xid'] + reader.fieldnames +
            ['query_type', 'duration_ms', 'pg_timestamp', 'rows_affected', 'table_name']
        )
        writer = csv.DictWriter(outputfile, fieldnames=fieldnames)

        writer.writeheader()
        for row in reader:
            xid = row['xid']
            pg_xid = xid_to_pg_xid_mapping.get(xid, '')

            query_info = pg_xid_to_query_info.get(pg_xid, {})
            row_with_pg_and_query = {
                'pg_xid': pg_xid,
                **row,
                'query_type': query_info.get('query_type', ''),
                'duration_ms': query_info.get('duration_ms', ''),
                'pg_timestamp': query_info.get('pg_timestamp', ''),
                'rows_affected': query_info.get('rows_affected', ''),
                'table_name': query_info.get('table_name', '')
            }
            writer.writerow(row_with_pg_and_query)

def log_watcher_thread(log_file: str, watch_count: int = 10, max_count: int = 50):
    last_log_line = ""
    count = 0
    with open(log_file) as f:
        f.seek(0, 2)  # Seek to end
        while True:
            if count > max_count:
                return
            line = f.readline()
            if line != last_log_line:
                last_log_line = line
                count = 0
            else:
                count += 1
                if count > watch_count:
                    return
                time.sleep(0.1)

def dump_traces(log_file):
    log_watcher_thread(log_file)
    with open("/tmp/output_trace.txt", 'w') as f:
        f.write("Dump logs")
    log_watcher_thread(log_file)

def generate_xid_traces(config_file: str = "load_config.yaml", log_file: str = "/opt/springtail/logs/pg_log_mgr.log"):
    # Create temporary files
    with open(config_file) as f:
        run_config = yaml.safe_load(f)

    query_info_file = get_file_path(run_config, "query_info")
    xid_traces_csv = get_file_path(run_config, "xid_traces")
    pgxid_traces_csv = get_file_path(run_config, "pgxid_traces")
    xid_pgxid_mapping_csv = get_file_path(run_config, "xid_pgxid_mapping")
    merged_traces_csv = get_file_path(run_config, "merged_traces")
    final_traces_csv = get_file_path(run_config, "final_traces")

    # Wait for the log file to stop streaming and dump the traces
    dump_traces(log_file)

    # Parse and write XID traces
    xid_traces = parse_xid_traces(log_file)
    write_traces_to_csv(xid_traces, xid_traces_csv, '')

    # Parse and write PG-XID traces
    pgxid_traces = parse_pgxid_traces(log_file)
    write_traces_to_csv(pgxid_traces, pgxid_traces_csv, 'pg')

    # Parse and write XID mapping
    xid_mappings = parse_xid_mapping(log_file)
    with open(xid_pgxid_mapping_csv, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['pg_xid', 'xid'])
        writer.writerows(xid_mappings)

    # Merge PG-XID traces with XID mapping
    merge_traces(xid_pgxid_mapping_csv, xid_traces_csv, pgxid_traces_csv, merged_traces_csv, final_traces_csv, query_info_file)

    print(f"[+] Generated {final_traces_csv} successfully!")

def parse_arguments() -> argparse.Namespace:
    """
    Parse command line arguments.

    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(description="Run ingestion metrics logger")
    parser.add_argument('-c', '--config-file', type=str, default="load_config.yaml", help='Path to the config file')
    parser.add_argument('-l', '--log-file', type=str, default="/opt/springtail/logs/pg_log_mgr.log", help='Path to the log file')

    return parser.parse_args()

if __name__ == "__main__":
    # Parse command line arguments
    args = parse_arguments()

    # Extract arguments
    config_file = args.config_file
    log_file = args.log_file

    # Run the main function
    generate_xid_traces(config_file, log_file)
