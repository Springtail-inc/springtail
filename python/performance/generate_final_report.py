import csv
import xlsxwriter
import pandas as pd
import yaml
import argparse

from utils import get_file_path

def generate_pg_xid_summary_file(final_traces_file: str, xid_summary_file: str):
    """
    Generate PG-XID summary file.

    Args:
        final_traces_file (str): Final traces file
        xid_summary_file (str): XID summary file
    """
    # Load your data
    df = pd.read_csv(final_traces_file)

    # Group-level aggregation
    summary = df.groupby("pg_xid").agg(
        ingest_time=('totalms', 'sum'),
        primary_time=('duration_ms', 'first'),
        max_counter=('counter', 'max')
    ).reset_index()

    # Row with max totalms per pg_xid
    max_funcs = df.loc[df.groupby("pg_xid")["totalms"].idxmax(), ["pg_xid", "function", "totalms"]]
    max_funcs = max_funcs.rename(columns={
        "function": "max_function",
        "totalms": "max_totalms"
    })

    # Merge all into one DataFrame
    final_df = pd.merge(summary, max_funcs, on="pg_xid")

    # Calculate the delta between ingest_time and primary_time
    final_df["ingest_time_minus_primary_time"] = final_df["ingest_time"] - final_df["primary_time"]

    # Save to CSV
    final_df.to_csv(xid_summary_file, index=False)

def write_csv_to_worksheet(worksheet, csv_file, normal_fmt, bold_fmt):
    """
    Write CSV to worksheet.

    Args:
        worksheet (xlsxwriter.worksheet.Worksheet): Worksheet to write to
        csv_file (str): CSV file path
        normal_fmt (xlsxwriter.format.Format): Normal format
        bold_fmt (xlsxwriter.format.Format): Bold format
    """
    col_widths = []
    with open(csv_file, 'r') as f:
        reader = csv.reader(f)
        for r, row in enumerate(reader):
            for c, col in enumerate(row):
                try:
                    float(col)
                    worksheet.write_number(r, c, float(col), bold_fmt if r == 0 else normal_fmt)
                except ValueError:
                    worksheet.write(r, c, col, bold_fmt if r == 0 else normal_fmt)
                # Expand col_widths if needed
                if len(col_widths) <= c:
                    col_widths.extend([0] * (c - len(col_widths) + 1))

                # Update max width
                col_widths[c] = max(col_widths[c], len(col))

    # Set the column widths with padding
    for c, width in enumerate(col_widths):
        worksheet.set_column(c, c, width)

def write_aggregates_to_worksheet(worksheet, run_config: dict, normal_fmt, bold_fmt):
    """
    Write aggregates to worksheet.

    Args:
        worksheet (xlsxwriter.worksheet.Worksheet): Worksheet to write to
        run_config (dict): Run configuration
        normal_fmt (xlsxwriter.format.Format): Normal format
        bold_fmt (xlsxwriter.format.Format): Bold format
    """
    # Get the files to calculate the aggregates
    final_aggregates_file = get_file_path(run_config, "final_aggregates")
    final_traces_file = get_file_path(run_config, "final_traces")
    pg_xid_summary_file = get_file_path(run_config, "pg_xid_summary")

    # Sum the total_ms and duration_ms columns in the final traces sheet
    total_ms = 0
    duration_ms = 0
    with open(final_traces_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_ms += float(row['totalms'])
            duration_ms += float(row['duration_ms'])

    # Track max width for each column
    col_widths = [0, 0, 0]  # three columns: key, label, and value

    metrics = run_config['metrics']

    aggregates = {}
    aggregates["ingest_total_time"] = total_ms
    aggregates["primary_total_time"] = duration_ms

    # Analyze pg_xid summary
    ingest_slower_count = 0
    ingest_faster_count = 0
    total_count = 0
    with open(pg_xid_summary_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            val = float(row['ingest_time_minus_primary_time'])
            total_count += 1
            if val > 0:
                ingest_slower_count += 1
            elif val < 0:
                ingest_faster_count += 1

    aggregates["ingest_outperform_primary_percentage"] = ingest_faster_count / total_count
    aggregates["ingest_outperform_primary_count"] = ingest_faster_count
    aggregates["primary_outperform_ingest_count"] = ingest_slower_count

    # Write aggregates to csv first
    with open(final_aggregates_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Key', 'Label', 'Value'])
        for key in metrics:
            writer.writerow([key, metrics[key]['label'], aggregates[key]])

    # Write to worksheet and track max widths
    for idx, key in enumerate(metrics):
        label_str = str(metrics[key]['label'])
        value_str = str(aggregates[key])

        worksheet.write(idx, 0, key, bold_fmt)
        worksheet.write(idx, 1, label_str, bold_fmt)
        worksheet.write(idx, 2, value_str, normal_fmt)

        col_widths[0] = max(col_widths[0], len(key))
        col_widths[1] = max(col_widths[1], len(label_str))
        col_widths[2] = max(col_widths[2], len(value_str))

    # Apply column widths with padding
    for c, width in enumerate(col_widths):
        worksheet.set_column(c, c, width)

def create_workbook(output_file: str):
    """
    Create a workbook.

    Args:
        output_file (str): Output file path

    Returns:
        tuple: Tuple of workbook, normal format, and bold format
    """
    workbook = xlsxwriter.Workbook(output_file, {'constant_memory': True})
    normal_fmt = workbook.add_format({'font_name': 'Trebuchet MS', 'font_size': 10})
    bold_fmt = workbook.add_format({'bold': True, 'font_name': 'Trebuchet MS', 'font_size': 10})
    return workbook, normal_fmt, bold_fmt

def generate_final_report(run_config: dict):
    """
    Generate the final report.

    Writes 5 sheets

    1. Run Configuration - Contains the run configuration
    2. Table Columns - Contains the table columns in the load generation
    3. Trace Data - Contains the trace data
    4. Transaction Aggregates - Contains the transaction aggregates
    5. Final Aggregates - Contains the final aggregates

    Args:
        run_config (dict): Run configuration
    """
    # Generate the final report
    final_traces_file = get_file_path(run_config, "final_traces")
    pg_xid_summary_file = get_file_path(run_config, "pg_xid_summary")
    run_config_file = get_file_path(run_config, "run_config")
    table_columns_file = get_file_path(run_config, "table_columns_csv")
    final_aggregates_file = get_file_path(run_config, "final_aggregates")
    final_report_file = get_file_path(run_config, "final_report")

    generate_pg_xid_summary_file(final_traces_file, pg_xid_summary_file)

    workbook, normal_fmt, bold_fmt = create_workbook(final_report_file)

    write_csv_to_worksheet(workbook.add_worksheet('Run Configuration'), run_config_file, normal_fmt, bold_fmt)
    write_csv_to_worksheet(workbook.add_worksheet('Table Columns'), table_columns_file, normal_fmt, bold_fmt)
    write_csv_to_worksheet(workbook.add_worksheet('Trace Data'), final_traces_file, normal_fmt, bold_fmt)
    write_csv_to_worksheet(workbook.add_worksheet('Transaction Aggregates'), pg_xid_summary_file, normal_fmt, bold_fmt)

    write_aggregates_to_worksheet(workbook.add_worksheet('Final Aggregates'), run_config, normal_fmt, bold_fmt)

    workbook.close()

def parse_arguments() -> argparse.Namespace:
    """
    Parse command line arguments.

    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(description="Run ingestion metrics logger")
    parser.add_argument('-c', '--config-file', type=str, default="load_config.yaml", help='Path to the config file')

    return parser.parse_args()

if __name__ == "__main__":
    # Parse command line arguments
    args = parse_arguments()

    # Extract arguments
    config_file = args.config_file

    with open(config_file) as f:
        run_config = yaml.safe_load(f)

    # Run the main function
    generate_final_report(run_config)
