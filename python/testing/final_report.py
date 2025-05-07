import csv
import xlsxwriter
import pandas as pd

# Load your data
df = pd.read_csv("/tmp/final_traces.csv")

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
final_df.to_csv("/tmp/pg_xid_summary.csv", index=False)

# Generate the final report
RUN_CONFIG_FILE = '/tmp/run_config.csv'
TABLE_COLUMNS_FILE = '/tmp/table_columns.csv'
PG_XID_SUMMARY_FILE = '/tmp/pg_xid_summary.csv'
FINAL_TRACES_FILE = '/tmp/final_traces.csv'

workbook = xlsxwriter.Workbook('final_report.xlsx', {'constant_memory': True})
bold = workbook.add_format({'bold': True})

def write_csv_to_worksheet(worksheet, csv_file):
    col_widths = []
    with open(csv_file, 'r') as f:
        reader = csv.reader(f)
        for r, row in enumerate(reader):
            for c, col in enumerate(row):
                if r == 0:
                    worksheet.write(r, c, col, bold)
                else:
                    worksheet.write(r, c, col)
                # Expand col_widths if needed
                if len(col_widths) <= c:
                    col_widths.extend([0] * (c - len(col_widths) + 1))

                # Update max width
                col_widths[c] = max(col_widths[c], len(col))

    # Set the column widths with padding
    for c, width in enumerate(col_widths):
        worksheet.set_column(c, c, width)

write_csv_to_worksheet(workbook.add_worksheet('Run Configuration'), RUN_CONFIG_FILE)
write_csv_to_worksheet(workbook.add_worksheet('Table Columns'), TABLE_COLUMNS_FILE)
write_csv_to_worksheet(workbook.add_worksheet('Trace Data'), FINAL_TRACES_FILE)
write_csv_to_worksheet(workbook.add_worksheet('Transaction Aggregates'), PG_XID_SUMMARY_FILE)

# Sum the total_ms and duration_ms columns in the final traces sheet
total_ms = 0
duration_ms = 0
with open(FINAL_TRACES_FILE, 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        total_ms += float(row['totalms'])
        duration_ms += float(row['duration_ms'])

def write_aggregates_to_worksheet():
    # Write the aggregates to a new sheet
    worksheet5 = workbook.add_worksheet('Final Aggregates')

    # Track max width for each column
    col_widths = [0, 0]  # two columns: label and value

    # Row/column values
    aggregates = [
        ('Ingest total time', total_ms),
        ('Primary total time', duration_ms)
    ]

    # Analyze pg_xid summary
    gt0 = 0
    lt0 = 0
    with open(PG_XID_SUMMARY_FILE, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            val = float(row['ingest_time_minus_primary_time'])
            if val > 0:
                gt0 += 1
            elif val < 0:
                lt0 += 1

    aggregates += [
        ('Number of times ingest is slower', gt0),
        ('Number of times ingest is faster', lt0)
    ]

    # Write to worksheet and track max widths
    for r, (label, value) in enumerate(aggregates):
        label_str = str(label)
        value_str = str(value)

        worksheet5.write(r, 0, label_str, bold)
        worksheet5.write(r, 1, value)

        col_widths[0] = max(col_widths[0], len(label_str))
        col_widths[1] = max(col_widths[1], len(value_str))

    # Apply column widths with padding
    for c, width in enumerate(col_widths):
        worksheet5.set_column(c, c, width)

write_aggregates_to_worksheet()

workbook.close()
