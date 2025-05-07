import csv
import sys

def main(mapping_file, trace_file, query_info_file, output_file):
    # Load xid to pg_xid mapping
    xid_to_pg_xid = {}
    with open(mapping_file, newline='') as mapfile:
        reader = csv.DictReader(mapfile)
        for row in reader:
            xid_to_pg_xid[row['xid']] = row['pg_xid']

    # Load query info mapping by pg_xid
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

    # Merge traces
    with open(trace_file, newline='') as tracesfile, open(output_file, 'w', newline='') as outputfile:
        reader = csv.DictReader(tracesfile)
        fieldnames = (
            ['pg_xid'] + reader.fieldnames +
            ['query_type', 'duration_ms', 'pg_timestamp', 'rows_affected', 'table_name']
        )
        writer = csv.DictWriter(outputfile, fieldnames=fieldnames)

        writer.writeheader()
        for row in reader:
            xid = row['xid']
            pg_xid = xid_to_pg_xid.get(xid, '')

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

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print(f"Usage: {sys.argv[0]} <mapping_file> <trace_file> <query_info_file> <output_file>")
        sys.exit(1)

    mapping_file = sys.argv[1]
    trace_file = sys.argv[2]
    query_info_file = sys.argv[3]
    output_file = sys.argv[4]

    main(mapping_file, trace_file, query_info_file, output_file)
