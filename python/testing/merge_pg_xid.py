import csv
import sys

def main(mapping_file, trace_file, output_file):
    # Load xid mapping
    xid_to_pg_xid = {}
    with open(mapping_file, newline='') as mapfile:
        reader = csv.DictReader(mapfile)
        for row in reader:
            xid_to_pg_xid[row['xid']] = row['pg_xid']

    # Merge into traces
    with open(trace_file, newline='') as tracesfile, open(output_file, 'w', newline='') as outputfile:
        reader = csv.DictReader(tracesfile)
        fieldnames = ['pg_xid'] + reader.fieldnames  # Add pg_xid as the first column
        writer = csv.DictWriter(outputfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for row in reader:
            xid = row['xid']
            pg_xid = xid_to_pg_xid.get(xid, '')
            row_with_pg = {'pg_xid': pg_xid, **row}
            writer.writerow(row_with_pg)

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} <mapping_file> <trace_file> <output_file>")
        sys.exit(1)
    
    mapping_file = sys.argv[1]
    trace_file = sys.argv[2]
    output_file = sys.argv[3]
    
    main(mapping_file, trace_file, output_file)
