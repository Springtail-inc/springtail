#!/bin/bash

set -e

if [ $# -ne 2 ]; then
    echo "Usage: $0 <log_file> <query_info_csv>"
    exit 1
fi

logfile="$1"
query_info="$2"

# Step 1: Extract traces.csv
grep '\[TRACE\] \[.*-xid_[0-9]\+,' "$logfile" \
| sed -E 's/.*\[(.*)-xid_([0-9]+),([^]]+)\].*/\1,\2,\3/' \
| sed 's/ms//g; s/us//g' \
| awk -F',' 'BEGIN {
    OFS=","; print "function,xid,totalms,totalmicros,counter,averagemicros,averagems"
} {
    split($3, kv1, "="); split($4, kv2, "="); split($5, kv3, "=");
    split($6, kv4, "="); split($7, kv5, "=");
    print $1, $2, kv1[2], kv2[2], kv3[2], kv4[2], kv5[2]
}' > traces.csv

# Step 2: Extract xid_mapping.csv
grep 'Committing PG XID:' "$logfile" \
| sed -E 's/.*PG XID: ([0-9]+) -> XID: ([0-9]+)/\1,\2/' \
| awk 'BEGIN {print "pg_xid,xid"} {print $0}' > xid_mapping.csv

# Step 3: Run Python script
python3 merge_pg_xid.py xid_mapping.csv traces.csv "$query_info" final_traces.csv

# Step 4: Cleanup intermediate CSVs
rm -f traces.csv xid_mapping.csv "$query_info"

echo "Generated final_traces.csv!"
