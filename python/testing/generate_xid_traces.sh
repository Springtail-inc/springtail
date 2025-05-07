set -euo pipefail

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <log_file> <query_info_csv>"
    exit 1
fi

logfile="$1"
query_info="$2"

###############################################################################
# STEP 0 ── Dump the traces in the log
###############################################################################
touch /tmp/output_trace.txt
echo "Waiting for traces to be dumped"
sleep 5

###############################################################################
# STEP 1A ── XID traces
###############################################################################
grep '\[TRACE\] \[.*-xid_[0-9]\+,' "$logfile" |
sed -E 's/.*\[(.*)-xid_([0-9]+),([^]]+)\].*/\1,\2,\3/' |
sed -E 's/([^,0-9])([0-9]+)(ms|us)/\1\2/g; s/([0-9]+)(ms|us)/\1/g' |
awk -F',' '
BEGIN { OFS=","; print "function,xid,totalms,totalmicros,counter,averagemicros,averagems" }
{
    split($3, kv1, "="); split($4, kv2, "="); split($5, kv3, "=")
    split($6, kv4, "="); split($7, kv5, "=")
    print $1, $2, kv1[2], kv2[2], kv3[2], kv4[2], kv5[2]
}' > /tmp/xid_traces.csv

###############################################################################
# STEP 1B ── PG-XID traces
###############################################################################
if grep -q '\[TRACE\] \[.*-pgxid_[0-9]\+,' "$logfile"; then
    grep '\[TRACE\] \[.*-pgxid_[0-9]\+,' "$logfile" |
    sed -E 's/.*\[(.*)-pgxid_([0-9]+),([^]]+)\].*/\1,\2,\3/' |
    sed 's/ms//g; s/us//g' |
    awk -F',' '
    BEGIN { OFS=","; print "function,pgxid,totalms,totalmicros,counter,averagemicros,averagems" }
    {
        split($3, kv1, "="); split($4, kv2, "="); split($5, kv3, "=")
        split($6, kv4, "="); split($7, kv5, "=")
        print $1, $2, kv1[2], kv2[2], kv3[2], kv4[2], kv5[2]
    }' > /tmp/pgxid_traces.csv
else
    echo "function,pgxid,totalms,totalmicros,counter,averagemicros,averagems" \
        > /tmp/pgxid_traces.csv
fi

###############################################################################
# STEP 2A ── xid_mapping.csv
###############################################################################
if grep -q 'Committing PG XID:' "$logfile"; then
    grep 'Committing PG XID:' "$logfile" |
    sed -E 's/.*PG XID: ([0-9]+) -> XID: ([0-9]+)/\1,\2/' |
    awk 'BEGIN { print "pg_xid,xid" } { print }' \
        > /tmp/xid_mapping.csv
else
    echo "pg_xid,xid" > /tmp/xid_mapping.csv
fi

###############################################################################
# STEP 2B ── rewrite pgxid → xid and merge (comma-separated output)
###############################################################################
awk -F',' -v OFS=',' '
    NR==FNR { if (NR>1) map[$1]=$2; next }        # load mapping
    FNR==1  { next }                              # skip pgxid header
    {
        if ($2 in map) $2 = map[$2];              # substitute when possible
        print                                      # **comma-delimited now**
    }
' /tmp/xid_mapping.csv /tmp/pgxid_traces.csv >> /tmp/xid_traces.csv

mv /tmp/xid_traces.csv /tmp/traces.csv     # keep interface for the Python step

###############################################################################
# STEP 3 ── Python post-processing
###############################################################################
python3 merge_pg_xid.py /tmp/xid_mapping.csv /tmp/traces.csv "$query_info" /tmp/final_traces.csv

###############################################################################
# STEP 4 ── clean up intermediates
###############################################################################
rm -f /tmp/xid_traces.csv /tmp/pgxid_traces.csv /tmp/xid_mapping.csv /tmp/traces.csv

echo "Generated final_traces.csv and removed temporary files!"
