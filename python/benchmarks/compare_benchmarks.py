#!/usr/bin/env python3
import re
import sys
from prettytable import PrettyTable

# Regex patterns for metrics
METRICS = {
    "First run: primary test time": re.compile(r"First run:\s*primary test time:\s*([0-9.]+)"),
    "First run: replica test time": re.compile(r"First run:\s*replica test time:\s*([0-9.]+)"),
    "Second run: primary test time": re.compile(r"Second run:\s*primary test time:\s*([0-9.]+)"),
    "Second run: replica test time": re.compile(r"Second run:\s*replica test time:\s*([0-9.]+)"),
}

def parse_benchmarks(text: str):
    """Parse logs starting with 'Benchmark:' lines."""
    results = {}
    blocks = re.split(r'(?=Benchmark:\s)', text)
    for block in blocks:
        match = re.match(r'Benchmark:\s*(.+)', block)
        if not match:
            continue
        name = match.group(1).strip()
        data = {}
        for key, rx in METRICS.items():
            m = rx.search(block)
            if m:
                data[key] = float(m.group(1))
        results[name] = data
    return results

def pct_diff(replica, primary):
    if primary is None or replica is None:
        return None
    if primary == 0:
        return "inf"
    return (replica - primary) / primary * 100.0

def fmt_num(x, digits=6):
    return f"{x:.{digits}f}" if isinstance(x, (int, float)) else "-"

def fmt_pct(x):
    if x is None:
        return "-"
    if x == "inf":
        return "inf"
    return f"{x:+.2f}%"

def fmt_diff(x):
    if x is None:
        return "-"
    return f"{x:+.2f}"

def main(main_path, branch_path):
    with open(main_path, "r", encoding="utf-8", errors="replace") as f:
        main_txt = f.read()
    with open(branch_path, "r", encoding="utf-8", errors="replace") as f:
        branch_txt = f.read()

    runs = {
        "Main": parse_benchmarks(main_txt),
        "Branch": parse_benchmarks(branch_txt),
    }

    all_benchmarks = sorted(set(runs["Main"].keys()) | set(runs["Branch"].keys()))

    table = PrettyTable()
    table.field_names = [
        "Benchmark File", "Source",
        "Primary (First)", "Replica (First)", "% Δ (First)",
        "Primary (Second)", "Replica (Second)", "% Δ (Second)",
    ]

    for bench in all_benchmarks:
        pcts = {"Main": {}, "Branch": {}}
        for src in ["Main", "Branch"]:
            d = runs[src].get(bench, {})
            p1 = d.get("First run: primary test time")
            r1 = d.get("First run: replica test time")
            p2 = d.get("Second run: primary test time")
            r2 = d.get("Second run: replica test time")

            pct1 = pct_diff(r1, p1)
            pct2 = pct_diff(r2, p2)
            pcts[src]["first"] = pct1
            pcts[src]["second"] = pct2

            table.add_row([
                bench if src == "Main" else "",
                src,
                fmt_num(p1),
                fmt_num(r1),
                fmt_pct(pct1),
                fmt_num(p2),
                fmt_num(r2),
                fmt_pct(pct2),
            ])

        # Add diff row
        if all(v is not None for v in [pcts["Main"]["first"], pcts["Branch"]["first"]]):
            diff_first = pcts["Branch"]["first"] - pcts["Main"]["first"]
        else:
            diff_first = None
        if all(v is not None for v in [pcts["Main"]["second"], pcts["Branch"]["second"]]):
            diff_second = pcts["Branch"]["second"] - pcts["Main"]["second"]
        else:
            diff_second = None

        table.add_row([
            "",
            "Δ Branch–Main",
            "", "", fmt_diff(diff_first),
            "", "", fmt_diff(diff_second),
        ])

    print(table)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python compare_benchmarks.py <main.txt> <branch.txt>")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])

