# Description: Pretty print the local cluster endpoints
# Usage: python pp.py
import os


def print_table(headers, rows):
    cols = list(zip(*([headers] + rows)))
    widths = [max(len(str(x)) for x in col) for col in cols]

    def fmt_row(r): return "  ".join(f"{str(c):<{w}}" for c, w in zip(r, widths))

    print(fmt_row(headers))
    # print("  ".join("-" * w for w in widths))
    for r in rows: print(fmt_row(r))


headers = ["CONTAINER NAME", "HOST ENDPOINT", "IN-CONTAINER ENDPOINT"]
rows = [
    ["redis", f"localhost:{os.environ['HOST_PORT_REDIS']}", "redis:6379"],
    ["mock AWS", f"localhost:{os.environ['HOST_PORT_MOTO']}", "aws-mock:7000"],
    ["primary", f"localhost:{os.environ['HOST_PORT_PRIMARY_DB']}", "primary:5432"],
    ["proxy", f"localhost:{os.environ['HOST_PORT_PROXY']}", "proxy:5432"],
    ["fdw1", f"localhost:{os.environ['HOST_PORT_FDW1']}", "fdw1:5432"],
    ["fdw2", f"localhost:{os.environ['HOST_PORT_FDW2']}", "fdw2:5432"],
    ["controller", f"localhost:19824", "controller:8000"],
]
# print a green dot before
print_table(headers, rows)
