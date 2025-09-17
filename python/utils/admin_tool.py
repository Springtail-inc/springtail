import argparse
import json
import os
from redis import Redis
import requests
import sys

# Get the parent directory of the current script (i.e., the project root directory)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the /shared directory to the Python path
sys.path.append(os.path.join(project_root, 'shared'))

# Now import the Properties class
from properties import Properties

class CustomHelpFormatter(argparse.RawTextHelpFormatter):
    def _get_help_string(self, action):
        """Override to include subparser arguments in the main help."""
        help_str = super()._get_help_string(action)
        if isinstance(action, argparse._SubParsersAction):
            for subparser in action.choices.values():
                help_str += '\n' + subparser.format_help()
        return help_str


def parse_arguments() -> argparse.Namespace:
    """Parse the command line arguments."""
    # Create the argument parser
    parser = argparse.ArgumentParser(description="Process command-line arguments for config file.",
                                     formatter_class=CustomHelpFormatter
                                     )

    parser.add_argument('-f', '--config-file', type=str, required=False, help='Path to the configuration file')
    parser.add_argument('-d', '--daemon', type=str, required=True,
                        choices=["proxy",
                                 "pg_log_mgr_daemon",
                                 "sys_tbl_mgr_daemon",
                                 "pg_ddl_daemon",
                                 "pg_xid_subscriber_daemon"],
                        help="Process name")
    subparsers = parser.add_subparsers(
        dest="action", required=False,
        help="Specify info to get, when none is specified, gets daemon specific info")

    # health
    health_parser = subparsers.add_parser("health", help="Get process health status", formatter_class=parser.formatter_class)

    # config
    config_parser = subparsers.add_parser("config", help="Get process configuration", formatter_class=parser.formatter_class)

    # logging
    logging_parser = subparsers.add_parser("logging", help="Get and set process logging details", formatter_class=parser.formatter_class)
    logging_parser.add_argument("--log_level", choices=["debug", "info", "warn", "error", "critical"],
                                type=str, help="Set logging level")
    logging_parser.add_argument("--debug_level", choices=[1, 2, 3, 4],
                                type=int, help="Select logging output")
    logging_parser.add_argument("--module_mask_toggle",
                                choices=[
                                    "pg_repl",
                                    "pg_log_mgr",
                                    "write_cache_server",
                                    "btree",
                                    "storage",
                                    "xid_mgr",
                                    "common",
                                    "proxy",
                                    "fdw",
                                    "cache",
                                    "schema",
                                    "committer",
                                    "sys_tbl_mgr",
                                    "none",
                                    "all"
                                ],
                                type=str, help="Toggle module masks")

    # Parse the arguments and return them
    args = parser.parse_args()

    return args

def get_ip_port(props: Properties, daemon: str) -> str:
    """Get IP and port from redis."""
    redis = props.get_config_redis()

    db_instance_id = props.get_db_instance_id()
    instance_key = props.get_instance_key()

    return redis.hget(db_instance_id + ":admin_console", instance_key + ":" + daemon)


def main():
    """Main function to admin console tool."""
    # Parse command line arguments
    args = parse_arguments()

    # Load the system properties from the system.json file
    props = Properties(args.config_file, False)

    ip_port = get_ip_port(props, args.daemon)

    print(f"Found IP and port: {ip_port}")

    match args.action:
        case "health" | "config":
            resp = requests.get("http://" + ip_port + "/" + args.action)
            print("GET response status code: ", resp.status_code)
            response_json = resp.json()
            print("GET response: ", json.dumps(response_json, indent=4))

        case "logging":
            with requests.Session() as session:
                get_resp = session.get("http://" + ip_port + "/logging")
                response_json = get_resp.json()
                resp = None
                if (get_resp.status_code != 200):
                    raise SystemExit(f"Failed to get logging info")
                if (args.log_level is not None):
                    payload = json.loads(f'{{"log_level": "{args.log_level}"}}')
                    resp = session.post("http://" + ip_port + "/logging", json=payload)
                if (args.debug_level is not None):
                    payload = json.loads(f'{{"debug_level": {args.debug_level} }}')
                    resp = session.post("http://" + ip_port + "/logging", json=payload)
                if (args.module_mask_toggle is not None):
                    module_value = response_json["module_mask"][args.module_mask_toggle]
                    payload = {
                        "module_mask": {
                            "module": args.module_mask_toggle,
                            "value": not module_value
                        }
                    }
                    resp = session.post("http://" + ip_port + "/logging", json=payload)
                if resp is None:
                    print("GET response status code: ", get_resp.status_code)
                    print("GET response: ", json.dumps(response_json, indent=4))
                else:
                    print("POST response status code: ", resp.status_code)
                    response_json = resp.json()
                    print("POST response: ", json.dumps(response_json, indent=4))

        case _:
            resp = requests.get("http://" + ip_port + "/" + args.daemon)
            print("GET response status code: ", resp.status_code)
            response_json = resp.json()
            print("GET response: ", json.dumps(response_json, indent=4))


if __name__ == "__main__":
    main()
