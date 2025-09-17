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

def parse_arguments() -> argparse.Namespace:
    """Parse the command line arguments."""
    # Create the argument parser
    parser = argparse.ArgumentParser(description="Process command-line arguments for config file.")

    # Add arguments -f for config file and -b for build directory
    parser.add_argument('-f', '--config-file', type=str, required=False, help='Path to the configuration file')
    parser.add_argument('-d', '--daemon', type=str, required=True,
                        choices=["proxy",
                                 "pg_log_mgr_daemon",
                                 "sys_tbl_mgr_daemon",
                                 "pg_ddl_daemon",
                                 "pg_xid_subscriber_daemon"],
                        help="Process name")
    subparsers = parser.add_subparsers(dest="action", required=True)

    # set command
    set_parser = subparsers.add_parser("set", help="Set a key to a value")
    set_parser.add_argument("key", type=str, help="Key to set")
    set_parser.add_argument("value", type=str, help="Value to set")

    # get command
    get_parser = subparsers.add_parser("get", help="Get the value of a key")
    get_parser.add_argument("key", type=str, help="Key tp get")

    # Parse the arguments and return them
    args = parser.parse_args()
    return args

def get_ip_port(props: Properties, daemon: str) -> str:

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

    if args.action == "get":
        resp = requests.get("http://" + ip_port + "/" + args.key)
        print("GET response status code: ", resp.status_code)
        response_json = resp.json()
        print("GET response: ", json.dumps(response_json, indent=4))

    if args.action == "set":
        try:
            payload = json.loads(args.value)
        except json.JSONDecodeError as e:
            raise SystemExit(f"Invalid JSON in 'value' argument: {e}")

        resp = requests.post("http://" + ip_port + "/" + args.key, json=payload)
        print("POST response status code: ", resp.status_code)
        response_json = resp.json()
        print("POST response: ", json.dumps(response_json, indent=4))


if __name__ == "__main__":
    main()
