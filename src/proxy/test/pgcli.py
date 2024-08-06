import socket
import argparse
import json

import pglib

def parse_arguments():
    parser = argparse.ArgumentParser(description="Parse database connection parameters.")

    parser.add_argument('-H', '--hostname', type=str, default='127.0.0.1', help="The hostname of the database server")
    parser.add_argument('-p', '--port', type=int, default=5432, help="The port number on which the database server is listening")
    parser.add_argument('-U', '--username', type=str, default='test', required=True, help="The username for connecting to the database")
    parser.add_argument('-P', '--password', type=str, required=True, help="The password for connecting to the database")
    parser.add_argument('-s', '--requires_ssl', action='store_true', help="Flag to indicate if SSL is required")
    parser.add_argument('-f', '--filename', type=str, help="The name of the file containing the SQL commands to execute")
    parser.add_argument('-d', '--database', type=str, default='postgres', required=True, help="The name of the database to connect to")

    args = parser.parse_args()
    return args

# read set of commands from a json file
def read_commands_from_json(file_path):
    json_objects = []
    try:
        with open(file_path, 'r') as file:
            for line in file:
                line = line.strip()  # Remove any leading/trailing whitespace
                if line and not line.startswith('#'):  # Ensure the line is not empty
                    json_object = json.loads(line)
                    json_objects.append(json_object)
    except Exception as e:
        print(f"Error reading or parsing file: {e}")
        return None

    return json_objects

def connect_to_postgres(host, port, username, password, database, requires_ssl):
    conn = socket.socket()
    conn.connect((host, port))

    pglib.send_startup(conn, database)
    code, length, salt = pglib.read_startup(conn)
    pglib.send_md5(conn, username, password, salt)
    ready_status = pglib.read_auth_response(conn)

    return conn

def main():
    # Parse command line arguments
    args = parse_arguments()

    # Connect to the PostgreSQL server
    conn = connect_to_postgres(args.hostname, args.port, args.username, args.password, args.database, args.requires_ssl)
    print("Connected to PostgreSQL server")

    # Read commands from json file
    commands = read_commands_from_json(args.filename);

    # Process each command
    for command in commands:
        command_name = command['name']
        command_args = command['args']

        # Process the command and its arguments here
        # Example:
        if command_name == 'parse' and len(command_args) == 2:
            statement_name = command_args[0]
            query = command_args[1]
            print(f"Sending parse message for statement: {statement_name} and query: {query}")
            pglib.send_parse_message(conn, statement_name, query)

        elif command_name == 'bind' and len(command_args) == 2:
            portal_name = command_args[0]
            statement_name = command_args[1]
            print(f"Sending bind message for portal: {portal_name} and statement: {statement_name}")
            pglib.send_bind_message(conn, portal_name, statement_name)

        elif command_name == 'execute' and len(command_args) == 1:
            portal_name = command_args[0]
            print(f"Sending execute message for portal: {portal_name}")
            pglib.send_execute_message(conn, portal_name)

        elif command_name == 'sync':
            print("Sending sync message")
            pglib.send_sync_message(conn)

        elif command_name == 'query' and len(command_args) == 1:
            query = command_args[0]
            print(f"Sending query: {query}")
            pglib.send_simple_query(conn, query)

        else:
            print (f"Unknown command or invalid args: {command_name}")

        # Wait for response and process it
        if command_name == 'sync' or command_name == 'query':
             ready_for_query = False
             while not ready_for_query:
                (ready_for_query, error) = pglib.read_response(conn)
                if error:
                    print("Error reading response from server")
                    conn.close()
                    return

    # Close the connection
    conn.close()

def cli(conn):
    print("Enter your commands (press Ctrl-D to quit):")
    while True:
        try:
            user_input = input("> ")  # Prompt user for input
            data = user_input.split()
            if data[0] == 'parse' and len(data) > 2:
                pglib.send_parse_message(conn, data[1], ' '.join(data[2:]))
            elif data[0] == 'bind' and len(data) > 2:
                pglib.send_bind_message(conn, data[1], data[2])
                pglib.send_sync_message(conn)
            elif data[0] == 'exec' and len(data) > 1:
                pglib.send_execute_message(conn, data[1])
            elif data[0] == 'sync':
                pglib.send_sync_message(conn)
            else:
                pglib.send_simple_query(conn, user_input)
                print ("Sending simple query: ", user_input)

            pglib.read_response(conn)

        except EOFError:
            print("\nExiting...")
            break

if __name__ == '__main__':
    main()
