import socket
import hashlib
import time
import argparse
import json

def send_startup(conn, database):
    bytes = bytearray()
    # Startup message
    # The first 4 bytes are the length of the message

    # version
    v_maj = 3
    v_min = 0
    bytes.extend(v_maj.to_bytes(2, 'big'))
    bytes.extend(v_min.to_bytes(2, 'big'))
    bytes += b'user' + b'\x00'
    bytes += b'test_md5' + b'\x00'
    bytes += b'database' + b'\x00'
    bytes.extend(database.encode('utf-8'))
    bytes += b'\x00'
#    bytes += b'postgres' + b'\x00'
    bytes += b'client_encoding' + b'\x00'
    bytes += b'UTF8' + b'\x00'
    bytes += b'\x00'

    p_len = len(bytes) + 4
    bytes = p_len.to_bytes(4, 'big') + bytes

    conn.send(bytes)

def md5_hash(username, password, salt):
    up = hashlib.md5(password.encode() + username.encode()).hexdigest()
    hasher = hashlib.md5()
    hasher.update(up.encode())
    hasher.update(salt)
    return hasher.hexdigest()

def read_startup(conn):
    bytes = conn.recv(1000)
    code = bytes[0:1].decode("utf-8")
    length = int.from_bytes(bytes[1:5], 'big')
    salt = bytes[9:13]
    return code, length, salt

def send_md5(conn, username, password, salt):
    md5 = 'md5' + md5_hash(username, password, salt)
    bytes = bytearray()
    bytes.extend(b'\x70') # 'p'
    length = 40
    bytes.extend(length.to_bytes(4, 'big'))
    bytes.extend(md5.encode())
    bytes.extend(b'\x00')

    conn.send(bytes)

def read_auth_response(conn):
    bytes = conn.recv(1000)
    pos = 0

    while pos < len(bytes):
        code = bytes[pos:pos+1].decode("utf-8")
        length = int.from_bytes(bytes[pos+1:pos+5], 'big')

        # print ("code={}, length={}".format(code, length))

        if (code == 'R'):
            auth_type = int.from_bytes(bytes[pos+5:pos+9], 'big')
            print("Auth type: ", auth_type)
            if (auth_type == 0):
                print("  Authentication OK")
            elif (auth_type == 5):
                print("  MD5 authentication required")

        if (code == 'S'):
            # decode the parameter status
            name, end = decode_string(bytes[pos+5:])
            value, end = decode_string(bytes[pos+5+end+1:])
            print("  Parameter status: name={}, value={}".format(name, value))

        if code == 'Z':
            ready_for_query = bytes[pos+5:pos+6].decode("utf-8")
            print("Ready for query: status={}".format(ready_for_query))
            return ready_for_query

        pos += length+1

def decode_string(bytes):
    end = bytes.find(b'\x00')
    return (bytes[:end].decode('utf-8'), end)

def send_parse_message(conn, statement_name: str, query: str):
    buf = bytearray()
    # Message type
    buf.append(ord('P'))
    # Reserve space for the length of the message (will fill this in later)
    buf.extend(b'\x00\x00\x00\x00')
    # Statement name (null-terminated)
    buf.extend(statement_name.encode('utf-8') + b'\x00')
    # Query string (null-terminated)
    buf.extend(query.encode('utf-8') + b'\x00')
    # Number of parameter types
    buf.extend(b'\x00\x00')
    # Calculate the length of the message
    message_length = len(buf) - 1
    # Replace the reserved length bytes with the actual length (excluding the type byte)
    buf[1:5] = message_length.to_bytes(4, byteorder='big')

    conn.send(buf)

def send_bind_message(conn, portal_name: str, statement_name: str):
    buf = bytearray()
    # Message type
    buf.append(ord('B'))
    # Reserve space for the length of the message (will fill this in later)
    buf.extend(b'\x00\x00\x00\x00')
    # Portal name (null-terminated)
    buf.extend(portal_name.encode('utf-8') + b'\x00')
    # Statement name (null-terminated)
    buf.extend(statement_name.encode('utf-8') + b'\x00')
    # Number of parameter format codes
    buf.extend(b'\x00\x00')
    # Number of parameter values
    buf.extend(b'\x00\x00')
    # Number of result-column format codes
    buf.extend(b'\x00\x00')
    # Calculate the length of the message
    message_length = len(buf) - 1
    # Replace the reserved length bytes with the actual length (excluding the type byte)
    buf[1:5] = message_length.to_bytes(4, byteorder='big')

    conn.send(buf)

def send_execute_message(conn, portal_name: str):
    buf = bytearray()
    # Message type
    buf.append(ord('E'))
    # Reserve space for the length of the message (will fill this in later)
    buf.extend(b'\x00\x00\x00\x00')
    # Portal name (null-terminated)
    buf.extend(portal_name.encode('utf-8') + b'\x00')
    # Maximum number of rows to return
    buf.extend(b'\x00\x00\x00\x00')
    # Calculate the length of the message
    message_length = len(buf) - 1
    # Replace the reserved length bytes with the actual length (excluding the type byte)
    buf[1:5] = message_length.to_bytes(4, byteorder='big')

    conn.send(buf)

def send_sync_message(conn):
    buf = bytearray()
    # Message type
    buf.append(ord('S'))
    # Reserve space for the length of the message (will fill this in later)
    buf.extend(b'\x00\x00\x00\x00')
    # Calculate the length of the message
    message_length = len(buf) - 1
    # Replace the reserved length bytes with the actual length (excluding the type byte)
    buf[1:5] = message_length.to_bytes(4, byteorder='big')

    conn.send(buf)

def send_close_message(conn, close_type: str, name: str):
    buf = bytearray()
    # Message type
    buf.append(ord('C'))
    # Reserve space for the length of the message (will fill this in later)
    buf.extend(b'\x00\x00\x00\x00')
    # Close type (either 'S' for statement or 'P' for portal)
    buf.append(ord(close_type))
    # Name of the prepared statement or portal (null-terminated)
    buf.extend(name.encode('utf-8') + b'\x00')
    # Calculate the length of the message (excluding the type byte)
    message_length = len(buf) - 1
    # Replace the reserved length bytes with the actual length
    buf[1:5] = message_length.to_bytes(4, byteorder='big')

    conn.send(buf)

def read_connection(conn):
    try:
        bytes = conn.recv(2048)
    except Exception as e:
        print ("Error reading response from server: ", e)
        return bytearray()

    return bytes


def read_response(conn):
    """
       Reads a response from the server and processes it.
       Returns a flag indicating ready for query was received and a
       flag indicating an error was received.
    """
    bytes = read_connection(conn)

    read_len = len(bytes)
    # print ("Read response: {} bytes".format(read_len))

    if read_len == 0:
        # Connection closed, return error
        return (False, True)

    pos = 0

    ready_for_query = False

    while pos < read_len:
        code = bytes[pos:pos+1].decode("utf-8")
        length = int.from_bytes(bytes[pos+1:pos+5], 'big')

        if (length > read_len - pos):
            # read more data
            # print("Reading more data")
            bytes += read_connection(conn)
            read_len = len(bytes)

        # handle different codes
        if code == 'C':
            print('Command complete')
        elif code == 'T':
            print('Row description')
            decode_row_description(bytes[pos:])
        elif code == 'D':
            print('Data row')
            decode_data_row(bytes[pos:])
        elif code == 'Z':
            status = bytes[pos+5:pos+6].decode("utf-8")
            print('Ready for query: status={}'.format(status))
            ready_for_query = True
        elif code == 'E':
            print('Error response')
            decode_error(bytes[pos:])
        elif code == 't':
            print('Parameter description')
        elif code == '1':
            print('Parse complete')
        elif code == '2':
            print('Bind complete')
        elif code == 'n':
            print('No data')
        else:
            print('Unknown code: {}'.format(code))

        pos += length+1

    return (ready_for_query, False)

def send_simple_query(conn, query: str):
    buf = bytearray()
    # Message type
    buf.append(ord('Q'))
    # Reserve space for the length of the message (will fill this in later)
    buf.extend(b'\x00\x00\x00\x00')
    # Query string (null-terminated)
    buf.extend(query.encode('utf-8') + b'\x00')
    # Calculate the length of the message
    message_length = len(buf)-1
    # Replace the reserved length bytes with the actual length (excluding the type byte)
    buf[1:5] = message_length.to_bytes(4, byteorder='big')

    conn.send(buf)

def decode_error(message):
    offset = 0

    # Convert message to bytearray for easier manipulation
    message = bytearray(message)

    # Read the message type and length
    message_type = chr(message[offset])
    offset += 1
    message_length = int.from_bytes(message[offset:offset+4], byteorder='big')
    offset += 4

    if message_type != 'E':
        raise ValueError(f"Unexpected message type: {message_type}")

    error_fields = {}

    while offset < message_length:
        field_type = chr(message[offset])
        offset += 1

        field_value = message[offset:message.find(b'\x00', offset)].decode('utf-8')
        offset += len(field_value) + 1

        error_fields[field_type] = field_value

    print("Error Fields: S:{}, V:{}, C:{}; {}".format(error_fields.get('S', ''), error_fields.get('V', ''), error_fields.get('C', ''), error_fields.get('M', '')))

def decode_row_description(message):
    offset = 0

    # Convert message to bytearray for easier manipulation
    message = bytearray(message)

    # Read the message type and length
    message_type = chr(message[offset])
    offset += 1
    message_length = int.from_bytes(message[offset:offset+4], byteorder='big')
    offset += 4

    if message_type != 'T':
        raise ValueError(f"Unexpected message type: {message_type}")

    # Read the number of fields
    num_fields = int.from_bytes(message[offset:offset+2], byteorder='big')
    offset += 2

    print(f"Number of fields: {num_fields}")

    fields = []

    for _ in range(num_fields):
        # Read the field name (null-terminated string)
        end = message.find(b'\x00', offset)
        field_name = message[offset:end].decode('utf-8')
        offset = end + 1

        table_oid = int.from_bytes(message[offset:offset+4], byteorder='big')
        offset += 4
        column_attr_num = int.from_bytes(message[offset:offset+2], byteorder='big')
        offset += 2
        data_type_oid = int.from_bytes(message[offset:offset+4], byteorder='big')
        offset += 4
        data_type_size = int.from_bytes(message[offset:offset+2], byteorder='big')
        offset += 2
        type_modifier = int.from_bytes(message[offset:offset+4], byteorder='big')
        offset += 4
        format_code = int.from_bytes(message[offset:offset+2], byteorder='big')
        offset += 2

        fields.append({
            'field_name': field_name,
            'table_oid': table_oid,
            'column_attr_num': column_attr_num,
            'data_type_oid': data_type_oid,
            'data_type_size': data_type_size,
            'type_modifier': type_modifier,
            'format_code': format_code
        })

    for field in fields:
        print(f"Field name: {field['field_name']}")
        print(f"  Table OID: {field['table_oid']}")
        print(f"  Column Attribute Number: {field['column_attr_num']}")
        print(f"  Data Type OID: {field['data_type_oid']}")
        print(f"  Data Type Size: {field['data_type_size']}")
        print(f"  Type Modifier: {field['type_modifier']}")
        print(f"  Format Code: {field['format_code']}")

def decode_data_row(message):
    offset = 0

    # Convert message to bytearray for easier manipulation
    message = bytearray(message)

    # Read the message type and length
    message_type = chr(message[offset])
    offset += 1
    message_length = int.from_bytes(message[offset:offset+4], byteorder='big')
    offset += 4

    if message_type != 'D':
        raise ValueError(f"Unexpected message type: {message_type}")

    # Read the number of columns
    num_columns = int.from_bytes(message[offset:offset+2], byteorder='big')
    offset += 2

    print(f"Number of columns: {num_columns}")

    columns = []

    for _ in range(num_columns):
        # Read the length of the column value
        column_length = int.from_bytes(message[offset:offset+4], byteorder='big')
        offset += 4

        if column_length == -1:
            column_value = None
        else:
            column_value = message[offset:offset+column_length].decode('utf-8')
            offset += column_length

        columns.append(column_value)

    for i, column in enumerate(columns):
        print(f"Column {i + 1}: {column}")

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

    send_startup(conn, database)
    code, length, salt = read_startup(conn)
    send_md5(conn, username, password, salt)
    ready_status = read_auth_response(conn)

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
            send_parse_message(conn, statement_name, query)

        elif command_name == 'bind' and len(command_args) == 2:
            portal_name = command_args[0]
            statement_name = command_args[1]
            print(f"Sending bind message for portal: {portal_name} and statement: {statement_name}")
            send_bind_message(conn, portal_name, statement_name)

        elif command_name == 'execute' and len(command_args) == 1:
            portal_name = command_args[0]
            print(f"Sending execute message for portal: {portal_name}")
            send_execute_message(conn, portal_name)

        elif command_name == 'sync':
            print("Sending sync message")
            send_sync_message(conn)

        elif command_name == 'query' and len(command_args) == 1:
            query = command_args[0]
            print(f"Sending query: {query}")
            send_simple_query(conn, query)

        else:
            print (f"Unknown command or invalid args: {command_name}")

        # Wait for response and process it
        if command_name == 'sync' or command_name == 'query':
             ready_for_query = False
             while not ready_for_query:
                (ready_for_query, error) = read_response(conn)
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
                send_parse_message(conn, data[1], ' '.join(data[2:]))
                read_response(conn)
            elif data[0] == 'bind' and len(data) > 2:
                send_bind_message(conn, data[1], data[2])
                send_sync_message(conn)
                read_response(conn)
            elif data[0] == 'exec' and len(data) > 1:
                send_execute_message(conn, data[1])
                read_response(conn)
            elif data[0] == 'sync':
                send_sync_message(conn)
                read_response(conn)
            else:
                send_simple_query(conn, user_input)
                print ("Sending simple query: ", user_input)
                read_response(conn)
        except EOFError:
            print("\nExiting...")
            break

if __name__ == '__main__':
    main()
