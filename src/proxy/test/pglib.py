import hashlib

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

def md5_hash_user_pass(username, password, salt):
    up = hashlib.md5(password.encode() + username.encode()).hexdigest()
    hasher = hashlib.md5()
    hasher.update(up.encode())
    hasher.update(salt)
    return hasher.hexdigest()

def md5_hash(strs):
    hasher = hashlib.md5()
    for str in strs:
        if str is not None:
            hasher.update(str.encode())
    return hasher.hexdigest()

def read_startup(conn):
    bytes = conn.recv(1000)
    code = bytes[0:1].decode("utf-8")
    length = int.from_bytes(bytes[1:5], 'big')
    salt = bytes[9:13]
    return code, length, salt

def send_md5(conn, username, password, salt):
    md5 = 'md5' + md5_hash_user_pass(username, password, salt)
    bytes = bytearray()
    bytes.extend(b'\x70') # 'p'
    length = 40
    bytes.extend(length.to_bytes(4, 'big'))
    bytes.extend(md5.encode())
    bytes.extend(b'\x00')

    conn.send(bytes)

def decode_auth_response(bytes):
    auth_type = int.from_bytes(bytes[5:9], 'big')
    return auth_type

def auth_type_to_string(auth_type):
    if (auth_type == 0):
        return "Authentication OK"
    elif (auth_type == 2):
        return "KerberosV5 authentication"
    elif (auth_type == 3):
        return "Cleartext authentication"
    elif (auth_type == 5):
        return "MD5 authentication"
    elif (auth_type == 7):
        return "GSSAPI authentication"
    elif (auth_type == 8):
        return "GSS continue"
    elif (auth_type == 9):
        return "SSPI authentication"
    elif (auth_type == 10):
        return "SCRAM-SHA-256 authentication"
    elif (auth_type == 11):
        return "SASL continue"
    elif (auth_type == 12):
        return "SASL final"
    else:
        return "Unknown authentication type: {}".format(auth_type)

def decode_parameter_status(bytes):
    name, end = decode_string(bytes[5:])
    value, end = decode_string(bytes[5+end+1:])
    return name, value

def decode_backend_key_data(bytes):
    pid = int.from_bytes(bytes[5:9], 'big')
    secret = int.from_bytes(bytes[9:13], 'big')
    return pid, secret

def decode_ready_for_query(bytes):
    return bytes[5:6].decode("utf-8")

def decode_close_request(bytes):
    type = bytes[5:6].decode("utf-8")
    name, end = decode_string(bytes[6:])
    return type, name

def decode_describe_request(bytes):
    type = bytes[5:6].decode("utf-8")
    name, end = decode_string(bytes[6:])
    return type, name

def decode_string(bytes):
    end = bytes.find(b'\x00')
    return (bytes[:end].decode('utf-8'), end)

def decode_parse_request(bytes):
    stmt, end = decode_string(bytes[5:])
    return stmt

def decode_bind_request(bytes):
    portal, end = decode_string(bytes[5:])
    stmt, end = decode_string(bytes[5+end+1:])
    return portal, stmt

def decode_execute_request(bytes):
    portal, end = decode_string(bytes[5:])
    return portal

def read_auth_response(conn):
    bytes = conn.recv(1000)
    pos = 0

    while pos < len(bytes):
        code = bytes[pos:pos+1].decode("utf-8")
        length = int.from_bytes(bytes[pos+1:pos+5], 'big')

        # print ("code={}, length={}".format(code, length))

        if (code == 'R'):
            auth_type = decode_auth_response(bytes[pos:])
            print("Auth type: ", auth_type_to_string(auth_type))

        if (code == 'S'):
            # decode the parameter status
            name, value = decode_parameter_status(bytes[pos:])
            print("  Parameter status: name={}, value={}".format(name, value))

        if code == 'Z':
            ready_for_query = decode_ready_for_query(bytes[pos:])
            print("Ready for query: status={}".format(ready_for_query))
            return ready_for_query

        pos += length+1


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
            fields = decode_row_description(bytes[pos:])
            for field in fields:
                print(f"Field name: {field['field_name']}")
                print(f"  Table OID: {field['table_oid']}")
                print(f"  Column Attribute Number: {field['column_attr_num']}")
                print(f"  Data Type OID: {field['data_type_oid']}")
                print(f"  Data Type Size: {field['data_type_size']}")
                print(f"  Type Modifier: {field['type_modifier']}")
                print(f"  Format Code: {field['format_code']}")
        elif code == 'D':
            print('Data row')
            columns = decode_data_row(bytes[pos:])
            print("Number of columns: ", len(columns))
            for i, column in enumerate(columns):
                print(f"Column {i + 1}: {column}")
        elif code == 'Z':
            status = decode_ready_for_query(bytes[pos:])
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

    return error_fields

def decode_row_description(message):
    offset = 0

    # Convert message to bytearray for easier manipulation
    message = bytearray(message)
    print("decode_row_description: message length = ", len(message))

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

    fields = []

    for _ in range(num_fields):
        if offset >= message_length:
            raise ValueError(f"Offset {offset} exceeded message length {message_length}")
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

    return fields

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
    columns = []
    num_columns = int.from_bytes(message[offset:offset+2], byteorder='big')
    offset += 2

    for _ in range(num_columns):
        # Read the length of the column value
        column_length = int.from_bytes(message[offset:offset+4], byteorder='big', signed=True)
        offset += 4

        if column_length == -1:
            column_value = None
        else:
            column_value = message[offset:offset+column_length].decode('utf-8')
            offset += column_length

        columns.append(column_value)

    return columns

def decode_startup_message(message, has_proto=False):
    offset = 0

    # Convert message to bytearray for easier manipulation
    message = bytearray(message)

    # Read the message type and length
    message_type = chr(message[offset])
    offset += 1
    message_length = int.from_bytes(message[offset:offset+4], byteorder='big')
    offset += 4

    if message_type != '?':
        raise ValueError(f"Unexpected message type: {message_type}")

    # NOTE: no protocol version is in the logged message, so we'll assume it's 3.0
    if not has_proto:
        proto_major = 3
        proto_minor = 0
    else:
        proto_major = int.from_bytes(message[offset:offset+2], byteorder='big')
        offset += 2
        proto_minor = int.from_bytes(message[offset:offset+2], byteorder='big')
        offset += 2

    params = []
    while message[offset] != 0:
        name, end = decode_string(message[offset:])
        offset += end + 1
        value, end = decode_string(message[offset:])
        offset += end + 1

        params.append((name, value))

    return proto_major, proto_minor, params
