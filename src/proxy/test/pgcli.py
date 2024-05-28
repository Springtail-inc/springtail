import socket
import hashlib
import time

def send_startup(conn):
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
    bytes += b'postgres' + b'\x00'
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
        pos += length+1

        print ("code={}, length={}".format(code, length))

        if code == 'Z':
            return bytes[pos+5:pos+6].decode("utf-8")

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

def read_response(conn):
    bytes = conn.recv(1000)
    print ("Read response: {} bytes".format(len(bytes)))
    pos = 0

    while pos < len(bytes):
        code = bytes[pos:pos+1].decode("utf-8")
        length = int.from_bytes(bytes[pos+1:pos+5], 'big')

        # handle different codes
        if code == 'C':
            print('Command complete')
        elif code == 'T':
            print('Row description')
        elif code == 'D':
            print('Data row')
        elif code == 'Z':
            status = bytes[pos+5:pos+6].decode("utf-8")
            print('Ready for query: status={}'.format(status))
        elif code == 'E':
            print('Error response')
        elif code == 't':
            print('Parameter description')
        elif code == '1':
            print('Parse complete')
        elif code == '2':
            print('Bind complete')
        elif code == 'n':
            print('No data')

        pos += length+1

    return

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


host = '127.0.0.1'
port = 5432
conn = socket.socket()
conn.connect((host,port))

send_startup(conn)
code, length, salt = read_startup(conn)
send_md5(conn, 'test_md5', 'test', salt)
ready_status = read_auth_response(conn)


send_parse_message(conn, 'test', 'SELECT 1')
send_bind_message(conn, 'portal_test', 'test')
send_execute_message(conn, 'portal_test')
#send_parse_message(conn, 'test1', 'INSERT into test values(1)')
#send_bind_message(conn, 'portal1', 'test1')
#send_execute_message(conn, 'portal1')
#send_simple_query(conn, 'SELECT 1/0')

# send_simple_query(conn, 'FETCH ALL from portal_test')
#send_close_message(conn, 'P', 'portal_test')

print ("Connected")
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

conn.close()
