import argparse
import struct
import datetime
import logging, sys
import psycopg2
from psycopg2.extras import LoggingConnection

import pglib

TYPE_MAP = {
    1: 'From Client',
    2: 'From Replica',
    3: 'From Primary',
    4: 'To Client',
    5: 'To Replica',
    6: 'To Primary'
}

# key is type flag value
TYPE_REQ_RESP_MAP = {
    1: "Request",
    2: "Response",
    3: "Response",
    4: "Response",
    5: "Request",
    6: "Request"
}

# map of session_id to partial data (in extended query)
PARTIAL_DATA_MAP = {
}

# map of session_id to id in sessions table (sessions.id)
SESSION_ID_MAP = {
}

def connect_to_postgres(hostname, port, username, password, database, requires_ssl=False):
    """Connect to the PostgreSQL server."""
    conn = psycopg2.connect(
        host=hostname,
        port=port,
        user=username,
        password=password,
        database=database,
        sslmode='require' if requires_ssl else 'prefer',
        connection_factory=LoggingConnection
    )

    return conn

def db_truncate_tables(conn):
    """Truncate the tables in the database."""
    if conn is None:
        return

    cursor = conn.cursor()

    cursor.execute("TRUNCATE TABLE messages")
    cursor.execute("TRUNCATE TABLE sessions")

    conn.commit()

def db_insert_session(conn, header, endpoint):
    """Insert a session into the database."""
    if conn is None:
        return

    cursor = conn.cursor()

    # Insert the session into the database

    cursor.execute(
        """INSERT INTO sessions (server_id, session_id, type, customer_id, endpoint, created_at)
           VALUES (%s, %s, %s, %s, %s, %s) RETURNING id""",
        (header['server_id'], header['session_id'], header['type_str'],
         0, endpoint, header['timestamp'])
    )

    id = cursor.fetchone()[0]
    conn.commit()

    SESSION_ID_MAP[header['session_id']] = id


def db_update_session_disconnect(conn, header):
    """Update the session in the database to indicate disconnect."""
    if conn is None:
        return

    if header['session_id'] not in SESSION_ID_MAP:
        logging.error("Session ID not found in map: {}".format(header['session_id']))
        raise Exception("Session ID not found in map: {}".format(header['session_id']))
        return

    cursor = conn.cursor()

    id = SESSION_ID_MAP[header['session_id']]

    # Update the session in the database
    cursor.execute(
        """UPDATE sessions SET disconnected_at = %s WHERE id = %s AND server_id = %s""",
        (header['timestamp'], id, header['server_id'])
    )

    conn.commit()

    del SESSION_ID_MAP[header['session_id']]


def db_insert_msg(conn, header, code, message, data_row=None):
    """Insert a message into the database."""
    if conn is None:
        return

    if header['session_id'] not in SESSION_ID_MAP:
        logging.error("Session ID not found in map: {}".format(header['session_id']))
        raise Exception("Session ID not found in map: {}".format(header['session_id']))
        return

    id = SESSION_ID_MAP[header['session_id']]

    cursor = conn.cursor()

    # Insert the message into the database
    cursor.execute(
        """INSERT INTO messages (sessions_id_pkey, server_id, session_id,
               type, client_session_id, seq_num, code, message, data_row_hash, timestamp)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (id, header['server_id'], header['session_id'], header['type_str'], header['seq_id_client_id'],
         header['seq_id_seq_id'], code, message, data_row, header['timestamp'])
    )

    conn.commit()

def dump_hex(data):
    """Dump the data in hex format."""
    for (i, c) in enumerate(data):
        print("{:02x}".format(c), end=' ')
        if i % 4 == 0:
            print()

def decode_header(file):
    """Decode the header of a log record.

    A code of 0 indicates that the record is in extended/raw Postgresql format.
    The seqID is split into two parts: the client session ID, and the seq ID.
    """
    # Define the format string according to the specified structure
    format_str = '<BBcBQLLLLL' # little-endian format

    # Calculate the size of the header based on the format string
    header_size = struct.calcsize(format_str)

    # Read the header data from the file
    logging.debug("Reading hdr: {} bytes".format(header_size))
    header_data = file.read(header_size)
    if not header_data:
        return None

    # dump_hex(header_data)

    # Unpack the header data
    unpacked_data = struct.unpack(format_str, header_data)

    # Assign the unpacked data to respective fields
    header = {
        'type_flag': unpacked_data[0],
        'type_str': TYPE_MAP[unpacked_data[0]],
        'final_flag': unpacked_data[1],
        'code': unpacked_data[2].decode("utf-8"),
        'timestamp': unpacked_data[4],
        'timestamp_str': datetime.datetime.fromtimestamp(unpacked_data[4]/1000).strftime('%Y-%m-%d %H:%M:%S'),
        'seq_id_seq_id': unpacked_data[5],
        'seq_id_client_id': unpacked_data[6],
        'server_id': unpacked_data[7],
        'session_id': unpacked_data[8],
        'length': unpacked_data[9]
    }

    return header

def decode_extended_data(header, data, data_len):
    """Decode extended data.

    This is a raw postgres packet data, which contains the code and length.
    Multiple packets can be concatenated together.
    Returns data (with header), and length of the input data consumed.
    """
    logging.debug("Decoding extended data: length: {}".format(header['length']))

    offset = 0

    if header['session_id'] in PARTIAL_DATA_MAP:
        # we have partial data from previous read, fetch it
        (buffer, buffer_len) = PARTIAL_DATA_MAP[header['session_id']]
        del PARTIAL_DATA_MAP[header['session_id']]

        if buffer_len < 5:
            # we need to add data from data to buffer
            buffer += data[0:5 - buffer_len]
            offset = 5 - buffer_len

    else:
        # no partial data, use the first 5 bytes for header
        buffer = data[0:5]
        buffer_len = 5
        offset = 5

    # we have enough data to decode the code and length
    (code,len) = struct.unpack('>cL', buffer[0:5])

    code = code.decode("utf-8")
    len -= 4

    if data_len - offset >= len:
        # we have enough data to decode the packet
        buffer += data[offset:offset + len]

        # strip out header and return the data, and length consumed
        return (code, buffer, len + offset)

    # we need more data, save buffer for later
    buffer += data[offset:]
    PARTIAL_DATA_MAP[header['session_id']] = (buffer, data_len - offset)

    return (None, None, None)


def decode_data(data_with_header, header, db_conn=None):
    """Decode data.

    data contains code, length, and the actual data.
    """
    # decode the code and length
    (code,pkt_len) = struct.unpack('>cL', data_with_header[0:5])
    data = data_with_header[5:] # strip out code and length
    code = code.decode("utf-8")

    print("{}, ClientID: {}, SessionID: {}, SeqID: {}, Code: {}, Length: {}"
          .format(header['type_str'], header['seq_id_client_id'],
                  header['session_id'], header['seq_id_seq_id'], code, pkt_len))

    # get request or response type
    req_resp_type = TYPE_REQ_RESP_MAP[header['type_flag']]

    if code == '*':
        # session connect
        endpoint, _ = pglib.decode_string(data)
        print("Session connect: endpoint={}, session_id={}".format(endpoint, header['session_id']))
        db_insert_session(db_conn, header, endpoint)
        return

    if code == '!':
        #session disconnect
        print("Session disconnect: session_id={}".format(header['session_id']))
        db_update_session_disconnect(db_conn, header)
        return

    msg = None
    data_row = None

    if code == 'D' and req_resp_type == 'Response':
        # data row
        columns = pglib.decode_data_row(data_with_header)
        data_row = pglib.md5_hash(columns)
        msg = "Data Row: columns={}, hash={}".format(len(columns), data_row)
        # for (i, column) in enumerate(columns):
        #    msg += "\nColumn {}: {}".format(i+1, column)
    elif code == 'D' and req_resp_type == 'Request':
        # describe
        type, name = pglib.decode_describe(data_with_header)
        msg = "Describe: type={}, name={}".format(type, name)
    elif code == 'Q':
        # simple query
        query, _ = pglib.decode_string(data)
        msg = "Simple Query: {}".format(query)
    elif code == 'C' and req_resp_type == 'Response':
        # command complete
        command, _ = pglib.decode_string(data)
        msg = "Command Complete: {}".format(command)
    elif code == 'C' and req_resp_type == 'Request':
        # close command
        type, name = pglib.decode_close_command(data_with_header)
        msg = "Close command: type={}, name={}".format(type, name)
    elif code == 'E' and req_resp_type == 'Response':
        # error response
        params = pglib.decode_error(data_with_header)
        msg = "Error: "
        for key in params:
            msg += "\nParam: {}={}".format(key, params[key])
    elif code == 'R':
        auth_type = pglib.decode_auth_response(data_with_header)
        msg = "Authentication response: auth_type={}".format(pglib.auth_type_to_string(auth_type))
    elif code == 'S':
        # parameter status
        name, value = pglib.decode_parameter_status(data_with_header)
        msg = "Parameter Status: name={}, value={}".format(name, value)
    elif code == 'Z':
        # ready for query
        status = pglib.decode_ready_for_query(data_with_header)
        msg = "Ready for query: status={}".format(status)
    elif code == 'K':
        # backend key data
        pid, secret = pglib.decode_backend_key_data(data_with_header)
        msg = "Backend Key: pid={}, secret={}".format(pid, secret)
    elif code == 't':
        # parameter description
        msg = "Parameter description"
    elif code == 'P':
        # parse request
        msg = "Parse: stmt={}".format(pglib.decode_parse_request(data_with_header))
    elif code == 'B':
        # bind request
        portal, stmt = pglib.decode_bind_request(data_with_header)
        msg = "Bind: portal={}, stmt={}".format(portal, stmt)
    elif code == 'E' and req_resp_type == 'Request':
        # execute request
        portal = pglib.decode_execute_request(data_with_header)
        msg = "Execute request: portal={}".format(portal)
    elif code == '1':
        # parse complete
        msg = "Parse complete"
    elif code == '2':
        # bind complete
        msg = "Bind complete"
    elif code == '3':
        # close complete
        msg = "Close complete"
    elif code == 'n':
        # no data
        msg = "No data"
    elif code == 'I':
        # empty query response
        msg = "Empty query response"
    elif code == 'p':
        # password message/response
        if req_resp_type == 'Request':
            msg = "Password message"
        else:
            msg = "Password response"
    elif code == '?':
        # startup message
        proto_major, proto_minor, params = pglib.decode_startup_message(data_with_header)
        msg = "Startup message: proto_major={}, proto_minor={}".format(proto_major, proto_minor)
        for param in params:
            msg += "\nParam: {}={}".format(param[0], param[1])
    elif code == 'T':
        # row description
        fields = pglib.decode_row_description(data_with_header)
        msg = "Row description: fields={}".format(len(fields))
        for i, field in enumerate(fields):
            msg += "\nField {}: name={}, type_oid={}".format(i+1, field['field_name'], field['data_type_oid'])
    else:
        msg = "Unhandled code: {}".format(code)

    print(msg)
    print()

    # insert message into database; if db_conn == None, it will do nothing
    db_insert_msg(db_conn, header, code, msg, data_row)

def handle_data(header, data, db_conn=None):
    """Handle the data based on the header."""
    if header['code'] == '\x00':
        # extended data, code and length are encoded in the data packet;
        # multiple packets can be concatenated together
        length = header['length']
        offset = 0
        while length > 0:
            (code, buffer, len_consumed) = decode_extended_data(header, data[offset:], length)
            if not buffer:
                break

            decode_data(buffer, header, db_conn)
            length -= len_consumed
            offset += len_consumed

    else:
        # non-extended data, code and length are in the header
        if header['final_flag'] == 0:
            raise Exception("Final flag must be set for non-extended data")

        # add header to data; length includes length field but not code field
        data = struct.pack('>cL', header['code'].encode("utf-8"), header['length']+4) + data
        decode_data(data, header, db_conn)


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Decode log file.")
    parser.add_argument('-f', '--filename', type=str, help="The name of the file containing the SQL commands to execute")
    parser.add_argument('-D', '--debug', action='store_true', help="The name of the file containing the SQL commands to execute")
    parser.add_argument('--db', action='store_true', help="Import messages to the database")

    parser.add_argument('-H', '--hostname', type=str, default='127.0.0.1', help="The hostname of the database server")
    parser.add_argument('-p', '--port', type=int, default=5432, help="The port number on which the database server is listening")
    parser.add_argument('-U', '--username', type=str, default='postgres', help="The username for connecting to the database")
    parser.add_argument('-P', '--password', type=str, default='postgres', help="The password for connecting to the database")
    parser.add_argument('-s', '--requires_ssl', action='store_true', help="Flag to indicate if SSL is required")
    parser.add_argument('-d', '--database', type=str, default='proxy_logs', help="The name of the database to connect to")

    args = parser.parse_args()
    return args


def main():
    """Main function."""
    # Parse command line arguments
    args = parse_arguments()

    # Set up logging
    if args.debug:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    logging.basicConfig(stream=sys.stderr, level=log_level)
    logger = logging.getLogger(__name__)


    conn = None
    if args.db:
        # Connect to the PostgreSQL server
        conn = connect_to_postgres(args.hostname, args.port, args.username, args.password, args.database, args.requires_ssl)
        logging.info("Connected to PostgreSQL server")
        conn.initialize(logger)

        # For testing: truncate the tables in the database
        db_truncate_tables(conn)

    # open log file in binary mode
    with open(args.filename, 'rb') as file:
        while (True):
            # decode log file header
            header = decode_header(file)
            if not header:
                break

            logging.debug(header)

            # read in file data based on the length
            data = file.read(header['length'])

            handle_data(header, data, conn)


if __name__ == '__main__':
    main()

