import argparse
import struct
import datetime

import pglib

TYPE_MAP = {
    1: 'From Client',
    2: 'From Replica',
    3: 'From Primary',
    4: 'To Client',
    5: 'To Replica',
    6: 'To Primary'
}

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
    format_str = '<BBcBQLLLL' # little-endian format

    # Calculate the size of the header based on the format string
    header_size = struct.calcsize(format_str)

    # Read the header data from the file
    print("Reading hdr: {} bytes".format(header_size))
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
        'session_id': unpacked_data[7],
        'length': unpacked_data[8]
    }

    return header

def decode_extended_data(file, header):
    """Decode extended data.

    This is a raw postgres packet data, which contains the code and length.
    Multiple packets can be concatenated together.
    """
    # if header code is 0, read the next 5 bytes
    # which contains the code and length (from postgres packet data)
    format_str = '>cL' # big-endian format
    data = file.read(5)
    if not data:
        return None

    # dump_hex(data)
    unpacked_data = struct.unpack(format_str, data)
    code = unpacked_data[0].decode("utf-8")
    length = unpacked_data[1] - 4

    print ("  code: {}, length: {}".format(code, length))

    data = file.read(length)

    decode_data(data, code, length, header)

    return (data, length + 5);

def decode_data(data, code, length, header):
    """Decode data based on the code.

    data does not contain code or length, only the actual data.
    length is length of data in bytes.
    """
    data_format = '>cL'
    packed_data = struct.pack(data_format, code.encode("utf-8"), (length + 4))
    data_with_header = packed_data + data;

    if code == 'D':
        # data row
        columns = pglib.decode_data_row(data_with_header)
    elif code == 'Q':
        # simple query
        query, _ = pglib.decode_string(data)
        print ("Simple Query: {}".format(query))
    elif code == 'C':
        # command complete
        command, _ = pglib.decode_string(data)
        print ("Command Complete: {}".format(command))
    elif code == 'E':
        # error response
        error = pglib.decode_error_response(data_with_header)
        print ("Error: {}".format(error))

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Decode log file.")
    parser.add_argument('-f', '--filename', type=str, help="The name of the file containing the SQL commands to execute")

    args = parser.parse_args()
    return args

def main():
    """Main function."""
    # Parse command line arguments
    args = parse_arguments()

    # open log file in binary mode
    with open(args.filename, 'rb') as file:
        while (True):
            # decode log file header
            header = decode_header(file)
            if not header:
                break

            print (header)

            # if header code is 0, read the next 5 bytes
            # which contains the code and length (from postgres packet data)
            if header['code'] == '\x00':
                print("Reading extended format: {} bytes".format(header['length']))
                to_read = header['length']
                while to_read > 0:
                    data, len_read = decode_extended_data(file, header)
                    to_read -= len_read
            else:
                # data follows header, decode it
                print ("Reading data: {} bytes".format(header['length']))
                data = file.read(header['length'])
                decode_data(data, header['code'], header['length'], header)


if __name__ == '__main__':
    main()

