import sys
import os
import grpc
import time
import argparse
import logging
from typing import Optional

# Add the directory containing the generated gRPC files to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'xid_manager')))

import xid_manager_pb2, xid_manager_pb2_grpc
from google.protobuf import empty_pb2

class XidMgrClient:
    def __init__(self,
                 host: str,
                 rpc_config: dict) -> None:
        """Initialize XidManager client with server connection details.

        Args:
            host: The hostname of the XidManager server
            rpc_config: Configuration dictionary with server connection details
        """
        self.logger = logging.getLogger('springtail')

        port: int = rpc_config['server_port']

        if 'ssl' in rpc_config and rpc_config['ssl']:
            ca_cert_path: str = rpc_config['client_trusted']
            ca_client_key_path: str = rpc_config['client_key']
            ca_client_cert_path: str = rpc_config['client_cert']

            with open(ca_cert_path, 'rb') as ca_cert_file, \
                 open(ca_client_key_path, 'rb') as ca_client_key_file, \
                 open(ca_client_cert_path, 'rb') as ca_client_cert_file:
                ca_cert = ca_cert_file.read()
                client_cert = ca_client_cert_file.read()
                client_key = ca_client_key_file.read()

            # Create gRPC channel with SSL credentials
            creds = grpc.ssl_channel_credentials(
                root_certificates=ca_cert,   # Verify server certificate
                private_key=client_key,      # Client private key
                certificate_chain=client_cert  # Client certificate
            )

            # Override the target name to match the server certificate's CN
            channel_options = (("grpc.ssl_target_name_override", "springtail_server"),)

            self.logger.debug(f"Connecting to {host}:{port} with SSL")
            self.channel = grpc.secure_channel(f"{host}:{port}", creds, options=channel_options)
        else:
            self.logger.debug(f"Connecting to {host}:{port} without SSL")
            self.channel = grpc.insecure_channel(f"{host}:{port}")

        self.stub = xid_manager_pb2_grpc.XidManagerStub(self.channel)

    def ping(self, count: int = 1, sleep_time: int = 1):
        """Send a ping request with retries

        Args:
            count: Number of retry attempts
            sleep_time: Sleep time (in seconds) between attempts

        Returns:
            The response from the Ping method

        Raises:
            Exception: The last exception encountered if all attempts fail
        """
        last_exc = None
        for attempt in range(count):
            try:
                return self.stub.Ping(empty_pb2.Empty())
            except Exception as exc:
                last_exc = exc
                time.sleep(sleep_time)
        raise last_exc

    def close(self):
        """Close the gRPC channel."""
        self.channel.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Xid Manager Client")
    parser.add_argument('--host', type=str, default='localhost', help='Host of the service')
    parser.add_argument('--port', type=int, default=55052, help='Port of the service')
    parser.add_argument('--cert-path', type=str, required=False, help='Path to client certificates/key')

    args = parser.parse_args()
    return args

if __name__ == "__main__":
    args = parse_arguments()

    rpc_config = {
        'server_port': args.port,
    }

    if args.cert_path:
        rpc_config['ssl'] = True
        rpc_config['client_trusted'] = os.path.join(args.cert_path, 'ca_cert.pem')
        rpc_config['client_key'] = os.path.join(args.cert_path, 'client_key.pem')
        rpc_config['client_cert'] = os.path.join(args.cert_path, 'client_cert.pem')

    with XidMgrClient(args.host, rpc_config) as client:
        client.ping()
        print("Ping successful")
        xid = client.get_committed_xid(1)
        print(f'Latest committed XID: {xid}')
