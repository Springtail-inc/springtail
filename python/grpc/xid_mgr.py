import sys
import os
import grpc
import time
from typing import Optional

# Add the directory containing the generated gRPC files to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'xid_manager')))

import xid_manager_pb2, xid_manager_pb2_grpc
from google.protobuf import empty_pb2

class XidMgrClient:
    def __init__(self,
                 host: str,
                 port: int,
                 ca_cert_path: Optional[str] = "",
                 ca_client_key_path: Optional[str] = "",
                 ca_client_cert_path: Optional[str] = ""):
        """Initialize XidManager client with server connection details.

        Args:
            host: The hostname of the XidManager server
            port: The port number of the XidManager server
            ca_cert_path: The path to the CA certificate file
            ca_client_key_path: The path to the client key file
            ca_client_cert_path: The path to the client certificate file
        """

        if ca_cert_path and ca_client_key_path and ca_client_cert_path:
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

            self.channel = grpc.secure_channel(f"{host}:{port}", creds, options=channel_options)
        else:
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

    def commit_xid(self, db_id: int, xid: int, has_schema_changes: bool = False) -> None:
        """Commit up to and including the provided XID.

        Args:
            db_id: Database identifier
            xid: Transaction ID to commit
            has_schema_changes: Whether the transaction includes schema changes
        """
        request = xid_manager_pb2.CommitXidRequest(
            db_id=db_id,
            xid=xid,
            has_schema_changes=has_schema_changes
        )
        return self.stub.CommitXid(request)

    def record_ddl_change(self, db_id: int, xid: int) -> None:
        """Record that the given XID contains a schema change.

        Args:
            db_id: Database identifier
            xid: Transaction ID containing DDL changes
        """
        request = xid_manager_pb2.RecordDdlChangeRequest(
            db_id=db_id,
            xid=xid
        )
        return self.stub.RecordDdlChange(request)

    def get_committed_xid(self, db_id: int, schema_xid: int = 0) -> int:
        """Get latest committed XID.

        Args:
            db_id: Database identifier
            schema_xid: Schema version XID

        Returns:
            The latest committed XID
        """
        request = xid_manager_pb2.GetCommittedXidRequest(
            db_id=db_id,
            schema_xid=schema_xid
        )
        response = self.stub.GetCommittedXid(request)
        return response.xid

    def close(self):
        """Close the gRPC channel."""
        self.channel.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

if __name__ == "__main__":
    # Example usage
    client_cert = "/home/dev/springtail/ca_certs/client_cert.pem"
    client_key = "/home/dev/springtail/ca_certs/client_key.pem"
    root_cert = "/home/dev/springtail/ca_certs/ca_cert.pem"

    with XidMgrClient('localhost', 5052, root_cert, client_key, client_cert) as client:
        client.ping()
        print("Ping successful")
        xid = client.get_committed_xid(1)
        print(f'Latest committed XID: {xid}')