import sys
import os
from typing import Tuple

# Get the project root directory
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the thrift generated code directory to the Python path
# Assuming thrift files are generated to python/gen-py
sys.path.append(os.path.join(project_root, 'thrift'))
sys.path.append(os.path.join(project_root, 'shared'))

print(f"project_root: {os.path.join(project_root, 'thrift')}")

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

# Import the generated code
from xid_mgr.ThriftXidMgr import Client as XidMgrClient
from xid_mgr.ttypes import Status as XidMgrStatus, StatusCode as XidMgrStatusCode

from sys_tbl_mgr.Service import Client as SysTblMgrClient
from sys_tbl_mgr.ttypes import Status as SysTblMgrStatus, StatusCode as SysTblMgrStatusCode

from properties import Properties

def ping_sys_tbl_mgr(host: str = 'localhost', port: int = 9090, timeout_ms: int = 1000) -> Tuple[bool, str]:
    """
    Connect to the System Table Manager service and send a ping request.

    Args:
        host: The hostname where System Table Manager is running
        port: The port number System Table Manager is listening on
        timeout_ms: Socket timeout in milliseconds (default 1000ms/1s)
    """
    try:
        # Create socket connection with timeout
        transport = TSocket.TSocket(host, port)
        transport.setTimeout(timeout_ms)

        # Buffering is critical for performance
        transport = TTransport.TFramedTransport(transport)

        # Use the binary protocol
        protocol = TBinaryProtocol.TBinaryProtocol(transport)

        # Create a client
        client = SysTblMgrClient(protocol)

        # Connect
        transport.open()

        # Send ping request
        status: SysTblMgrStatus = client.ping()

        # Close connection
        transport.close()

        if status.status == SysTblMgrStatusCode.SUCCESS:
            return True, "Ping successful"
        else:
            return False, status.message or "Ping failed with unknown error"

    except Thrift.TException as tx:
        return False, f"Thrift error: {str(tx)}"
    except Exception as e:
        return False, f"Error: {str(e)}"


def ping_xid_mgr(host: str = 'localhost', port: int = 9090, timeout_ms: int = 1000) -> Tuple[bool, str]:
    """
    Connect to the XID manager service and send a ping request.

    Args:
        host: The hostname where XID manager is running
        port: The port number XID manager is listening on
        timeout_ms: Socket timeout in milliseconds (default 1000ms/1s)

    Returns:
        Tuple of (success: bool, message: str)
        success is True if ping was successful, False otherwise
        message contains error details if success is False
    """
    try:
        # Create socket connection with timeout
        transport = TSocket.TSocket(host, port)
        transport.setTimeout(timeout_ms)

        # Buffering is critical for performance
        transport = TTransport.TFramedTransport(transport)

        # Use the binary protocol
        protocol = TBinaryProtocol.TBinaryProtocol(transport)

        # Create a client
        client = XidMgrClient(protocol)

        # Connect
        transport.open()

        # Send ping request
        status: XidMgrStatus = client.ping()

        # Close connection
        transport.close()

        if status.status == XidMgrStatusCode.SUCCESS:
            return True, "Ping successful"
        else:
            return False, status.message or "Ping failed with unknown error"

    except Thrift.TException as tx:
        return False, f"Thrift error: {str(tx)}"
    except Exception as e:
        return False, f"Error: {str(e)}"


if __name__ == "__main__":
    # Example usage
    props = Properties()
    sys_config = props.get_system_config()
    host = props.get_hostname('ingestion')
    success, message = ping_xid_mgr(host, sys_config['xid_mgr']['rpc_config']['server_port'], timeout_ms=3000)
    print(f"Success: {success}")
    print(f"Message: {message}")