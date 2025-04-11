import sys
import os
import grpc
import time
from typing import Optional

# Add the directory containing the generated gRPC files to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'sys_tbl_mgr')))

import sys_tbl_mgr_pb2, sys_tbl_mgr_pb2_grpc
from google.protobuf import empty_pb2

class SysTblMgrClient:
    """Client for the SysTblMgr gRPC service"""

    def __init__(self,
        host : str,
        rpc_config : dict) -> None:
        """Initialize SysTableMgr client with server connection details.

        Args:
            host: The hostname of the SysTblManager server
            rpc_config: Configuration dictionary with server connection details
        """
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

            self.channel = grpc.secure_channel(f"{host}:{port}", creds, options=channel_options)
        else:
            self.channel = grpc.insecure_channel(f"{host}:{port}")

        self.stub = sys_tbl_mgr_pb2_grpc.SysTblMgrStub(self.channel)

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

    def create_index(self, db_id: int, xid: int, lsn: int,
                    index_id: int, namespace_name: str, index_name: str,
                    is_unique: bool, table_id: int, table_name: str,
                    state: int, columns: list):
        """Create a new index by constructing the request message from individual parameters

        Args:
            db_id: Database ID
            xid: Transaction ID
            lsn: Log sequence number
            index_id: Index ID
            namespace_name: Namespace of the index
            index_name: Name of the index
            is_unique: Whether the index is unique
            table_id: Table ID to which the index belongs
            table_name: Table name
            state: State of the index
            columns: List of dictionaries representing index columns.
                    Each dict should have keys: 'name', 'position', 'idx_position'
        Returns:
            DDLStatement message
        """
        index_columns = [sys_tbl_mgr_pb2.IndexColumn(name=col['name'],
                                                    position=col['position'],
                                                    idx_position=col['idx_position'])
                        for col in columns]
        index = sys_tbl_mgr_pb2.IndexInfo(id=index_id,
                                        namespace_name=namespace_name,
                                        name=index_name,
                                        is_unique=is_unique,
                                        table_id=table_id,
                                        table_name=table_name,
                                        state=state,
                                        columns=index_columns)
        request = sys_tbl_mgr_pb2.IndexRequest(db_id=db_id, xid=xid, lsn=lsn, index=index)
        return self.stub.CreateIndex(request)

    def drop_index(self, db_id: int, xid: int, lsn: int,
                index_id: int, namespace_name: str, index_name: str):
        """Drop an existing index by constructing the request message

        Args:
            db_id: Database ID
            xid: Transaction ID
            lsn: Log sequence number
            index_id: Index ID
            namespace_name: Namespace of the index
            index_name: Name of the index
        Returns:
            DDLStatement message
        """
        request = sys_tbl_mgr_pb2.DropIndexRequest(db_id=db_id, xid=xid, lsn=lsn,
                                                    index_id=index_id,
                                                    namespace_name=namespace_name,
                                                    name=index_name)
        return self.stub.DropIndex(request)

    def set_index_state(self, db_id: int, xid: int, lsn: int,
                        table_id: int, index_id: int, state: int):
        """Set index state by constructing the request message

        Args:
            db_id: Database ID
            xid: Transaction ID
            lsn: Log sequence number
            table_id: Table ID
            index_id: Index ID
            state: New state for the index
        Returns:
            Empty message
        """
        request = sys_tbl_mgr_pb2.SetIndexStateRequest(db_id=db_id, xid=xid, lsn=lsn,
                                                    table_id=table_id,
                                                    index_id=index_id,
                                                    state=state)
        return self.stub.SetIndexState(request)

    def get_index_info(self, db_id: int, xid: int, lsn: int,
                    index_id: int, table_id: int = None):
        """Get index info by constructing the request message

        Args:
            db_id: Database ID
            xid: Transaction ID
            lsn: Log sequence number
            index_id: Index ID
            table_id: (Optional) Table ID if required by server logic
        Returns:
            IndexInfo message
        """
        # table_id is an optional field. It will be set only if provided.
        kwargs = {'db_id': db_id, 'xid': xid, 'lsn': lsn, 'index_id': index_id}
        if table_id is not None:
            kwargs['table_id'] = table_id
        request = sys_tbl_mgr_pb2.GetIndexInfoRequest(**kwargs)
        return self.stub.GetIndexInfo(request)

    def create_table(self, db_id: int, xid: int, lsn: int,
                    table_id: int, namespace_name: str, table_name: str,
                    columns: list):
        """Create a new table by constructing the request message

        Args:
            db_id: Database ID
            xid: Transaction ID
            lsn: Log sequence number
            table_id: Table ID
            namespace_name: Namespace name
            table_name: Table name
            columns: List of dictionaries for table columns. Each dict should have keys:
                    'name', 'type', 'pg_type', 'position', 'is_nullable', 'is_generated',
                    and optionally 'pk_position' and 'default_value'
        Returns:
            DDLStatement message
        """
        table_columns = []
        for col in columns:
            kwargs = {
                'name': col['name'],
                'type': col['type'],
                'pg_type': col['pg_type'],
                'position': col['position'],
                'is_nullable': col['is_nullable'],
                'is_generated': col['is_generated']
            }
            if 'pk_position' in col:
                kwargs['pk_position'] = col['pk_position']
            if 'default_value' in col:
                kwargs['default_value'] = col['default_value']
            table_columns.append(sys_tbl_mgr_pb2.TableColumn(**kwargs))
        table_info = sys_tbl_mgr_pb2.TableInfo(id=table_id,
                                            namespace_name=namespace_name,
                                            name=table_name,
                                            columns=table_columns)
        request = sys_tbl_mgr_pb2.TableRequest(db_id=db_id, xid=xid, lsn=lsn, table=table_info)
        return self.stub.CreateTable(request)

    def alter_table(self, db_id: int, xid: int, lsn: int,
                    table_id: int, namespace_name: str, table_name: str,
                    columns: list):
        """Alter existing table by constructing the request message

        Args:
            db_id: Database ID
            xid: Transaction ID
            lsn: Log sequence number
            table_id: Table ID
            namespace_name: Namespace name
            table_name: Table name
            columns: List of dictionaries for table columns (see create_table)
        Returns:
            DDLStatement message
        """
        table_columns = []
        for col in columns:
            kwargs = {
                'name': col['name'],
                'type': col['type'],
                'pg_type': col['pg_type'],
                'position': col['position'],
                'is_nullable': col['is_nullable'],
                'is_generated': col['is_generated']
            }
            if 'pk_position' in col:
                kwargs['pk_position'] = col['pk_position']
            if 'default_value' in col:
                kwargs['default_value'] = col['default_value']
            table_columns.append(sys_tbl_mgr_pb2.TableColumn(**kwargs))
        table_info = sys_tbl_mgr_pb2.TableInfo(id=table_id,
                                            namespace_name=namespace_name,
                                            name=table_name,
                                            columns=table_columns)
        request = sys_tbl_mgr_pb2.TableRequest(db_id=db_id, xid=xid, lsn=lsn, table=table_info)
        return self.stub.AlterTable(request)

    def drop_table(self, db_id: int, xid: int, lsn: int,
                table_id: int, namespace_name: str, table_name: str):
        """Drop existing table by constructing the request message

        Args:
            db_id: Database ID
            xid: Transaction ID
            lsn: Log sequence number
            table_id: Table ID
            namespace_name: Namespace name
            table_name: Table name
        Returns:
            DDLStatement message
        """
        request = sys_tbl_mgr_pb2.DropTableRequest(db_id=db_id, xid=xid, lsn=lsn,
                                                    table_id=table_id,
                                                    namespace_name=namespace_name,
                                                    name=table_name)
        return self.stub.DropTable(request)

    def create_namespace(self, db_id: int, namespace_id: int, name: str,
                        xid: int, lsn: int):
        """Create namespace by constructing the request message

        Args:
            db_id: Database ID
            namespace_id: Namespace ID
            name: Namespace name
            xid: Transaction ID
            lsn: Log sequence number
        Returns:
            DDLStatement message
        """
        request = sys_tbl_mgr_pb2.NamespaceRequest(db_id=db_id, namespace_id=namespace_id,
                                                    name=name, xid=xid, lsn=lsn)
        return self.stub.CreateNamespace(request)

    def alter_namespace(self, db_id: int, namespace_id: int, name: str,
                        xid: int, lsn: int):
        """Alter namespace by constructing the request message

        Args:
            db_id: Database ID
            namespace_id: Namespace ID
            name: Namespace name
            xid: Transaction ID
            lsn: Log sequence number
        Returns:
            DDLStatement message
        """
        request = sys_tbl_mgr_pb2.NamespaceRequest(db_id=db_id, namespace_id=namespace_id,
                                                    name=name, xid=xid, lsn=lsn)
        return self.stub.AlterNamespace(request)

    def drop_namespace(self, db_id: int, namespace_id: int, name: str,
                    xid: int, lsn: int):
        """Drop namespace by constructing the request message

        Args:
            db_id: Database ID
            namespace_id: Namespace ID
            name: Namespace name
            xid: Transaction ID
            lsn: Log sequence number
        Returns:
            DDLStatement message
        """
        request = sys_tbl_mgr_pb2.NamespaceRequest(db_id=db_id, namespace_id=namespace_id,
                                                    name=name, xid=xid, lsn=lsn)
        return self.stub.DropNamespace(request)

    def update_roots(self, db_id: int, xid: int, table_id: int,
                    roots: list, stats: dict, snapshot_xid: int):
        """Update root pointers and stats by constructing the request message

        Args:
            db_id: Database ID
            xid: Transaction ID
            table_id: Table ID
            roots: List of dictionaries representing roots. Each dict should have keys: 'index_id' and 'extent_id'
            stats: Dictionary with key 'row_count' and 'end_offset'
            snapshot_xid: Snapshot transaction ID
        Returns:
            Empty message
        """
        root_objs = [sys_tbl_mgr_pb2.RootInfo(index_id=root['index_id'],
                                            extent_id=root['extent_id'])
                    for root in roots]
        stats_obj = sys_tbl_mgr_pb2.TableStats(row_count=stats['row_count'],
                                               end_offset=stats['end_offset'])
        request = sys_tbl_mgr_pb2.UpdateRootsRequest(db_id=db_id, xid=xid,
                                                    table_id=table_id,
                                                    roots=root_objs,
                                                    stats=stats_obj,
                                                    snapshot_xid=snapshot_xid)
        return self.stub.UpdateRoots(request)

    def finalize(self, db_id: int, xid: int):
        """Finalize system tables by constructing the request message

        Args:
            db_id: Database ID
            xid: Transaction ID
        Returns:
            Empty message
        """
        request = sys_tbl_mgr_pb2.FinalizeRequest(db_id=db_id, xid=xid)
        return self.stub.Finalize(request)

    def get_roots(self, db_id: int, xid: int, table_id: int):
        """Get root pointers and stats by constructing the request message

        Args:
            db_id: Database ID
            xid: Transaction ID
            table_id: Table ID
        Returns:
            GetRootsResponse message
        """
        request = sys_tbl_mgr_pb2.GetRootsRequest(db_id=db_id, xid=xid, table_id=table_id)
        return self.stub.GetRoots(request)

    def get_schema(self, db_id: int, table_id: int, xid: int, lsn: int):
        """Get schema information by constructing the request message

        Args:
            db_id: Database ID
            table_id: Table ID
            xid: Transaction ID
            lsn: Log sequence number
        Returns:
            GetSchemaResponse message
        """
        request = sys_tbl_mgr_pb2.GetSchemaRequest(db_id=db_id, table_id=table_id, xid=xid, lsn=lsn)
        return self.stub.GetSchema(request)

    def get_target_schema(self, db_id: int, table_id: int,
                        access_xid: int, access_lsn: int,
                        target_xid: int, target_lsn: int):
        """Get schema information with target by constructing the request message

        Args:
            db_id: Database ID
            table_id: Table ID
            access_xid: Access transaction ID
            access_lsn: Access log sequence number
            target_xid: Target transaction ID
            target_lsn: Target log sequence number
        Returns:
            GetSchemaResponse message
        """
        request = sys_tbl_mgr_pb2.GetTargetSchemaRequest(db_id=db_id, table_id=table_id,
                                                        access_xid=access_xid, access_lsn=access_lsn,
                                                        target_xid=target_xid, target_lsn=target_lsn)
        return self.stub.GetTargetSchema(request)

    def exists(self, db_id: int, table_id: int, xid: int, lsn: int):
        """Check if table exists by constructing the request message

        Args:
            db_id: Database ID
            table_id: Table ID
            xid: Transaction ID
            lsn: Log sequence number
        Returns:
            Empty message
        """
        request = sys_tbl_mgr_pb2.ExistsRequest(db_id=db_id, table_id=table_id, xid=xid, lsn=lsn)
        return self.stub.Exists(request)

    def swap_sync_table(self, namespace_params: dict, create_table_params: dict,
                        index_params: list, roots_params: dict):
        """Swap sync table operation by constructing the request message

        Args:
            namespace_params: Dictionary with keys for NamespaceRequest (e.g., db_id, namespace_id, name, xid, lsn)
            create_table_params: Dictionary with keys for TableRequest (e.g., db_id, xid, lsn, table_id, namespace_name, table_name, columns)
            index_params: List of dictionaries, each containing keys for an IndexRequest. Each dictionary should include:
                        db_id, xid, lsn, index_id, namespace_name, index_name, is_unique,
                        table_id, table_name, state, and columns (list of dict for IndexColumn)
            roots_params: Dictionary with keys for UpdateRootsRequest (e.g., db_id, xid, table_id, roots, stats, snapshot_xid)
        Returns:
            DDLStatement message
        """
        ns_req = sys_tbl_mgr_pb2.NamespaceRequest(**namespace_params)

        # Construct TableRequest for create table
        # Assume create_table_params contains a 'columns' key as list of dicts.
        table_columns = []
        for col in create_table_params.pop('columns'):
            kwargs = {
                'name': col['name'],
                'type': col['type'],
                'pg_type': col['pg_type'],
                'position': col['position'],
                'is_nullable': col['is_nullable'],
                'is_generated': col['is_generated']
            }
            if 'pk_position' in col:
                kwargs['pk_position'] = col['pk_position']
            if 'default_value' in col:
                kwargs['default_value'] = col['default_value']
            table_columns.append(sys_tbl_mgr_pb2.TableColumn(**kwargs))
        table_info = sys_tbl_mgr_pb2.TableInfo(id=create_table_params.pop('table_id'),
                                            namespace_name=create_table_params.pop('namespace_name'),
                                            name=create_table_params.pop('table_name'),
                                            columns=table_columns)
        create_req = sys_tbl_mgr_pb2.TableRequest(**create_table_params, table=table_info)

        index_reqs = []
        for ip in index_params:
            index_columns = [sys_tbl_mgr_pb2.IndexColumn(name=col['name'],
                                                        position=col['position'],
                                                        idx_position=col['idx_position'])
                            for col in ip.pop('columns')]
            index = sys_tbl_mgr_pb2.IndexInfo(id=ip.pop('index_id'),
                                            namespace_name=ip.pop('namespace_name'),
                                            name=ip.pop('index_name'),
                                            is_unique=ip.pop('is_unique'),
                                            table_id=ip.pop('table_id'),
                                            table_name=ip.pop('table_name'),
                                            state=ip.pop('state'),
                                            columns=index_columns)
            index_req = sys_tbl_mgr_pb2.IndexRequest(index=index, **ip)
            index_reqs.append(index_req)

        # Construct roots request
        root_objs = [sys_tbl_mgr_pb2.RootInfo(index_id=r['index_id'], extent_id=r['extent_id'])
                    for r in roots_params.pop('roots')]
        stats_obj = sys_tbl_mgr_pb2.TableStats(**roots_params.pop('stats'))
        roots_req = sys_tbl_mgr_pb2.UpdateRootsRequest(**roots_params, roots=root_objs, stats=stats_obj)

        request = sys_tbl_mgr_pb2.SwapSyncTableRequest(namespace_req=ns_req,
                                                    create_req=create_req,
                                                    index_reqs=index_reqs,
                                                    roots_req=roots_req)
        return self.stub.SwapSyncTable(request)

    def close(self):
        """Close the channel"""
        self.channel.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

if __name__ == "__main__":
    # Example usage
    with SysTblMgrClient('localhost', 5053) as client:
        client.ping()
        print("Ping successful")
