import concurrent.futures
import io
import logging
from lxml import etree
import os
import shlex
import psycopg2
import springtail
import time
import common
import threading

_GLOBAL_CONFIG_FILE = '__config.sql'

class TestCase:
    """Class to manage a single test-case.  Handles all phases of the
    test case and stores the result of the test.

    """
    def __init__(self,
                 filename: str,
                 build_dir: str,
                 test_params: dict = {},
                 valid_sections: list = ['test', 'verify', 'cleanup']) -> None:
        """Initialize the test case"""
        self._filename = os.path.abspath(filename)
        self._name = os.path.basename(self._filename)
        self._directory = os.path.dirname(self._filename)
        self._build_dir = build_dir
        self._test_params = test_params
        self._status = 'INIT'
        self._result = 'UNKNOWN'
        self._duration = 0
        self._error = ''
        self._log_errors = []
        self._logged_output = ''

        self._metadata = {
            'autocommit': True,
            'sync_timeout': 200,
            'default_txn': 'default',
            'query_timeout': 5,
            'live_startup': None,
            'poll_interval': 0.001
        }

        self._added_databases = []

        # Each section is composed of an array of sub-sections which
        # are either "sequential" or "parallel".  Sequential
        # sub-sections contain an array of commands, with each command
        # specifying which transaction they should be run against.
        # Parallel sub-sections contain an array of commands
        # per-transaction, with transactions being run in parallel.
        self._sections = { }
        for section in valid_sections:
            self._sections[section] = []

        self._txns = set({}) # the set of transaction names referenced in this test
        self._connections = {} # connections to the primary for each transaction
        self._fdw = {} # connection to Springtail
        self._sync_step = 0 # incrementing ID used for replica synchronization
        self._recovery_points = { } # map from recovery point name to XID


    def set_props(self, props: springtail.Properties) -> None:
        self._props = props
        fdw_config = props.get_fdw_config()
        db_configs = props.get_db_configs()
        self._primary_name = db_configs[0]['name']
        self._db_prefix = ""
        if 'db_prefix' in fdw_config:
            self._db_prefix = fdw_config['db_prefix']
        self._replica_name = self._db_prefix + self._primary_name

    def get_added_databases(self) -> list:
        return self._added_databases

    def _setup_default_fdw(self) -> None:
        if len(self._fdw) == 0:
            # connect to the replica database -- used to perform any 'sync' directives
            self._fdw[self._replica_name] = springtail.connect_fdw_instance(self._props, self._replica_name)
            self._fdw[self._replica_name].autocommit = True

    def _cleanup_fdw_connections(self) -> None:
        if len(self._fdw) == 0:
            return
        for db_name in self._fdw:
            self._fdw[db_name].close()
        self._fdw = {}

    def _append_command(self,
                        command: dict,
                        section: str,
                        is_threaded: bool,
                        txn_id: str,
                        line_num: int) -> None:
        """Add a command to the appropriate position in the test
        sequence.

        """
        command['line'] = line_num
        command['txn'] = txn_id
        self._txns.add(txn_id)

        # if no sub-section directive has been specified, then we default to a sequential section
        if len(self._sections[section]) == 0:
            self._sections[section].append({'sequential': []})

        if is_threaded:
            if txn_id not in self._sections[section][-1]['parallel']:
                self._sections[section][-1]['parallel'][txn_id] = []
            self._sections[section][-1]['parallel'][txn_id].append(command)
        else:
            self._sections[section][-1]['sequential'].append(command)


    def _raise_error(self, error: str) -> None:
        """Called where there is a configuration error in the test."""
        self._result = 'ERROR'
        self._error = error
        raise Exception(error)


    def _raise_failure(self, error: str) -> None:
        """Called where there is an execution or verification failure."""
        self._result = 'FAILED'
        self._error = error
        raise Exception(error)


    def parse_file(self) -> None:
        """Parse the test file."""
        is_threaded = False
        sql = []
        cur_txn = self._metadata['default_txn']
        line_num = 0

        with open(self._filename, 'r') as f:
            for line in f:
                line_num += 1

                # remove leading and trailing whitespace and ignore comments
                line = line.strip()
                if not line or line.startswith('--'):
                    continue

                # check for special directives
                if line.startswith('###'):
                    if len(sql) > 0:
                        self._raise_error(f'{line_num}: directives cannot be placed within a SQL statement')

                    # parse the directive
                    directive = shlex.split(line[3:])
                    if directive[0] == 'parallel':
                        if section != 'test':
                            self._raise_error(f'{line_num}: "parallel" must be in the "test" section')
                        is_threaded = True
                        self._sections['test'].append({'parallel': {}})
                        cur_txn = self._metadata['default_txn']

                    elif directive[0] == 'txn':
                        if section != 'test':
                            self._raise_error(f'{line_num}: "txn" must be in the "test" section')
                        if len(directive) < 2:
                            self._raise_error(f'{line_num}: "txn" missing the transaction ID')
                        cur_txn = directive[1]

                    elif directive[0] == 'sequential':
                        if section != 'test':
                            self._raise_error(f'{line_num}: "sequential" must be in the "test" section')
                        is_threaded = False
                        self._sections['test'].append({'sequential': []})
                        cur_txn = directive[1] if len(directive) > 1 else self._metadata['default_txn']

                    elif directive[0] == 'load_csv':
                        if section != 'test' and section != 'setup' and section != 'cleanup':
                            self._raise_error(f'{line_num}: "load_csv" must be in either the "setup", "test", or "cleanup" sections')
                        self._append_command({
                            'type': 'load_csv',
                            'file': os.path.join(self._directory, directive[1]),
                            'table': directive[2]
                        }, section, is_threaded, cur_txn, line_num)

                    elif directive[0] == 'sleep':
                        if section != 'test':
                            self._raise_error(f'{line_num}: "sleep" must be part of the "test" section')
                        if not is_threaded:
                            self._raise_error(f'{line_num}: "sleep" must be part of a transaction within a parallel sub-section')
                        if len(directive) < 2:
                            self._raise_error(f'{line_num}: "sleep" must specify a duration')

                        self._append_command({
                            'type': 'sleep',
                            'duration': directive[1]
                        }, section, is_threaded, cur_txn, line_num)

                    elif directive[0] == 'sync':
                        if section != 'test':
                            self._raise_error(f'{line_num}: "sync" must be part of the "test" section')
                        if is_threaded:
                            self._raise_error(f'{line_num}: "sync" must be within a sequential sub-section')

                        self._append_command({
                            'type': 'sync'
                        }, section, is_threaded, cur_txn, line_num)

                    elif directive[0] == 'recovery_point':
                        if section != 'test':
                            self._raise_error(f'{line_num}: "recovery_point" must be part of the "test" section')
                        if is_threaded:
                            self._raise_error(f'{line_num}: "recovery_point" must be within a sequential sub-section')

                        # note: force a sync prior to capturing the XID recovery point so that the recovery is consistent
                        self._append_command({
                            'type': 'sync'
                        }, section, is_threaded, cur_txn, line_num)
                        self._append_command({
                            'type': 'recovery_point',
                            'name': directive[1] if len(directive) > 1 else 'default'
                        }, section, is_threaded, cur_txn, line_num)

                    elif directive[0] == 'force_recovery':
                        if section != 'test':
                            self._raise_error(f'{line_num}: "force_recovery" must be part of the "test" section')
                        if is_threaded:
                            self._raise_error(f'{line_num}: "force_recovery" must be within a sequential sub-section')

                        # note: always force a sync before recovery
                        self._append_command({
                            'type': 'sync'
                        }, section, is_threaded, cur_txn, line_num)
                        self._append_command({
                            'type': 'force_recovery',
                            'name': directive[1] if len(directive) > 1 else 'default'
                        }, section, is_threaded, cur_txn, line_num)

                    elif directive[0] == 'restart':
                        if section != 'test':
                            self._raise_error(f'{line_num}: "restart" must be part of the "test" section')
                        if is_threaded:
                            self._raise_error(f'{line_num}: "restart" must be within a sequential sub-section')

                        # note: always force a sync before recovery
                        self._append_command({
                            'type': 'sync'
                        }, section, is_threaded, cur_txn, line_num)
                        self._append_command({
                            'type': 'restart',
                        }, section, is_threaded, cur_txn, line_num)

                    elif directive[0] == 'streaming':
                        if section != 'test':
                            self._raise_error(f'{line_num}: "streaming" must be part of the "test" section')
                        self._append_command({
                            'type': 'streaming',
                            'enable': directive[1] == 'true'
                        }, section, is_threaded, cur_txn, line_num)

                    elif directive[0] == 'schema_check':
                        if section != 'verify':
                            self._raise_error(f'{line_num}: "schema_check" must be part of the "verify" section')
                        if len(directive) < 3:
                            self._raise_error(f'{line_num}: "schema_check" must specify a schema and table, \
                                    with an optional wait time for secondary indexes reconciliation')

                        self._append_command({
                            'type': 'schema_check',
                            'schema': directive[1],
                            'table': directive[2],
                            'wait_for': int(directive[3]) if len(directive) > 3 else self._metadata['sync_timeout']
                        }, section, is_threaded, cur_txn, line_num)

                    # Usage - table_exists <schema> <table> <replica_exists>
                    # Ex: ### table_exists public test_init true
                    # Determines if a specific table is present in the replica, used in scenarios where an valid table
                    # is altered to add some invalid columns
                    elif directive[0] == 'table_exists':
                        if section != 'verify':
                            self._raise_error(f'{line_num}: "table_exists" must be part of the "verify" section')
                        if len(directive) < 4:
                            self._raise_error(f'{line_num}: "table_exists" must specify a schema, table, and replica exists value')

                        self._append_command({
                            'type': 'table_exists',
                            'schema': directive[1],
                            'table': directive[2],
                            'replica_exists': directive[3] == 'true'
                        }, section, is_threaded, cur_txn, line_num)

                    # Usage - benchmark <release_time_ms> <debug_time_ms> "<query sql>"
                    # Ex: ### benchmark 1000 5000 "SELECT * FROM customers"
                    # Measures the execution time of the query as reported by EXPLAIN ANALYZE 
                    # and compares it to release_time or debug_time respectively
                    elif directive[0] == 'benchmark':
                        if section != 'verify':
                            self._raise_error(f'{line_num}: "benchmark" must be part of the "verify" section')
                        if len(directive) < 3:
                            self._raise_error(f'{line_num}: "benchmark" must specify the expected release and debug times')

                        self._append_command({
                            'type': 'benchmark',
                            'query_time_release': directive[1],
                            'query_time_debug': directive[2],
                            'benchmark_query': directive[3]
                        }, section, is_threaded, cur_txn, line_num)

                    # Usage - index_exists <schema> <table> <index> <replica_exists>
                    # Ex: ### index_exists public test_init test_init_index true
                    # Determines if a specific index is present in the replica, used in scenarios where we do not replicate an index
                    # and want to verify that
                    elif directive[0] == 'index_exists':
                        if section != 'verify':
                            self._raise_error(f'{line_num}: "index_exists" must be part of the "verify" section')
                        if len(directive) < 5:
                            self._raise_error(f'{line_num}: "index_exists" must specify a schema, table, index, and replica exists value')

                        self._append_command({
                            'type': 'index_exists',
                            'schema': directive[1],
                            'table': directive[2],
                            'index': directive[3],
                            'replica_exists': directive[4] == 'true'
                        }, section, is_threaded, cur_txn, line_num)

                    elif directive[0] == 'autocommit':
                        if section != 'metadata':
                            self._raise_error(f'{line_num}: "autocommit" must be specified in the "metadata" section')
                        self._metadata['autocommit'] = (directive[1].lower() == 'true')

                    elif directive[0] == 'sync_timeout':
                        if section != 'metadata':
                            self._raise_error(f'{line_num}: "sync_timeout" must be specified in the "metadata" section')
                        self._metadata['sync_timeout'] = int(directive[1])

                    elif directive[0] == 'query_timeout':
                        if section != 'metadata':
                            self._raise_error(f'{line_num}: "query_timeout" must be specified in the "metadata" section')
                        self._metadata['query_timeout'] = int(directive[1])

                    elif directive[0] == 'poll_interval':
                        if section != 'metadata':
                            self._raise_error(f'{line_num}: "poll_interval" must be specified in the "metadata" section')
                        self._metadata['poll_interval'] = float(directive[1])

                    elif directive[0] == 'default_txn':
                        if section != 'metadata':
                            self._raise_error(f'{line_num}: "default_txn" must be specified in the "metadata" section')
                        self._metadata['default_txn'] = directive[1]

                    elif directive[0] == 'live_startup':
                        if section != 'metadata':
                            self._raise_error(f'{line_num}: "live_startup" must be specified in the "metadata" section')
                        if not self._filename.endswith(_GLOBAL_CONFIG_FILE):
                            self._raise_error(f'{line_num}: "live_startup" must be specified in the "{_GLOBAL_CONFIG_FILE}" file')
                        self._metadata['live_startup'] = float(directive[1])

                    elif directive[0] == 'add_db':
                        if section != 'setup':
                            self._raise_error(f'{line_num}: "add_db" must be specified in the "metadata" section')
                        if not self._filename.endswith(_GLOBAL_CONFIG_FILE):
                            self._raise_error(f'{line_num}: "add_db" must be specified in the "{_GLOBAL_CONFIG_FILE}" file')
                        if len(directive) < 2:
                            self._raise_error(f'{line_num}: "add_db" must specify a database_name value')
                        db_name = directive[1]
                        self._append_command({
                            'type': 'add_db',
                            'database_name': db_name
                        }, section, is_threaded, cur_txn, line_num)
                        self._added_databases.append(db_name)

                    elif directive[0] == 'switch_db':
                        if section != 'test' and section != 'setup' and section != 'verify' and section != 'cleanup':
                            self._raise_error(f'{line_num}: "switch_db" must be in either the "setup", "test", "verify", or "cleanup" sections')
                        if len(directive) < 2:
                            self._raise_error(f'{line_num}: "switch_db" must specify a database_name value')
                        self._append_command({
                            'type': 'switch_db',
                            'database_name': directive[1]
                        }, section, is_threaded, cur_txn, line_num)

                    else:
                        self._raise_error(f'{line_num}: unknown directive "{directive[0]}"')

                elif line.startswith('##'):
                    # entering a new section
                    section = line[2:].strip()
                    # reset the threaded and current transaction for the new section
                    is_threaded = False
                    cur_txn = self._metadata['default_txn']
                    if section not in self._sections and section != 'metadata':
                        self._raise_error(f'{line_num}: Unknown section: {section}')

                else:
                    # metadata section cannot contain SQL
                    if section == 'metadata':
                        self._raise_error(f'{line_num}: "metadata" section may not contain SQL statements')

                    # continue the sql statement
                    sql.append(line)

                    # record the sql statement
                    if line.endswith(';'):
                        # end of SQL statement
                        self._append_command({
                            'type': 'sql',
                            'sql': ' '.join(sql)
                        }, section, is_threaded, cur_txn, line_num)
                        sql = []


    def _load_csv(self, cursor: psycopg2.extensions.cursor, filename: str, table: str) -> None:
        """Load the provided CSV file into the specified table."""
        logging.debug(f'Load CSV {filename} into {table}')

        with open(filename, 'r') as f:
            f.readline() # skip the header
            cursor.copy_from(f, table, sep=',', null='')


    def _execute_sql(self, cursor: psycopg2.extensions.cursor, sql: str, do_fetch: bool, txn: str = 'replica', quiet: bool = False) -> list:
        """Execute the provided SQL using the provided cursor."""
        db_name = cursor.connection.info.dbname
        if not quiet:
            logging.debug(f'Execute transaction \'{txn}\' database \'{db_name}\' SQL: {sql}')
        try:
            cursor.execute(sql)

            if do_fetch:
                return cursor.fetchall()
            return None
        except psycopg2.OperationalError as e:
            self._raise_failure(f'Query timed out: {e}')
        except Exception as e:
            logging.error(f"Error executing SQL:\n{sql},\n\ttxn: {txn},\n\tdatabse: {db_name}\n\tError:{str(e)}")
            self._raise_failure(f'Unknown error: {e}')


    def _get_db_id(self, db_name: str) -> int:
        """Get database id from the configuration for the given database name."""
        configs = self._props.get_db_configs()
        for item in configs:
            if item['name'] == db_name:
                return int(item['id'])
        return None

    def _execute_command(self, command: dict, do_fetch: bool = False) -> list:
        """Execute a sql command or test directive.  When executing a
        SQL statement, will use the "txn" key to determine which
        transaction to run the SQL statement within.

        """
        logging.debug(f'Execute command {command["type"]} from line {command["line"]}')

        # check for non-SQL statements
        if command['type'] == 'sleep':
            # sleep for 'duration' seconds
            time.sleep(float(command['duration']))
            return None

        if command['type'] == 'recovery_point':
            # check the current XID and store it as a recovery point using the provided name
            txn = command['txn']
            current_db = self._connections[txn]['current_db']
            db_id = self._get_db_id(current_db)
            current_xid = springtail.current_xid(self._props, db_id)
            self._recovery_points[command['name']] = (db_id, current_xid)

        if command['type'] == 'force_recovery':
            # confirm we have a recorded recovery point
            if command['name'] not in self._recovery_points:
                self._raise_error(f'Tried to recover to undefined recovery point: {command["name"]}')

            # check the current XID and revert to an earlier target XID
            (target_db_id, target_xid) = self._recovery_points[command['name']]
            logging.debug(f'Force recovery to database: {target_db_id}, xid: {target_xid}')

            # close all connections to the replica database before shutting down
            self._cleanup_fdw_connections()

            # restart Springtail at the target XID
            springtail.restart(self._props, self._build_dir,
                               db_id=target_db_id, start_xid=target_xid, unarchive_logs=True)

            # reconnect to the replica database
            self._open_db_connections_for_fdw()

            return None

        if command['type'] == 'restart':
            # restart Springtail at the target XID
            springtail.restart(self._props, self._build_dir,
                               db_id=None, start_xid=None, unarchive_logs=True)

            # reconnect to the replica database
            self._cleanup_fdw_connections()
            self._open_db_connections_for_fdw()

            return None

        # handle SQL statements
        txn = command['txn']
        current_db = self._connections[txn]['current_db']
        connection = self._connections[txn]['connections'][current_db]
        with connection.cursor() as cursor:
            if command['type'] == 'streaming':
                is_enabling = command['enable']
                if is_enabling:
                    logging.debug(f'Enabling streaming')
                    # set to lower value to enable streaming
                    self._execute_sql(cursor, f"ALTER SYSTEM SET logical_decoding_work_mem = '64kB'", False, txn)
                    self._execute_sql(cursor, f"SELECT pg_reload_conf()", False, txn)
                else:
                    # reset to default value
                    self._execute_sql(cursor, f"ALTER SYSTEM SET logical_decoding_work_mem = '64MB'", False, txn)
                    self._execute_sql(cursor, f"SELECT pg_reload_conf()", False, txn)

            elif command['type'] == 'load_csv':
                # call the helper to read the CSV file and populate the table
                self._load_csv(cursor, command['file'], command['table'])
                return None

            elif command['type'] == 'sql':
                # execute a SQL command
                return self._execute_sql(cursor, command['sql'], do_fetch, txn)

            elif command['type'] == "benchmark":
                sql = f"EXPLAIN (FORMAT JSON, ANALYZE) {command['benchmark_query']};" 
                return self._execute_sql(cursor, sql, do_fetch, txn)

        if command['type'] == 'sync':
            # insert a row to the sync_control table
            self._sync_step += 1
            for db_name, connection in self._connections[txn]["connections"].items():
                with connection.cursor() as cursor:
                    self._execute_sql(cursor, f"BEGIN; SET statement_timeout = 5000; INSERT INTO sync_control (sync, test) VALUES ({self._sync_step}, '{self._name}'); COMMIT;", False, txn)

            for db_name in self._connections[txn]["connections"].keys():
                # Wait for sync row to appear in replica
                try:
                    replica_name = self._db_prefix + db_name
                    sync_time = common.wait_for_replica_condition(
                        self._fdw[replica_name],
                        f"SELECT MAX(sync) FROM sync_control WHERE test = '{self._name}'",
                        (self._sync_step,),
                        timeout=self._metadata['sync_timeout'],
                        poll_interval=self._metadata['poll_interval']
                    )
                except Exception as e:
                    self._raise_failure(f'Sync control error: {e}')

            return []

        if command['type'] == 'table_exists' or command['type'] == 'index_exists':
            results = {}

            results['exists'] = command['replica_exists']

            return results

        connection = self._connections[txn]['connections'][current_db]
        with connection.cursor() as cursor:
            if command['type'] == 'schema_check':
                results = {}

                sql = f""" SELECT a.attname AS name,
                                    CASE
                                        WHEN t.oid = 1560 THEN 1562 --remap bitoid to varbitoid
                                        ELSE t.oid
                                    END AS pg_type,
                                    NOT a.attnotnull AS nullable,
                                    a.attnum AS position
                            FROM pg_catalog.pg_attribute a
                            JOIN pg_class c ON a.attrelid = c.oid
                            JOIN pg_type t ON a.atttypid = t.oid
                            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
                            WHERE n.nspname = '{command["schema"]}' AND c.relname = '{command["table"]}'
                                    AND c.relkind IN ('r','p')
                                    AND a.attnum > 0
                                    AND NOT a.attisdropped
                            ORDER BY a.attnum ASC;"""
                results['columns'] = self._execute_sql(cursor, sql, True, txn)

                # retrieve the partition information for the table
                sql = f"""SELECT
                            CASE WHEN t.relispartition THEN
                                (SELECT inhparent FROM pg_inherits WHERE inhrelid = t.oid)
                            END as parent_oid,
                            pg_get_expr(t.relpartbound, t.oid, TRUE) as partition_bound,
                            pg_get_partkeydef(t.oid) as partition_key
                        FROM pg_class t
                        JOIN pg_catalog.pg_namespace n ON (t.relnamespace = n.oid)
                        WHERE n.nspname = '{command["schema"]}' AND t.relname = '{command["table"]}';"""
                results['partition_info'] = self._execute_sql(cursor, sql, True, txn)

                # retrieve the primary index information for the table
                sql = f"""SELECT unnest(conkey) AS column_id,
                                    generate_subscripts(conkey, 1) - 1 AS position
                            FROM pg_catalog.pg_constraint c
                            JOIN pg_catalog.pg_class t ON (t.oid = c.conrelid)
                            JOIN pg_catalog.pg_namespace n ON (t.relnamespace = n.oid)
                            WHERE n.nspname = '{command["schema"]}' AND t.relname = '{command["table"]}' AND c.contype = 'p';"""
                results['primary'] = self._execute_sql(cursor, sql, True, txn)

                sql = f"""SELECT c.oid as table_id,
                                    i.indexrelid as index_id,
                                    unnest(string_to_array(i.indkey::text, ' '))::int as column_id
                    FROM pg_index i
                    JOIN pg_class c ON c.oid = i.indrelid
                    JOIN pg_namespace ns ON ns.oid = c.relnamespace
                    WHERE c.relname = '{command["table"]}' AND ns.nspname = '{command["schema"]}'
                    AND i.indisprimary IS FALSE
                    ORDER BY column_id ASC;
                """
                results['secondary'] = self._execute_sql(cursor, sql, True, txn)

                return results

        if command['type'] == "add_db":
            return None

        if command['type'] == "switch_db":
            self._connections[txn]['current_db'] = command['database_name']
            return None



    def _wait_for_index_reconciliation(self, wait_for: int) -> bool:
        query = """
            SELECT count(DISTINCT index_id)
            FROM __pg_springtail_catalog.index_names
            WHERE index_id <> 0 AND (
                -- Rule 1 violation: index_id in state 0 but not in 1 or 2
                (index_id IN (SELECT index_id FROM __pg_springtail_catalog.index_names WHERE state = 0)
                 AND index_id NOT IN (SELECT index_id FROM __pg_springtail_catalog.index_names WHERE state IN (1, 2)))

                OR

                -- Rule 2 violation: index_id in state 3 but not in 2
                (index_id IN (SELECT index_id FROM __pg_springtail_catalog.index_names WHERE state = 3)
                 AND index_id NOT IN (SELECT index_id FROM __pg_springtail_catalog.index_names WHERE state = 2))
            );
        """

        try:
            common.wait_for_replica_condition(self._fdw[self._replica_name], query, (0, ), timeout=wait_for)
        except Exception as e:
            self._raise_failure(f'Secondary indexes not in sync within {wait_for}s.')


    def _get_ranking_sql(self, is_index_query: bool = False) -> str:
        index_cond = 'AND n.index_id <> 0' if is_index_query is True else 'AND n.index_id = 0'

        xid_sql = f"""SELECT distinct on (n.index_id) index_id, n.xid, n.lsn,n.state
            FROM "__pg_springtail_catalog"."index_names" n
            WHERE n.table_id = (SELECT table_id FROM latest_table WHERE exists IS TRUE)
            {index_cond}
            ORDER BY n.index_id, n.xid DESC, n.lsn DESC"""

        ranking_sql = f"""SELECT i.*
            FROM "__pg_springtail_catalog"."indexes" i
            JOIN ({xid_sql}) n
            ON i.index_id=n.index_id AND i.xid = n.xid AND i.lsn = n.lsn
            WHERE i.table_id = (SELECT table_id FROM latest_table WHERE exists IS TRUE)
            AND n.state = 1"""

        return ranking_sql

    def _replica_command(self, command: dict) -> list:
        """Runs a SQL command against the Springtail replica
        database.

        """
        if command['type'] == 'switch_db':
            return None

        txn = command['txn']
        db_name = self._connections[txn]['current_db']
        connection = self._fdw[self._db_prefix + db_name]

        with connection.cursor() as cursor:
            if command['type'] == 'sql':
                return self._execute_sql(cursor, command['sql'], True, 'replica')

            elif command['type'] == 'table_exists':
                results = {}
                replica_result = True

                with_sql = f"""SELECT "table_names"."table_id", "table_names"."exists"
                               FROM "__pg_springtail_catalog"."table_names"
                               JOIN "__pg_springtail_catalog"."namespace_names" ON "namespace_names"."namespace_id" = "table_names"."namespace_id"
                               WHERE "namespace_names"."name" = '{command["schema"]}' AND "table_names"."name" = '{command["table"]}'
                               ORDER BY "table_names"."xid" DESC, "table_names"."lsn" DESC
                               LIMIT 1"""
                sql = f"""WITH latest_table AS ({with_sql})
                          SELECT exists FROM latest_table LIMIT 1"""

                sql_result = self._execute_sql(cursor, sql, True, 'replica')

                replica_result = False if not sql_result else sql_result[0][0]

                results['exists'] = replica_result

                return results
            elif command['type'] == 'index_exists':
                results = {}
                replica_result = True

                with_sql = f"""SELECT EXISTS (SELECT "index_names"."name"
                                FROM "__pg_springtail_catalog"."index_names"
                                JOIN "__pg_springtail_catalog"."table_names"
                                ON "table_names"."table_id" = "index_names"."table_id"
                                JOIN "__pg_springtail_catalog"."namespace_names"
                                ON "namespace_names"."namespace_id" = "table_names"."namespace_id"
                                    AND "namespace_names"."namespace_id" = "index_names"."namespace_id"
                                WHERE "namespace_names"."name" = '{command["schema"]}'
                                AND "table_names"."name" = '{command["table"]}'
                                AND "index_names"."name" = '{command["schema"]}.{command["index"]}') AS index_exists
                            """
                sql = f"""WITH latest_table AS ({with_sql})
                          SELECT index_exists FROM latest_table LIMIT 1"""

                sql_result = self._execute_sql(cursor, sql, True, 'replica')

                replica_result = False if not sql_result else sql_result[0][0]
                results['exists'] = replica_result

                return results
            elif command['type'] == 'schema_check':
                results = {}

                # retrieve the column data
                with_sql = f"""SELECT "table_names"."table_id", "table_names"."exists",
                                "table_names"."parent_table_id", "table_names"."partition_bound", "table_names"."partition_key"
                                FROM "__pg_springtail_catalog"."table_names"
                                JOIN "__pg_springtail_catalog"."namespace_names" ON "namespace_names"."namespace_id" = "table_names"."namespace_id"
                                WHERE "namespace_names"."name" = '{command["schema"]}' AND "table_names"."name" = '{command["table"]}'
                                ORDER BY "table_names"."xid" DESC, "table_names"."lsn" DESC
                                LIMIT 1"""
                ranking_sql = """SELECT *,
                                 ROW_NUMBER() OVER (PARTITION BY name ORDER BY xid DESC, lsn DESC) AS rn
                                 FROM "__pg_springtail_catalog"."schemas"
                                 WHERE table_id = (SELECT table_id FROM latest_table WHERE exists IS TRUE)"""
                sql = f"""WITH latest_table AS ({with_sql}), ranked_columns AS ({ranking_sql})
                          SELECT name, pg_type, nullable, position FROM ranked_columns WHERE rn = 1 AND exists IS TRUE ORDER BY position ASC;"""

                results['columns'] = self._execute_sql(cursor, sql, True, 'replica')

                # retrieve the partition information for the table
                sql = f"""WITH latest_table AS ({with_sql})
                          SELECT parent_table_id AS parent_table_id,
                                 partition_bound AS partition_bound,
                                 partition_key AS partition_key FROM latest_table WHERE exists IS TRUE LIMIT 1;"""
                results['partition_info'] = self._execute_sql(cursor, sql, True, 'replica')

                # retrieve the primary key data
                with_sql = f"""SELECT "table_names"."table_id", "table_names"."exists"
                                FROM "__pg_springtail_catalog"."table_names"
                                JOIN "__pg_springtail_catalog"."namespace_names" ON "namespace_names"."namespace_id" = "table_names"."namespace_id"
                                WHERE "namespace_names"."name" = '{command["schema"]}' AND "table_names"."name" = '{command["table"]}'
                                ORDER BY "table_names"."xid" DESC, "table_names"."lsn" DESC
                                LIMIT 1"""

                ranking_sql = self._get_ranking_sql()
                sql = f"""WITH latest_table AS ({with_sql}), ranked_columns AS ({ranking_sql})
                          SELECT column_id, position FROM ranked_columns ORDER BY position ASC;"""
                results['primary'] = self._execute_sql(cursor, sql, True, 'replica')

                # Wait for index reconciliation
                self._wait_for_index_reconciliation(command["wait_for"])

                index_sql = self._get_ranking_sql(is_index_query=True)
                sql = f"""WITH latest_table AS ({with_sql}), ranked_indexes AS ({index_sql})
                         SELECT table_id, index_id, column_id FROM ranked_indexes ORDER BY column_id ASC;"""
                results['secondary'] = self._execute_sql(cursor, sql, True, 'replica')

                return results

            elif command['type'] == 'benchmark':
                sql = f"EXPLAIN (FORMAT JSON, ANALYZE) {command['benchmark_query']};" 
                return self._execute_sql(cursor, sql, True, 'replica')

            else:
                self._raise_error(f'Cannot execute "{command["type"]}" commands against the replica.')


    def _execute_commands(self, commands: list) -> None:
        """Helper to execute a set of commands.  Also used as a helper
        to execute parallel subsections via ThreadPoolExecutor.

        """
        for command in commands:
            self._execute_command(command)


    def _run_background(self, frequency: float):
        connection = springtail.connect_db_instance(self._props, self._primary_name)
        with connection.cursor() as cursor:
            self._execute_sql(cursor, f'BEGIN; DROP TABLE IF EXISTS background_control; CREATE TABLE background_control (value INT); COMMIT;', False, 'background')

        # run periodically
        while not self._stop_thread.wait(frequency):
            self._value += 1
            with connection.cursor() as cursor:
                self._execute_sql(cursor, f"BEGIN; INSERT INTO background_control (value) VALUES ({self._value}); COMMIT;", False, 'background', True)

            pass
        connection.close()

    def _open_db_connections_for_txn(self, txn: str, use_proxy: bool) -> None:
        self._connections[txn] = {
            'current_db': self._primary_name,
            'connections': {}
        }
        for db_config in self._props.get_db_configs():
            # connect to the db instance
            db_name = db_config['name']
            if db_name in self._connections[txn]['connections']:
                self._connections[txn]['connections'][db_name].close()

            if use_proxy:
                logging.debug(f'Connecting to proxy for txn "{txn}" database "{db_name}"')
                self._connections[txn]['connections'][db_name] = springtail.connect_proxy(self._props, db_name)
            else:
                logging.debug(f'Connecting to primary for txn "{txn}" database "{db_name}"')
                self._connections[txn]['connections'][db_name] = springtail.connect_db_instance(self._props, db_name)
            self._connections[txn]['connections'][db_name].autocommit = self._metadata['autocommit']

    def _open_db_connections_for_fdw(self) -> None:
        for db_config in self._props.get_db_configs():
            # connect to the db instance
            db_name = self._db_prefix + db_config['name']
            if db_name in self._fdw:
                self._fdw[db_name].close()
            connected = False
            conn_attempts = 0
            while not connected:
                try:
                    self._fdw[db_name] = springtail.connect_db_instance(self._props, db_name)
                    self._fdw[db_name].autocommit = True
                    connected = True
                except Exception as e:
                    conn_attempts += 1
                    if conn_attempts == 5:
                        logging.error("Tried to connect {conn_attempts} times")
                        raise e
                    time.sleep(2)

    def start_background(self) -> None:
        if self._metadata['live_startup'] is not None:
            logging.debug("Start background mutations")
            self._stop_thread = threading.Event()
            self._value = 0
            self._bg_thread = threading.Thread(target=self._run_background, args=[ self._metadata['live_startup'] ])
            self._bg_thread.start()


    def setup(self) -> None:
        """Run SQL commands prior to starting Springtail.  Used to
        prepare tables and data that will be copied into Springtail on
        startup.

        """
        if self._status != 'INIT':
            self._raise_error('Must run setup() first for global config')
        self._status = 'SETUP_BEGIN'

        logging.info(f'{self._name} -- Running setup()')

        # construct a connection for each transaction in the test
        if self._metadata['default_txn'] not in self._txns:
            self._txns.add(self._metadata['default_txn'])

        for txn in self._txns:
            logging.debug(f'Connecting to databases for txn "{txn}"')
            self._open_db_connections_for_txn(txn, False)

        # execute all of the setup commands
        if len(self._sections['setup']) > 0:
            self._execute_commands(self._sections['setup'][0]['sequential'])

        # create the sync control table
        txn = self._metadata['default_txn']
        for db_name, connection in self._connections[txn]["connections"].items():
            with connection.cursor() as cursor:
                self._execute_sql(cursor, 'BEGIN; DROP TABLE IF EXISTS sync_control; CREATE TABLE sync_control (sync INT, test TEXT); COMMIT;', False, txn)

        self._status = 'SETUP_END'


    def start_capture(self) -> None:
        # capture the logs
        self._log_stream = io.StringIO()
        self._log_handler = logging.StreamHandler(self._log_stream)

        logger = logging.getLogger()
        logger.addHandler(self._log_handler)


    def stop_capture(self) -> None:
        logger = logging.getLogger()
        logger.removeHandler(self._log_handler)

        self._logged_output = self._log_stream.getvalue()


    def test(self) -> None:
        """Run SQL commands that form the actual test.  Will be
        executed while Springtail is actively replicating data.

        """
        if self._status != 'INIT':
            self._raise_error('Must run test() first for individual tests')
        self._status = 'TEST_BEGIN'

        logging.info(f'{self._name} -- Running test()')

        # Determine what config to use for the test phase
        use_proxy_for_test = self._test_params.get('use_proxy_for_test', False)

        # construct a connection for each transaction in the test
        for txn in self._txns:
            self._open_db_connections_for_txn(txn, use_proxy_for_test)

        # connect to the replica database -- used to perform any 'sync' directives
        self._open_db_connections_for_fdw()
        with self._fdw[self._replica_name].cursor() as c:
            self._execute_sql(c, f'BEGIN; SET statement_timeout = {self._metadata["query_timeout"] * 1000}; COMMIT;', False, 'replica')

        # XXX need a way to determine when the database is up and running... poll Redis?

        # begin the timer
        start = time.time()

        # go through each subsection
        for subsection in self._sections['test']:
            if 'sequential' in subsection:
                logging.debug("Entering sequential section")

                # go through each command and execute it against the appropriate transaction
                self._execute_commands(subsection['sequential'])

            elif 'parallel' in subsection:
                logging.debug("Entering parallel section")

                # for parallel subsections, execute each transaction's commands in parallel
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    futures = []

                    # execute the transactions in parallel
                    for txn in subsection['parallel']:
                        future = executor.submit(self._execute_commands, subsection['parallel'][txn])
                        futures.append(future)

                    # wait for completion of all threads
                    concurrent.futures.wait(futures)

        # force a commit on all connections at the end of the test section
        for txn in self._connections:
            for db_config in self._props.get_db_configs():
                db_name = db_config['name']
                self._connections[txn]["connections"][db_name].commit()

        # pick a connection from any transaction on the primary database to run the sync against
        txn = self._metadata['default_txn']
        self._connections[txn]['current_db'] = self._primary_name

        # wait for the primary and replica to come into sync
        self._execute_command({
            'type': 'sync',
            'txn': txn,
            'line': -1
        })

        # end the timer and record the duration
        # XXX currently will be skewed by the 1s polling of the sync call
        end = time.time()
        self._duration = end - start

        self._status = 'TEST_END'


    def verify(self) -> None:
        """Run SQL commands to verify that the replication worked as
        expected.

        """
        if self._status != 'TEST_END':
            self._raise_error('Must run verify() after test()')
        self._status = 'VERIFY_BEGIN'

        logging.info(f'{self._name} -- Running verify()')

        # Determine what config to use for verify phase
        use_proxy_for_verify = self._test_params.get('use_proxy_for_verify', False)
        txn = self._metadata['default_txn']
        self._open_db_connections_for_txn(txn, use_proxy_for_verify)
        self._open_db_connections_for_fdw()

        # execute the verification commands against both databases, compare the results
        for command in self._sections['verify'][0]['sequential']:
            primary_result = self._execute_command(command, True)
            replica_result = self._replica_command(command)
            if command["type"] == "benchmark":
                primary_time = primary_result[0][0][0]['Execution Time']
                replica_time = replica_result[0][0][0]['Execution Time']
                print(f"Benchmarks: primary:{primary_time}ms, replica:{replica_time}ms")
                if "debug" in self._build_dir:
                    expected_time = float(command['query_time_debug'])
                else:
                    expected_time = float(command['query_time_release'])

                if replica_time > expected_time:
                    self._raise_failure(
                            f"Benchmark verification failed for {self._name}:\n"
                            f"Statement: {command}\n"
                            f"Main DB time: {primary_time}ms\n"
                            f"Replica DB: {replica_time}ms\n"
                            f"Expected time: {expected_time}ms\n"
                        )
                continue

            if primary_result != replica_result and str(primary_result) != str(replica_result):
                self._raise_failure(
                        f"Verification failed for {self._name}:\n"
                        f"Statement: {command}\n"
                        f"Main DB: {primary_result}\n"
                        f"Replica DB: {replica_result}"
                    )

        self._result = 'SUCCESS'
        self._status = 'VERIFY_END'

    def stop_background(self) -> bool:
        if self._metadata['live_startup'] is None:
            return True

        logging.debug('Stop background mutations and verify')
        self._stop_thread.set()
        self._bg_thread.join()

        # wait for the background job to complete -- if it never does then we fail
        try:
            self._setup_default_fdw()

            common.wait_for_replica_condition(
                self._fdw[self._replica_name],
                "SELECT COUNT(value), MAX(value) FROM background_control;",
                (self._value, self._value),
                timeout=self._metadata['sync_timeout']
            )
        except Exception as e:
            logging.error(f'Background job error: {e}')
            return False

        return True

    def cleanup(self) -> None:
        """Run SQL commands to clean up the primary database and close
        all database connections.

        """
        logging.info(f'{self._name} -- Running cleanup()')

        # re-connect to the database in case there was an error on the connection
        self._cleanup_fdw_connections()
        txn = self._metadata['default_txn']
        for db_name, connection in self._connections[txn]['connections'].items():
            connection.close()
            self._connections[txn]['connections'][db_name] = springtail.connect_db_instance(self._props, db_name)
        self._connections[txn]['current_db'] = self._primary_name

        # run the cleanup commands
        if len(self._sections['cleanup']) > 0:
            self._execute_commands(self._sections['cleanup'][0]['sequential'])

        # close all database connections
        for txn in self._connections:
            for db_name, connection in self._connections[txn]['connections'].items():
                connection.close()

        # close the connections to the foreign data wrappers
        for connection in self._fdw.values():
            connection.close()


    def skip(self) -> None:
        self._result = 'SKIPPED'


    def get_result(self) -> dict:
        return {
            'name': self._name,
            'status': self._status,
            'result': self._result,
            'duration': self._duration,
            'error': self._error
        }


    def junit(self) -> etree.Element:
        root = etree.Element('testcase',
                             name=self._name,
                             time=f'{self._duration:.2f}')
        if self._result == 'ERROR':
            error = etree.Element('error')
            error.text = self._error
            root.append(error)

        elif self._result == 'FAILED':
            failure = etree.Element('failure')
            failure.text = self._error
            root.append(failure)

        elif self._result == 'SKIPPED':
            skipped = etree.Element('skipped',
                                    message='Skipped due to user request')
            root.append(skipped)

        elif self._result == 'UNKNOWN':
            skipped = etree.Element('skipped',
                                    message='Skipped due to earlier failure')
            root.append(skipped)

        # record the logging output for the test case
        if self._logged_output:
            system_out = etree.Element('system-out')
            system_out.text = self._logged_output
            root.append(system_out)

        # record any backtraces to stderr
        if self._log_errors:
            system_err = etree.Element('system-err')
            system_err.text = '\n'.join(self._log_errors)
            root.append(system_err)

        return root
