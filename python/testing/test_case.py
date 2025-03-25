import concurrent.futures
import csv
import io
import logging
from lxml import etree
import os
import psycopg2
import springtail
import sysutils
import time
import common

class TestCase:
    """Class to manage a single test-case.  Handles all phases of the
    test case and stores the result of the test.

    """
    def __init__(self,
                 filename: str,
                 props: springtail.Properties,
                 build_dir: str,
                 valid_sections: list = ['test', 'verify', 'cleanup']) -> None:
        """Initialize the test case"""
        self._filename = os.path.abspath(filename)
        self._name = os.path.basename(self._filename)
        self._directory = os.path.dirname(self._filename)
        self._props = props
        self._build_dir = build_dir
        self._status = 'INIT'
        self._result = 'UNKNOWN'
        self._duration = 0
        self._error = ''
        self._log_errors = []
        self._logged_output = ''

        self._metadata = {
            'autocommit': True,
            'sync_timeout': 3,
            'default_txn': 'default',
            'query_timeout': 5
        }

        fdw_config = props.get_fdw_config()
        db_configs = props.get_db_configs()
        self._primary_name = db_configs[0]['name']
        if 'db_prefix' in fdw_config:
            self._replica_name = fdw_config['db_prefix'] + self._primary_name
        else:
            self._replica_name = self._primary_name

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
        self._fdw = None # connection to Springtail
        self._sync_step = 0 # incrementing ID used for replica synchronization


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
        txns = { }
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
                    directive = line[3:].split()
                    logging.debug(f'directive: {directive}')
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

                    elif directive[0] == 'force_recovery':
                        if section != 'test':
                            self._raise_error(f'{line_num}: "force_recovery" must be part of the "test" section')
                        self._append_command({
                            'type': 'force_recovery',
                            'count': int(directive[1])
                        }, section, is_threaded, cur_txn, line_num)

                    elif directive[0] == 'schema_check':
                        if section != 'verify':
                            self._raise_error(f'{line_num}: "schema_check" must be part of the "verify" section')
                        if len(directive) < 3:
                            self._raise_error(f'{line_num}: "schema_check" must specify a schema and table')

                        self._append_command({
                            'type': 'schema_check',
                            'schema': directive[1],
                            'table': directive[2]
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

                    elif directive[0] == 'default_txn':
                        if section != 'metadata':
                            self._raise_error(f'{line_num}: "default_txn" must be specified in the "metadata" section')
                        self._metadata['default_txn'] = directive[1]

                    else:
                        self._raise_error(f'{line_num}: unknown directive "{directive[0]}"')

                elif line.startswith('##'):
                    # entering a new section
                    section = line[2:].strip()
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


    def _execute_sql(self, cursor: psycopg2.extensions.cursor, sql: str, do_fetch: bool) -> list:
        """Execute the provided SQL using the provided cursor."""
        logging.debug(f'Execute SQL: {sql}')
        try:
            cursor.execute(sql)

            if do_fetch:
                return cursor.fetchall()
            return None
        except psycopg2.OperationalError as e:
            self._raise_failure(f'Query timed out: {e}')
        except Exception as e:
            self._raise_failure(f'Unknown error: {e}')


    def _execute_command(self, command: dict, do_fetch: bool = False) -> list:
        """Execute a sql command or test directive.  When executing a
        SQL statement, will use the "txn" key to determine which
        transaction to run the SQL statement within.

        """
        logging.debug(f'Execute command {command["type"]} from line {command["line"]}')

        # check for non-SQL statements
        if command['type'] == 'sleep':
            # sleep for 'duration' seconds
            time.sleep(command['duration'])
            return None

        if command['type'] == 'force_recovery':
            # check the current XID and revert to an earlier target XID
            db_id_str = self._props.get_db_configs()[0]['id']
            logging.debug(f'Force recovery for {db_id_str}')
            db_id = int(db_id_str)
            current_xid = springtail.current_xid(self._props, db_id)
            target_xid = current_xid - command['count']
            logging.debug(f'Force recovery from {current_xid} to {target_xid}')

            # restart Springtail at the target XID
            springtail.restart(self._props, self._build_dir, start_xid=target_xid)
            return None

        # handle SQL statements
        with self._connections[command['txn']].cursor() as cursor:
            if command['type'] == 'load_csv':
                # call the helper to read the CSV file and populate the table
                self._load_csv(cursor, command['file'], command['table'])
                return None

            elif command['type'] == 'sql':
                # execute a SQL command
                return self._execute_sql(cursor, command['sql'], do_fetch)

            elif command['type'] == 'sync':
                # insert a row to the sync_control table
                self._sync_step += 1
                self._execute_sql(cursor, f"BEGIN; SET statement_timeout = 5000; INSERT INTO sync_control (sync, test) VALUES ({self._sync_step}, '{self._name}'); COMMIT;", False)

                # Wait for sync row to appear in replica
                try:
                    sync_time = common.wait_for_replica_condition(
                        self._fdw,
                        f"SELECT MAX(sync) FROM sync_control WHERE test = '{self._name}'",
                        (self._sync_step,),
                        timeout=self._metadata['sync_timeout']
                    )
                except Exception as e:
                    self._raise_failure(f'Sync control error: {e}')

                return []

            elif command['type'] == 'schema_check':
                results = {}

                sql = f""" SELECT a.attname AS name,
                                  t.oid AS pg_type,
                                  NOT a.attnotnull AS nullable,
                                  a.attnum AS position
                           FROM pg_catalog.pg_attribute a
                           JOIN pg_class c ON a.attrelid = c.oid
                           JOIN pg_type t ON a.atttypid = t.oid
                           JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
                           WHERE n.nspname = '{command["schema"]}' AND c.relname = '{command["table"]}'
                                 AND c.relkind = 'r'
                                 AND a.attnum > 0
                                 AND NOT a.attisdropped
                           ORDER BY a.attnum ASC;"""
                results['columns'] = self._execute_sql(cursor, sql, True)

                # retrieve the primary index information for the table
                sql = f"""SELECT unnest(conkey) AS column_id,
                                 generate_subscripts(conkey, 1) - 1 AS position
                            FROM pg_catalog.pg_constraint c
                            JOIN pg_catalog.pg_class t ON (t.oid = c.conrelid)
                            JOIN pg_catalog.pg_namespace n ON (t.relnamespace = n.oid)
                           WHERE n.nspname = '{command["schema"]}' AND t.relname = '{command["table"]}' AND c.contype = 'p';"""
                results['primary'] = self._execute_sql(cursor, sql, True)

                sql = f"""SELECT c.oid as table_id, i.indexrelid as index_id, unnest(string_to_array(i.indkey::text, ' '))::int as column_id
                    FROM pg_index i
                    JOIN pg_class c ON c.oid = i.indrelid
                    JOIN pg_namespace ns ON ns.oid = c.relnamespace
                    WHERE c.relname = '{command["table"]}' AND ns.nspname = '{command["schema"]}'
                    AND i.indisprimary IS FALSE
                    ORDER BY column_id ASC;
                """
                results['secondary'] = self._execute_sql(cursor, sql, True)

                return results


    def _get_ranking_sql(self, is_index_query: bool = False) -> str:
        index_cond = 'AND i.index_id <> 0' if is_index_query is True else 'AND i.index_id = 0'

        xid_sql = f"""SELECT i.xid, i.lsn
            FROM "__pg_springtail_catalog"."indexes" i
            JOIN "__pg_springtail_catalog"."index_names" n
            ON i.xid = n.xid AND i.lsn = n.lsn
            WHERE i.table_id = (SELECT table_id FROM latest_table WHERE exists IS TRUE)
            AND n.state = 1
            {index_cond}
            ORDER BY i.xid DESC, i.lsn DESC
            {"LIMIT 1" if not is_index_query else ""}"""

        ranking_sql = f"""SELECT i.*
            FROM "__pg_springtail_catalog"."indexes" i
            JOIN "__pg_springtail_catalog"."index_names" n
            ON i.xid = n.xid AND i.lsn = n.lsn
            WHERE i.table_id = (SELECT table_id FROM latest_table WHERE exists IS TRUE)
            AND n.state = 1
            {index_cond}
            AND (i.xid, i.lsn) IN ({xid_sql})"""

        return ranking_sql

    def _replica_command(self, command: dict) -> list:
        """Runs a SQL command against the Springtail replica
        database.

        """
        with self._fdw.cursor() as cursor:
            if command['type'] == 'sql':
                return self._execute_sql(cursor, command['sql'], True)

            elif command['type'] == 'schema_check':
                results = {}

                # retrieve the column data
                with_sql = f"""SELECT "table_names"."table_id", "table_names"."exists"
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

                results['columns'] = self._execute_sql(cursor, sql, True)

                # retrieve the primary key data
                with_sql = f"""SELECT "table_names"."table_id", "table_names"."exists"
                                FROM "__pg_springtail_catalog"."table_names"
                                JOIN "__pg_springtail_catalog"."namespace_names" ON "namespace_names"."namespace_id" = "table_names"."namespace_id"
                                WHERE "namespace_names"."name" = '{command["schema"]}' AND "table_names"."name" = '{command["table"]}'
                                ORDER BY "table_names"."xid" DESC, "table_names"."lsn" DESC
                                LIMIT 1"""
                xid_sql = """SELECT "indexes"."xid", "indexes"."lsn"
                                FROM "__pg_springtail_catalog"."indexes"
                                WHERE table_id = (SELECT table_id FROM latest_table WHERE exists IS TRUE)
                                AND index_id = 0
                                ORDER BY xid DESC, lsn DESC
                                LIMIT 1"""

                ranking_sql = self._get_ranking_sql()
                sql = f"""WITH latest_table AS ({with_sql}), ranked_columns AS ({ranking_sql})
                          SELECT column_id, position FROM ranked_columns ORDER BY position ASC;"""
                results['primary'] = self._execute_sql(cursor, sql, True)

                index_sql = self._get_ranking_sql(is_index_query=True)
                sql = f"""WITH latest_table AS ({with_sql}), ranked_indexes AS ({index_sql})
                         SELECT table_id, index_id, column_id FROM ranked_indexes ORDER BY column_id ASC;"""
                results['secondary'] = self._execute_sql(cursor, sql, True)

                return results

            else:
                self._raise_error(f'Cannot execute "{command["type"]}" commands against the replica.')


    def _execute_commands(self, commands: list) -> None:
        """Helper to execute a set of commands.  Also used as a helper
        to execute parallel subsections via ThreadPoolExecutor.

        """
        for command in commands:
            self._execute_command(command)


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
        if len(self._txns) == 0:
            self._txns.add(self._metadata['default_txn'])

        for txn in self._txns:
            logging.debug(f'Connecting to database for txn "{txn}"')
            self._connections[txn] = springtail.connect_db_instance(self._props, self._primary_name)
            self._connections[txn].autocommit = self._metadata['autocommit']

        # execute all of the setup commands
        if len(self._sections['setup']) > 0:
            self._execute_commands(self._sections['setup'][0]['sequential'])

        # create the sync control table
        with self._connections[next(iter(self._txns))].cursor() as cursor:
            self._execute_sql(cursor, 'BEGIN; DROP TABLE IF EXISTS sync_control; CREATE TABLE sync_control (sync INT, test TEXT); COMMIT;', False)

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

        # construct a connection for each transaction in the test
        for txn in self._txns:
            logging.debug(f'Connecting to database for txn "{txn}"')
            self._connections[txn] = springtail.connect_instance(self._props, self._primary_name)
            self._connections[txn].autocommit = self._metadata['autocommit']

        # connect to the replica database -- used to perform any 'sync' directives
        self._fdw = springtail.connect_fdw_instance(self._props, self._replica_name)
        self._fdw.autocommit = True
        with self._fdw.cursor() as c:
            self._execute_sql(c, f'BEGIN; SET statement_timeout = {self._metadata["query_timeout"] * 1000}; COMMIT;', False)

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
                        future = executor.submit(self._execute_commands, subsction['parallel'][txn])
                        futures.append(future)

                    # wait for completion of all threads
                    concurrent.futures.wait(futures)

        # force a commit on all connections at the end of the test section
        for txn in self._connections:
            self._connections[txn].commit()

        # pick a connection to run the sync against, any will do
        txn = next(iter(self._txns))

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

        # execute the verification commands against both databases, compare the results
        for command in self._sections['verify'][0]['sequential']:
            primary_result = self._execute_command(command, True)
            replica_result = self._replica_command(command)

            if primary_result != replica_result:
                self._raise_failure(
                    f"Verification failed for {self._name}:\n"
                    f"Statement: {command}\n"
                    f"Main DB: {primary_result}\n"
                    f"Replica DB: {replica_result}"
                )

        self._result = 'SUCCESS'
        self._status = 'VERIFY_END'


    def cleanup(self) -> None:
        """Run SQL commands to clean up the primary database and close
        all database connections.

        """
        logging.info(f'{self._name} -- Running cleanup()')

        # re-connect to the database in case there was an error on the connection
        if self._fdw:
            self._fdw.close()
        for connection in self._connections:
            self._connections[connection].close()
            self._connections[connection] = springtail.connect_db_instance(self._props, self._primary_name)

        # run the cleanup commands
        if len(self._sections['cleanup']) > 0:
            self._execute_commands(self._sections['cleanup'][0]['sequential'])

        # close the database connections
        for connection in self._connections:
            self._connections[connection].close()


    def check_logs(self) -> None:
        log_path = self._props.get_log_path()
        error_logs = sysutils.check_backtrace(log_path)
        if not error_logs:
            return # if no errors, return

        logging.error(f'Found errors in logs: {error_logs}')

        for log in error_logs:
            backtrace = sysutils.extract_backtrace(log)
            if backtrace:
                self._log_errors.append(f'Error in {os.path.basename(log)}:\n{"\n".join(backtrace)}\n')
            else:
                self._log_errors.append(f'Error in {os.path.basename(log)} -- could not extract backtrace\n')


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
