To install FDW:

1. Find share extension dir:
  - pg_config --sharedir
  - for homebrew: /opt/homebrew/opt/postgresql@16/share/postgresql@16/extension
  - for ubuntu: /usr/share/postgresql/16/extension/
  - Note: there should be a bunch of .control and .sql files in that dir, if you don't see that you are in the wrong place

2. Copy springtail_fdw--1.0.sql to share/extension dir

3. Copy springtail_fdw.control to share/extension dir

4. Build and copy libspringtail_fdw.dylib to lib dir as springtail_fdw.dylib (MAC OSX) or libspringtail_fdw.so (LINUX)
  - for homebrew: /opt/homebrew/opt/postgresql@16/lib/postgresql/
  - for ubuntu: /usr/share/postgresql/16/lib/ (maybe /postgresql)

5a. Create path for springtail tables (e.g., /opt/springtail) and update system.json config file (optional)

 b. Add system.json path to postgresql.conf file:
    springtail_fdw.config_file_path = '/opt/springtail/system.json'

6. In psql execute:

CREATE EXTENSION springtail_fdw;
CREATE SERVER springtail_fdw_server FOREIGN DATA WRAPPER springtail_fdw;

7. Create the foreign tables
  - one can copy data from an existing Postgres schema using copy_schema (optional)
  - setup the foreign tables using the IMPORT FOREIGN SCHEMA command like:

  IMPORT FOREIGN SCHEMA <remote_schema_name>
    FROM SERVER springtail_fdw_server
    INTO <local_schema_name>;

  - remote_schema_name refers to the namespace name in springtail, the local_schema_name refers to the schema in Postgres (FDW)

8. Make sure the xid_mgr_daemon is running and it's xid is >= table's xid
  - can use the -x option to the xid_mgr_daemon
