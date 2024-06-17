To install FDW:

1. Find share extension dir:
  - pg_config --sharedir
  - for homebrew: /opt/homebrew/opt/postgresql@16/share/postgresql@16/extension
  - for ubuntu: /usr/share/postgresql/16/extension/

2. Copy springtail_fdw--1.0.sql to share/extension dir

3. Copy springtail_fdw.control to share/extension dir

4. Build and copy libspringtail_fdw.dylib to lib dir as springtail_fdw.dylib (MAC OSX) or libspringtail_fdw.so (LINUX)
  - for homebrew: /opt/homebrew/opt/postgresql@16/lib/postgresql/
  - for ubuntu: /usr/share/postgresql/16/lib/ (maybe postgresql)

5. In psql execute:

CREATE EXTENSION springtail_fdw;
CREATE SERVER springtail_fdw_server FOREIGN DATA WRAPPER springtail_fdw;

6. Create the foreign tables like (where tid is the table ID).
 - can run gen_schema_sql to copy an existing schema from a running
   Postgres instance.  This will output a bunch of sql commands to
   create the foreign tables, like below:

CREATE FOREIGN TABLE some_table ( val integer )
  SERVER springtail_fdw_server
  OPTIONS (tid '1234');

7. Make sure the xid_mgr_daemon is running and it's xid is >= table's xid
  - can use the -x option to the xid_mgr_daemon
