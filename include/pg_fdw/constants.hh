#pragma once

#define SPRINGTAIL_STARTUP_COST 100 ///< Default startup cost is 100, added to tuple cost
                                    ///
// cost multipliers
#define SPRINGTAIL_PRIMARY_COST 1 ///< Scan and lookup cost for primary indexes
#define SPRINGTAIL_SECONDARY_SCAN_COST 7 ///< Scan cost for secondary indexes
#define SPRINGTAIL_SECONDARY_LOOKUP_COST 2 ///< Lookup cost for secondary indexes

#define SPRINGTAIL_FDW_EXTENSION "springtail_fdw"               ///< Name of FDW extension
#define SPRINGTAIL_FDW_SERVER_NAME "springtail_fdw_server"      ///< Name of foreign server on import schema
#define SPRINGTAIL_FDW_CATALOG_SCHEMA "__pg_springtail_catalog" ///< Name of catalog schema

#define SPRINGTAIL_FDW_DB_ID_OPTION "db_id"                  ///< Option name for database id
#define SPRINGTAIL_FDW_DB_NAME_OPTION "db_name"              ///< Option name for database name
#define SPRINGTAIL_FDW_SCHEMA_XID_OPTION "schema_xid"        ///< Option name for schema xid
