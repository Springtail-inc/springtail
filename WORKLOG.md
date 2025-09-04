# Codebase Analysis Log

## Key Points
- C++20, CMake mono-repo with vcpkg; heavy use of gRPC/Protobuf, Boost, spdlog, redis++, OpenTelemetry, lz4, xxhash, libpq.
- Ingestion path: `pg_repl` decodes WAL/Copy; `pg_log_mgr` orchestrates ingestion, recovery, indexing; `sys_tbl_mgr` manages schema/state.
- Query path: FDW (`pg_fdw`) exposes reads inside Postgres; `proxy` speaks PG wire protocol, routes reads to FDW and writes to primary.
- Coordination: `xid_mgr` manages transaction and xid state; `write_cache` supports indexing and staging.

## Module Overview
- `src/common`: logging (`logging.cc`), config (`properties.cc`), telemetry (`open_telemetry.cc`), Redis cache, DNS, allocator, threading utils. Linked broadly across repo.
- `src/pg_repl`: WAL streaming/decoding (`pg_msg_stream.cc`, `pg_repl_connection.cc`), table copying (`pg_copy_table.cc`), validation; uses `PostgreSQL::PostgreSQL`, `springtail_common`, `springtail_sys_tbl_mgr_client`.
- `src/pg_log_mgr`: ingestion orchestrators (`pg_log_mgr.cc`, `committer.cc`, `indexer.cc`, `pg_log_reader.cc`, recovery); links `springtail_pg`, `storage`, `xid_mgr`, `redis`, `sys_tbl_mgr_client`.
- `src/sys_tbl_mgr`: gRPC service and client for schema/system tables, shared memory cache; depends on `springtail_grpc`, `storage`.
- `src/storage`: core data structures and IO (`btree.cc`, `extent.cc`, `schema.cc`, `io_mgr.cc`, `cache.cc`, `numeric_utils.cc`, `vacuumer.cc`); links `lz4`, `xxhash`, `write_cache`, `xid_mgr_client`.
- `src/xid_mgr`: gRPC service/client for xid log management and subscribers.
- `src/write_cache`: client/server/service for write cache and indexing; ties into gRPC + protobuf.
- `src/pg_fdw`: FDW shim (`pg_fdw.c`, managers, DDL daemon, xid subscriber); dynamic-link flags vary per OS; links `springtail_pg`, `storage`, `sys_tbl_mgr_client`, `xid_mgr(_client)`.
- `src/proxy`: PostgreSQL wire proxy (sessions, parser via `libpg_query`, auth, history cache); links `springtail_common`, `springtail_pg`, OpenSSL.
- `src/grpc`: thin wrappers for gRPC client/server manager.
- `src/proto`: generates C++ and Python stubs for `write_cache`, `xid_manager`, `pg_copy_table`, `sys_tbl_mgr`.

## Data/Control Flow
- WAL/Copy ingestion → `pg_repl` → `pg_log_mgr` (commit/index/recover) → `storage` and `sys_tbl_mgr`.
- Reads: `proxy` routes to FDW-backed views via `pg_fdw` over `storage`/schema caches.
- Writes: `proxy` forwards to primary; `xid_mgr` coordinates transaction visibility across components; `write_cache` supports fast lookup.

## Potential Issues / Improvements
- FDW linking: Linux uses `-Wl,--unresolved-symbols=ignore-all`; macOS uses `-undefined,dynamic_lookup`. Risky for hidden symbol failures; consider explicit symbol exports and `PGXS`-style builds.
- Hardcoded rpath `/home/dev/springtail/shared-lib` in FDW on Linux is brittle; prefer relative rpath (`$ORIGIN/..`) or `CMAKE_INSTALL_RPATH`.
- `proto` copies Python stubs into `python/grpc/...` during C++ builds; cross-language side-effect may confuse build graph. Consider optional copy target or separate pipeline.
- Concurrency: widespread `Boost::thread` and caches; ensure consistent locking in `storage/cache.*` and `sys_tbl_mgr/shm_cache.*` and verify memory ordering with atomics where applicable.
- Configuration: `SPRINGTAIL_PROPERTIES` and env overrides—document precedence; ensure secrets (AWS Secrets Manager usage in `common/aws.cc`) never leak to logs.
- Testing: many unit tests exist; consider adding integration tests around `proxy` read/write routing and FDW error handling.

