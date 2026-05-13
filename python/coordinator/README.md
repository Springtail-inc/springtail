# Springtail Coordinator

The coordinator (`coordinator.py`) is the supervisor process that manages the
lifecycle of every springtail service running on a host. One coordinator runs
per host and is responsible for a single service type: `ingestion`, `fdw`, or
`proxy`.

> **Note:** the coordinator is intended as a *reference example* of how to
> deploy Springtail end-to-end (binary install from S3, systemd-managed
> postgres, Redis-driven lifecycle, SNS notifications, etc.). It encodes the
> assumptions of one particular deployment — paths, AWS services, secret
> layout, systemd unit naming, and so on — and will likely need to be
> adjusted (or replaced) to match a different target environment.

The coordinator:

1. Downloads and installs binaries from S3 (production only).
2. Registers the components for its service type with the `Scheduler`.
3. Starts the components in dependency order.
4. Monitors components via PID checks, Redis liveness timeouts, and a Redis
   pub/sub channel, restarting on failure.
5. Reacts to state transitions written to Redis (`coordinator_state` and, for
   FDW, `fdw_state`) so that operators can drive lifecycle changes from
   outside the host.

State and orchestration are coordinated through Redis. The coordinator
reads/writes two state values that drive the four operations described in
this document:

| Key | Field | Type | Values |
| --- | --- | --- | --- |
| `<db_instance_id>:coordinator_state` | `<service>:<instance_key>` | `CoordinatorState` | `startup`, `reloading`, `running`, `reload`, `shutdown`, `dead` |
| `<db_instance_id>:fdw` | `<fdw_id>` (JSON `state` field) | string | `initialize`, `running`, `draining`, `stopped` |

---

## 1. Starting Springtail

A springtail database instance consists of three service tiers, each launched
on its own host(s) by an independent coordinator process:

| Service | Components started (in order) |
| --- | --- |
| `ingestion` | `pg_log_mgr_daemon` |
| `fdw` | `postgres` → `pg_xid_subscriber_daemon` → `pg_ddl_daemon` |
| `proxy` | `proxy` |

### Launch command

The coordinator is launched as:

```bash
python coordinator.py -c config.yaml -s <ingestion|fdw|proxy> [--debug] [--manual] [-f /path/to/postmaster.pid]
```

In production it is normally started by `systemd` as the
`springtail-coordinator` unit; the same command runs interactively from the
shell for development.

Flags:

- `-c, --config-file` — path to the local `config.yaml` (defaults to
  `config.yaml` in the working directory). The YAML supplies
  `system_json_path`, `install_dir`, the `production` flag, and log rotation
  settings.
- `-s, --service` — one of `ingestion`, `fdw`, or `proxy`. May be supplied via
  the `SERVICE_NAME` environment variable instead.
- `--debug` — verbose logging.
- `--manual` — in production mode, skip the loader self-restart so the
  coordinator can be driven from a shell.
- `-f, --postgres-pid-file` — explicit path to the postgres PID file (FDW
  service only).

### Startup sequence

`Coordinator.startup()` (in `coordinator.py`) executes:

1. **Read coordinator state from Redis.** The state field is keyed by
   `<service>:<instance_key>` so each coordinator on the host has its own
   record.
2. **Install binaries (production only).** If state is `STARTUP`, the
   `Production` helper downloads the latest `springtail-*.tgz` from
   `s3://<S3_BUCKET>/packages/`, validates the package's `Config Hash` in
   `INFO.txt` against the value Redis has for the deployment, and rsyncs it
   to `install_dir`. The state is then advanced to `RELOADING` and (unless
   `--manual` was passed) `loader.startup` is invoked to relaunch the
   coordinator from the freshly installed code, after which the current
   process exits. The loader will bring the coordinator back up under the
   new binary set.
3. **Sanity-check paths.** The mount path, log path, and `<install>/bin/system`
   directory are verified to exist.
4. **Stop stragglers.** `stop_daemons` kills any orphaned daemons listed in
   `ALL_DAEMONS` so the coordinator starts from a clean slate.
5. **Build the scheduler and register components.** A `ComponentFactory`
   produces the `Component` objects for the selected service. For `fdw`, the
   coordinator first calls `Production.install_pgfdw` to install the
   `springtail_fdw` extension into the local PostgreSQL (see
   [Host PostgreSQL prerequisites](#host-postgresql-prerequisites) for what
   this assumes about the cluster), then waits for the ingestion service to
   be reachable (pings `XidMgrClient` and `SysTblMgrClient` until both
   respond) before starting postgres and the FDW daemons.
6. **Start components in order.** `Scheduler.start_all` launches each
   component by ascending `startup_order` and waits for each to be running
   before moving on. Once everything is up, the coordinator state is set to
   `RUNNING`.
7. **Enter the monitor loop.** `Scheduler.monitor_timeouts` loops every
   ~5 seconds:
   - Reacts to coordinator-state changes (see "Reload" and "Stopping" below).
   - Tracks per-database state changes and emits SNS notifications.
   - Reads the liveness hash in Redis and flags components whose
     heartbeat is older than `allowed_timeout` (40s by default).
   - Drains the `pubsub:liveness_notify` channel for failure messages from
     the daemons.
   - Calls `Component.is_alive()` on every registered component.
   - On any failure, calls `Scheduler.restart_all` (subject to the
     `MAX_FAILURES` / `FAILURE_WINDOW_THRESHOLD` rate limiter — 5 failures
     within 5 seconds aborts the loop and waits 5 minutes before retrying).
   - For the FDW service, also runs `_process_fdw_state_change` to honor the
     external "remove replica" flow described below.

Once `monitor_timeouts` exits, `shutdown_all` is invoked, the coordinator
state is set to `DEAD`, and the process exits.

### Reloading binaries on a running coordinator

To pick up a new build without manually restarting, set the coordinator state
to `RELOAD`:

```python
props.set_coordinator_state(CoordinatorState.RELOAD)
```

The monitor loop's `_check_coordinator_state` will:

1. Call `shutdown_all` on every registered component.
2. Re-run `install_binaries` (and `install_pgfdw` for the FDW service).
3. Call `restart_all` to bring the components back under the new binaries.
4. Set the state back to `RUNNING`.

---

## 2. Adding Replica Nodes (FDW Hosts)

Springtail "replica nodes" are FDW hosts: instances running the `fdw` service
that expose a PostgreSQL endpoint backed by the springtail foreign data
wrapper. Each FDW host is identified by a unique `FDW_ID` and has its own
config record under the Redis hash `<db_instance_id>:fdw`.

The coordinator does not provision new hosts itself — that is the
responsibility of the controller / infrastructure layer. From the
coordinator's perspective, adding a replica is simply "stand up a new host
and start an FDW coordinator on it." See
[Host PostgreSQL prerequisites](#host-postgresql-prerequisites) first for
what each FDW host must have in place before the coordinator can run; the
flow below assumes those prerequisites are met.

### Host PostgreSQL prerequisites

The FDW coordinator does not install or initialize PostgreSQL — it expects
a specific cluster to already be running on the host. The local-cluster
ansible at `local-cluster/opt/springtail/helpers/customize-pg.yml` is the
canonical reference for how that cluster is laid down; the requirements
below summarize what the coordinator itself actually depends on.

**1. The custom (patched) PostgreSQL 16 build.** Springtail does not run on
stock PostgreSQL. The FDW host must run the patched PG 16 fork (currently
`postgresql-16.9`, distributed as `springtail-pg.tar.gz`), which adds RLS
support for foreign tables that the `springtail_fdw` extension relies on.
The reference build is in `docker/ansible/roles/custom-pg/tasks/main.yml`:
it downloads the tarball from `custom_pg_package_url`, configures with
`--prefix=/usr/lib/postgresql/16` (with `--bindir` / `--datadir` /
`--libdir` under that prefix), and runs `make install-world`.

`install_pgfdw` and `PostgresComponent` discover the install layout at
runtime via `pg_config --sharedir`, `--pkglibdir`, `--bindir`, and
`--version`, so **the `pg_config` first on the coordinator's PATH must
resolve to the custom build**. If a stock `pg_config` shadows it, the FDW
extension files will be copied into the wrong cluster.

**2. Cluster naming driven by the FDW superuser.** The data directory,
systemd unit, OS owner, and environment file are all named after the
`fdw_superuser` username pulled from AWS Secrets Manager (secret
`sk/{org_id}/{account_id}/aws/dbi/{db_instance_id}/primary_db_password`,
read via `Properties.get_role(DB_USER_ROLE_FDW)`). Throughout this section
`{fdw_user}` is that username and `{pg_version}` is the PG major version
returned by `pg_config --version` (currently `16`):

| What | Path / name |
| --- | --- |
| Data directory | `/var/lib/postgresql/{pg_version}/{fdw_user}` |
| `pg_hba.conf` | `/var/lib/postgresql/{pg_version}/{fdw_user}/pg_hba.conf` |
| `postmaster.pid` | `/var/lib/postgresql/{pg_version}/{fdw_user}/postmaster.pid` (overridable via the `-f` flag) |
| Environment file | `/etc/postgresql/{pg_version}/{fdw_user}/environment` |
| systemd unit | `postgresql-{fdw_user}.service` |

The OS user `{fdw_user}` must exist and have passwordless sudo — the
coordinator runs `sudo cp`, `sudo systemctl start/stop ...`, and
`sudo -u {fdw_user} psql ...` for liveness and connection-count probes.

The maintenance database that `is_alive` / `get_connection_count` connect
to comes from `PGDATABASE` (defaults to `__springtail` in the env-driven
path, `postgres` when properties are loaded from a `config.yaml`) and must
already exist in the cluster.

**3. What `install_pgfdw` mutates on every coordinator start/reload.** It
does not create the cluster, but it does modify it on each invocation (see
`python/coordinator/production.py:201`):

- Copies the FDW extension into the cluster's `sharedir/extension`
  (`springtail_fdw--1.0.sql`, `springtail_fdw.control`) and the shared
  library into `pkglibdir` as `springtail_fdw.so`.
- Removes any stale `libspringtail_pgext.so` from the springtail install's
  `shared-lib` directory.
- Rewrites the local-auth line in `pg_hba.conf` to `scram-sha-256`.
- Writes the springtail runtime env vars (the `ENV_VARS` list in
  `production.py` — Redis, AWS, mount, FDW, `LD_LIBRARY_PATH`) into the
  postgres environment file so the systemd unit picks them up.
- Stops postgres, so the scheduler can start it cleanly under the refreshed
  environment in the next step of the startup sequence.

### Flow

1. **Provision an FDW config record.** Add the new FDW to
   `system.json` under `fdws` (or insert it directly into Redis at
   `<db_instance_id>:fdw` and `<db_instance_id>:fdw_ids`). Newly added FDW
   configs are written with `state = "initialize"` (see
   `Properties._load_redis`).
2. **Provision the host.** Bring up an EC2 instance (or equivalent) with
   the custom PostgreSQL cluster configured per
   [Host PostgreSQL prerequisites](#host-postgresql-prerequisites) and the
   springtail environment variables set. The coordinator requires:
   - `SERVICE_NAME=fdw`
   - `FDW_ID=<the new fdw id>`
   - `INSTANCE_KEY` (used as part of the coordinator-state record key)
   - The standard Redis / AWS / mount-path env vars
     (`REDIS_HOSTNAME`, `REDIS_PORT`, `REDIS_USER`, `REDIS_PASSWORD`,
     `REDIS_USER_DATABASE_ID`, `REDIS_CONFIG_DATABASE_ID`,
     `ORGANIZATION_ID`, `ACCOUNT_ID`, `DATABASE_INSTANCE_ID`,
     `LUSTRE_DNS_NAME`, `LUSTRE_MOUNT_NAME`, `MOUNT_POINT`,
     `SNS_TOPIC_ARN`, `AWS_REGION`).
3. **Start the coordinator on the new host.** Either via `systemctl start
   springtail-coordinator` (production) or directly:

   ```bash
   python coordinator.py -c config.yaml -s fdw
   ```

   The coordinator will run the standard FDW startup sequence:

   - Download the binaries from S3 and install them locally
     (`Production.install_binaries`).
   - Install the `springtail_fdw` extension into the host's PostgreSQL
     and rewrite `pg_hba.conf` / the postgres environment file
     (`Production.install_pgfdw`).
   - Wait for the ingestion service to be reachable.
   - Start `postgres`, `pg_xid_subscriber_daemon`, and `pg_ddl_daemon`.
   - Set the coordinator state to `RUNNING`.

   The FDW daemons themselves transition the FDW config's `state` field
   away from `initialize` (toward `running`) once they have synchronized
   their state.
4. **Route traffic.** Once the new FDW is healthy, the proxy can begin
   routing connections to it. The proxy reads its target list from Redis,
   so no coordinator-side action is needed beyond the new FDW reaching
   `running`.

There is no "add replica" RPC inside the coordinator: each FDW is added by
spinning up a new host and pointing a new coordinator at the right
`FDW_ID`.

---

## 3. Removing Replica Nodes (Draining an FDW Host)

Removal is the case the coordinator itself implements explicitly, in
`Scheduler._process_fdw_state_change` (called every monitor iteration when
the service type is `fdw`). The flow is operator-driven via the FDW state
field:

1. **Operator marks the FDW as draining.** Externally (controller, admin
   tool, or `Properties.set_fdw_state('draining')`), set the FDW config's
   `state` to `draining` in Redis at `<db_instance_id>:fdw[<fdw_id>]`.
2. **Coordinator detects the draining state.** On its next monitor tick the
   FDW coordinator reads the FDW config (`get_fdw_config(nocache=True)`),
   sees `state == 'draining'`, and enters the drain path.
3. **Wait for connections to drain.** The coordinator polls
   `PostgresComponent.get_connection_count()` (which runs
   `select count(client_port) from pg_stat_activity where client_port != -1`)
   every 5 seconds and only proceeds once it returns 0. While waiting the
   proxy is responsible for steering new connections away from this FDW.
4. **Shut down all components.** `Scheduler.shutdown_all` stops `pg_ddl_daemon`,
   `pg_xid_subscriber_daemon`, and `postgres` in reverse startup order.
5. **Wait for `stopped`.** The coordinator calls
   `Properties.wait_for_fdw_state('stopped', 30)`. The expectation is that
   the operator (or controller) advances the FDW state to `stopped` once
   they have confirmed the host is quiesced. If the wait times out, the
   coordinator forces the state to `stopped` itself.
6. **Clean up Redis state.** The coordinator removes everything in Redis
   that this FDW owned:
   - `DELETE <db_instance_id>:queue:ddl:fdw:<fdw_id>` (DDL work queue).
   - `HDEL` matching members of `<db_instance_id>:hash:ddl:fdw` whose key
     ends in `:<fdw_id>`.
   - `HDEL` matching members of `<db_instance_id>:fdw_min_xids` whose key
     starts with `<fdw_id>:`.
   - `SREM` matching members of `<db_instance_id>:fdw_pids` whose value
     starts with `<fdw_id>:`.
7. **Stay idle.** The coordinator sets `services_stopped = True`. The
   monitor loop continues running but skips its component checks
   (`IDLE_SLEEP_INTERVAL = 5s`), so the host can be safely terminated by
   the controller. To fully stop the coordinator, follow the "Stopping
   Springtail" flow below.

The FDW config record itself is left in Redis — the controller is
responsible for removing it from `system.json`/`<db_instance_id>:fdw` once
the host is gone if the FDW should not be re-used.

---

## 4. Stopping Springtail

There are two ways to stop a coordinator and its components, depending on
whether the stop should be initiated locally (process signal) or remotely
(Redis state).

### A. Local stop (signals)

The coordinator installs handlers for `SIGINT` and `SIGTERM`
(`make_signal_handler` in `coordinator.py`). Either signal:

1. Sets `coordinator.shutdown_event`, which causes the startup wait loops
   (e.g. `_wait_for_ingestion`) to exit early.
2. Calls `Scheduler.shutdown()`, which sets the scheduler's
   `shutdown_event` and breaks `monitor_timeouts` out of its loop.
3. Once the loop exits, `Scheduler.shutdown_all` shuts down components in
   reverse startup order (`shutdown` first, then `kill` if the graceful
   shutdown times out).
4. The Redis pub/sub subscription and connection are closed, and the
   coordinator state is set to `DEAD`.
5. After `Coordinator.startup()` returns, `coordinator.py`'s `__main__`
   calls `stop_daemons` for `ALL_DAEMONS` to be sure no orphans are left,
   then sends an SNS `shutdown` notification (production only).

In production, this is normally done with:

```bash
sudo systemctl stop springtail-coordinator
```

### B. Remote stop (coordinator state)

To stop a running coordinator from outside the host, write `SHUTDOWN` to
Redis:

```python
props.set_coordinator_state(CoordinatorState.SHUTDOWN)
```

On the next monitor tick `_check_coordinator_state` matches `SHUTDOWN` and
calls `Scheduler.shutdown()`, which then follows the same teardown path as
the signal-driven case (reverse-order component shutdown, pub/sub close,
state set to `DEAD`).

### C. Stopping the whole instance

To stop springtail across all hosts:

1. **Drain proxies first** by stopping their coordinators (signal or
   Redis-state). With proxies down, no new client traffic can reach FDW
   hosts.
2. **Drain each FDW host** using the "Removing Replica Nodes" flow
   (`set_fdw_state('draining')`), which gracefully waits for in-flight
   connections, shuts down `pg_ddl_daemon`, `pg_xid_subscriber_daemon`, and
   `postgres`, and cleans up the per-FDW Redis state. Then stop each FDW
   coordinator (signal or `coordinator_state = SHUTDOWN`).
3. **Stop the ingestion coordinator** (signal or
   `coordinator_state = SHUTDOWN`); this brings down `pg_log_mgr_daemon`.

When every coordinator has reached the `DEAD` state in
`<db_instance_id>:coordinator_state`, the instance is fully stopped.
