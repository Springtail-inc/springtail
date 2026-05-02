# Springtail

Springtail is a database system that scales read performance for existing PostgreSQL databases without requiring a migration or changes to your application.

It achieves this by replicating data from your existing PostgreSQL instance and routing read queries to on-demand replica nodes. This allows you to add replicas for periods of high traffic and remove them when they are no longer needed, improving read performance while reducing infrastructure costs.

The system is composed of several cooperating daemons (proxy, log manager, write cache, system table manager, XID manager) that communicate over gRPC, with Redis used for configuration and state management.

## Benefits

Springtail provides significant advantages over traditional Postgres read replicas:

- **Instant scale-up:** Springtail separates storage from compute, allowing additional replicas to spin up in seconds without requiring a new copy of the data.
- **Cost-efficiency:** Only pay for what you use — reduce compute resources when they're not needed, saving on peak provisioning costs.
- **Built-in optimization:** Springtail's proxy handles load balancing and connection pooling, so you don't need to manage these services separately.
- **No downtime:** Bring replicas online quickly without the downtime of snapshot creation or synchronization delays.

## Key features

Springtail enhances performance and efficiency through several key capabilities:

- **Scalable compute:** Increase or decrease compute capacity as your workload changes, reducing unnecessary overhead.
- **Scalable storage:** Independently scales storage from compute, allowing faster setup and efficient data access.
- **Optimized ingest pipeline:** Reduces replication lag and ensures that reads receive the most up-to-date snapshot of the data.
- **Built-in proxy:** Automatically manages load balancing and connection pooling, simplifying replica management.
- **Custom storage format:** Optimizes storage for fast primary key access patterns, reducing latency on common database operations.

## Use cases

Springtail replicas are well-suited for traditional read replica workloads:

- **Nightly batch jobs:** Efficiently process recurring large-scale operations without impacting your primary database.
- **ETL jobs:** Seamlessly handle extract, transform, and load processes with scalable compute.
- **Read-heavy operations:** Handle large queries, such as SaaS dashboards or table scans, without overloading your primary instance.

Any read-only transaction that might cause contention on your primary database is an ideal candidate for Springtail.

## Prerequisites

- Docker and Docker Compose
- Python 3
- AWS CLI (for local cluster only)

## Quick Start: Local Development Environment

The local dev environment runs a PostgreSQL 16 instance and a development container with all build tooling pre-installed.

### 1. Build the base Docker image

From the repository root:

```shell
cd docker
./docker_base.sh
```

This builds the `springtail:base` image from `Dockerfile.base`, which includes a patched PostgreSQL 16 (built from source with RLS support for foreign tables), Redis, the C++ toolchain, and all Ansible-provisioned dependencies.

### 2. Start the dev environment

```shell
export SPRINGTAIL_SRC=/path/to/springtail   # absolute path to your repo checkout
docker compose up -d
```

This starts two services:

| Service    | Container         | Description                      | Host Port |
|------------|-------------------|----------------------------------|-----------|
| `postgres` | `pg16`            | PostgreSQL 16 with logical replication enabled | 5432 |
| `dev`      | `dev-springtail`  | Development container with build tools         | 2222 (SSH) |

The dev container mounts your source tree at `/home/dev/springtail` and starts PostgreSQL, Redis, and SSH automatically via its entrypoint.

### 3. Build Springtail inside the container

Shell into the dev container and run the debug build:

```shell
docker exec -it dev-springtail bash

# Inside the container:
cd ~/springtail
./vcpkg.sh          # one-time: install C++ dependencies
./debug.sh          # build debug binaries into ./debug/
```

### 4. Run the unit tests (C++ / CTest)

```shell
cd ~/springtail/debug
make build_tests
ctest
```

Or build and run in one step:

```shell
cmake --build debug --target check
```

The `check` target kills any running Springtail processes, installs SQL triggers, builds the tests, and runs them via CTest.

### 5. Run the integration tests

The integration test runner is a Python script that exercises Springtail end-to-end against a real PostgreSQL instance. It must be run from its own directory:

```shell
cd ~/springtail/python/testing
python3 test_runner.py
```

This runs the **default** test configuration, which includes the test sets `basic`, `framework`, `preload`, `enum_bits`, `complex`, `numeric`, `query_benchmark`, and `recovery` (with various overlay configurations).

#### Common test_runner.py options

```shell
# Run the default configuration (same as no arguments)
python3 test_runner.py

# Run a specific named configuration (e.g., nightly, github_ci_p1)
python3 test_runner.py -c nightly

# Run a single test set
python3 test_runner.py basic

# Run specific test cases within a test set
python3 test_runner.py basic test_create.sql test_insert.sql

# Run with a specific overlay
python3 test_runner.py -o small_log_rotate recovery

# Skip downloading test data from S3 (useful offline or in CI)
python3 test_runner.py --skip-downloads

# Output JUnit XML report
python3 test_runner.py -j results.xml
```

Available test sets: `basic`, `complex`, `enum_bits`, `framework`, `include_schema`, `large_data`, `live_startup`, `numeric`, `policy_roles`, `preload`, `query_benchmark`, `recovery`, `text_tables`.

Available overlays: `small_log_rotate`, `small_log_rotate_with_streaming`, `small_cache_size`, `streaming_postgres_config`, `integration_test_config`, `include_schema_config`.

### 6. Tear down

```shell
cd /path/to/springtail/docker
docker compose down -v
```

---

## Quick Start: Local Cluster (Multi-Node)

The local cluster simulates a full multi-node Springtail deployment using Docker Compose. It runs a primary database, Redis, a mock AWS environment, and the full set of Springtail services (proxy, ingestion, FDW nodes, and controller).

All cluster commands are run from the `local-cluster/` directory:

```shell
cd local-cluster
```

### 1. Build the required Docker images

From the repository root, build the base service image if it doesn't already exist:

```shell
docker build -t local-cluster-img:latest -f docker/Dockerfile.local-cluster .
```

The `cluster up` command will also build the controller image (`local-cluster-controller:latest`) and custom PostgreSQL image (`postgres-custom:16`) automatically if needed.

### 2. Build a Springtail package

```shell
./cluster build-package /tmp/springtail-packages
```

This runs the full build and packaging process inside the base image and outputs a tarball named `springtail-<date>-<gitsha>.tar.gz` into the specified directory.

### 3. Start the cluster

```shell
./cluster up /tmp/springtail-packages/springtail-<date>-<gitsha>.tar.gz
```

Optionally disable SSL for inter-service connections:

```shell
./cluster up /path/to/package.tar.gz --disable-ssl
```

Startup takes 1-2 minutes. The cluster will:
1. Start the mock AWS service (Moto), Redis, and primary PostgreSQL
2. Upload the package to mock S3
3. Run the bootstrap container to configure secrets, Redis auth, and shared environment
4. Launch the proxy, ingestion, and FDW services

### 4. Check cluster status

```shell
./cluster status
```

### 5. Interact with services

```shell
# Shell into a service
./cluster sh proxy
./cluster sh ingestion
./cluster sh fdw1
./cluster sh controller

# View logs
./cluster logs proxy
./cluster logs ingestion

# Restart a service
./cluster restart proxy

# List all services
./cluster ls
```

### 6. Host ports

Services are exposed on the host at these ports:

| Service        | Host Port |
|----------------|-----------|
| Primary DB     | 15432     |
| Redis          | 16379     |
| Proxy          | 55432     |
| FDW 1          | 45432     |
| FDW 2          | 45433     |
| AWS Mock (Moto)| 29999     |
| Controller API | 19824     |

Connect to the proxy from the host with any PostgreSQL client:

```shell
psql -h localhost -p 55432 -U postgres
```

### 7. Tear down the cluster

```shell
# Stop Springtail services but keep the primary DB and dependencies
./cluster down

# Stop a specific service
./cluster down proxy

# Stop everything and remove all data volumes and networks
./cluster down all
```

## License

This project is licensed under the Elastic License 2.0 (ELv2).

You may use, copy, modify, and distribute this software, subject to the terms of the license.

Limitations include:
- You may not provide this software as a hosted or managed service.