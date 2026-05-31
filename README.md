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

## Documentation

Full documentation is available at **[docs.springtail.io](https://docs.springtail.io/)**, including:

- **[Quickstart](https://docs.springtail.io/quickstart)** — the fastest way to get a local cluster running for testing.
- **[Local dev environment](https://docs.springtail.io/local-dev-environment)** — build Springtail and run the unit and integration tests in a development container.
- **[Local cluster deployment](https://docs.springtail.io/local-cluster-deployment)** — run a full multi-node deployment locally with Docker Compose.
- **[Production deployment](https://docs.springtail.io/deployment)** — deploy and manage Springtail in production with the [coordinator](python/coordinator/README.md).

## License

This project is licensed under the Elastic License 2.0 (ELv2).

You may use, copy, modify, and distribute this software, subject to the terms of the license.

Limitations include:
- You may not provide this software as a hosted or managed service.