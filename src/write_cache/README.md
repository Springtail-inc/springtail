# Write Cache

## Overview

Write Cache is a distributed caching service that stores database extents during transaction processing. It provides high-performance temporary storage for transaction data with automatic memory management and disk spillover capabilities.

## Functional Capabilities

### Extent Management

The write cache manages data extents which are blocks of transaction data associated with specific tables and transactions. Each extent is identified by:
- Database ID
- Table ID
- Postgres transaction ID (XID)
- Log Sequence Number (LSN)

#### Adding Extents
Extents can be added to the cache during active transactions. The system automatically determines whether to store extents in memory or on disk based on current memory utilization.

#### Retrieving Extents
Extents are retrieved by specifying the database, table, and Springtail transaction ID. The system supports pagination for large result sets using a cursor-based mechanism.

### Transaction Management

#### Commit Operations
When a transaction commits, the write cache creates a mapping between the Springtail transaction ID and one or more Postgres transaction IDs. This mapping includes metadata such as the commit timestamp.

#### Abort Operations
Transaction aborts remove all data associated with the specified Postgres transaction ID(s) from the cache, freeing memory and disk resources.

### Table Operations

#### Listing Tables
The write cache can enumerate all tables that contain data for a specific transaction ID. Results are paginated using cursor-based iteration.

#### Dropping Tables
Tables can be removed from the cache along with all associated transaction data. This operation is typically used when tables are deleted from the database.

#### Evicting Tables
Individual tables can be evicted from the cache for a specific transaction ID without affecting other transactions or tables.

### Cache Eviction

#### XID Eviction
Complete eviction of all data associated with a specific Springtail transaction ID. This removes all tables and extents for that transaction from the cache.

#### Table Eviction
Selective eviction of a single table for a specific transaction ID, leaving other tables and transactions intact.

### Memory Management

The write cache implements a two-tier storage strategy with automatic memory management:

#### Memory Watermarks
- **High Watermark**: When memory usage exceeds this threshold, new extents are stored on disk
- **Low Watermark**: When memory usage drops below this level, new extents can be stored in memory again

#### Storage Tiers
- **In-Memory Storage**: Fast access for frequently used data below the high watermark
- **Disk Storage**: Spillover storage for data when memory pressure is high

### Database Operations

#### Database Support
The write cache maintains separate indexes for each database, allowing multi-tenant operation with isolation between databases.

#### Database Removal
Complete removal of all data for a specific database, including all tables, transactions, and extents.

## gRPC Interface

The write cache exposes its functionality through a gRPC service with the following operations:

### Service: WriteCache

#### Ping
**Request**: Empty
**Response**: Empty
**Purpose**: Health check to verify service availability

#### GetExtents
**Request**: GetExtentsRequest
- `db_id`: Database identifier
- `table_id`: Table identifier
- `xid`: Springtail transaction ID
- `cursor`: Pagination cursor for result iteration
- `count`: Maximum number of extents to return

**Response**: GetExtentsResponse
- `table_id`: Table identifier
- `cursor`: Updated cursor for next iteration
- `extents`: Array of extent objects
- `commit_ts`: Postgres commit timestamp in microseconds since January 1, 2000 00:00:00 UTC

**Purpose**: Retrieve extents for a specific table and transaction with pagination support

#### ListTables
**Request**: ListTablesRequest
- `db_id`: Database identifier
- `xid`: Springtail transaction ID
- `cursor`: Pagination cursor for result iteration
- `count`: Maximum number of tables to return

**Response**: ListTablesResponse
- `cursor`: Updated cursor for next iteration
- `table_ids`: Array of table identifiers

**Purpose**: Enumerate tables containing data for a specific transaction

#### EvictTable
**Request**: EvictTableRequest
- `db_id`: Database identifier
- `table_id`: Table identifier
- `xid`: Springtail transaction ID

**Response**: Empty

**Purpose**: Remove a specific table from the cache for a given transaction

#### EvictXid
**Request**: EvictXidRequest
- `db_id`: Database identifier
- `xid`: Springtail transaction ID

**Response**: Empty

**Purpose**: Remove all data for a specific transaction from the cache

### Data Types

#### Extent
Represents a block of transaction data:
- `xid`: Postgres transaction ID
- `xid_seq`: Sequence number within the transaction
- `data`: Binary extent data

## Configuration

The write cache is configured through the system settings JSON file under the `write_cache` section:

### RPC Configuration
- `server_port`: Port number for the gRPC server
- `server_worker_threads`: Number of worker threads for request processing
- `ssl`: Enable/disable SSL/TLS encryption
- `client_connections`: Client connection pool size
- Certificate paths for SSL/TLS when enabled

### Storage Configuration
- `disk_storage_dir`: Directory path for disk-based extent storage

### Memory Configuration
- `memory_high_watermark_bytes`: Memory threshold for switching to disk storage
- `memory_low_watermark_bytes`: Memory threshold for resuming in-memory storage

## Architecture

### Partitioning Strategy
The write cache uses a partitioned index structure where tables are distributed across multiple partitions based on their table ID. This enables parallel processing and reduces contention.

### Storage Hierarchy
1. **In-Memory Index**: Fast lookup structure for active transactions
2. **Memory-Resident Extents**: Hot data stored in RAM
3. **Disk-Backed Extents**: Cold data or overflow stored on persistent storage

### Concurrency Model
The write cache supports concurrent operations through partition-level locking, allowing multiple transactions to read and write different tables simultaneously.
