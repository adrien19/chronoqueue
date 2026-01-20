# ADR-002: Migration to Postgres as Primary Storage

**Status:** Accepted
**Date:** 2025-01-18
**Authors:** ChronoQueue Team - @adrien19
**Related Issues:** refactor/migrate_to_postgres_storage
**Supersedes:** ADR-001 (Redis Streams Architecture)

## Context

Following the migration to Redis Streams (ADR-001), ChronoQueue operated successfully with improved atomicity, consumer tracking, and horizontal scalability. However, as the system development matured and workloads increased, fundamental limitations of Redis as a primary storage backend for a queue orchestration system became apparent.

### Critical Challenges with Redis for Queue Systems

#### 1. **Transactional Integrity and Data Consistency**

Redis lacks multi-operation ACID transactions across different data structures:

- **No Cross-Structure Atomicity**: Operations spanning multiple streams, sorted sets, and hashes cannot be grouped into atomic transactions
- **Lua Script Limitations**: While Lua scripts provide atomicity, they:
  - Cannot call blocking commands
  - Have limited debugging capabilities
  - Create maintenance burden (complex business logic in scripts)
  - Don't support cross-key transactions in Redis Cluster
- **Race Condition Risks**: Despite Redis Streams improvements, coordinating queue metadata updates, message state changes, and schedule operations still required careful orchestration

Example failure scenario:

```
1. Consumer claims message from stream
2. Updates processing state counter in hash
3. Connection drops before XACK
→ Message stuck in PEL, counter inconsistent, manual intervention required
```

#### 2. **Durability and Persistence Concerns**

Redis prioritizes performance over durability:

- **AOF/RDB Trade-offs**:
  - `appendfsync always`: Severe performance degradation
  - `appendfsync everysec`: Risk of 1-second data loss
  - RDB snapshots: Point-in-time backups with potential data loss between snapshots
- **Memory Constraints**: All data must fit in RAM, limiting queue depth and retention policies
- **Crash Recovery**: Reloading large datasets from AOF/RDB on restart can take minutes to hours
- **No Write-Ahead Logging**: Unlike PostgreSQL's WAL, Redis persistence mechanisms aren't designed for high write throughput with durability guarantees

For queue systems handling critical business workflows (payments, notifications, orchestration), data loss is unacceptable.

#### 3. **Query Flexibility and Observability**

Redis excels at key-value operations but struggles with complex queries:

- **Limited Query Patterns**: No SQL-like filtering, aggregation, or joins
- **Manual Indexing**: Secondary indexes require maintaining additional sorted sets/sets
- **Observability Gaps**:
  - Cannot query "messages enqueued in last hour with priority > 5 that failed validation"
  - No built-in support for time-series analysis of queue metrics
  - Difficult to debug message processing history
- **Audit Trail Limitations**: Tracking message lifecycle events requires custom instrumentation

#### 4. **Schema Evolution and Data Migration**

- **No Schema Versioning**: Changes to protobuf serialization or data structure require custom migration logic
- **Backward Compatibility Burden**: Mixing old/new message formats in streams creates deserialization complexity
- **Rolling Updates**: Difficult to coordinate schema changes across distributed Redis instances

#### 5. **Operational Complexity**

- **Redis Cluster Management**: Sharding, slot migration, network partitions, split-brain scenarios
- **Memory Management**: Eviction policies, fragmentation, OOM scenarios
- **Monitoring**: Custom tooling required for queue-specific metrics
- **Backup Strategy**: Point-in-time recovery requires external tools (Redis Sentinel, custom snapshotting)

### Why Not Optimize Redis Further?

We considered several Redis-based improvements:

1. **Sentinel + AOF with `appendfsync always`**: Unacceptable performance penalty (70% throughput reduction)
2. **Redis Cluster with Raft-based replication**: Still in-memory, no true durability
3. **KeyDB/Dragonfly**: Different implementations, same fundamental limitations
4. **Redis Streams + PostgreSQL audit log**: Complexity of dual storage without full benefits

**Conclusion**: Redis is an excellent cache and pub/sub system, but fundamentally misaligned with queue system requirements.

## Decision

We will migrate ChronoQueue's primary storage from Redis to **PostgreSQL**, with **SQLite** as a first-class option for development and single-node deployments.

### Rationale for PostgreSQL

1. **ACID Transactions**: True multi-row, multi-table transactions with serializable isolation
2. **Durability Guarantee**: Write-Ahead Logging (WAL) ensures no data loss on crash
3. **Rich Query Capabilities**: SQL enables complex filtering, aggregation, analytics
4. **Proven Reliability**: 30+ years of production use, well-understood failure modes
5. **Operational Maturity**: Extensive tooling for backup, replication, monitoring
6. **Schema Management**: Native support for migrations, versioning, constraints
7. **Performance**: Modern PostgreSQL rivals Redis for many workloads when properly indexed
8. **Cost Efficiency**: No memory constraints—pay for disk, not RAM

### Rationale for SQLite (Optional)

1. **Zero Configuration**: Single file, no server process
2. **Development Velocity**: Instant local setup without Docker dependencies
3. **Embedded Deployments**: IoT, edge computing, single-tenant SaaS
4. **Testing**: Blazing-fast test suite with in-memory databases
5. **Production Viable**: Powers millions of mobile apps, small-scale deployments

## Architecture Design

### Repository Structure

ChronoQueue's new storage layer follows a **three-tier architecture**:

```
pkg/repository/
├── storage.go           # High-level Storage interface (gRPC-aligned)
├── backend.go           # Low-level BackendStorage interface (simple signatures)
├── implementation.go    # Storage implementation (wraps BackendStorage)
│
├── sql/                 # Shared SQL logic
│   ├── base.go         # BaseSQL - common SQL operations
│   ├── queries.go      # Parameterized SQL queries
│   ├── state.go        # Message state machine
│   ├── lease.go        # Lease calculation logic
│   ├── transactions.go # Transaction helpers
│   ├── schema/         # Schema versioning
│   │   └── schema.go   # Generic SchemaManager
│   ├── background/     # Background services
│   │   ├── reclaim.go  # Expired lease reclamation
│   │   ├── scheduler.go # Scheduled message dispatcher
│   │   └── cleanup.go  # Retention policy cleanup
│   └── priority/       # Priority queue logic
│       └── priority.go
│
├── postgres/            # PostgreSQL implementation
│   ├── storage.go      # postgres.Storage (implements BackendStorage)
│   ├── connection.go   # Connection pool management
│   ├── schema.go       # Postgres-specific schema & migrations
│   ├── queues.go       # Queue operations
│   ├── messages.go     # Message CRUD
│   ├── schedules.go    # Schedule management
│   ├── reclaim.go      # Lease reclaim (delegates to sql/background)
│   ├── dlq.go          # Dead Letter Queue
│   └── dialect.go      # Postgres SQL dialect specifics
│
└── sqlite/              # SQLite implementation
    ├── storage.go      # sqlite.Storage (implements BackendStorage)
    ├── connection.go   # SQLite connection setup (WAL mode, pragmas)
    ├── schema.go       # SQLite-specific schema & migrations
    ├── queues.go       # Queue operations
    ├── messages.go     # Message CRUD
    ├── schedules.go    # Schedule management
    ├── reclaim.go      # Lease reclaim (delegates to sql/background)
    ├── dlq.go          # Dead Letter Queue
    └── dialect.go      # SQLite SQL dialect specifics
```

### Three-Tier Abstraction

1. **Storage Interface** (`storage.go`)
   - gRPC-aligned method signatures
   - Takes `*queueservicepb.CreateQueueRequest`, returns `*queueservicepb.CreateQueueResponse`
   - Consumed by `pkg/chronoqueue/server.go`

2. **BackendStorage Interface** (`backend.go`)
   - Simple, database-friendly signatures
   - Takes `*queuepb.Queue`, returns `error`
   - Implemented by `postgres.Storage` and `sqlite.Storage`

3. **Implementation** (`implementation.go`)
   - Bridges Storage ↔ BackendStorage
   - Handles validation, error wrapping, protobuf conversion
   - Business logic (priority calculation, lease expiration)

### Database Schema

#### Core Tables

```sql
-- Queues: Stores queue metadata and configuration
CREATE TABLE cq_queues (
    name TEXT PRIMARY KEY,
    metadata_pb BYTEA NOT NULL,           -- Protobuf-serialized QueueMetadata
    state_counts JSONB DEFAULT '{}',      -- {PENDING: 10, RUNNING: 5, ...}
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);

-- Messages: All message states in single table
CREATE TABLE cq_messages (
    id TEXT PRIMARY KEY,
    queue_name TEXT NOT NULL,
    metadata_pb BYTEA NOT NULL,           -- Full Message protobuf
    state INTEGER NOT NULL,               -- PENDING=0, RUNNING=1, COMPLETED=2, ...
    priority INTEGER NOT NULL DEFAULT 5,
    scheduled_at BIGINT NOT NULL,         -- Unix milliseconds
    claimed_at BIGINT,                    -- When consumer claimed message
    lease_expires_at BIGINT,              -- When lease expires
    last_heartbeat_at BIGINT,             -- Last heartbeat timestamp
    attempt_count INTEGER NOT NULL DEFAULT 0,
    worker_id TEXT,                       -- ID of consumer processing message
    attempt_id TEXT,                      -- Unique attempt identifier
    completed_at BIGINT,                  -- Completion timestamp (for retention)
    deleted_at BIGINT,                    -- Soft delete timestamp
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    
    FOREIGN KEY (queue_name) REFERENCES cq_queues(name) ON DELETE CASCADE
);

-- Critical indexes for performance
CREATE INDEX idx_messages_claim ON cq_messages(queue_name, state, priority DESC, scheduled_at)
    WHERE deleted_at IS NULL;
CREATE INDEX idx_messages_reclaim ON cq_messages(state, lease_expires_at)
    WHERE deleted_at IS NULL AND state = 1;  -- RUNNING state
CREATE INDEX idx_messages_cleanup ON cq_messages(deleted_at)
    WHERE deleted_at IS NOT NULL;
CREATE INDEX idx_messages_heartbeat ON cq_messages(queue_name, id, attempt_id)
    WHERE deleted_at IS NULL AND state = 1;

-- Schedules: Cron and calendar-based message scheduling
CREATE TABLE cq_schedules (
    id TEXT PRIMARY KEY,
    queue_name TEXT NOT NULL,
    metadata_pb BYTEA NOT NULL,
    state INTEGER NOT NULL,               -- SCHEDULED=0, PAUSED=1, DISABLED=2
    cron_schedule TEXT,                   -- Cron expression (if applicable)
    next_run BIGINT,                      -- Next execution timestamp
    last_run BIGINT,                      -- Last execution timestamp
    execution_count BIGINT NOT NULL DEFAULT 0,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    
    FOREIGN KEY (queue_name) REFERENCES cq_queues(name) ON DELETE CASCADE
);

CREATE INDEX idx_schedules_state_next_run ON cq_schedules(state, next_run);

-- Schedule History: Audit log of schedule executions
CREATE TABLE cq_schedule_history (
    id SERIAL PRIMARY KEY,
    schedule_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    executed_at BIGINT NOT NULL,
    success BOOLEAN NOT NULL,
    error_message TEXT,
    
    FOREIGN KEY (schedule_id) REFERENCES cq_schedules(id) ON DELETE CASCADE
);

CREATE INDEX idx_schedule_history_schedule ON cq_schedule_history(schedule_id, executed_at DESC);

-- Dead Letter Queue: Failed messages
CREATE TABLE cq_dlq (
    id SERIAL PRIMARY KEY,
    queue_name TEXT NOT NULL,
    message_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    metadata_pb BYTEA NOT NULL,
    created_at BIGINT NOT NULL,
    
    FOREIGN KEY (queue_name) REFERENCES cq_queues(name) ON DELETE CASCADE
);

CREATE INDEX idx_dlq_queue ON cq_dlq(queue_name, created_at DESC);

-- Schema Version: Migration tracking
CREATE TABLE cq_schema_version (
    version INTEGER PRIMARY KEY,
    description TEXT NOT NULL,
    applied_at TIMESTAMP NOT NULL DEFAULT NOW()
);
```

### Key Design Decisions

#### 1. Soft Deletes with Retention Policies

Messages aren't immediately deleted on acknowledgment:

- **completed_at**: Timestamp when message completed successfully
- **deleted_at**: Timestamp when message marked for deletion (soft delete)
- **Retention Modes**:
  - `DELETE_IMMEDIATELY`: Set `deleted_at` on ACK
  - `RETAIN_DURATION(seconds)`: Set `deleted_at = completed_at + retention_seconds`
  - `RETAIN_FOREVER`: Never set `deleted_at`

Background cleanup service periodically hard-deletes messages where `deleted_at < now()`.

Benefits:

- Audit trails for debugging
- Replayable message history
- Compliance (GDPR right to be forgotten via cleanup)

#### 2. Single Messages Table for All States

Unlike Redis's separate streams per priority, all messages live in `cq_messages`:

- **Simplified Queries**: Single table scan for statistics
- **Atomic State Transitions**: `UPDATE cq_messages SET state = $1 WHERE id = $2 AND state = $3`
- **Index-Driven Performance**: Indexes ensure fast claim/reclaim operations

#### 3. Claim-Based Message Retrieval

```sql
-- Atomic message claim with priority and schedule time
UPDATE cq_messages 
SET 
    state = 1,  -- RUNNING
    claimed_at = $now,
    lease_expires_at = $now + $lease_duration,
    worker_id = $worker_id,
    attempt_id = $attempt_id,
    attempt_count = attempt_count + 1,
    updated_at = $now
WHERE id = (
    SELECT id FROM cq_messages
    WHERE queue_name = $queue_name
      AND state = 0  -- PENDING
      AND scheduled_at <= $now
      AND deleted_at IS NULL
    ORDER BY priority DESC, scheduled_at ASC
    LIMIT 1
    FOR UPDATE SKIP LOCKED  -- Prevents concurrent claims
)
RETURNING *;
```

**`FOR UPDATE SKIP LOCKED`**: PostgreSQL's secret weapon—prevents lock contention by skipping locked rows.

#### 4. Background Services

Three background goroutines handle asynchronous operations:

1. **Reclaim Service** (`sql/background/reclaim.go`)
   - Scans for `lease_expires_at < now() AND state = RUNNING`
   - Requeues if `attempt_count < max_attempts`
   - Moves to DLQ if max attempts exceeded
   - Runs every 5-10 seconds

2. **Scheduler Service** (`sql/background/scheduler.go`)
   - Polls `cq_schedules` where `state = SCHEDULED AND next_run <= now()`
   - Enqueues message to target queue
   - Updates `last_run`, `next_run`, `execution_count`
   - Records execution in `cq_schedule_history`

3. **Cleanup Service** (`sql/background/cleanup.go`)
   - Deletes messages where `deleted_at IS NOT NULL AND deleted_at < now()`
   - Honors queue retention policies
   - Runs every 1-6 hours (configurable)

#### 5. Schema Migrations

Managed via `sql/schema/schema.go`:

```go
type SchemaManager interface {
    Initialize(ctx context.Context, db *sql.DB) error
    GetVersion(ctx context.Context, db *sql.DB) (uint, bool, error)
    MigrateTo(ctx context.Context, db *sql.DB, targetVersion uint) error
}
```

Each backend (`postgres`, `sqlite`) implements dialect-specific migrations (e.g., `SERIAL` vs `AUTOINCREMENT`).

### SQLite Specifics

SQLite requires special configuration for production-like behavior:

```go
// Connection pragmas
PRAGMA journal_mode = WAL;           // Write-Ahead Logging for concurrency
PRAGMA synchronous = NORMAL;         // Balance durability/performance
PRAGMA busy_timeout = 5000;          // Wait 5s on lock contention
PRAGMA foreign_keys = ON;            // Enforce referential integrity
PRAGMA cache_size = -64000;          // 64MB cache
```

Differences from Postgres:

- `AUTOINCREMENT` instead of `SERIAL`
- No `FOR UPDATE SKIP LOCKED` (uses `busy_timeout` + retry logic)
- `DATETIME('now')` instead of `NOW()`
- JSON stored as `TEXT` instead of `JSONB`

## Consequences

### Positive

1. **Data Integrity**: ACID transactions eliminate race conditions and state inconsistencies
2. **Durability**: Zero data loss on crash/restart (WAL + fsync)
3. **Query Flexibility**: SQL enables rich analytics, debugging, and observability
4. **Cost Efficiency**: Disk-based storage is 10-100x cheaper than RAM at scale
5. **Operational Maturity**: Standard PostgreSQL tooling (pg_dump, replication, monitoring)
6. **Schema Evolution**: Versioned migrations with rollback support
7. **Developer Experience**: SQLite makes local development instant (no Docker required)
8. **Audit Trail**: Message history and schedule execution logs for compliance
9. **Retention Policies**: Soft deletes + cleanup service enable GDPR compliance
10. **Simpler Codebase**: Removed ~1200 lines of Redis-specific Lua scripts and connection management

### Negative

1. **Migration Complexity**:
   - Breaking change requiring requiring rewrite of repository package
   - Many setup configurations required updating protobuf APIs

2. **Performance Characteristics**:
   - Single-node Postgres: ~5-10k msg/sec (vs Redis 50k+ msg/sec)
   - Mitigated by proper indexing, connection pooling, and read replicas
   - Good enough for 95% of queue workloads (most cases < 1k msg/sec)

3. **Horizontal Scaling**:
   - Redis Cluster shards automatically; Postgres requires partitioning strategy
   - Mitigated by read replicas, logical replication, and future Citus/pgpool integration

4. **Latency**:
   - P99 latency ~10-50ms (vs Redis ~1-5ms)
   - Acceptable for ChronoQueue as queue systems with delayed & priority based messaging (not real-time pub/sub)

5. **Operational Shift**:
   - Teams must learn PostgreSQL tuning (vs Redis tuning)
   - Disk I/O becomes bottleneck instead of network/CPU
   - Requires PostgreSQL DBA expertise at scale

### Neutral

1. **Storage Size**: Disk-based storage enables larger queues but requires disk space planning
2. **Backup Strategy**: PostgreSQL backups (pg_dump, WAL archiving) replace Redis RDB/AOF
3. **Monitoring**: New metrics (connection pool, query latency, WAL size) vs Redis memory/eviction
4. **SQLite Limitations**: Single-writer concurrency acceptable for dev, but not production at scale

## Implementation Status

- ✅ Three-tier repository architecture (`storage.go`, `backend.go`, `implementation.go`)
- ✅ PostgreSQL backend (`pkg/repository/postgres/`)
- ✅ SQLite backend (`pkg/repository/sqlite/`)
- ✅ Shared SQL logic (`pkg/repository/sql/`)
- ✅ Schema versioning and migrations
- ✅ Background services (reclaim, scheduler, cleanup)
- ✅ Message retention policies (DELETE_IMMEDIATELY, RETAIN_DURATION, RETAIN_FOREVER)
- ✅ Soft delete pattern with `completed_at`/`deleted_at`
- ✅ Priority queue implementation
- ✅ Lease management and heartbeats
- ✅ Dead Letter Queue (DLQ)
- ✅ Schedule execution history
- ✅ Integration tests (59 tests passing)
- ✅ Example applications updated (event-processor, interview-platform)
- ✅ Client library compatibility maintained

## Alternatives Considered

1. **Keep Redis, Add Postgres Audit Log**:
   - Rejected: Complexity of dual storage, eventual consistency issues

2. **Redis Enterprise with Flash Storage**:
   - Rejected: Proprietary, expensive, doesn't solve transactional integrity

3. **Kafka/RabbitMQ/NATS**:
   - Rejected: We need queue semantics (claim, lease, DLQ), not just pub/sub

4. **DynamoDB/Cassandra**:
   - Rejected: Tuning for queue access patterns harder than Postgres

5. **YugabyteDB/CockroachDB**:
   - Considered for future: Postgres-compatible with horizontal scaling

## Future Considerations

1. **Read Replicas**: Offload analytics/metrics queries from primary
2. **Partitioning**: Shard `cq_messages` by queue_name for large deployments
3. **YugabyteDB**: Distributed Postgres for extreme scale

## References

- [PostgreSQL Write-Ahead Logging](https://www.postgresql.org/docs/current/wal-intro.html)
- [FOR UPDATE SKIP LOCKED](https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE)
- [SQLite Write-Ahead Logging](https://www.sqlite.org/wal.html)
- ADR-001: Redis Streams Architecture (superseded)
