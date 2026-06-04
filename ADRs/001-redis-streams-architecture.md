# ADR-001: Redis Streams Architecture

**Status:** Accepted
**Date:** 2025-11-02
**Authors:** ChronoQueue Team - @adrien19
**Related Issues:** refactor/migrate_to_redis_stream

## Context

ChronoQueue initially used Redis Sorted Sets for message queue management, where messages were scored by their scheduled execution time. While this approach worked, it presented several operational and scalability challenges as the system matured.

### Challenges with Sorted Set Architecture

1. **Race Conditions**: Multiple consumers competing for the same message via `ZPOPMIN` led to race conditions requiring complex Lua scripts and distributed locks for atomicity.

2. **Consumer Group Management**: No native support for consumer groups meant implementing custom tracking of which consumer was processing which message, increasing complexity.

3. **Message Ownership**: Tracking message leases required maintaining separate Redis keys and manual expiration handling, adding operational overhead.

4. **Reclaim Logic Complexity**: Identifying and reclaiming messages with expired leases required scanning all messages and checking metadata, resulting in O(N) operations.

5. **Limited Observability**: Determining which messages were pending vs. being processed required custom state tracking across multiple data structures.

6. **Scalability Concerns**: As message volume grew, sorted set operations (ZRANGE, ZPOPMIN with Lua) became bottlenecks during high throughput periods.

## Decision

We will migrate ChronoQueue's core queue implementation from Redis Sorted Sets to **Redis Streams**, leveraging native consumer group functionality for message distribution and processing.

### Key Design Choices

1. **Three-Tier Priority Streams**: Messages are routed to `stream:high:{queue}`, `stream:medium:{queue}`, or `stream:low:{queue}` based on priority thresholds (high ≥3, medium 2, low ≤1).

2. **Consumer Groups**: Each queue has a consumer group `cg:{queue}` enabling automatic message distribution via `XREADGROUP` with fair consumer balancing.

3. **Pending Entries List (PEL)**: Redis Streams natively tracks which messages are being processed by which consumer, eliminating custom lease tracking.

4. **Scheduled Messages**: Messages with future execution times remain in `schedule:{queue}` sorted set. A background scheduler service moves them to appropriate priority streams when due.

5. **Reclaim via XAUTOCLAIM**: Expired leases are automatically reclaimed using `XAUTOCLAIM`, providing O(1) reclaim operations instead of O(N) scanning.

6. **State Counters**: Terminal states (COMPLETED, CANCELED, ERRORED) are tracked via `stats:{queue}` hash for O(1) state queries.

7. **Message Cleanup**: Acknowledged messages are both XACK'd (removed from PEL) and XDEL'd (removed from stream) to prevent stream growth.

## Consequences

### Positive

- **Atomicity**: `XREADGROUP` provides atomic message claiming without race conditions
- **Native Consumer Tracking**: PEL eliminates need for custom lease management
- **Efficient Reclaim**: `XAUTOCLAIM` provides O(1) message reclaim vs O(N) scanning
- **Better Observability**: Stream info commands (`XINFO`, `XPENDING`) provide real-time insights
- **Horizontal Scalability**: Multiple consumers can read from same consumer group with automatic load balancing
- **Reduced Complexity**: Removed ~500 lines of Lua scripts and custom lock management
- **Priority Fairness**: Round-robin polling (high → medium → low) prevents starvation while maintaining priority

### Negative

- **Migration Complexity**: Requires updating all clients to pass `streamEntryID` to acknowledge operations
- **Three Streams per Queue**: More Redis keys per queue (3 streams vs 1 sorted set)
- **Scheduled Message Delay**: Messages aren't immediately available—scheduler service introduces ~300ms delay (configurable)
- **Learning Curve**: Team must understand Redis Streams semantics (PEL, consumer groups, XAUTOCLAIM)

### Neutral

- **State Counter Trade-off**: Terminal state tracking via counters requires updating on acknowledgment but provides O(1) queries vs scanning
- **Backward Compatibility**: Breaking change requiring coordinated deployment of server and clients

## Implementation Status

- ✅ Core storage layer migrated to Redis Streams
- ✅ Priority-based message routing implemented
- ✅ Consumer group management with automatic creation
- ✅ Scheduler service for scheduled message processing
- ✅ Reclaim service using XAUTOCLAIM
- ✅ State counter system for terminal states
- ✅ Message cleanup (XACK + XDEL) on acknowledgment
- ✅ Client library updated with streamEntryID support
- ✅ Integration tests passing (50 tests, ~82s execution)
- ✅ Example applications updated (event-processor, interview-platform)

## Alternative Considered

**Status Quo (Sorted Sets)**: Continue with sorted set architecture and optimize Lua scripts. Rejected due to inherent scalability limitations and complexity of handling consumer groups manually.

## References

- [Redis Streams Documentation](https://redis.io/docs/latest/develop/data-types/streams/)
- [XREADGROUP Command](https://redis.io/commands/xreadgroup/)
- [XAUTOCLAIM Command](https://redis.io/commands/xautoclaim/)
