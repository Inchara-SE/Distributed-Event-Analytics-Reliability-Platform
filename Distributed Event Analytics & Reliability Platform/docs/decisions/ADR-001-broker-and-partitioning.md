# ADR-001: Broker Selection and Partitioning Strategy

## Status
Accepted

## Context
PulseStream requires a high-throughput, partitioned event log to ingest and buffer HTTP request-completion telemetry from distributed microservices. The technical baseline requires Kafka compatibility, reproducible local setup via Docker Compose, low resource overhead on developer machines, and realistic partition/consumer-group semantics.

## Decision
1. **Broker Technology**: Redpanda (single-node local container for MVP).
   - Fully compatible with the Kafka v2/v3 protocol and `confluent-kafka` client.
   - Starts in seconds with zero JVM/Zookeeper dependencies, keeping memory consumption bounded (<1GB).
2. **Partition Count**: 3 partitions for `application-events` and 3 partitions for `application-events-dlq`.
   - Demonstrates parallel consumption and key-based routing without overwhelming local CPU.
   - Maximum partition-level parallelism is 3 consumers per group.
3. **Partition Key**: `{service}|{route_template}`
   - Guarantees strict ordering per service-route combination.
   - Avoids hot-partitioning that would occur if using `service` alone.
   - Independent routes within the same service can be processed in parallel across partitions.
4. **Retention**: 7 days (local demo), matching the 7-day analytical deduplication horizon.

## Consequences
- Single-node Redpanda demonstrates partition assignment, rebalancing, and consumer groups, but not broker high availability or leader failover.
- Ordering is guaranteed only within a single partition; there is no global topic-wide order.
