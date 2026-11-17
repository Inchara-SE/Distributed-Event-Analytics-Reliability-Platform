# ADR-002: At-Least-Once Delivery and Multi-Layer Deduplication

## Status
Accepted

## Context
In distributed stream processing, network failures, node crashes, and consumer restarts create uncertainty around whether a storage write succeeded before the corresponding broker offset could be committed. Claiming "exactly-once" delivery across independent distributed systems (Kafka broker -> ClickHouse sink) creates false confidence and masks real failure modes.

## Decision
1. **Delivery Guarantee**: The system provides **at-least-once processing**.
2. **Offset Commit Discipline**:
   - `enable.auto.commit` is strictly disabled on all consumers.
   - For valid events: commit source offset ONLY after ClickHouse confirms durable insertion.
   - For invalid events: commit source offset ONLY after dead-letter queue (DLQ) publish is acknowledged.
   - Contiguous offset tracking: Never commit an offset past an earlier unpersisted/unhandled message in that partition.
3. **Two-Layer Deduplication**:
   - **Physical Sink Retry Duplicates**: Same `(topic, partition_id, offset_id)` re-inserted into ClickHouse after a consumer restart or commit crash. Deduplicated in dbt staging/intermediate view (`int_broker_delivery_dedup`).
   - **Logical Event Duplicates**: Different broker coordinates carrying the identical `event_id` due to upstream client/producer retries. Deduplicated in dbt intermediate view (`int_canonical_http_events`) retaining one canonical row per `event_id` within a 7-day deduplication horizon.
4. **Auditability**: Raw rows are never overwritten or mutated; duplicate delivery counts are explicitly computed and exposed in `mart_pipeline_health`.

## Consequences
- Metrics are shielded from both network retries and duplicate HTTP submissions.
- Raw delivery logs remain 100% auditable for forensic reconciliation.
- Duplicates outside the 7-day horizon require explicit historical backfill.
