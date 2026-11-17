# ADR-004: Storage Separation and Conformed Service Natural Key

## Status
Accepted

## Context
PulseStream requires both high-velocity append-oriented time-series storage for telemetry (millions of rows, columnar aggregations) and a relational metadata store for application service registration, ownership, and configuration. Using a single relational database for high-velocity streaming creates lock contention and storage bloat; using ClickHouse for relational metadata creates transactional limitations.

## Decision
1. **PostgreSQL**: Stores relational metadata:
   - `services`: Service slug (PK/unique natural key), display name, owner team, active status.
   - `stream_settings`: Stream-level configuration defaults.
2. **ClickHouse**: Stores high-velocity raw event deliveries, DLQ records, and dbt analytical models (`MergeTree` engine).
3. **No Cross-Database Foreign Keys**:
   - There is no physical cross-database foreign key between ClickHouse and PostgreSQL.
   - The lowercase slug `service_name` (e.g. `order-service`) acts as the conformed natural key.
   - Referential integrity is validated at the API ingestion boundary and by automated dbt relationship tests.

## Consequences
- Clean separation of concerns between operational metadata and columnar analytics.
- Linear scalability for event writes in ClickHouse without degrading relational metadata access.
- Service names must adhere to strict slug constraints (`^[a-z0-9-]+$`).
