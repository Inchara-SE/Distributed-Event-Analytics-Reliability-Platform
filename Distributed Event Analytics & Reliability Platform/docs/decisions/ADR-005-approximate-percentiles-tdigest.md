# ADR-005: Approximate Latency Percentiles Using ClickHouse t-digest

## Status
Accepted

## Context
Computing exact percentiles (p50, p95, p99) over millions of streaming events across high-cardinality minute windows requires sorting every raw duration, creating high CPU and memory overhead in ClickHouse. Approximations such as t-digest provide high accuracy with constant-bounded memory and fast mergeability across partitions.

## Decision
1. **Algorithm**: Production minute mart (`fct_service_minute_metrics`) uses `quantileTDigest(0.50)`, `quantileTDigest(0.95)`, and `quantileTDigest(0.99)` on `duration_ms`.
2. **Explicit Labeling**: All API responses, Grafana panel descriptions, and documentation must explicitly label latency percentiles as "approximate (t-digest)" to avoid misleading users into expecting exact rank values.
3. **Tolerance-Based Testing**: Correctness and automated SQL tests assert against known fixtures using documented relative tolerances (e.g. ±2% error margin) rather than strict equality.

## Consequences
- Fast aggregation queries in ClickHouse even under heavy load.
- Transparent communication of statistical approximations to downstream consumers.
