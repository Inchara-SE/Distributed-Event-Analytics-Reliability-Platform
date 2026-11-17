# ADR-003: Event Time Policy and 15-Minute Automatic Correction Horizon

## Status
Accepted

## Context
Telemetry events reflect when an HTTP request completed in an application (`event_time`), whereas pipeline ingestion and consumer processing happen later (`processed_at`). Network congestion, consumer backlog, or clock skew mean `processed_at != event_time`. Computing service analytics on `processed_at` would distort incident timelines. Furthermore, recomputing arbitrary historical periods for late-arriving events creates runaway database load.

## Decision
1. **Time Grain**: All service-level analytics are partitioned and grouped strictly by `event_time` in UTC, aligned to fixed 1-minute windows: `[window_start, window_end)`.
2. **Signed Processing Delay**:
   - `processing_delay_ms = processed_at - event_time`.
   - Signed values are preserved; negative values indicate producer clock skew and are never clamped to zero.
3. **15-Minute Correction Horizon**:
   - Events arriving with `processing_delay_ms <= 15 minutes` are eligible for automatic micro-batch aggregate correction.
   - The serialized dbt analytics runner updates the current and previous UTC dates using partition overwriting.
   - Events arriving older than 15 minutes are retained in `fct_http_requests`, flagged in `mart_late_events`, and excluded from automatic minute mart recomputation until an explicit backfill command is executed.

## Consequences
- Fixed-minute aggregate queries remain deterministic and fast.
- Incidents are evaluated against true application occurrence times.
- Out-of-bounds late data is never silently dropped; it is isolated for audit and backfill.
