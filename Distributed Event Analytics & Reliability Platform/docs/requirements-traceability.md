# PulseStream Requirements Traceability Matrix (RTM)

This document maps every P0 requirement and acceptance criterion from `PulseStream_PRD.md` and `PulseStream_TAD.md` to its implementation files, automated tests, and verification/demo commands.

---

## 1. P0 Requirements Traceability

| Requirement ID | Capability | Specification | Implementation Files | Automated Test Files | Demo / Verification Command |
|---|---|---|---|---|---|
| **P0-01** | Event Contract | Versioned JSON envelope v1 for `http.request.completed` with schema version 1, UTC ISO timestamps, bounded attributes. | `app/contracts/events_v1.py`<br>`app/contracts/validation.py` | `tests/unit/test_contracts.py`<br>`tests/contract/test_event_v1_contract.py` | `pytest tests/contract/` |
| **P0-02** | Load Generator | Configurable service count, rate (eps), error probability, duration, burst, duplicate, late events, deterministic seed. | `app/producer/load_generator.py`<br>`scripts/benchmark.py` | `tests/unit/test_load_generator.py` | `python -m app.producer.load_generator --rate 100 --duration 10 --seed 42` |
| **P0-03** | Ingestion API | FastAPI endpoints (`POST /v1/events`, `POST /v1/events/batch`, `GET /v1/services`), validates input, returns 202 only after broker ack. | `app/api/main.py`<br>`app/api/routes/events.py`<br>`app/api/routes/services.py` | `tests/unit/test_api_events.py`<br>`tests/integration/test_api_ingestion.py` | `pytest tests/unit/test_api_events.py` |
| **P0-04** | Broker Topology | Redpanda `application-events` & `application-events-dlq` with 3 partitions, key `{service}\|{route_template}`, 7-day retention. | `infra/docker-compose.yml`<br>`scripts/create_topics.py`<br>`app/producer/kafka_producer.py` | `tests/unit/test_producer.py`<br>`tests/integration/test_broker_topics.py` | `python scripts/create_topics.py` |
| **P0-05** | Consumer Group | `pulsestream-event-processors` consumer group, auto-commit disabled, bounded batch max 500, contiguous per-partition offsets. | `app/consumers/event_processor.py`<br>`app/consumers/handlers.py` | `tests/unit/test_event_processor.py`<br>`tests/resilience/test_consumer_recovery.py` | `python -m app.consumers.event_processor` |
| **P0-06** | Consumer Validation | Defense-in-depth version & semantic validation in consumer; routes malformed/unsupported to DLQ. | `app/contracts/validation.py`<br>`app/consumers/handlers.py` | `tests/unit/test_consumer_validation.py` | `pytest tests/unit/test_consumer_validation.py` |
| **P0-07** | Dead-Letter Queue (DLQ) | Invalid broker records published to `application-events-dlq` with failure code, message, payload, and coordinates; source offset committed only after DLQ ack. | `app/consumers/event_processor.py`<br>`app/consumers/handlers.py`<br>`scripts/inject_invalid_event.py` | `tests/unit/test_dlq_routing.py`<br>`tests/integration/test_dlq_pipeline.py` | `python scripts/inject_invalid_event.py` |
| **P0-08** | Raw Storage | ClickHouse `raw_event_deliveries` and `raw_dlq_records` MergeTree tables with broker coordinates and payload audit hash. | `infra/clickhouse/init.sql`<br>`app/storage/clickhouse.py` | `tests/unit/test_clickhouse_adapter.py`<br>`tests/integration/test_storage_persistence.py` | `clickhouse-client --query "SELECT count() FROM raw_event_deliveries"` |
| **P0-09** | Duplicate Semantics | Distinguishes physical sink retry duplicates `(topic, partition_id, offset_id)` from logical duplicate `event_id` deliveries within 7-day horizon. | `dbt/models/intermediate/int_broker_delivery_dedup.sql`<br>`dbt/models/intermediate/int_http_event_dedup_audit.sql`<br>`dbt/models/intermediate/int_canonical_http_events.sql` | `tests/unit/test_dedup_logic.py`<br>`dbt test --select intermediate` | `pytest tests/unit/test_dedup_logic.py` |
| **P0-10** | Analytics dbt Models | Staging, intermediate, dimension, fact, and mart models in dbt Core + dbt-clickhouse (`dim_service`, `fct_http_requests`, `fct_service_minute_metrics`, etc.). | `dbt/models/`<br>`dbt/dbt_project.yml` | `tests/unit/test_dbt_models_sql.py`<br>`dbt test` | `dbt build` |
| **P0-11** | Metric Definitions | Explicit contracts: `request_count`, `request_rate_per_sec`, `server_error_rate`, `client_error_count`, approximate t-digest p50/p95/p99, processing delay, lag, etc. | `docs/analytics/metric-catalog.md`<br>`dbt/models/marts/mart_service_reliability.sql` | `tests/unit/test_metrics_calculation.py` | `pytest tests/unit/test_metrics_calculation.py` |
| **P0-12** | Query API | FastAPI endpoints (`GET /v1/metrics/services`, `GET /v1/metrics/pipeline`), UTC validation, null-safe division. | `app/api/routes/metrics.py`<br>`app/api/schemas/metrics.py` | `tests/unit/test_api_metrics.py` | `pytest tests/unit/test_api_metrics.py` |
| **P0-13** | Observability | Prometheus metrics collector and 2 provisioned Grafana dashboards (`Service Analytics`, `Pipeline Reliability`). | `app/observability/metrics.py`<br>`infra/prometheus/prometheus.yml`<br>`infra/grafana/dashboards/` | `tests/unit/test_prometheus_metrics.py` | `curl http://localhost:8000/metrics` |
| **P0-14** | Recovery Behavior | Bounded exponential retry with jitter; no commit before write/DLQ ack; graceful shutdown; lag growth during outage. | `app/common/retry.py`<br>`app/consumers/event_processor.py` | `tests/unit/test_retry_policy.py`<br>`tests/resilience/test_storage_outage.py` | `pytest tests/resilience/` |
| **P0-15** | Automated Tests | Comprehensive test suite spanning unit, contract, integration, resilience, and reconciliation. | `tests/unit/`<br>`tests/contract/`<br>`tests/integration/`<br>`tests/resilience/` | Full `pytest` execution | `pytest` |
| **P0-16** | Benchmark Evidence | Reproducible workload profiles: 100 eps, 250 eps, 500 eps, optional 1000 eps, tracking throughput, visibility delay, lag, and loss. | `scripts/benchmark.py`<br>`docs/quality/benchmark-report.md` | `tests/unit/test_benchmark_runner.py` | `python scripts/benchmark.py --profile baseline` |
| **P0-17** | Technical Documentation | Architecture docs, ADRs, data dictionary, runbook, failure lab, interview guide, elite GitHub README. | `README.md`<br>`docs/` | Link validation & markdown checks | Review `README.md` |

---

## 2. Acceptance Criteria (AC-01 through AC-14) Traceability

| AC ID | Acceptance Criterion Description | Verification Method / Test Target |
|---|---|---|
| **AC-01** | Clean local startup succeeds from documented commands. | Docker Compose configuration & validation; `make up` idempotent check. |
| **AC-02** | Valid event requests acknowledged only after broker publication acknowledgement. | `tests/unit/test_api_events.py` & `tests/integration/test_api_ingestion.py`. |
| **AC-03** | Invalid API payloads rejected with clear 4xx responses. | `tests/unit/test_api_events.py` (malformed schema, invalid methods, negative duration). |
| **AC-04** | Broker-injected unsupported/malformed event published to DLQ and archived. | `scripts/inject_invalid_event.py`, `tests/unit/test_dlq_routing.py`. |
| **AC-05** | Repeated broker delivery after simulated crash does not inflate canonical request counts. | `tests/unit/test_dedup_logic.py`, `dbt/models/intermediate/int_broker_delivery_dedup.sql`. |
| **AC-06** | Consumer stop causes lag to increase; restart causes processing to resume. | `tests/resilience/test_consumer_recovery.py`, Prometheus lag gauge assertions. |
| **AC-07** | ClickHouse outage causes retries and prevents premature source offset commits. | `tests/resilience/test_storage_outage.py`, `app/common/retry.py`. |
| **AC-08** | ClickHouse recovery allows consumer to catch up without unexplained data loss. | `scripts/reconcile_run.py`, invariant checks. |
| **AC-09** | Late event within 15 minutes corrects its event-time minute metric after refresh. | `tests/unit/test_late_events.py`, `dbt/models/marts/mart_late_events.sql`. |
| **AC-10** | Request count, 5xx rate, and latency percentiles match controlled fixture within quantile tolerance. | `tests/unit/test_metrics_calculation.py`, controlled fixture comparisons. |
| **AC-11** | Grafana dashboards show required traffic, latency, error, lag, retry, DLQ, delay, and last-success panels. | `infra/grafana/dashboards/*.json` validation. |
| **AC-12** | Reconciliation accounts for all broker source records as valid unique, duplicates, or DLQ, with physical sink retries separately identified. | `scripts/reconcile_run.py`, `tests/unit/test_reconciliation.py`. |
| **AC-13** | Benchmark report includes environment and workload metadata. | `docs/quality/benchmark-report.md`, `artifacts/benchmarks/`. |
| **AC-14** | Repository includes architecture, contract, metric, data dictionary, runbook, and interview documentation. | Files in `docs/` and `README.md`. |
