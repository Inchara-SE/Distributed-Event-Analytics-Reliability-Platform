# PulseStream — Product Requirements Document (PRD)

**Project:** PulseStream — Distributed Event Analytics & Reliability Platform  
**Document:** Product Requirements Document  
**Version:** 1.0  
**Status:** MVP scope baseline  
**Audience:** Project owner, implementation agents, technical reviewers, recruiters  
**Target role:** Analytics Engineer (primary), Data Engineer (secondary)  
**Deployment target:** Local, reproducible Docker Compose environment  

---

## 1. Executive summary

PulseStream is a portfolio-grade event analytics and reliability platform. It accepts HTTP request-completion events from simulated microservices, transports them through a partitioned message broker, validates and processes them with Python consumers, stores event-level data in ClickHouse, transforms that data with dbt, and exposes operational and analytical metrics through FastAPI and Grafana.

The project is deliberately more than an event-ingestion demo. It must demonstrate that data remains explainable and analytically trustworthy when messages are duplicated, malformed, delayed, processed more than once, or temporarily blocked by a storage outage.

The primary portfolio story is:

> PulseStream turns noisy application telemetry into tested, reconciled, service-level analytics while making ingestion and processing failures visible and recoverable.

The MVP is designed for a recruiter to clone, start locally, generate events, inspect dashboards, trigger a failure scenario, observe recovery, run automated tests, and review reproducible benchmark results.

## 2. Product context and problem statement

### 2.1 Primary user

A backend, platform, or reliability engineer who needs to understand the behavior of a group of application services from event data.

### 2.2 Problem

Raw application events are high-volume and noisy. They can contain invalid records, duplicate event IDs, late-arriving timestamps, and periods of delayed consumption. A dashboard is only useful when its underlying metrics have consistent definitions and the pipeline can explain how records were accepted, rejected, duplicated, delayed, or recovered.

### 2.3 Product response

PulseStream provides:

- A documented, versioned HTTP event contract.
- A partitioned event stream and consumer group.
- Validation, bounded retries, dead-letter routing, and replay guidance.
- Append-oriented event storage and tested analytical transformations.
- Consistent service-level request, error, latency, and processing-delay metrics.
- Operational visibility into throughput, consumer lag, retries, dead letters, and last successful processing.
- Automated correctness, recovery, and benchmark evidence.

## 3. Goals and success criteria

### 3.1 Product goals

1. **Reliable ingestion:** Publish accepted events to a broker and make ingestion outcomes observable.
2. **Traceable processing:** Preserve enough delivery metadata to investigate replays, duplicates, invalid records, and consumer recovery.
3. **Trustworthy analytics:** Convert raw deliveries into canonical events and clearly defined service metrics using SQL and dbt.
4. **Observable failure behavior:** Expose processing delay, consumer lag, retries, DLQ volume, and last successful processing time.
5. **Demonstrable resilience:** Reproduce and verify failures rather than claiming reliability without evidence.
6. **Recruiter-friendly operation:** Make the application understandable and runnable from a clean clone using documented commands.

### 3.2 Portfolio success criteria

The MVP is successful when a reviewer can:

- Start the local stack using the documented Docker Compose workflow.
- Generate events while controlling approximate event rate, service count, error rate, and burstiness.
- Inspect service request volume, server error rate, latency percentiles, processing delay, and pipeline health.
- Trigger a malformed-message, duplicate, consumer-stop, storage-outage, burst, and late-event scenario.
- See the expected lag, retry, DLQ, or metric-correction behavior.
- Run automated tests and a controlled reconciliation.
- Read benchmark results with the hardware and configuration that produced them.

### 3.3 Non-goals

The MVP is not intended to be:

- A managed, multi-tenant SaaS product.
- A highly available production cluster.
- A general-purpose log aggregation platform.
- A full application performance monitoring product.
- A distributed tracing backend or replacement for OpenTelemetry Collector, Jaeger, or Tempo.
- A business-event warehouse for orders, payments, or customer behavior.
- A machine-learning anomaly detection system.
- A Kubernetes deployment or cloud infrastructure project.
- A guarantee of exactly-once delivery.

## 4. Target users and primary use cases

### 4.1 Primary persona: platform engineer

Needs to answer:

- Which service is receiving the most traffic?
- Which service is producing the most server errors?
- Is latency degrading, and is the degradation concentrated in one service or route?
- Is the consumer keeping up with the broker?
- Are events malformed, duplicated, late, or stuck?
- What happens when the analytics store is unavailable?

### 4.2 Secondary persona: Analytics Engineer / data reviewer

Needs to understand:

- What one row in the raw and canonical event datasets represents.
- How duplicate deliveries are treated.
- How request and error metrics are defined.
- How event time differs from processing time.
- How raw records reconcile to fact tables, aggregates, and DLQ records.
- Which models and tests guarantee the reporting logic.

### 4.3 Core user journeys

**Journey A — Start and inspect traffic**

1. Start PulseStream.
2. Generate events for multiple simulated services.
3. Open Grafana.
4. Compare request rate, server error rate, and latency by service.
5. Query a service and time range through the API.

**Journey B — Investigate pipeline delay**

1. Stop the event consumer.
2. Continue generating events.
3. Observe consumer lag increase.
4. Restart the consumer.
5. Observe lag reduce and the last-success timestamp advance.
6. Run reconciliation to verify that accepted events are represented in raw and analytical data.

**Journey C — Investigate bad or repeated data**

1. Publish a malformed record through the test/broker injection path, or submit invalid input to the API.
2. Observe the API rejection or consumer DLQ routing, depending on where the invalidity is detected.
3. Publish the same valid event ID more than once.
4. Verify that raw deliveries remain auditable while the canonical fact contains one logical event within the deduplication guarantee.

**Journey D — Recover from analytics-store outage**

1. Stop ClickHouse while event production continues.
2. Observe retries and consumer lag growth.
3. Verify that valid source offsets are not committed before durable storage succeeds.
4. Restart ClickHouse.
5. Observe the consumer catch up.
6. Run reconciliation and compare expected counts with persisted and modeled counts.

## 5. MVP scope

### 5.1 P0 — Required for MVP acceptance

| ID | Capability | Requirement |
|---|---|---|
| P0-01 | Event contract | Versioned JSON envelope for HTTP request-completion events. |
| P0-02 | Load generator | Configurable service count, event rate, error rate, duration, and burst mode. |
| P0-03 | Ingestion API | FastAPI endpoint validates the external contract and publishes accepted events to Redpanda. |
| P0-04 | Broker | Redpanda topic with documented partition count and key strategy. |
| P0-05 | Consumer group | Python consumer processes events in bounded batches. |
| P0-06 | Validation | Consumer performs version and semantic validation as defense in depth. |
| P0-07 | DLQ | Invalid broker records are forwarded to a dead-letter topic with failure metadata. |
| P0-08 | Raw storage | Valid event deliveries and DLQ records are persisted to ClickHouse with broker coordinates. |
| P0-09 | Duplicate semantics | Raw deliveries are retained; dbt produces a canonical event model with a documented seven-day deduplication guarantee. |
| P0-10 | Analytics | dbt models produce service-level minute metrics and pipeline-quality metrics. |
| P0-11 | Metric definitions | Request count/rate, server error rate, p50/p95/p99 latency, processing delay, duplicate count, invalid count, DLQ count and consumer lag are documented. |
| P0-12 | Query API | FastAPI exposes service metrics and pipeline-health summaries. |
| P0-13 | Observability | Prometheus metrics and Grafana dashboards cover ingestion, processing, lag, retries, DLQ, and last success. |
| P0-14 | Recovery behavior | Bounded retries, no source offset commit before successful sink write or successful DLQ publication, graceful shutdown, and restart recovery. |
| P0-15 | Automated tests | Unit, integration, data-quality, reconciliation, and failure-injection tests. |
| P0-16 | Benchmark evidence | Reproducible throughput, delay, lag, and recovery measurements with environment details. |
| P0-17 | Documentation | README, architecture, event contract, metric definitions, data dictionary, runbook, test and benchmark reports. |

### 5.2 P1 — Include only if P0 is stable

- Optional second consumer instance to demonstrate partition assignment and parallelism.
- A simple service registry UI or CRUD endpoints.
- A lightweight threshold-alert rule evaluator and alert history.
- OpenTelemetry traces across API, producer, and consumer.
- A scheduled historical backfill command for late events older than the automatic correction window.
- Redpanda Console for local broker inspection, if it does not make the environment too heavy.

### 5.3 Explicitly deferred

Kubernetes, Spark, Flink, Airflow, Redis, a complex React frontend, cloud deployment, multi-broker high availability, external schema registry, authentication/RBAC, multi-tenancy, notification integrations, and machine-learning anomaly detection.

## 6. Functional requirements

### 6.1 Event generation

**FR-01:** The generator shall emit synthetic HTTP request-completion events for configurable service names.

**FR-02:** The generator shall accept configuration for:

- Target event rate in events per second.
- Number and names of simulated services.
- Server-error probability.
- Client-error probability, if enabled.
- Event duration and duration distribution.
- Run duration and total event count.
- Burst size and burst interval.
- Duplicate injection rate or selected event IDs.
- Late-event injection amount.
- Random seed for repeatable tests.

**FR-03:** The generator shall report attempted, accepted, rejected, and failed publish counts. It shall not label an attempted event as accepted until the ingestion API confirms broker publication.

### 6.2 Ingestion API

**FR-04:** The API shall validate the envelope and HTTP attributes before publication.

**FR-05:** The API shall return `202 Accepted` only after the broker acknowledges the message under the configured producer acknowledgement policy.

**FR-06:** Structurally invalid external requests shall receive a clear 4xx response and shall not be published to the main topic.

**FR-07:** API readiness shall fail when a required dependency is unavailable; liveness shall indicate whether the process itself is running.

### 6.3 Stream processing

**FR-08:** The consumer shall process messages in bounded batches with configurable maximum batch size and poll interval.

**FR-09:** The consumer shall validate supported schema versions and required event attributes.

**FR-10:** Invalid messages found after entering the broker shall be published to the DLQ topic with original payload, source topic/partition/offset, failure code, failure reason, and failure timestamp.

**FR-11:** The source offset for an invalid message shall be committed only after the DLQ broker publication is acknowledged.

**FR-12:** Transient ClickHouse write failures shall trigger bounded exponential backoff. If the sink remains unavailable, the consumer shall stop advancing source offsets and allow broker lag to grow rather than silently discarding records.

**FR-13:** The source offset for a valid batch shall be committed only after the corresponding ClickHouse write succeeds.

**FR-14:** The consumer shall expose message throughput, batch size, processing latency, retry count, last successful processing time, and lag where measurable.

**FR-15:** The consumer shall stop accepting new work and attempt to flush/finish its in-flight batch during graceful shutdown. It shall not commit offsets for data that has not been durably written.

### 6.4 Storage and analytics

**FR-16:** ClickHouse shall retain event delivery records with broker coordinates, event identifiers, event timestamps, processing timestamps, service attributes, and payload metadata required for audit and analytics.

**FR-17:** ClickHouse shall retain DLQ records separately from valid event deliveries.

**FR-18:** dbt shall expose staging, intermediate, fact, and mart models with declared grains and documented metric logic.

**FR-19:** Physical retries of the same broker record shall be removed from the analytical delivery model using broker topic/partition/offset identity.

**FR-20:** Repeated event IDs across distinct broker records shall be flagged as duplicate deliveries. The canonical event fact shall retain one event per event ID within the documented seven-day deduplication guarantee.

**FR-21:** dbt tests shall cover required fields, accepted status-code range, non-negative duration, uniqueness at the declared canonical grain, valid service keys, and aggregate reconciliation.

### 6.5 Metrics and query API

**FR-22:** The platform shall expose request count and request rate by service and UTC minute.

**FR-23:** The platform shall expose server error count and server error rate. Server errors are defined as HTTP status codes from 500 through 599. 4xx responses shall be available separately and shall not be silently combined with 5xx errors.

**FR-24:** The platform shall expose p50, p95, and p99 latency by service and UTC minute. The production mart may use an approximate t-digest quantile implementation; this must be labeled and documented.

**FR-25:** The platform shall expose event-to-processing delay, including negative values caused by clock skew where observed.

**FR-26:** The query API shall support service and time-range filters, validate range parameters, and return UTC timestamps.

**FR-27:** If a metric denominator is zero, the API shall return `null`/no value rather than inventing a zero rate.

### 6.6 Observability and dashboard

**FR-28:** Prometheus shall scrape application and broker metrics available in the local deployment.

**FR-29:** Grafana shall include at least two dashboards:

1. **Service Analytics:** request volume/rate, 5xx error rate, 4xx rate, p50/p95/p99 latency, service comparison, event-time range.
2. **Pipeline Reliability:** ingestion rate, consumer throughput, lag by partition, retries, ClickHouse write errors, DLQ count, processing delay, and last successful processing time.

**FR-30:** Dashboard panels shall label units, time zones, aggregation intervals, and approximate percentile calculations where relevant.

### 6.7 Reconciliation and replay

**FR-31:** A controlled test run shall record the event IDs and expected categories needed for reconciliation.

**FR-32:** Reconciliation shall compare broker source records with valid deliveries, duplicate deliveries, invalid/DLQ records, canonical facts, and aggregate totals.

**FR-33:** A documented replay procedure shall explain how to republish selected DLQ records after correction. Replay shall retain original identifiers or record replay metadata so the resulting duplicate/replay behavior is measurable.

## 7. Event contract and data rules

### 7.1 Envelope

The v1 event envelope contains:

| Field | Type | Required | Rule |
|---|---|---:|---|
| `event_id` | UUID string | Yes | Stable identifier for one logical HTTP request event. |
| `schema_version` | Integer | Yes | Starts at `1`; unsupported versions are not silently interpreted. |
| `event_time` | ISO-8601 timestamp | Yes | UTC timestamp of request completion. |
| `producer` | String | Yes | Component that emitted the event, e.g. `load-generator`. |
| `service` | String | Yes | Lowercase service slug. |
| `event_type` | String | Yes | `http.request.completed` for MVP. |
| `attributes` | Object | Yes | HTTP request attributes defined below. |

### 7.2 HTTP attributes

| Field | Type | Required | Rule |
|---|---|---:|---|
| `method` | String | Yes | HTTP method such as GET, POST, PUT, PATCH, DELETE. |
| `route_template` | String | Yes | Templated route such as `/orders/{order_id}`; do not store full URLs or user-specific path values. |
| `status_code` | Integer | Yes | 100–599. |
| `duration_ms` | Number | Yes | Non-negative request duration in milliseconds. |
| `trace_id` | String/null | No | Trace identifier; no credentials or personal data. |

### 7.3 Data safety

- Generate synthetic data only.
- Do not store authorization headers, cookies, request/response bodies, passwords, tokens, email addresses, or other secrets.
- Use route templates rather than raw URLs containing IDs or query strings.
- Keep event payloads small and bounded.

## 8. Metric contracts

All analytical windows use UTC event time and are aligned to fixed one-minute boundaries unless stated otherwise.

| Metric | Grain | Definition | Notes |
|---|---|---|---|
| `request_count` | Service × UTC minute | Count of canonical HTTP request events in the minute. | One logical event per `event_id` within the deduplication guarantee. |
| `request_rate_per_sec` | Service × UTC minute | `request_count / 60`. | A fixed-window average rate, not an instantaneous rate. |
| `server_error_count` | Service × UTC minute | Count where `status_code >= 500 AND status_code <= 599`. | 5xx only. |
| `server_error_rate` | Service × UTC minute | `server_error_count / request_count`. | Null when request count is zero. |
| `client_error_count` | Service × UTC minute | Count where status code is 400–499. | Report separately from 5xx. |
| `latency_p50_ms` | Service × UTC minute | 50th percentile of canonical `duration_ms`. | Approximate t-digest in operational mart; document approximation. |
| `latency_p95_ms` | Service × UTC minute | 95th percentile of canonical `duration_ms`. | Same method and limitations as p50. |
| `latency_p99_ms` | Service × UTC minute | 99th percentile of canonical `duration_ms`. | Same method and limitations as p50. |
| `processing_delay_ms` | Event / service × time window | `consumer_processed_at - event_time`. | Can be negative due to producer clock skew. |
| `duplicate_delivery_count` | Service × UTC minute or run | Count of extra broker records sharing an event ID inside the deduplication horizon. | Raw deliveries remain available for audit. |
| `invalid_event_count` | Failure reason × time window | Count of broker records routed to DLQ. | API-level 4xx rejections are separate from broker DLQ count. |
| `consumer_lag` | Consumer group × partition | Latest broker offset minus committed/processed offset, as supported by the client metrics. | Gauge, not an additive event metric. |
| `last_successful_processing_at` | Consumer group | Timestamp of last successfully persisted and committed batch. | Must not advance on failed writes. |

### 8.1 Service health interpretation

The MVP may show a simple illustrative service state:

- **Unknown:** insufficient traffic to evaluate.
- **Degraded:** configured 5xx-rate or p95-latency threshold is breached.
- **Healthy:** sufficient traffic exists and neither threshold is breached.

Default demo thresholds must be labeled as demonstration settings, not universal SLOs. Health-state logic must be documented and deterministic.

## 9. Reliability and delivery semantics

### 9.1 Delivery guarantee

The MVP provides **at-least-once processing** from the broker to the analytics sink. A consumer may write a batch successfully and crash before committing its source offset, causing the same broker records to be delivered again.

The architecture must therefore:

- Preserve broker coordinates in the raw storage layer.
- Make sink-write retry behavior visible.
- Deduplicate repeated physical writes of the same broker record in analytical models.
- Deduplicate repeated logical `event_id` values within the documented horizon.
- Avoid claiming exactly-once end-to-end processing.

### 9.2 Duplicate policy

- Keep raw event delivery evidence.
- Treat the same `(topic, partition, offset)` as the same broker record even if the sink write is repeated.
- Treat repeated `event_id` values on different broker coordinates as duplicate logical deliveries within the seven-day guarantee.
- Keep one canonical event for metric calculations and expose the excluded duplicate count.
- Duplicates outside the seven-day guarantee are not promised to be suppressed automatically; historical reconciliation/backfill is required.

### 9.3 Invalid event policy

- Invalid external API requests receive a 4xx response and are not published.
- Invalid records already present in the broker (for example, from an older producer, direct test injection, or schema mismatch) are sent to the DLQ.
- The DLQ is a durable broker topic and is separately archived to ClickHouse.

### 9.4 Late event policy

- Events are aggregated by `event_time`, not arrival time.
- Events arriving up to 15 minutes after their event-time window are eligible for automatic correction on the next analytics refresh.
- Events arriving later than 15 minutes are retained and flagged for historical reconciliation/backfill; the MVP does not promise immediate correction of arbitrarily old closed windows.
- The seven-day duplicate guarantee and 15-minute automatic late-correction window are separate policies.

### 9.5 Storage outage policy

- Retry transient sink writes with bounded exponential backoff.
- Do not commit valid source offsets until ClickHouse confirms the write.
- After bounded retries, keep the consumer alive but stop advancing the affected source partition(s), or safely pause consumption, so lag grows instead of losing messages.
- After ClickHouse recovers, resume and reconcile.

## 10. Non-functional requirements

| Category | Requirement |
|---|---|
| Reproducibility | A clean clone can start the local environment using documented commands and configuration. |
| Correctness | Controlled event runs reconcile across source records, raw deliveries, canonical facts, DLQ, and aggregates. |
| Reliability | Valid source offsets are not committed before durable sink writes; invalid source offsets are not committed before DLQ publication acknowledgement. |
| Bounded resource use | Producer/consumer batches and event payload sizes are configurable and capped. |
| Observability | Every important failure path has a metric or log with useful context. |
| Query usability | Metric endpoints filter by service and UTC time range. |
| Maintainability | Python modules, dbt models, SQL tests, and infrastructure configuration are separated by responsibility. |
| Security hygiene | No secrets or sensitive request payloads are generated or stored; local credentials are provided through environment variables. |
| Documentation | Setup, architecture, schema, metric definitions, limitations, recovery, tests, and benchmark methodology are documented. |

## 11. Benchmark plan and performance targets

### 11.1 Workload profiles

The initial benchmark scenarios are targets for measurement, not guaranteed capacity claims.

| Profile | Target rate | Purpose |
|---|---:|---|
| Baseline | 100 events/sec | Sustained normal load. |
| Elevated | 250 events/sec | Observe steady-state throughput and lag. |
| Burst | 500 events/sec | Observe short-term backpressure and catch-up. |
| Stretch | 1,000 events/sec | Optional, only if hardware allows stable measurement. |

### 11.2 Required benchmark metadata

Every report must include:

- CPU model and available cores.
- RAM and available disk.
- Operating system and Docker version.
- Container CPU/memory limits, if configured.
- Redpanda topic partitions and broker settings.
- ClickHouse table engines/order keys and relevant settings.
- Producer count, consumer count, batch size, event size, and duration.
- Number of services and event distribution.
- Event error and duplicate rates.
- Whether dashboards and dbt refreshes were active during the test.

### 11.3 Measurements

- Producer attempted and API-accepted events/sec.
- Consumer successfully persisted events/sec.
- Consumer lag over time and time to return to baseline.
- p50/p95/p99 ingestion-to-query visibility delay.
- Consumer batch processing duration.
- Retry and DLQ counts.
- Reconciliation result and any unexplained difference.

Do not publish performance claims without the corresponding report and environment details. The project must never claim “millions of events per second” without reproducible evidence.

## 12. Acceptance criteria

The MVP is accepted when all mandatory criteria below pass.

| ID | Acceptance criterion | Evidence |
|---|---|---|
| AC-01 | A clean local startup succeeds from documented commands. | README walkthrough or recorded demo. |
| AC-02 | Valid event requests are acknowledged only after broker publication acknowledgement. | API integration test. |
| AC-03 | Invalid API payloads are rejected with clear 4xx responses. | API validation tests. |
| AC-04 | A broker-injected unsupported/malformed event is published to DLQ and archived. | Integration test and DLQ dashboard panel. |
| AC-05 | Repeated broker delivery after simulated crash does not inflate canonical request counts. | Duplicate/replay test and dbt uniqueness test. |
| AC-06 | A consumer stop causes lag to increase; restart causes processing to resume. | Failure demo and metrics screenshot. |
| AC-07 | ClickHouse outage causes retries and prevents premature source offset commits. | Integration/failure-injection test. |
| AC-08 | ClickHouse recovery allows the consumer to catch up without unexplained data loss. | Reconciliation report. |
| AC-09 | A late event within 15 minutes corrects its event-time minute metric after refresh. | Late-event integration test. |
| AC-10 | Request count, 5xx rate, and latency percentiles match a controlled fixture within the documented quantile tolerance. | SQL/dbt tests. |
| AC-11 | Dashboard shows required traffic, latency, error, lag, retry, DLQ, delay, and last-success panels. | Dashboard export/screenshots. |
| AC-12 | Reconciliation accounts for all broker source records as valid unique events, duplicate deliveries, or invalid/DLQ records, with physical sink retry writes separately identified. | Reconciliation output. |
| AC-13 | Benchmark report includes environment and workload metadata. | `docs/benchmark-report.md`. |
| AC-14 | Repository includes architecture, contract, metric, data dictionary, runbook, and interview documentation. | Repository review. |

### 12.1 Reconciliation invariants

For a bounded test interval with known source offsets:

`source_broker_records = valid_logical_deliveries + invalid_records_routed_to_dlq`

And:

`valid_logical_deliveries = canonical_unique_events + duplicate_logical_deliveries`

Physical repeated writes of the same broker coordinate are measured separately and must not be counted as new broker records or logical events.

For each service-minute group:

`request_count = server_error_count + client_error_count + successful_or_redirect_count`

where the final category contains statuses 100–399. The exact status classification must be applied consistently in SQL and tests.

`SUM(minute_request_count) = COUNT(canonical_events)` for the same service and event-time range, subject to documented late-event inclusion and deduplication boundaries.

## 13. Risks and mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Too many infrastructure components for local RAM | Unreliable demo or difficult setup | Start with one Redpanda node, bounded data, lightweight profiles, and documented minimum resources. |
| ClickHouse retries produce duplicate physical rows | Inflated metrics if modeled incorrectly | Persist broker coordinates and deduplicate physical writes in dbt. |
| Confusion between API rejection and DLQ records | Misleading failure counts | Define the boundary clearly and expose API 4xx and broker DLQ metrics separately. |
| Approximate percentile interpretation | Misleading latency claims | Label t-digest percentiles as approximate and test with tolerances. |
| Late-event correction complexity | Incorrect historical windows | Limit automatic correction to 15 minutes and document backfill for older events. |
| Broker delivery semantics misunderstood | False exactly-once claim | Document at-least-once behavior and demonstrate duplicate replay. |
| Benchmark numbers depend on hardware | Unsupported performance claims | Record all environment and workload metadata. |
| AI-generated code has hidden defects | Inconsistent architecture or unexplained code | Build phase-by-phase, run tests after each phase, and review each contract and model. |

## 14. MVP roadmap and exit gates

| Phase | Deliverable | Exit gate |
|---|---|---|
| 0 — Foundation | Repository, Compose skeleton, environment configuration, lint/test baseline. | Clean start and health checks pass. |
| 1 — Event contract and ingestion | Schema v1, generator, FastAPI, Redpanda topics. | Valid/invalid ingestion tests pass. |
| 2 — Processing and resilience | Consumer group, validation, raw writes, DLQ, retries, offset safety, shutdown. | Duplicate, DLQ, consumer restart, and sink outage tests pass. |
| 3 — Storage and analytics | ClickHouse tables, dbt models, metric contracts, query API. | Model tests and controlled metric comparisons pass. |
| 4 — Observability | Prometheus metrics and Grafana dashboards. | Required panels show live test traffic and injected failures. |
| 5 — Proof and polish | Reconciliation, benchmarks, runbook, README, interview guide, demo. | Acceptance criteria AC-01 through AC-14 pass or documented exceptions are clearly listed. |

## 15. Repository deliverables

Proposed repository name: `pulsestream-event-analytics`.

Minimum documentation set:

```text
docs/
  architecture/
    technical-architecture.md
    architecture-diagram.mmd
    adr/
  contracts/
    event-schema-v1.md
  analytics/
    metric-catalog.md
    data-dictionary.md
    data-model.md
  operations/
    runbook.md
    failure-scenarios.md
  quality/
    reconciliation.md
    test-report.md
    benchmark-report.md
  interview-guide.md
README.md
```

The PRD and TAD are the source documents for the MVP. Any change to scope, semantics, or delivery guarantees should be reflected in these documents and, where appropriate, recorded as an Architecture Decision Record (ADR).

## 16. Explicit assumptions and decisions to validate during implementation

1. The first release uses one local Redpanda node with three partitions. It demonstrates partitioning and consumer-group behavior but not broker high availability.
2. The system uses at-least-once processing, not end-to-end exactly-once semantics.
3. The supported event type is `http.request.completed` with schema version 1.
4. The API rejects structurally invalid requests. The consumer DLQ path is demonstrated using malformed, unsupported-version, or otherwise invalid records already present in the broker.
5. Raw valid deliveries and DLQ records are stored in ClickHouse; PostgreSQL stores service and stream metadata.
6. dbt performs micro-batch analytics refreshes approximately once per minute. The platform is streaming at ingestion and micro-batch at the analytical mart layer; it does not promise sub-second dashboard freshness.
7. Automatic event-time correction is limited to events arriving within 15 minutes of their event-time window. Older events are retained and require a backfill/reconciliation operation.
8. Duplicate suppression is guaranteed only within a seven-day horizon. Raw evidence is retained according to the local demo retention configuration.
9. Operational percentile metrics use an approximate t-digest calculation unless an exact calculation is explicitly selected for a small test fixture.
10. Full alerting, public cloud deployment, multi-broker HA, and authentication are deferred unless the P0 MVP is completed early.

---

**End of PRD**
