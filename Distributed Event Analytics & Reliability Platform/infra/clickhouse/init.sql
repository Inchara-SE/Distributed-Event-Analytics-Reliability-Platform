-- ClickHouse bootstrap script for PulseStream
-- Initializes the raw event deliveries and dead-letter queue tables

CREATE DATABASE IF NOT EXISTS pulsestream;

USE pulsestream;

-- Raw event delivery table
-- Physical grain: one sink insert row for one processing attempt of one broker record
CREATE TABLE IF NOT EXISTS raw_event_deliveries (
    topic LowCardinality(String),
    partition_id UInt16,
    offset_id UInt64,
    event_id UUID,
    schema_version UInt16,
    event_time DateTime64(3, 'UTC'),
    producer LowCardinality(String),
    service LowCardinality(String),
    event_type LowCardinality(String),
    method LowCardinality(String),
    route_template String,
    status_code UInt16,
    duration_ms Float64,
    trace_id Nullable(String),
    broker_timestamp Nullable(DateTime64(3, 'UTC')),
    processed_at DateTime64(3, 'UTC'),
    payload_json String,
    payload_hash FixedString(64)
) ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (service, event_time, event_id, topic, partition_id, offset_id)
TTL event_time + INTERVAL 7 DAY;

-- Raw dead-letter queue (DLQ) archive table
-- Physical grain: one ClickHouse sink insert attempt for one DLQ broker record
CREATE TABLE IF NOT EXISTS raw_dlq_records (
    dlq_topic LowCardinality(String),
    dlq_partition_id UInt16,
    dlq_offset_id UInt64,
    source_topic LowCardinality(String),
    source_partition_id UInt16,
    source_offset_id UInt64,
    event_id Nullable(String),
    schema_version Nullable(UInt16),
    failure_code LowCardinality(String),
    failure_message String,
    failed_at DateTime64(3, 'UTC'),
    archived_at DateTime64(3, 'UTC'),
    original_payload String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(failed_at)
ORDER BY (failure_code, failed_at, dlq_topic, dlq_partition_id, dlq_offset_id)
TTL failed_at + INTERVAL 7 DAY;
