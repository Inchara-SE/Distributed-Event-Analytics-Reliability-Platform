"""Unit tests for PulseStream Event Envelope Contract v1."""

from datetime import datetime, timezone, timedelta
import json
import pytest
from uuid import uuid4

from app.contracts.events_v1 import EventEnvelopeV1, HttpRequestAttributesV1
from app.contracts.validation import parse_and_validate_payload, sanitize_error_message
from app.common.errors import FailureCode


def test_valid_event_envelope():
    event_id = uuid4()
    now = datetime(2026, 11, 11, 10, 15, 22, 384000, tzinfo=timezone.utc)
    attrs = HttpRequestAttributesV1(
        method="POST",
        route_template="/orders/{order_id}",
        status_code=201,
        duration_ms=42.7,
        trace_id="trace-abc-123",
    )
    envelope = EventEnvelopeV1(
        event_id=event_id,
        schema_version=1,
        event_time=now,
        producer="load-generator",
        service="order-service",
        event_type="http.request.completed",
        attributes=attrs,
    )

    assert envelope.schema_version == 1
    assert envelope.service == "order-service"
    assert envelope.to_partition_key() == "order-service|/orders/{order_id}"

    # Canonical dictionary
    d = envelope.to_canonical_dict()
    assert d["event_id"] == str(event_id)
    assert d["event_time"] == "2026-11-11T10:15:22.384Z"
    assert d["attributes"]["method"] == "POST"

    # Deterministic JSON and hash
    json_str = envelope.to_canonical_json()
    assert json.loads(json_str) == d
    h = envelope.compute_payload_hash()
    assert len(h) == 64
    assert h == envelope.compute_payload_hash()


def test_timezone_normalization_to_utc():
    # Provide +05:00 timestamp
    tz_plus5 = timezone(timedelta(hours=5))
    dt_input = datetime(2026, 11, 11, 15, 30, 0, tzinfo=tz_plus5)  # corresponds to 10:30:00 UTC
    attrs = HttpRequestAttributesV1(
        method="GET",
        route_template="/health",
        status_code=200,
        duration_ms=5.0,
    )
    envelope = EventEnvelopeV1(
        event_id=uuid4(),
        event_time=dt_input,
        producer="test",
        service="user-service",
        attributes=attrs,
    )
    assert envelope.event_time.tzinfo == timezone.utc
    assert envelope.event_time.hour == 10
    assert envelope.event_time.minute == 30


def test_validation_unsupported_schema_version():
    payload = {
        "event_id": str(uuid4()),
        "schema_version": 2,
        "event_time": "2026-11-11T10:15:22Z",
        "producer": "test",
        "service": "order-service",
        "event_type": "http.request.completed",
        "attributes": {
            "method": "GET",
            "route_template": "/orders",
            "status_code": 200,
            "duration_ms": 10.0,
        },
    }
    envelope, failure_code, err = parse_and_validate_payload(payload)
    assert envelope is None
    assert failure_code == FailureCode.UNSUPPORTED_SCHEMA_VERSION
    assert "schema_version" in err


def test_validation_invalid_status_code():
    payload = {
        "event_id": str(uuid4()),
        "schema_version": 1,
        "event_time": "2026-11-11T10:15:22Z",
        "producer": "test",
        "service": "order-service",
        "event_type": "http.request.completed",
        "attributes": {
            "method": "GET",
            "route_template": "/orders",
            "status_code": 700,
            "duration_ms": 10.0,
        },
    }
    envelope, failure_code, err = parse_and_validate_payload(payload)
    assert envelope is None
    assert failure_code == FailureCode.INVALID_STATUS_CODE


def test_validation_negative_duration():
    payload = {
        "event_id": str(uuid4()),
        "schema_version": 1,
        "event_time": "2026-11-11T10:15:22Z",
        "producer": "test",
        "service": "order-service",
        "event_type": "http.request.completed",
        "attributes": {
            "method": "GET",
            "route_template": "/orders",
            "status_code": 200,
            "duration_ms": -5.0,
        },
    }
    envelope, failure_code, err = parse_and_validate_payload(payload)
    assert envelope is None
    assert failure_code == FailureCode.NEGATIVE_DURATION


def test_validation_invalid_route_template():
    payload = {
        "event_id": str(uuid4()),
        "schema_version": 1,
        "event_time": "2026-11-11T10:15:22Z",
        "producer": "test",
        "service": "order-service",
        "event_type": "http.request.completed",
        "attributes": {
            "method": "GET",
            "route_template": "http://api.internal/orders?id=123",
            "status_code": 200,
            "duration_ms": 10.0,
        },
    }
    envelope, failure_code, err = parse_and_validate_payload(payload)
    assert envelope is None
    assert failure_code == FailureCode.INVALID_ROUTE_TEMPLATE


def test_validation_malformed_json():
    envelope, failure_code, err = parse_and_validate_payload("{malformed json")
    assert envelope is None
    assert failure_code == FailureCode.INVALID_JSON


def test_sanitize_error_message():
    long_msg = "A" * 500
    sanitized = sanitize_error_message(long_msg, max_length=50)
    assert len(sanitized) == 50
    assert sanitized.endswith("...")
