"""Contract test suite validating official JSON schema examples for PulseStream v1."""

from uuid import uuid4
import pytest
from app.contracts.events_v1 import EventEnvelopeV1
from app.contracts.validation import parse_and_validate_payload


def test_official_tad_example_envelope():
    """Validates the exact example given in TAD section 3.1."""
    example_json = {
        "event_id": "8a9b2d50-4fc6-4f6b-8dbe-741bbf7d4e1f",
        "schema_version": 1,
        "event_time": "2026-11-11T10:15:22.384Z",
        "producer": "load-generator",
        "service": "order-service",
        "event_type": "http.request.completed",
        "attributes": {
            "method": "POST",
            "route_template": "/orders",
            "status_code": 201,
            "duration_ms": 42.7,
            "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
        },
    }

    envelope, failure_code, err = parse_and_validate_payload(example_json)
    assert failure_code is None
    assert err is None
    assert envelope is not None
    assert str(envelope.event_id) == "8a9b2d50-4fc6-4f6b-8dbe-741bbf7d4e1f"
    assert envelope.schema_version == 1
    assert envelope.service == "order-service"
    assert envelope.attributes.method == "POST"
    assert envelope.attributes.route_template == "/orders"
    assert envelope.attributes.status_code == 201
    assert envelope.attributes.duration_ms == 42.7
    assert envelope.attributes.trace_id == "4bf92f3577b34da6a3ce929d0e0e4736"
    assert envelope.to_partition_key() == "order-service|/orders"


def test_optional_trace_id_omitted():
    """Attributes without trace_id must parse cleanly with trace_id as None."""
    payload = {
        "event_id": str(uuid4()),
        "schema_version": 1,
        "event_time": "2026-11-11T12:00:00Z",
        "producer": "user-service",
        "service": "user-service",
        "event_type": "http.request.completed",
        "attributes": {
            "method": "GET",
            "route_template": "/users/{user_id}",
            "status_code": 200,
            "duration_ms": 15.2,
        },
    }
    envelope, failure_code, err = parse_and_validate_payload(payload)
    assert envelope is not None
    assert envelope.attributes.trace_id is None
