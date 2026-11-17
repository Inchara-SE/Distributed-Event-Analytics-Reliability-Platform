"""PulseStream Event Envelope Contract Version 1.

Defines the canonical Pydantic v1 HTTP request-completion event model,
partition key derivation, payload hashing, and validation constraints.
"""

from datetime import datetime, timezone
import hashlib
import json
import re
from typing import Literal, Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator

from app.common.time import ensure_utc, to_iso8601_utc

SLUG_PATTERN = re.compile(r"^[a-z0-9-]+$")
HTTP_METHODS = {"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"}


class HttpRequestAttributesV1(BaseModel):
    """HTTP request completion attributes."""

    method: str = Field(..., description="HTTP method in uppercase")
    route_template: str = Field(..., description="Templated route path, e.g. /orders/{order_id}")
    status_code: int = Field(..., ge=100, le=599, description="HTTP status code (100-599)")
    duration_ms: float = Field(..., ge=0.0, description="Request duration in milliseconds (>= 0)")
    trace_id: Optional[str] = Field(default=None, max_length=128, description="Optional trace ID")

    @field_validator("method")
    @classmethod
    def validate_method(cls, v: str) -> str:
        upper = v.strip().upper()
        if upper not in HTTP_METHODS:
            raise ValueError(f"Unsupported HTTP method: {v}. Must be one of {sorted(HTTP_METHODS)}")
        return upper

    @field_validator("route_template")
    @classmethod
    def validate_route_template(cls, v: str) -> str:
        cleaned = v.strip()
        if not cleaned:
            raise ValueError("route_template must not be empty")
        if not cleaned.startswith("/"):
            raise ValueError("route_template must start with '/'")
        if "?" in cleaned or "#" in cleaned:
            raise ValueError("route_template must not contain query parameters or fragments")
        if "://" in cleaned:
            raise ValueError("route_template must not be a full URL")
        return cleaned


class EventEnvelopeV1(BaseModel):
    """Canonical v1 Event Envelope for http.request.completed telemetry."""

    event_id: UUID = Field(..., description="Unique logical identifier for the request event")
    schema_version: int = Field(default=1, description="Event contract version, strictly 1 for MVP")
    event_time: datetime = Field(..., description="UTC timestamp of request completion")
    producer: str = Field(..., min_length=1, max_length=100, description="Emitting component name")
    service: str = Field(..., min_length=1, max_length=100, description="Lowercase service slug")
    event_type: Literal["http.request.completed"] = Field(
        default="http.request.completed", description="Telemetry event type"
    )
    attributes: HttpRequestAttributesV1

    @field_validator("schema_version")
    @classmethod
    def validate_schema_version(cls, v: int) -> int:
        if v != 1:
            raise ValueError(f"Unsupported schema_version: {v}. Only version 1 is supported in MVP.")
        return v

    @field_validator("service")
    @classmethod
    def validate_service_slug(cls, v: str) -> str:
        cleaned = v.strip().lower()
        if not SLUG_PATTERN.match(cleaned):
            raise ValueError(
                f"Invalid service slug: '{v}'. Must contain only lowercase letters, digits, and hyphens."
            )
        return cleaned

    @field_validator("event_time")
    @classmethod
    def normalize_event_time(cls, v: datetime) -> datetime:
        return ensure_utc(v)

    def to_partition_key(self) -> str:
        """Derive the deterministic partition key: {service}|{route_template}."""
        return f"{self.service}|{self.attributes.route_template}"

    def to_canonical_dict(self) -> dict:
        """Convert to a standardized dict with normalized UTC ISO timestamps and string UUIDs."""
        return {
            "event_id": str(self.event_id),
            "schema_version": self.schema_version,
            "event_time": to_iso8601_utc(self.event_time),
            "producer": self.producer,
            "service": self.service,
            "event_type": self.event_type,
            "attributes": {
                "method": self.attributes.method,
                "route_template": self.attributes.route_template,
                "status_code": self.attributes.status_code,
                "duration_ms": round(float(self.attributes.duration_ms), 3),
                "trace_id": self.attributes.trace_id,
            },
        }

    def to_canonical_json(self) -> str:
        """Serialize to compact, deterministic JSON with sorted keys."""
        return json.dumps(self.to_canonical_dict(), sort_keys=True, separators=(",", ":"))

    def compute_payload_hash(self) -> str:
        """Compute SHA-256 hex digest of the canonical JSON payload."""
        return hashlib.sha256(self.to_canonical_json().encode("utf-8")).hexdigest()
