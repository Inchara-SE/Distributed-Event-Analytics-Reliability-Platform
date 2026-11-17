"""PulseStream event payload validation utilities.

Used by both API ingestion (for 4xx rejections) and consumer groups (for DLQ routing).
Ensures bounded and sanitized error messages with stable failure codes.
"""

import json
from typing import Any, Optional
from pydantic import ValidationError

from app.contracts.events_v1 import EventEnvelopeV1
from app.common.errors import FailureCode


def sanitize_error_message(message: str, max_length: int = 256) -> str:
    """Sanitize and bound diagnostic error message.

    Strips control characters and truncates to avoid unbounded DLQ strings.
    """
    clean = " ".join(message.split())
    if len(clean) > max_length:
        return clean[: max_length - 3] + "..."
    return clean


def parse_and_validate_payload(
    raw_payload: str | bytes | dict[str, Any],
) -> tuple[Optional[EventEnvelopeV1], Optional[FailureCode], Optional[str]]:
    """Parse and validate an event payload.

    Returns:
        (envelope, None, None) on success
        (None, failure_code, error_message) on failure
    """
    data: dict[str, Any]
    if isinstance(raw_payload, (str, bytes)):
        try:
            data = json.loads(raw_payload)
        except Exception as e:
            return None, FailureCode.INVALID_JSON, sanitize_error_message(f"Malformed JSON: {e}")
    elif isinstance(raw_payload, dict):
        data = raw_payload
    else:
        return None, FailureCode.INVALID_ATTRIBUTE_TYPE, "Payload must be a JSON object or string."

    if not isinstance(data, dict):
        return None, FailureCode.INVALID_ATTRIBUTE_TYPE, "Payload root must be a JSON object."

    # Inspect schema_version upfront
    schema_version = data.get("schema_version")
    if schema_version != 1:
        return (
            None,
            FailureCode.UNSUPPORTED_SCHEMA_VERSION,
            sanitize_error_message(
                f"Unsupported schema_version: {schema_version}. Only version 1 is supported."
            ),
        )

    try:
        envelope = EventEnvelopeV1.model_validate(data)
        return envelope, None, None
    except ValidationError as ve:
        # Determine most specific failure code
        first_err = ve.errors()[0]
        loc = ".".join(str(l) for l in first_err.get("loc", []))
        msg = first_err.get("msg", "Validation error")
        error_type = first_err.get("type", "")

        failure_code = FailureCode.INVALID_ATTRIBUTE_TYPE
        if "missing" in error_type:
            failure_code = FailureCode.MISSING_REQUIRED_FIELD
        elif "status_code" in loc:
            failure_code = FailureCode.INVALID_STATUS_CODE
        elif "duration_ms" in loc:
            failure_code = FailureCode.NEGATIVE_DURATION
        elif "route_template" in loc:
            failure_code = FailureCode.INVALID_ROUTE_TEMPLATE
        elif "service" in loc:
            failure_code = FailureCode.UNREGISTERED_SERVICE

        diag = f"Field '{loc}': {msg}"
        return None, failure_code, sanitize_error_message(diag)
    except Exception as ex:
        return None, FailureCode.INTERNAL_PROCESSOR_ERROR, sanitize_error_message(str(ex))
