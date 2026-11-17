"""PulseStream custom exceptions and failure codes."""

from enum import Enum


class FailureCode(str, Enum):
    INVALID_JSON = "INVALID_JSON"
    UNSUPPORTED_SCHEMA_VERSION = "UNSUPPORTED_SCHEMA_VERSION"
    MISSING_REQUIRED_FIELD = "MISSING_REQUIRED_FIELD"
    INVALID_ATTRIBUTE_TYPE = "INVALID_ATTRIBUTE_TYPE"
    INVALID_STATUS_CODE = "INVALID_STATUS_CODE"
    NEGATIVE_DURATION = "NEGATIVE_DURATION"
    INVALID_ROUTE_TEMPLATE = "INVALID_ROUTE_TEMPLATE"
    UNREGISTERED_SERVICE = "UNREGISTERED_SERVICE"
    INTERNAL_PROCESSOR_ERROR = "INTERNAL_PROCESSOR_ERROR"


class PulseStreamError(Exception):
    """Base exception for PulseStream platform."""


class ContractValidationError(PulseStreamError):
    """Raised when an event fails envelope or attribute contract validation."""

    def __init__(self, message: str, failure_code: FailureCode = FailureCode.INVALID_ATTRIBUTE_TYPE):
        super().__init__(message)
        self.failure_code = failure_code


class BrokerDeliveryError(PulseStreamError):
    """Raised when publishing to the broker fails or times out."""


class StorageOutageError(PulseStreamError):
    """Raised when storage sink (ClickHouse/PostgreSQL) fails or is unreachable."""


class DLQPublishError(PulseStreamError):
    """Raised when forwarding an invalid record to DLQ fails."""
