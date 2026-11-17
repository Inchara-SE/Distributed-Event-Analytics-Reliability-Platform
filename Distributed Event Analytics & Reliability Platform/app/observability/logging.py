"""PulseStream structured logging configuration using structlog.

Provides JSON-formatted, bounded, diagnostic logging across all processes.
"""

import logging
import sys
import structlog


def setup_logging(log_level: str = "INFO") -> None:
    """Configure structured logging for PulseStream services."""
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=numeric_level)

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.set_exc_info,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(numeric_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str):
    """Retrieve a structured logger instance."""
    return structlog.get_logger(name)
