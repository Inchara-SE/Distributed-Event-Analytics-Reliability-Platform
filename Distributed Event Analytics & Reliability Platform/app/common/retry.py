"""PulseStream retry policy with bounded exponential backoff and jitter."""

import random
import time
from typing import Callable, TypeVar, Any
import structlog

logger = structlog.get_logger(__name__)

T = TypeVar("T")


class RetryPolicy:
    """Configurable exponential backoff retry policy with jitter."""

    def __init__(
        self,
        max_retries: int = 5,
        initial_delay_seconds: float = 0.1,
        max_delay_seconds: float = 5.0,
        backoff_multiplier: float = 2.0,
        jitter_fraction: float = 0.25,
    ):
        self.max_retries = max_retries
        self.initial_delay_seconds = initial_delay_seconds
        self.max_delay_seconds = max_delay_seconds
        self.backoff_multiplier = backoff_multiplier
        self.jitter_fraction = jitter_fraction

    def get_delay(self, attempt: int) -> float:
        """Calculate delay in seconds for given attempt number (1-based)."""
        delay = self.initial_delay_seconds * (self.backoff_multiplier ** (attempt - 1))
        delay = min(delay, self.max_delay_seconds)
        # Apply symmetric jitter
        jitter = delay * self.jitter_fraction * (random.uniform(-1.0, 1.0))
        return max(0.01, delay + jitter)

    def execute_with_retry(
        self,
        func: Callable[..., T],
        retryable_exceptions: tuple[type[Exception], ...],
        operation_name: str = "operation",
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """Execute callable with retries for retryable exceptions.

        Raises the final exception when max_retries is exhausted.
        """
        attempt = 1
        while True:
            try:
                return func(*args, **kwargs)
            except retryable_exceptions as ex:
                if attempt > self.max_retries:
                    logger.error(
                        "retry_exhausted",
                        operation=operation_name,
                        attempt=attempt,
                        max_retries=self.max_retries,
                        error=str(ex),
                    )
                    raise
                delay = self.get_delay(attempt)
                logger.warning(
                    "retry_attempt",
                    operation=operation_name,
                    attempt=attempt,
                    max_retries=self.max_retries,
                    delay_seconds=round(delay, 3),
                    error=str(ex),
                )
                time.sleep(delay)
                attempt += 1
