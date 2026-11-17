"""PulseStream time utilities.

Enforces UTC normalization, millisecond precision, window calculations,
and signed processing delay tracking (preserving clock skew).
"""

from datetime import datetime, timezone, timedelta


def now_utc() -> datetime:
    """Return current timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


def ensure_utc(dt: datetime) -> datetime:
    """Normalize datetime to timezone-aware UTC.

    If naive, assume UTC. If aware, convert to UTC timezone.
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def to_iso8601_utc(dt: datetime) -> str:
    """Format datetime as UTC ISO-8601 string with millisecond precision and Z suffix."""
    utc_dt = ensure_utc(dt)
    # Format with 3-digit millisecond precision
    return utc_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def get_minute_window(dt: datetime) -> tuple[datetime, datetime]:
    """Calculate the inclusive minute start and exclusive minute end in UTC.

    For example, 2026-11-11T10:15:22.384Z ->
    (2026-11-11T10:15:00.000Z, 2026-11-11T10:16:00.000Z)
    """
    utc_dt = ensure_utc(dt)
    window_start = utc_dt.replace(second=0, microsecond=0)
    window_end = window_start + timedelta(minutes=1)
    return window_start, window_end


def calculate_processing_delay_ms(event_time: datetime, processed_at: datetime) -> float:
    """Calculate signed processing delay in milliseconds: processed_at - event_time.

    Signed value is retained. Negative values reflect producer clock skew
    and must not be clamped silently to zero.
    """
    utc_event = ensure_utc(event_time)
    utc_proc = ensure_utc(processed_at)
    delta = utc_proc - utc_event
    return delta.total_seconds() * 1000.0


def is_within_correction_horizon(
    event_time: datetime,
    processed_at: datetime,
    horizon_minutes: int = 15,
) -> bool:
    """Check if event arrived within the automatic correction window.

    Returns True if processing delay <= horizon_minutes * 60 seconds.
    Events older than horizon_minutes are excluded from automatic minute mart correction.
    """
    delay_ms = calculate_processing_delay_ms(event_time, processed_at)
    return delay_ms <= (horizon_minutes * 60.0 * 1000.0)
