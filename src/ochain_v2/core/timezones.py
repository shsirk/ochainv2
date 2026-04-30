"""
IST timezone helpers used everywhere in OChain v2.

All timestamps stored in the DB are tz-aware (IST).
All "trade_date" values are the IST calendar date of the timestamp.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytz

IST: pytz.BaseTzInfo = pytz.timezone("Asia/Kolkata")
UTC: timezone = timezone.utc


def now_ist() -> datetime:
    """Current wall-clock time as an IST-aware datetime."""
    return datetime.now(tz=IST)


def to_ist(dt: datetime) -> datetime:
    """
    Convert any datetime to IST-aware.

    - Naive datetime → assumed UTC → converted to IST.
    - Already tz-aware → converted to IST.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(IST)


def to_utc(dt: datetime) -> datetime:
    """Convert any datetime to UTC-aware."""
    if dt.tzinfo is None:
        dt = IST.localize(dt)
    return dt.astimezone(UTC)


def trade_date_ist(ts: datetime) -> date:
    """Return the IST calendar date corresponding to *ts*."""
    return to_ist(ts).date()


def localize_ist(dt: datetime) -> datetime:
    """Attach IST tzinfo to a naive datetime (no conversion)."""
    if dt.tzinfo is not None:
        raise ValueError(f"datetime is already tz-aware: {dt!r}")
    return IST.localize(dt)


def ts_str(dt: datetime) -> str:
    """Canonical timestamp string: 'YYYY-MM-DD HH:MM:SS' in IST."""
    return to_ist(dt).strftime("%Y-%m-%d %H:%M:%S")


def parse_ts(s: str) -> datetime:
    """
    Parse a 'YYYY-MM-DD HH:MM:SS' string and return an IST-aware datetime.
    Treats the string as IST (matches how v1 stored timestamps).
    """
    naive = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    return IST.localize(naive)
