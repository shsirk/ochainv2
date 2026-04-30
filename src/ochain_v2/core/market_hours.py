"""
NSE market-hours logic for the OChain v2 collector scheduler.

Responsibilities:
  - Determine whether the market is currently open for trading.
  - Find the next market open time from any given datetime.
  - Provide session bounds (open, close) for a given trading date.
  - Load NSE holidays from the project YAML calendar.

All datetimes in/out are IST-aware unless noted.
"""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import yaml

from ochain_v2.core.timezones import IST, now_ist, to_ist


# ---------------------------------------------------------------------------
# Default session times (configurable via settings)
# ---------------------------------------------------------------------------

_DEFAULT_OPEN = (9, 15)    # HH, MM
_DEFAULT_CLOSE = (15, 30)  # HH, MM


class MarketHours:
    """
    Encapsulates NSE market-hours logic for a fixed set of holidays and
    session open/close times.

    Designed to be instantiated once at startup (or in tests with a small
    fixture holiday set) and reused across the process lifetime.
    """

    def __init__(
        self,
        holidays: set[date],
        open_hhmm: tuple[int, int] = _DEFAULT_OPEN,
        close_hhmm: tuple[int, int] = _DEFAULT_CLOSE,
    ) -> None:
        self._holidays = frozenset(holidays)
        self._open_h, self._open_m = open_hhmm
        self._close_h, self._close_m = close_hhmm

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def is_trading_day(self, d: date) -> bool:
        """True if *d* is a weekday and not an NSE holiday."""
        return d.weekday() < 5 and d not in self._holidays

    def session_open(self, d: date) -> datetime:
        """Return the market-open datetime (IST) for trading date *d*."""
        return IST.localize(
            datetime(d.year, d.month, d.day, self._open_h, self._open_m, 0)
        )

    def session_close(self, d: date) -> datetime:
        """Return the market-close datetime (IST) for trading date *d*."""
        return IST.localize(
            datetime(d.year, d.month, d.day, self._close_h, self._close_m, 0)
        )

    def session_bounds(self, d: date) -> tuple[datetime, datetime]:
        """Return (open, close) as IST-aware datetimes for trading date *d*."""
        return self.session_open(d), self.session_close(d)

    def is_market_open(self, dt: Optional[datetime] = None) -> bool:
        """
        True if the market is accepting trades at *dt* (defaults to now).

        Open interval: [session_open, session_close)
        i.e. exactly at close → market is closed.
        """
        dt = to_ist(dt) if dt is not None else now_ist()
        d = dt.date()
        if not self.is_trading_day(d):
            return False
        open_dt = self.session_open(d)
        close_dt = self.session_close(d)
        return open_dt <= dt < close_dt

    def next_open(self, dt: Optional[datetime] = None) -> datetime:
        """
        Return the next market-open datetime strictly after *dt*.

        If the market is currently open, returns the open of the NEXT
        trading session (useful for scheduling end-of-day tasks).
        If we are before today's open on a trading day, returns today's open.
        """
        dt = to_ist(dt) if dt is not None else now_ist()
        d = dt.date()

        # If before today's open on a trading day, return today's open
        if self.is_trading_day(d) and dt < self.session_open(d):
            return self.session_open(d)

        # Otherwise advance to the next trading day
        candidate = d + timedelta(days=1)
        for _ in range(30):  # safety: no more than 30 calendar days of holidays
            if self.is_trading_day(candidate):
                return self.session_open(candidate)
            candidate += timedelta(days=1)

        raise RuntimeError(
            f"Could not find next trading day within 30 days of {d}. "
            "Check your holidays calendar."
        )

    def seconds_until_open(self, dt: Optional[datetime] = None) -> float:
        """Seconds from *dt* until the next market open. 0.0 if currently open."""
        dt = to_ist(dt) if dt is not None else now_ist()
        if self.is_market_open(dt):
            return 0.0
        open_dt = self.next_open(dt)
        return max(0.0, (open_dt - dt).total_seconds())

    def seconds_until_close(self, dt: Optional[datetime] = None) -> float:
        """Seconds until market close. 0.0 if market is closed."""
        dt = to_ist(dt) if dt is not None else now_ist()
        if not self.is_market_open(dt):
            return 0.0
        close_dt = self.session_close(dt.date())
        return max(0.0, (close_dt - dt).total_seconds())

    def pre_open_time(self, d: date, hhmm: str = "09:00") -> datetime:
        """
        Return the pre-open snapshot capture time for date *d*.
        *hhmm* is a 'HH:MM' string (default '09:00').
        """
        h, m = map(int, hhmm.split(":"))
        return IST.localize(datetime(d.year, d.month, d.day, h, m, 0))

    @property
    def holidays(self) -> frozenset[date]:
        return self._holidays


# ---------------------------------------------------------------------------
# Factory — load from YAML
# ---------------------------------------------------------------------------

def load_market_hours(
    holidays_file: str | Path = "config/nse_holidays.yaml",
    open_time: str = "09:15",
    close_time: str = "15:30",
) -> MarketHours:
    """
    Load holidays from the project YAML calendar and return a MarketHours
    instance configured with the given session times.

    *open_time* and *close_time* are 'HH:MM' strings.
    """
    p = Path(holidays_file)
    holidays: set[date] = set()
    if p.exists():
        with open(p) as f:
            data = yaml.safe_load(f) or {}
        for raw_date in (data.get("holidays") or {}).keys():
            if isinstance(raw_date, date):
                holidays.add(raw_date)
            else:
                holidays.add(date.fromisoformat(str(raw_date)))

    def _parse(hhmm: str) -> tuple[int, int]:
        h, m = map(int, hhmm.split(":"))
        return h, m

    return MarketHours(
        holidays=holidays,
        open_hhmm=_parse(open_time),
        close_hhmm=_parse(close_time),
    )


# ---------------------------------------------------------------------------
# Module-level singleton (lazy, configured by settings on first access)
# ---------------------------------------------------------------------------

_instance: Optional[MarketHours] = None


def get_market_hours() -> MarketHours:
    """
    Return the module-level MarketHours singleton.
    Initialised from settings on first call.
    """
    global _instance
    if _instance is None:
        try:
            from ochain_v2.core.settings import get_settings
            cfg = get_settings()
            _instance = load_market_hours(
                holidays_file=cfg.collector.holidays_file,
                open_time=cfg.collector.market_open,
                close_time=cfg.collector.market_close,
            )
        except Exception:
            # Fallback to defaults (e.g. during tests without settings.yaml)
            _instance = load_market_hours()
    return _instance


def reset_market_hours() -> None:
    """Reset the singleton (useful in tests)."""
    global _instance
    _instance = None


# ---------------------------------------------------------------------------
# Module-level convenience shims
# ---------------------------------------------------------------------------

def is_market_open(dt: Optional[datetime] = None) -> bool:
    return get_market_hours().is_market_open(dt)


def next_open(dt: Optional[datetime] = None) -> datetime:
    return get_market_hours().next_open(dt)


def seconds_until_open(dt: Optional[datetime] = None) -> float:
    return get_market_hours().seconds_until_open(dt)


def seconds_until_close(dt: Optional[datetime] = None) -> float:
    return get_market_hours().seconds_until_close(dt)
