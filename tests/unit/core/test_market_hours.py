"""
Unit tests for core/market_hours.py

Covers:
  - is_market_open: open, closed, boundary conditions, weekends, holidays
  - is_trading_day: weekday, weekend, holiday
  - session_bounds: correct IST times
  - next_open: from closed (after-hours, weekend, holiday chain)
  - seconds_until_open / seconds_until_close: basic arithmetic
"""

from datetime import date, datetime, timedelta

import pytest
import pytz

from ochain_v2.core.market_hours import MarketHours
from ochain_v2.core.timezones import IST

# ---------------------------------------------------------------------------
# Fixture: a MarketHours instance with a known holiday set
# ---------------------------------------------------------------------------

_HOLIDAYS = {
    date(2026, 1, 26),   # Republic Day (Monday)
    date(2026, 3, 20),   # Holi (Friday)
}

@pytest.fixture
def mh() -> MarketHours:
    return MarketHours(holidays=_HOLIDAYS)


def _ist(year: int, month: int, day: int, hour: int, minute: int, second: int = 0) -> datetime:
    return IST.localize(datetime(year, month, day, hour, minute, second))


# ---------------------------------------------------------------------------
# is_trading_day
# ---------------------------------------------------------------------------

class TestIsTradingDay:
    def test_regular_monday(self, mh: MarketHours) -> None:
        assert mh.is_trading_day(date(2026, 2, 2)) is True

    def test_saturday(self, mh: MarketHours) -> None:
        assert mh.is_trading_day(date(2026, 2, 7)) is False

    def test_sunday(self, mh: MarketHours) -> None:
        assert mh.is_trading_day(date(2026, 2, 8)) is False

    def test_holiday_republic_day(self, mh: MarketHours) -> None:
        assert mh.is_trading_day(date(2026, 1, 26)) is False

    def test_holiday_holi(self, mh: MarketHours) -> None:
        assert mh.is_trading_day(date(2026, 3, 20)) is False


# ---------------------------------------------------------------------------
# is_market_open
# ---------------------------------------------------------------------------

class TestIsMarketOpen:
    def test_open_at_session_start(self, mh: MarketHours) -> None:
        # Exactly 09:15 → open
        assert mh.is_market_open(_ist(2026, 2, 2, 9, 15)) is True

    def test_open_midday(self, mh: MarketHours) -> None:
        assert mh.is_market_open(_ist(2026, 2, 2, 12, 0)) is True

    def test_open_one_minute_before_close(self, mh: MarketHours) -> None:
        assert mh.is_market_open(_ist(2026, 2, 2, 15, 29)) is True

    def test_closed_exactly_at_close(self, mh: MarketHours) -> None:
        # 15:30:00 exactly → closed (half-open interval [open, close))
        assert mh.is_market_open(_ist(2026, 2, 2, 15, 30)) is False

    def test_closed_after_close(self, mh: MarketHours) -> None:
        assert mh.is_market_open(_ist(2026, 2, 2, 16, 0)) is False

    def test_closed_before_open(self, mh: MarketHours) -> None:
        assert mh.is_market_open(_ist(2026, 2, 2, 9, 14, 59)) is False

    def test_closed_midnight(self, mh: MarketHours) -> None:
        assert mh.is_market_open(_ist(2026, 2, 2, 0, 0)) is False

    def test_closed_saturday(self, mh: MarketHours) -> None:
        assert mh.is_market_open(_ist(2026, 2, 7, 10, 0)) is False

    def test_closed_sunday(self, mh: MarketHours) -> None:
        assert mh.is_market_open(_ist(2026, 2, 8, 10, 0)) is False

    def test_closed_on_holiday(self, mh: MarketHours) -> None:
        # Republic Day 2026 (Monday) — market closed
        assert mh.is_market_open(_ist(2026, 1, 26, 10, 0)) is False


# ---------------------------------------------------------------------------
# session_bounds
# ---------------------------------------------------------------------------

class TestSessionBounds:
    def test_open_time(self, mh: MarketHours) -> None:
        open_dt, _ = mh.session_bounds(date(2026, 2, 2))
        assert open_dt == _ist(2026, 2, 2, 9, 15)

    def test_close_time(self, mh: MarketHours) -> None:
        _, close_dt = mh.session_bounds(date(2026, 2, 2))
        assert close_dt == _ist(2026, 2, 2, 15, 30)

    def test_tz_is_ist(self, mh: MarketHours) -> None:
        open_dt, close_dt = mh.session_bounds(date(2026, 2, 2))
        assert open_dt.tzinfo is not None
        assert close_dt.tzinfo is not None
        # Both should be IST offset (+05:30)
        from datetime import timezone, timedelta as td
        offset = open_dt.utcoffset()
        assert offset == td(hours=5, minutes=30)


# ---------------------------------------------------------------------------
# next_open
# ---------------------------------------------------------------------------

class TestNextOpen:
    def test_before_open_on_trading_day_returns_today(self, mh: MarketHours) -> None:
        dt = _ist(2026, 2, 2, 8, 0)  # Monday 08:00
        assert mh.next_open(dt) == _ist(2026, 2, 2, 9, 15)

    def test_after_close_on_monday_returns_tuesday(self, mh: MarketHours) -> None:
        dt = _ist(2026, 2, 2, 16, 0)  # Monday after close
        assert mh.next_open(dt) == _ist(2026, 2, 3, 9, 15)

    def test_after_close_on_friday_returns_monday(self, mh: MarketHours) -> None:
        # Friday 2026-03-27 after close → Monday 2026-03-30
        dt = _ist(2026, 3, 27, 16, 0)
        result = mh.next_open(dt)
        assert result == _ist(2026, 3, 30, 9, 15)

    def test_saturday_returns_monday(self, mh: MarketHours) -> None:
        dt = _ist(2026, 2, 7, 12, 0)  # Saturday noon
        assert mh.next_open(dt) == _ist(2026, 2, 9, 9, 15)

    def test_sunday_returns_monday(self, mh: MarketHours) -> None:
        dt = _ist(2026, 2, 8, 12, 0)  # Sunday noon
        assert mh.next_open(dt) == _ist(2026, 2, 9, 9, 15)

    def test_holiday_monday_returns_tuesday(self, mh: MarketHours) -> None:
        # Republic Day 2026 is Monday Jan 26 → next open is Tuesday Jan 27
        dt = _ist(2026, 1, 25, 16, 0)  # Sunday after a normal Friday
        result = mh.next_open(dt)
        # Jan 26 is holiday, so should skip to Jan 27
        assert result == _ist(2026, 1, 27, 9, 15)

    def test_holiday_friday_returns_following_monday(self, mh: MarketHours) -> None:
        # Holi 2026 is Friday Mar 20 → next after Thursday close is Monday Mar 23
        dt = _ist(2026, 3, 19, 16, 0)  # Thursday after close
        result = mh.next_open(dt)
        assert result == _ist(2026, 3, 23, 9, 15)

    def test_during_open_returns_next_day_open(self, mh: MarketHours) -> None:
        # Market is open → next_open returns NEXT session
        dt = _ist(2026, 2, 2, 11, 0)  # Monday midday
        result = mh.next_open(dt)
        assert result == _ist(2026, 2, 3, 9, 15)


# ---------------------------------------------------------------------------
# seconds_until_open / seconds_until_close
# ---------------------------------------------------------------------------

class TestSecondsUntil:
    def test_seconds_until_open_when_open(self, mh: MarketHours) -> None:
        dt = _ist(2026, 2, 2, 11, 0)
        assert mh.seconds_until_open(dt) == 0.0

    def test_seconds_until_open_before_open(self, mh: MarketHours) -> None:
        dt = _ist(2026, 2, 2, 9, 14)  # 1 minute before
        assert mh.seconds_until_open(dt) == pytest.approx(60.0)

    def test_seconds_until_close_when_closed(self, mh: MarketHours) -> None:
        dt = _ist(2026, 2, 2, 16, 0)
        assert mh.seconds_until_close(dt) == 0.0

    def test_seconds_until_close_one_minute_before(self, mh: MarketHours) -> None:
        dt = _ist(2026, 2, 2, 15, 29)  # 1 minute before close
        assert mh.seconds_until_close(dt) == pytest.approx(60.0)


# ---------------------------------------------------------------------------
# Custom session times
# ---------------------------------------------------------------------------

class TestCustomSessionTimes:
    def test_custom_open_close(self) -> None:
        mh = MarketHours(holidays=set(), open_hhmm=(10, 0), close_hhmm=(14, 0))
        assert mh.is_market_open(_ist(2026, 2, 2, 10, 0)) is True
        assert mh.is_market_open(_ist(2026, 2, 2, 9, 59)) is False
        assert mh.is_market_open(_ist(2026, 2, 2, 14, 0)) is False
        assert mh.is_market_open(_ist(2026, 2, 2, 13, 59)) is True
