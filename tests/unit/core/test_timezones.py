"""Unit tests for core/timezones.py"""

from datetime import date, datetime, timezone, timedelta

import pytest
import pytz

from ochain_v2.core.timezones import (
    IST, to_ist, to_utc, trade_date_ist, localize_ist, ts_str, parse_ts, now_ist
)


class TestToIst:
    def test_naive_treated_as_utc(self) -> None:
        naive = datetime(2026, 3, 20, 3, 45, 0)  # 03:45 UTC = 09:15 IST
        result = to_ist(naive)
        assert result.hour == 9
        assert result.minute == 15
        assert result.tzinfo is not None

    def test_utc_aware_converts(self) -> None:
        utc_dt = datetime(2026, 3, 20, 3, 45, 0, tzinfo=timezone.utc)
        result = to_ist(utc_dt)
        assert result.hour == 9
        assert result.minute == 15

    def test_already_ist_no_change(self) -> None:
        ist_dt = IST.localize(datetime(2026, 3, 20, 9, 15, 0))
        result = to_ist(ist_dt)
        assert result == ist_dt


class TestTradeDateIst:
    def test_midday_ist(self) -> None:
        dt = IST.localize(datetime(2026, 3, 20, 12, 0, 0))
        assert trade_date_ist(dt) == date(2026, 3, 20)

    def test_midnight_utc_is_ist_morning(self) -> None:
        # Midnight UTC = 05:30 IST (same calendar date in IST)
        dt = datetime(2026, 3, 20, 0, 0, 0, tzinfo=timezone.utc)
        assert trade_date_ist(dt) == date(2026, 3, 20)

    def test_late_evening_utc_is_next_ist_day(self) -> None:
        # 23:00 UTC = 04:30 IST next day
        dt = datetime(2026, 3, 19, 23, 0, 0, tzinfo=timezone.utc)
        assert trade_date_ist(dt) == date(2026, 3, 20)


class TestLocalizeIst:
    def test_attaches_ist(self) -> None:
        naive = datetime(2026, 3, 20, 9, 15)
        result = localize_ist(naive)
        offset = result.utcoffset()
        assert offset == timedelta(hours=5, minutes=30)

    def test_raises_on_aware(self) -> None:
        aware = IST.localize(datetime(2026, 3, 20, 9, 15))
        with pytest.raises(ValueError):
            localize_ist(aware)


class TestTsStr:
    def test_format(self) -> None:
        dt = IST.localize(datetime(2026, 3, 20, 9, 15, 0))
        assert ts_str(dt) == "2026-03-20 09:15:00"


class TestParseTs:
    def test_roundtrip(self) -> None:
        original = IST.localize(datetime(2026, 3, 20, 9, 15, 0))
        s = ts_str(original)
        parsed = parse_ts(s)
        assert parsed == original

    def test_tzinfo_is_ist(self) -> None:
        parsed = parse_ts("2026-03-20 09:15:00")
        assert parsed.tzinfo is not None
        assert parsed.utcoffset() == timedelta(hours=5, minutes=30)


class TestNowIst:
    def test_returns_ist_aware(self) -> None:
        dt = now_ist()
        assert dt.tzinfo is not None
        assert dt.utcoffset() == timedelta(hours=5, minutes=30)
