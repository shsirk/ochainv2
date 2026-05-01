"""Unit tests for FixtureBroker."""

from __future__ import annotations

import pytest

from ochain_v2.ingestion.brokers.fixtures import FixtureBroker, _next_thursdays
from ochain_v2.ingestion.brokers.base import BrokerProtocol


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------

def test_fixture_broker_is_broker_protocol():
    assert isinstance(FixtureBroker(), BrokerProtocol)


# ---------------------------------------------------------------------------
# connect / disconnect
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_connect_sets_connected():
    b = FixtureBroker()
    assert not b.is_connected
    await b.connect()
    assert b.is_connected
    assert b.status.is_connected


@pytest.mark.asyncio
async def test_disconnect_clears_connected():
    b = FixtureBroker()
    await b.connect()
    await b.disconnect()
    assert not b.is_connected


# ---------------------------------------------------------------------------
# get_expiries
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_expiries_returns_two_thursdays():
    b = FixtureBroker()
    expiries = await b.get_expiries("NIFTY")
    assert len(expiries) == 2
    # Both should parse as ISO dates (no ValueError)
    from datetime import date
    dates = [date.fromisoformat(e) for e in expiries]
    for d in dates:
        assert d.weekday() == 3  # Thursday


# ---------------------------------------------------------------------------
# get_option_chain
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_option_chain_returns_dataframe():
    import pandas as pd
    b = FixtureBroker()
    df = await b.get_option_chain("NIFTY", "2025-05-29")
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0


@pytest.mark.asyncio
async def test_get_option_chain_has_required_columns():
    b = FixtureBroker()
    df = await b.get_option_chain("NIFTY", "2025-05-29")
    required = [
        "strikePrice", "CE_openInterest", "CE_lastPrice", "CE_impliedVolatility",
        "PE_openInterest", "PE_lastPrice", "PE_impliedVolatility",
        "underlyingValue",
    ]
    for col in required:
        assert col in df.columns, f"Missing column: {col}"


@pytest.mark.asyncio
async def test_get_option_chain_deterministic():
    """Same seed → same DataFrame for same (symbol, expiry, call_n)."""
    b1 = FixtureBroker(seed=99)
    b2 = FixtureBroker(seed=99)
    df1 = await b1.get_option_chain("BANKNIFTY", "2025-05-29")
    df2 = await b2.get_option_chain("BANKNIFTY", "2025-05-29")
    import pandas as pd
    pd.testing.assert_frame_equal(df1, df2)


@pytest.mark.asyncio
async def test_different_seeds_produce_different_data():
    b1 = FixtureBroker(seed=1)
    b2 = FixtureBroker(seed=2)
    df1 = await b1.get_option_chain("NIFTY", "2025-05-29")
    df2 = await b2.get_option_chain("NIFTY", "2025-05-29")
    assert not df1["CE_openInterest"].equals(df2["CE_openInterest"])


@pytest.mark.asyncio
async def test_successive_calls_advance_call_count():
    """With call_rate > 0 successive calls should produce slightly different data."""
    b = FixtureBroker(seed=42, call_rate=0.1)
    df1 = await b.get_option_chain("NIFTY", "2025-05-29")
    df2 = await b.get_option_chain("NIFTY", "2025-05-29")
    # underlyingValue may differ due to per-call drift
    assert not df1.equals(df2)


@pytest.mark.asyncio
async def test_request_count_increments():
    b = FixtureBroker()
    assert b.status.request_count == 0
    await b.get_option_chain("NIFTY", "2025-05-29")
    assert b.status.request_count == 1
    await b.get_option_chain("NIFTY", "2025-05-29")
    assert b.status.request_count == 2


# ---------------------------------------------------------------------------
# _next_thursdays helper
# ---------------------------------------------------------------------------

def test_next_thursdays_count():
    from datetime import date
    thursdays = _next_thursdays(3, from_date=date(2025, 5, 1))
    assert len(thursdays) == 3


def test_next_thursdays_are_thursdays():
    from datetime import date
    for t in _next_thursdays(4, from_date=date(2025, 5, 1)):
        assert date.fromisoformat(t).weekday() == 3


def test_next_thursdays_sorted():
    from datetime import date
    ts = _next_thursdays(5, from_date=date(2025, 5, 1))
    assert ts == sorted(ts)
