"""Unit tests for InstrumentExpiryJob."""

from __future__ import annotations

import asyncio
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from ochain_v2.ingestion.circuit_breaker import CircuitBreaker
from ochain_v2.ingestion.job import InstrumentExpiryJob
from ochain_v2.ingestion.live_publisher import LivePublisher
from ochain_v2.ingestion.token_bucket import AsyncTokenBucket


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_df(n: int = 5) -> pd.DataFrame:
    """Minimal option-chain DataFrame with broker-native columns."""
    return pd.DataFrame({
        "strikePrice":          [22000.0 + i * 50 for i in range(n)],
        "CE_openInterest":      [10000.0] * n,
        "CE_totalTradedVolume": [1000.0] * n,
        "CE_lastPrice":         [100.0] * n,
        "CE_impliedVolatility": [15.0] * n,
        "CE_bidprice":          [99.5] * n,
        "CE_askPrice":          [100.5] * n,
        "CE_bidQty":            [75.0] * n,
        "CE_askQty":            [75.0] * n,
        "PE_openInterest":      [8000.0] * n,
        "PE_totalTradedVolume": [900.0] * n,
        "PE_lastPrice":         [80.0] * n,
        "PE_impliedVolatility": [14.0] * n,
        "PE_bidprice":          [79.5] * n,
        "PE_askPrice":          [80.5] * n,
        "PE_bidQty":            [75.0] * n,
        "PE_askQty":            [75.0] * n,
        "underlyingValue":      [22500.0] * n,
    })


def _make_job(
    df: pd.DataFrame | None = None,
    broker_error: Exception | None = None,
    save_snapshot_id: int = 1001,
    session_base_dates: dict | None = None,
) -> tuple[InstrumentExpiryJob, MagicMock, MagicMock, LivePublisher]:
    broker = MagicMock()
    broker.get_option_chain = AsyncMock(
        side_effect=broker_error if broker_error else None,
        return_value=df if df is not None else _make_df(),
    )

    store = MagicMock()
    store.save_snapshot = MagicMock(return_value=save_snapshot_id)

    publisher = LivePublisher()

    job = InstrumentExpiryJob(
        symbol="NIFTY",
        expiry="2025-05-29",
        broker=broker,
        store=store,
        publisher=publisher,
        token_bucket=AsyncTokenBucket(rate=1000.0),  # fast
        circuit_breaker=CircuitBreaker(),
        poll_interval=0.01,
        session_base_dates=session_base_dates,
    )
    return job, broker, store, publisher


# ---------------------------------------------------------------------------
# run_once — happy path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_once_returns_snapshot_id():
    job, _, _, _ = _make_job()
    sid = await job.run_once()
    assert sid == 1001


@pytest.mark.asyncio
async def test_run_once_calls_save_snapshot():
    job, _, store, _ = _make_job()
    await job.run_once()
    store.save_snapshot.assert_called_once()


@pytest.mark.asyncio
async def test_run_once_publishes_event():
    publisher = LivePublisher()
    received: list[dict] = []

    async def consumer():
        async for ev in publisher.subscribe():
            received.append(ev)
            return

    task = asyncio.create_task(consumer())
    await asyncio.sleep(0)

    broker = MagicMock()
    broker.get_option_chain = AsyncMock(return_value=_make_df())
    store = MagicMock()
    store.save_snapshot = MagicMock(return_value=42)

    job = InstrumentExpiryJob(
        symbol="NIFTY", expiry="2025-05-29",
        broker=broker, store=store, publisher=publisher,
        token_bucket=AsyncTokenBucket(rate=1000.0),
        circuit_breaker=CircuitBreaker(),
    )
    await job.run_once()
    await task
    assert len(received) == 1
    assert received[0]["symbol"] == "NIFTY"
    assert received[0]["snapshot_id"] == 42


# ---------------------------------------------------------------------------
# session_base detection
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_first_call_is_session_base():
    job, _, store, _ = _make_job(session_base_dates={})
    await job.run_once()
    _, kwargs = store.save_snapshot.call_args
    assert kwargs.get("is_session_base") is True


@pytest.mark.asyncio
async def test_second_same_day_call_is_not_session_base():
    from ochain_v2.core.timezones import now_ist, trade_date_ist
    today = trade_date_ist(now_ist())
    sbd = {("NIFTY", "2025-05-29"): today}
    job, _, store, _ = _make_job(session_base_dates=sbd)
    await job.run_once()
    _, kwargs = store.save_snapshot.call_args
    assert kwargs.get("is_session_base") is False


# ---------------------------------------------------------------------------
# Empty DataFrame → returns None
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_empty_df_returns_none():
    job, _, store, _ = _make_job(df=pd.DataFrame())
    result = await job.run_once()
    assert result is None
    store.save_snapshot.assert_not_called()


# ---------------------------------------------------------------------------
# Circuit breaker integration
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_open_circuit_returns_none():
    from ochain_v2.core.errors import BrokerError
    job, broker, _, _ = _make_job()
    # Trip the circuit breaker
    broker.get_option_chain.side_effect = BrokerError("timeout")
    cb = job._breaker
    # Open the circuit manually
    cb._failures["NIFTY:2025-05-29"] = 5
    cb._state["NIFTY:2025-05-29"] = __import__(
        "ochain_v2.ingestion.circuit_breaker", fromlist=["CircuitState"]
    ).CircuitState.OPEN
    import time
    cb._opened_at["NIFTY:2025-05-29"] = time.monotonic()

    result = await job.run_once()
    assert result is None


# ---------------------------------------------------------------------------
# run_loop stops on stop_event
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_loop_stops_on_event():
    job, _, store, _ = _make_job()
    stop = asyncio.Event()
    stop.set()  # stop immediately

    await job.run_loop(stop)
    # The loop runs at least the first time before checking the event
    # (implementation may vary, so just verify it terminates)
    assert not job._running
