"""
Integration test: FixtureBroker → InstrumentExpiryJob → DuckDB

Verifies the full ingest path without any network calls:
  - FixtureBroker produces deterministic DataFrames
  - InstrumentExpiryJob writes snapshots + deltas correctly
  - DuckDB ends up with the expected row counts
  - First snapshot per day is session_base; subsequent ones are not
  - Delta tables are populated after the second snapshot
"""

from __future__ import annotations

import asyncio
from datetime import date

import duckdb
import pytest

from ochain_v2.db.duckdb_store import DuckDBStore
from ochain_v2.ingestion.brokers.fixtures import FixtureBroker
from ochain_v2.ingestion.circuit_breaker import CircuitBreaker
from ochain_v2.ingestion.job import InstrumentExpiryJob
from ochain_v2.ingestion.live_publisher import LivePublisher
from ochain_v2.ingestion.token_bucket import AsyncTokenBucket


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def duck_conn():
    conn = duckdb.connect(":memory:")
    from tests.conftest import _apply_schema
    _apply_schema(conn)
    yield conn
    conn.close()


@pytest.fixture
def store(duck_conn):
    return DuckDBStore(_conn=duck_conn)


@pytest.fixture
def broker():
    return FixtureBroker(seed=7, num_strikes=10)


@pytest.fixture
def publisher():
    return LivePublisher()


def _make_job(broker, store, publisher, session_base_dates=None):
    return InstrumentExpiryJob(
        symbol="NIFTY",
        expiry="2025-05-29",
        broker=broker,
        store=store,
        publisher=publisher,
        token_bucket=AsyncTokenBucket(rate=1000.0),
        circuit_breaker=CircuitBreaker(),
        poll_interval=0.0,
        session_base_dates=session_base_dates if session_base_dates is not None else {},
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_single_snapshot_written(broker, store, publisher):
    job = _make_job(broker, store, publisher)
    await broker.connect()
    sid = await job.run_once()
    assert isinstance(sid, int)

    # snapshots table has 1 row
    rows = store._conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
    assert rows == 1


@pytest.mark.asyncio
async def test_chain_rows_written(broker, store, publisher):
    job = _make_job(broker, store, publisher)
    await broker.connect()
    await job.run_once()

    n_rows = store._conn.execute("SELECT COUNT(*) FROM chain_rows").fetchone()[0]
    assert n_rows == 10  # FixtureBroker with num_strikes=10


@pytest.mark.asyncio
async def test_first_snapshot_is_session_base(broker, store, publisher):
    job = _make_job(broker, store, publisher, session_base_dates={})
    await broker.connect()
    await job.run_once()

    is_base = store._conn.execute(
        "SELECT is_session_base FROM snapshots LIMIT 1"
    ).fetchone()[0]
    assert bool(is_base) is True


@pytest.mark.asyncio
async def test_second_snapshot_not_session_base(broker, store, publisher):
    from ochain_v2.core.timezones import now_ist, trade_date_ist
    today = trade_date_ist(now_ist())
    sbd = {("NIFTY", "2025-05-29"): today}
    job = _make_job(broker, store, publisher, session_base_dates=sbd)
    await broker.connect()
    await job.run_once()

    is_base = store._conn.execute(
        "SELECT is_session_base FROM snapshots LIMIT 1"
    ).fetchone()[0]
    assert bool(is_base) is False


@pytest.mark.asyncio
async def test_delta_prev_null_for_first_snapshot(broker, store, publisher):
    job = _make_job(broker, store, publisher)
    await broker.connect()
    await job.run_once()

    # For the very first snapshot, all chain_deltas_prev entries should have
    # ref_available=False
    n_available = store._conn.execute(
        "SELECT COUNT(*) FROM chain_deltas_prev WHERE ref_available = TRUE"
    ).fetchone()[0]
    assert n_available == 0


@pytest.mark.asyncio
async def test_delta_prev_populated_after_two_snapshots(broker, store, publisher):
    sbd: dict = {}
    job = _make_job(broker, store, publisher, session_base_dates=sbd)
    await broker.connect()
    await job.run_once()

    # Second snapshot — reuse same session_base_dates so it's not flagged as base
    from ochain_v2.core.timezones import now_ist, trade_date_ist
    sbd[("NIFTY", "2025-05-29")] = trade_date_ist(now_ist())
    job2 = _make_job(broker, store, publisher, session_base_dates=sbd)
    await job2.run_once()

    # Second snapshot should have prev deltas available
    n_available = store._conn.execute(
        """
        SELECT COUNT(*) FROM chain_deltas_prev dp
        JOIN snapshots s USING (snapshot_id)
        WHERE dp.ref_available = TRUE
        AND s.snapshot_id = (SELECT MAX(snapshot_id) FROM snapshots)
        """
    ).fetchone()[0]
    assert n_available > 0


@pytest.mark.asyncio
async def test_live_event_published(broker, store, publisher):
    job = _make_job(broker, store, publisher)
    await broker.connect()

    received: list[dict] = []

    async def consume():
        async for ev in publisher.subscribe():
            received.append(ev)
            return

    task = asyncio.create_task(consume())
    await asyncio.sleep(0)

    await job.run_once()
    await task

    assert len(received) == 1
    assert received[0]["symbol"] == "NIFTY"
    assert received[0]["expiry"] == "2025-05-29"
    assert "snapshot_id" in received[0]


@pytest.mark.asyncio
async def test_three_snapshots_row_counts(broker, store, publisher):
    sbd: dict = {}
    for i in range(3):
        if i > 0:
            from ochain_v2.core.timezones import now_ist, trade_date_ist
            sbd[("NIFTY", "2025-05-29")] = trade_date_ist(now_ist())
        job = _make_job(broker, store, publisher, session_base_dates=sbd)
        await broker.connect()
        await job.run_once()

    n_snaps = store._conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
    n_chain = store._conn.execute("SELECT COUNT(*) FROM chain_rows").fetchone()[0]
    assert n_snaps == 3
    assert n_chain == 30  # 3 snapshots × 10 strikes
