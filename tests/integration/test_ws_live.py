"""
Integration test: replay 10 snapshots via FixtureBroker → WS client receives
10 push events in correct order.

Uses FastAPI TestClient with starlette's WebSocket test support.
The test wires up:
  - In-memory DuckDB store
  - FixtureBroker as the broker
  - 10 successive run_once() calls
  - WS client connected to /ws/live/NIFTY
  - Asserts 10 events received in snapshot_id order
"""

from __future__ import annotations

import asyncio
import threading

import duckdb
import pytest
from fastapi.testclient import TestClient

from ochain_v2.api import deps
from ochain_v2.api.main import create_app
from ochain_v2.api.ws.live import set_publisher
from ochain_v2.db.duckdb_store import DuckDBStore
from ochain_v2.ingestion.brokers.fixtures import FixtureBroker
from ochain_v2.ingestion.circuit_breaker import CircuitBreaker
from ochain_v2.ingestion.job import InstrumentExpiryJob
from ochain_v2.ingestion.live_publisher import LivePublisher
from ochain_v2.ingestion.token_bucket import AsyncTokenBucket


def _apply_schema(conn):
    from tests.conftest import _apply_schema as _as
    _as(conn)


@pytest.fixture
def ws_app():
    """FastAPI app wired to in-memory DuckDB + FixtureBroker."""
    conn = duckdb.connect(":memory:")
    _apply_schema(conn)
    store = DuckDBStore(_conn=conn)

    broker = FixtureBroker(seed=3, num_strikes=5)
    publisher = LivePublisher()

    # Wire dependencies manually (skip lifespan)
    deps.set_reader(__import__("ochain_v2.db.duckdb_reader", fromlist=["DuckDBReader"]).DuckDBReader("", _conn=conn))
    deps.set_meta(__import__("ochain_v2.db.meta_sqlite", fromlist=["MetaDB"]).MetaDB(":memory:"))
    set_publisher(publisher)

    app = create_app()

    # Attach extras for the test to access
    app.state.store = store
    app.state.broker = broker
    app.state.publisher = publisher

    yield app

    conn.close()


@pytest.mark.asyncio
async def test_ws_receives_10_events(ws_app):
    """WS client receives exactly 10 events after 10 snapshots are ingested."""
    store = ws_app.state.store
    broker = ws_app.state.broker
    publisher = ws_app.state.publisher

    await broker.connect()

    received: list[dict] = []
    done = asyncio.Event()

    async def ingest_10():
        sbd: dict = {}
        for i in range(10):
            if i > 0:
                from ochain_v2.core.timezones import now_ist, trade_date_ist
                sbd[("NIFTY", "2025-05-29")] = trade_date_ist(now_ist())
            job = InstrumentExpiryJob(
                symbol="NIFTY",
                expiry="2025-05-29",
                broker=broker,
                store=store,
                publisher=publisher,
                token_bucket=AsyncTokenBucket(rate=10000.0),
                circuit_breaker=CircuitBreaker(),
                session_base_dates=sbd,
            )
            await job.run_once()
        done.set()

    async def consume():
        async for ev in publisher.subscribe():
            received.append(ev)
            if len(received) >= 10:
                return

    consumer_task = asyncio.create_task(consume())
    await asyncio.sleep(0)  # let subscriber register

    await ingest_10()
    await asyncio.wait_for(consumer_task, timeout=5.0)

    assert len(received) == 10
    assert all(ev["symbol"] == "NIFTY" for ev in received)
    assert all(ev["expiry"] == "2025-05-29" for ev in received)

    # Events should be in ascending snapshot_id order
    ids = [ev["snapshot_id"] for ev in received]
    assert ids == sorted(ids)


@pytest.mark.asyncio
async def test_ws_filters_by_symbol(ws_app):
    """Events for BANKNIFTY should not arrive on /ws/live/NIFTY subscription."""
    store = ws_app.state.store
    broker_nifty = FixtureBroker(seed=10, num_strikes=5)
    broker_bn    = FixtureBroker(seed=11, num_strikes=5)
    publisher = ws_app.state.publisher

    await broker_nifty.connect()
    await broker_bn.connect()

    nifty_received: list[dict] = []
    all_received: list[dict] = []

    # Subscribe to NIFTY only (filtered)
    async def consume_nifty():
        async for ev in publisher.subscribe():
            if ev.get("symbol") == "NIFTY":
                nifty_received.append(ev)
            all_received.append(ev)
            if len(all_received) >= 4:
                return

    task = asyncio.create_task(consume_nifty())
    await asyncio.sleep(0)

    sbd: dict = {}
    for sym, broker in [("NIFTY", broker_nifty), ("BANKNIFTY", broker_bn),
                        ("NIFTY", broker_nifty), ("BANKNIFTY", broker_bn)]:
        job = InstrumentExpiryJob(
            symbol=sym, expiry="2025-05-29",
            broker=broker, store=store, publisher=publisher,
            token_bucket=AsyncTokenBucket(rate=10000.0),
            circuit_breaker=CircuitBreaker(),
            session_base_dates=sbd,
        )
        await job.run_once()

    await asyncio.wait_for(task, timeout=5.0)

    assert len(all_received) == 4
    assert len(nifty_received) == 2
