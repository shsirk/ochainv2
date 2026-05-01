"""Unit tests for LivePublisher."""

from __future__ import annotations

import asyncio

import pytest

from ochain_v2.ingestion.live_publisher import LivePublisher


_EVENT = {"symbol": "NIFTY", "expiry": "2025-05-29", "snapshot_id": 1, "ts": "2025-05-01T09:15:00+05:30"}


# ---------------------------------------------------------------------------
# Basic fan-out
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_single_subscriber_receives_event():
    pub = LivePublisher()
    received: list[dict] = []

    async def consume():
        async for ev in pub.subscribe():
            received.append(ev)
            return  # exit after first event

    task = asyncio.create_task(consume())
    await asyncio.sleep(0)  # let subscribe register
    await pub.publish(_EVENT)
    await task
    assert received == [_EVENT]


@pytest.mark.asyncio
async def test_multiple_subscribers_all_receive():
    pub = LivePublisher()
    results: list[list[dict]] = [[], [], []]

    async def consume(idx: int):
        async for ev in pub.subscribe():
            results[idx].append(ev)
            return

    tasks = [asyncio.create_task(consume(i)) for i in range(3)]
    await asyncio.sleep(0)
    await pub.publish(_EVENT)
    await asyncio.gather(*tasks)
    for r in results:
        assert r == [_EVENT]


@pytest.mark.asyncio
async def test_subscriber_count():
    pub = LivePublisher()
    assert pub.subscriber_count == 0

    async def consume():
        async for _ in pub.subscribe():
            await asyncio.sleep(10)  # hold the subscription open

    task = asyncio.create_task(consume())
    await asyncio.sleep(0)
    assert pub.subscriber_count == 1

    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    # After cancellation, the finally block in subscribe() removes the queue
    await asyncio.sleep(0)
    assert pub.subscriber_count == 0


# ---------------------------------------------------------------------------
# Queue-full → drop + warn (no crash)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_full_queue_drops_event_without_crash():
    pub = LivePublisher(maxsize=1)

    # Subscribe but don't consume
    gen = pub.subscribe().__aiter__()
    queue_registered = asyncio.Event()

    async def slow_consumer():
        async for _ in pub.subscribe():
            queue_registered.set()
            await asyncio.sleep(100)  # hold without draining

    task = asyncio.create_task(slow_consumer())
    await asyncio.sleep(0)  # let subscribe register

    # First publish fills the queue
    await pub.publish(_EVENT)
    # Second publish should drop (queue full) without raising
    await pub.publish(_EVENT)

    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


# ---------------------------------------------------------------------------
# No subscribers — publish is a no-op
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_publish_with_no_subscribers():
    pub = LivePublisher()
    # Should not raise
    await pub.publish(_EVENT)
    assert pub.subscriber_count == 0


# ---------------------------------------------------------------------------
# Subscriber unregistered after generator exits
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_subscriber_removed_on_generator_exit():
    pub = LivePublisher()

    async def consume_one():
        async for ev in pub.subscribe():
            return  # exits generator after first event

    task = asyncio.create_task(consume_one())
    await asyncio.sleep(0)
    assert pub.subscriber_count == 1
    await pub.publish(_EVENT)
    await task
    await asyncio.sleep(0)
    assert pub.subscriber_count == 0
