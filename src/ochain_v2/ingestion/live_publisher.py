"""
In-process live-event publisher.

The collector calls ``publish()`` after each successful snapshot.
API WebSocket handlers call ``subscribe()`` to receive an async stream
of events.

Design: asyncio.Queue per subscriber — no Redis needed for single-process
deployment.  Swap the backend (Redis pub/sub) by replacing this module
without touching callers.
"""

from __future__ import annotations

import asyncio
import logging
from typing import AsyncGenerator

log = logging.getLogger(__name__)


class LivePublisher:
    """
    Fan-out publisher: one queue per active subscriber.

    Events are plain dicts:
        {
            "symbol":      str,
            "expiry":      str,
            "snapshot_id": int,
            "ts":          str,   # ISO format
            "summary":     dict,  # PCR, ATM IV, etc.
        }
    """

    def __init__(self, maxsize: int = 100) -> None:
        self._maxsize = maxsize
        self._queues: list[asyncio.Queue] = []
        self._lock = asyncio.Lock()

    async def publish(self, event: dict) -> None:
        """Deliver *event* to all current subscribers (non-blocking, drops if full)."""
        async with self._lock:
            queues = list(self._queues)

        for q in queues:
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                log.warning(
                    "LivePublisher: subscriber queue full — dropping event for %s/%s",
                    event.get("symbol"), event.get("expiry"),
                )

    async def subscribe(self) -> AsyncGenerator[dict, None]:
        """
        Async generator that yields events as they arrive.

        Usage::

            async for event in publisher.subscribe():
                await websocket.send_json(event)
        """
        q: asyncio.Queue = asyncio.Queue(maxsize=self._maxsize)
        async with self._lock:
            self._queues.append(q)
        try:
            while True:
                event = await q.get()
                yield event
        finally:
            async with self._lock:
                try:
                    self._queues.remove(q)
                except ValueError:
                    pass

    @property
    def subscriber_count(self) -> int:
        return len(self._queues)
