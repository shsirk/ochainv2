"""
WS /ws/live/{symbol}

Pushes a JSON event to connected clients on every new snapshot ingested
for the given symbol.

Event format:
    {
        "event":       "snapshot",
        "symbol":      "NIFTY",
        "expiry":      "2025-05-29",
        "snapshot_id": 1234567890,
        "ts":          "2025-05-01T09:15:00+05:30",
        "summary":     { ... }          # optional; omitted if not built yet
    }

Clients outside market hours receive a single ``{"event": "market_closed"}``
message and the connection is kept open — the server will push again when
the next snapshot arrives (i.e. when the market opens next day).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from ochain_v2.ingestion.live_publisher import LivePublisher

log = logging.getLogger(__name__)
router = APIRouter()

# Module-level publisher reference — set at startup in main.py
_publisher: Optional[LivePublisher] = None


def set_publisher(pub: LivePublisher) -> None:
    global _publisher
    _publisher = pub


@router.websocket("/ws/live/{symbol}")
async def ws_live(websocket: WebSocket, symbol: str):
    await websocket.accept()
    log.info("WS /ws/live/%s connected from %s", symbol, websocket.client)

    if _publisher is None:
        await websocket.send_json({"event": "error", "detail": "Publisher not initialised"})
        await websocket.close()
        return

    try:
        async for event in _publisher.subscribe():
            if event.get("symbol") != symbol:
                continue  # filter by symbol
            try:
                await websocket.send_json(event)
            except WebSocketDisconnect:
                break
            except Exception as exc:
                log.warning("WS send error for %s: %s", symbol, exc)
                break
    except WebSocketDisconnect:
        pass
    finally:
        log.info("WS /ws/live/%s disconnected", symbol)
