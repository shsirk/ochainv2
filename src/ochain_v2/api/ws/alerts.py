"""
WS /ws/alerts

Pushes new alert_events rows as they are written to the meta SQLite DB.

The server polls the alerts table every 2 seconds and pushes any new rows
(``id > last_seen_id``) to all connected clients.  All symbols are pushed
to every connected client — the client filters by symbol if desired.

Event format:
    {
        "event":      "alert",
        "id":         42,
        "ts":         "2025-05-01T09:20:00+05:30",
        "symbol":     "NIFTY",
        "expiry":     "2025-05-29",
        "alert_type": "OI_SPIKE",
        "detail":     "CE OI +15% at 22500",
        "magnitude":  0.15
    }
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from ochain_v2.db.meta_sqlite import MetaDB

log = logging.getLogger(__name__)
router = APIRouter()

_meta: Optional[MetaDB] = None
_POLL_INTERVAL = 2.0  # seconds between DB polls


def set_meta(meta: MetaDB) -> None:
    global _meta
    _meta = meta


@router.websocket("/ws/alerts")
async def ws_alerts(websocket: WebSocket):
    await websocket.accept()
    log.info("WS /ws/alerts connected from %s", websocket.client)

    if _meta is None:
        await websocket.send_json({"event": "error", "detail": "MetaDB not initialised"})
        await websocket.close()
        return

    last_id = 0
    # Send any alerts that already exist
    existing = _meta.get_alerts(limit=50)
    if existing:
        last_id = max(a["id"] for a in existing)
        for alert in reversed(existing):
            try:
                await websocket.send_json({"event": "alert", **alert})
            except Exception:
                return

    try:
        while True:
            await asyncio.sleep(_POLL_INTERVAL)
            new_alerts = _meta.get_alerts(since_id=last_id, limit=100)
            if new_alerts:
                last_id = max(a["id"] for a in new_alerts)
                for alert in reversed(new_alerts):
                    try:
                        await websocket.send_json({"event": "alert", **alert})
                    except WebSocketDisconnect:
                        return
                    except Exception as exc:
                        log.warning("WS alerts send error: %s", exc)
                        return
    except WebSocketDisconnect:
        pass
    except asyncio.CancelledError:
        pass
    finally:
        log.info("WS /ws/alerts disconnected")
