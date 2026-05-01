"""
Collector control API — localhost-only endpoints.

    GET  /collector/api/status
    GET  /collector/api/errors
    POST /collector/api/connect
    POST /collector/api/disconnect
    GET  /collector/api/symbols
    POST /collector/api/symbols
    DEL  /collector/api/symbols/{symbol}
    GET  /collector/api/expiries/{symbol}

These routes are registered on a separate router and should only be mounted
when the API process is also running the collector (single-process mode), or
when the collector exports a status endpoint the API proxies.

For production (two-process mode) these should only bind to 127.0.0.1.
"""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from ochain_v2.api.deps import MetaDep, ReaderDep

log = logging.getLogger(__name__)
router = APIRouter(prefix="/collector/api", tags=["collector"])

# Module-level reference to the Collector instance (set at startup if applicable)
_collector = None


def set_collector(c) -> None:
    global _collector
    _collector = c


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

@router.get("/status")
async def collector_status(meta: MetaDep):
    statuses = meta.get_status()
    collector_info = _collector.status() if _collector is not None else {}
    return JSONResponse({
        "collector": collector_info,
        "instruments": statuses,
    })


@router.get("/errors")
async def collector_errors(meta: MetaDep, limit: int = 50):
    errors = meta.get_recent_errors(limit=limit)
    return JSONResponse({"errors": errors})


# ---------------------------------------------------------------------------
# Connect / disconnect
# ---------------------------------------------------------------------------

@router.post("/connect")
async def collector_connect():
    if _collector is None:
        raise HTTPException(503, "Collector not available in this process")
    try:
        await _collector._broker.connect()
    except Exception as exc:
        raise HTTPException(500, str(exc))
    return JSONResponse({"ok": True})


@router.post("/disconnect")
async def collector_disconnect():
    if _collector is None:
        raise HTTPException(503, "Collector not available in this process")
    try:
        await _collector._broker.disconnect()
    except Exception as exc:
        raise HTTPException(500, str(exc))
    return JSONResponse({"ok": True})


# ---------------------------------------------------------------------------
# Symbols
# ---------------------------------------------------------------------------

@router.get("/symbols")
async def collector_symbols(reader: ReaderDep):
    return JSONResponse(reader.get_symbols())


class SymbolRequest(BaseModel):
    symbol: str
    expiries_per_symbol: int = 2


@router.post("/symbols")
async def add_symbol(req: SymbolRequest):
    if _collector is None:
        raise HTTPException(503, "Collector not available in this process")
    if req.symbol not in _collector._symbols:
        _collector._symbols.append(req.symbol)
    return JSONResponse({"ok": True, "symbols": _collector._symbols})


@router.delete("/symbols/{symbol}")
async def remove_symbol(symbol: str):
    if _collector is None:
        raise HTTPException(503, "Collector not available in this process")
    _collector._symbols = [s for s in _collector._symbols if s != symbol]
    return JSONResponse({"ok": True, "symbols": _collector._symbols})


@router.get("/expiries/{symbol}")
async def collector_expiries(symbol: str, reader: ReaderDep):
    return JSONResponse(reader.get_expiries(symbol))
