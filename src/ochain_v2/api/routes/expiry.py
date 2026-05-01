"""
Expiry / date / snapshot metadata endpoints.

These are thin wrappers over DuckDBReader methods, kept separate so the
chain router stays focused on analysis payloads.
"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from ochain_v2.api.deps import ReaderDep
from ochain_v2.api.routes.chain import _parse_date, _parse_tf

router = APIRouter()


@router.get("/api/expiries/{symbol}")
async def api_expiries(
    symbol: str,
    reader: ReaderDep,
    date: Optional[str] = Query(None),
):
    trade_date = _parse_date(date) if date else None
    expiries = reader.get_expiries(symbol, trade_date)
    return JSONResponse({"symbol": symbol, "expiries": expiries})
