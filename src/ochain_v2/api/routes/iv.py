"""GET /api/iv_surface/{symbol}?date=&expiry="""

from __future__ import annotations

import asyncio
from datetime import date
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from ochain_v2.analyzers.iv_surface import compute_iv_smile, compute_iv_surface
from ochain_v2.api.deps import CacheDep, ReaderDep
from ochain_v2.api.routes.chain import _parse_date

router = APIRouter()


@router.get("/api/iv_surface/{symbol}")
async def api_iv_surface(
    symbol: str,
    reader: ReaderDep,
    cache: CacheDep,
    date: Optional[str] = Query(None),
    expiry: Optional[str] = Query(default=""),
):
    trade_date = _parse_date(date)
    expiries = reader.get_expiries(symbol, trade_date)
    if not expiries:
        raise HTTPException(404, f"No expiries for {symbol} on {trade_date}")

    use_expiry = expiry or expiries[0]
    cache_key = (symbol, use_expiry, str(trade_date), "iv")
    cached = cache.window.get(cache_key)
    if cached is not None:
        return JSONResponse(cached)

    loop = asyncio.get_running_loop()
    snaps = reader.get_snapshot_list(symbol, trade_date, use_expiry)
    if not snaps:
        raise HTTPException(404, "No snapshots")

    sid = snaps[-1]["snapshot_id"]
    df = await loop.run_in_executor(None, reader.get_chain_rows, sid)
    spot = reader.get_underlying_ltp(sid) or 0.0

    try:
        expiry_date = date.fromisoformat(use_expiry)
        iv_smile = await loop.run_in_executor(
            None, compute_iv_smile, df, expiry_date, spot
        )
    except Exception:
        iv_smile = {}

    # Multi-expiry surface: collect near + next expiry
    expiries_dict = {}
    for exp in expiries[:3]:
        exp_snaps = reader.get_snapshot_list(symbol, trade_date, exp)
        if exp_snaps:
            exp_df = await loop.run_in_executor(
                None, reader.get_chain_rows, exp_snaps[-1]["snapshot_id"]
            )
            expiries_dict[exp] = exp_df

    try:
        surface = compute_iv_surface(expiries_dict, spot) if expiries_dict else []
    except Exception:
        surface = []

    result = {
        "symbol": symbol,
        "expiry": use_expiry,
        "trade_date": str(trade_date),
        "underlying_ltp": spot,
        "iv_smile": iv_smile,
        "iv_surface": surface,
    }
    cache.window.set(cache_key, result)
    return JSONResponse(result)
