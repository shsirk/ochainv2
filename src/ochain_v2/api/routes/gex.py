"""GET /api/gex/{symbol}?date=&expiry=&tf=&from_idx=&to_idx="""

from __future__ import annotations

import asyncio
from datetime import date
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from ochain_v2.analyzers.gex import compute_gex
from ochain_v2.api.deps import CacheDep, ReaderDep
from ochain_v2.api.routes.chain import _parse_date, _parse_tf, _estimate_spot

router = APIRouter()

_DEFAULT_LOT = {"NIFTY": 75, "BANKNIFTY": 30, "FINNIFTY": 65, "MIDCPNIFTY": 120}


@router.get("/api/gex/{symbol}")
async def api_gex(
    symbol: str,
    reader: ReaderDep,
    cache: CacheDep,
    date: Optional[str] = Query(None),
    expiry: Optional[str] = Query(default=""),
    tf: Optional[str] = Query(None),
    to_idx: int = Query(-1),
):
    trade_date = _parse_date(date)
    if not expiry:
        expiries = reader.get_expiries(symbol, trade_date)
        expiry = expiries[0] if expiries else ""
    if not expiry:
        raise HTTPException(404, f"No expiries for {symbol}")

    tf_sec = _parse_tf(tf)
    cache_key = (symbol, expiry, str(trade_date), to_idx, tf_sec, "gex")
    cached = cache.window.get(cache_key)
    if cached is not None:
        return JSONResponse(cached)

    snaps = reader.get_snapshot_list(symbol, trade_date, expiry, timeframe_sec=tf_sec)
    if not snaps:
        raise HTTPException(404, "No snapshots")
    if to_idx < 0:
        to_idx = len(snaps) + to_idx
    to_idx = max(0, min(to_idx, len(snaps) - 1))
    sid = snaps[to_idx]["snapshot_id"]

    loop = asyncio.get_running_loop()
    df = await loop.run_in_executor(None, reader.get_chain_rows, sid)
    spot = reader.get_underlying_ltp(sid) or 0.0
    if not spot:
        spot = _estimate_spot(df)
    lot = _DEFAULT_LOT.get(symbol, 50)

    try:
        gex_data = await loop.run_in_executor(None, compute_gex, df, spot, lot)
    except Exception as exc:
        gex_data = {"error": str(exc)}

    strike_records = (
        df[["strike", "ce_gamma", "pe_gamma", "ce_oi", "pe_oi"]]
        .where(pd.notna(df[["strike", "ce_gamma", "pe_gamma", "ce_oi", "pe_oi"]]), other=None)
        .to_dict(orient="records")
    ) if not df.empty else []

    result = {
        "symbol": symbol,
        "expiry": expiry,
        "trade_date": str(trade_date),
        "snapshot_ts": snaps[to_idx]["ts"],
        "underlying_ltp": spot,
        "strikes": strike_records,
        **gex_data,
    }
    cache.window.set(cache_key, result)
    return JSONResponse(result)
