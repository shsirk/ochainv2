"""GET /api/scalper/{symbol}?date=&expiry=&tf=&to_idx="""

from __future__ import annotations

import asyncio
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from ochain_v2.analyzers.strategies import get_strategy
from ochain_v2.analyzers.strategies.base import AnalysisContext
from ochain_v2.api.deps import CacheDep, ReaderDep
from ochain_v2.api.routes.chain import _parse_date, _parse_tf, _estimate_spot

router = APIRouter()

_DEFAULT_LOT = {"NIFTY": 75, "BANKNIFTY": 30, "FINNIFTY": 65, "MIDCPNIFTY": 120}


@router.get("/api/scalper/{symbol}")
async def api_scalper(
    symbol: str,
    reader: ReaderDep,
    cache: CacheDep,
    date: Optional[str] = Query(None),
    expiry: Optional[str] = Query(default=""),
    tf: Optional[str] = Query(None),
    to_idx: int = Query(-1),
    strategy: str = Query(default="naked_buyer"),
):
    trade_date = _parse_date(date)
    if not expiry:
        expiries = reader.get_expiries(symbol, trade_date)
        expiry = expiries[0] if expiries else ""
    if not expiry:
        raise HTTPException(404, f"No expiries for {symbol}")

    tf_sec = _parse_tf(tf)
    cache_key = (symbol, expiry, str(trade_date), to_idx, tf_sec, strategy, "scalper")
    cached = cache.window.get(cache_key)
    if cached is not None:
        return JSONResponse(cached)

    snaps = reader.get_snapshot_list(symbol, trade_date, expiry, timeframe_sec=tf_sec)
    if not snaps:
        raise HTTPException(404, "No snapshots")
    if to_idx < 0:
        to_idx = len(snaps) + to_idx
    to_idx = max(0, min(to_idx, len(snaps) - 1))

    loop = asyncio.get_running_loop()
    sid = snaps[to_idx]["snapshot_id"]
    sid_prev = snaps[to_idx - 1]["snapshot_id"] if to_idx > 0 else None

    async def _noop():
        return None

    df, df_prev = await asyncio.gather(
        loop.run_in_executor(None, reader.get_chain_rows, sid),
        loop.run_in_executor(None, reader.get_chain_rows, sid_prev) if sid_prev else _noop(),
    )

    spot = reader.get_underlying_ltp(sid) or 0.0
    if not spot:
        spot = _estimate_spot(df)
    lot = _DEFAULT_LOT.get(symbol, 50)

    from datetime import date as _date
    expiry_date = _date.fromisoformat(expiry)
    dte = max(0.0, (expiry_date - trade_date).days)

    try:
        strat = get_strategy(strategy)
        ctx = await loop.run_in_executor(
            None,
            AnalysisContext.build,
            df, spot, symbol, expiry, lot, dte, df_prev,
        )
        signals = strat.signals(ctx)
        metrics = strat.metrics(ctx)
    except Exception as exc:
        signals, metrics = [], {"error": str(exc)}

    result = {
        "symbol": symbol,
        "expiry": expiry,
        "trade_date": str(trade_date),
        "underlying_ltp": spot,
        "strategy": strategy,
        "signals": signals,
        "metrics": metrics,
    }
    cache.window.set(cache_key, result)
    return JSONResponse(result)
