"""
GET /api/v2/strategies
GET /api/v2/strategies/{name}/signals/{symbol}?date=&expiry=&tf=&to_idx=
"""

from __future__ import annotations

import asyncio
from datetime import date
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from ochain_v2.analyzers.strategies import list_strategies, get_strategy
from ochain_v2.analyzers.strategies.base import AnalysisContext
from ochain_v2.api.deps import CacheDep, ReaderDep
from ochain_v2.api.routes.chain import _parse_date, _parse_tf
from ochain_v2.api.routes.gex import _DEFAULT_LOT

router = APIRouter()


@router.get("/api/v2/strategies")
async def api_v2_strategies():
    strategies = list_strategies()
    return JSONResponse({
        "strategies": [
            {"name": s.name, "display_name": s.display_name, "description": s.description}
            for s in strategies
        ]
    })


@router.get("/api/v2/strategies/{name}/signals/{symbol}")
async def api_v2_strategy_signals(
    name: str,
    symbol: str,
    reader: ReaderDep,
    cache: CacheDep,
    date: Optional[str] = Query(None),
    expiry: Optional[str] = Query(default=""),
    tf: Optional[str] = Query(None),
    to_idx: int = Query(-1),
):
    try:
        strat = get_strategy(name)
    except KeyError:
        raise HTTPException(404, f"Unknown strategy: {name!r}")

    trade_date = _parse_date(date)
    if not expiry:
        expiries = reader.get_expiries(symbol, trade_date)
        expiry = expiries[0] if expiries else ""
    if not expiry:
        raise HTTPException(404, f"No expiries for {symbol}")

    tf_sec = _parse_tf(tf)
    snaps = reader.get_snapshot_list(symbol, trade_date, expiry, timeframe_sec=tf_sec)
    if not snaps:
        raise HTTPException(404, "No snapshots")
    if to_idx < 0:
        to_idx = len(snaps) + to_idx
    to_idx = max(0, min(to_idx, len(snaps) - 1))
    sid = snaps[to_idx]["snapshot_id"]
    sid_prev = snaps[to_idx - 1]["snapshot_id"] if to_idx > 0 else None

    loop = asyncio.get_running_loop()
    async def _noop():
        return None

    df, df_prev = await asyncio.gather(
        loop.run_in_executor(None, reader.get_chain_rows, sid),
        loop.run_in_executor(None, reader.get_chain_rows, sid_prev) if sid_prev else _noop(),
    )

    spot = reader.get_underlying_ltp(sid) or 0.0
    lot = _DEFAULT_LOT.get(symbol, 50)
    expiry_date = date.fromisoformat(expiry)
    dte = max(0.0, (expiry_date - trade_date).days)

    ctx = await loop.run_in_executor(
        None, AnalysisContext.build, df, spot, symbol, expiry, lot, dte, df_prev
    )
    signals = strat.signals(ctx)
    metrics = strat.metrics(ctx)

    return JSONResponse({
        "symbol": symbol,
        "expiry": expiry,
        "trade_date": str(trade_date),
        "strategy": name,
        "signals": signals,
        "metrics": metrics,
    })
