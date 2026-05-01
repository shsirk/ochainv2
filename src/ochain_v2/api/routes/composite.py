"""GET /api/v2/composite/{symbol}?date=&expiry=&tf=&to_idx=

Returns chain + gex + scalper + alerts in one round-trip, fanned out via asyncio.gather.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from ochain_v2.api.deps import CacheDep, MetaDep, ReaderDep
from ochain_v2.api.routes.chain import _build_summary, _df_to_records, _parse_date, _parse_tf
from ochain_v2.api.routes.gex import _DEFAULT_LOT
from ochain_v2.analyzers.gex import compute_gex
from ochain_v2.analyzers.strategies import get_strategy
from ochain_v2.analyzers.strategies.base import AnalysisContext

log = logging.getLogger(__name__)
router = APIRouter()


@router.get("/api/v2/composite/{symbol}")
async def api_v2_composite(
    symbol: str,
    reader: ReaderDep,
    meta: MetaDep,
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
    snaps = reader.get_snapshot_list(symbol, trade_date, expiry, timeframe_sec=tf_sec)
    if not snaps:
        raise HTTPException(404, "No snapshots")

    if to_idx < 0:
        to_idx = len(snaps) + to_idx
    to_idx = max(0, min(to_idx, len(snaps) - 1))
    sid = snaps[to_idx]["snapshot_id"]

    loop = asyncio.get_running_loop()

    async def _fetch_chain():
        df = await loop.run_in_executor(None, reader.get_chain_rows, sid)
        spot = reader.get_underlying_ltp(sid) or 0.0
        summary = await loop.run_in_executor(
            None, _build_summary, df, df, df, spot, symbol, expiry, trade_date
        )
        return {"strikes": _df_to_records(df), "summary": summary, "underlying_ltp": spot}

    async def _fetch_gex():
        df = await loop.run_in_executor(None, reader.get_chain_rows, sid)
        spot = reader.get_underlying_ltp(sid) or 0.0
        lot = _DEFAULT_LOT.get(symbol, 50)
        try:
            return await loop.run_in_executor(None, compute_gex, df, spot, lot)
        except Exception:
            return {}

    async def _fetch_scalper():
        df = await loop.run_in_executor(None, reader.get_chain_rows, sid)
        spot = reader.get_underlying_ltp(sid) or 0.0
        lot = _DEFAULT_LOT.get(symbol, 50)
        from datetime import date as _date
        expiry_date = _date.fromisoformat(expiry)
        dte = max(0.0, (expiry_date - trade_date).days)
        try:
            strat = get_strategy(strategy)
            ctx = await loop.run_in_executor(None, AnalysisContext.build, df, spot, symbol, expiry, lot, dte, None)
            return {"signals": strat.signals(ctx), "metrics": strat.metrics(ctx)}
        except Exception:
            return {"signals": [], "metrics": {}}

    async def _fetch_alerts():
        return meta.get_alerts(symbol=symbol, limit=20)

    chain, gex, scalper, alerts = await asyncio.gather(
        _fetch_chain(), _fetch_gex(), _fetch_scalper(), _fetch_alerts()
    )

    return JSONResponse({
        "symbol": symbol,
        "expiry": expiry,
        "trade_date": str(trade_date),
        "underlying_ltp": chain.get("underlying_ltp"),
        "chain": chain,
        "gex": gex,
        "scalper": scalper,
        "alerts": alerts,
    })
