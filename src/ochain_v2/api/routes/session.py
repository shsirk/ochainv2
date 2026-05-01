"""GET /api/v2/session/{symbol}?expiry="""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from ochain_v2.api.deps import ReaderDep
from ochain_v2.api.routes.chain import _parse_date
from ochain_v2.core.market_hours import is_market_open
from ochain_v2.core.timezones import now_ist, trade_date_ist

router = APIRouter()


@router.get("/api/v2/session/{symbol}")
async def api_v2_session(
    symbol: str,
    reader: ReaderDep,
    expiry: Optional[str] = Query(default=""),
    date: Optional[str] = Query(None),
):
    trade_date = _parse_date(date)
    if not expiry:
        expiries = reader.get_expiries(symbol, trade_date)
        expiry = expiries[0] if expiries else ""

    snaps = reader.get_snapshot_list(symbol, trade_date, expiry) if expiry else []
    latest = snaps[-1] if snaps else None
    base_sid = reader.get_session_base_snapshot_id(symbol, expiry, trade_date) if expiry else None
    spot = reader.get_underlying_ltp(latest["snapshot_id"]) if latest else None

    return JSONResponse({
        "symbol": symbol,
        "expiry": expiry,
        "trade_date": str(trade_date),
        "market_open": is_market_open(),
        "latest_snapshot_id": latest["snapshot_id"] if latest else None,
        "latest_ts": latest["ts"] if latest else None,
        "session_base_snapshot_id": base_sid,
        "underlying_ltp": spot,
        "total_snapshots": len(snaps),
    })
