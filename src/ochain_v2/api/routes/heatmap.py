"""GET /api/heatmap/{symbol}?date=&expiry=&metric=&from_bucket=&to_bucket="""

from __future__ import annotations

from datetime import date
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from ochain_v2.api.deps import CacheDep, ReaderDep
from ochain_v2.core.timezones import now_ist, trade_date_ist

router = APIRouter()


@router.get("/api/heatmap/{symbol}")
async def api_heatmap(
    symbol: str,
    reader: ReaderDep,
    cache: CacheDep,
    date: Optional[str] = Query(None),
    expiry: Optional[str] = Query(default=""),
    metric: str = Query(default="ce_oi"),
    from_bucket: int = Query(default=0),
    to_bucket: int = Query(default=374),
):
    trade_date = date_obj(date)
    if not expiry:
        expiries = reader.get_expiries(symbol, trade_date)
        expiry = expiries[0] if expiries else ""
    if not expiry:
        raise HTTPException(404, f"No expiries for {symbol}")

    cache_key = (symbol, expiry, str(trade_date), from_bucket, to_bucket, metric, "heatmap")
    cached = cache.window.get(cache_key)
    if cached is not None:
        return JSONResponse(cached)

    try:
        data = reader.get_heatmap_matrix(
            symbol, expiry, trade_date, metric,
            from_bucket=from_bucket, to_bucket=to_bucket,
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc))

    result = {
        "symbol": symbol,
        "expiry": expiry,
        "trade_date": str(trade_date),
        "metric": metric,
        **data,
    }
    cache.window.set(cache_key, result)
    return JSONResponse(result)


def date_obj(date_str: Optional[str]) -> date:
    if not date_str:
        return trade_date_ist(now_ist())
    return date.fromisoformat(date_str)
