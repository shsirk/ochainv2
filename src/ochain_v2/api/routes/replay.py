"""GET /api/v2/replay/{symbol}/{date}?expiry=&page=&page_size=

Returns paginated snapshot list for a full trading day — useful for
the back-test harness to walk through a day's data.
"""

from __future__ import annotations

from datetime import date
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from ochain_v2.api.deps import ReaderDep

router = APIRouter()


@router.get("/api/v2/replay/{symbol}/{trade_date}")
async def api_v2_replay(
    symbol: str,
    trade_date: str,
    reader: ReaderDep,
    expiry: Optional[str] = Query(default=""),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, le=375),
):
    try:
        td = date.fromisoformat(trade_date)
    except ValueError:
        raise HTTPException(400, f"Invalid date: {trade_date!r}")

    if not expiry:
        expiries = reader.get_expiries(symbol, td)
        expiry = expiries[0] if expiries else ""
    if not expiry:
        raise HTTPException(404, f"No expiries for {symbol} on {trade_date}")

    snaps = reader.get_snapshot_list(symbol, td, expiry)
    total = len(snaps)
    start = (page - 1) * page_size
    end = start + page_size
    page_snaps = snaps[start:end]

    return JSONResponse({
        "symbol": symbol,
        "expiry": expiry,
        "trade_date": trade_date,
        "total": total,
        "page": page,
        "page_size": page_size,
        "pages": (total + page_size - 1) // page_size if total > 0 else 0,
        "snapshots": page_snaps,
    })
