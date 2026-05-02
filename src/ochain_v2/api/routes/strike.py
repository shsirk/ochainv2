"""GET /api/strike/{symbol}/{strike}?date=&expiry="""

from __future__ import annotations

import asyncio
from datetime import date
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from ochain_v2.api.deps import ReaderDep
from ochain_v2.api.routes.chain import _parse_date

router = APIRouter()


@router.get("/api/strike/{symbol}/{strike}")
async def api_strike_drill(
    symbol: str,
    strike: float,
    reader: ReaderDep,
    date: Optional[str] = Query(None),
    expiry: Optional[str] = Query(default=""),
):
    trade_date = _parse_date(date)
    if not expiry:
        expiries = reader.get_expiries(symbol, trade_date)
        expiry = expiries[0] if expiries else ""
    if not expiry:
        raise HTTPException(404, f"No expiries for {symbol}")

    loop = asyncio.get_running_loop()
    snaps = reader.get_snapshot_list(symbol, trade_date, expiry)
    if not snaps:
        raise HTTPException(404, "No snapshots")

    # Fetch all chain rows for the day and filter to the requested strike
    from datetime import datetime
    first_ts = datetime.fromisoformat(snaps[0]["ts"]) if snaps else None
    last_ts  = datetime.fromisoformat(snaps[-1]["ts"]) if snaps else None

    df = await loop.run_in_executor(
        None,
        reader.get_chain_rows_range,
        symbol, expiry, trade_date, first_ts, last_ts,
    )

    if not df.empty:
        # Filter to the nearest matching strike (within step tolerance)
        min_dist = (df["strike"] - strike).abs().min()
        df = df[((df["strike"] - strike).abs() <= max(min_dist + 0.01, 0.5))]

    from ochain_v2.api.routes.chain import _df_to_records
    rows = _df_to_records(df) if not df.empty else []

    return JSONResponse({
        "symbol": symbol,
        "expiry": expiry,
        "strike": strike,
        "trade_date": str(trade_date),
        "rows": rows,
    })
