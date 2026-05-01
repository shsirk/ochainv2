"""
GET /api/v2/export/{symbol}.csv?date=&expiry=
GET /api/v2/export/{symbol}.parquet?date=&expiry=
"""

from __future__ import annotations

import asyncio
import io
from datetime import date
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response, StreamingResponse

from ochain_v2.api.deps import ReaderDep
from ochain_v2.api.routes.chain import _parse_date
from ochain_v2.core.timezones import to_ist

router = APIRouter()


@router.get("/api/v2/export/{symbol}.csv")
async def api_v2_export_csv(
    symbol: str,
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

    snaps = reader.get_snapshot_list(symbol, trade_date, expiry)
    if not snaps:
        raise HTTPException(404, "No snapshots")

    loop = asyncio.get_running_loop()
    dfs = await asyncio.gather(*[
        loop.run_in_executor(None, reader.get_chain_rows, s["snapshot_id"])
        for s in snaps
    ])

    import pandas as pd
    combined = pd.concat([df for df in dfs if not df.empty], ignore_index=True)

    buf = io.StringIO()
    combined.to_csv(buf, index=False)
    csv_bytes = buf.getvalue().encode()

    filename = f"{symbol}_{expiry}_{trade_date}.csv"
    return Response(
        content=csv_bytes,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/api/v2/export/{symbol}.parquet")
async def api_v2_export_parquet(
    symbol: str,
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

    snaps = reader.get_snapshot_list(symbol, trade_date, expiry)
    if not snaps:
        raise HTTPException(404, "No snapshots")

    loop = asyncio.get_running_loop()
    dfs = await asyncio.gather(*[
        loop.run_in_executor(None, reader.get_chain_rows, s["snapshot_id"])
        for s in snaps
    ])

    import pandas as pd
    combined = pd.concat([df for df in dfs if not df.empty], ignore_index=True)

    buf = io.BytesIO()
    combined.to_parquet(buf, index=False)
    buf.seek(0)

    filename = f"{symbol}_{expiry}_{trade_date}.parquet"
    return Response(
        content=buf.read(),
        media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
