"""GET /api/alerts/{symbol}?limit=&since_id="""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from ochain_v2.api.deps import MetaDep

router = APIRouter()


@router.get("/api/alerts/{symbol}")
async def api_alerts(
    symbol: str,
    meta: MetaDep,
    limit: int = Query(default=100, le=500),
    since_id: int = Query(default=0),
):
    alerts = meta.get_alerts(symbol=symbol, limit=limit, since_id=since_id)
    return JSONResponse({"symbol": symbol, "alerts": alerts})
