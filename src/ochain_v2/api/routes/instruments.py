"""GET /api/v2/instruments"""

from __future__ import annotations

from pathlib import Path

import yaml
from fastapi import APIRouter
from fastapi.responses import JSONResponse

router = APIRouter()


@router.get("/api/v2/instruments")
async def api_v2_instruments():
    instruments_file = Path("config/instruments.yaml")
    if not instruments_file.exists():
        return JSONResponse({"instruments": []})
    data = yaml.safe_load(instruments_file.read_text()) or {}
    items = []
    for sym, cfg in (data.get("instruments") or {}).items():
        items.append({
            "symbol":      sym,
            "lot_size":    cfg.get("lot_size", 1),
            "tick_size":   cfg.get("tick_size", 0.05),
            "strike_step": cfg.get("strike_step", 50),
            "num_strikes": cfg.get("num_strikes", 20),
            "exchange":    cfg.get("exchange", "NSE"),
            "is_index":    cfg.get("is_index", True),
        })
    return JSONResponse({"instruments": items})
