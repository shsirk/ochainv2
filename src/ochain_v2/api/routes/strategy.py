"""POST /api/strategy/payoff"""

from __future__ import annotations

import asyncio
import math

import numpy as np
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from ochain_v2.analyzers.greeks import compute_payoff, compute_pop

router = APIRouter()


class PayoffLeg(BaseModel):
    option_type: str    # "CE" or "PE"
    strike: float
    premium: float
    quantity: int       # +ve = long, -ve = short
    expiry: str = ""


class PayoffRequest(BaseModel):
    legs: list[PayoffLeg]
    spot: float
    iv: float = 15.0
    dte: float = 30.0
    spot_range_pct: float = 10.0


@router.post("/api/strategy/payoff")
async def api_payoff(req: PayoffRequest):
    legs = [
        {
            "option_type": leg.option_type,
            "strike":      leg.strike,
            "premium":     leg.premium,
            "quantity":    leg.quantity,
        }
        for leg in req.legs
    ]

    half = req.spot_range_pct / 100 * req.spot
    spot_range = list(
        np.linspace(req.spot - half, req.spot + half, 201)
    )

    loop = asyncio.get_running_loop()
    pnl = await loop.run_in_executor(None, compute_payoff, legs, spot_range)
    pop = await loop.run_in_executor(None, compute_pop, legs, req.spot, req.iv, req.dte)

    pnl_list = [round(float(v), 2) for v in pnl]
    max_profit = max(pnl_list) if pnl_list else None
    max_loss = min(pnl_list) if pnl_list else None

    # Breakevens: sign changes in PnL
    breakevens = []
    for i in range(1, len(pnl_list)):
        if pnl_list[i - 1] * pnl_list[i] < 0:
            # Linear interpolation
            x0, x1 = spot_range[i - 1], spot_range[i]
            y0, y1 = pnl_list[i - 1], pnl_list[i]
            be = x0 - y0 * (x1 - x0) / (y1 - y0)
            breakevens.append(round(be, 2))

    return JSONResponse({
        "spot_range": [round(float(s), 2) for s in spot_range],
        "pnl": pnl_list,
        "pop": round(float(pop), 4) if pop is not None else None,
        "max_profit": max_profit,
        "max_loss": max_loss,
        "breakevens": breakevens,
    })
