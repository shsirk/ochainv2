"""
Expected move (1-sigma range) estimator.

Two methods:
  1. IV + DTE  — uses Black-Scholes-derived formula when IV and DTE are available.
  2. Straddle  — uses ATM straddle price when IV/DTE are absent or unreliable.

The 0.85 multiplier accounts for skew: the theoretical straddle slightly
overestimates the realised EM because IV is not constant across strikes.
"""

from __future__ import annotations

import math
from typing import Optional

_STRADDLE_FACTOR = 0.85


def compute_expected_move(
    spot: float,
    atm_iv: Optional[float],
    dte: Optional[float],
    atm_straddle: Optional[float] = None,
) -> dict:
    """
    Estimate the 1-sigma expected move by expiry.

    Parameters
    ----------
    spot          : current underlying price
    atm_iv        : ATM implied volatility as a percentage (e.g. 15.0 for 15%)
    dte           : days to expiry
    atm_straddle  : ATM straddle price (CE_ltp + PE_ltp); used as fallback

    Returns
    -------
    {
        "expected_move":  float,
        "upper":          float,
        "lower":          float,
        "pct_move":       float,    % of spot
        "method":         "iv_dte" | "straddle" | None
    }
    """
    em: Optional[float] = None
    method: Optional[str] = None

    # Primary: IV + DTE
    if atm_iv is not None and dte is not None and atm_iv > 0 and dte > 0:
        iv_decimal = atm_iv / 100.0 if atm_iv > 1.0 else atm_iv
        em     = spot * iv_decimal * math.sqrt(dte / 365.0) * _STRADDLE_FACTOR
        method = "iv_dte"

    # Fallback: straddle price
    elif atm_straddle is not None and atm_straddle > 0:
        em     = atm_straddle * _STRADDLE_FACTOR
        method = "straddle"

    if em is None or spot <= 0:
        return {"expected_move": None, "upper": None, "lower": None,
                "pct_move": None, "method": None}

    return {
        "expected_move": round(em, 2),
        "upper":         round(spot + em, 2),
        "lower":         round(spot - em, 2),
        "pct_move":      round(em / spot * 100, 3),
        "method":        method,
    }
