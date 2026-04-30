"""
Black-Scholes pricing and Greeks, plus multi-leg payoff utilities.

Uses only Python stdlib (math) and numpy — no scipy dependency.

Conventions
-----------
- S  : underlying spot price
- K  : strike price
- T  : time to expiry in years (DTE / 365)
- r  : annualised risk-free rate (e.g. 0.07 for 7%)
- sigma : annualised implied volatility as a decimal (e.g. 0.15 for 15%)
- option_type : "CE" or "PE"

Greeks are per-unit (not per lot). Multiply by lot_size and position size
in the strategy layer.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Literal, Optional

import numpy as np

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_SQRT2 = math.sqrt(2.0)
_SQRT2PI = math.sqrt(2.0 * math.pi)
_MIN_T = 1.0 / (365 * 24 * 60)   # 1 minute floor — avoids division-by-zero at expiry
_MIN_SIGMA = 1e-6
_DEFAULT_RATE = 0.07              # India risk-free rate (approx)


def _cdf(x: float) -> float:
    """Standard normal CDF using math.erf (exact)."""
    return (1.0 + math.erf(x / _SQRT2)) / 2.0


def _pdf(x: float) -> float:
    """Standard normal PDF."""
    return math.exp(-0.5 * x * x) / _SQRT2PI


def _d1d2(S: float, K: float, T: float, r: float, sigma: float) -> tuple[float, float]:
    T = max(T, _MIN_T)
    sigma = max(sigma, _MIN_SIGMA)
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    return d1, d2


# ---------------------------------------------------------------------------
# Pricing
# ---------------------------------------------------------------------------

def bs_price(
    S: float,
    K: float,
    T: float,
    r: float,
    sigma: float,
    option_type: Literal["CE", "PE"],
) -> float:
    """Black-Scholes theoretical option price."""
    T = max(T, _MIN_T)
    d1, d2 = _d1d2(S, K, T, r, sigma)
    disc = math.exp(-r * T)
    if option_type == "CE":
        return S * _cdf(d1) - K * disc * _cdf(d2)
    return K * disc * _cdf(-d2) - S * _cdf(-d1)


# ---------------------------------------------------------------------------
# Greeks
# ---------------------------------------------------------------------------

def bs_delta(
    S: float,
    K: float,
    T: float,
    r: float,
    sigma: float,
    option_type: Literal["CE", "PE"],
) -> float:
    """Delta: sensitivity to a 1-unit change in S."""
    d1, _ = _d1d2(S, K, T, r, sigma)
    return _cdf(d1) if option_type == "CE" else _cdf(d1) - 1.0


def bs_gamma(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """Gamma: rate of change of delta per 1-unit move in S (same for CE and PE)."""
    T = max(T, _MIN_T)
    sigma = max(sigma, _MIN_SIGMA)
    d1, _ = _d1d2(S, K, T, r, sigma)
    return _pdf(d1) / (S * sigma * math.sqrt(T))


def bs_theta(
    S: float,
    K: float,
    T: float,
    r: float,
    sigma: float,
    option_type: Literal["CE", "PE"],
) -> float:
    """Theta: time decay per calendar day (negative for long positions)."""
    T = max(T, _MIN_T)
    sigma = max(sigma, _MIN_SIGMA)
    d1, d2 = _d1d2(S, K, T, r, sigma)
    disc = math.exp(-r * T)
    common = -(S * _pdf(d1) * sigma) / (2.0 * math.sqrt(T))
    if option_type == "CE":
        return (common - r * K * disc * _cdf(d2)) / 365.0
    return (common + r * K * disc * _cdf(-d2)) / 365.0


def bs_vega(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """Vega: sensitivity to a 1-percentage-point change in IV (same for CE and PE)."""
    T = max(T, _MIN_T)
    d1, _ = _d1d2(S, K, T, r, sigma)
    return S * _pdf(d1) * math.sqrt(T) * 0.01


def bs_iv(
    market_price: float,
    S: float,
    K: float,
    T: float,
    r: float,
    option_type: Literal["CE", "PE"],
    max_iter: int = 200,
    tol: float = 1e-6,
) -> Optional[float]:
    """
    Implied volatility via bisection.

    Returns None if the market price is outside the valid BS range
    (e.g. below intrinsic value or negative).
    """
    T = max(T, _MIN_T)
    intrinsic = max(0.0, S - K) if option_type == "CE" else max(0.0, K - S)
    if market_price <= intrinsic:
        return None

    lo, hi = 1e-4, 10.0   # sigma search range
    for _ in range(max_iter):
        mid = (lo + hi) / 2.0
        price = bs_price(S, K, T, r, mid, option_type)
        if abs(price - market_price) < tol:
            return mid
        if price < market_price:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2.0


# ---------------------------------------------------------------------------
# Multi-leg payoff
# ---------------------------------------------------------------------------

@dataclass
class Leg:
    """One leg of a multi-leg options strategy."""
    option_type:  Literal["CE", "PE"]
    strike:       float
    premium:      float          # price paid (positive = bought, negative = sold)
    quantity:     int   = 1      # +ve = long, -ve = short
    lot_size:     int   = 1
    dte:          float = 30.0
    iv:           float = 0.15


def compute_payoff(
    legs: list[Leg],
    spot_range: Optional[np.ndarray] = None,
    spot: Optional[float] = None,
) -> dict:
    """
    Compute P&L profile across a range of underlying prices.

    Parameters
    ----------
    legs       : list of Leg dataclasses
    spot_range : 1-D numpy array of underlying prices to evaluate
    spot       : current spot (used to build a default range if spot_range is None)

    Returns
    -------
    {
        "spot_range":  [float, ...],
        "pnl":         [float, ...],
        "breakevens":  [float, ...],
        "max_profit":  float | None,
        "max_loss":    float | None,
    }
    """
    if spot_range is None:
        centre = spot or legs[0].strike
        spot_range = np.linspace(centre * 0.85, centre * 1.15, 300)

    pnl = np.zeros(len(spot_range))
    for leg in legs:
        if leg.option_type == "CE":
            intrinsic = np.maximum(spot_range - leg.strike, 0.0)
        else:
            intrinsic = np.maximum(leg.strike - spot_range, 0.0)
        pnl += (intrinsic - leg.premium) * leg.quantity * leg.lot_size

    # Breakevens: sign changes in pnl
    breakevens: list[float] = []
    for i in range(len(pnl) - 1):
        if pnl[i] * pnl[i + 1] < 0:
            # Linear interpolation
            be = float(
                spot_range[i]
                - pnl[i] * (spot_range[i + 1] - spot_range[i]) / (pnl[i + 1] - pnl[i])
            )
            breakevens.append(round(be, 2))

    finite_pnl = pnl[np.isfinite(pnl)]
    return {
        "spot_range":  [round(float(x), 2) for x in spot_range],
        "pnl":         [round(float(x), 2) for x in pnl],
        "breakevens":  breakevens,
        "max_profit":  round(float(finite_pnl.max()), 2) if len(finite_pnl) else None,
        "max_loss":    round(float(finite_pnl.min()), 2) if len(finite_pnl) else None,
    }


def compute_pop(
    legs: list[Leg],
    spot: float,
    iv: Optional[float] = None,
    dte: Optional[float] = None,
    n_points: int = 500,
) -> float:
    """
    Probability of Profit: fraction of the log-normal spot distribution
    at expiry where the strategy P&L > 0.

    Uses each leg's IV/DTE if not overridden.
    """
    _iv  = iv  or legs[0].iv
    _dte = dte or legs[0].dte
    T = max(_dte / 365.0, _MIN_T)
    sigma = max(_iv, _MIN_SIGMA)

    # Sample expiry spot prices from log-normal distribution
    log_mean = math.log(spot) + (_DEFAULT_RATE - 0.5 * sigma * sigma) * T
    log_std  = sigma * math.sqrt(T)
    expiry_spots = np.exp(np.random.default_rng(42).normal(log_mean, log_std, n_points))

    payoff_result = compute_payoff(legs, spot_range=expiry_spots, spot=spot)
    pnl_arr = np.array(payoff_result["pnl"])
    return float(np.mean(pnl_arr > 0))
