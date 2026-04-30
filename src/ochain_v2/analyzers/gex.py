"""
Gamma Exposure (GEX) and Delta Exposure (DEX) analytics.

Convention
----------
- Retail buys calls  → dealers short calls  → dealers SHORT gamma (negative GEX).
- Retail buys puts   → dealers short puts   → dealers SHORT gamma (negative GEX).

Net dealer GEX per strike:
    gex = (ce_gamma × ce_oi - pe_gamma × pe_oi) × lot_size × spot² × 0.01

Positive total GEX  → dealers net long gamma → dampening (pin/chop) regime.
Negative total GEX  → dealers net short gamma → amplifying (trending/explosive) regime.

Gamma source precedence: broker-provided column (ce_gamma) → Black-Scholes computed.
"""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from ochain_v2.analyzers.greeks import bs_gamma, _DEFAULT_RATE, _MIN_T

_DEADBAND_DEFAULT = 0.05   # ±5% of |max GEX| is "neutral"


# ---------------------------------------------------------------------------
# Core computation
# ---------------------------------------------------------------------------

def compute_gex(
    df: pd.DataFrame,
    spot: float,
    lot_size: int,
    dte: float = 30.0,
    r: float = _DEFAULT_RATE,
) -> dict:
    """
    Compute per-strike and total GEX/DEX.

    Parameters
    ----------
    df       : chain_rows DataFrame (single snapshot)
    spot     : current underlying price
    lot_size : contract lot size for this instrument
    dte      : days to expiry (used for BS gamma when broker doesn't provide it)
    r        : risk-free rate

    Returns
    -------
    {
        "strikes":       [float, ...],
        "ce_gex":        [float, ...],   per-strike CE gamma exposure
        "pe_gex":        [float, ...],   per-strike PE gamma exposure
        "net_gex":       [float, ...],   per-strike net (CE - PE)
        "total_gex":     float,          sum of net_gex across all strikes
        "total_ce_gex":  float,
        "total_pe_gex":  float,
        "flip_point":    float | None,   strike where cumulative GEX crosses zero
        "regime":        "positive" | "negative" | "neutral",
        "dex":           float,          net delta exposure (informational)
    }
    """
    if df.empty or spot <= 0:
        return _empty_gex()

    T = max(dte / 365.0, _MIN_T)
    scale = lot_size * (spot ** 2) * 0.01

    # Use broker gamma if available; compute with BS otherwise
    if "ce_gamma" in df.columns and df["ce_gamma"].notna().any():
        ce_g = df["ce_gamma"].fillna(0).values
    else:
        ce_g = np.array([
            bs_gamma(spot, float(k), T, r, _iv_or_default(row, "ce_iv"))
            for _, row in df.iterrows()
            for k in [float(row["strike"])]
        ])

    if "pe_gamma" in df.columns and df["pe_gamma"].notna().any():
        pe_g = df["pe_gamma"].fillna(0).values
    else:
        pe_g = np.array([
            bs_gamma(spot, float(k), T, r, _iv_or_default(row, "pe_iv"))
            for _, row in df.iterrows()
            for k in [float(row["strike"])]
        ])

    ce_oi = df["ce_oi"].fillna(0).values
    pe_oi = df["pe_oi"].fillna(0).values

    ce_gex  = ce_g * ce_oi * scale
    pe_gex  = pe_g * pe_oi * scale
    net_gex = ce_gex - pe_gex

    total_ce  = float(ce_gex.sum())
    total_pe  = float(pe_gex.sum())
    total_net = float(net_gex.sum())

    # DEX (delta exposure): sum(ce_delta × ce_oi - pe_delta × pe_oi) × lot_size × spot
    dex = _compute_dex(df, spot, lot_size, T, r)

    return {
        "strikes":      [float(s) for s in df["strike"].values],
        "ce_gex":       [round(float(v), 2) for v in ce_gex],
        "pe_gex":       [round(float(v), 2) for v in pe_gex],
        "net_gex":      [round(float(v), 2) for v in net_gex],
        "total_gex":    round(total_net, 2),
        "total_ce_gex": round(total_ce,  2),
        "total_pe_gex": round(total_pe,  2),
        "flip_point":   _find_flip_point(df["strike"].values, net_gex),
        "regime":       _gex_regime(total_net, net_gex),
        "dex":          round(dex, 2),
    }


# ---------------------------------------------------------------------------
# Regime and flip point
# ---------------------------------------------------------------------------

def _gex_regime(
    total_gex: float,
    net_gex_per_strike: np.ndarray,
    deadband_pct: float = _DEADBAND_DEFAULT,
) -> str:
    """
    Classify the market's gamma regime.

    deadband_pct: fraction of the maximum absolute GEX used as a neutral zone.
    This prevents flipping between regimes on trivial changes near zero.
    """
    max_abs = float(np.abs(net_gex_per_strike).max()) if len(net_gex_per_strike) else 0.0
    deadband = max_abs * deadband_pct
    if total_gex > deadband:
        return "positive"
    if total_gex < -deadband:
        return "negative"
    return "neutral"


def _find_flip_point(
    strikes: np.ndarray,
    net_gex: np.ndarray,
) -> Optional[float]:
    """
    Find the strike where cumulative net GEX (sorted ascending by strike)
    changes sign — this is the 'gamma flip' level.
    """
    sorted_idx = np.argsort(strikes)
    s_sorted   = strikes[sorted_idx]
    g_sorted   = net_gex[sorted_idx]
    cum        = np.cumsum(g_sorted)

    for i in range(len(cum) - 1):
        if cum[i] * cum[i + 1] <= 0 and cum[i] != 0:
            # Linear interpolation between strikes
            w = abs(cum[i]) / (abs(cum[i]) + abs(cum[i + 1]))
            return float(round(s_sorted[i] * (1 - w) + s_sorted[i + 1] * w, 2))
    return None


# ---------------------------------------------------------------------------
# DEX helper
# ---------------------------------------------------------------------------

def _compute_dex(
    df: pd.DataFrame,
    spot: float,
    lot_size: int,
    T: float,
    r: float,
) -> float:
    from ochain_v2.analyzers.greeks import bs_delta

    if "ce_delta" in df.columns and df["ce_delta"].notna().any():
        ce_d = df["ce_delta"].fillna(0).values
    else:
        ce_d = np.array([
            bs_delta(spot, float(row["strike"]), T, r,
                     _iv_or_default(row, "ce_iv"), "CE")
            for _, row in df.iterrows()
        ])

    if "pe_delta" in df.columns and df["pe_delta"].notna().any():
        pe_d = df["pe_delta"].fillna(0).values
    else:
        pe_d = np.array([
            bs_delta(spot, float(row["strike"]), T, r,
                     _iv_or_default(row, "pe_iv"), "PE")
            for _, row in df.iterrows()
        ])

    ce_oi = df["ce_oi"].fillna(0).values
    pe_oi = df["pe_oi"].fillna(0).values
    return float(((ce_d * ce_oi - pe_d * pe_oi) * lot_size * spot).sum())


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _iv_or_default(row: pd.Series, col: str, default: float = 0.15) -> float:
    val = row.get(col) if hasattr(row, "get") else getattr(row, col, None)
    try:
        v = float(val)
        return v / 100.0 if v > 1.0 else v   # accept both % and decimal
    except (TypeError, ValueError):
        return default


def _empty_gex() -> dict:
    return {
        "strikes": [], "ce_gex": [], "pe_gex": [], "net_gex": [],
        "total_gex": 0.0, "total_ce_gex": 0.0, "total_pe_gex": 0.0,
        "flip_point": None, "regime": "neutral", "dex": 0.0,
    }
