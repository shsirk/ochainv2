"""
Implied Volatility smile and surface analytics.

iv_smile  — per-strike IV vs moneyness for one expiry.
iv_surface — multi-expiry surface (strike × expiry × IV), for 3-D Plotly chart.

None-gap handling: strikes with missing/zero IV are interpolated from neighbours
rather than passed through as None (which creates artificial terrain in 3-D charts).
"""

from __future__ import annotations

from datetime import date
from typing import Optional

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# IV Smile (single expiry)
# ---------------------------------------------------------------------------

def compute_iv_smile(
    df: pd.DataFrame,
    expiry_date: date,
    spot: float,
) -> dict:
    """
    Build IV smile data for one expiry snapshot.

    Parameters
    ----------
    df          : chain_rows DataFrame (single snapshot)
    expiry_date : expiry date (for DTE calculation)
    spot        : current underlying price

    Returns
    -------
    {
        "strikes":   [float, ...],
        "moneyness": [float, ...],   (strike - spot) / spot × 100  (% OTM/ITM)
        "ce_iv":     [float|None, ...],
        "pe_iv":     [float|None, ...],
        "atm_iv":    float | None,
        "skew":      float | None,   OTM put IV - OTM call IV (25-delta proxy)
        "expiry":    str,
        "dte":       int,
    }
    """
    if df.empty:
        return _empty_smile(str(expiry_date))

    dte = max(0, (expiry_date - _today()).days)
    df_s = df.sort_values("strike").copy()

    strikes    = df_s["strike"].tolist()
    moneyness  = [round((s - spot) / spot * 100, 3) for s in strikes]
    ce_iv_raw  = _clean_iv(df_s["ce_iv"].values  if "ce_iv"  in df_s.columns else None)
    pe_iv_raw  = _clean_iv(df_s["pe_iv"].values  if "pe_iv"  in df_s.columns else None)

    # ATM IV (nearest strike to spot)
    idx_atm = int(np.argmin(np.abs(df_s["strike"].values - spot)))
    atm_ce  = ce_iv_raw[idx_atm] if ce_iv_raw is not None else None
    atm_pe  = pe_iv_raw[idx_atm] if pe_iv_raw is not None else None
    atm_iv  = _avg(atm_ce, atm_pe)

    # Skew: 25% OTM put IV minus 25% OTM call IV (approximate)
    skew = _compute_skew(strikes, spot, ce_iv_raw, pe_iv_raw)

    return {
        "strikes":   [float(s) for s in strikes],
        "moneyness": moneyness,
        "ce_iv":     [round(v, 3) if v is not None else None for v in (ce_iv_raw or [None] * len(strikes))],
        "pe_iv":     [round(v, 3) if v is not None else None for v in (pe_iv_raw or [None] * len(strikes))],
        "atm_iv":    round(atm_iv, 3) if atm_iv is not None else None,
        "skew":      round(skew, 3) if skew is not None else None,
        "expiry":    str(expiry_date),
        "dte":       dte,
    }


# ---------------------------------------------------------------------------
# IV Surface (multi-expiry)
# ---------------------------------------------------------------------------

def compute_iv_surface(
    expiries_data: dict[str, pd.DataFrame],
    spot: float,
) -> dict:
    """
    Build a 3-D IV surface across multiple expiries.

    Parameters
    ----------
    expiries_data : {expiry_str: chain_rows_df}  (one entry per expiry)
    spot          : current underlying price

    Returns
    -------
    {
        "strikes":  [float, ...],    common strike axis
        "expiries": [str, ...],      expiry date strings (sorted near → far)
        "dte":      [int, ...],      days to expiry per expiry
        "ce_surface": [[iv, ...], ...],  shape (expiries, strikes); None filled → interpolated
        "pe_surface": [[iv, ...], ...],
        "atm_ivs":  [float|None, ...],  per-expiry ATM IV
    }
    """
    if not expiries_data:
        return {"strikes": [], "expiries": [], "dte": [],
                "ce_surface": [], "pe_surface": [], "atm_ivs": []}

    sorted_expiries = sorted(expiries_data.keys())

    # Build a common strike axis (union of all strikes, sorted)
    all_strikes = sorted(
        set(
            float(s)
            for df in expiries_data.values()
            for s in df["strike"].tolist()
        )
    )

    ce_surface: list[list[Optional[float]]] = []
    pe_surface: list[list[Optional[float]]] = []
    atm_ivs: list[Optional[float]] = []
    dtes: list[int] = []

    for exp_str in sorted_expiries:
        df = expiries_data[exp_str]
        exp_date = date.fromisoformat(exp_str)
        dte = max(0, (exp_date - _today()).days)
        dtes.append(dte)

        # Build IV per strike (None for missing)
        ce_by_strike = (
            df.set_index("strike")["ce_iv"].to_dict()
            if "ce_iv" in df.columns else {}
        )
        pe_by_strike = (
            df.set_index("strike")["pe_iv"].to_dict()
            if "pe_iv" in df.columns else {}
        )

        ce_row = [_safe_iv(ce_by_strike.get(s)) for s in all_strikes]
        pe_row = [_safe_iv(pe_by_strike.get(s)) for s in all_strikes]

        # Interpolate None gaps (avoids artificial terrain)
        ce_row = _interpolate_gaps(all_strikes, ce_row)
        pe_row = _interpolate_gaps(all_strikes, pe_row)

        ce_surface.append(ce_row)
        pe_surface.append(pe_row)

        # ATM IV
        idx_atm = int(np.argmin([abs(s - spot) for s in all_strikes]))
        atm_ivs.append(_avg(ce_row[idx_atm], pe_row[idx_atm]))

    return {
        "strikes":    all_strikes,
        "expiries":   sorted_expiries,
        "dte":        dtes,
        "ce_surface": ce_surface,
        "pe_surface": pe_surface,
        "atm_ivs":    [round(v, 3) if v is not None else None for v in atm_ivs],
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _today() -> date:
    from ochain_v2.core.timezones import now_ist
    return now_ist().date()


def _safe_iv(val: object) -> Optional[float]:
    try:
        v = float(val)           # type: ignore[arg-type]
        return v if v > 0 else None
    except (TypeError, ValueError):
        return None


def _clean_iv(arr: Optional[np.ndarray]) -> Optional[list[Optional[float]]]:
    if arr is None:
        return None
    return [_safe_iv(v) for v in arr]


def _interpolate_gaps(
    strikes: list[float],
    ivs: list[Optional[float]],
) -> list[Optional[float]]:
    """
    Fill None values using linear interpolation from neighbouring valid points.
    Boundary Nones (leading / trailing) remain None.
    """
    result = list(ivs)
    valid_idx = [i for i, v in enumerate(result) if v is not None]
    if len(valid_idx) < 2:
        return result

    for i in range(valid_idx[0], valid_idx[-1] + 1):
        if result[i] is None:
            # Find surrounding valid indices
            lo = max(j for j in valid_idx if j < i)
            hi = min(j for j in valid_idx if j > i)
            w = (strikes[i] - strikes[lo]) / (strikes[hi] - strikes[lo])
            result[i] = round(
                result[lo] + w * (result[hi] - result[lo]),  # type: ignore[operator]
                3,
            )
    return result


def _avg(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is not None and b is not None:
        return (a + b) / 2.0
    return a or b


def _compute_skew(
    strikes: list,
    spot: float,
    ce_iv: Optional[list],
    pe_iv: Optional[list],
    otm_pct: float = 0.05,
) -> Optional[float]:
    """Approximate volatility skew: IV of 5% OTM put minus 5% OTM call."""
    if ce_iv is None or pe_iv is None:
        return None
    otm_call_strike = spot * (1 + otm_pct)
    otm_put_strike  = spot * (1 - otm_pct)

    def _nearest_iv(target: float, iv_list: list) -> Optional[float]:
        if not strikes:
            return None
        idx = int(np.argmin([abs(s - target) for s in strikes]))
        return iv_list[idx]

    call_iv_5 = _nearest_iv(otm_call_strike, ce_iv)
    put_iv_5  = _nearest_iv(otm_put_strike,  pe_iv)
    if call_iv_5 is None or put_iv_5 is None:
        return None
    return round(put_iv_5 - call_iv_5, 3)


def _empty_smile(expiry: str) -> dict:
    return {
        "strikes": [], "moneyness": [], "ce_iv": [], "pe_iv": [],
        "atm_iv": None, "skew": None, "expiry": expiry, "dte": 0,
    }
