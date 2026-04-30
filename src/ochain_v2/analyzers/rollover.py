"""
Rollover detection between two expiry snapshots.

A rollover is when participants close near-expiry positions and open the
same position in the next expiry — OI shifts from near to far.

Typically visible in the final 3–5 days before expiry.
"""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd


def detect_rollover(
    near_df: pd.DataFrame,
    far_df: pd.DataFrame,
    prev_near_df: Optional[pd.DataFrame] = None,
    prev_far_df:  Optional[pd.DataFrame] = None,
) -> dict:
    """
    Compare near-expiry and far-expiry OI to detect active rollover.

    Parameters
    ----------
    near_df       : current snapshot for near expiry
    far_df        : current snapshot for far expiry
    prev_near_df  : previous snapshot for near expiry (optional, improves signal)
    prev_far_df   : previous snapshot for far expiry (optional)

    Returns
    -------
    {
        "is_rolling":          bool,
        "near_total_oi":       int,
        "far_total_oi":        int,
        "near_oi_change":      int | None,   vs prev
        "far_oi_change":       int | None,   vs prev
        "rollover_ratio":      float | None, far_oi / near_oi
        "rollover_pct":        float | None, fraction of near OI that moved to far
        "dominant_side":       "CE" | "PE" | "both" | None,
        "interpretation":      str,
    }
    """
    near_ce_oi = int(near_df["ce_oi"].fillna(0).sum())
    near_pe_oi = int(near_df["pe_oi"].fillna(0).sum())
    far_ce_oi  = int(far_df["ce_oi"].fillna(0).sum())
    far_pe_oi  = int(far_df["pe_oi"].fillna(0).sum())

    near_total = near_ce_oi + near_pe_oi
    far_total  = far_ce_oi  + far_pe_oi

    # Changes vs previous snapshots
    near_oi_change: Optional[int] = None
    far_oi_change:  Optional[int] = None
    rollover_pct:   Optional[float] = None

    if prev_near_df is not None and not prev_near_df.empty:
        prev_near_total = int(
            prev_near_df["ce_oi"].fillna(0).sum()
            + prev_near_df["pe_oi"].fillna(0).sum()
        )
        near_oi_change = near_total - prev_near_total

    if prev_far_df is not None and not prev_far_df.empty:
        prev_far_total = int(
            prev_far_df["ce_oi"].fillna(0).sum()
            + prev_far_df["pe_oi"].fillna(0).sum()
        )
        far_oi_change = far_total - prev_far_total

    ratio = far_total / near_total if near_total > 0 else None

    # Estimate rollover percentage: if near OI drops and far OI rises by similar amount
    if (
        near_oi_change is not None
        and far_oi_change is not None
        and near_oi_change < 0
        and far_oi_change > 0
        and near_total > 0
    ):
        rollover_pct = min(abs(near_oi_change) / near_total * 100, 100.0)

    # Identify dominant side
    dominant_side: Optional[str] = None
    ce_rolling = (
        near_oi_change is not None
        and far_oi_change is not None
        and near_ce_oi > near_pe_oi * 1.1
        and far_ce_oi  > far_pe_oi  * 0.9
    )
    pe_rolling = (
        near_oi_change is not None
        and far_oi_change is not None
        and near_pe_oi > near_ce_oi * 1.1
        and far_pe_oi  > far_ce_oi  * 0.9
    )
    if ce_rolling and pe_rolling:
        dominant_side = "both"
    elif ce_rolling:
        dominant_side = "CE"
    elif pe_rolling:
        dominant_side = "PE"

    # Heuristic: rollover is active when near OI declining and far OI increasing
    is_rolling = bool(
        near_oi_change is not None
        and far_oi_change is not None
        and near_oi_change < 0
        and far_oi_change > 0
    )

    interp = _interpret(is_rolling, near_oi_change, far_oi_change, rollover_pct)

    return {
        "is_rolling":      is_rolling,
        "near_total_oi":   near_total,
        "far_total_oi":    far_total,
        "near_oi_change":  near_oi_change,
        "far_oi_change":   far_oi_change,
        "rollover_ratio":  round(ratio, 3) if ratio is not None else None,
        "rollover_pct":    round(rollover_pct, 2) if rollover_pct is not None else None,
        "dominant_side":   dominant_side,
        "interpretation":  interp,
    }


def _interpret(
    is_rolling: bool,
    near_chg: Optional[int],
    far_chg: Optional[int],
    pct: Optional[float],
) -> str:
    if near_chg is None:
        return "Insufficient data (need two consecutive snapshots)"
    if not is_rolling:
        if near_chg >= 0 and far_chg is not None and far_chg >= 0:
            return "OI building in both expiries — no rollover in progress"
        if near_chg >= 0:
            return "Near expiry OI stable/growing — no rollover signal"
        return "Near expiry OI declining but far expiry not absorbing — possible unwinding"
    pct_str = f" (~{pct:.1f}% of near OI)" if pct is not None else ""
    return f"Active rollover{pct_str}: closing near, opening far"
