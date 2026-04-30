"""
Incremental option chain alert detection.

Design
------
- detect_alerts(current_df, prev_df, thresholds) works on exactly two
  consecutive snapshots — O(n_strikes) per call, no full-day recompute.
- Each alert is a plain dict; callers persist them to meta_sqlite.alert_events.
- Thresholds are configurable via AlertThresholds dataclass.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------

@dataclass
class AlertThresholds:
    # OI Spike: OI grew by this fraction in one polling interval
    oi_spike_pct:           float = 20.0

    # OI Initiation: strike had OI below this and crossed above it
    oi_initiation_min:      int   = 50_000

    # Volume Surge: current volume > this × previous volume (both non-zero)
    volume_surge_multiplier: float = 3.0

    # Volume Initiation: volume was zero/tiny and crossed this threshold
    volume_initiation_min:  int   = 10_000

    # IV Jump: IV changed by this many percentage points in one step
    iv_jump_pts:            float = 2.0

    # Minimum OI to consider a strike "active" (filters deep OTM noise)
    min_oi_for_alerts:      int   = 10_000


# ---------------------------------------------------------------------------
# Alert types
# ---------------------------------------------------------------------------

OI_SPIKE       = "OI_SPIKE"
OI_INITIATION  = "OI_INITIATION"
VOLUME_SURGE   = "VOLUME_SURGE"
VOL_INITIATION = "VOLUME_INITIATION"
IV_JUMP        = "IV_JUMP"


# ---------------------------------------------------------------------------
# detect_alerts
# ---------------------------------------------------------------------------

def detect_alerts(
    current_df: pd.DataFrame,
    prev_df: Optional[pd.DataFrame],
    thresholds: Optional[AlertThresholds] = None,
    symbol: str = "",
    expiry: str = "",
    ts: Optional[datetime] = None,
) -> list[dict]:
    """
    Compare *current_df* against *prev_df* and return a list of alert dicts.

    Each alert dict:
    {
        "alert_type": str,
        "symbol":     str,
        "expiry":     str,
        "strike":     float,
        "side":       "CE" | "PE",
        "detail":     str,
        "magnitude":  float,
        "ts":         str,
    }

    Returns an empty list if prev_df is None/empty (no reference for comparison).
    """
    if prev_df is None or prev_df.empty or current_df.empty:
        return []

    thr = thresholds or AlertThresholds()
    ts_str = ts.strftime("%Y-%m-%d %H:%M:%S") if ts else ""

    # Merge on strike (left join — current is authoritative)
    merged = current_df.merge(
        prev_df[["strike", "ce_oi", "pe_oi", "ce_volume", "pe_volume",
                 "ce_iv", "pe_iv"]].rename(
            columns={
                "ce_oi": "_p_ce_oi", "pe_oi": "_p_pe_oi",
                "ce_volume": "_p_ce_vol", "pe_volume": "_p_pe_vol",
                "ce_iv": "_p_ce_iv", "pe_iv": "_p_pe_iv",
            }
        ),
        on="strike",
        how="left",
    )

    alerts: list[dict] = []

    for _, row in merged.iterrows():
        strike = float(row["strike"])

        for side, curr_oi, prev_oi, curr_vol, prev_vol, curr_iv, prev_iv in [
            ("CE", row.get("ce_oi"), row.get("_p_ce_oi"),
                   row.get("ce_volume"), row.get("_p_ce_vol"),
                   row.get("ce_iv"), row.get("_p_ce_iv")),
            ("PE", row.get("pe_oi"), row.get("_p_pe_oi"),
                   row.get("pe_volume"), row.get("_p_pe_vol"),
                   row.get("pe_iv"), row.get("_p_pe_iv")),
        ]:
            curr_oi  = _f(curr_oi)
            prev_oi  = _f(prev_oi)
            curr_vol = _f(curr_vol)
            prev_vol = _f(prev_vol)
            curr_iv  = _f(curr_iv)
            prev_iv  = _f(prev_iv)

            if curr_oi < thr.min_oi_for_alerts and prev_oi < thr.min_oi_for_alerts:
                continue

            # OI Spike
            if prev_oi > 0:
                pct_change = (curr_oi - prev_oi) / prev_oi * 100
                if pct_change >= thr.oi_spike_pct:
                    alerts.append(_alert(
                        OI_SPIKE, symbol, expiry, strike, side, ts_str,
                        f"{side} OI +{pct_change:.1f}% ({int(prev_oi):,} → {int(curr_oi):,})",
                        pct_change,
                    ))

            # OI Initiation
            elif prev_oi == 0 and curr_oi >= thr.oi_initiation_min:
                alerts.append(_alert(
                    OI_INITIATION, symbol, expiry, strike, side, ts_str,
                    f"{side} OI initiated at {int(curr_oi):,}",
                    float(curr_oi),
                ))

            # Volume Surge
            if prev_vol > 0 and curr_vol >= prev_vol * thr.volume_surge_multiplier:
                mult = curr_vol / prev_vol
                alerts.append(_alert(
                    VOLUME_SURGE, symbol, expiry, strike, side, ts_str,
                    f"{side} volume {mult:.1f}× previous ({int(prev_vol):,} → {int(curr_vol):,})",
                    mult,
                ))
            elif prev_vol == 0 and curr_vol >= thr.volume_initiation_min:
                alerts.append(_alert(
                    VOL_INITIATION, symbol, expiry, strike, side, ts_str,
                    f"{side} volume initiated at {int(curr_vol):,}",
                    float(curr_vol),
                ))

            # IV Jump
            if curr_iv > 0 and prev_iv > 0:
                iv_chg = curr_iv - prev_iv
                if abs(iv_chg) >= thr.iv_jump_pts:
                    direction = "+" if iv_chg > 0 else ""
                    alerts.append(_alert(
                        IV_JUMP, symbol, expiry, strike, side, ts_str,
                        f"{side} IV {direction}{iv_chg:.2f}pp ({prev_iv:.1f}→{curr_iv:.1f})",
                        abs(iv_chg),
                    ))

    return alerts


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _f(val: object) -> float:
    """Coerce to float, return 0.0 for None/NaN."""
    if val is None:
        return 0.0
    try:
        v = float(val)
        return 0.0 if np.isnan(v) else v
    except (TypeError, ValueError):
        return 0.0


def _alert(
    alert_type: str,
    symbol: str,
    expiry: str,
    strike: float,
    side: str,
    ts: str,
    detail: str,
    magnitude: float,
) -> dict:
    return {
        "alert_type": alert_type,
        "symbol":     symbol,
        "expiry":     expiry,
        "strike":     strike,
        "side":       side,
        "ts":         ts,
        "detail":     detail,
        "magnitude":  round(magnitude, 4),
    }
