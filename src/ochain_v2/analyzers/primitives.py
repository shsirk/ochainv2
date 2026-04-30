"""
Core option chain analytical primitives.

All functions operate on DataFrames with chain_rows column schema:
  strike, ce_oi, pe_oi, ce_ltp, pe_ltp, ce_iv, pe_iv,
  ce_volume, pe_volume  (plus optional greeks columns)

Functions are pure (no DB calls, no side effects) and fully vectorized.
"""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Column name helpers
# ---------------------------------------------------------------------------

_DELTA_PAIRS = [
    ("ce_oi",     "ce_oi_chg"),
    ("pe_oi",     "pe_oi_chg"),
    ("ce_ltp",    "ce_ltp_chg"),
    ("pe_ltp",    "pe_ltp_chg"),
    ("ce_iv",     "ce_iv_chg"),
    ("pe_iv",     "pe_iv_chg"),
    ("ce_volume", "ce_vol_chg"),
    ("pe_volume", "pe_vol_chg"),
]

# PCR regime thresholds (typical for Nifty/BankNifty)
_PCR_BULLISH_THRESHOLD  = 1.2
_PCR_BEARISH_THRESHOLD  = 0.8


# ---------------------------------------------------------------------------
# compute_delta
# ---------------------------------------------------------------------------

def compute_delta(
    current_df: pd.DataFrame,
    ref_df: Optional[pd.DataFrame],
) -> pd.DataFrame:
    """
    Add per-strike change columns to *current_df* by diffing against *ref_df*.

    Returns a copy of *current_df* with additional columns:
        ce_oi_chg, pe_oi_chg, ce_ltp_chg, pe_ltp_chg,
        ce_iv_chg, pe_iv_chg, ce_vol_chg, pe_vol_chg, ref_available

    When *ref_df* is None / empty, all *_chg columns are NaN and
    ref_available is False for every row.
    """
    result = current_df.copy()

    if ref_df is None or ref_df.empty:
        for _, chg_col in _DELTA_PAIRS:
            result[chg_col] = np.nan
        result["ref_available"] = False
        return result

    ref_indexed = ref_df.set_index("strike")

    for src_col, chg_col in _DELTA_PAIRS:
        if src_col in current_df.columns and src_col in ref_indexed.columns:
            result[chg_col] = (
                result["strike"]
                .map(ref_indexed[src_col])
                .rsub(result[src_col])   # current - ref
            )
        else:
            result[chg_col] = np.nan

    result["ref_available"] = result["strike"].isin(ref_indexed.index)
    return result


# ---------------------------------------------------------------------------
# compute_pcr
# ---------------------------------------------------------------------------

def compute_pcr(df: pd.DataFrame) -> dict:
    """
    Put-Call Ratio by OI and by volume, with a simple regime label.

    Returns
    -------
    {
        "pcr_oi":       float,
        "pcr_volume":   float | None,
        "oi_regime":    "bullish" | "bearish" | "neutral",
        "total_ce_oi":  int,
        "total_pe_oi":  int,
        "total_ce_vol": int,
        "total_pe_vol": int,
    }
    """
    ce_oi  = float(df["ce_oi"].sum())
    pe_oi  = float(df["pe_oi"].sum())
    ce_vol = float(df["ce_volume"].sum()) if "ce_volume" in df.columns else 0.0
    pe_vol = float(df["pe_volume"].sum()) if "pe_volume" in df.columns else 0.0

    pcr_oi  = pe_oi / ce_oi if ce_oi > 0 else float("nan")
    pcr_vol = pe_vol / ce_vol if ce_vol > 0 else None

    if np.isnan(pcr_oi):
        regime = "neutral"
    elif pcr_oi > _PCR_BULLISH_THRESHOLD:
        regime = "bullish"
    elif pcr_oi < _PCR_BEARISH_THRESHOLD:
        regime = "bearish"
    else:
        regime = "neutral"

    return {
        "pcr_oi":       round(pcr_oi, 4) if not np.isnan(pcr_oi) else None,
        "pcr_volume":   round(pcr_vol, 4) if pcr_vol is not None else None,
        "oi_regime":    regime,
        "total_ce_oi":  int(ce_oi),
        "total_pe_oi":  int(pe_oi),
        "total_ce_vol": int(ce_vol),
        "total_pe_vol": int(pe_vol),
    }


# ---------------------------------------------------------------------------
# compute_atm
# ---------------------------------------------------------------------------

def compute_atm(df: pd.DataFrame, spot: float) -> dict:
    """
    Identify the ATM strike and associated metrics.

    Returns
    -------
    {
        "atm_strike":     float,
        "ce_ltp":         float | None,
        "pe_ltp":         float | None,
        "straddle_price": float | None,
        "ce_iv":          float | None,
        "pe_iv":          float | None,
        "avg_iv":         float | None,
    }
    """
    if df.empty:
        return {"atm_strike": spot, "ce_ltp": None, "pe_ltp": None,
                "straddle_price": None, "ce_iv": None, "pe_iv": None, "avg_iv": None}

    idx = (df["strike"] - spot).abs().idxmin()
    row = df.loc[idx]

    ce_ltp = float(row["ce_ltp"]) if "ce_ltp" in row and pd.notna(row["ce_ltp"]) else None
    pe_ltp = float(row["pe_ltp"]) if "pe_ltp" in row and pd.notna(row["pe_ltp"]) else None
    ce_iv  = float(row["ce_iv"])  if "ce_iv"  in row and pd.notna(row["ce_iv"])  else None
    pe_iv  = float(row["pe_iv"])  if "pe_iv"  in row and pd.notna(row["pe_iv"])  else None

    straddle = (ce_ltp + pe_ltp) if (ce_ltp is not None and pe_ltp is not None) else None
    avg_iv = (
        (ce_iv + pe_iv) / 2
        if ce_iv is not None and pe_iv is not None
        else (ce_iv or pe_iv)
    )

    return {
        "atm_strike":     float(row["strike"]),
        "ce_ltp":         ce_ltp,
        "pe_ltp":         pe_ltp,
        "straddle_price": round(straddle, 2) if straddle is not None else None,
        "ce_iv":          round(ce_iv, 2) if ce_iv is not None else None,
        "pe_iv":          round(pe_iv, 2) if pe_iv is not None else None,
        "avg_iv":         round(avg_iv, 2) if avg_iv is not None else None,
    }


# ---------------------------------------------------------------------------
# compute_max_pain
# ---------------------------------------------------------------------------

def compute_max_pain(df: pd.DataFrame) -> dict:
    """
    Compute max-pain expiry price (minimises total option-writer loss).

    Algorithm: for each candidate price P (every strike in the chain),
    total writer pain = Σ_K max(P-K,0)×CE_OI_K + Σ_K max(K-P,0)×PE_OI_K.

    Returns
    -------
    {
        "max_pain_price": float,
        "pain_curve":     [{"price": float, "pain": float}, ...]  (sorted by price)
    }
    """
    strikes   = df["strike"].values.astype(float)
    ce_oi     = df["ce_oi"].fillna(0).values.astype(float)
    pe_oi     = df["pe_oi"].fillna(0).values.astype(float)

    pain_vals = []
    for test_price in strikes:
        ce_loss = np.sum(np.maximum(test_price - strikes, 0) * ce_oi)
        pe_loss = np.sum(np.maximum(strikes - test_price, 0) * pe_oi)
        pain_vals.append(ce_loss + pe_loss)

    pain_arr = np.array(pain_vals)
    max_pain_idx = int(np.argmin(pain_arr))

    pain_curve = [
        {"price": float(s), "pain": float(p)}
        for s, p in zip(strikes, pain_arr)
    ]

    return {
        "max_pain_price": float(strikes[max_pain_idx]),
        "pain_curve":     pain_curve,
    }


# ---------------------------------------------------------------------------
# compute_buildups
# ---------------------------------------------------------------------------

_BUILDUP_LABELS = {
    (True,  True):  "Long Buildup",
    (False, True):  "Short Buildup",
    (True,  False): "Short Covering",
    (False, False): "Long Unwinding",
}


def compute_buildups(
    current_df: pd.DataFrame,
    prev_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Classify per-strike OI action for CE and PE legs.

    If *prev_df* is provided, deltas are computed here.
    If *current_df* already has *_chg columns (from DB delta tables), they are used.

    Adds columns: ce_buildup, pe_buildup  (one of the four labels above).

    Returns the augmented *current_df*.
    """
    if prev_df is not None:
        df = compute_delta(current_df, prev_df)
    else:
        df = current_df.copy()

    # CE buildup
    if "ce_ltp_chg" in df.columns and "ce_oi_chg" in df.columns:
        ce_price_up = df["ce_ltp_chg"].fillna(0) >= 0
        ce_oi_up    = df["ce_oi_chg"].fillna(0) >= 0
        df["ce_buildup"] = [
            _BUILDUP_LABELS[(bool(pu), bool(ou))]
            for pu, ou in zip(ce_price_up, ce_oi_up)
        ]
    else:
        df["ce_buildup"] = "Unknown"

    # PE buildup
    if "pe_ltp_chg" in df.columns and "pe_oi_chg" in df.columns:
        pe_price_up = df["pe_ltp_chg"].fillna(0) >= 0
        pe_oi_up    = df["pe_oi_chg"].fillna(0) >= 0
        df["pe_buildup"] = [
            _BUILDUP_LABELS[(bool(pu), bool(ou))]
            for pu, ou in zip(pe_price_up, pe_oi_up)
        ]
    else:
        df["pe_buildup"] = "Unknown"

    return df


# ---------------------------------------------------------------------------
# compute_support_resistance
# ---------------------------------------------------------------------------

def compute_support_resistance(df: pd.DataFrame, n: int = 5) -> dict:
    """
    Identify OI-based support and resistance levels.

    Support   = top-N strikes by PE OI (put writers defend these)
    Resistance = top-N strikes by CE OI (call writers defend these)

    Returns
    -------
    {
        "support":    [{"strike": float, "pe_oi": int}, ...]  top-N
        "resistance": [{"strike": float, "ce_oi": int}, ...]  top-N
    }
    """
    support = (
        df.nlargest(n, "pe_oi")[["strike", "pe_oi"]]
        .assign(pe_oi=lambda x: x["pe_oi"].astype(int))
        .sort_values("strike")
        .to_dict(orient="records")
    )
    resistance = (
        df.nlargest(n, "ce_oi")[["strike", "ce_oi"]]
        .assign(ce_oi=lambda x: x["ce_oi"].astype(int))
        .sort_values("strike")
        .to_dict(orient="records")
    )
    return {"support": support, "resistance": resistance}
