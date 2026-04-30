"""
Vectorized heatmap matrix builder.

Operates on a multi-snapshot chain_rows DataFrame (from
DuckDBReader.get_chain_rows_range) and pivots it into a
strike × time matrix suitable for the frontend heatmap renderer.

No Python cell loops — uses pandas pivot_table for O(n) performance.
"""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from ochain_v2.core.timezones import to_ist

# Metrics that can be built into a heatmap
VALID_METRICS = frozenset([
    "ce_oi", "pe_oi",
    "ce_volume", "pe_volume",
    "ce_ltp", "pe_ltp",
    "ce_iv", "pe_iv",
    "ce_bid", "pe_bid",
    "ce_ask", "pe_ask",
    "ce_delta", "pe_delta",
    "ce_gamma", "pe_gamma",
    # Pre-computed delta columns (available when merged with delta tables)
    "ce_oi_chg", "pe_oi_chg",
    "ce_vol_chg", "pe_vol_chg",
    "ce_ltp_chg", "pe_ltp_chg",
    "ce_iv_chg",  "pe_iv_chg",
])


def build_heatmap_matrix(
    chain_rows_df: pd.DataFrame,
    metric: str,
    *,
    from_bucket: Optional[int] = None,
    to_bucket:   Optional[int] = None,
    ts_format:   str = "%H:%M",
) -> dict:
    """
    Build a pivot-table heatmap matrix from a multi-snapshot chain DataFrame.

    Parameters
    ----------
    chain_rows_df :
        DataFrame from DuckDBReader.get_chain_rows_range().
        Required columns: strike, ts (TIMESTAMPTZ or datetime), <metric>
        Optional:         bucket_1m (used for bucket filtering)
    metric :
        Column name to pivot on.  Must be in VALID_METRICS.
    from_bucket, to_bucket :
        Inclusive bucket_1m filter applied before pivoting.
    ts_format :
        strftime format for timestamp labels (default: HH:MM).

    Returns
    -------
    {
        "strikes":    [float, ...],            # sorted ascending
        "timestamps": ["HH:MM", ...],          # one per sampled snapshot
        "matrix":     [[float|None, ...], ...],# shape (strikes, timestamps)
        "min_val":    float | None,
        "max_val":    float | None,
        "metric":     str,
    }
    """
    if metric not in VALID_METRICS:
        raise ValueError(
            f"Invalid metric '{metric}'. Allowed: {sorted(VALID_METRICS)}"
        )

    if chain_rows_df.empty:
        return _empty(metric)

    df = chain_rows_df.copy()

    # Bucket filter
    if "bucket_1m" in df.columns:
        if from_bucket is not None:
            df = df[df["bucket_1m"] >= from_bucket]
        if to_bucket is not None:
            df = df[df["bucket_1m"] <= to_bucket]

    if df.empty or metric not in df.columns:
        return _empty(metric)

    # Normalise ts to IST-aware datetime for consistent label formatting
    if not pd.api.types.is_datetime64_any_dtype(df["ts"]):
        df["ts"] = pd.to_datetime(df["ts"], utc=True)

    # Pivot: rows=strike, columns=ts, values=metric
    pivot = df.pivot_table(
        index="strike",
        columns="ts",
        values=metric,
        aggfunc="last",   # if duplicate (ts, strike) pairs exist, keep last
    )

    strikes    = [float(s) for s in pivot.index.tolist()]
    timestamps = [
        to_ist(t).strftime(ts_format) if hasattr(t, "tzinfo") else str(t)
        for t in pivot.columns.tolist()
    ]

    # Replace NaN with None for JSON serialisation
    matrix = [
        [None if (v is None or (isinstance(v, float) and np.isnan(v))) else float(v)
         for v in row]
        for row in pivot.values.tolist()
    ]

    flat_vals = df[metric].dropna().values
    return {
        "strikes":    strikes,
        "timestamps": timestamps,
        "matrix":     matrix,
        "min_val":    float(flat_vals.min()) if len(flat_vals) else None,
        "max_val":    float(flat_vals.max()) if len(flat_vals) else None,
        "metric":     metric,
    }


def _empty(metric: str) -> dict:
    return {
        "strikes": [], "timestamps": [], "matrix": [],
        "min_val": None, "max_val": None, "metric": metric,
    }
