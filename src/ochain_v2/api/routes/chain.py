"""
v1-compatible chain routes.

    GET /api/symbols
    GET /api/dates/{symbol}?expiry=
    GET /api/expiry_list/{symbol}?date=
    GET /api/snapshots/{symbol}?date=&expiry=&tf=
    GET /api/analyze/{symbol}?date=&expiry=&tf=&from_idx=&to_idx=
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from ochain_v2.analyzers.primitives import (
    compute_atm,
    compute_buildups,
    compute_pcr,
    compute_support_resistance,
)
from ochain_v2.analyzers.gex import compute_gex
from ochain_v2.analyzers.expected_move import compute_expected_move
from ochain_v2.analyzers.iv_surface import compute_iv_smile
from ochain_v2.api.deps import CacheDep, ReaderDep
from ochain_v2.core.timezones import now_ist, trade_date_ist

log = logging.getLogger(__name__)
router = APIRouter()

_TF_MAP = {
    "1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600,
}


def _parse_tf(tf: Optional[str]) -> int:
    if not tf:
        return 60
    return _TF_MAP.get(tf.lower(), int(tf) if tf.isdigit() else 60)


def _parse_date(date_str: Optional[str]) -> date:
    if not date_str:
        return trade_date_ist(now_ist())
    return date.fromisoformat(date_str)


# ---------------------------------------------------------------------------
# Symbols / dates / expiries
# ---------------------------------------------------------------------------

@router.get("/api/symbols")
async def api_symbols(reader: ReaderDep):
    return JSONResponse(reader.get_symbols())


@router.get("/api/dates/{symbol}")
async def api_dates(
    symbol: str,
    reader: ReaderDep,
    expiry: Optional[str] = Query(None),
):
    dates = reader.get_trade_dates(symbol)
    return JSONResponse(dates)


@router.get("/api/expiry_list/{symbol}")
async def api_expiry_list(
    symbol: str,
    reader: ReaderDep,
    date: Optional[str] = Query(None),
):
    trade_date = _parse_date(date) if date else None
    expiries = reader.get_expiries(symbol, trade_date)
    return JSONResponse(expiries)


# ---------------------------------------------------------------------------
# Snapshots list
# ---------------------------------------------------------------------------

@router.get("/api/snapshots/{symbol}")
async def api_snapshots(
    symbol: str,
    reader: ReaderDep,
    date: Optional[str] = Query(None),
    expiry: Optional[str] = Query(default=""),
    tf: Optional[str] = Query(None),
):
    trade_date = _parse_date(date)
    if not expiry:
        expiries = reader.get_expiries(symbol, trade_date)
        expiry = expiries[0] if expiries else ""
    if not expiry:
        raise HTTPException(404, f"No expiries found for {symbol} on {trade_date}")

    tf_sec = _parse_tf(tf)
    snaps = reader.get_snapshot_list(symbol, trade_date, expiry, timeframe_sec=tf_sec)
    return JSONResponse([{"id": s["snapshot_id"], "ts": s["ts"]} for s in snaps])


# ---------------------------------------------------------------------------
# Analyze — main chain endpoint
# ---------------------------------------------------------------------------

@router.get("/api/analyze/{symbol}")
async def api_analyze(
    symbol: str,
    reader: ReaderDep,
    cache: CacheDep,
    date: Optional[str] = Query(None),
    expiry: Optional[str] = Query(default=""),
    tf: Optional[str] = Query(None),
    from_idx: int = Query(0),
    to_idx: int = Query(-1),
):
    trade_date = _parse_date(date)
    if not expiry:
        expiries = reader.get_expiries(symbol, trade_date)
        expiry = expiries[0] if expiries else ""
    if not expiry:
        raise HTTPException(404, f"No expiries for {symbol} on {trade_date}")

    tf_sec = _parse_tf(tf)
    cache_key = (symbol, expiry, str(trade_date), from_idx, to_idx, tf_sec, "analyze")
    cached = cache.window.get(cache_key)
    if cached is not None:
        return JSONResponse(cached)

    snaps = reader.get_snapshot_list(symbol, trade_date, expiry, timeframe_sec=tf_sec)
    if not snaps:
        raise HTTPException(404, "No snapshots found")

    n = len(snaps)
    if to_idx < 0:
        to_idx = n + to_idx
    to_idx = min(to_idx, n - 1)
    from_idx = max(0, min(from_idx, to_idx))

    # Fetch DataFrames concurrently in the thread pool
    loop = asyncio.get_running_loop()
    sid_to  = snaps[to_idx]["snapshot_id"]
    sid_from = snaps[from_idx]["snapshot_id"]
    sid_base = snaps[0]["snapshot_id"]

    # Check snapshot-level cache
    cached_snap = cache.snapshot.get(sid_to)

    if cached_snap is None:
        async def _noop():
            return None

        df_to, df_from, df_base = await asyncio.gather(
            loop.run_in_executor(None, reader.get_chain_rows, sid_to),
            loop.run_in_executor(None, reader.get_chain_rows, sid_from) if sid_from != sid_to else _noop(),
            loop.run_in_executor(None, reader.get_chain_rows, sid_base) if sid_base != sid_to else _noop(),
        )
        if df_from is None:
            df_from = df_to
        if df_base is None:
            df_base = df_to
    else:
        df_to = cached_snap
        df_from = await loop.run_in_executor(None, reader.get_chain_rows, sid_from)
        df_base = await loop.run_in_executor(None, reader.get_chain_rows, sid_base)

    spot = reader.get_underlying_ltp(sid_to) or 0.0

    summary = await loop.run_in_executor(
        None, _build_summary, df_to, df_from, df_base, spot, symbol, expiry, trade_date
    )
    strikes = _df_to_records(df_to)
    deltas = _df_to_records(df_from) if sid_from != sid_to else None

    result = {
        "symbol": symbol,
        "expiry": expiry,
        "trade_date": str(trade_date),
        "base_ts":      snaps[0]["ts"],
        "compare_ts":   snaps[from_idx]["ts"],
        "snapshot_ts":  snaps[to_idx]["ts"],
        "from_idx":     from_idx,
        "to_idx":       to_idx,
        # v1-compat top-level fields
        "bias":         summary.get("bias"),
        "atm_strike":   summary.get("atm", {}).get("atm_strike"),
        "atm_iv":       summary.get("atm", {}).get("atm_iv"),
        "max_pain":     summary.get("atm", {}).get("max_pain"),
        "pcr":          summary.get("pcr"),
        "tf_seconds":   tf_sec,
        "total_snapshots": n,
        "underlying_ltp": spot,
        "strikes": strikes,
        "summary": summary,
        "deltas": deltas,
    }

    cache.snapshot.set(sid_to, df_to)
    cache.window.set(cache_key, result)
    return JSONResponse(result)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_summary(
    df_to: pd.DataFrame,
    df_from: pd.DataFrame,
    df_base: pd.DataFrame,
    spot: float,
    symbol: str,
    expiry: str,
    trade_date,
) -> dict:
    try:
        pcr = compute_pcr(df_to)
    except Exception:
        pcr = {}
    try:
        atm = compute_atm(df_to, spot)
    except Exception:
        atm = {}
    _LOT_DEFAULTS = {"NIFTY": 75, "BANKNIFTY": 30, "FINNIFTY": 65, "MIDCPNIFTY": 120}
    lot_size = _LOT_DEFAULTS.get(symbol, 50)
    try:
        gex = compute_gex(df_to, spot, lot_size)
    except Exception:
        gex = {}
    try:
        sr = compute_support_resistance(df_to)
    except Exception:
        sr = {}
    try:
        expiry_date = date.fromisoformat(str(expiry)) if isinstance(expiry, str) else expiry
        from datetime import date as _date
        dte = max(0.0, (expiry_date - trade_date).days)
        em = compute_expected_move(df_to, spot=spot, dte=dte)
    except Exception:
        em = {}
    try:
        expiry_date = date.fromisoformat(str(expiry)) if isinstance(expiry, str) else expiry
        iv_smile = compute_iv_smile(df_to, expiry_date, spot)
    except Exception:
        iv_smile = {}

    # Bias — mirrors v1's PCR-based heuristic so compare_v1_v2.py passes
    pcr_oi = pcr.get("pcr_oi", 1.0) if pcr else 1.0
    if pcr_oi > 1.3:
        bias = {
            "direction":  "BULLISH",
            "reason":     "High PCR — heavy PE writing indicates support",
            "confidence": round(min(pcr_oi / 2, 1.0), 4),
        }
    elif pcr_oi < 0.7:
        safe_pcr = pcr_oi if pcr_oi > 0 else 0.01
        bias = {
            "direction":  "BEARISH",
            "reason":     "Low PCR — heavy CE writing indicates resistance",
            "confidence": round(min((1 / safe_pcr) / 2, 1.0), 4),
        }
    else:
        bias = {
            "direction":  "NEUTRAL",
            "reason":     "PCR near 1 — no clear directional bias",
            "confidence": 0.3,
        }

    return {
        "pcr": pcr,
        "atm": atm,
        "gex": gex,
        "support_resistance": sr,
        "expected_move": em,
        "iv_smile": iv_smile,
        "bias": bias,
    }


def _df_to_records(df: pd.DataFrame) -> list[dict]:
    if df is None or df.empty:
        return []
    # Convert datetime columns to ISO strings, replace NaN with None
    df = df.copy()
    for col in df.select_dtypes(include=["datetime64[ns, UTC]", "datetime64[ns]", "datetimetz"]).columns:
        df[col] = df[col].dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    # Also catch object columns holding Timestamps
    for col in df.select_dtypes(include="object").columns:
        try:
            sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
            if isinstance(sample, pd.Timestamp):
                df[col] = df[col].apply(lambda v: v.isoformat() if pd.notna(v) else None)
        except Exception:
            pass
    return df.where(pd.notna(df), other=None).to_dict(orient="records")
