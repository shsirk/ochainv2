"""
DuckDB read-only connection pool for the API layer.

Design
------
- One connection per thread (thread-local storage).
- Connections are opened read-only so multiple API worker threads can run
  concurrently alongside the single write connection in the collector process.
- All query methods return pandas DataFrames or plain Python dicts/lists so
  the API layer has no DuckDB dependency.

Usage
-----
    reader = DuckDBReader("data/ochain.duckdb")
    snaps  = reader.get_snapshot_list("NIFTY", date(2026,3,20), "2026-03-27")
    df     = reader.get_chain_rows(snapshot_id)
"""

from __future__ import annotations

import threading
from datetime import date, datetime
from typing import Optional

import duckdb
import pandas as pd

from ochain_v2.core.timezones import to_ist


class DuckDBReader:
    """
    Thread-safe read-only DuckDB accessor.

    Each thread gets its own connection via threading.local().
    """

    def __init__(self, db_path: str, *, _conn: Optional[duckdb.DuckDBPyConnection] = None) -> None:
        self._db_path = db_path
        self._injected_conn = _conn          # for tests (in-memory conn)
        self._local = threading.local()

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def _conn(self) -> duckdb.DuckDBPyConnection:
        if self._injected_conn is not None:
            return self._injected_conn
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = duckdb.connect(self._db_path, read_only=True)
        return self._local.conn

    def close(self) -> None:
        """Close the current thread's connection (if any)."""
        if self._injected_conn is not None:
            return  # injected conn is managed externally
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None

    # ------------------------------------------------------------------
    # Metadata queries
    # ------------------------------------------------------------------

    def get_trade_dates(self, symbol: str) -> list[str]:
        """All distinct trade dates for *symbol*, newest first."""
        rows = self._conn().execute(
            """
            SELECT DISTINCT trade_date::VARCHAR
            FROM snapshots
            WHERE symbol = ?
            ORDER BY trade_date DESC
            """,
            [symbol],
        ).fetchall()
        return [r[0] for r in rows]

    def get_expiries(self, symbol: str, trade_date: Optional[date] = None) -> list[str]:
        """Expiry dates for *symbol*, optionally filtered to a trade date."""
        if trade_date is not None:
            rows = self._conn().execute(
                """
                SELECT DISTINCT expiry_date::VARCHAR
                FROM snapshots
                WHERE symbol = ? AND trade_date = ?
                ORDER BY expiry_date
                """,
                [symbol, trade_date],
            ).fetchall()
        else:
            rows = self._conn().execute(
                """
                SELECT DISTINCT expiry_date::VARCHAR
                FROM snapshots WHERE symbol = ?
                ORDER BY expiry_date
                """,
                [symbol],
            ).fetchall()
        return [r[0] for r in rows]

    def get_symbols(self) -> list[str]:
        rows = self._conn().execute(
            "SELECT DISTINCT symbol FROM snapshots ORDER BY symbol"
        ).fetchall()
        return [r[0] for r in rows]

    # ------------------------------------------------------------------
    # Snapshot list
    # ------------------------------------------------------------------

    def get_snapshot_list(
        self,
        symbol: str,
        trade_date: date,
        expiry: str | date,
        timeframe_sec: int = 60,
    ) -> list[dict]:
        """
        Return snapshot metadata (id, ts, bucket_1m) for one day.

        *timeframe_sec* samples to every N seconds (e.g. 300 = every 5m).
        """
        expiry_date = date.fromisoformat(str(expiry)) if isinstance(expiry, str) else expiry
        rows = self._conn().execute(
            """
            SELECT snapshot_id, ts::VARCHAR, bucket_1m
            FROM snapshots
            WHERE symbol = ? AND expiry_date = ? AND trade_date = ?
            ORDER BY ts
            """,
            [symbol, expiry_date, trade_date],
        ).fetchall()

        if timeframe_sec <= 60:
            return [{"snapshot_id": r[0], "ts": r[1], "bucket_1m": r[2]} for r in rows]

        # Downsample: keep one snapshot per N-second bucket
        bucket_size = timeframe_sec // 60
        seen: set[int] = set()
        result = []
        for snap_id, ts_str, bucket in rows:
            tf_bucket = bucket // bucket_size
            if tf_bucket not in seen:
                seen.add(tf_bucket)
                result.append({"snapshot_id": snap_id, "ts": ts_str, "bucket_1m": bucket})
        return result

    # ------------------------------------------------------------------
    # Chain rows
    # ------------------------------------------------------------------

    def get_chain_rows(self, snapshot_id: int) -> pd.DataFrame:
        """All strike rows for one snapshot, ordered by strike."""
        return self._conn().execute(
            """
            SELECT * FROM chain_rows
            WHERE snapshot_id = ?
            ORDER BY strike
            """,
            [snapshot_id],
        ).df()

    def get_chain_rows_range(
        self,
        symbol: str,
        expiry: str | date,
        trade_date: date,
        from_ts: datetime,
        to_ts: datetime,
    ) -> pd.DataFrame:
        """All strike rows between two timestamps (inclusive), ordered by ts, strike."""
        expiry_date = date.fromisoformat(str(expiry)) if isinstance(expiry, str) else expiry
        return self._conn().execute(
            """
            SELECT * FROM chain_rows
            WHERE symbol = ? AND expiry_date = ? AND trade_date = ?
              AND ts BETWEEN ? AND ?
            ORDER BY ts, strike
            """,
            [symbol, expiry_date, trade_date, to_ist(from_ts), to_ist(to_ts)],
        ).df()

    # ------------------------------------------------------------------
    # Delta tables
    # ------------------------------------------------------------------

    def get_delta_base(self, snapshot_id: int) -> pd.DataFrame:
        """vs-session-base deltas for one snapshot."""
        return self._conn().execute(
            "SELECT * FROM chain_deltas_base WHERE snapshot_id = ? ORDER BY strike",
            [snapshot_id],
        ).df()

    def get_delta_prev(self, snapshot_id: int) -> pd.DataFrame:
        """vs-previous-snapshot deltas for one snapshot."""
        return self._conn().execute(
            "SELECT * FROM chain_deltas_prev WHERE snapshot_id = ? ORDER BY strike",
            [snapshot_id],
        ).df()

    # ------------------------------------------------------------------
    # Heatmap matrix
    # ------------------------------------------------------------------

    def get_heatmap_matrix(
        self,
        symbol: str,
        expiry: str | date,
        trade_date: date,
        metric: str,
        from_bucket: int = 0,
        to_bucket: int = 374,
    ) -> dict:
        """
        Return a pivot-table dict suitable for the heatmap renderer:
          {
            "strikes":    [float, ...],          # sorted ascending
            "timestamps": ["HH:MM", ...],        # one per sampled bucket
            "matrix":     [[float|None, ...], ...],  # shape (len(strikes), len(timestamps))
          }

        *metric* must be a valid chain_rows column (e.g. 'ce_oi', 'pe_iv').
        """
        _validate_metric(metric)
        expiry_date = date.fromisoformat(str(expiry)) if isinstance(expiry, str) else expiry

        df = self._conn().execute(
            f"""
            SELECT ts, strike, {metric}
            FROM chain_rows
            WHERE symbol = ? AND expiry_date = ? AND trade_date = ?
              AND bucket_1m BETWEEN ? AND ?
            ORDER BY strike, ts
            """,
            [symbol, expiry_date, trade_date, from_bucket, to_bucket],
        ).df()

        if df.empty:
            return {"strikes": [], "timestamps": [], "matrix": []}

        pivot = df.pivot_table(index="strike", columns="ts", values=metric, aggfunc="last")
        strikes = [float(s) for s in pivot.index.tolist()]
        timestamps = [to_ist(t).strftime("%H:%M") for t in pivot.columns.tolist()]
        raw = pivot.where(pivot.notna(), other=None).values.tolist()
        matrix = [[None if (isinstance(v, float) and v != v) else v for v in row] for row in raw]

        return {"strikes": strikes, "timestamps": timestamps, "matrix": matrix}

    def get_atm_iv_intraday(
        self,
        symbol: str,
        expiry: str | date,
        trade_date: date,
        spot: float,
    ) -> list[dict]:
        """
        Return ATM CE/PE IV time series for the day.
        Each row: {"ts": "HH:MM", "ce_iv": float|None, "pe_iv": float|None, "avg_iv": float|None}
        """
        expiry_date = date.fromisoformat(str(expiry)) if isinstance(expiry, str) else expiry

        df = self._conn().execute(
            """
            WITH ranked AS (
                SELECT ts,
                       ce_iv,
                       pe_iv,
                       ABS(strike - ?) AS dist,
                       ROW_NUMBER() OVER (PARTITION BY ts ORDER BY ABS(strike - ?)) AS rn
                FROM chain_rows
                WHERE symbol = ? AND expiry_date = ? AND trade_date = ?
            )
            SELECT ts, ce_iv, pe_iv
            FROM ranked
            WHERE rn = 1
            ORDER BY ts
            """,
            [spot, spot, symbol, expiry_date, trade_date],
        ).df()

        if df.empty:
            return []

        rows = []
        for _, r in df.iterrows():
            ce = float(r["ce_iv"]) if r["ce_iv"] is not None and r["ce_iv"] == r["ce_iv"] else None
            pe = float(r["pe_iv"]) if r["pe_iv"] is not None and r["pe_iv"] == r["pe_iv"] else None
            avg = round((ce + pe) / 2, 3) if ce is not None and pe is not None else (ce or pe)
            rows.append({
                "ts":     to_ist(r["ts"]).strftime("%H:%M"),
                "ce_iv":  round(ce, 3) if ce is not None else None,
                "pe_iv":  round(pe, 3) if pe is not None else None,
                "avg_iv": avg,
            })
        return rows

    # ------------------------------------------------------------------
    # Underlying price
    # ------------------------------------------------------------------

    def get_underlying_ltp(self, snapshot_id: int) -> Optional[float]:
        row = self._conn().execute(
            "SELECT underlying_ltp FROM snapshots WHERE snapshot_id = ?",
            [snapshot_id],
        ).fetchone()
        return float(row[0]) if row and row[0] is not None else None

    def get_session_base_snapshot_id(
        self, symbol: str, expiry: str | date, trade_date: date
    ) -> Optional[int]:
        expiry_date = date.fromisoformat(str(expiry)) if isinstance(expiry, str) else expiry
        row = self._conn().execute(
            """
            SELECT snapshot_id FROM snapshots
            WHERE symbol = ? AND expiry_date = ? AND trade_date = ? AND is_session_base = TRUE
            LIMIT 1
            """,
            [symbol, expiry_date, trade_date],
        ).fetchone()
        return int(row[0]) if row else None


# ---------------------------------------------------------------------------
# Security: prevent SQL injection via metric names
# ---------------------------------------------------------------------------

_ALLOWED_METRICS = frozenset(
    [
        "ce_oi", "ce_volume", "ce_ltp", "ce_iv",
        "ce_bid", "ce_ask", "ce_bid_qty", "ce_ask_qty",
        "ce_delta", "ce_gamma", "ce_theta", "ce_vega",
        "pe_oi", "pe_volume", "pe_ltp", "pe_iv",
        "pe_bid", "pe_ask", "pe_bid_qty", "pe_ask_qty",
        "pe_delta", "pe_gamma", "pe_theta", "pe_vega",
    ]
)


def _validate_metric(metric: str) -> None:
    if metric not in _ALLOWED_METRICS:
        raise ValueError(
            f"Invalid metric '{metric}'. "
            f"Allowed: {sorted(_ALLOWED_METRICS)}"
        )
