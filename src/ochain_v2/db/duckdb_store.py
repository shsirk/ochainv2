"""
DuckDB write-side connection manager.

One DuckDBStore instance holds a single exclusive write connection for the
lifetime of the collector process.  All writes are transactional: a single
call to save_snapshot() atomically inserts the snapshot header, all strike
rows, and both delta tables (vs-prev and vs-session-base).

Thread safety: DuckDBStore is NOT thread-safe by design — the collector calls
it from a single asyncio thread via run_in_executor.  If you need concurrent
writes, create one DuckDBStore per thread (but DuckDB only allows one writer
per file).
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Generator, Optional

import duckdb
import pandas as pd

from ochain_v2.core.timezones import now_ist, to_ist, trade_date_ist
from ochain_v2.core.ulid import new_id

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Column mapping: broker DataFrame → chain_rows schema
# Keys = possible broker column names; values = schema column names.
# The mapping is applied via DataFrame.rename(columns=..., errors='ignore').
# ---------------------------------------------------------------------------

_BROKER_TO_SCHEMA: dict[str, str] = {
    "strikePrice":            "strike",
    "CE_openInterest":        "ce_oi",
    "CE_totalTradedVolume":   "ce_volume",
    "CE_lastPrice":           "ce_ltp",
    "CE_impliedVolatility":   "ce_iv",
    "CE_bidprice":            "ce_bid",
    "CE_askPrice":            "ce_ask",
    "CE_bidQty":              "ce_bid_qty",
    "CE_askQty":              "ce_ask_qty",
    "CE_delta":               "ce_delta",
    "CE_gamma":               "ce_gamma",
    "CE_theta":               "ce_theta",
    "CE_vega":                "ce_vega",
    "PE_openInterest":        "pe_oi",
    "PE_totalTradedVolume":   "pe_volume",
    "PE_lastPrice":           "pe_ltp",
    "PE_impliedVolatility":   "pe_iv",
    "PE_bidprice":            "pe_bid",
    "PE_askPrice":            "pe_ask",
    "PE_bidQty":              "pe_bid_qty",
    "PE_askQty":              "pe_ask_qty",
    "PE_delta":               "pe_delta",
    "PE_gamma":               "pe_gamma",
    "PE_theta":               "pe_theta",
    "PE_vega":                "pe_vega",
}

# All chain_rows columns that come from the broker DataFrame (excluding
# the metadata columns injected by the store: snapshot_id, symbol, etc.)
_CHAIN_COLS = [
    "strike",
    "ce_oi", "ce_volume", "ce_ltp", "ce_iv",
    "ce_bid", "ce_ask", "ce_bid_qty", "ce_ask_qty",
    "ce_delta", "ce_gamma", "ce_theta", "ce_vega",
    "pe_oi", "pe_volume", "pe_ltp", "pe_iv",
    "pe_bid", "pe_ask", "pe_bid_qty", "pe_ask_qty",
    "pe_delta", "pe_gamma", "pe_theta", "pe_vega",
]


# ---------------------------------------------------------------------------
# Transaction helper
# ---------------------------------------------------------------------------

@contextmanager
def _txn(conn: duckdb.DuckDBPyConnection) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    conn.execute("BEGIN TRANSACTION")
    try:
        yield conn
        conn.execute("COMMIT")
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise


# ---------------------------------------------------------------------------
# DuckDBStore
# ---------------------------------------------------------------------------

class DuckDBStore:
    """
    Write-side DuckDB connection manager.

    Parameters
    ----------
    db_path:
        Path to the DuckDB file.  Pass ':memory:' or None for in-memory (tests).
    _conn:
        Inject an existing connection (tests only).
    """

    def __init__(
        self,
        db_path: Optional[str] = None,
        *,
        _conn: Optional[duckdb.DuckDBPyConnection] = None,
    ) -> None:
        if _conn is not None:
            self._conn = _conn
            self._owns_conn = False
        else:
            path = db_path or ":memory:"
            Path(path).parent.mkdir(parents=True, exist_ok=True) if path != ":memory:" else None
            self._conn = duckdb.connect(path)
            self._owns_conn = True

    # ------------------------------------------------------------------
    # Schema management
    # ------------------------------------------------------------------

    def init_schema(self) -> None:
        """Apply schema.sql (idempotent — all statements use IF NOT EXISTS)."""
        schema_path = Path(__file__).parent / "schema.sql"
        ddl = schema_path.read_text(encoding="utf-8")
        self._conn.execute(ddl)
        log.debug("schema initialised")

    # ------------------------------------------------------------------
    # Reference data
    # ------------------------------------------------------------------

    def upsert_instrument(
        self,
        symbol: str,
        lot_size: int,
        tick_size: float,
        strike_step: float,
        num_strikes: int = 20,
        exchange: str = "NSE",
        is_index: bool = True,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO instruments
                (symbol, exchange, lot_size, tick_size, strike_step, num_strikes, is_index)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (symbol) DO UPDATE SET
                lot_size    = excluded.lot_size,
                tick_size   = excluded.tick_size,
                strike_step = excluded.strike_step,
                num_strikes = excluded.num_strikes,
                is_index    = excluded.is_index,
                active      = TRUE
            """,
            [symbol, exchange, lot_size, tick_size, strike_step, num_strikes, is_index],
        )

    def upsert_instruments_from_config(self, instruments_file: str) -> None:
        """Load instruments.yaml and upsert all entries."""
        import yaml
        data = yaml.safe_load(Path(instruments_file).read_text())
        for sym, cfg in (data.get("instruments") or {}).items():
            self.upsert_instrument(
                symbol=sym,
                lot_size=cfg["lot_size"],
                tick_size=cfg["tick_size"],
                strike_step=cfg["strike_step"],
                num_strikes=cfg.get("num_strikes", 20),
                exchange=cfg.get("exchange", "NSE"),
                is_index=cfg.get("is_index", True),
            )

    def _upsert_expiry(
        self,
        conn: duckdb.DuckDBPyConnection,
        symbol: str,
        expiry_date: date,
        now: datetime,
    ) -> None:
        conn.execute(
            """
            INSERT INTO expiries (symbol, expiry_date, first_seen_at, last_seen_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (symbol, expiry_date) DO UPDATE SET
                last_seen_at = excluded.last_seen_at,
                active = TRUE
            """,
            [symbol, expiry_date, now, now],
        )

    # ------------------------------------------------------------------
    # Snapshot save (atomic)
    # ------------------------------------------------------------------

    def save_snapshot(
        self,
        df: pd.DataFrame,
        symbol: str,
        expiry: str | date,
        ts: datetime,
        source: str = "dhan",
        is_session_base: bool = False,
    ) -> int:
        """
        Persist one option-chain snapshot transactionally.

        Writes: snapshots, chain_rows, chain_deltas_prev, chain_deltas_base.

        Returns the new snapshot_id.
        """
        if df.empty:
            raise ValueError(f"Empty DataFrame for {symbol}/{expiry} at {ts}")

        snapshot_id = new_id()
        ingested_at = now_ist()
        ts_ist = to_ist(ts)
        trade_date = trade_date_ist(ts_ist)
        expiry_date = date.fromisoformat(str(expiry)) if isinstance(expiry, str) else expiry
        bucket = _bucket_1m(ts_ist)

        underlying_ltp: Optional[float] = None
        if "underlyingValue" in df.columns:
            val = df["underlyingValue"].dropna()
            if not val.empty:
                underlying_ltp = float(val.iloc[0])

        chain_df = _normalize_chain_df(df, snapshot_id, symbol, expiry_date, ts_ist, trade_date, bucket)

        with _txn(self._conn):
            # 1. Snapshot header
            self._conn.execute(
                """
                INSERT INTO snapshots
                    (snapshot_id, symbol, expiry_date, ts, trade_date, bucket_1m,
                     underlying_ltp, is_session_base, source, ingested_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (symbol, expiry_date, ts) DO NOTHING
                """,
                [
                    snapshot_id, symbol, expiry_date, ts_ist, trade_date, bucket,
                    underlying_ltp, is_session_base, source, ingested_at,
                ],
            )

            # 2. Expiry metadata
            self._upsert_expiry(self._conn, symbol, expiry_date, ingested_at)

            # 3. Strike rows
            self._conn.register("_cr_tmp", chain_df)
            self._conn.execute("INSERT INTO chain_rows SELECT * FROM _cr_tmp")
            self._conn.unregister("_cr_tmp")

            # 4. Deltas (computed via SQL within same txn — sees the rows just inserted)
            _write_delta_prev(self._conn, snapshot_id, symbol, expiry_date, trade_date, ts_ist)
            _write_delta_base(self._conn, snapshot_id, symbol, expiry_date, trade_date)

        log.debug(
            "snapshot saved",
            extra={
                "snapshot_id": snapshot_id,
                "symbol": symbol,
                "expiry": str(expiry_date),
                "ts": str(ts_ist),
                "rows": len(chain_df),
            },
        )
        return snapshot_id

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        if self._owns_conn:
            self._conn.close()

    def __enter__(self) -> "DuckDBStore":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


# ---------------------------------------------------------------------------
# DataFrame normalisation
# ---------------------------------------------------------------------------

def _normalize_chain_df(
    df: pd.DataFrame,
    snapshot_id: int,
    symbol: str,
    expiry_date: date,
    ts: datetime,
    trade_date: date,
    bucket_1m: int,
) -> pd.DataFrame:
    """
    Rename broker columns to schema names, inject metadata columns,
    and select only the chain_rows schema columns in order.
    """
    renamed = df.rename(columns=_BROKER_TO_SCHEMA, errors="ignore")

    # Ensure all schema columns exist (fill missing broker columns with None)
    for col in _CHAIN_COLS:
        if col not in renamed.columns:
            renamed[col] = None

    result = renamed[_CHAIN_COLS].copy()

    # Inject metadata
    result.insert(0, "bucket_1m",   bucket_1m)
    result.insert(0, "trade_date",  trade_date)
    result.insert(0, "ts",          ts)
    result.insert(0, "expiry_date", expiry_date)
    result.insert(0, "symbol",      symbol)
    result.insert(0, "snapshot_id", snapshot_id)

    # Drop rows where strike is null/NaN
    result = result.dropna(subset=["strike"])
    result["strike"] = result["strike"].astype(float)
    return result.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Delta helpers (SQL-based, run inside the same transaction)
# ---------------------------------------------------------------------------

_DELTA_COLS = ["ce_oi_chg", "ce_vol_chg", "ce_ltp_chg", "ce_iv_chg",
               "pe_oi_chg", "pe_vol_chg", "pe_ltp_chg", "pe_iv_chg"]

_DELTA_EXPR = """
    CASE WHEN ref.snapshot_id IS NOT NULL THEN cur.ce_oi     - ref.ce_oi     ELSE NULL END,
    CASE WHEN ref.snapshot_id IS NOT NULL THEN cur.ce_volume - ref.ce_volume ELSE NULL END,
    CASE WHEN ref.snapshot_id IS NOT NULL THEN cur.ce_ltp    - ref.ce_ltp    ELSE NULL END,
    CASE WHEN ref.snapshot_id IS NOT NULL THEN cur.ce_iv     - ref.ce_iv     ELSE NULL END,
    CASE WHEN ref.snapshot_id IS NOT NULL THEN cur.pe_oi     - ref.pe_oi     ELSE NULL END,
    CASE WHEN ref.snapshot_id IS NOT NULL THEN cur.pe_volume - ref.pe_volume ELSE NULL END,
    CASE WHEN ref.snapshot_id IS NOT NULL THEN cur.pe_ltp    - ref.pe_ltp    ELSE NULL END,
    CASE WHEN ref.snapshot_id IS NOT NULL THEN cur.pe_iv     - ref.pe_iv     ELSE NULL END,
    ref.snapshot_id IS NOT NULL
"""


def _write_delta_prev(
    conn: duckdb.DuckDBPyConnection,
    snapshot_id: int,
    symbol: str,
    expiry_date: date,
    trade_date: date,
    ts: datetime,
) -> None:
    """Insert chain_deltas_prev for snapshot_id vs the immediately prior snapshot."""
    row = conn.execute(
        """
        SELECT snapshot_id FROM snapshots
        WHERE symbol = ? AND expiry_date = ? AND trade_date = ? AND ts < ?
        ORDER BY ts DESC LIMIT 1
        """,
        [symbol, expiry_date, trade_date, ts],
    ).fetchone()
    prev_id = row[0] if row else None

    conn.execute(
        f"""
        INSERT INTO chain_deltas_prev
        SELECT
            cur.snapshot_id, cur.symbol, cur.expiry_date, cur.ts, cur.strike,
            {_DELTA_EXPR}
        FROM chain_rows cur
        LEFT JOIN chain_rows ref
            ON ref.snapshot_id = ? AND ref.strike = cur.strike
        WHERE cur.snapshot_id = ?
        """,
        [prev_id, snapshot_id],
    )


def _write_delta_base(
    conn: duckdb.DuckDBPyConnection,
    snapshot_id: int,
    symbol: str,
    expiry_date: date,
    trade_date: date,
) -> None:
    """Insert chain_deltas_base for snapshot_id vs the session-base snapshot."""
    row = conn.execute(
        """
        SELECT snapshot_id FROM snapshots
        WHERE symbol = ? AND expiry_date = ? AND trade_date = ? AND is_session_base = TRUE
        LIMIT 1
        """,
        [symbol, expiry_date, trade_date],
    ).fetchone()
    base_id = row[0] if row else None

    conn.execute(
        f"""
        INSERT INTO chain_deltas_base
        SELECT
            cur.snapshot_id, cur.symbol, cur.expiry_date, cur.ts, cur.strike,
            {_DELTA_EXPR}
        FROM chain_rows cur
        LEFT JOIN chain_rows ref
            ON ref.snapshot_id = ? AND ref.strike = cur.strike
        WHERE cur.snapshot_id = ?
        """,
        [base_id, snapshot_id],
    )


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _bucket_1m(ts_ist: datetime) -> int:
    """Minutes offset from 09:15 IST. Negative for pre-open snapshots."""
    return (ts_ist.hour * 60 + ts_ist.minute) - (9 * 60 + 15)
