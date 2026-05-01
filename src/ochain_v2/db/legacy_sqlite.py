"""
V1 → V2 migration reader.

Reads the v1 SQLite `ochain.db` (single denormalized `snapshots` table with a
`raw_json` column) and streams each snapshot into a v2 DuckDBStore.

V1 schema (snapshot row):
    id, symbol, expiry, ts (TEXT "%Y-%m-%d %H:%M:%S" IST), trade_date, raw_json

V1 raw_json: JSON array of per-strike dicts with human-readable field names
("CE OI", "Strike Price", etc.).  This module maps them to v2 schema names.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Iterator, Optional

import pandas as pd
import pytz

from ochain_v2.db.duckdb_store import DuckDBStore

log = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")

# ---------------------------------------------------------------------------
# V1 human-readable column → v2 schema column
# "CE Chg in OI" / "PE Chg in OI" are intentionally omitted: v2 recomputes
# them in SQL via the delta tables.
# ---------------------------------------------------------------------------

_V1_TO_SCHEMA: dict[str, str] = {
    "Strike Price": "strike",
    # CE side
    "CE OI":        "ce_oi",
    "CE Volume":    "ce_volume",
    "CE LTP":       "ce_ltp",
    "CE IV":        "ce_iv",
    "CE Bid":       "ce_bid",
    "CE Ask":       "ce_ask",
    "CE Bid Qty":   "ce_bid_qty",
    "CE Ask Qty":   "ce_ask_qty",
    "CE Delta":     "ce_delta",
    "CE Gamma":     "ce_gamma",
    "CE Theta":     "ce_theta",
    "CE Vega":      "ce_vega",
    # PE side
    "PE OI":        "pe_oi",
    "PE Volume":    "pe_volume",
    "PE LTP":       "pe_ltp",
    "PE IV":        "pe_iv",
    "PE Bid":       "pe_bid",
    "PE Ask":       "pe_ask",
    "PE Bid Qty":   "pe_bid_qty",
    "PE Ask Qty":   "pe_ask_qty",
    "PE Delta":     "pe_delta",
    "PE Gamma":     "pe_gamma",
    "PE Theta":     "pe_theta",
    "PE Vega":      "pe_vega",
}

_TS_FORMATS = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M")


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class MigrationResult:
    total:     int   = 0
    migrated:  int   = 0
    skipped:   int   = 0
    errors:    int   = 0
    elapsed_s: float = 0.0
    messages:  list[str] = field(default_factory=list)

    def summary(self) -> str:
        return (
            f"Migration complete in {self.elapsed_s:.1f}s — "
            f"{self.migrated} migrated, {self.skipped} skipped, "
            f"{self.errors} errors (total={self.total})"
        )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def iter_v1_snapshots(
    db_path: str | Path,
    symbol_filter: Optional[str] = None,
) -> Iterator[tuple[str, str, datetime, pd.DataFrame]]:
    """
    Yield (symbol, expiry, ts_ist, chain_df) for every row in the v1 DB.

    Rows are ordered by (symbol, expiry, ts) so is_session_base detection
    in the caller processes each (symbol, expiry, trade_date) group in order.

    chain_df has columns already renamed to v2 schema names.
    """
    path = Path(db_path)
    if not path.exists():
        raise FileNotFoundError(f"V1 database not found: {path}")

    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    try:
        where = "WHERE symbol = ?" if symbol_filter else ""
        params = [symbol_filter] if symbol_filter else []
        cursor = conn.execute(
            f"SELECT symbol, expiry, ts, raw_json FROM snapshots "
            f"{where} ORDER BY symbol, expiry, ts",
            params,
        )
        for row in cursor:
            symbol: str = row["symbol"]
            expiry: str = row["expiry"] or ""
            ts_str: str = row["ts"]
            raw:    str = row["raw_json"]

            if not expiry:
                log.debug("Skipping row with empty expiry: symbol=%s ts=%s", symbol, ts_str)
                yield symbol, expiry, None, pd.DataFrame()   # type: ignore[arg-type]
                continue

            ts_ist = _parse_ts_ist(ts_str)
            if ts_ist is None:
                log.warning("Unparseable timestamp '%s' for %s/%s — skipping", ts_str, symbol, expiry)
                yield symbol, expiry, None, pd.DataFrame()   # type: ignore[arg-type]
                continue

            try:
                records = json.loads(raw)
            except (json.JSONDecodeError, TypeError) as exc:
                log.warning("Bad JSON for %s/%s at %s: %s", symbol, expiry, ts_str, exc)
                yield symbol, expiry, ts_ist, pd.DataFrame()
                continue

            df = _records_to_df(records)
            yield symbol, expiry, ts_ist, df
    finally:
        conn.close()


def migrate_to_duckdb(
    src_db_path: str | Path,
    store: DuckDBStore,
    *,
    symbol_filter: Optional[str] = None,
    source_tag: str = "legacy_v1",
    progress_interval: int = 50,
    dry_run: bool = False,
) -> MigrationResult:
    """
    Migrate every snapshot from the v1 SQLite into the v2 DuckDBStore.

    Parameters
    ----------
    src_db_path       : Path to v1 `ochain.db`
    store             : Initialised v2 DuckDBStore (schema already applied)
    symbol_filter     : Only migrate this symbol (None = all)
    source_tag        : Written into snapshots.source column
    progress_interval : Print progress every N snapshots
    dry_run           : Parse and validate but do NOT write to DuckDB

    Returns
    -------
    MigrationResult with counts and elapsed time
    """
    result = MigrationResult()
    t0 = time.monotonic()

    # Track first snapshot per (symbol, expiry, trade_date) for session_base flag
    seen_session: set[tuple[str, str, str]] = set()

    for symbol, expiry, ts_ist, chain_df in iter_v1_snapshots(src_db_path, symbol_filter):
        result.total += 1

        # Skip: empty expiry or unparseable timestamp
        if not expiry or ts_ist is None:
            result.skipped += 1
            log.debug("skip symbol=%s expiry=%r — empty expiry or bad ts", symbol, expiry)
            continue

        # Skip: empty chain
        if chain_df.empty:
            result.skipped += 1
            log.debug("skip symbol=%s expiry=%s ts=%s — empty chain", symbol, expiry, ts_ist)
            continue

        trade_date_str = ts_ist.strftime("%Y-%m-%d")
        session_key = (symbol, expiry, trade_date_str)
        is_session_base = session_key not in seen_session
        seen_session.add(session_key)

        if dry_run:
            result.migrated += 1
        else:
            try:
                store.save_snapshot(
                    chain_df,
                    symbol=symbol,
                    expiry=expiry,
                    ts=ts_ist,
                    source=source_tag,
                    is_session_base=is_session_base,
                )
                result.migrated += 1
            except Exception as exc:
                result.errors += 1
                msg = f"ERROR symbol={symbol} expiry={expiry} ts={ts_ist}: {exc}"
                result.messages.append(msg)
                log.error(msg)

        if result.total % progress_interval == 0:
            elapsed = time.monotonic() - t0
            log.info(
                "progress: %d processed, %d migrated, %d skipped, %d errors (%.1fs)",
                result.total, result.migrated, result.skipped, result.errors, elapsed,
            )

    result.elapsed_s = round(time.monotonic() - t0, 2)
    log.info(result.summary())
    return result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_ts_ist(ts_str: str) -> Optional[datetime]:
    """Parse a naive IST timestamp string and return an IST-aware datetime."""
    for fmt in _TS_FORMATS:
        try:
            naive = datetime.strptime(ts_str.strip(), fmt)
            return IST.localize(naive)
        except ValueError:
            continue
    return None


def _records_to_df(records: list[dict]) -> pd.DataFrame:
    """Convert v1 JSON records to a v2-schema-named DataFrame."""
    df = pd.DataFrame(records)
    df = df.rename(columns=_V1_TO_SCHEMA, errors="ignore")
    # Drop v1-only cols not in schema (e.g. "CE Chg in OI")
    keep = [c for c in df.columns if c in _V1_TO_SCHEMA.values()]
    df = df[keep] if keep else df
    return df
