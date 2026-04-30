"""
Public entry point for delta computation (Phase 1c / P1-14).

The actual SQL delta logic lives in db/duckdb_store.py (_write_delta_prev,
_write_delta_base) and is called atomically inside DuckDBStore.save_snapshot().

This module exposes a standalone function for cases where you need to
re-derive deltas for an existing snapshot (e.g. backfill after schema change
or after importing v1 data that was stored without pre-computed deltas).
"""

from __future__ import annotations

import logging
from datetime import date, datetime

import duckdb

from ochain_v2.core.timezones import to_ist
from ochain_v2.db.duckdb_store import _write_delta_base, _write_delta_prev, _txn

log = logging.getLogger(__name__)


def recompute_deltas(
    conn: duckdb.DuckDBPyConnection,
    snapshot_id: int,
) -> None:
    """
    Re-derive and overwrite both delta tables for *snapshot_id*.

    Useful for:
    - Backfill after v1 → v2 migration
    - Recovery after a partial write failure
    """
    row = conn.execute(
        "SELECT symbol, expiry_date, trade_date, ts FROM snapshots WHERE snapshot_id = ?",
        [snapshot_id],
    ).fetchone()
    if row is None:
        raise ValueError(f"snapshot_id {snapshot_id} not found in snapshots table")

    symbol, expiry_date, trade_date, ts_raw = row
    ts = to_ist(ts_raw) if isinstance(ts_raw, datetime) else ts_raw

    with _txn(conn):
        conn.execute(
            "DELETE FROM chain_deltas_prev WHERE snapshot_id = ?", [snapshot_id]
        )
        conn.execute(
            "DELETE FROM chain_deltas_base WHERE snapshot_id = ?", [snapshot_id]
        )
        _write_delta_prev(conn, snapshot_id, symbol, expiry_date, trade_date, ts)
        _write_delta_base(conn, snapshot_id, symbol, expiry_date, trade_date)

    log.debug("deltas recomputed for snapshot_id=%s", snapshot_id)


def backfill_deltas_for_day(
    conn: duckdb.DuckDBPyConnection,
    symbol: str,
    expiry: str | date,
    trade_date: date,
) -> int:
    """
    Recompute deltas for every snapshot of one (symbol, expiry, trade_date).
    Processes snapshots in chronological order so each prev-delta is correct.

    Returns the number of snapshots processed.
    """
    expiry_date = date.fromisoformat(str(expiry)) if isinstance(expiry, str) else expiry
    rows = conn.execute(
        """
        SELECT snapshot_id FROM snapshots
        WHERE symbol = ? AND expiry_date = ? AND trade_date = ?
        ORDER BY ts
        """,
        [symbol, expiry_date, trade_date],
    ).fetchall()

    for (snap_id,) in rows:
        recompute_deltas(conn, snap_id)

    log.info(
        "delta backfill complete: %s/%s/%s — %d snapshots",
        symbol, expiry_date, trade_date, len(rows),
    )
    return len(rows)
