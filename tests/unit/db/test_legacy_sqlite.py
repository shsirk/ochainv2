"""
Integration tests for db/legacy_sqlite.py (P1-35).

Uses a synthetic in-memory SQLite v1 database so the real ochain.db
is not required — tests run offline and in CI.
"""

from __future__ import annotations

import json
import sqlite3
import tempfile
from datetime import date, datetime
from pathlib import Path

import duckdb
import pandas as pd
import pytz
import pytest

from ochain_v2.db.duckdb_store import DuckDBStore
from ochain_v2.db.legacy_sqlite import (
    MigrationResult,
    _parse_ts_ist,
    _records_to_df,
    iter_v1_snapshots,
    migrate_to_duckdb,
)

IST = pytz.timezone("Asia/Kolkata")

# ---------------------------------------------------------------------------
# Helpers to build a synthetic v1 SQLite
# ---------------------------------------------------------------------------

_V1_STRIKES = [22400.0, 22450.0, 22500.0, 22550.0, 22600.0]
_SYMBOLS    = ["NIFTY", "BANKNIFTY"]
_EXPIRIES   = ["2026-03-27", "2026-04-24"]


def _make_v1_record(strike: float, ts_offset_min: int = 0) -> dict:
    """Return a v1-style strike dict with human-readable field names."""
    return {
        "Strike Price":  strike,
        "CE OI":         100_000 + ts_offset_min * 1_000,
        "CE Volume":     5_000,
        "CE IV":         15.0,
        "CE LTP":        50.0,
        "CE Bid":        49.5,
        "CE Ask":        50.5,
        "CE Bid Qty":    75,
        "CE Ask Qty":    75,
        "CE Delta":      0.5,
        "CE Gamma":      0.003,
        "CE Theta":      -5.0,
        "CE Vega":       20.0,
        "CE Chg in OI":  1_000,   # should be ignored
        "PE OI":         80_000,
        "PE Volume":     4_000,
        "PE IV":         14.0,
        "PE LTP":        40.0,
        "PE Bid":        39.5,
        "PE Ask":        40.5,
        "PE Bid Qty":    75,
        "PE Ask Qty":    75,
        "PE Delta":      -0.5,
        "PE Gamma":      0.003,
        "PE Theta":      -4.0,
        "PE Vega":       18.0,
        "PE Chg in OI":  -500,    # should be ignored
    }


def _build_v1_db(path: str, *, include_empty_expiry: bool = False) -> None:
    """
    Populate a v1-style SQLite with:
      2 symbols × 2 expiries × 3 snapshots each = 12 rows
      Each snapshot has 5 strikes.
    """
    conn = sqlite3.connect(path)
    conn.execute(
        """CREATE TABLE snapshots (
            id         INTEGER PRIMARY KEY,
            symbol     TEXT NOT NULL,
            expiry     TEXT DEFAULT '',
            ts         TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            raw_json   TEXT NOT NULL
        )"""
    )

    rows = []
    for sym in _SYMBOLS:
        for exp in _EXPIRIES:
            for minute in range(3):         # 09:15, 09:16, 09:17
                ts_str = f"2026-03-20 09:{15 + minute:02d}:00"
                records = [_make_v1_record(s, minute) for s in _V1_STRIKES]
                rows.append((sym, exp, ts_str, "2026-03-20", json.dumps(records)))

    if include_empty_expiry:
        rows.append(("NIFTY", "", "2026-03-20 09:18:00", "2026-03-20",
                     json.dumps([_make_v1_record(22500.0)])))

    conn.executemany(
        "INSERT INTO snapshots (symbol, expiry, ts, trade_date, raw_json) VALUES (?,?,?,?,?)",
        rows,
    )
    conn.commit()
    conn.close()


def _make_duck_store() -> tuple[duckdb.DuckDBPyConnection, DuckDBStore]:
    conn = duckdb.connect(":memory:")
    store = DuckDBStore(_conn=conn)
    store.init_schema()
    return conn, store


# ---------------------------------------------------------------------------
# _parse_ts_ist
# ---------------------------------------------------------------------------

class TestParseTsIst:
    def test_full_format(self) -> None:
        dt = _parse_ts_ist("2026-03-20 09:15:00")
        assert dt is not None
        assert dt.tzinfo is not None
        assert dt.year == 2026
        assert dt.hour == 9
        assert dt.minute == 15

    def test_short_format(self) -> None:
        dt = _parse_ts_ist("2026-03-20 09:15")
        assert dt is not None
        assert dt.minute == 15

    def test_invalid_returns_none(self) -> None:
        assert _parse_ts_ist("not-a-date") is None
        assert _parse_ts_ist("") is None

    def test_ist_timezone_attached(self) -> None:
        dt = _parse_ts_ist("2026-03-20 10:00:00")
        assert str(dt.tzinfo) == "Asia/Kolkata"


# ---------------------------------------------------------------------------
# _records_to_df
# ---------------------------------------------------------------------------

class TestRecordsToDf:
    def test_renames_v1_columns(self) -> None:
        records = [_make_v1_record(22500.0)]
        df = _records_to_df(records)
        assert "strike" in df.columns
        assert "ce_oi" in df.columns
        assert "pe_oi" in df.columns
        assert "ce_iv" in df.columns

    def test_drops_v1_only_columns(self) -> None:
        records = [_make_v1_record(22500.0)]
        df = _records_to_df(records)
        # "CE Chg in OI" and "PE Chg in OI" should be gone
        assert "CE Chg in OI" not in df.columns
        assert "PE Chg in OI" not in df.columns

    def test_all_five_strikes(self) -> None:
        records = [_make_v1_record(s) for s in _V1_STRIKES]
        df = _records_to_df(records)
        assert len(df) == len(_V1_STRIKES)

    def test_empty_records_returns_empty_df(self) -> None:
        df = _records_to_df([])
        assert df.empty


# ---------------------------------------------------------------------------
# iter_v1_snapshots
# ---------------------------------------------------------------------------

class TestIterV1Snapshots:
    def test_yields_correct_count(self, tmp_path: Path) -> None:
        db = tmp_path / "v1.db"
        _build_v1_db(str(db))
        rows = list(iter_v1_snapshots(str(db)))
        # 2 symbols × 2 expiries × 3 snapshots = 12 rows
        assert len(rows) == 12

    def test_empty_expiry_rows_yielded_with_none_ts(self, tmp_path: Path) -> None:
        db = tmp_path / "v1.db"
        _build_v1_db(str(db), include_empty_expiry=True)
        rows = list(iter_v1_snapshots(str(db)))
        # 12 normal + 1 empty-expiry row
        assert len(rows) == 13
        empty = [r for r in rows if not r[1]]
        assert len(empty) == 1
        assert empty[0][2] is None   # ts is None for empty-expiry rows

    def test_symbol_filter(self, tmp_path: Path) -> None:
        db = tmp_path / "v1.db"
        _build_v1_db(str(db))
        rows = list(iter_v1_snapshots(str(db), symbol_filter="NIFTY"))
        assert all(r[0] == "NIFTY" for r in rows)
        assert len(rows) == 6   # 1 symbol × 2 expiries × 3 snapshots

    def test_file_not_found_raises(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            list(iter_v1_snapshots(str(tmp_path / "nonexistent.db")))

    def test_chain_df_has_schema_columns(self, tmp_path: Path) -> None:
        db = tmp_path / "v1.db"
        _build_v1_db(str(db))
        _, _, _, df = next(iter_v1_snapshots(str(db)))
        assert "strike" in df.columns
        assert "ce_oi"  in df.columns
        assert "pe_oi"  in df.columns


# ---------------------------------------------------------------------------
# migrate_to_duckdb — core integration
# ---------------------------------------------------------------------------

class TestMigrateToducKdb:
    def test_row_counts_match(self, tmp_path: Path) -> None:
        db = tmp_path / "v1.db"
        _build_v1_db(str(db))
        conn, store = _make_duck_store()

        result = migrate_to_duckdb(str(db), store)

        # 2 sym × 2 exp × 3 snaps = 12 snapshots
        n_snap = conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
        assert n_snap == 12

        # 12 snapshots × 5 strikes = 60 chain_rows
        n_rows = conn.execute("SELECT COUNT(*) FROM chain_rows").fetchone()[0]
        assert n_rows == 60

        conn.close()

    def test_result_counts(self, tmp_path: Path) -> None:
        db = tmp_path / "v1.db"
        _build_v1_db(str(db))
        _, store = _make_duck_store()
        result = migrate_to_duckdb(str(db), store)

        assert result.total    == 12
        assert result.migrated == 12
        assert result.skipped  == 0
        assert result.errors   == 0
        assert result.elapsed_s >= 0

    def test_skips_empty_expiry(self, tmp_path: Path) -> None:
        db = tmp_path / "v1.db"
        _build_v1_db(str(db), include_empty_expiry=True)
        conn, store = _make_duck_store()

        result = migrate_to_duckdb(str(db), store)

        # 13 total, 1 skipped (empty expiry), 12 migrated
        assert result.total    == 13
        assert result.skipped  == 1
        assert result.migrated == 12
        conn.close()

    def test_is_session_base_first_snapshot_only(self, tmp_path: Path) -> None:
        db = tmp_path / "v1.db"
        _build_v1_db(str(db))
        conn, store = _make_duck_store()
        migrate_to_duckdb(str(db), store)

        n_base = conn.execute(
            "SELECT COUNT(*) FROM snapshots WHERE is_session_base = TRUE"
        ).fetchone()[0]
        # 2 symbols × 2 expiries × 1 trade_date = 4 session-base snapshots
        assert n_base == 4

        n_non_base = conn.execute(
            "SELECT COUNT(*) FROM snapshots WHERE is_session_base = FALSE"
        ).fetchone()[0]
        assert n_non_base == 8
        conn.close()

    def test_symbol_filter(self, tmp_path: Path) -> None:
        db = tmp_path / "v1.db"
        _build_v1_db(str(db))
        conn, store = _make_duck_store()

        result = migrate_to_duckdb(str(db), store, symbol_filter="NIFTY")

        assert result.migrated == 6
        symbols = {r[0] for r in conn.execute("SELECT DISTINCT symbol FROM snapshots").fetchall()}
        assert symbols == {"NIFTY"}
        conn.close()

    def test_dry_run_writes_nothing(self, tmp_path: Path) -> None:
        db = tmp_path / "v1.db"
        _build_v1_db(str(db))
        conn, store = _make_duck_store()

        result = migrate_to_duckdb(str(db), store, dry_run=True)

        assert result.migrated == 12
        n = conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
        assert n == 0   # nothing actually written
        conn.close()

    def test_deltas_populated(self, tmp_path: Path) -> None:
        db = tmp_path / "v1.db"
        _build_v1_db(str(db))
        conn, store = _make_duck_store()
        migrate_to_duckdb(str(db), store)

        # chain_deltas_prev: first snapshot per group → ref_available=False
        # subsequent snapshots → ref_available=True
        n_with_ref = conn.execute(
            "SELECT COUNT(*) FROM chain_deltas_prev WHERE ref_available = TRUE"
        ).fetchone()[0]
        # 2 sym × 2 exp = 4 groups; each has 3 snaps; 1st has no ref → 4 groups × 2 snaps × 5 strikes
        assert n_with_ref == 4 * 2 * 5

        conn.close()

    def test_expiries_populated(self, tmp_path: Path) -> None:
        db = tmp_path / "v1.db"
        _build_v1_db(str(db))
        conn, store = _make_duck_store()
        migrate_to_duckdb(str(db), store)

        n_exp = conn.execute("SELECT COUNT(*) FROM expiries").fetchone()[0]
        # 2 symbols × 2 expiries = 4 entries
        assert n_exp == 4
        conn.close()

    def test_source_tag_written(self, tmp_path: Path) -> None:
        db = tmp_path / "v1.db"
        _build_v1_db(str(db))
        conn, store = _make_duck_store()
        migrate_to_duckdb(str(db), store, source_tag="test_migration")

        sources = {r[0] for r in
                   conn.execute("SELECT DISTINCT source FROM snapshots").fetchall()}
        assert sources == {"test_migration"}
        conn.close()

    def test_idempotent_second_run_skips_duplicates(self, tmp_path: Path) -> None:
        db = tmp_path / "v1.db"
        _build_v1_db(str(db))
        conn, store = _make_duck_store()

        migrate_to_duckdb(str(db), store)
        # Second run: ON CONFLICT DO NOTHING on snapshots → no new rows
        result2 = migrate_to_duckdb(str(db), store)

        n_snap = conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
        assert n_snap == 12   # still only 12
        # Errors=0 (idempotent), migrated count reflects attempted+skipped at store level
        assert result2.errors == 0
        conn.close()
