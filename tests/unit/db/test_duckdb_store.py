"""
Unit tests for db/duckdb_store.py and db/duckdb_reader.py  (P1-13)

Covers:
  - save_snapshot: chain_rows row count matches input DataFrame
  - First snapshot of day: chain_deltas_prev has ref_available=False for all strikes
  - Second snapshot: chain_deltas_prev has ref_available=True; OI delta is correct
  - Session base: chain_deltas_base uses the is_session_base snapshot
  - Non-session-base first snapshot: chain_deltas_base ref_available=False
  - upsert_instrument is idempotent
  - DuckDBReader: get_snapshot_list, get_chain_rows, get_delta_base, heatmap_matrix
  - _validate_metric: rejects unknown column names
"""

from datetime import date, datetime

import pandas as pd
import pytest
import pytz

from ochain_v2.db.duckdb_reader import DuckDBReader, _validate_metric
from ochain_v2.db.duckdb_store import DuckDBStore, _bucket_1m
from ochain_v2.core.timezones import IST

# Shared test coordinates
SYM = "NIFTY"
EXPIRY = date(2026, 3, 27)
DATE = date(2026, 3, 20)
SPOT = 22500.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts(h: int, m: int) -> datetime:
    return IST.localize(datetime(DATE.year, DATE.month, DATE.day, h, m, 0))


def _make_df(strikes: list[float], ce_oi_base: int = 100_000, pe_oi_base: int = 80_000) -> pd.DataFrame:
    rows = [
        {
            "strikePrice": s,
            "CE_openInterest": int(ce_oi_base + s),
            "CE_lastPrice": 50.0,
            "CE_impliedVolatility": 15.0,
            "CE_totalTradedVolume": 5000,
            "CE_changeinOpenInterest": 0,
            "CE_bidQty": 75, "CE_bidprice": 49.5, "CE_askPrice": 50.5, "CE_askQty": 75,
            "PE_openInterest": int(pe_oi_base + s),
            "PE_lastPrice": 40.0,
            "PE_impliedVolatility": 14.0,
            "PE_totalTradedVolume": 4000,
            "PE_changeinOpenInterest": 0,
            "PE_bidQty": 75, "PE_bidprice": 39.5, "PE_askPrice": 40.5, "PE_askQty": 75,
            "underlyingValue": SPOT,
        }
        for s in strikes
    ]
    return pd.DataFrame(rows)


_STRIKES = [22250.0, 22300.0, 22350.0, 22400.0, 22450.0,
            22500.0, 22550.0, 22600.0, 22650.0, 22700.0, 22750.0]


# ---------------------------------------------------------------------------
# DuckDBStore.save_snapshot
# ---------------------------------------------------------------------------

class TestSaveSnapshot:
    def test_chain_rows_count_matches_df(
        self, duck_store: DuckDBStore, duck_conn: object, sample_chain_df: pd.DataFrame
    ) -> None:
        snap_id = duck_store.save_snapshot(sample_chain_df, SYM, EXPIRY, _ts(9, 15))
        count = duck_conn.execute(
            "SELECT count(*) FROM chain_rows WHERE snapshot_id = ?", [snap_id]
        ).fetchone()[0]
        assert count == len(sample_chain_df)

    def test_snapshot_header_written(self, duck_store: DuckDBStore, duck_conn: object) -> None:
        df = _make_df(_STRIKES)
        snap_id = duck_store.save_snapshot(df, SYM, EXPIRY, _ts(9, 15))
        row = duck_conn.execute(
            "SELECT symbol, expiry_date, bucket_1m, underlying_ltp FROM snapshots WHERE snapshot_id = ?",
            [snap_id],
        ).fetchone()
        assert row is not None
        assert row[0] == SYM
        assert row[2] == 0   # 09:15 → bucket 0
        assert row[3] == pytest.approx(SPOT)

    def test_expiry_upserted(self, duck_store: DuckDBStore, duck_conn: object) -> None:
        duck_store.save_snapshot(_make_df(_STRIKES), SYM, EXPIRY, _ts(9, 15))
        row = duck_conn.execute(
            "SELECT symbol, expiry_date FROM expiries WHERE symbol=? AND expiry_date=?",
            [SYM, EXPIRY],
        ).fetchone()
        assert row is not None

    def test_empty_df_raises(self, duck_store: DuckDBStore) -> None:
        with pytest.raises(ValueError, match="Empty"):
            duck_store.save_snapshot(pd.DataFrame(), SYM, EXPIRY, _ts(9, 15))

    def test_bucket_1m_pre_open(self, duck_store: DuckDBStore, duck_conn: object) -> None:
        df = _make_df(_STRIKES)
        snap_id = duck_store.save_snapshot(df, SYM, EXPIRY, _ts(9, 0), is_session_base=True)
        row = duck_conn.execute(
            "SELECT bucket_1m, is_session_base FROM snapshots WHERE snapshot_id=?",
            [snap_id],
        ).fetchone()
        assert row[0] == -15      # 09:00 is 15 min before 09:15
        assert row[1] is True

    def test_expiry_as_string(self, duck_store: DuckDBStore, duck_conn: object) -> None:
        snap_id = duck_store.save_snapshot(_make_df(_STRIKES), SYM, "2026-03-27", _ts(9, 15))
        count = duck_conn.execute(
            "SELECT count(*) FROM chain_rows WHERE snapshot_id=?", [snap_id]
        ).fetchone()[0]
        assert count == len(_STRIKES)


# ---------------------------------------------------------------------------
# Delta vs previous snapshot
# ---------------------------------------------------------------------------

class TestDeltaPrev:
    def test_first_snapshot_ref_unavailable(
        self, duck_store: DuckDBStore, duck_conn: object
    ) -> None:
        snap_id = duck_store.save_snapshot(_make_df(_STRIKES), SYM, EXPIRY, _ts(9, 15))
        rows = duck_conn.execute(
            "SELECT ref_available FROM chain_deltas_prev WHERE snapshot_id=?", [snap_id]
        ).fetchall()
        assert len(rows) == len(_STRIKES)
        assert all(r[0] is False for r in rows)

    def test_second_snapshot_ref_available(
        self, duck_store: DuckDBStore, duck_conn: object
    ) -> None:
        duck_store.save_snapshot(_make_df(_STRIKES), SYM, EXPIRY, _ts(9, 15))
        snap2 = duck_store.save_snapshot(_make_df(_STRIKES), SYM, EXPIRY, _ts(9, 16))
        rows = duck_conn.execute(
            "SELECT ref_available FROM chain_deltas_prev WHERE snapshot_id=?", [snap2]
        ).fetchall()
        assert all(r[0] is True for r in rows)

    def test_oi_delta_correct(self, duck_store: DuckDBStore, duck_conn: object) -> None:
        # First snapshot: CE OI = base + strike value
        snap1 = duck_store.save_snapshot(_make_df(_STRIKES, ce_oi_base=100_000), SYM, EXPIRY, _ts(9, 15))
        # Second snapshot: CE OI increased by 1000 for all strikes
        snap2 = duck_store.save_snapshot(_make_df(_STRIKES, ce_oi_base=101_000), SYM, EXPIRY, _ts(9, 16))
        row = duck_conn.execute(
            "SELECT ce_oi_chg FROM chain_deltas_prev WHERE snapshot_id=? AND strike=22500.0",
            [snap2],
        ).fetchone()
        assert row is not None
        assert row[0] == 1000

    def test_delta_count_matches_chain_rows(
        self, duck_store: DuckDBStore, duck_conn: object
    ) -> None:
        duck_store.save_snapshot(_make_df(_STRIKES), SYM, EXPIRY, _ts(9, 15))
        snap2 = duck_store.save_snapshot(_make_df(_STRIKES), SYM, EXPIRY, _ts(9, 16))
        prev_count = duck_conn.execute(
            "SELECT count(*) FROM chain_deltas_prev WHERE snapshot_id=?", [snap2]
        ).fetchone()[0]
        chain_count = duck_conn.execute(
            "SELECT count(*) FROM chain_rows WHERE snapshot_id=?", [snap2]
        ).fetchone()[0]
        assert prev_count == chain_count


# ---------------------------------------------------------------------------
# Delta vs session base
# ---------------------------------------------------------------------------

class TestDeltaBase:
    def test_no_session_base_ref_unavailable(
        self, duck_store: DuckDBStore, duck_conn: object
    ) -> None:
        # Save without is_session_base=True
        snap = duck_store.save_snapshot(_make_df(_STRIKES), SYM, EXPIRY, _ts(9, 15))
        rows = duck_conn.execute(
            "SELECT ref_available FROM chain_deltas_base WHERE snapshot_id=?", [snap]
        ).fetchall()
        assert all(r[0] is False for r in rows)

    def test_session_base_self_delta_is_zero(
        self, duck_store: DuckDBStore, duck_conn: object
    ) -> None:
        # The session base snapshot vs itself → all deltas = 0, ref_available=True
        base = duck_store.save_snapshot(
            _make_df(_STRIKES), SYM, EXPIRY, _ts(9, 0), is_session_base=True
        )
        row = duck_conn.execute(
            "SELECT ce_oi_chg, pe_oi_chg, ref_available FROM chain_deltas_base "
            "WHERE snapshot_id=? AND strike=22500.0",
            [base],
        ).fetchone()
        assert row[2] is True
        assert row[0] == 0
        assert row[1] == 0

    def test_subsequent_snapshot_vs_base(
        self, duck_store: DuckDBStore, duck_conn: object
    ) -> None:
        duck_store.save_snapshot(
            _make_df(_STRIKES, ce_oi_base=100_000), SYM, EXPIRY, _ts(9, 0), is_session_base=True
        )
        snap = duck_store.save_snapshot(
            _make_df(_STRIKES, ce_oi_base=102_000), SYM, EXPIRY, _ts(9, 15)
        )
        row = duck_conn.execute(
            "SELECT ce_oi_chg, ref_available FROM chain_deltas_base "
            "WHERE snapshot_id=? AND strike=22500.0",
            [snap],
        ).fetchone()
        assert row[1] is True
        assert row[0] == 2000


# ---------------------------------------------------------------------------
# upsert_instrument
# ---------------------------------------------------------------------------

class TestUpsertInstrument:
    def test_insert_and_query(self, duck_store: DuckDBStore, duck_conn: object) -> None:
        duck_store.upsert_instrument("NIFTY", 75, 0.05, 50.0)
        row = duck_conn.execute(
            "SELECT lot_size, strike_step FROM instruments WHERE symbol='NIFTY'"
        ).fetchone()
        assert row[0] == 75
        assert row[1] == 50.0

    def test_upsert_updates_lot_size(self, duck_store: DuckDBStore, duck_conn: object) -> None:
        duck_store.upsert_instrument("NIFTY", 75, 0.05, 50.0)
        duck_store.upsert_instrument("NIFTY", 25, 0.05, 50.0)   # lot size changed
        row = duck_conn.execute(
            "SELECT lot_size FROM instruments WHERE symbol='NIFTY'"
        ).fetchone()
        assert row[0] == 25


# ---------------------------------------------------------------------------
# DuckDBReader
# ---------------------------------------------------------------------------

class TestDuckDBReader:
    def _populate(self, store: DuckDBStore, n: int = 5) -> list[int]:
        ids = []
        for i in range(n):
            snap_id = store.save_snapshot(
                _make_df(_STRIKES, ce_oi_base=100_000 + i * 1000),
                SYM, EXPIRY, _ts(9, 15 + i),
            )
            ids.append(snap_id)
        return ids

    def test_get_snapshot_list_count(
        self, duck_store: DuckDBStore, duck_reader: DuckDBReader
    ) -> None:
        self._populate(duck_store, 5)
        snaps = duck_reader.get_snapshot_list(SYM, DATE, EXPIRY)
        assert len(snaps) == 5

    def test_get_snapshot_list_sorted(
        self, duck_store: DuckDBStore, duck_reader: DuckDBReader
    ) -> None:
        self._populate(duck_store, 3)
        snaps = duck_reader.get_snapshot_list(SYM, DATE, EXPIRY)
        buckets = [s["bucket_1m"] for s in snaps]
        assert buckets == sorted(buckets)

    def test_get_chain_rows_shape(
        self, duck_store: DuckDBStore, duck_reader: DuckDBReader
    ) -> None:
        snap_id = duck_store.save_snapshot(_make_df(_STRIKES), SYM, EXPIRY, _ts(9, 15))
        df = duck_reader.get_chain_rows(snap_id)
        assert len(df) == len(_STRIKES)
        assert "ce_oi" in df.columns
        assert "pe_oi" in df.columns

    def test_get_delta_base_ref_unavailable_no_base(
        self, duck_store: DuckDBStore, duck_reader: DuckDBReader
    ) -> None:
        snap_id = duck_store.save_snapshot(_make_df(_STRIKES), SYM, EXPIRY, _ts(9, 15))
        df = duck_reader.get_delta_base(snap_id)
        assert len(df) == len(_STRIKES)
        assert not df["ref_available"].any()

    def test_get_symbols(self, duck_store: DuckDBStore, duck_reader: DuckDBReader) -> None:
        duck_store.save_snapshot(_make_df(_STRIKES), SYM, EXPIRY, _ts(9, 15))
        duck_store.save_snapshot(_make_df(_STRIKES), "BANKNIFTY", EXPIRY, _ts(9, 15))
        syms = duck_reader.get_symbols()
        assert SYM in syms
        assert "BANKNIFTY" in syms

    def test_get_trade_dates(self, duck_store: DuckDBStore, duck_reader: DuckDBReader) -> None:
        duck_store.save_snapshot(_make_df(_STRIKES), SYM, EXPIRY, _ts(9, 15))
        dates = duck_reader.get_trade_dates(SYM)
        assert str(DATE) in dates

    def test_heatmap_matrix_shape(
        self, duck_store: DuckDBStore, duck_reader: DuckDBReader
    ) -> None:
        for i in range(3):
            duck_store.save_snapshot(_make_df(_STRIKES), SYM, EXPIRY, _ts(9, 15 + i))
        result = duck_reader.get_heatmap_matrix(SYM, EXPIRY, DATE, "ce_oi")
        assert len(result["strikes"]) == len(_STRIKES)
        assert len(result["timestamps"]) == 3
        assert len(result["matrix"]) == len(_STRIKES)

    def test_heatmap_invalid_metric_raises(
        self, duck_store: DuckDBStore, duck_reader: DuckDBReader
    ) -> None:
        with pytest.raises(ValueError, match="Invalid metric"):
            duck_reader.get_heatmap_matrix(SYM, EXPIRY, DATE, "DROP TABLE snapshots")


# ---------------------------------------------------------------------------
# _bucket_1m helper
# ---------------------------------------------------------------------------

class TestBucket1m:
    def test_at_open(self) -> None:
        assert _bucket_1m(_ts(9, 15)) == 0

    def test_one_minute_after_open(self) -> None:
        assert _bucket_1m(_ts(9, 16)) == 1

    def test_last_bucket(self) -> None:
        assert _bucket_1m(_ts(15, 29)) == 374

    def test_pre_open(self) -> None:
        assert _bucket_1m(_ts(9, 0)) == -15


# ---------------------------------------------------------------------------
# _validate_metric
# ---------------------------------------------------------------------------

class TestValidateMetric:
    def test_valid_passes(self) -> None:
        _validate_metric("ce_oi")
        _validate_metric("pe_iv")

    def test_injection_attempt_raises(self) -> None:
        with pytest.raises(ValueError):
            _validate_metric("ce_oi; DROP TABLE chain_rows")

    def test_unknown_column_raises(self) -> None:
        with pytest.raises(ValueError):
            _validate_metric("underlyingValue")
