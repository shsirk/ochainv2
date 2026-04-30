"""
Shared pytest fixtures for OChain v2 test suite.
"""

from datetime import date, datetime
from typing import Generator

import duckdb
import pandas as pd
import pytest
import pytz

from ochain_v2.db.duckdb_store import DuckDBStore
from ochain_v2.db.duckdb_reader import DuckDBReader

IST = pytz.timezone("Asia/Kolkata")
SAMPLE_SYMBOL = "NIFTY"
SAMPLE_EXPIRY = date(2026, 3, 27)
SAMPLE_DATE = date(2026, 3, 20)
SAMPLE_SPOT = 22500.0


# ---------------------------------------------------------------------------
# In-memory DuckDB (isolated per test)
# ---------------------------------------------------------------------------

@pytest.fixture
def duck_conn() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Fresh in-memory DuckDB connection, schema applied, torn down after test."""
    conn = duckdb.connect(":memory:")
    _apply_schema(conn)
    yield conn
    conn.close()


def _apply_schema(conn: duckdb.DuckDBPyConnection) -> None:
    schema_path = (
        __file__[: __file__.rfind("tests")]
        + "src/ochain_v2/db/schema.sql"
    )
    try:
        with open(schema_path) as f:
            conn.execute(f.read())
    except FileNotFoundError:
        # schema not written yet during Phase 0
        pass


# ---------------------------------------------------------------------------
# Sample option chain DataFrame (mirrors v1 raw_json shape)
# ---------------------------------------------------------------------------

def _make_chain_row(
    strike: float,
    ce_oi: int = 100_000,
    pe_oi: int = 80_000,
    ce_ltp: float = 50.0,
    pe_ltp: float = 40.0,
    ce_iv: float = 15.0,
    pe_iv: float = 14.0,
    ce_volume: int = 5000,
    pe_volume: int = 4000,
) -> dict:
    return {
        "strikePrice": strike,
        "CE_openInterest": ce_oi,
        "CE_lastPrice": ce_ltp,
        "CE_impliedVolatility": ce_iv,
        "CE_totalTradedVolume": ce_volume,
        "CE_changeinOpenInterest": 0,
        "CE_bidQty": 75,
        "CE_bidprice": ce_ltp - 0.5,
        "CE_askPrice": ce_ltp + 0.5,
        "CE_askQty": 75,
        "PE_openInterest": pe_oi,
        "PE_lastPrice": pe_ltp,
        "PE_impliedVolatility": pe_iv,
        "PE_totalTradedVolume": pe_volume,
        "PE_changeinOpenInterest": 0,
        "PE_bidQty": 75,
        "PE_bidprice": pe_ltp - 0.5,
        "PE_askPrice": pe_ltp + 0.5,
        "PE_askQty": 75,
        "underlyingValue": SAMPLE_SPOT,
    }


@pytest.fixture
def sample_chain_df() -> pd.DataFrame:
    """A minimal 11-strike option chain centred on ATM."""
    atm = int(SAMPLE_SPOT / 50) * 50  # nearest 50
    strikes = [atm + (i - 5) * 50 for i in range(11)]
    rows = [_make_chain_row(float(s)) for s in strikes]
    return pd.DataFrame(rows)


@pytest.fixture
def duck_store(duck_conn: duckdb.DuckDBPyConnection) -> DuckDBStore:
    """DuckDBStore wired to the per-test in-memory connection."""
    return DuckDBStore(_conn=duck_conn)


@pytest.fixture
def duck_reader(duck_conn: duckdb.DuckDBPyConnection) -> DuckDBReader:
    """DuckDBReader wired to the per-test in-memory connection."""
    return DuckDBReader("", _conn=duck_conn)


@pytest.fixture
def sample_snapshot_ts() -> datetime:
    return IST.localize(datetime(SAMPLE_DATE.year, SAMPLE_DATE.month, SAMPLE_DATE.day, 9, 15, 0))


@pytest.fixture
def sample_snapshots(sample_chain_df: pd.DataFrame) -> list[dict]:
    """A list of 5 snapshot dicts as returned by db.get_snapshots_for_date."""
    snaps = []
    for i in range(5):
        ts = IST.localize(
            datetime(SAMPLE_DATE.year, SAMPLE_DATE.month, SAMPLE_DATE.day, 9, 15 + i, 0)
        )
        snaps.append(
            {
                "ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
                "symbol": SAMPLE_SYMBOL,
                "expiry": str(SAMPLE_EXPIRY),
                "data": sample_chain_df.to_dict(orient="records"),
            }
        )
    return snaps
