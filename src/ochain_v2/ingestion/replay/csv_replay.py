"""
CSV replay harness — feed historical option chain rows from a CSV file
as if they were arriving from a live broker.

Expected CSV format (v1-compatible)
------------------------------------
Columns must include at least:

    symbol, expiry, ts (or timestamp), strike (or strikePrice),
    CE_openInterest, CE_totalTradedVolume, CE_lastPrice,
    CE_impliedVolatility, PE_openInterest, PE_totalTradedVolume,
    PE_lastPrice, PE_impliedVolatility, underlyingValue (optional)

Timestamps are parsed with ``pandas.to_datetime`` and converted to IST.

Usage
-----
    replay = CsvReplay("data/nifty_2025.csv", speed=1.0)
    async for symbol, expiry, ts, df in replay.stream():
        await store.save_snapshot(df, symbol, expiry, ts)
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import AsyncGenerator

import pandas as pd

from ochain_v2.core.timezones import IST, to_ist

log = logging.getLogger(__name__)

# Column aliases: normalise common naming variants before processing
_TS_ALIASES = ("ts", "timestamp", "datetime", "time")
_SYM_ALIASES = ("symbol",)
_EXP_ALIASES = ("expiry", "expiry_date", "expiry date")
_STRIKE_ALIASES = ("strike", "strikePrice", "strike_price", "Strike Price")


def _find_col(df: pd.DataFrame, aliases: tuple[str, ...]) -> str:
    for a in aliases:
        if a in df.columns:
            return a
    raise KeyError(f"Could not find any of {aliases!r} in columns {list(df.columns)!r}")


class CsvReplay:
    """
    Replay option chain data from a CSV file.

    Parameters
    ----------
    path : str | Path
        Path to the CSV file.
    speed : float
        Replay speed multiplier (default 1.0 = real-time).
        0.0 = no sleeping (replay as fast as possible, useful for tests).
    symbol : str | None
        If set, only rows with this symbol are replayed.
    expiry : str | None
        If set, only rows with this expiry are replayed.
    """

    def __init__(
        self,
        path: str | Path,
        speed: float = 0.0,
        symbol: str | None = None,
        expiry: str | None = None,
    ) -> None:
        self._path = Path(path)
        self._speed = speed
        self._filter_symbol = symbol
        self._filter_expiry = expiry

    async def stream(
        self,
    ) -> AsyncGenerator[tuple[str, str, pd.Timestamp, pd.DataFrame], None]:
        """
        Async generator yielding ``(symbol, expiry, ts, df)`` tuples.

        *df* contains all strikes for that snapshot in broker-native column
        names so it can be passed directly to ``DuckDBStore.save_snapshot``.
        """
        df_all = await asyncio.get_running_loop().run_in_executor(
            None, self._load
        )

        ts_col  = _find_col(df_all, _TS_ALIASES)
        sym_col = _find_col(df_all, _SYM_ALIASES)
        exp_col = _find_col(df_all, _EXP_ALIASES)

        if self._filter_symbol:
            df_all = df_all[df_all[sym_col] == self._filter_symbol]
        if self._filter_expiry:
            df_all = df_all[df_all[exp_col] == self._filter_expiry]

        groups = df_all.groupby([sym_col, exp_col, ts_col], sort=True)
        prev_ts = None

        for (sym, exp, ts_raw), group_df in groups:
            ts = pd.Timestamp(ts_raw)
            if ts.tzinfo is None:
                ts = ts.tz_localize(IST)
            else:
                ts = ts.tz_convert(IST)

            if self._speed > 0.0 and prev_ts is not None:
                gap = (ts - prev_ts).total_seconds()
                wait = gap / self._speed
                if wait > 0:
                    await asyncio.sleep(wait)

            prev_ts = ts
            yield str(sym), str(exp), ts, group_df.reset_index(drop=True)

    def _load(self) -> pd.DataFrame:
        df = pd.read_csv(self._path, low_memory=False)
        log.info("CsvReplay loaded %d rows from %s", len(df), self._path)
        return df
