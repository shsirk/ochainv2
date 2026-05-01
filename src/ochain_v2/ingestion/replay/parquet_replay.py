"""
Parquet replay harness — feed historical option chain rows from Parquet
partitions as if they were arriving from a live broker.

Expected Parquet schema
-----------------------
Same columns as ``CsvReplay`` (see csv_replay.py).  Partition columns
``symbol`` and ``expiry_date`` are optional but improve load performance
when filtering by symbol/expiry.

Usage
-----
    replay = ParquetReplay("data/archive/2025-03/", speed=0.0)
    async for symbol, expiry, ts, df in replay.stream():
        await store.save_snapshot(df, symbol, expiry, ts)
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import AsyncGenerator, Optional

import pandas as pd

from ochain_v2.core.timezones import IST
from ochain_v2.ingestion.replay.csv_replay import _find_col, _TS_ALIASES, _SYM_ALIASES, _EXP_ALIASES

log = logging.getLogger(__name__)


class ParquetReplay:
    """
    Replay option chain data from a Parquet file or directory.

    Parameters
    ----------
    path : str | Path
        Path to a ``.parquet`` file or a directory containing them
        (optionally partitioned by Hive convention).
    speed : float
        Replay speed multiplier (default 0.0 = no sleeping).
    symbol : str | None
        Filter to a single symbol.
    expiry : str | None
        Filter to a single expiry.
    columns : list[str] | None
        If set, read only these columns (for large files).
    """

    def __init__(
        self,
        path: str | Path,
        speed: float = 0.0,
        symbol: Optional[str] = None,
        expiry: Optional[str] = None,
        columns: Optional[list[str]] = None,
    ) -> None:
        self._path = Path(path)
        self._speed = speed
        self._filter_symbol = symbol
        self._filter_expiry = expiry
        self._columns = columns

    async def stream(
        self,
    ) -> AsyncGenerator[tuple[str, str, pd.Timestamp, pd.DataFrame], None]:
        """
        Async generator yielding ``(symbol, expiry, ts, df)`` tuples.

        Identical contract to ``CsvReplay.stream()``.
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
        p = self._path
        if p.is_dir():
            # Load all .parquet files under the directory
            files = sorted(p.rglob("*.parquet"))
            if not files:
                raise FileNotFoundError(f"No .parquet files found under {p}")
            frames = [pd.read_parquet(f, columns=self._columns) for f in files]
            df = pd.concat(frames, ignore_index=True)
        else:
            df = pd.read_parquet(p, columns=self._columns)

        log.info("ParquetReplay loaded %d rows from %s", len(df), p)
        return df
