"""
Broker protocol and shared status dataclass.

Every broker adapter (Dhan, Kite, Fixture) must satisfy BrokerProtocol.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Protocol, runtime_checkable

import pandas as pd


@dataclass
class BrokerStatus:
    broker_name:    str
    is_connected:   bool               = False
    last_poll_at:   Optional[datetime] = None
    last_error:     Optional[str]      = None
    error_count:    int                = 0
    request_count:  int                = 0
    symbols_active: list[str]          = field(default_factory=list)


@runtime_checkable
class BrokerProtocol(Protocol):
    """
    Async interface every broker adapter must implement.

    DataFrames returned by ``get_option_chain`` should use broker-native
    column names (e.g. ``CE_openInterest``, ``strikePrice``).
    ``DuckDBStore._normalize_chain_df`` handles renaming to schema columns.
    The column ``underlyingValue``, if present, is used for ``underlying_ltp``.
    """

    name: str

    @property
    def is_connected(self) -> bool: ...

    @property
    def status(self) -> BrokerStatus: ...

    async def connect(self) -> None:
        """Establish connection / authenticate."""
        ...

    async def disconnect(self) -> None:
        """Gracefully close the connection."""
        ...

    async def get_expiries(self, symbol: str) -> list[str]:
        """Return active expiry date strings (ISO format, sorted near→far)."""
        ...

    async def get_option_chain(self, symbol: str, expiry: str) -> pd.DataFrame:
        """
        Fetch the full option chain snapshot for (symbol, expiry).

        Returns a DataFrame with broker-native column names.
        Raises ``BrokerError`` (or subclass) on any network / auth failure.
        """
        ...
