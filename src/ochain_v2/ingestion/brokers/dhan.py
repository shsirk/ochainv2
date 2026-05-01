"""
Dhan broker adapter wrapping the Tradehull SDK.

The ``dhanhq`` / Tradehull package is an optional dependency (``pip install
ochain-v2[dhan]``).  If it is not installed the class can still be imported;
any call to ``connect()`` or ``get_option_chain()`` will raise ``BrokerError``
with a helpful install message.

Architecture notes
------------------
* Tradehull is a synchronous SDK.  All blocking calls are dispatched via
  ``asyncio.get_event_loop().run_in_executor(None, ...)`` so the async event
  loop is never blocked.
* Expiries are cached per symbol with a date-aware TTL: the cache is
  invalidated at midnight IST so a new trading day always fetches fresh
  expiries from the broker.
* Status fields are updated under a lock so ``status`` is safe to read from
  any coroutine.
* Credentials are read from the path given in settings (default:
  ``%LOCALAPPDATA%\\OChain\\dhan_creds.yaml``).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd

from ochain_v2.core.errors import BrokerError
from ochain_v2.core.timezones import now_ist
from ochain_v2.ingestion.brokers.base import BrokerProtocol, BrokerStatus

log = logging.getLogger(__name__)

_INSTALL_MSG = (
    "Tradehull/dhanhq is not installed.  "
    "Run: pip install ochain-v2[dhan]"
)

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _load_credentials(creds_path: str | Path) -> dict:
    import yaml
    p = Path(creds_path)
    if not p.exists():
        raise BrokerError(
            f"Dhan credentials file not found: {p}.  "
            "Copy config/settings.example.yaml and set collector.dhan_creds_path."
        )
    data = yaml.safe_load(p.read_text()) or {}
    if "client_id" not in data or "access_token" not in data:
        raise BrokerError(
            f"Dhan credentials file {p} must contain 'client_id' and 'access_token'."
        )
    return data


# ---------------------------------------------------------------------------
# DhanBroker
# ---------------------------------------------------------------------------

class DhanBroker:
    """
    Dhan / Tradehull broker adapter.

    Parameters
    ----------
    creds_path : str | Path
        Path to YAML file with ``client_id`` and ``access_token`` keys.
    expiry_cache_ttl_days : int
        Number of days to cache expiry lists per symbol (default 1 — refreshes
        daily at midnight IST because the cache key includes the trade date).
    """

    name = "dhan"

    def __init__(
        self,
        creds_path: str | Path = "",
        expiry_cache_ttl_days: int = 1,
    ) -> None:
        self._creds_path = creds_path
        self._expiry_cache_ttl_days = expiry_cache_ttl_days
        self._th = None              # Tradehull instance, set on connect()
        self._status = BrokerStatus(broker_name="dhan")
        self._lock = asyncio.Lock()
        # Expiry cache: key = (symbol, trade_date); value = list[str]
        self._expiry_cache: dict[tuple[str, date], list[str]] = {}

    # ------------------------------------------------------------------
    # BrokerProtocol
    # ------------------------------------------------------------------

    @property
    def is_connected(self) -> bool:
        return self._th is not None and self._status.is_connected

    @property
    def status(self) -> BrokerStatus:
        return self._status

    async def connect(self) -> None:
        """Load credentials and initialise the Tradehull session."""
        try:
            import tradehull  # type: ignore[import]
        except ImportError:
            raise BrokerError(_INSTALL_MSG) from None

        loop = asyncio.get_running_loop()
        creds = await loop.run_in_executor(None, _load_credentials, self._creds_path)

        def _init() -> object:
            return tradehull.Tradehull(
                client_id=creds["client_id"],
                access_token=creds["access_token"],
            )

        th = await loop.run_in_executor(None, _init)
        async with self._lock:
            self._th = th
            self._status.is_connected = True
        log.info("DhanBroker connected (client_id=%s)", creds.get("client_id"))

    async def disconnect(self) -> None:
        async with self._lock:
            self._th = None
            self._status.is_connected = False
        log.info("DhanBroker disconnected")

    async def get_expiries(self, symbol: str) -> list[str]:
        """Return sorted expiry list, using the per-day cache."""
        trade_date = now_ist().date()
        cache_key = (symbol, trade_date)
        if cache_key in self._expiry_cache:
            return self._expiry_cache[cache_key]

        raw = await self._run(lambda th: th.get_expiry_list(symbol))
        # Normalise to ISO strings and sort near→far
        expiries = sorted(
            e if isinstance(e, str) else e.strftime("%Y-%m-%d")
            for e in (raw or [])
        )
        self._expiry_cache[cache_key] = expiries
        return expiries

    async def get_option_chain(self, symbol: str, expiry: str) -> pd.DataFrame:
        """
        Fetch the full option chain from Tradehull.

        Tradehull returns a list of dicts with broker-native keys; we wrap
        them in a DataFrame without renaming so ``DuckDBStore._normalize_chain_df``
        can apply ``_BROKER_TO_SCHEMA`` in the normal path.
        """
        self._status.request_count += 1
        raw = await self._run(lambda th: th.get_option_chain(symbol, expiry))

        if not raw:
            raise BrokerError(
                f"Tradehull returned empty option chain for {symbol}/{expiry}"
            )

        df = pd.DataFrame(raw) if not isinstance(raw, pd.DataFrame) else raw
        self._status.last_poll_at = now_ist()
        log.debug(
            "DhanBroker got %d strikes for %s/%s", len(df), symbol, expiry
        )
        return df

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _run(self, fn):
        """Run a synchronous Tradehull call in a thread-pool executor."""
        if self._th is None:
            raise BrokerError("DhanBroker is not connected.  Call connect() first.")
        loop = asyncio.get_running_loop()
        th = self._th
        try:
            return await loop.run_in_executor(None, lambda: fn(th))
        except Exception as exc:
            async with self._lock:
                self._status.last_error = str(exc)
                self._status.error_count += 1
            raise BrokerError(f"Tradehull call failed: {exc}") from exc


assert isinstance(DhanBroker(), BrokerProtocol)
