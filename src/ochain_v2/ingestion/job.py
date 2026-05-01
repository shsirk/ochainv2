"""
InstrumentExpiryJob — one async polling task per (symbol, expiry) pair.

Each job:
  1. Acquires a rate-limit token from the shared per-broker token bucket.
  2. Guards the call through the per-key circuit breaker.
  3. Calls ``broker.get_option_chain(symbol, expiry)``.
  4. Writes the snapshot to DuckDB (including delta tables, in one transaction).
  5. Publishes a live event to all WebSocket subscribers.

Session-base tracking
---------------------
The first snapshot of each trading day for a (symbol, expiry) pair is flagged
``is_session_base=True`` so the delta-base table has a reference point.  The
job tracks this with ``_session_base_date`` — a dict keyed by (symbol, expiry)
holding the date the session base was already written.  It is reset at process
start, so a restart on the same day correctly re-flags the first snapshot.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date
from typing import TYPE_CHECKING, Optional

from ochain_v2.core.errors import BrokerError, IngestError
from ochain_v2.core.timezones import now_ist, trade_date_ist
from ochain_v2.ingestion.circuit_breaker import CircuitBreaker, CircuitOpenError
from ochain_v2.ingestion.live_publisher import LivePublisher
from ochain_v2.ingestion.token_bucket import AsyncTokenBucket

if TYPE_CHECKING:
    from ochain_v2.db.duckdb_store import DuckDBStore
    from ochain_v2.ingestion.brokers.base import BrokerProtocol

log = logging.getLogger(__name__)


class InstrumentExpiryJob:
    """
    Async polling coroutine for a single (symbol, expiry) pair.

    Parameters
    ----------
    symbol : str
    expiry : str          ISO date string, e.g. '2025-05-29'
    broker : BrokerProtocol
    store  : DuckDBStore
    publisher : LivePublisher
    token_bucket : AsyncTokenBucket   shared per-broker rate limiter
    circuit_breaker : CircuitBreaker  shared per-broker breaker
    poll_interval : float             seconds between polls (default 60)
    source : str                      label stored in ``snapshots.source``
    """

    def __init__(
        self,
        symbol: str,
        expiry: str,
        broker: "BrokerProtocol",
        store: "DuckDBStore",
        publisher: LivePublisher,
        token_bucket: AsyncTokenBucket,
        circuit_breaker: CircuitBreaker,
        poll_interval: float = 60.0,
        source: str = "live",
        # Shared session-base tracker — injected by the Collector so all jobs
        # on the same process share the same date dict.
        session_base_dates: Optional[dict[tuple[str, str], date]] = None,
    ) -> None:
        self.symbol = symbol
        self.expiry = expiry
        self._broker = broker
        self._store = store
        self._publisher = publisher
        self._bucket = token_bucket
        self._breaker = circuit_breaker
        self._poll_interval = poll_interval
        self._source = source
        self._session_base_dates: dict[tuple[str, str], date] = (
            session_base_dates if session_base_dates is not None else {}
        )
        self._key = f"{symbol}:{expiry}"
        self._running = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def run_once(self) -> Optional[int]:
        """
        Execute a single poll-and-store cycle.

        Returns the new ``snapshot_id`` on success, or ``None`` if the circuit
        is open or the DataFrame is empty.  Raises ``IngestError`` for
        unexpected failures (after recording them in the circuit breaker).
        """
        try:
            async with self._breaker.guard(self._key):
                await self._bucket.acquire()
                df = await self._broker.get_option_chain(self.symbol, self.expiry)
        except CircuitOpenError:
            log.warning("Circuit open for %s — skipping poll", self._key)
            return None
        except BrokerError as exc:
            log.error("Broker error for %s: %s", self._key, exc)
            raise IngestError(str(exc)) from exc

        if df is None or df.empty:
            log.warning("Empty DataFrame for %s — skipping", self._key)
            return None

        ts = now_ist()
        trade_date = trade_date_ist(ts)
        sbase_key = (self.symbol, self.expiry)
        is_session_base = self._session_base_dates.get(sbase_key) != trade_date

        loop = asyncio.get_running_loop()
        try:
            snapshot_id = await loop.run_in_executor(
                None,
                lambda: self._store.save_snapshot(
                    df,
                    self.symbol,
                    self.expiry,
                    ts,
                    source=self._source,
                    is_session_base=is_session_base,
                ),
            )
        except Exception as exc:
            raise IngestError(
                f"save_snapshot failed for {self._key}: {exc}"
            ) from exc

        if is_session_base:
            self._session_base_dates[sbase_key] = trade_date
            log.info("Session base written for %s at %s", self._key, ts)

        await self._publisher.publish(
            {
                "symbol":      self.symbol,
                "expiry":      self.expiry,
                "snapshot_id": snapshot_id,
                "ts":          ts.isoformat(),
            }
        )
        log.debug("Snapshot %d written for %s", snapshot_id, self._key)
        return snapshot_id

    async def run_loop(self, stop_event: asyncio.Event) -> None:
        """
        Continuously poll until *stop_event* is set.

        Sleeps for ``poll_interval`` seconds between polls.  Errors are logged
        but do not stop the loop — the circuit breaker handles repeated failures.
        """
        self._running = True
        log.info("Job started: %s (interval=%.0fs)", self._key, self._poll_interval)
        try:
            while not stop_event.is_set():
                try:
                    await self.run_once()
                except IngestError as exc:
                    log.error("IngestError for %s: %s", self._key, exc)
                except Exception as exc:
                    log.exception("Unexpected error in job %s: %s", self._key, exc)

                try:
                    await asyncio.wait_for(
                        stop_event.wait(), timeout=self._poll_interval
                    )
                except asyncio.TimeoutError:
                    pass
        finally:
            self._running = False
            log.info("Job stopped: %s", self._key)
