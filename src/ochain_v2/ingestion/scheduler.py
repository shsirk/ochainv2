"""
Collector — orchestrates all InstrumentExpiryJobs.

Responsibilities
----------------
* Load instrument and expiry configuration from settings.
* Create one ``InstrumentExpiryJob`` per (symbol, expiry) pair.
* Gate all jobs behind market-hours checks: sleep until market open,
  stop all jobs at market close.
* Handle SIGTERM / SIGINT gracefully: signal all jobs to stop, wait for
  in-flight fetches to complete, then exit.
* Expose a ``status()`` method for the collector HTTP API.

Architecture note
-----------------
All jobs run as concurrent asyncio tasks via ``asyncio.gather``.  The shared
``stop_event`` (``asyncio.Event``) is set on signal or when the market closes,
which causes every job's ``run_loop`` to exit cleanly.
"""

from __future__ import annotations

import asyncio
import logging
import signal
from datetime import date
from typing import Optional

from ochain_v2.core.errors import BrokerError
from ochain_v2.core.timezones import now_ist
from ochain_v2.ingestion.circuit_breaker import CircuitBreaker
from ochain_v2.ingestion.job import InstrumentExpiryJob
from ochain_v2.ingestion.live_publisher import LivePublisher
from ochain_v2.ingestion.market_hours import (
    is_market_open,
    seconds_until_close,
    sleep_until_open,
)
from ochain_v2.ingestion.token_bucket import AsyncTokenBucket

log = logging.getLogger(__name__)


class Collector:
    """
    Top-level collector orchestrator.

    Parameters
    ----------
    broker : BrokerProtocol
        The connected broker adapter.
    store : DuckDBStore
        Write-side DuckDB connection.
    publisher : LivePublisher
        Event fan-out for WebSocket subscribers.
    symbols : list[str]
        Instruments to collect (e.g. ``['NIFTY', 'BANKNIFTY']``).
    expiries_per_symbol : int
        How many expiries to fetch per symbol on startup (default 2).
    poll_interval : float
        Seconds between polls per job (default 60).
    rate : float
        Token-bucket rate in calls/second (default 5.0).
    circuit_threshold : int
        Consecutive failures before opening the circuit (default 5).
    circuit_timeout : float
        Seconds before HALF_OPEN transition (default 300).
    source : str
        Label stored in ``snapshots.source`` (default ``'live'``).
    """

    def __init__(
        self,
        broker,
        store,
        publisher: Optional[LivePublisher] = None,
        symbols: Optional[list[str]] = None,
        expiries_per_symbol: int = 2,
        poll_interval: float = 60.0,
        rate: float = 5.0,
        circuit_threshold: int = 5,
        circuit_timeout: float = 300.0,
        source: str = "live",
    ) -> None:
        self._broker = broker
        self._store = store
        self._publisher = publisher or LivePublisher()
        self._symbols = list(symbols or [])
        self._expiries_per_symbol = expiries_per_symbol
        self._poll_interval = poll_interval
        self._source = source

        self._bucket = AsyncTokenBucket(rate=rate)
        self._breaker = CircuitBreaker(
            failure_threshold=circuit_threshold,
            recovery_timeout=circuit_timeout,
        )
        self._session_base_dates: dict[tuple[str, str], date] = {}
        self._jobs: list[InstrumentExpiryJob] = []
        self._stop_event: asyncio.Event = asyncio.Event()
        self._running = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """
        Main entry point.

        1. Connect to the broker.
        2. Wait for market open.
        3. Resolve expiries for each symbol.
        4. Spawn one job per (symbol, expiry).
        5. Run all jobs concurrently until market close or stop signal.
        6. Repeat from step 2 for the next trading session.
        """
        self._setup_signal_handlers()

        while not self._stop_event.is_set():
            await sleep_until_open()
            if self._stop_event.is_set():
                break

            await self._connect_broker()
            if self._stop_event.is_set():
                break

            expiry_pairs = await self._resolve_expiries()
            if not expiry_pairs:
                log.warning("No expiries found — sleeping 60 s and retrying")
                await asyncio.sleep(60)
                continue

            self._jobs = [
                InstrumentExpiryJob(
                    symbol=sym,
                    expiry=exp,
                    broker=self._broker,
                    store=self._store,
                    publisher=self._publisher,
                    token_bucket=self._bucket,
                    circuit_breaker=self._breaker,
                    poll_interval=self._poll_interval,
                    source=self._source,
                    session_base_dates=self._session_base_dates,
                )
                for sym, exp in expiry_pairs
            ]
            log.info(
                "Collector starting %d jobs: %s",
                len(self._jobs),
                [f"{j.symbol}/{j.expiry}" for j in self._jobs],
            )
            self._running = True

            # Schedule a task that fires when market closes
            close_watcher = asyncio.create_task(self._wait_for_close())

            job_tasks = [
                asyncio.create_task(j.run_loop(self._stop_event))
                for j in self._jobs
            ]
            await asyncio.gather(close_watcher, *job_tasks, return_exceptions=True)
            self._running = False
            log.info("All jobs completed — market session ended")

        log.info("Collector shut down cleanly")

    def stop(self) -> None:
        """Signal all jobs to stop after their current poll completes."""
        log.info("Collector stop requested")
        self._stop_event.set()

    def status(self) -> dict:
        """Return a status dict suitable for the collector HTTP API."""
        return {
            "running": self._running,
            "jobs": [
                {
                    "symbol": j.symbol,
                    "expiry": j.expiry,
                    "circuit_state": self._breaker.state(f"{j.symbol}:{j.expiry}").value,
                }
                for j in self._jobs
            ],
            "broker": {
                "name": self._broker.name,
                "connected": self._broker.is_connected,
                "error_count": self._broker.status.error_count,
                "request_count": self._broker.status.request_count,
            },
            "subscribers": self._publisher.subscriber_count,
        }

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _connect_broker(self) -> None:
        if self._broker.is_connected:
            return
        try:
            await self._broker.connect()
            log.info("Broker '%s' connected", self._broker.name)
        except BrokerError as exc:
            log.error("Broker connect failed: %s", exc)

    async def _resolve_expiries(self) -> list[tuple[str, str]]:
        """Return list of (symbol, expiry) pairs for all configured symbols."""
        pairs: list[tuple[str, str]] = []
        for sym in self._symbols:
            try:
                expiries = await self._broker.get_expiries(sym)
                for exp in expiries[: self._expiries_per_symbol]:
                    pairs.append((sym, exp))
            except BrokerError as exc:
                log.error("Failed to get expiries for %s: %s", sym, exc)
        return pairs

    async def _wait_for_close(self) -> None:
        """Suspend until market closes, then trigger stop."""
        while not self._stop_event.is_set():
            if not is_market_open():
                log.info("Market closed — stopping all jobs")
                self._stop_event.set()
                return
            secs = seconds_until_close()
            # Poll every 30 s for the last minute; otherwise sleep in 30s ticks
            await asyncio.sleep(min(30.0, max(1.0, secs - 30.0)))

        # Reset stop_event so the outer loop can sleep until next open
        self._stop_event.clear()

    def _setup_signal_handlers(self) -> None:
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self._handle_signal)
            except (NotImplementedError, OSError):
                # Windows does not support add_signal_handler for all signals
                pass

    def _handle_signal(self) -> None:
        log.warning("Signal received — stopping collector")
        # Set stop permanently so outer loop exits too
        self._running = False
        self._stop_event.set()
        # Prevent the outer loop from re-entering after market-hours reset
        self._symbols = []
