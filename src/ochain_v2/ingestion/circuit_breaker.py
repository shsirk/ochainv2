"""
Per-key async circuit breaker.

State machine:
  CLOSED    → normal operation
  OPEN      → blocking all calls (too many consecutive failures)
  HALF_OPEN → one test call allowed; success → CLOSED, failure → OPEN

Usage
-----
    breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=300.0)

    async def safe_call(key):
        async with breaker.guard(key):
            result = await broker.get_option_chain(symbol, expiry)
        return result
"""

from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from enum import Enum
from typing import AsyncGenerator

from ochain_v2.core.errors import BrokerError

log = logging.getLogger(__name__)


class CircuitState(str, Enum):
    CLOSED    = "closed"
    OPEN      = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(BrokerError):
    """Raised when a call is attempted against an open circuit."""


class CircuitBreaker:
    """
    Parameters
    ----------
    failure_threshold : consecutive failures before opening the circuit
    recovery_timeout  : seconds before transitioning OPEN → HALF_OPEN
    """

    def __init__(
        self,
        failure_threshold: int   = 5,
        recovery_timeout:  float = 300.0,
    ) -> None:
        self._threshold  = failure_threshold
        self._timeout    = recovery_timeout
        self._failures:  dict[str, int]          = {}
        self._state:     dict[str, CircuitState] = {}
        self._opened_at: dict[str, float]        = {}
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def state(self, key: str) -> CircuitState:
        raw = self._state.get(key, CircuitState.CLOSED)
        if raw == CircuitState.OPEN:
            opened = self._opened_at.get(key, 0.0)
            if time.monotonic() - opened >= self._timeout:
                self._state[key] = CircuitState.HALF_OPEN
                log.info("Circuit '%s' → HALF_OPEN (recovery timeout elapsed)", key)
                return CircuitState.HALF_OPEN
        return raw

    def is_open(self, key: str) -> bool:
        return self.state(key) == CircuitState.OPEN

    def reset(self, key: str) -> None:
        """Manually close the circuit (e.g. after operator intervention)."""
        self._state.pop(key, None)
        self._failures.pop(key, None)
        self._opened_at.pop(key, None)

    @asynccontextmanager
    async def guard(self, key: str) -> AsyncGenerator[None, None]:
        """
        Async context manager that enforces circuit state.

        Raises ``CircuitOpenError`` when the circuit is OPEN.
        Records success/failure automatically.
        """
        current = self.state(key)
        if current == CircuitState.OPEN:
            raise CircuitOpenError(
                f"Circuit '{key}' is OPEN — calls blocked "
                f"(threshold={self._threshold}, timeout={self._timeout}s)"
            )

        try:
            yield
            await self._record_success(key)
        except CircuitOpenError:
            raise
        except Exception:
            await self._record_failure(key)
            raise

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _record_success(self, key: str) -> None:
        async with self._lock:
            self._failures[key] = 0
            if self._state.get(key) in (CircuitState.HALF_OPEN, CircuitState.OPEN):
                self._state[key] = CircuitState.CLOSED
                log.info("Circuit '%s' → CLOSED (recovered)", key)

    async def _record_failure(self, key: str) -> None:
        async with self._lock:
            count = self._failures.get(key, 0) + 1
            self._failures[key] = count
            if count >= self._threshold:
                self._state[key]     = CircuitState.OPEN
                self._opened_at[key] = time.monotonic()
                log.warning(
                    "Circuit '%s' → OPEN after %d consecutive failures", key, count
                )
