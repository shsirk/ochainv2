"""
Async token-bucket rate limiter.

Designed for use in a single-process asyncio event loop.  Multiple coroutines
can call ``acquire()`` concurrently; the internal lock serialises token
consumption so the configured rate is never exceeded.

    bucket = AsyncTokenBucket(rate=5.0)  # 5 calls/second

    async def call():
        await bucket.acquire()
        return await broker.get_option_chain(...)
"""

from __future__ import annotations

import asyncio
from typing import Optional


class AsyncTokenBucket:
    """
    Token-bucket rate limiter for async code.

    Parameters
    ----------
    rate  : tokens (calls) per second
    burst : max tokens that can accumulate (defaults to ``rate``)
    """

    def __init__(self, rate: float, burst: Optional[float] = None) -> None:
        if rate <= 0:
            raise ValueError(f"rate must be positive, got {rate}")
        self._rate  = float(rate)
        self._burst = float(burst) if burst is not None else float(rate)
        self._tokens: float = self._burst
        self._last_refill: Optional[float] = None
        self._lock: Optional[asyncio.Lock] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def acquire(self, n: float = 1.0) -> None:
        """Wait until *n* tokens are available, then consume them."""
        lock = self._get_lock()
        async with lock:
            loop = asyncio.get_running_loop()

            if self._last_refill is None:
                self._last_refill = loop.time()

            # Refill based on elapsed time
            now = loop.time()
            elapsed = now - self._last_refill
            self._tokens = min(self._burst, self._tokens + elapsed * self._rate)
            self._last_refill = now

            if self._tokens >= n:
                self._tokens -= n
                return

            # Need to wait for tokens to accumulate
            wait = (n - self._tokens) / self._rate
            self._tokens = 0.0
            self._last_refill = loop.time() + wait  # advance virtual clock
            await asyncio.sleep(wait)

    @property
    def available_tokens(self) -> float:
        """Current token count (approximate — not thread-safe to read)."""
        return self._tokens

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock
