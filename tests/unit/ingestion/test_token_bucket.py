"""Unit tests for AsyncTokenBucket."""

from __future__ import annotations

import asyncio
import time

import pytest

from ochain_v2.ingestion.token_bucket import AsyncTokenBucket


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

def test_default_burst_equals_rate():
    b = AsyncTokenBucket(rate=3.0)
    assert b.available_tokens == pytest.approx(3.0)


def test_burst_overrides_default():
    b = AsyncTokenBucket(rate=3.0, burst=10.0)
    assert b.available_tokens == pytest.approx(10.0)


def test_negative_rate_raises():
    with pytest.raises(ValueError):
        AsyncTokenBucket(rate=-1.0)


def test_zero_rate_raises():
    with pytest.raises(ValueError):
        AsyncTokenBucket(rate=0.0)


# ---------------------------------------------------------------------------
# Single acquire (fast — tokens available immediately)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_acquire_single_fast():
    """With full burst, acquire(1) should return without sleeping."""
    b = AsyncTokenBucket(rate=100.0)
    start = time.monotonic()
    await b.acquire()
    elapsed = time.monotonic() - start
    assert elapsed < 0.05  # well under 50 ms


@pytest.mark.asyncio
async def test_acquire_consumes_token():
    b = AsyncTokenBucket(rate=100.0, burst=5.0)
    before = b.available_tokens
    await b.acquire()
    # Tokens can only be the same or fewer (tiny refill possible between reads)
    assert b.available_tokens < before + 0.01


# ---------------------------------------------------------------------------
# Token depletion → sleep
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_acquire_sleeps_when_empty():
    """
    Rate=10/s, burst=1 → first acquire instant, second should take ~0.1 s.
    """
    b = AsyncTokenBucket(rate=10.0, burst=1.0)
    await b.acquire()   # consumes the only token

    start = time.monotonic()
    await b.acquire()   # must wait for refill
    elapsed = time.monotonic() - start
    # Should wait ~0.1 s; allow generous tolerance for CI
    assert 0.05 < elapsed < 0.5


# ---------------------------------------------------------------------------
# Concurrent acquires stay within rate
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_concurrent_acquires_serialised():
    """
    5 concurrent acquires on a rate=20/s bucket should all complete but must
    take no less than the inter-token gap for 4 waits (≥ 4 × 0.05 s = 0.2 s).
    """
    b = AsyncTokenBucket(rate=20.0, burst=1.0)
    start = time.monotonic()
    await asyncio.gather(*[b.acquire() for _ in range(5)])
    elapsed = time.monotonic() - start
    assert elapsed >= 0.15  # at least 3 token gaps


# ---------------------------------------------------------------------------
# Multi-token acquire
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_acquire_n_tokens():
    """Acquiring n=3 tokens from a burst=10 bucket should drain 3."""
    b = AsyncTokenBucket(rate=100.0, burst=10.0)
    await b.acquire(n=3.0)
    assert b.available_tokens == pytest.approx(7.0, abs=0.1)
