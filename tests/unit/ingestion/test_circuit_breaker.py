"""Unit tests for CircuitBreaker."""

from __future__ import annotations

import asyncio
import time

import pytest

from ochain_v2.ingestion.circuit_breaker import CircuitBreaker, CircuitOpenError, CircuitState


# ---------------------------------------------------------------------------
# Initial state
# ---------------------------------------------------------------------------

def test_initial_state_closed():
    cb = CircuitBreaker()
    assert cb.state("k") == CircuitState.CLOSED
    assert not cb.is_open("k")


# ---------------------------------------------------------------------------
# Success path — stays CLOSED
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_success_stays_closed():
    cb = CircuitBreaker(failure_threshold=3)
    for _ in range(10):
        async with cb.guard("k"):
            pass  # no exception
    assert cb.state("k") == CircuitState.CLOSED


# ---------------------------------------------------------------------------
# Failure threshold → OPEN
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_opens_after_threshold():
    cb = CircuitBreaker(failure_threshold=3)
    for _ in range(3):
        with pytest.raises(RuntimeError):
            async with cb.guard("k"):
                raise RuntimeError("boom")
    assert cb.state("k") == CircuitState.OPEN
    assert cb.is_open("k")


@pytest.mark.asyncio
async def test_open_circuit_raises_circuit_open_error():
    cb = CircuitBreaker(failure_threshold=2)
    for _ in range(2):
        with pytest.raises(RuntimeError):
            async with cb.guard("k"):
                raise RuntimeError()

    with pytest.raises(CircuitOpenError):
        async with cb.guard("k"):
            pass  # should never reach here


# ---------------------------------------------------------------------------
# Recovery: OPEN → HALF_OPEN after timeout
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_transitions_to_half_open_after_timeout():
    cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0.05)
    with pytest.raises(ValueError):
        async with cb.guard("k"):
            raise ValueError()

    assert cb.state("k") == CircuitState.OPEN
    await asyncio.sleep(0.1)
    assert cb.state("k") == CircuitState.HALF_OPEN


@pytest.mark.asyncio
async def test_half_open_success_closes_circuit():
    cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0.05)
    with pytest.raises(ValueError):
        async with cb.guard("k"):
            raise ValueError()
    await asyncio.sleep(0.1)

    async with cb.guard("k"):
        pass  # succeeds in HALF_OPEN

    assert cb.state("k") == CircuitState.CLOSED


@pytest.mark.asyncio
async def test_half_open_failure_reopens():
    cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0.05)
    with pytest.raises(ValueError):
        async with cb.guard("k"):
            raise ValueError()
    await asyncio.sleep(0.1)

    with pytest.raises(ValueError):
        async with cb.guard("k"):
            raise ValueError()

    assert cb.state("k") == CircuitState.OPEN


# ---------------------------------------------------------------------------
# Manual reset
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_reset_closes_open_circuit():
    cb = CircuitBreaker(failure_threshold=1)
    with pytest.raises(RuntimeError):
        async with cb.guard("k"):
            raise RuntimeError()
    assert cb.is_open("k")
    cb.reset("k")
    assert cb.state("k") == CircuitState.CLOSED


# ---------------------------------------------------------------------------
# Key isolation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_keys_are_independent():
    cb = CircuitBreaker(failure_threshold=2)
    for _ in range(2):
        with pytest.raises(RuntimeError):
            async with cb.guard("a"):
                raise RuntimeError()
    assert cb.is_open("a")
    assert cb.state("b") == CircuitState.CLOSED


# ---------------------------------------------------------------------------
# CircuitOpenError is not re-recorded as a failure
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_circuit_open_error_not_counted_as_failure():
    cb = CircuitBreaker(failure_threshold=3)
    for _ in range(3):
        with pytest.raises(RuntimeError):
            async with cb.guard("k"):
                raise RuntimeError()
    # Now open — trigger several CircuitOpenError and confirm failure count
    # doesn't keep rising (it would if CircuitOpenError were re-caught)
    for _ in range(5):
        with pytest.raises(CircuitOpenError):
            async with cb.guard("k"):
                pass
    assert cb._failures.get("k", 0) == 3  # still at threshold, not higher
