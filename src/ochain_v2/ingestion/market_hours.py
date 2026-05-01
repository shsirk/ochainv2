"""
Ingestion-layer market-hours shims.

Re-exports the ``MarketHours`` class and module-level helpers from
``ochain_v2.core.market_hours`` so ingestion code can import from a single
local namespace without pulling in the core package directly.

Adds two scheduler-specific helpers:
  - ``should_poll_now()`` — quick boolean check for the scheduler hot-path
  - ``sleep_until_open()`` — coroutine that suspends until market open
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Optional

from ochain_v2.core.market_hours import (
    MarketHours,
    get_market_hours,
    is_market_open,
    load_market_hours,
    next_open,
    reset_market_hours,
    seconds_until_close,
    seconds_until_open,
)

__all__ = [
    "MarketHours",
    "get_market_hours",
    "is_market_open",
    "load_market_hours",
    "next_open",
    "reset_market_hours",
    "seconds_until_close",
    "seconds_until_open",
    "should_poll_now",
    "sleep_until_open",
]

log = logging.getLogger(__name__)


def should_poll_now(dt: Optional[datetime] = None) -> bool:
    """
    Return True if the collector should fire a polling round right now.

    Equivalent to ``is_market_open(dt)`` — kept as a distinct name so call
    sites read naturally (``if should_poll_now(): ...``).
    """
    return is_market_open(dt)


async def sleep_until_open(dt: Optional[datetime] = None) -> None:
    """
    Suspend the current coroutine until the next market open.

    If the market is already open, returns immediately.  Logs the wait
    duration so operators can confirm the scheduler is sleeping correctly.
    """
    secs = seconds_until_open(dt)
    if secs <= 0.0:
        return
    open_dt = next_open(dt)
    log.info(
        "Market closed — sleeping %.0f s until %s",
        secs,
        open_dt.strftime("%Y-%m-%d %H:%M %Z"),
    )
    await asyncio.sleep(secs)
