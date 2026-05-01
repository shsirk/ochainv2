"""
Two-level LRU cache for the API layer.

Level 1 — Snapshot sidecar cache
    key   = snapshot_id (int)
    value = fully-computed analysis dict for that snapshot
    TTL   = 1 hour (snapshots are immutable once written)

Level 2 — Window cache
    key   = (symbol, expiry, trade_date, from_idx, to_idx, tf_sec, endpoint)
    value = serialised response dict
    TTL   = 60 seconds (short-lived: index positions change as new snapshots arrive)

Design: pure Python `functools.lru_cache` is not suitable here because we
need TTL-based expiry and thread-safe invalidation.  We use a simple
``OrderedDict``-backed LRU with a ``maxsize`` and per-entry expiry timestamp.
The implementation is intentionally minimal — no background eviction thread.
Entries expire on the next access after their TTL.

Invalidation
-----------
Call ``snapshot_cache.invalidate(snapshot_id)`` from the ingest path to
force re-computation on next read (e.g. after a backfill).
Call ``window_cache.invalidate_symbol(symbol)`` to drop all windows for a
symbol (e.g. after market-hours rollover).
"""

from __future__ import annotations

import threading
import time
from collections import OrderedDict
from typing import Any, Optional


class LRUCache:
    """
    Thread-safe LRU cache with per-entry TTL.

    Parameters
    ----------
    maxsize : int   Maximum number of entries to keep.
    ttl     : float Entry TTL in seconds (0 = never expire).
    """

    def __init__(self, maxsize: int = 512, ttl: float = 3600.0) -> None:
        self._maxsize = maxsize
        self._ttl = ttl
        self._data: OrderedDict[Any, tuple[Any, float]] = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key: Any) -> Optional[Any]:
        with self._lock:
            if key not in self._data:
                return None
            value, expires_at = self._data[key]
            if self._ttl > 0 and time.monotonic() > expires_at:
                del self._data[key]
                return None
            self._data.move_to_end(key)
            return value

    def set(self, key: Any, value: Any) -> None:
        expires_at = time.monotonic() + self._ttl if self._ttl > 0 else float("inf")
        with self._lock:
            if key in self._data:
                self._data.move_to_end(key)
            self._data[key] = (value, expires_at)
            while len(self._data) > self._maxsize:
                self._data.popitem(last=False)  # evict LRU

    def invalidate(self, key: Any) -> None:
        with self._lock:
            self._data.pop(key, None)

    def invalidate_prefix(self, prefix_check) -> None:
        """Remove all entries where ``prefix_check(key)`` is True."""
        with self._lock:
            keys = [k for k in self._data if prefix_check(k)]
            for k in keys:
                del self._data[k]

    def clear(self) -> None:
        with self._lock:
            self._data.clear()

    def __len__(self) -> int:
        return len(self._data)


class ApiCache:
    """
    Composite cache used by the FastAPI dependency.

    Attributes
    ----------
    snapshot : LRUCache   Keyed by snapshot_id (immutable, long TTL).
    window   : LRUCache   Keyed by (symbol, expiry, date, from, to, tf, ep).
    """

    def __init__(
        self,
        snapshot_ttl: float = 3600.0,
        snapshot_maxsize: int = 1000,
        window_ttl: float = 60.0,
        window_maxsize: int = 200,
    ) -> None:
        self.snapshot = LRUCache(maxsize=snapshot_maxsize, ttl=snapshot_ttl)
        self.window   = LRUCache(maxsize=window_maxsize,   ttl=window_ttl)

    def invalidate_symbol(self, symbol: str) -> None:
        """Drop all window-cache entries for *symbol*."""
        self.window.invalidate_prefix(lambda k: isinstance(k, tuple) and k[0] == symbol)


# Module-level singleton — shared across all FastAPI workers in the same process.
_cache: Optional[ApiCache] = None
_cache_lock = threading.Lock()


def get_api_cache() -> ApiCache:
    global _cache
    if _cache is None:
        with _cache_lock:
            if _cache is None:
                try:
                    from ochain_v2.core.settings import get_settings
                    cfg = get_settings()
                    _cache = ApiCache(
                        snapshot_ttl=cfg.cache.snapshot_ttl_sec,
                        snapshot_maxsize=cfg.cache.max_window_entries * 5,
                        window_ttl=cfg.cache.window_ttl_sec,
                        window_maxsize=cfg.cache.max_window_entries,
                    )
                except Exception:
                    _cache = ApiCache()
    return _cache


def reset_api_cache() -> None:
    """Reset the singleton (useful in tests)."""
    global _cache
    with _cache_lock:
        _cache = None
