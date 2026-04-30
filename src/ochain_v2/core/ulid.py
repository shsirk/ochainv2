"""
Monotonic bigint ID generator for snapshot_id.

Format (64-bit signed int, always positive):
  [41 bits: ms since epoch] [12 bits: per-ms sequence counter]

Properties:
  - Sortable by insertion time (chronological order = numeric order).
  - 4096 unique IDs per millisecond before wrapping.
  - Safe as a DuckDB BIGINT primary key and as a JavaScript number
    (< 2^53, so no precision loss in JSON).
  - Thread-safe via a lock.

Epoch headroom: 2^41 ms ≈ 69 years → safe through 2040+.
"""

from __future__ import annotations

import threading
import time

_SEQ_BITS = 12
_SEQ_MASK = (1 << _SEQ_BITS) - 1  # 0xFFF

_lock = threading.Lock()
_last_ms: int = 0
_seq: int = 0


def new_id() -> int:
    """Return a new monotonic snapshot ID (positive int64)."""
    global _last_ms, _seq

    with _lock:
        ms = int(time.time() * 1000)

        if ms == _last_ms:
            _seq = (_seq + 1) & _SEQ_MASK
            if _seq == 0:
                # Sequence exhausted — spin until next millisecond
                while ms <= _last_ms:
                    ms = int(time.time() * 1000)
        else:
            _last_ms = ms
            _seq = 0

        return (ms << _SEQ_BITS) | _seq


def ts_from_id(snapshot_id: int) -> float:
    """Extract the Unix timestamp (seconds, float) embedded in a snapshot ID."""
    return (snapshot_id >> _SEQ_BITS) / 1000.0


def seq_from_id(snapshot_id: int) -> int:
    """Extract the per-millisecond sequence number from a snapshot ID."""
    return snapshot_id & _SEQ_MASK
