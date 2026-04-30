"""
SQLite store for operational metadata (not time-series data).

Tables
------
collector_status  — one row per (symbol, expiry); updated by the collector.
error_log         — append-only ring of recent errors.
alert_events      — fired alerts ready for the WebSocket feed.
user_views        — named trader views (saved slider positions, etc.).

This module is intentionally simple: no ORM, no migrations framework — just
parameterised queries and a single connection-per-call pattern that is safe
for multiple readers and one writer on SQLite's default journal mode.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Generator, Optional

from ochain_v2.core.timezones import now_ist, ts_str

_DDL = """
CREATE TABLE IF NOT EXISTS collector_status (
    symbol       TEXT    NOT NULL,
    expiry       TEXT    NOT NULL,
    last_fetch   TEXT,
    last_error   TEXT,
    error_count  INTEGER NOT NULL DEFAULT 0,
    is_running   INTEGER NOT NULL DEFAULT 0,
    status_json  TEXT,
    updated_at   TEXT    NOT NULL,
    PRIMARY KEY (symbol, expiry)
);

CREATE TABLE IF NOT EXISTS error_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           TEXT    NOT NULL,
    symbol       TEXT,
    expiry       TEXT,
    error_type   TEXT    NOT NULL,
    message      TEXT    NOT NULL,
    traceback    TEXT,
    extra_json   TEXT
);
CREATE INDEX IF NOT EXISTS ix_error_log_ts ON error_log (ts DESC);

CREATE TABLE IF NOT EXISTS alert_events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           TEXT    NOT NULL,
    symbol       TEXT    NOT NULL,
    expiry       TEXT    NOT NULL,
    strike       REAL,
    side         TEXT,
    alert_type   TEXT    NOT NULL,
    detail       TEXT,
    magnitude    REAL,
    payload_json TEXT,
    dispatched_at TEXT
);
CREATE INDEX IF NOT EXISTS ix_alert_ts   ON alert_events (ts DESC);
CREATE INDEX IF NOT EXISTS ix_alert_sym  ON alert_events (symbol, ts DESC);

CREATE TABLE IF NOT EXISTS user_views (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT    NOT NULL UNIQUE,
    query_json  TEXT    NOT NULL,
    created_at  TEXT    NOT NULL,
    updated_at  TEXT    NOT NULL
);
"""


# ---------------------------------------------------------------------------
# MetaDB
# ---------------------------------------------------------------------------

class MetaDB:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @contextmanager
    def _conn(self) -> Generator[sqlite3.Connection, None, None]:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_schema(self) -> None:
        with self._conn() as conn:
            conn.executescript(_DDL)

    # ------------------------------------------------------------------
    # Collector status
    # ------------------------------------------------------------------

    def update_status(
        self,
        symbol: str,
        expiry: str,
        *,
        last_fetch: Optional[datetime] = None,
        last_error: Optional[str] = None,
        increment_error: bool = False,
        is_running: Optional[bool] = None,
        status: Optional[dict] = None,
    ) -> None:
        now = ts_str(now_ist())
        with self._conn() as conn:
            existing = conn.execute(
                "SELECT error_count FROM collector_status WHERE symbol=? AND expiry=?",
                [symbol, expiry],
            ).fetchone()
            error_count = (existing["error_count"] if existing else 0) + (1 if increment_error else 0)

            conn.execute(
                """
                INSERT INTO collector_status
                    (symbol, expiry, last_fetch, last_error, error_count, is_running, status_json, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (symbol, expiry) DO UPDATE SET
                    last_fetch   = COALESCE(excluded.last_fetch, last_fetch),
                    last_error   = COALESCE(excluded.last_error, last_error),
                    error_count  = excluded.error_count,
                    is_running   = COALESCE(excluded.is_running, is_running),
                    status_json  = COALESCE(excluded.status_json, status_json),
                    updated_at   = excluded.updated_at
                """,
                [
                    symbol,
                    expiry,
                    ts_str(last_fetch) if last_fetch else None,
                    last_error,
                    error_count,
                    int(is_running) if is_running is not None else None,
                    json.dumps(status) if status else None,
                    now,
                ],
            )

    def get_status(self) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM collector_status ORDER BY symbol, expiry"
            ).fetchall()
        return [dict(r) for r in rows]

    def reset_error_count(self, symbol: str, expiry: str) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE collector_status SET error_count=0 WHERE symbol=? AND expiry=?",
                [symbol, expiry],
            )

    # ------------------------------------------------------------------
    # Error log
    # ------------------------------------------------------------------

    def log_error(
        self,
        error_type: str,
        message: str,
        *,
        symbol: Optional[str] = None,
        expiry: Optional[str] = None,
        traceback: Optional[str] = None,
        extra: Optional[dict] = None,
    ) -> None:
        now = ts_str(now_ist())
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO error_log
                    (ts, symbol, expiry, error_type, message, traceback, extra_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [now, symbol, expiry, error_type, message, traceback,
                 json.dumps(extra) if extra else None],
            )
            # Rolling window: keep last 1000 errors
            conn.execute(
                "DELETE FROM error_log WHERE id NOT IN "
                "(SELECT id FROM error_log ORDER BY id DESC LIMIT 1000)"
            )

    def get_recent_errors(self, limit: int = 50) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM error_log ORDER BY id DESC LIMIT ?", [limit]
            ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Alert events
    # ------------------------------------------------------------------

    def save_alert(
        self,
        symbol: str,
        expiry: str,
        alert_type: str,
        *,
        ts: Optional[datetime] = None,
        strike: Optional[float] = None,
        side: Optional[str] = None,
        detail: Optional[str] = None,
        magnitude: Optional[float] = None,
        payload: Optional[dict] = None,
    ) -> int:
        now = ts_str(ts or now_ist())
        with self._conn() as conn:
            cur = conn.execute(
                """
                INSERT INTO alert_events
                    (ts, symbol, expiry, strike, side, alert_type, detail, magnitude, payload_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [now, symbol, expiry, strike, side, alert_type, detail, magnitude,
                 json.dumps(payload) if payload else None],
            )
            return cur.lastrowid  # type: ignore[return-value]

    def get_alerts(
        self,
        symbol: Optional[str] = None,
        limit: int = 100,
        since_id: int = 0,
    ) -> list[dict]:
        with self._conn() as conn:
            if symbol:
                rows = conn.execute(
                    "SELECT * FROM alert_events WHERE symbol=? AND id>? ORDER BY id DESC LIMIT ?",
                    [symbol, since_id, limit],
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM alert_events WHERE id>? ORDER BY id DESC LIMIT ?",
                    [since_id, limit],
                ).fetchall()
        return [dict(r) for r in rows]

    def mark_alert_dispatched(self, alert_id: int) -> None:
        now = ts_str(now_ist())
        with self._conn() as conn:
            conn.execute(
                "UPDATE alert_events SET dispatched_at=? WHERE id=?", [now, alert_id]
            )

    # ------------------------------------------------------------------
    # User views
    # ------------------------------------------------------------------

    def save_view(self, name: str, query: dict) -> int:
        now = ts_str(now_ist())
        with self._conn() as conn:
            cur = conn.execute(
                """
                INSERT INTO user_views (name, query_json, created_at, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (name) DO UPDATE SET
                    query_json = excluded.query_json,
                    updated_at = excluded.updated_at
                """,
                [name, json.dumps(query), now, now],
            )
            return cur.lastrowid  # type: ignore[return-value]

    def get_views(self) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute("SELECT * FROM user_views ORDER BY name").fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d["query"] = json.loads(d.pop("query_json"))
            result.append(d)
        return result

    def delete_view(self, name: str) -> bool:
        with self._conn() as conn:
            cur = conn.execute("DELETE FROM user_views WHERE name=?", [name])
            return cur.rowcount > 0
