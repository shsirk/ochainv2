"""
Migration runner for OChain v2 DuckDB schema.

Applies numbered .sql files in order (0001_initial.sql, 0002_xyz.sql, …).
Each migration is recorded in the _migrations table so it runs exactly once.
The initial schema (0001_initial.sql) is idempotent, so running it on a
pre-existing database is safe.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

import duckdb

log = logging.getLogger(__name__)

_MIGRATIONS_DIR = Path(__file__).parent


def run_migrations(conn: duckdb.DuckDBPyConnection) -> list[str]:
    """
    Apply any unapplied migrations to *conn*.

    Returns a list of migration names that were applied in this call.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS _migrations (
            name       VARCHAR   PRIMARY KEY,
            applied_at TIMESTAMP NOT NULL
        )
        """
    )

    applied: set[str] = {
        r[0] for r in conn.execute("SELECT name FROM _migrations").fetchall()
    }

    migration_files = sorted(
        f for f in _MIGRATIONS_DIR.glob("*.sql") if f.stem[0].isdigit()
    )

    newly_applied: list[str] = []
    for f in migration_files:
        if f.name in applied:
            log.debug("migration already applied: %s", f.name)
            continue

        log.info("applying migration: %s", f.name)
        sql = f.read_text(encoding="utf-8")
        conn.execute("BEGIN TRANSACTION")
        try:
            conn.execute(sql)
            conn.execute(
                "INSERT INTO _migrations (name, applied_at) VALUES (?, ?)",
                [f.name, datetime.now(tz=timezone.utc)],
            )
            conn.execute("COMMIT")
            newly_applied.append(f.name)
        except Exception:
            conn.execute("ROLLBACK")
            raise

    if newly_applied:
        log.info("migrations applied: %s", newly_applied)
    else:
        log.debug("no new migrations to apply")

    return newly_applied
