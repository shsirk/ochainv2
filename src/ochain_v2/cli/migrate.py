"""
CLI handler for `python -m ochain_v2 migrate`.

Called from ochain_v2.__main__._run_migrate().
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path


def run(args: argparse.Namespace) -> None:
    """Entry point called from __main__._run_migrate()."""
    src = Path(args.source)
    if not src.exists():
        print(f"ERROR: source database not found: {src}", file=sys.stderr)
        sys.exit(1)

    # Resolve target DuckDB path
    target = _resolve_target(args)
    print(f"[migrate] source : {src}")
    print(f"[migrate] target : {target}")
    if getattr(args, "dry_run", False):
        print("[migrate] DRY RUN — no data will be written")

    # Import here to keep startup fast for other commands
    from ochain_v2.db.duckdb_store import DuckDBStore
    from ochain_v2.db.legacy_sqlite import migrate_to_duckdb

    symbol = getattr(args, "symbol", None) or None

    with DuckDBStore(str(target)) as store:
        store.init_schema()
        _maybe_upsert_instruments(store)

        t0 = time.monotonic()
        result = migrate_to_duckdb(
            src,
            store,
            symbol_filter=symbol,
            dry_run=getattr(args, "dry_run", False),
        )

    print()
    print(result.summary())
    if result.errors:
        print(f"\n[migrate] {result.errors} error(s):")
        for msg in result.messages[:20]:    # cap at 20 lines
            print(f"  {msg}")
        if len(result.messages) > 20:
            print(f"  ... and {len(result.messages) - 20} more (see log)")
        sys.exit(1)


def _resolve_target(args: argparse.Namespace) -> Path:
    if getattr(args, "target", None):
        return Path(args.target)
    try:
        from ochain_v2.core.settings import get_settings
        return Path(get_settings().db.duckdb_path)
    except Exception:
        pass
    return Path("data") / "ochain_v2.duckdb"


def _maybe_upsert_instruments(store: "DuckDBStore") -> None:
    """
    Upsert instruments from instruments.yaml if it exists next to config/.
    Silently skips if the file is absent — migration still works (instruments
    row is not required for chain_rows insert).
    """
    instruments_yaml = Path("config") / "instruments.yaml"
    if instruments_yaml.exists():
        try:
            store.upsert_instruments_from_config(str(instruments_yaml))
        except Exception as exc:
            print(f"[migrate] WARNING: could not load instruments.yaml: {exc}")
