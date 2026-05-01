"""
Collector entry point.

    python -m ochain_v2.ingestion [--config config/settings.yaml] [--broker dhan|fixture]

Loads settings, wires up the broker, DuckDBStore, LivePublisher, and
Collector, then runs the event loop until SIGTERM/SIGINT.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

log = logging.getLogger(__name__)


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m ochain_v2.ingestion",
        description="OChain v2 — option chain collector",
    )
    p.add_argument(
        "--config",
        default="config/settings.yaml",
        help="Path to settings YAML (default: config/settings.yaml)",
    )
    p.add_argument(
        "--broker",
        choices=["dhan", "fixture"],
        default=None,
        help="Override broker name from settings",
    )
    p.add_argument(
        "--symbols",
        nargs="+",
        default=None,
        metavar="SYMBOL",
        help="Override active symbols from settings (e.g. NIFTY BANKNIFTY)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate config and connect to broker, then exit without polling",
    )
    return p


async def _run(args: argparse.Namespace) -> None:
    from ochain_v2.core.settings import get_settings
    from ochain_v2.db.duckdb_store import DuckDBStore
    from ochain_v2.ingestion.live_publisher import LivePublisher
    from ochain_v2.ingestion.scheduler import Collector

    cfg = get_settings(args.config)

    # Initialise database
    store = DuckDBStore(cfg.db.duckdb_path)
    store.init_schema()

    instruments_file = Path(cfg.collector.instruments_file)
    if instruments_file.exists():
        store.upsert_instruments_from_config(str(instruments_file))

    # Broker selection
    broker_name = args.broker or cfg.broker.name
    broker = _make_broker(broker_name, cfg)

    # Symbols
    symbols = args.symbols or cfg.instruments.active

    publisher = LivePublisher()

    collector = Collector(
        broker=broker,
        store=store,
        publisher=publisher,
        symbols=symbols,
        expiries_per_symbol=cfg.instruments.expiries_per_symbol,
        poll_interval=float(cfg.collector.interval_sec),
        rate=cfg.broker.rate_limit_per_sec,
        circuit_threshold=cfg.broker.circuit_breaker_threshold,
        circuit_timeout=float(cfg.broker.circuit_breaker_pause_sec),
        source=broker_name,
    )

    if args.dry_run:
        log.info("Dry-run: connecting to broker '%s'", broker_name)
        await broker.connect()
        log.info("Broker connected — dry-run complete, exiting")
        await broker.disconnect()
        store.close()
        return

    try:
        await collector.start()
    finally:
        await broker.disconnect()
        store.close()
        log.info("Collector process exited cleanly")


def _make_broker(name: str, cfg):
    if name == "fixture":
        from ochain_v2.ingestion.brokers.fixtures import FixtureBroker
        return FixtureBroker()

    if name == "dhan":
        from ochain_v2.ingestion.brokers.dhan import DhanBroker
        return DhanBroker(creds_path=cfg.broker.credentials_path)

    if name == "kite":
        from ochain_v2.ingestion.brokers.kite import KiteBroker
        return KiteBroker()

    raise ValueError(f"Unknown broker name: {name!r}")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = _build_arg_parser()
    args = parser.parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
