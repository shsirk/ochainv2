"""
OChain v2 — command-line entry point.

Usage:
    python -m ochain_v2 api        Start the FastAPI server
    python -m ochain_v2 collector  Start the option chain collector
    python -m ochain_v2 migrate    Import data from a v1 ochain.db
    python -m ochain_v2 archive    Archive snapshots older than N days
    python -m ochain_v2 seed       Load demo data from bundled CSV
    python -m ochain_v2 doctor     Check DB integrity and collector health
"""

import argparse
import sys


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="ochain_v2",
        description="OChain v2 — Indian market option chain collector and analyzer",
    )
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")

    # api
    p_api = sub.add_parser("api", help="Start the FastAPI server")
    p_api.add_argument("--port", type=int, default=5051)
    p_api.add_argument("--host", default="0.0.0.0")
    p_api.add_argument("--workers", type=int, default=1)
    p_api.add_argument("--reload", action="store_true", help="Enable auto-reload (dev only)")

    # collector
    p_col = sub.add_parser("collector", help="Start the option chain collector")
    p_col.add_argument("--config", default="config/settings.yaml")

    # migrate
    p_mig = sub.add_parser("migrate", help="Import data from a v1 ochain.db")
    p_mig.add_argument("--from", dest="source", required=True, metavar="PATH",
                       help="Path to v1 ochain.db SQLite file")
    p_mig.add_argument("--to", dest="target", default=None,
                       help="Target DuckDB path (defaults to settings value)")
    p_mig.add_argument("--symbol", default=None,
                       help="Migrate only this symbol (e.g. NIFTY)")
    p_mig.add_argument("--dry-run", action="store_true",
                       help="Parse and count rows without writing to DuckDB")

    # archive
    p_arc = sub.add_parser("archive", help="Archive snapshots older than N days to Parquet")
    p_arc.add_argument("--before", metavar="YYYY-MM-DD",
                       help="Archive snapshots before this date")
    p_arc.add_argument("--days", type=int, default=90,
                       help="Archive snapshots older than this many days (default: 90)")

    # seed
    p_seed = sub.add_parser("seed", help="Load demo data from the bundled option_chain.csv")
    p_seed.add_argument("--csv", default=None, help="Path to CSV (defaults to bundled file)")

    # doctor
    sub.add_parser("doctor", help="Check DB integrity and collector health")

    args = parser.parse_args()

    if args.command == "api":
        _run_api(args)
    elif args.command == "collector":
        _run_collector(args)
    elif args.command == "migrate":
        _run_migrate(args)
    elif args.command == "archive":
        _run_archive(args)
    elif args.command == "seed":
        _run_seed(args)
    elif args.command == "doctor":
        _run_doctor(args)
    else:
        parser.print_help()
        sys.exit(0)


def _run_api(args: argparse.Namespace) -> None:
    try:
        import uvicorn
    except ImportError:
        print("ERROR: uvicorn not installed. Run: pip install ochain-v2")
        sys.exit(1)
    uvicorn.run(
        "ochain_v2.api.main:app",
        host=args.host,
        port=args.port,
        workers=args.workers,
        reload=args.reload,
    )


def _run_collector(args: argparse.Namespace) -> None:
    print(f"[ochain_v2] Starting collector with config: {args.config}")
    print("  (Not implemented yet — Phase 2)")


def _run_migrate(args: argparse.Namespace) -> None:
    from ochain_v2.cli.migrate import run
    run(args)


def _run_archive(args: argparse.Namespace) -> None:
    print("[ochain_v2] Running archival job")
    print("  (Not implemented yet — Phase 6b)")


def _run_seed(args: argparse.Namespace) -> None:
    print("[ochain_v2] Seeding demo data")
    print("  (Not implemented yet — Phase 6c)")


def _run_doctor(args: argparse.Namespace) -> None:
    print("[ochain_v2] Running health checks")
    print("  (Not implemented yet — Phase 6c)")


if __name__ == "__main__":
    main()
