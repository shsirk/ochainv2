#!/usr/bin/env python3
"""
OChain v1 vs v2 A/B API comparison tool.

Picks N random (symbol, expiry, date, snapshot_idx) samples from the v1
SQLite database, calls both v1 and v2 /api/analyze endpoints, diffs the
key numeric fields within declared tolerances, and reports pass/fail.

Usage
-----
    python scripts/compare_v1_v2.py \\
        --v1-db   ../OChain/ochain.db \\
        --v1-url  http://localhost:5050 \\
        --v2-url  http://localhost:5051 \\
        --samples 20 \\
        --symbols NIFTY BANKNIFTY

    # Quick offline check (no servers needed) — validates the v2 response
    # shape against the v1 DB data via the v2 process-internal path:
    python scripts/compare_v1_v2.py --offline --v1-db ../OChain/ochain.db

Exit codes
----------
    0 — all comparisons passed (or --offline ran without errors)
    1 — one or more comparisons failed
    2 — configuration / connectivity error
"""

from __future__ import annotations

import argparse
import json
import random
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Optional
from urllib.error import URLError
from urllib.request import urlopen, Request

# ---------------------------------------------------------------------------
# Tolerance declarations
# ---------------------------------------------------------------------------

# Key → (path_in_v1, path_in_v2, abs_tol, rel_tol, required)
# path = dot-delimited; list[0] means first element if list
COMPARISONS: list[tuple[str, str, str, float, float, bool]] = [
    # label,          v1_path,                  v2_path,                      abs,   rel,   required
    ("total_snaps",  "total_snapshots",         "total_snapshots",            0,     0,     True),
    ("pcr_oi",       "pcr.pcr_oi",              "summary.pcr.pcr_oi",         0.005, 0.01,  True),
    ("pcr_vol",      "pcr.pcr_vol",             "summary.pcr.pcr_vol",        0.005, 0.01,  True),
    ("atm_strike",   "atm_strike",              "summary.atm.atm_strike",     0,     0.001, True),
    ("max_pain",     "max_pain",                "summary.atm.max_pain",       0,     0.002, False),
    ("atm_iv",       "atm_iv",                  "summary.atm.atm_iv",         0.5,   0.05,  False),
    ("bias_dir",     "bias.direction",          "bias.direction",             0,     0,     False),
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get(obj: Any, path: str) -> Any:
    """Traverse dot-delimited path into nested dicts."""
    parts = path.split(".")
    cur = obj
    for p in parts:
        if cur is None:
            return None
        if isinstance(cur, dict):
            cur = cur.get(p)
        elif isinstance(cur, list):
            try:
                cur = cur[int(p)]
            except (IndexError, ValueError):
                return None
        else:
            return None
    return cur


def _close_enough(v1: Any, v2: Any, abs_tol: float, rel_tol: float) -> bool:
    if v1 is None and v2 is None:
        return True
    if v1 is None or v2 is None:
        return False
    if isinstance(v1, str) and isinstance(v2, str):
        return v1 == v2
    try:
        v1f, v2f = float(v1), float(v2)
        if abs_tol > 0 and abs(v1f - v2f) <= abs_tol:
            return True
        if rel_tol > 0 and v1f != 0:
            return abs(v1f - v2f) / abs(v1f) <= rel_tol
        return v1f == v2f
    except (TypeError, ValueError):
        return str(v1) == str(v2)


def _fetch(url: str, timeout: int = 10) -> dict:
    req = Request(url, headers={"Accept": "application/json"})
    with urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


# ---------------------------------------------------------------------------
# Sampling from v1 DB
# ---------------------------------------------------------------------------

def _sample_v1(db_path: str, symbols: list[str], n: int) -> list[dict]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    sym_clause = ""
    params: list = []
    if symbols:
        sym_clause = f"WHERE symbol IN ({','.join('?' * len(symbols))})"
        params = list(symbols)

    cursor.execute(
        f"""
        SELECT symbol, expiry, strftime('%Y-%m-%d', ts) AS trade_date, COUNT(*) AS cnt
        FROM snapshots
        {sym_clause}
        GROUP BY symbol, expiry, trade_date
        HAVING cnt >= 5
        ORDER BY trade_date DESC
        """,
        params,
    )
    groups = cursor.fetchall()
    conn.close()

    if not groups:
        print("ERROR: No qualifying snapshot groups found in v1 DB.")
        return []

    rng = random.Random(42)
    selected = rng.sample(groups, min(n, len(groups)))
    samples = []
    for row in selected:
        cnt = row["cnt"]
        to_idx = rng.randint(2, cnt - 1)
        from_idx = rng.randint(0, to_idx - 1)
        samples.append({
            "symbol":     row["symbol"],
            "expiry":     row["expiry"],
            "trade_date": row["trade_date"],
            "from_idx":   from_idx,
            "to_idx":     to_idx,
        })
    return samples


# ---------------------------------------------------------------------------
# Comparison runner
# ---------------------------------------------------------------------------

@dataclass
class CompareResult:
    symbol: str
    expiry: str
    date: str
    from_idx: int
    to_idx: int
    passed: bool = True
    failures: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    v1_status: int = 200
    v2_status: int = 200
    elapsed_v1: float = 0.0
    elapsed_v2: float = 0.0


def compare_one(sample: dict, v1_url: str, v2_url: str) -> CompareResult:
    sym  = sample["symbol"]
    exp  = sample["expiry"]
    date = sample["trade_date"]
    fi   = sample["from_idx"]
    ti   = sample["to_idx"]

    result = CompareResult(symbol=sym, expiry=exp, date=date, from_idx=fi, to_idx=ti)

    params = f"?date={date}&expiry={exp}&from_idx={fi}&to_idx={ti}"

    # Fetch v1
    t0 = time.monotonic()
    try:
        v1_data = _fetch(f"{v1_url}/api/analyze/{sym}{params}")
        result.elapsed_v1 = time.monotonic() - t0
    except Exception as exc:
        result.passed = False
        result.failures.append(f"v1 fetch failed: {exc}")
        return result

    # Fetch v2
    t0 = time.monotonic()
    try:
        v2_data = _fetch(f"{v2_url}/api/analyze/{sym}{params}")
        result.elapsed_v2 = time.monotonic() - t0
    except Exception as exc:
        result.passed = False
        result.failures.append(f"v2 fetch failed: {exc}")
        return result

    # Compare
    for label, v1_path, v2_path, abs_tol, rel_tol, required in COMPARISONS:
        v1_val = _get(v1_data, v1_path)
        v2_val = _get(v2_data, v2_path)
        ok = _close_enough(v1_val, v2_val, abs_tol, rel_tol)
        if not ok:
            msg = f"{label}: v1={v1_val!r} v2={v2_val!r} (tol abs={abs_tol} rel={rel_tol})"
            if required:
                result.passed = False
                result.failures.append(msg)
            else:
                result.warnings.append(msg)

    # Chain row count
    v1_strikes = v1_data.get("chain", [])
    v2_strikes = v2_data.get("strikes", [])
    if abs(len(v1_strikes) - len(v2_strikes)) > 2:
        result.warnings.append(
            f"strike_count: v1={len(v1_strikes)} v2={len(v2_strikes)}"
        )

    return result


# ---------------------------------------------------------------------------
# Offline self-check: validate v2 response shape internally
# ---------------------------------------------------------------------------

def run_offline(db_path: str, symbols: list[str], n: int) -> int:
    """
    Validate the v2 /api/analyze endpoint response shape without starting
    either server.  Imports the v2 app directly and exercises it via
    FastAPI's TestClient.
    """
    try:
        from fastapi.testclient import TestClient
        from ochain_v2.api.main import create_app
        from ochain_v2.api import deps
        from ochain_v2.db.duckdb_reader import DuckDBReader
        from ochain_v2.db.meta_sqlite import MetaDB
    except ImportError as exc:
        print(f"ERROR: Cannot import v2 app: {exc}")
        return 2

    # Point reader at migrated v2 DuckDB (if exists) or v1 DB
    import pathlib
    v2_db = pathlib.Path("data/ochain_v2.duckdb")
    if not v2_db.exists():
        # Try default location
        v2_db = pathlib.Path("data/ochain.duckdb")
    if not v2_db.exists():
        print(f"No v2 DuckDB found at {v2_db}.  Run: python -m ochain_v2 migrate --from {db_path}")
        return 2

    reader = DuckDBReader(str(v2_db))
    meta   = MetaDB(":memory:")
    deps.set_reader(reader)
    deps.set_meta(meta)

    from ochain_v2.api.ws.live   import set_publisher
    from ochain_v2.api.ws.alerts import set_meta as ws_set_meta
    from ochain_v2.ingestion.live_publisher import LivePublisher
    pub = LivePublisher()
    set_publisher(pub)
    ws_set_meta(meta)

    app = create_app()
    client = TestClient(app, raise_server_exceptions=False)

    available_symbols = reader.get_symbols()
    if not available_symbols:
        print("No symbols found in v2 DB.  Migrate v1 data first.")
        return 2

    use_symbols = symbols or available_symbols
    print(f"Offline check: symbols={use_symbols}, n={n}")

    passed = failed = 0
    for sym in use_symbols[:4]:
        dates = reader.get_trade_dates(sym)[:3]
        for date_str in dates:
            from datetime import date
            d = date.fromisoformat(date_str)
            expiries = reader.get_expiries(sym, d)
            for exp in expiries[:1]:
                snaps = reader.get_snapshot_list(sym, d, exp)
                if len(snaps) < 2:
                    continue
                ti = len(snaps) - 1
                fi = max(0, ti - 5)
                url = f"/api/analyze/{sym}?date={date_str}&expiry={exp}&from_idx={fi}&to_idx={ti}"
                resp = client.get(url)
                if resp.status_code != 200:
                    print(f"  FAIL [{sym}/{exp}/{date_str}] HTTP {resp.status_code}: {resp.text[:200]}")
                    failed += 1
                    continue
                data = resp.json()
                # Validate required top-level keys
                required_keys = ["symbol", "expiry", "trade_date", "strikes", "summary",
                                  "total_snapshots", "snapshot_ts"]
                missing = [k for k in required_keys if k not in data]
                if missing:
                    print(f"  FAIL [{sym}/{exp}/{date_str}] missing keys: {missing}")
                    failed += 1
                else:
                    print(f"  PASS [{sym}/{exp}/{date_str}] {len(data['strikes'])} strikes, "
                          f"pcr={data['summary'].get('pcr', {}).get('pcr_oi', 'N/A')}")
                    passed += 1

    reader.close()
    print(f"\nOffline check: {passed} passed, {failed} failed")
    return 0 if failed == 0 else 1


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compare v1 and v2 OChain API responses",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--v1-db",  default="../OChain/ochain.db",
                        help="Path to v1 SQLite DB (default: ../OChain/ochain.db)")
    parser.add_argument("--v1-url", default="http://localhost:5050",
                        help="v1 API base URL (default: http://localhost:5050)")
    parser.add_argument("--v2-url", default="http://localhost:5051",
                        help="v2 API base URL (default: http://localhost:5051)")
    parser.add_argument("--samples", type=int, default=10,
                        help="Number of random samples to compare (default: 10)")
    parser.add_argument("--symbols", nargs="+", default=[],
                        help="Filter to these symbols (default: all)")
    parser.add_argument("--seed", type=int, default=42,
                        help="RNG seed for reproducible sampling (default: 42)")
    parser.add_argument("--timeout", type=int, default=10,
                        help="HTTP request timeout in seconds (default: 10)")
    parser.add_argument("--offline", action="store_true",
                        help="Offline mode: validate v2 shape without running servers")
    parser.add_argument("--json", action="store_true",
                        help="Output results as JSON to stdout")
    args = parser.parse_args()

    if args.offline:
        return run_offline(args.v1_db, args.symbols, args.samples)

    # Live A/B mode — check connectivity
    for name, url in [("v1", args.v1_url), ("v2", args.v2_url)]:
        try:
            _fetch(f"{url}/healthz" if name == "v2" else f"{url}/api/symbols",
                   timeout=5)
        except Exception as exc:
            print(f"ERROR: Cannot reach {name} API at {url}: {exc}")
            print("  Start v1: python -m OChain.app --port 5050")
            print("  Start v2: .\\scripts\\run_api.ps1")
            return 2

    print(f"Sampling {args.samples} cases from {args.v1_db} ...")
    samples = _sample_v1(args.v1_db, args.symbols, args.samples)
    if not samples:
        return 2

    results: list[CompareResult] = []
    total_pass = total_fail = 0

    for i, sample in enumerate(samples, 1):
        r = compare_one(sample, args.v1_url, args.v2_url)
        results.append(r)
        status = "PASS" if r.passed else "FAIL"
        if r.passed:
            total_pass += 1
        else:
            total_fail += 1

        sym_tag = f"{r.symbol}/{r.expiry}/{r.date}[{r.from_idx}:{r.to_idx}]"
        latency = f"v1={r.elapsed_v1*1000:.0f}ms v2={r.elapsed_v2*1000:.0f}ms"
        print(f"[{i:3d}/{len(samples)}] {status} {sym_tag} {latency}")
        for msg in r.failures:
            print(f"        FAIL: {msg}")
        for msg in r.warnings:
            print(f"        WARN: {msg}")

    print(f"\n{'='*60}")
    print(f"Results: {total_pass} passed, {total_fail} failed out of {len(samples)}")
    v1_p50 = sorted(r.elapsed_v1 for r in results)[len(results)//2] * 1000
    v2_p50 = sorted(r.elapsed_v2 for r in results)[len(results)//2] * 1000
    print(f"Latency p50: v1={v1_p50:.0f}ms  v2={v2_p50:.0f}ms")

    if args.json:
        import dataclasses
        print(json.dumps([dataclasses.asdict(r) for r in results], indent=2))

    return 0 if total_fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
