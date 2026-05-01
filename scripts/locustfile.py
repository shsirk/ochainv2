"""
OChain v2 load test — Locust.

Simulates 5 concurrent traders each cycling through the chain analysis and
heatmap tabs.  Target: p99 < 200 ms for all read endpoints.

Usage
-----
    # Quick headless run (60 s, 5 users, ramp 1 user/s):
    locust -f scripts/locustfile.py --headless \
           -u 5 -r 1 --run-time 60s \
           --host http://localhost:5051 \
           --html reports/locust_report.html

    # Interactive UI:
    locust -f scripts/locustfile.py --host http://localhost:5051

Requirements
------------
    pip install locust          # or pip install ochain-v2[dev]

The test auto-discovers (symbol, expiry, date) triples from the /api/symbols
and /api/snapshots endpoints on first request so no hardcoding is needed.
"""

from __future__ import annotations

import random
import threading
from typing import Optional

from locust import HttpUser, between, events, task


# ---------------------------------------------------------------------------
# Shared discovery state (populated once by first worker to avoid hammering)
# ---------------------------------------------------------------------------

_lock = threading.Lock()
_combos: list[dict] = []          # [{symbol, expiry, date, total}]


def _discover(client) -> list[dict]:
    global _combos
    with _lock:
        if _combos:
            return _combos
        combos: list[dict] = []
        try:
            symbols = client.get("/api/symbols", name="/api/symbols [setup]").json()
        except Exception:
            return combos
        for sym in symbols[:4]:
            try:
                dates = client.get(
                    f"/api/dates/{sym}", name="/api/dates [setup]"
                ).json()
            except Exception:
                continue
            for d in dates[:3]:
                try:
                    exp_resp = client.get(
                        f"/api/expiry_list/{sym}?date={d}",
                        name="/api/expiry_list [setup]",
                    )
                    expiries = exp_resp.json()
                except Exception:
                    continue
                for exp in expiries[:1]:
                    try:
                        snaps = client.get(
                            f"/api/snapshots/{sym}?date={d}&expiry={exp}",
                            name="/api/snapshots [setup]",
                        ).json()
                    except Exception:
                        continue
                    if len(snaps) >= 2:
                        combos.append(
                            {"symbol": sym, "expiry": exp, "date": d, "total": len(snaps)}
                        )
        _combos = combos
        return combos


# ---------------------------------------------------------------------------
# User behaviour
# ---------------------------------------------------------------------------

class TraderUser(HttpUser):
    """Simulates a trader tabbing between chain and heatmap views."""

    wait_time = between(0.5, 2.0)

    def on_start(self):
        self._combos = _discover(self.client)
        self._rng = random.Random()

    def _pick(self) -> Optional[dict]:
        if not self._combos:
            return None
        return self._rng.choice(self._combos)

    def _random_indices(self, total: int) -> tuple[int, int]:
        to_idx = self._rng.randint(max(1, total - 10), total - 1)
        from_idx = self._rng.randint(0, max(0, to_idx - 1))
        return from_idx, to_idx

    @task(5)
    def chain_analyze(self):
        combo = self._pick()
        if not combo:
            return
        fi, ti = self._random_indices(combo["total"])
        params = (
            f"?date={combo['date']}&expiry={combo['expiry']}"
            f"&from_idx={fi}&to_idx={ti}"
        )
        self.client.get(
            f"/api/analyze/{combo['symbol']}{params}",
            name="/api/analyze/[symbol]",
        )

    @task(3)
    def heatmap(self):
        combo = self._pick()
        if not combo:
            return
        fi, ti = self._random_indices(combo["total"])
        params = (
            f"?date={combo['date']}&expiry={combo['expiry']}"
            f"&from_idx={fi}&to_idx={ti}"
        )
        self.client.get(
            f"/api/heatmap/{combo['symbol']}{params}",
            name="/api/heatmap/[symbol]",
        )

    @task(2)
    def gex(self):
        combo = self._pick()
        if not combo:
            return
        fi, ti = self._random_indices(combo["total"])
        params = (
            f"?date={combo['date']}&expiry={combo['expiry']}"
            f"&from_idx={fi}&to_idx={ti}"
        )
        self.client.get(
            f"/api/gex/{combo['symbol']}{params}",
            name="/api/gex/[symbol]",
        )

    @task(1)
    def snapshot_list(self):
        combo = self._pick()
        if not combo:
            return
        self.client.get(
            f"/api/snapshots/{combo['symbol']}?date={combo['date']}&expiry={combo['expiry']}",
            name="/api/snapshots/[symbol]",
        )

    @task(1)
    def symbols(self):
        self.client.get("/api/symbols", name="/api/symbols")


# ---------------------------------------------------------------------------
# p99 assertion hook (headless mode)
# ---------------------------------------------------------------------------

@events.quitting.add_listener
def _check_p99(environment, **kwargs):
    """Fail the test run if any endpoint's p99 exceeds 200 ms."""
    stats = environment.runner.stats
    failed_endpoints = []
    for entry in stats.entries.values():
        p99 = entry.get_response_time_percentile(0.99)
        if p99 is not None and p99 > 200:
            failed_endpoints.append(f"  {entry.name}: p99={p99:.0f}ms")

    if failed_endpoints:
        print("\n[LOCUST] p99 > 200ms threshold breached:")
        for msg in failed_endpoints:
            print(msg)
        environment.process_exit_code = 1
    else:
        total_reqs = stats.total.num_requests
        if total_reqs > 0:
            p99_all = stats.total.get_response_time_percentile(0.99)
            print(f"\n[LOCUST] All endpoints within p99 < 200ms target. "
                  f"Overall p99={p99_all:.0f}ms over {total_reqs} requests.")
