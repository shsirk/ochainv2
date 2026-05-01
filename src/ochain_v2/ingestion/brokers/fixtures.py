"""
Deterministic fixture broker for testing and offline development.

``FixtureBroker`` returns synthetic option chain data derived from a seeded
random number generator.  The same seed always produces the same sequence,
making unit tests fully reproducible without network access.

Column names match the broker-native format expected by
``DuckDBStore._normalize_chain_df`` (i.e. ``_BROKER_TO_SCHEMA`` keys):
    strikePrice, CE_openInterest, CE_totalTradedVolume, CE_lastPrice,
    CE_impliedVolatility, CE_bidprice, CE_askPrice, CE_bidQty, CE_askQty,
    CE_delta, CE_gamma, CE_theta, CE_vega,
    PE_openInterest, PE_totalTradedVolume, PE_lastPrice,
    PE_impliedVolatility, PE_bidprice, PE_askPrice, PE_bidQty, PE_askQty,
    PE_delta, PE_gamma, PE_theta, PE_vega,
    underlyingValue
"""

from __future__ import annotations

import random
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd

from ochain_v2.core.timezones import now_ist
from ochain_v2.ingestion.brokers.base import BrokerProtocol, BrokerStatus


# ---------------------------------------------------------------------------
# Helper: next N Thursday dates from today
# ---------------------------------------------------------------------------

def _next_thursdays(n: int = 2, from_date: Optional[date] = None) -> list[str]:
    """Return the next *n* Thursday dates as ISO strings."""
    d = from_date or now_ist().date()
    thursdays: list[str] = []
    # advance to next Thursday
    days_ahead = (3 - d.weekday()) % 7
    if days_ahead == 0:
        days_ahead = 7
    candidate = d + timedelta(days=days_ahead)
    while len(thursdays) < n:
        thursdays.append(candidate.isoformat())
        candidate += timedelta(weeks=1)
    return thursdays


# ---------------------------------------------------------------------------
# FixtureBroker
# ---------------------------------------------------------------------------

_SPOT_BY_SYMBOL: dict[str, float] = {
    "NIFTY":      22500.0,
    "BANKNIFTY":  48000.0,
    "FINNIFTY":   20000.0,
    "MIDCPNIFTY": 10000.0,
}

_STRIKE_STEP: dict[str, float] = {
    "NIFTY":      50.0,
    "BANKNIFTY":  100.0,
    "FINNIFTY":   50.0,
    "MIDCPNIFTY": 25.0,
}


class FixtureBroker:
    """
    Synthetic broker that returns deterministic DataFrames.

    Parameters
    ----------
    seed : int
        RNG seed (default 42).  Two brokers with the same seed produce
        identical sequences of DataFrames for the same (symbol, expiry).
    num_strikes : int
        Number of strikes around ATM to include (default 20).
    base_iv : float
        Base implied volatility in percent (default 15.0).
    call_rate : float
        Rate at which OI drifts each successive call (default 0.0 — static).
    """

    name = "fixture"

    def __init__(
        self,
        seed: int = 42,
        num_strikes: int = 20,
        base_iv: float = 15.0,
        call_rate: float = 0.0,
        from_date: Optional[date] = None,
    ) -> None:
        self._seed = seed
        self._num_strikes = num_strikes
        self._base_iv = base_iv
        self._call_rate = call_rate
        self._from_date = from_date
        self._connected = False
        self._status = BrokerStatus(broker_name="fixture")
        self._call_counts: dict[str, int] = {}

    # ------------------------------------------------------------------
    # BrokerProtocol
    # ------------------------------------------------------------------

    @property
    def is_connected(self) -> bool:
        return self._connected

    @property
    def status(self) -> BrokerStatus:
        return self._status

    async def connect(self) -> None:
        self._connected = True
        self._status.is_connected = True

    async def disconnect(self) -> None:
        self._connected = False
        self._status.is_connected = False

    async def get_expiries(self, symbol: str) -> list[str]:
        return _next_thursdays(2, from_date=self._from_date)

    async def get_option_chain(self, symbol: str, expiry: str) -> pd.DataFrame:
        key = f"{symbol}:{expiry}"
        call_n = self._call_counts.get(key, 0)
        self._call_counts[key] = call_n + 1
        self._status.request_count += 1
        self._status.last_poll_at = now_ist()

        rng = random.Random(f"{self._seed}:{symbol}:{expiry}:{call_n}")

        spot = _SPOT_BY_SYMBOL.get(symbol, 22500.0)
        # Tiny random drift per call
        spot *= 1.0 + rng.uniform(-0.002, 0.002) * (1 + call_n * self._call_rate)

        step = _STRIKE_STEP.get(symbol, 50.0)
        atm = round(spot / step) * step
        half = self._num_strikes // 2
        strikes = [atm + step * (i - half) for i in range(self._num_strikes)]

        rows = []
        for k in strikes:
            moneyness = (k - spot) / spot  # +ve → OTM call
            # IV smile: ATM lowest, wings higher
            smile = self._base_iv + 2.0 * (moneyness ** 2) * 100.0
            ce_iv = max(1.0, smile + rng.uniform(-0.5, 0.5))
            pe_iv = max(1.0, smile + rng.uniform(-0.5, 0.5))

            oi_scale = max(100, int(50_000 * (1 - abs(moneyness) * 5)))
            ce_oi   = oi_scale + rng.randint(-500, 500)
            pe_oi   = oi_scale + rng.randint(-500, 500)
            ce_vol  = max(0, ce_oi // 10 + rng.randint(-50, 50))
            pe_vol  = max(0, pe_oi // 10 + rng.randint(-50, 50))
            ce_ltp  = max(0.05, max(0.0, spot - k) + rng.uniform(0.1, 2.0))
            pe_ltp  = max(0.05, max(0.0, k - spot) + rng.uniform(0.1, 2.0))

            rows.append({
                "strikePrice":           float(k),
                "CE_openInterest":       float(ce_oi),
                "CE_totalTradedVolume":  float(ce_vol),
                "CE_lastPrice":          round(ce_ltp, 2),
                "CE_impliedVolatility":  round(ce_iv, 4),
                "CE_bidprice":           round(ce_ltp - 0.05, 2),
                "CE_askPrice":           round(ce_ltp + 0.05, 2),
                "CE_bidQty":             float(rng.randint(10, 500)),
                "CE_askQty":             float(rng.randint(10, 500)),
                "CE_delta":              round(max(0.01, 0.5 - moneyness * 2), 4),
                "CE_gamma":              round(rng.uniform(0.0001, 0.005), 6),
                "CE_theta":              round(-rng.uniform(5.0, 50.0), 4),
                "CE_vega":               round(rng.uniform(10.0, 80.0), 4),
                "PE_openInterest":       float(pe_oi),
                "PE_totalTradedVolume":  float(pe_vol),
                "PE_lastPrice":          round(pe_ltp, 2),
                "PE_impliedVolatility":  round(pe_iv, 4),
                "PE_bidprice":           round(pe_ltp - 0.05, 2),
                "PE_askPrice":           round(pe_ltp + 0.05, 2),
                "PE_bidQty":             float(rng.randint(10, 500)),
                "PE_askQty":             float(rng.randint(10, 500)),
                "PE_delta":              round(min(-0.01, -0.5 + moneyness * 2), 4),
                "PE_gamma":              round(rng.uniform(0.0001, 0.005), 6),
                "PE_theta":              round(-rng.uniform(5.0, 50.0), 4),
                "PE_vega":               round(rng.uniform(10.0, 80.0), 4),
                "underlyingValue":       round(spot, 2),
            })

        return pd.DataFrame(rows)


assert isinstance(FixtureBroker(), BrokerProtocol)
