"""Unit tests for analyzers/primitives.py"""

import numpy as np
import pandas as pd
import pytest

from ochain_v2.analyzers.primitives import (
    compute_atm,
    compute_buildups,
    compute_delta,
    compute_max_pain,
    compute_pcr,
    compute_support_resistance,
)

SPOT = 22500.0
STRIKES = [22250.0, 22300.0, 22350.0, 22400.0, 22450.0,
           22500.0, 22550.0, 22600.0, 22650.0, 22700.0, 22750.0]


def _df(ce_oi=None, pe_oi=None, ce_ltp=50.0, pe_ltp=45.0,
        ce_iv=15.0, pe_iv=14.0, ce_vol=5000, pe_vol=4000) -> pd.DataFrame:
    n = len(STRIKES)
    ce_oi = ce_oi or [100_000 + i * 5_000 for i in range(n)]
    pe_oi = pe_oi or [90_000  + i * 3_000 for i in range(n)]
    return pd.DataFrame({
        "strike":    STRIKES,
        "ce_oi":     ce_oi,
        "pe_oi":     pe_oi,
        "ce_ltp":    [ce_ltp] * n,
        "pe_ltp":    [pe_ltp] * n,
        "ce_iv":     [ce_iv] * n,
        "pe_iv":     [pe_iv] * n,
        "ce_volume": [ce_vol] * n,
        "pe_volume": [pe_vol] * n,
    })


# ---------------------------------------------------------------------------
# compute_delta
# ---------------------------------------------------------------------------

class TestComputeDelta:
    def test_no_ref_all_nan(self) -> None:
        df = compute_delta(_df(), None)
        assert "ce_oi_chg" in df.columns
        assert df["ce_oi_chg"].isna().all()
        assert df["ref_available"].eq(False).all()

    def test_same_df_all_zero(self) -> None:
        base = _df()
        df = compute_delta(base, base)
        assert df["ce_oi_chg"].eq(0).all()
        assert df["pe_oi_chg"].eq(0).all()
        assert df["ref_available"].all()

    def test_known_diff(self) -> None:
        base = _df(ce_oi=[100_000] * len(STRIKES))
        curr = _df(ce_oi=[110_000] * len(STRIKES))
        df = compute_delta(curr, base)
        assert (df["ce_oi_chg"] == 10_000).all()

    def test_partial_strike_overlap(self) -> None:
        base = _df().iloc[:5]        # only first 5 strikes
        curr = _df()                 # all 11 strikes
        df = compute_delta(curr, base)
        # Strikes present in both → ref_available=True
        assert df.loc[df["strike"].isin(STRIKES[:5]), "ref_available"].all()
        # Strikes only in curr → ref_available=False
        assert not df.loc[df["strike"].isin(STRIKES[5:]), "ref_available"].any()


# ---------------------------------------------------------------------------
# compute_pcr
# ---------------------------------------------------------------------------

class TestComputePcr:
    def test_known_ratio(self) -> None:
        df = _df(ce_oi=[100_000]*len(STRIKES), pe_oi=[150_000]*len(STRIKES))
        result = compute_pcr(df)
        expected = (150_000 * len(STRIKES)) / (100_000 * len(STRIKES))
        assert result["pcr_oi"] == pytest.approx(expected, rel=1e-4)

    def test_regime_bearish(self) -> None:
        # Low PCR → more call OI → bearish sentiment
        df = _df(ce_oi=[200_000]*len(STRIKES), pe_oi=[80_000]*len(STRIKES))
        assert compute_pcr(df)["oi_regime"] == "bearish"

    def test_regime_bullish(self) -> None:
        # High PCR → more put OI → bullish sentiment (put writers support)
        df = _df(ce_oi=[80_000]*len(STRIKES), pe_oi=[200_000]*len(STRIKES))
        assert compute_pcr(df)["oi_regime"] == "bullish"

    def test_zero_ce_oi_returns_none(self) -> None:
        df = _df(ce_oi=[0]*len(STRIKES))
        result = compute_pcr(df)
        assert result["pcr_oi"] is None


# ---------------------------------------------------------------------------
# compute_atm
# ---------------------------------------------------------------------------

class TestComputeAtm:
    def test_exact_atm_strike(self) -> None:
        # 22500 is exactly in the strikes list
        result = compute_atm(_df(), SPOT)
        assert result["atm_strike"] == SPOT

    def test_nearest_strike(self) -> None:
        # spot = 22480 → nearest strike is 22500
        result = compute_atm(_df(), 22480.0)
        assert result["atm_strike"] == 22500.0

    def test_straddle_price(self) -> None:
        result = compute_atm(_df(ce_ltp=50.0, pe_ltp=45.0), SPOT)
        assert result["straddle_price"] == pytest.approx(95.0)

    def test_empty_df(self) -> None:
        result = compute_atm(pd.DataFrame(), SPOT)
        assert result["ce_ltp"] is None


# ---------------------------------------------------------------------------
# compute_max_pain
# ---------------------------------------------------------------------------

class TestComputeMaxPain:
    def test_trivial_case(self) -> None:
        # Single strike — max pain is the only strike
        df = pd.DataFrame({
            "strike": [22500.0],
            "ce_oi":  [100_000],
            "pe_oi":  [100_000],
        })
        result = compute_max_pain(df)
        assert result["max_pain_price"] == 22500.0

    def test_three_strikes_known(self) -> None:
        # Pain at T=100: PE from 110→10×50=500 + PE from 120→20×100=2000 = 2500
        # Pain at T=110: CE from 100→10×100=1000 + PE from 120→10×100=1000 = 2000 (min)
        # Pain at T=120: CE from 100→20×100=2000 + CE from 110→10×50=500 = 2500
        df = pd.DataFrame({
            "strike": [100.0, 110.0, 120.0],
            "ce_oi":  [100,   50,    0  ],
            "pe_oi":  [0,     50,    100],
        })
        result = compute_max_pain(df)
        assert result["max_pain_price"] == pytest.approx(110.0)

    def test_pain_curve_length(self) -> None:
        result = compute_max_pain(_df())
        assert len(result["pain_curve"]) == len(STRIKES)

    def test_pain_curve_sorted(self) -> None:
        result = compute_max_pain(_df())
        prices = [item["price"] for item in result["pain_curve"]]
        assert prices == sorted(prices)


# ---------------------------------------------------------------------------
# compute_buildups
# ---------------------------------------------------------------------------

class TestComputeBuildups:
    def _prev_curr(self, ce_ltp_delta=1.0, pe_ltp_delta=1.0,
                   ce_oi_delta=5_000, pe_oi_delta=5_000):
        n = len(STRIKES)
        prev = _df(ce_ltp=50.0, pe_ltp=45.0,
                   ce_oi=[100_000] * n, pe_oi=[90_000] * n)
        curr = _df(
            ce_ltp=50.0 + ce_ltp_delta,
            pe_ltp=45.0 + pe_ltp_delta,
            ce_oi=[100_000 + ce_oi_delta] * n,
            pe_oi=[90_000  + pe_oi_delta] * n,
        )
        return prev, curr

    def test_long_buildup(self) -> None:
        prev, curr = self._prev_curr(ce_ltp_delta=2.0, ce_oi_delta=5_000)
        result = compute_buildups(curr, prev)
        assert (result["ce_buildup"] == "Long Buildup").all()

    def test_short_buildup(self) -> None:
        prev, curr = self._prev_curr(ce_ltp_delta=-2.0, ce_oi_delta=5_000)
        result = compute_buildups(curr, prev)
        assert (result["ce_buildup"] == "Short Buildup").all()

    def test_long_unwinding(self) -> None:
        prev, curr = self._prev_curr(ce_ltp_delta=-2.0, ce_oi_delta=-5_000)
        result = compute_buildups(curr, prev)
        assert (result["ce_buildup"] == "Long Unwinding").all()

    def test_short_covering(self) -> None:
        prev, curr = self._prev_curr(ce_ltp_delta=2.0, ce_oi_delta=-5_000)
        result = compute_buildups(curr, prev)
        assert (result["ce_buildup"] == "Short Covering").all()

    def test_uses_existing_chg_columns(self) -> None:
        # If _chg columns already exist, prev_df is not needed
        df = _df()
        df["ce_ltp_chg"] = 1.0
        df["ce_oi_chg"]  = 1000.0
        df["pe_ltp_chg"] = -1.0
        df["pe_oi_chg"]  = 1000.0
        result = compute_buildups(df)    # no prev_df
        assert (result["ce_buildup"] == "Long Buildup").all()
        assert (result["pe_buildup"] == "Short Buildup").all()


# ---------------------------------------------------------------------------
# compute_support_resistance
# ---------------------------------------------------------------------------

class TestComputeSupportResistance:
    def test_returns_n_levels(self) -> None:
        result = compute_support_resistance(_df(), n=3)
        assert len(result["support"])    == 3
        assert len(result["resistance"]) == 3

    def test_support_has_pe_oi(self) -> None:
        result = compute_support_resistance(_df())
        assert all("pe_oi" in r for r in result["support"])

    def test_resistance_has_ce_oi(self) -> None:
        result = compute_support_resistance(_df())
        assert all("ce_oi" in r for r in result["resistance"])

    def test_highest_oi_strike_is_top_resistance(self) -> None:
        ce_oi = [0] * len(STRIKES)
        ce_oi[-1] = 999_999   # last strike has highest CE OI
        result = compute_support_resistance(_df(ce_oi=ce_oi), n=1)
        assert result["resistance"][0]["strike"] == STRIKES[-1]
