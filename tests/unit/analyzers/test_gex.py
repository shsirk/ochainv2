"""Unit tests for analyzers/gex.py"""

import numpy as np
import pandas as pd
import pytest

from ochain_v2.analyzers.gex import (
    _find_flip_point, _gex_regime, _iv_or_default, compute_gex,
)

SPOT = 22500.0
LOT  = 50
STRIKES = [22250.0, 22400.0, 22500.0, 22600.0, 22750.0]


def _df(ce_oi=None, pe_oi=None, ce_iv=15.0, pe_iv=14.0) -> pd.DataFrame:
    n = len(STRIKES)
    return pd.DataFrame({
        "strike":    STRIKES,
        "ce_oi":     ce_oi if ce_oi is not None else [100_000] * n,
        "pe_oi":     pe_oi if pe_oi is not None else [80_000]  * n,
        "ce_iv":     [ce_iv] * n,
        "pe_iv":     [pe_iv] * n,
    })


class TestComputeGex:
    def test_empty_df_returns_empty(self) -> None:
        result = compute_gex(pd.DataFrame(), SPOT, LOT)
        assert result["strikes"] == []
        assert result["total_gex"] == 0.0
        assert result["regime"] == "neutral"

    def test_invalid_spot_returns_empty(self) -> None:
        result = compute_gex(_df(), spot=0.0, lot_size=LOT)
        assert result["strikes"] == []

    def test_returns_all_keys(self) -> None:
        result = compute_gex(_df(), SPOT, LOT)
        expected = {"strikes", "ce_gex", "pe_gex", "net_gex",
                    "total_gex", "total_ce_gex", "total_pe_gex",
                    "flip_point", "regime", "dex"}
        assert expected.issubset(result.keys())

    def test_strike_count_matches(self) -> None:
        result = compute_gex(_df(), SPOT, LOT)
        assert len(result["strikes"]) == len(STRIKES)
        assert len(result["ce_gex"])  == len(STRIKES)
        assert len(result["pe_gex"])  == len(STRIKES)
        assert len(result["net_gex"]) == len(STRIKES)

    def test_ce_pe_gex_nonnegative(self) -> None:
        result = compute_gex(_df(), SPOT, LOT)
        assert all(v >= 0 for v in result["ce_gex"])
        assert all(v >= 0 for v in result["pe_gex"])

    def test_net_gex_is_ce_minus_pe(self) -> None:
        result = compute_gex(_df(), SPOT, LOT)
        for ce, pe, net in zip(result["ce_gex"], result["pe_gex"], result["net_gex"]):
            assert net == pytest.approx(ce - pe, rel=1e-4)

    def test_total_gex_sum(self) -> None:
        result = compute_gex(_df(), SPOT, LOT)
        assert result["total_gex"] == pytest.approx(sum(result["net_gex"]), rel=1e-4)

    def test_positive_regime_when_heavy_ce_oi(self) -> None:
        # Massive CE OI → high ce_gex → positive total_gex → positive regime
        result = compute_gex(_df(ce_oi=[1_000_000]*5, pe_oi=[1]*5), SPOT, LOT)
        assert result["regime"] == "positive"

    def test_negative_regime_when_heavy_pe_oi(self) -> None:
        result = compute_gex(_df(ce_oi=[1]*5, pe_oi=[1_000_000]*5), SPOT, LOT)
        assert result["regime"] == "negative"

    def test_uses_broker_gamma_when_available(self) -> None:
        df = _df()
        df["ce_gamma"] = 0.05
        df["pe_gamma"] = 0.03
        result = compute_gex(df, SPOT, LOT, dte=30.0)
        # ce_gex[i] = 0.05 × 100_000 × 50 × 22500² × 0.01
        expected_ce_gex = 0.05 * 100_000 * 50 * (SPOT ** 2) * 0.01
        assert result["ce_gex"][0] == pytest.approx(expected_ce_gex, rel=1e-4)

    def test_dex_nonzero(self) -> None:
        result = compute_gex(_df(), SPOT, LOT)
        # DEX may be large but must be a finite float
        assert isinstance(result["dex"], float)
        assert np.isfinite(result["dex"])


class TestGexRegime:
    def test_positive(self) -> None:
        net = np.array([100.0, 200.0])
        assert _gex_regime(200.0, net) == "positive"

    def test_negative(self) -> None:
        net = np.array([-100.0, -200.0])
        assert _gex_regime(-200.0, net) == "negative"

    def test_neutral_within_deadband(self) -> None:
        net = np.array([100.0, -100.0])
        # total_gex = 1.0 but max_abs = 100.0 → deadband = 5.0 → 1.0 < 5.0 → neutral
        assert _gex_regime(1.0, net) == "neutral"


class TestFindFlipPoint:
    def test_flip_between_two_strikes(self) -> None:
        strikes  = np.array([100.0, 110.0, 120.0])
        net_gex  = np.array([-50.0,  50.0,  50.0])
        flip = _find_flip_point(strikes, net_gex)
        assert flip is not None
        assert 100.0 < flip < 120.0

    def test_no_flip_all_positive(self) -> None:
        strikes = np.array([100.0, 110.0, 120.0])
        net_gex = np.array([10.0, 20.0, 30.0])
        assert _find_flip_point(strikes, net_gex) is None

    def test_flip_at_exact_zero_crossing(self) -> None:
        # cumsum = [50, 0, -50] → flip between index 1 and 2
        strikes = np.array([100.0, 110.0, 120.0])
        net_gex = np.array([50.0, -50.0, -50.0])
        flip = _find_flip_point(strikes, net_gex)
        assert flip is not None


class TestIvOrDefault:
    def test_percentage_iv(self) -> None:
        row = pd.Series({"ce_iv": 15.0})
        assert _iv_or_default(row, "ce_iv") == pytest.approx(0.15)

    def test_decimal_iv(self) -> None:
        row = pd.Series({"ce_iv": 0.15})
        assert _iv_or_default(row, "ce_iv") == pytest.approx(0.15)

    def test_missing_returns_default(self) -> None:
        row = pd.Series({"pe_iv": 14.0})
        assert _iv_or_default(row, "ce_iv") == pytest.approx(0.15)

    def test_none_returns_default(self) -> None:
        row = pd.Series({"ce_iv": None})
        assert _iv_or_default(row, "ce_iv") == pytest.approx(0.15)
