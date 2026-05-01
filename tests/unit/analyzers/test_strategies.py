"""
Unit tests for the strategy layer (P1-24 through P1-32).

Covers:
  - AnalysisContext construction (build() + manual)
  - Registry (register, get, list, duplicate prevention)
  - Signal contract (required keys, strength bounds)
  - Per-strategy signal firing under known inputs
  - metrics() completeness
"""

from __future__ import annotations

import pandas as pd
import pytest

import ochain_v2.analyzers.strategies  # triggers all registrations

from ochain_v2.analyzers.strategies.base import (
    ABOVE_FLIP_POINT, BEARISH_OI_BUILDUP, BELOW_FLIP_POINT,
    BULLISH_OI_BUILDUP, CHEAP_STRADDLE, EXPENSIVE_STRADDLE,
    HIGH_IV_SELL_PREMIUM, NEAR_FLIP_POINT, NEGATIVE_GEX_TRENDING,
    PCR_HIGH_BUY_CE, PCR_LOW_BUY_PE, POSITIVE_GEX_DAMPENING,
    AnalysisContext, TradingStrategy,
)
from ochain_v2.analyzers.strategies.registry import (
    clear_registry, get_strategy, list_names, list_strategies, register,
)
from ochain_v2.analyzers.strategies.naked_buyer  import NakedBuyerStrategy
from ochain_v2.analyzers.strategies.naked_seller import NakedSellerStrategy
from ochain_v2.analyzers.strategies.straddle     import StraddleStrategy
from ochain_v2.analyzers.strategies.strangle     import StrangleStrategy
from ochain_v2.analyzers.strategies.spread       import SpreadStrategy
from ochain_v2.analyzers.strategies.iron_condor  import IronCondorStrategy
from ochain_v2.analyzers.strategies.gamma_scalper import GammaScalperStrategy

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

SPOT    = 22500.0
LOT     = 50
STRIKES = [22250.0, 22350.0, 22450.0, 22500.0, 22550.0, 22650.0, 22750.0]
_REQUIRED_SIGNAL_KEYS = {"signal_type", "strength", "side", "strike", "reason", "metadata"}


def _df(ce_oi=100_000, pe_oi=80_000, ce_ltp=50.0, pe_ltp=45.0,
        ce_iv=15.0, pe_iv=14.0, ce_vol=5_000, pe_vol=4_000) -> pd.DataFrame:
    n = len(STRIKES)
    return pd.DataFrame({
        "strike":    STRIKES,
        "ce_oi":     [ce_oi]  * n,
        "pe_oi":     [pe_oi]  * n,
        "ce_ltp":    [ce_ltp] * n,
        "pe_ltp":    [pe_ltp] * n,
        "ce_iv":     [ce_iv]  * n,
        "pe_iv":     [pe_iv]  * n,
        "ce_volume": [ce_vol] * n,
        "pe_volume": [pe_vol] * n,
    })


def _ctx(**overrides) -> AnalysisContext:
    """Build a minimal context, allowing field overrides."""
    base = dict(current_df=_df(), spot=SPOT, lot_size=LOT, dte=30.0)
    base.update(overrides)
    return AnalysisContext.build(**base)


def _assert_signal_contract(signals: list[dict]) -> None:
    for sig in signals:
        assert _REQUIRED_SIGNAL_KEYS.issubset(sig.keys()), f"Missing keys in {sig}"
        assert 0.0 <= sig["strength"] <= 1.0, f"Strength out of range: {sig['strength']}"
        assert sig["side"] in ("CE", "PE", "neutral"), f"Invalid side: {sig['side']}"
        assert isinstance(sig["reason"], str) and sig["reason"]
        assert isinstance(sig["metadata"], dict)


# ---------------------------------------------------------------------------
# AnalysisContext
# ---------------------------------------------------------------------------

class TestAnalysisContext:
    def test_build_populates_primitives(self) -> None:
        ctx = _ctx()
        assert ctx.pcr.get("pcr_oi") is not None
        assert ctx.atm.get("atm_strike") is not None
        assert ctx.gex.get("regime") in ("positive", "negative", "neutral")
        assert ctx.expected_move.get("expected_move") is not None

    def test_build_empty_df_does_not_crash(self) -> None:
        ctx = AnalysisContext.build(pd.DataFrame(), SPOT)
        assert ctx.pcr == {}
        assert ctx.atm == {}

    def test_build_with_prev_df_populates_buildups(self) -> None:
        prev = _df(ce_oi=90_000)
        ctx  = AnalysisContext.build(_df(), SPOT, prev_df=prev, lot_size=LOT, dte=30.0)
        assert ctx.buildups is not None
        assert "ce_buildup" in ctx.buildups.columns

    def test_build_without_prev_df_buildups_is_none(self) -> None:
        ctx = _ctx()
        assert ctx.buildups is None

    def test_manual_construction(self) -> None:
        ctx = AnalysisContext(current_df=_df(), spot=SPOT)
        assert ctx.pcr == {}
        assert ctx.buildups is None


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

class TestRegistry:
    def test_all_builtins_registered(self) -> None:
        names = list_names()
        for n in ["naked_buyer", "naked_seller", "straddle", "strangle",
                  "spread", "iron_condor", "gamma_scalper"]:
            assert n in names

    def test_get_strategy_returns_correct_instance(self) -> None:
        s = get_strategy("naked_buyer")
        assert s.name == "naked_buyer"

    def test_get_strategy_unknown_raises(self) -> None:
        with pytest.raises(KeyError):
            get_strategy("nonexistent_strategy")

    def test_list_strategies_non_empty(self) -> None:
        assert len(list_strategies()) == 7

    def test_duplicate_registration_raises(self) -> None:
        clear_registry()
        dummy = NakedBuyerStrategy()
        register(dummy)
        with pytest.raises(ValueError, match="already registered"):
            register(NakedBuyerStrategy())

    def test_protocol_conformance(self) -> None:
        for s in list_strategies():
            assert isinstance(s, TradingStrategy)

    def setup_method(self) -> None:
        # Re-register builtins after potential clear in tests
        pass

    def teardown_method(self) -> None:
        # Restore registry after duplicate test clears it
        clear_registry()
        import ochain_v2.analyzers.strategies as _pkg
        for s in _pkg._BUILTIN_INSTANCES:
            try:
                register(s)
            except ValueError:
                pass


# ---------------------------------------------------------------------------
# Signal contract (all strategies)
# ---------------------------------------------------------------------------

class TestSignalContract:
    @pytest.mark.parametrize("strategy_name", [
        "naked_buyer", "naked_seller", "straddle", "strangle",
        "spread", "iron_condor", "gamma_scalper",
    ])
    def test_signals_return_list(self, strategy_name: str) -> None:
        s = get_strategy(strategy_name)
        assert isinstance(s.signals(_ctx()), list)

    @pytest.mark.parametrize("strategy_name", [
        "naked_buyer", "naked_seller", "straddle", "strangle",
        "spread", "iron_condor", "gamma_scalper",
    ])
    def test_signal_keys_valid(self, strategy_name: str) -> None:
        s = get_strategy(strategy_name)
        ctx = _ctx()
        _assert_signal_contract(s.signals(ctx))

    @pytest.mark.parametrize("strategy_name", [
        "naked_buyer", "naked_seller", "straddle", "strangle",
        "spread", "iron_condor", "gamma_scalper",
    ])
    def test_metrics_return_dict(self, strategy_name: str) -> None:
        s = get_strategy(strategy_name)
        m = s.metrics(_ctx())
        assert isinstance(m, dict)

    @pytest.mark.parametrize("strategy_name", [
        "naked_buyer", "naked_seller", "straddle", "strangle",
        "spread", "iron_condor", "gamma_scalper",
    ])
    def test_empty_context_no_crash(self, strategy_name: str) -> None:
        s   = get_strategy(strategy_name)
        ctx = AnalysisContext(current_df=pd.DataFrame(), spot=0.0)
        sigs = s.signals(ctx)
        assert isinstance(sigs, list)
        _assert_signal_contract(sigs)


# ---------------------------------------------------------------------------
# NakedBuyer
# ---------------------------------------------------------------------------

class TestNakedBuyer:
    def _strategy(self) -> NakedBuyerStrategy:
        return get_strategy("naked_buyer")  # type: ignore[return-value]

    def test_bullish_oi_buildup_fires(self) -> None:
        prev = _df(ce_oi=80_000)
        curr = _df(ce_ltp=52.0, ce_oi=100_000)   # OI up, price up → Long Buildup
        ctx = AnalysisContext.build(curr, SPOT, prev_df=prev, lot_size=LOT, dte=30.0)
        types = [s["signal_type"] for s in self._strategy().signals(ctx)]
        assert BULLISH_OI_BUILDUP in types

    def test_bearish_oi_buildup_fires(self) -> None:
        prev = _df(pe_oi=60_000)
        curr = _df(pe_ltp=47.0, pe_oi=80_000)    # OI up, price up → Long Buildup on PE
        ctx = AnalysisContext.build(curr, SPOT, prev_df=prev, lot_size=LOT, dte=30.0)
        types = [s["signal_type"] for s in self._strategy().signals(ctx)]
        assert BEARISH_OI_BUILDUP in types

    def test_pcr_high_fires_ce_signal(self) -> None:
        ctx = AnalysisContext(
            current_df=_df(), spot=SPOT,
            pcr={"pcr_oi": 1.8, "oi_regime": "bullish"},
            atm={"atm_strike": SPOT, "avg_iv": 15.0},
        )
        types = [s["signal_type"] for s in self._strategy().signals(ctx)]
        assert PCR_HIGH_BUY_CE in types

    def test_pcr_low_fires_pe_signal(self) -> None:
        ctx = AnalysisContext(
            current_df=_df(), spot=SPOT,
            pcr={"pcr_oi": 0.4, "oi_regime": "bearish"},
            atm={"atm_strike": SPOT, "avg_iv": 15.0},
        )
        types = [s["signal_type"] for s in self._strategy().signals(ctx)]
        assert PCR_LOW_BUY_PE in types

    def test_negative_gex_fires(self) -> None:
        ctx = AnalysisContext(
            current_df=_df(), spot=SPOT,
            gex={"regime": "negative", "total_gex": -1e9},
            atm={"atm_strike": SPOT, "avg_iv": 15.0},
        )
        types = [s["signal_type"] for s in self._strategy().signals(ctx)]
        assert NEGATIVE_GEX_TRENDING in types

    def test_no_signal_for_neutral_conditions(self) -> None:
        ctx = AnalysisContext(
            current_df=_df(), spot=SPOT,
            pcr={"pcr_oi": 1.0, "oi_regime": "neutral"},
            atm={"atm_strike": SPOT, "avg_iv": 15.0},
            gex={"regime": "neutral", "total_gex": 0.0},
        )
        sigs = self._strategy().signals(ctx)
        assert BULLISH_OI_BUILDUP not in [s["signal_type"] for s in sigs]
        assert BEARISH_OI_BUILDUP not in [s["signal_type"] for s in sigs]

    def test_metrics_has_pcr_and_gex(self) -> None:
        m = self._strategy().metrics(_ctx())
        assert "pcr_oi" in m
        assert "gex_regime" in m
        assert "expected_move" in m


# ---------------------------------------------------------------------------
# NakedSeller
# ---------------------------------------------------------------------------

class TestNakedSeller:
    def _strategy(self) -> NakedSellerStrategy:
        return get_strategy("naked_seller")  # type: ignore[return-value]

    def test_high_iv_fires(self) -> None:
        ctx = AnalysisContext(
            current_df=_df(), spot=SPOT,
            atm={"atm_strike": SPOT, "avg_iv": 28.0, "straddle_price": 200.0},
        )
        types = [s["signal_type"] for s in self._strategy().signals(ctx)]
        assert HIGH_IV_SELL_PREMIUM in types

    def test_positive_gex_fires(self) -> None:
        ctx = AnalysisContext(
            current_df=_df(), spot=SPOT,
            gex={"regime": "positive", "total_gex": 1e9},
            atm={"atm_strike": SPOT, "avg_iv": 15.0},
        )
        types = [s["signal_type"] for s in self._strategy().signals(ctx)]
        assert POSITIVE_GEX_DAMPENING in types

    def test_metrics_has_max_pain(self) -> None:
        m = self._strategy().metrics(_ctx())
        assert "max_pain" in m


# ---------------------------------------------------------------------------
# Straddle
# ---------------------------------------------------------------------------

class TestStraddle:
    def _strategy(self) -> StraddleStrategy:
        return get_strategy("straddle")  # type: ignore[return-value]

    def test_cheap_straddle_fires(self) -> None:
        ctx = AnalysisContext(
            current_df=_df(), spot=SPOT,
            atm={"atm_strike": SPOT, "avg_iv": 15.0, "straddle_price": 300.0},
            expected_move={"expected_move": 600.0, "method": "iv_dte"},
        )
        types = [s["signal_type"] for s in self._strategy().signals(ctx)]
        assert CHEAP_STRADDLE in types

    def test_expensive_straddle_fires(self) -> None:
        ctx = AnalysisContext(
            current_df=_df(), spot=SPOT,
            atm={"atm_strike": SPOT, "avg_iv": 25.0, "straddle_price": 900.0},
            expected_move={"expected_move": 600.0, "method": "iv_dte"},
        )
        types = [s["signal_type"] for s in self._strategy().signals(ctx)]
        assert EXPENSIVE_STRADDLE in types

    def test_metrics_has_straddle_em_ratio(self) -> None:
        ctx = AnalysisContext(
            current_df=_df(), spot=SPOT,
            atm={"atm_strike": SPOT, "avg_iv": 15.0, "straddle_price": 400.0},
            expected_move={"expected_move": 500.0},
        )
        m = self._strategy().metrics(ctx)
        assert "straddle_em_ratio" in m
        assert m["straddle_em_ratio"] == pytest.approx(0.8, rel=1e-3)


# ---------------------------------------------------------------------------
# Strangle
# ---------------------------------------------------------------------------

class TestStrangle:
    def _strategy(self) -> StrangleStrategy:
        return get_strategy("strangle")  # type: ignore[return-value]

    def test_high_iv_fires(self) -> None:
        ctx = AnalysisContext(
            current_df=_df(), spot=SPOT,
            atm={"atm_strike": SPOT, "avg_iv": 22.0},
        )
        types = [s["signal_type"] for s in self._strategy().signals(ctx)]
        assert HIGH_IV_SELL_PREMIUM in types

    def test_metrics_has_wing_suggestions(self) -> None:
        m = self._strategy().metrics(_ctx())
        assert "suggested_call_wing" in m
        assert "suggested_put_wing" in m


# ---------------------------------------------------------------------------
# Spread
# ---------------------------------------------------------------------------

class TestSpread:
    def _strategy(self) -> SpreadStrategy:
        return get_strategy("spread")  # type: ignore[return-value]

    def test_no_signals_without_buildups(self) -> None:
        ctx = AnalysisContext(current_df=_df(), spot=SPOT)
        assert self._strategy().signals(ctx) == []

    def test_bull_spread_signal_fires_with_ce_buildup(self) -> None:
        prev = _df(ce_oi=80_000)
        curr = _df(ce_ltp=52.0, ce_oi=100_000)
        ctx  = AnalysisContext.build(curr, SPOT, prev_df=prev, lot_size=LOT, dte=30.0)
        types = [s["signal_type"] for s in self._strategy().signals(ctx)]
        assert "BULL_SPREAD_SIGNAL" in types

    def test_spread_signal_contains_sell_strike(self) -> None:
        prev = _df(ce_oi=80_000)
        curr = _df(ce_ltp=52.0, ce_oi=100_000)
        ctx  = AnalysisContext.build(curr, SPOT, prev_df=prev, lot_size=LOT, dte=30.0)
        bull = [s for s in self._strategy().signals(ctx)
                if s["signal_type"] == "BULL_SPREAD_SIGNAL"]
        assert bull
        assert bull[0]["metadata"].get("sell_strike") is not None

    def test_metrics_has_spread_mode(self) -> None:
        m = self._strategy().metrics(_ctx())
        assert m.get("spread_mode") in ("debit", "credit")


# ---------------------------------------------------------------------------
# IronCondor
# ---------------------------------------------------------------------------

class TestIronCondor:
    def _strategy(self) -> IronCondorStrategy:
        return get_strategy("iron_condor")  # type: ignore[return-value]

    def test_positive_gex_fires(self) -> None:
        ctx = AnalysisContext(
            current_df=_df(), spot=SPOT,
            gex={"regime": "positive", "total_gex": 1e9},
            atm={"atm_strike": SPOT, "avg_iv": 15.0},
        )
        types = [s["signal_type"] for s in self._strategy().signals(ctx)]
        assert POSITIVE_GEX_DAMPENING in types

    def test_range_bound_fires_when_em_inside_wings(self) -> None:
        ctx = AnalysisContext(
            current_df=_df(), spot=SPOT,
            gex={"regime": "positive", "total_gex": 5e9},
            atm={"atm_strike": SPOT, "avg_iv": 18.0},
            expected_move={"expected_move": 100.0},  # very small EM
        )
        types = [s["signal_type"] for s in self._strategy().signals(ctx)]
        # Range bound fires because EM << wing width
        from ochain_v2.analyzers.strategies.base import RANGE_BOUND_SIGNAL
        assert RANGE_BOUND_SIGNAL in types

    def test_metrics_has_wing_strikes(self) -> None:
        m = self._strategy().metrics(_ctx())
        assert "suggested_call_wing" in m
        assert "suggested_put_wing" in m
        assert "em_vs_half_wing" in m


# ---------------------------------------------------------------------------
# GammaScalper
# ---------------------------------------------------------------------------

class TestGammaScalper:
    def _strategy(self) -> GammaScalperStrategy:
        return get_strategy("gamma_scalper")  # type: ignore[return-value]

    def test_above_flip_fires_when_spot_above(self) -> None:
        ctx = AnalysisContext(
            current_df=_df(), spot=22500.0,
            gex={"flip_point": 22000.0, "regime": "positive", "total_gex": 5e9},
            atm={"atm_strike": 22500.0},
        )
        types = [s["signal_type"] for s in self._strategy().signals(ctx)]
        assert ABOVE_FLIP_POINT in types

    def test_below_flip_fires_when_spot_below(self) -> None:
        ctx = AnalysisContext(
            current_df=_df(), spot=21500.0,
            gex={"flip_point": 22000.0, "regime": "negative", "total_gex": -5e9},
            atm={"atm_strike": 21500.0},
        )
        types = [s["signal_type"] for s in self._strategy().signals(ctx)]
        assert BELOW_FLIP_POINT in types

    def test_near_flip_fires_within_threshold(self) -> None:
        ctx = AnalysisContext(
            current_df=_df(), spot=22500.0,
            gex={"flip_point": 22510.0, "regime": "neutral", "total_gex": 0.0},
            atm={"atm_strike": 22500.0},
        )
        types = [s["signal_type"] for s in self._strategy().signals(ctx)]
        assert NEAR_FLIP_POINT in types

    def test_no_signals_when_gex_empty(self) -> None:
        ctx = AnalysisContext(current_df=_df(), spot=SPOT)
        assert self._strategy().signals(ctx) == []

    def test_metrics_has_flip_point(self) -> None:
        ctx = AnalysisContext(
            current_df=_df(), spot=SPOT,
            gex={"flip_point": 22200.0, "regime": "positive",
                 "total_gex": 5e9, "total_ce_gex": 7e9, "total_pe_gex": 2e9, "dex": 1e7},
        )
        m = self._strategy().metrics(ctx)
        assert m["flip_point"] == 22200.0
        assert "dist_from_flip_pct" in m
        assert "dex" in m
