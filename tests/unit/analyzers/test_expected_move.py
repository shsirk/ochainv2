"""Unit tests for analyzers/expected_move.py"""

import math
import pytest

from ochain_v2.analyzers.expected_move import compute_expected_move

SPOT = 22500.0


class TestComputeExpectedMove:
    # ------------------------------------------------------------------
    # None / invalid inputs
    # ------------------------------------------------------------------

    def test_no_inputs_returns_none_result(self) -> None:
        result = compute_expected_move(SPOT, None, None, None)
        assert result["expected_move"] is None
        assert result["method"] is None

    def test_zero_spot_returns_none(self) -> None:
        result = compute_expected_move(0.0, 15.0, 30.0)
        assert result["expected_move"] is None

    def test_negative_spot_returns_none(self) -> None:
        result = compute_expected_move(-100.0, 15.0, 30.0)
        assert result["expected_move"] is None

    def test_zero_dte_returns_none_for_iv_method(self) -> None:
        result = compute_expected_move(SPOT, 15.0, 0.0)
        assert result["method"] is None

    def test_zero_iv_returns_none_for_iv_method(self) -> None:
        result = compute_expected_move(SPOT, 0.0, 30.0)
        assert result["method"] is None

    # ------------------------------------------------------------------
    # IV + DTE method
    # ------------------------------------------------------------------

    def test_iv_dte_method_selected(self) -> None:
        result = compute_expected_move(SPOT, 15.0, 30.0)
        assert result["method"] == "iv_dte"

    def test_iv_dte_formula_correct(self) -> None:
        # EM = spot × (IV/100) × sqrt(dte/365) × 0.85
        spot, iv, dte = 22500.0, 15.0, 30.0
        expected_em = spot * (iv / 100.0) * math.sqrt(dte / 365.0) * 0.85
        result = compute_expected_move(spot, iv, dte)
        assert result["expected_move"] == pytest.approx(expected_em, rel=1e-3)

    def test_iv_accepts_decimal_format(self) -> None:
        # 15.0 (>1.0) is divided by 100 → 0.15; 0.15 (<1.0) stays as-is → same EM
        r1 = compute_expected_move(SPOT, 15.0, 30.0)   # percentage form
        r2 = compute_expected_move(SPOT, 0.15, 30.0)   # already decimal
        assert r1["method"] == "iv_dte"
        assert r2["method"] == "iv_dte"
        assert r1["expected_move"] == pytest.approx(r2["expected_move"], rel=1e-4)

    def test_upper_lower_symmetry(self) -> None:
        result = compute_expected_move(SPOT, 15.0, 30.0)
        em = result["expected_move"]
        assert result["upper"] == pytest.approx(SPOT + em, abs=0.01)
        assert result["lower"] == pytest.approx(SPOT - em, abs=0.01)

    def test_pct_move_correct(self) -> None:
        result = compute_expected_move(SPOT, 15.0, 30.0)
        expected_pct = result["expected_move"] / SPOT * 100
        assert result["pct_move"] == pytest.approx(expected_pct, rel=1e-3)

    def test_longer_dte_larger_em(self) -> None:
        r30  = compute_expected_move(SPOT, 15.0,  30.0)
        r365 = compute_expected_move(SPOT, 15.0, 365.0)
        assert r365["expected_move"] > r30["expected_move"]

    def test_higher_iv_larger_em(self) -> None:
        r15 = compute_expected_move(SPOT, 15.0, 30.0)
        r30 = compute_expected_move(SPOT, 30.0, 30.0)
        assert r30["expected_move"] == pytest.approx(r15["expected_move"] * 2, rel=1e-3)

    # ------------------------------------------------------------------
    # Straddle fallback method
    # ------------------------------------------------------------------

    def test_straddle_method_fallback(self) -> None:
        result = compute_expected_move(SPOT, None, None, atm_straddle=200.0)
        assert result["method"] == "straddle"

    def test_straddle_formula_correct(self) -> None:
        straddle = 200.0
        expected_em = straddle * 0.85
        result = compute_expected_move(SPOT, None, None, atm_straddle=straddle)
        assert result["expected_move"] == pytest.approx(expected_em, rel=1e-3)

    def test_straddle_zero_returns_none(self) -> None:
        result = compute_expected_move(SPOT, None, None, atm_straddle=0.0)
        assert result["expected_move"] is None

    def test_iv_dte_preferred_over_straddle(self) -> None:
        # When both are provided, IV+DTE wins
        result = compute_expected_move(SPOT, 15.0, 30.0, atm_straddle=200.0)
        assert result["method"] == "iv_dte"

    # ------------------------------------------------------------------
    # Sanity ranges for NIFTY
    # ------------------------------------------------------------------

    def test_nifty_30dte_15iv_reasonable_range(self) -> None:
        result = compute_expected_move(22500.0, 15.0, 30.0)
        # 22500 × 0.15 × sqrt(30/365) × 0.85 ≈ 822
        assert 500 < result["expected_move"] < 1100

    def test_result_keys_present(self) -> None:
        result = compute_expected_move(SPOT, 15.0, 30.0)
        assert {"expected_move", "upper", "lower", "pct_move", "method"} == set(result.keys())
