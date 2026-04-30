"""Unit tests for analyzers/iv_surface.py"""

from datetime import date, timedelta

import pandas as pd
import pytest

from ochain_v2.analyzers.iv_surface import (
    _interpolate_gaps, compute_iv_smile, compute_iv_surface,
)

SPOT    = 22500.0
STRIKES = [22250.0, 22350.0, 22450.0, 22500.0, 22550.0, 22650.0, 22750.0]

# Use a date far enough in the future that DTE > 0 in tests
_FUTURE  = date(2099, 12, 31)
_NEAR    = date(2099, 12, 24)   # 7 days before _FUTURE


def _df(ce_iv=15.0, pe_iv=14.0, strikes=None) -> pd.DataFrame:
    s = strikes or STRIKES
    return pd.DataFrame({
        "strike": s,
        "ce_iv":  [ce_iv] * len(s),
        "pe_iv":  [pe_iv] * len(s),
    })


class TestComputeIvSmile:
    def test_empty_df_returns_empty(self) -> None:
        result = compute_iv_smile(pd.DataFrame(), _FUTURE, SPOT)
        assert result["strikes"] == []
        assert result["atm_iv"] is None

    def test_returns_expected_keys(self) -> None:
        result = compute_iv_smile(_df(), _FUTURE, SPOT)
        assert "strikes" in result
        assert "moneyness" in result
        assert "ce_iv" in result
        assert "pe_iv" in result
        assert "atm_iv" in result
        assert "skew" in result
        assert "expiry" in result
        assert "dte" in result

    def test_strike_count_matches(self) -> None:
        result = compute_iv_smile(_df(), _FUTURE, SPOT)
        n = len(STRIKES)
        assert len(result["strikes"])   == n
        assert len(result["moneyness"]) == n
        assert len(result["ce_iv"])     == n
        assert len(result["pe_iv"])     == n

    def test_moneyness_atm_near_zero(self) -> None:
        result = compute_iv_smile(_df(), _FUTURE, SPOT)
        # ATM strike 22500 → moneyness = 0
        idx = result["strikes"].index(22500.0)
        assert result["moneyness"][idx] == pytest.approx(0.0)

    def test_atm_iv_is_average(self) -> None:
        result = compute_iv_smile(_df(ce_iv=16.0, pe_iv=14.0), _FUTURE, SPOT)
        assert result["atm_iv"] == pytest.approx(15.0, abs=0.1)

    def test_strikes_sorted(self) -> None:
        result = compute_iv_smile(_df(), _FUTURE, SPOT)
        assert result["strikes"] == sorted(result["strikes"])

    def test_dte_future(self) -> None:
        result = compute_iv_smile(_df(), _FUTURE, SPOT)
        assert result["dte"] > 0

    def test_zero_iv_returns_none(self) -> None:
        # Zero IV rows should be cleaned to None
        df = _df(ce_iv=0.0, pe_iv=0.0)
        result = compute_iv_smile(df, _FUTURE, SPOT)
        assert all(v is None for v in result["ce_iv"])
        assert result["atm_iv"] is None

    def test_skew_put_minus_call(self) -> None:
        # 5% OTM put IV higher than 5% OTM call IV → positive skew
        df = pd.DataFrame({
            "strike": [21375.0, 22500.0, 23625.0],   # -5%, ATM, +5%
            "ce_iv":  [14.0, 15.0, 13.0],
            "pe_iv":  [17.0, 15.0, 13.0],
        })
        result = compute_iv_smile(df, _FUTURE, SPOT)
        # put 5% OTM IV (17.0) - call 5% OTM IV (13.0) = 4.0
        assert result["skew"] is not None
        assert result["skew"] > 0


class TestComputeIvSurface:
    def test_empty_input(self) -> None:
        result = compute_iv_surface({}, SPOT)
        assert result["strikes"] == []
        assert result["expiries"] == []

    def test_single_expiry(self) -> None:
        result = compute_iv_surface({str(_FUTURE): _df()}, SPOT)
        assert len(result["expiries"]) == 1
        assert len(result["ce_surface"]) == 1
        assert len(result["ce_surface"][0]) == len(STRIKES)

    def test_two_expiries_sorted(self) -> None:
        result = compute_iv_surface(
            {str(_FUTURE): _df(), str(_NEAR): _df(ce_iv=16.0)},
            SPOT,
        )
        # Sorted near → far
        assert result["expiries"][0] == str(_NEAR)
        assert result["expiries"][1] == str(_FUTURE)

    def test_common_strike_axis_union(self) -> None:
        # Two expiries with different strikes; surface uses union
        df1 = _df(strikes=[22400.0, 22500.0])
        df2 = _df(strikes=[22500.0, 22600.0])
        result = compute_iv_surface({str(_NEAR): df1, str(_FUTURE): df2}, SPOT)
        assert 22400.0 in result["strikes"]
        assert 22500.0 in result["strikes"]
        assert 22600.0 in result["strikes"]

    def test_interpolation_fills_missing_strikes(self) -> None:
        # near expiry has only subset of the union strikes
        df_near = _df(strikes=[22400.0, 22600.0])      # missing 22500
        df_far  = _df(strikes=[22400.0, 22500.0, 22600.0])
        result = compute_iv_surface({str(_NEAR): df_near, str(_FUTURE): df_far}, SPOT)
        # Near expiry row: 22500 should be interpolated, not None
        near_row = result["ce_surface"][0]
        idx_22500 = result["strikes"].index(22500.0)
        assert near_row[idx_22500] is not None

    def test_atm_ivs_length_matches_expiries(self) -> None:
        result = compute_iv_surface(
            {str(_NEAR): _df(), str(_FUTURE): _df()}, SPOT
        )
        assert len(result["atm_ivs"]) == 2

    def test_dte_values_non_negative(self) -> None:
        result = compute_iv_surface({str(_FUTURE): _df()}, SPOT)
        assert all(d >= 0 for d in result["dte"])


class TestInterpolateGaps:
    def test_no_gaps(self) -> None:
        result = _interpolate_gaps([1.0, 2.0, 3.0], [10.0, 20.0, 30.0])
        assert result == [10.0, 20.0, 30.0]

    def test_interior_gap_filled(self) -> None:
        result = _interpolate_gaps([0.0, 1.0, 2.0], [10.0, None, 30.0])
        assert result[1] == pytest.approx(20.0)

    def test_leading_none_stays_none(self) -> None:
        result = _interpolate_gaps([0.0, 1.0, 2.0], [None, 20.0, 30.0])
        assert result[0] is None

    def test_trailing_none_stays_none(self) -> None:
        result = _interpolate_gaps([0.0, 1.0, 2.0], [10.0, 20.0, None])
        assert result[2] is None

    def test_all_none_unchanged(self) -> None:
        result = _interpolate_gaps([0.0, 1.0, 2.0], [None, None, None])
        assert result == [None, None, None]

    def test_single_valid_no_interpolation(self) -> None:
        result = _interpolate_gaps([0.0, 1.0, 2.0], [None, 20.0, None])
        assert result[0] is None
        assert result[1] == 20.0
        assert result[2] is None
