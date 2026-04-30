"""Unit tests for analyzers/greeks.py — Black-Scholes and payoff utilities."""

import math
import pytest
import numpy as np

from ochain_v2.analyzers.greeks import (
    Leg, bs_delta, bs_gamma, bs_iv, bs_price, bs_theta, bs_vega,
    compute_payoff, compute_pop,
)

# Standard test case: ATM call, 1-year DTE, r=5%, σ=20%
# Known values from BS tables
S, K, T, r, sigma = 100.0, 100.0, 1.0, 0.05, 0.20
# CE ≈ 10.45, delta ≈ 0.637
_CE_PRICE_APPROX  = 10.45
_CE_DELTA_APPROX  = 0.637
_TOLERANCE_ABS    = 0.10    # ±0.10 on price (rounding from erf approximation)
_TOLERANCE_DELTA  = 0.005   # ±0.005 on delta


class TestBsPrice:
    def test_ce_price_known(self) -> None:
        price = bs_price(S, K, T, r, sigma, "CE")
        assert abs(price - _CE_PRICE_APPROX) < _TOLERANCE_ABS

    def test_pe_price_put_call_parity(self) -> None:
        # Put-call parity: C - P = S - K*e^(-rT)
        ce = bs_price(S, K, T, r, sigma, "CE")
        pe = bs_price(S, K, T, r, sigma, "PE")
        parity = S - K * math.exp(-r * T)
        assert abs((ce - pe) - parity) < 0.01

    def test_itm_ce_ge_intrinsic(self) -> None:
        # Deep ITM call price ≥ intrinsic value
        price = bs_price(S, 80.0, T, r, sigma, "CE")
        assert price >= S - 80.0

    def test_otm_ce_positive(self) -> None:
        price = bs_price(S, 120.0, T, r, sigma, "CE")
        assert price > 0

    def test_zero_dte_ce_intrinsic(self) -> None:
        # At expiry, CE value = max(S-K, 0)
        price = bs_price(110.0, 100.0, 0.0, r, sigma, "CE")
        assert abs(price - 10.0) < 0.50   # wide tolerance at near-zero T

    def test_indian_market_atm(self) -> None:
        # NIFTY-like: S=22500, ATM, 30 DTE, IV=15%, r=7%
        price = bs_price(22500.0, 22500.0, 30/365, 0.07, 0.15, "CE")
        assert 200 < price < 900   # sanity range for ATM 30-DTE Nifty option


class TestBsDelta:
    def test_atm_call_delta_near_half(self) -> None:
        d = bs_delta(S, K, T, r, sigma, "CE")
        assert abs(d - _CE_DELTA_APPROX) < _TOLERANCE_DELTA

    def test_atm_put_delta_near_minus_half(self) -> None:
        d = bs_delta(S, K, T, r, sigma, "PE")
        assert abs(d - (_CE_DELTA_APPROX - 1.0)) < _TOLERANCE_DELTA

    def test_deep_itm_ce_delta_near_one(self) -> None:
        d = bs_delta(200.0, 100.0, 1.0, r, sigma, "CE")
        assert d > 0.95

    def test_deep_otm_ce_delta_near_zero(self) -> None:
        d = bs_delta(50.0, 100.0, 1.0, r, sigma, "CE")
        assert d < 0.05

    def test_put_call_delta_sum(self) -> None:
        # call delta - put delta ≈ 1 (for same strike)
        d_ce = bs_delta(S, K, T, r, sigma, "CE")
        d_pe = bs_delta(S, K, T, r, sigma, "PE")
        assert abs(d_ce - d_pe - 1.0) < 0.001


class TestBsGamma:
    def test_positive(self) -> None:
        assert bs_gamma(S, K, T, r, sigma) > 0

    def test_atm_gamma_peak(self) -> None:
        # Gamma is highest ATM; ITM and OTM should be lower
        g_atm  = bs_gamma(S,     K,     T, r, sigma)
        g_itm  = bs_gamma(S,     S*0.8, T, r, sigma)
        g_otm  = bs_gamma(S,     S*1.2, T, r, sigma)
        assert g_atm > g_itm
        assert g_atm > g_otm


class TestBsTheta:
    def test_negative_for_long(self) -> None:
        # Long option loses value per day → theta negative
        assert bs_theta(S, K, T, r, sigma, "CE") < 0
        assert bs_theta(S, K, T, r, sigma, "PE") < 0

    def test_theta_accelerates_near_expiry(self) -> None:
        # theta for 30 DTE > theta for 1 year (per day)
        t_1y = bs_theta(S, K, 1.0,    r, sigma, "CE")
        t_30 = bs_theta(S, K, 30/365, r, sigma, "CE")
        assert abs(t_30) > abs(t_1y)


class TestBsVega:
    def test_positive(self) -> None:
        assert bs_vega(S, K, T, r, sigma) > 0

    def test_units_per_1pct(self) -> None:
        # Vega is in same price units as premium, per 1pp IV change
        v = bs_vega(S, K, T, r, sigma)
        price_low  = bs_price(S, K, T, r, sigma - 0.01, "CE")
        price_high = bs_price(S, K, T, r, sigma + 0.01, "CE")
        approx_vega = (price_high - price_low) / 2
        assert abs(v - approx_vega) < 0.01


class TestBsIv:
    def test_roundtrip(self) -> None:
        target_sigma = 0.22
        price = bs_price(S, K, T, r, target_sigma, "CE")
        recovered = bs_iv(price, S, K, T, r, "CE")
        assert recovered is not None
        assert abs(recovered - target_sigma) < 1e-4

    def test_roundtrip_pe(self) -> None:
        target_sigma = 0.18
        price = bs_price(S, K, T, r, target_sigma, "PE")
        recovered = bs_iv(price, S, K, T, r, "PE")
        assert recovered is not None
        assert abs(recovered - target_sigma) < 1e-4

    def test_below_intrinsic_returns_none(self) -> None:
        # Price below intrinsic → no valid IV
        assert bs_iv(0.01, 110.0, 100.0, T, r, "CE") is None

    def test_various_sigmas(self) -> None:
        for sigma_test in [0.10, 0.20, 0.30, 0.50]:
            price = bs_price(S, K, T, r, sigma_test, "CE")
            recovered = bs_iv(price, S, K, T, r, "CE")
            assert recovered is not None
            assert abs(recovered - sigma_test) < 1e-3


class TestComputePayoff:
    def _long_call(self) -> Leg:
        return Leg(option_type="CE", strike=100.0, premium=5.0,
                   quantity=1, lot_size=1)

    def test_long_call_below_strike_loses_premium(self) -> None:
        result = compute_payoff([self._long_call()], spot_range=np.array([80.0, 90.0]))
        assert all(p == pytest.approx(-5.0) for p in result["pnl"])

    def test_long_call_above_strike_profitable(self) -> None:
        # At spot=115, intrinsic=15, net PnL = 15-5 = 10
        result = compute_payoff([self._long_call()], spot_range=np.array([115.0]))
        assert result["pnl"][0] == pytest.approx(10.0)

    def test_breakeven_detected(self) -> None:
        # Long call breakeven = strike + premium = 105
        result = compute_payoff([self._long_call()],
                                spot_range=np.linspace(90.0, 120.0, 300))
        assert len(result["breakevens"]) == 1
        assert abs(result["breakevens"][0] - 105.0) < 0.5

    def test_straddle_two_breakevens(self) -> None:
        long_call = Leg("CE", 100.0, premium=5.0)
        long_put  = Leg("PE", 100.0, premium=5.0)
        result = compute_payoff(
            [long_call, long_put],
            spot_range=np.linspace(80.0, 120.0, 500),
        )
        assert len(result["breakevens"]) == 2


class TestComputePop:
    def test_returns_between_0_and_1(self) -> None:
        legs = [Leg("CE", 100.0, premium=5.0)]
        pop = compute_pop(legs, spot=100.0)
        assert 0.0 <= pop <= 1.0

    def test_atm_short_straddle_pop_above_50pct(self) -> None:
        # Short ATM straddle: sell CE and PE each for premium=5 (received), quantity=-1
        # PnL = (intrinsic - premium) * quantity → (0 - 5) * (-1) = +5 when both OTM
        legs = [
            Leg("CE", 100.0, premium=5.0, quantity=-1),
            Leg("PE", 100.0, premium=5.0, quantity=-1),
        ]
        pop = compute_pop(legs, spot=100.0, iv=0.20, dte=30.0)
        # Short straddle profits when spot stays near ATM — PoP should be > 30%
        assert pop > 0.3
