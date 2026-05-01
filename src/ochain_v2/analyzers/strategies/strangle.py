"""
Strangle strategy.

Sell OTM call + OTM put to collect premium in a defined range.
Looks for: high IV, positive GEX, defined support/resistance wings.
"""

from __future__ import annotations

from ochain_v2.analyzers.strategies.base import (
    HIGH_IV_SELL_PREMIUM, IV_SKEW_ASYMMETRIC, POSITIVE_GEX_DAMPENING,
    RANGE_BOUND_SIGNAL, AnalysisContext,
)
from ochain_v2.analyzers.strategies.naked_buyer import _sig

_IV_MIN          = 16.0    # minimum ATM IV to sell strangle
_WING_OTM_PCT    = 0.03    # default OTM offset for wings (3% from spot)


class StrangleStrategy:
    name         = "strangle"
    display_name = "Strangle"
    description  = "Wide non-directional premium collection signals"

    def signals(self, ctx: AnalysisContext) -> list[dict]:
        sigs: list[dict] = []
        atm_strike = ctx.atm.get("atm_strike")

        # 1. High IV — premium collection opportunity
        atm_iv = ctx.atm.get("avg_iv")
        if atm_iv is not None and atm_iv > _IV_MIN:
            strength = round(min((atm_iv - _IV_MIN) / 20.0, 1.0), 3)
            sigs.append(_sig(
                HIGH_IV_SELL_PREMIUM, strength, "neutral", atm_strike,
                f"ATM IV={atm_iv:.1f}% — sell OTM strangle for premium",
                {"atm_iv": atm_iv, "threshold": _IV_MIN},
            ))

        # 2. Positive GEX → range-bound regime
        if ctx.gex.get("regime") == "positive":
            sigs.append(_sig(
                POSITIVE_GEX_DAMPENING, 0.65, "neutral", None,
                "Positive GEX: dealers long gamma → market likely to stay ranged",
                {"gex_regime": "positive", "total_gex": ctx.gex.get("total_gex")},
            ))

        # 3. Support/resistance defines strangle wings
        sr = ctx.support_resistance
        supports    = sr.get("support",    [])
        resistances = sr.get("resistance", [])
        if supports and resistances and ctx.spot > 0:
            top_resistance = resistances[0].get("strike") if resistances else None
            top_support    = supports[0].get("strike")    if supports    else None
            if top_resistance and top_support:
                range_pct = (top_resistance - top_support) / ctx.spot
                if range_pct <= 0.06:    # tight range → strangle favorable
                    sigs.append(_sig(
                        RANGE_BOUND_SIGNAL,
                        round(max(0.0, 1.0 - range_pct / 0.06), 3),
                        "neutral", atm_strike,
                        f"Range {top_support:.0f}–{top_resistance:.0f} "
                        f"({range_pct:.1%} wide) — sell strangle inside wings",
                        {"support": top_support, "resistance": top_resistance,
                         "range_pct": range_pct},
                    ))

        # 4. IV skew — if skew is low, symmetric strangle is favoured
        iv_smile = ctx.iv_smile
        skew = iv_smile.get("skew")
        if skew is not None and abs(skew) < 1.0:
            strength = round(max(0.0, 1.0 - abs(skew)), 3)
            sigs.append(_sig(
                IV_SKEW_ASYMMETRIC, strength, "neutral", atm_strike,
                f"IV skew={skew:.2f}pp — symmetric wings, strangle favorable",
                {"iv_skew": skew},
            ))

        return sigs

    def metrics(self, ctx: AnalysisContext) -> dict:
        sr = ctx.support_resistance
        supports    = sr.get("support",    [])
        resistances = sr.get("resistance", [])
        call_wing = (
            resistances[0].get("strike") if resistances
            else (ctx.spot * (1 + _WING_OTM_PCT) if ctx.spot else None)
        )
        put_wing = (
            supports[0].get("strike") if supports
            else (ctx.spot * (1 - _WING_OTM_PCT) if ctx.spot else None)
        )
        return {
            "atm_iv":          ctx.atm.get("avg_iv"),
            "atm_strike":      ctx.atm.get("atm_strike"),
            "straddle_price":  ctx.atm.get("straddle_price"),
            "suggested_call_wing": round(call_wing, 0) if call_wing else None,
            "suggested_put_wing":  round(put_wing,  0) if put_wing  else None,
            "gex_regime":      ctx.gex.get("regime"),
            "iv_skew":         ctx.iv_smile.get("skew"),
            "expected_move":   ctx.expected_move.get("expected_move"),
        }
