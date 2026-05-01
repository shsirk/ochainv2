"""
Gamma Scalper strategy.

Trades the dealer gamma map.  Key concepts:

  GEX flip point  — the strike where cumulative net GEX crosses zero.
                    Above flip → dealers long gamma → mean-reverting.
                    Below flip → dealers short gamma → trending.

  Positive regime — spot above flip, range-trade between flip and ATM.
  Negative regime — spot below flip, follow momentum.
"""

from __future__ import annotations

from ochain_v2.analyzers.strategies.base import (
    ABOVE_FLIP_POINT, BELOW_FLIP_POINT, NEAR_FLIP_POINT,
    NEGATIVE_GEX_TRENDING, POSITIVE_GEX_DAMPENING, AnalysisContext,
)
from ochain_v2.analyzers.strategies.naked_buyer import _sig

_NEAR_FLIP_PCT  = 0.005   # spot within 0.5% of flip → near signal
_HIGH_GEX_MIN   = 1e9     # absolute total GEX above this is "high"


class GammaScalperStrategy:
    name         = "gamma_scalper"
    display_name = "Gamma Scalper"
    description  = "Dealer gamma map and flip-point signals for gamma scalping"

    def signals(self, ctx: AnalysisContext) -> list[dict]:
        sigs: list[dict] = []
        gex         = ctx.gex
        flip        = gex.get("flip_point")
        regime      = gex.get("regime")
        total_gex   = gex.get("total_gex", 0.0)
        spot        = ctx.spot
        atm_strike  = ctx.atm.get("atm_strike")

        if not gex or spot <= 0:
            return sigs

        # 1. Spot vs flip point
        if flip is not None:
            dist_pct = abs(spot - flip) / spot

            if dist_pct <= _NEAR_FLIP_PCT:
                sigs.append(_sig(
                    NEAR_FLIP_POINT, round(1.0 - dist_pct / _NEAR_FLIP_PCT, 3),
                    "neutral", flip,
                    f"Spot={spot:.0f} within {dist_pct:.2%} of GEX flip={flip:.0f}",
                    {"flip_point": flip, "spot": spot, "dist_pct": dist_pct},
                ))
            elif spot > flip:
                strength = round(min((spot - flip) / spot / 0.02, 1.0), 3)
                sigs.append(_sig(
                    ABOVE_FLIP_POINT, strength, "neutral", flip,
                    f"Spot={spot:.0f} above flip={flip:.0f} — dealers long gamma, "
                    f"range-trade mean reversion",
                    {"flip_point": flip, "spot": spot},
                ))
            else:
                strength = round(min((flip - spot) / spot / 0.02, 1.0), 3)
                sigs.append(_sig(
                    BELOW_FLIP_POINT, strength, "neutral", flip,
                    f"Spot={spot:.0f} below flip={flip:.0f} — dealers short gamma, "
                    f"follow momentum",
                    {"flip_point": flip, "spot": spot},
                ))

        # 2. Regime signal
        if regime == "positive":
            sigs.append(_sig(
                POSITIVE_GEX_DAMPENING, 0.65, "neutral", atm_strike,
                f"Total GEX={total_gex:.2e} — positive regime, scalp mean reversion",
                {"total_gex": total_gex, "regime": regime},
            ))
        elif regime == "negative":
            sigs.append(_sig(
                NEGATIVE_GEX_TRENDING, 0.65, "neutral", atm_strike,
                f"Total GEX={total_gex:.2e} — negative regime, follow trend momentum",
                {"total_gex": total_gex, "regime": regime},
            ))

        return sigs

    def metrics(self, ctx: AnalysisContext) -> dict:
        gex = ctx.gex
        flip = gex.get("flip_point")
        spot = ctx.spot
        dist_from_flip = (
            round(abs(spot - flip) / spot * 100, 3)
            if flip is not None and spot > 0 else None
        )
        return {
            "flip_point":        flip,
            "spot":              spot,
            "dist_from_flip_pct": dist_from_flip,
            "gex_regime":        gex.get("regime"),
            "total_gex":         gex.get("total_gex"),
            "total_ce_gex":      gex.get("total_ce_gex"),
            "total_pe_gex":      gex.get("total_pe_gex"),
            "dex":               gex.get("dex"),
        }
