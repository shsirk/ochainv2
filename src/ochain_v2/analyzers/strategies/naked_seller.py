"""
Naked Option Seller strategy.

Signals target premium sellers who want:
  - High implied vol (sell expensive options)
  - Range-bound / dampening GEX regime
  - OI pinning near ATM (max pain effect)
  - Balanced PCR (neither side has conviction)
"""

from __future__ import annotations

from ochain_v2.analyzers.strategies.base import (
    HIGH_IV_SELL_PREMIUM, OI_PIN_EFFECT, PCR_NEUTRAL_BALANCED,
    POSITIVE_GEX_DAMPENING, SHORT_BUILDUP_DOMINANT, AnalysisContext,
)
from ochain_v2.analyzers.strategies.naked_buyer import _sig

_IV_RICH_THRESHOLD    = 20.0   # ATM IV above this is "expensive"
_PCR_NEUTRAL_LOW      = 0.85
_PCR_NEUTRAL_HIGH     = 1.15
_PIN_DISTANCE_PCT     = 0.01   # max pain within 1% of spot → pin signal
_MIN_SHORT_FRAC       = 0.40   # >40% short buildup on either side


class NakedSellerStrategy:
    name         = "naked_seller"
    display_name = "Naked Option Seller"
    description  = "IV and OI-based signals for premium collection"

    def signals(self, ctx: AnalysisContext) -> list[dict]:
        sigs: list[dict] = []
        atm_strike = ctx.atm.get("atm_strike")

        # 1. High ATM IV — sell premium
        atm_iv = ctx.atm.get("avg_iv")
        if atm_iv is not None and atm_iv > _IV_RICH_THRESHOLD:
            strength = round(min((atm_iv - _IV_RICH_THRESHOLD) / 20.0, 1.0), 3)
            sigs.append(_sig(
                HIGH_IV_SELL_PREMIUM, strength, "neutral", atm_strike,
                f"ATM IV={atm_iv:.1f}% above {_IV_RICH_THRESHOLD}% — sell premium",
                {"atm_iv": atm_iv, "threshold": _IV_RICH_THRESHOLD},
            ))

        # 2. Positive GEX → dealers long gamma → range-bound / dampening
        if ctx.gex.get("regime") == "positive":
            total_gex = ctx.gex.get("total_gex", 0.0)
            sigs.append(_sig(
                POSITIVE_GEX_DAMPENING, 0.70, "neutral", None,
                "Positive GEX: dealers net long gamma → pinning / range-bound",
                {"total_gex": total_gex, "regime": "positive"},
            ))

        # 3. PCR near 1.0 → balanced market, neither side has conviction
        pcr_oi = ctx.pcr.get("pcr_oi")
        if pcr_oi is not None and _PCR_NEUTRAL_LOW <= pcr_oi <= _PCR_NEUTRAL_HIGH:
            distance = abs(pcr_oi - 1.0)
            strength = round(max(0.0, 1.0 - distance / 0.15), 3)
            sigs.append(_sig(
                PCR_NEUTRAL_BALANCED, strength, "neutral", atm_strike,
                f"PCR={pcr_oi:.2f} balanced — no directional conviction",
                {"pcr_oi": pcr_oi},
            ))

        # 4. OI pin effect — max pain close to spot
        from ochain_v2.analyzers.primitives import compute_max_pain
        if not ctx.current_df.empty and ctx.spot > 0:
            mp = compute_max_pain(ctx.current_df)
            pain_price = mp.get("max_pain_price")
            if pain_price is not None:
                dist_pct = abs(pain_price - ctx.spot) / ctx.spot
                if dist_pct <= _PIN_DISTANCE_PCT:
                    strength = round(max(0.0, 1.0 - dist_pct / _PIN_DISTANCE_PCT), 3)
                    sigs.append(_sig(
                        OI_PIN_EFFECT, strength, "neutral", pain_price,
                        f"Max pain={pain_price:.0f} within {dist_pct:.1%} of spot={ctx.spot:.0f}",
                        {"max_pain": pain_price, "spot": ctx.spot, "dist_pct": dist_pct},
                    ))

        # 5. Short buildup dominant — writers are in control
        if ctx.buildups is not None and not ctx.buildups.empty:
            ce_short_frac = float(
                (ctx.buildups["ce_buildup"] == "Short Buildup").mean()
            )
            pe_short_frac = float(
                (ctx.buildups["pe_buildup"] == "Short Buildup").mean()
            )
            if ce_short_frac >= _MIN_SHORT_FRAC or pe_short_frac >= _MIN_SHORT_FRAC:
                frac = max(ce_short_frac, pe_short_frac)
                sigs.append(_sig(
                    SHORT_BUILDUP_DOMINANT, round(frac, 3), "neutral", atm_strike,
                    f"Short buildup on {frac:.0%} of strikes — writers adding positions",
                    {"ce_short_frac": ce_short_frac, "pe_short_frac": pe_short_frac},
                ))

        return sigs

    def metrics(self, ctx: AnalysisContext) -> dict:
        from ochain_v2.analyzers.primitives import compute_max_pain
        mp: dict = {}
        if not ctx.current_df.empty:
            mp = compute_max_pain(ctx.current_df)

        return {
            "pcr_oi":         ctx.pcr.get("pcr_oi"),
            "pcr_regime":     ctx.pcr.get("oi_regime"),
            "atm_iv":         ctx.atm.get("avg_iv"),
            "atm_strike":     ctx.atm.get("atm_strike"),
            "straddle_price": ctx.atm.get("straddle_price"),
            "gex_regime":     ctx.gex.get("regime"),
            "total_gex":      ctx.gex.get("total_gex"),
            "max_pain":       mp.get("max_pain_price"),
            "expected_move":  ctx.expected_move.get("expected_move"),
        }
