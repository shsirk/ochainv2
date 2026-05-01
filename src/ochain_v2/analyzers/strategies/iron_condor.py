"""
Iron Condor strategy.

Sell OTM call spread + OTM put spread in a range-bound market.
Signals fire when GEX is positive, PCR balanced, and the expected
move is smaller than the proposed wing width.
"""

from __future__ import annotations

from ochain_v2.analyzers.strategies.base import (
    HIGH_IV_SELL_PREMIUM, PCR_NEUTRAL_BALANCED, POSITIVE_GEX_DAMPENING,
    RANGE_BOUND_SIGNAL, AnalysisContext,
)
from ochain_v2.analyzers.strategies.naked_buyer import _sig

_IV_MIN           = 16.0    # minimum ATM IV to sell condor
_PCR_NEUTRAL_LOW  = 0.85
_PCR_NEUTRAL_HIGH = 1.15
_WING_STEPS       = 3       # wing is 3 strikes OTM from ATM


class IronCondorStrategy:
    name         = "iron_condor"
    display_name = "Iron Condor"
    description  = "Range-bound regime detection for iron condor positioning"

    def signals(self, ctx: AnalysisContext) -> list[dict]:
        sigs: list[dict] = []
        atm_strike = ctx.atm.get("atm_strike")
        atm_iv     = ctx.atm.get("avg_iv")

        # 1. Positive GEX — dealers long gamma, market is pinned / ranged
        if ctx.gex.get("regime") == "positive":
            sigs.append(_sig(
                POSITIVE_GEX_DAMPENING, 0.75, "neutral", None,
                "Positive GEX: dealers long gamma → iron condor favorable",
                {"gex_regime": "positive", "total_gex": ctx.gex.get("total_gex")},
            ))

        # 2. Balanced PCR — no directional dominance
        pcr_oi = ctx.pcr.get("pcr_oi")
        if pcr_oi is not None and _PCR_NEUTRAL_LOW <= pcr_oi <= _PCR_NEUTRAL_HIGH:
            strength = round(max(0.0, 1.0 - abs(pcr_oi - 1.0) / 0.15), 3)
            sigs.append(_sig(
                PCR_NEUTRAL_BALANCED, strength, "neutral", atm_strike,
                f"PCR={pcr_oi:.2f} balanced — no directional bias, condor candidate",
                {"pcr_oi": pcr_oi},
            ))

        # 3. High IV → sell premium across wings
        if atm_iv is not None and atm_iv > _IV_MIN:
            strength = round(min((atm_iv - _IV_MIN) / 20.0, 1.0), 3)
            sigs.append(_sig(
                HIGH_IV_SELL_PREMIUM, strength, "neutral", atm_strike,
                f"ATM IV={atm_iv:.1f}% — sell condor wings for premium",
                {"atm_iv": atm_iv},
            ))

        # 4. Expected move inside wings → profitable range likely
        em = ctx.expected_move.get("expected_move")
        call_wing, put_wing = _wing_strikes(ctx, atm_strike)
        if em is not None and call_wing and put_wing and ctx.spot > 0:
            half_wing_width = (call_wing - put_wing) / 2
            if em < half_wing_width:
                strength = round(min(1.0 - em / half_wing_width, 1.0), 3)
                sigs.append(_sig(
                    RANGE_BOUND_SIGNAL, strength, "neutral", atm_strike,
                    f"EM={em:.0f} < half-wing-width={half_wing_width:.0f} "
                    f"— condor breakevens ({put_wing:.0f}–{call_wing:.0f}) outside EM",
                    {
                        "expected_move": em,
                        "call_wing": call_wing,
                        "put_wing":  put_wing,
                    },
                ))

        return sigs

    def metrics(self, ctx: AnalysisContext) -> dict:
        atm_strike = ctx.atm.get("atm_strike")
        call_wing, put_wing = _wing_strikes(ctx, atm_strike)
        em = ctx.expected_move.get("expected_move")
        half_wing = (call_wing - put_wing) / 2 if (call_wing and put_wing) else None

        return {
            "atm_iv":           ctx.atm.get("avg_iv"),
            "atm_strike":       atm_strike,
            "straddle_price":   ctx.atm.get("straddle_price"),
            "pcr_oi":           ctx.pcr.get("pcr_oi"),
            "gex_regime":       ctx.gex.get("regime"),
            "expected_move":    em,
            "suggested_call_wing": call_wing,
            "suggested_put_wing":  put_wing,
            "em_vs_half_wing":  round(em / half_wing, 3) if (em and half_wing) else None,
        }


def _wing_strikes(ctx: AnalysisContext, atm: object) -> tuple[float | None, float | None]:
    if atm is None or ctx.current_df.empty:
        return None, None
    strikes = sorted(ctx.current_df["strike"].tolist())
    try:
        idx = strikes.index(float(atm))
    except ValueError:
        return None, None
    call_idx = min(idx + _WING_STEPS, len(strikes) - 1)
    put_idx  = max(idx - _WING_STEPS, 0)
    return float(strikes[call_idx]), float(strikes[put_idx])
