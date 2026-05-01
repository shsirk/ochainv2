"""
Straddle strategy.

Buy straddle when implied move is cheap (straddle < expected move).
Sell straddle when implied move is expensive (straddle > expected move).
"""

from __future__ import annotations

from ochain_v2.analyzers.strategies.base import (
    CHEAP_STRADDLE, EXPENSIVE_STRADDLE, PCR_NEUTRAL_BALANCED,
    POSITIVE_GEX_DAMPENING, AnalysisContext,
)
from ochain_v2.analyzers.strategies.naked_buyer import _sig

_BUY_RATIO   = 0.85    # straddle / EM < this → buy (cheap vol)
_SELL_RATIO  = 1.15    # straddle / EM > this → sell (expensive vol)
_IV_SELL_MIN = 18.0    # minimum ATM IV to flag sell straddle


class StraddleStrategy:
    name         = "straddle"
    display_name = "Straddle / Strangle"
    description  = "Volatility direction signals for non-directional positions"

    def signals(self, ctx: AnalysisContext) -> list[dict]:
        sigs: list[dict] = []
        atm_strike  = ctx.atm.get("atm_strike")
        straddle    = ctx.atm.get("straddle_price")
        em          = ctx.expected_move.get("expected_move")

        # 1. Straddle vs expected move
        if straddle is not None and em is not None and em > 0:
            ratio = straddle / em
            if ratio < _BUY_RATIO:
                strength = round(min((_BUY_RATIO - ratio) / _BUY_RATIO, 1.0), 3)
                sigs.append(_sig(
                    CHEAP_STRADDLE, strength, "neutral", atm_strike,
                    f"Straddle={straddle:.0f} < {ratio:.2f}× EM={em:.0f} — buy vol",
                    {"straddle": straddle, "expected_move": em, "ratio": ratio},
                ))
            elif ratio > _SELL_RATIO:
                strength = round(min((ratio - _SELL_RATIO) / _SELL_RATIO, 1.0), 3)
                sigs.append(_sig(
                    EXPENSIVE_STRADDLE, strength, "neutral", atm_strike,
                    f"Straddle={straddle:.0f} > {ratio:.2f}× EM={em:.0f} — sell vol",
                    {"straddle": straddle, "expected_move": em, "ratio": ratio},
                ))

        # 2. Positive GEX + balanced PCR → straddle selling environment
        pcr_oi = ctx.pcr.get("pcr_oi")
        if ctx.gex.get("regime") == "positive":
            sigs.append(_sig(
                POSITIVE_GEX_DAMPENING, 0.60, "neutral", None,
                "Positive GEX: dampening regime supports straddle selling",
                {"gex_regime": "positive"},
            ))
        if pcr_oi is not None and 0.90 <= pcr_oi <= 1.10:
            sigs.append(_sig(
                PCR_NEUTRAL_BALANCED, round(1.0 - abs(pcr_oi - 1.0) / 0.10, 3),
                "neutral", atm_strike,
                f"PCR={pcr_oi:.2f} balanced — directionally ambiguous, straddle candidate",
                {"pcr_oi": pcr_oi},
            ))

        # 3. High IV → sell straddle signal (complementary to expensive straddle)
        atm_iv = ctx.atm.get("avg_iv")
        if atm_iv is not None and atm_iv > _IV_SELL_MIN and straddle is not None:
            sigs.append(_sig(
                EXPENSIVE_STRADDLE,
                round(min((atm_iv - _IV_SELL_MIN) / 20.0, 1.0), 3),
                "neutral", atm_strike,
                f"ATM IV={atm_iv:.1f}% — elevated, sell straddle for mean reversion",
                {"atm_iv": atm_iv, "straddle_price": straddle},
            ))

        return _deduplicate(sigs)

    def metrics(self, ctx: AnalysisContext) -> dict:
        straddle = ctx.atm.get("straddle_price")
        em       = ctx.expected_move.get("expected_move")
        ratio    = round(straddle / em, 3) if straddle and em else None
        return {
            "straddle_price":  straddle,
            "expected_move":   em,
            "straddle_em_ratio": ratio,
            "atm_iv":          ctx.atm.get("avg_iv"),
            "atm_strike":      ctx.atm.get("atm_strike"),
            "pcr_oi":          ctx.pcr.get("pcr_oi"),
            "gex_regime":      ctx.gex.get("regime"),
        }


def _deduplicate(sigs: list[dict]) -> list[dict]:
    seen: set[str] = set()
    result = []
    for s in sigs:
        if s["signal_type"] not in seen:
            seen.add(s["signal_type"])
            result.append(s)
        else:
            # Keep the one with higher strength
            for i, existing in enumerate(result):
                if existing["signal_type"] == s["signal_type"]:
                    if s["strength"] > existing["strength"]:
                        result[i] = s
                    break
    return result
