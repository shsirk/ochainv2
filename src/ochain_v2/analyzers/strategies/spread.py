"""
Vertical Spread strategy.

Bull call spread  — buy ATM CE, sell OTM CE.
Bear put spread   — buy ATM PE, sell OTM PE.

Signals fire when a directional view is present but IV is moderate
(neither so cheap that naked buy is better, nor so high that credit
spread is the only rational trade).
"""

from __future__ import annotations

from ochain_v2.analyzers.strategies.base import (
    BEAR_SPREAD_SIGNAL, BULL_SPREAD_SIGNAL, AnalysisContext,
)
from ochain_v2.analyzers.strategies.naked_buyer import _sig

_IV_DEBIT_MAX   = 22.0    # above this, debit spread is expensive → credit spread better
_MIN_BUILDUP_FRAC = 0.45  # directional threshold (slightly looser than naked buyer)
_OTM_STEPS      = 2       # sell strike is 2 strikes OTM from ATM


class SpreadStrategy:
    name         = "spread"
    display_name = "Vertical Spread"
    description  = "Bull/bear vertical spread entry and exit signals"

    def signals(self, ctx: AnalysisContext) -> list[dict]:
        sigs: list[dict] = []
        atm_strike = ctx.atm.get("atm_strike")

        # Need directional view from OI buildups
        if ctx.buildups is None or ctx.buildups.empty:
            return sigs

        ce_long_frac  = float((ctx.buildups["ce_buildup"] == "Long Buildup").mean())
        pe_long_frac  = float((ctx.buildups["pe_buildup"] == "Long Buildup").mean())
        atm_iv        = ctx.atm.get("avg_iv")

        # Sell strike suggestion (OTM from ATM)
        sell_ce_strike = _otm_strike(ctx, atm_strike, "CE")
        sell_pe_strike = _otm_strike(ctx, atm_strike, "PE")

        # Bull call spread
        if ce_long_frac >= _MIN_BUILDUP_FRAC:
            # Prefer debit spread when IV is moderate; credit when IV is high
            credit_vs_debit = "credit" if (atm_iv or 0) > _IV_DEBIT_MAX else "debit"
            strength = round(min(ce_long_frac, 1.0), 3)
            sigs.append(_sig(
                BULL_SPREAD_SIGNAL, strength, "CE", atm_strike,
                f"{ce_long_frac:.0%} CE long buildup — {credit_vs_debit} bull spread "
                f"(buy {atm_strike:.0f} CE / sell {sell_ce_strike:.0f} CE)",
                {
                    "buy_strike":  atm_strike,
                    "sell_strike": sell_ce_strike,
                    "spread_type": credit_vs_debit,
                    "atm_iv":      atm_iv,
                    "ce_long_frac": ce_long_frac,
                },
            ))

        # Bear put spread
        if pe_long_frac >= _MIN_BUILDUP_FRAC:
            credit_vs_debit = "credit" if (atm_iv or 0) > _IV_DEBIT_MAX else "debit"
            strength = round(min(pe_long_frac, 1.0), 3)
            sigs.append(_sig(
                BEAR_SPREAD_SIGNAL, strength, "PE", atm_strike,
                f"{pe_long_frac:.0%} PE long buildup — {credit_vs_debit} bear spread "
                f"(buy {atm_strike:.0f} PE / sell {sell_pe_strike:.0f} PE)",
                {
                    "buy_strike":  atm_strike,
                    "sell_strike": sell_pe_strike,
                    "spread_type": credit_vs_debit,
                    "atm_iv":      atm_iv,
                    "pe_long_frac": pe_long_frac,
                },
            ))

        return sigs

    def metrics(self, ctx: AnalysisContext) -> dict:
        atm_iv = ctx.atm.get("avg_iv")
        return {
            "atm_iv":            atm_iv,
            "atm_strike":        ctx.atm.get("atm_strike"),
            "pcr_oi":            ctx.pcr.get("pcr_oi"),
            "pcr_regime":        ctx.pcr.get("oi_regime"),
            "spread_mode":       "credit" if (atm_iv or 0) > _IV_DEBIT_MAX else "debit",
            "expected_move":     ctx.expected_move.get("expected_move"),
            "ce_long_buildup_pct": (
                round(float((ctx.buildups["ce_buildup"] == "Long Buildup").mean()) * 100, 1)
                if ctx.buildups is not None and not ctx.buildups.empty else None
            ),
            "pe_long_buildup_pct": (
                round(float((ctx.buildups["pe_buildup"] == "Long Buildup").mean()) * 100, 1)
                if ctx.buildups is not None and not ctx.buildups.empty else None
            ),
        }


def _otm_strike(ctx: AnalysisContext, atm: object, side: str) -> float:
    """Return the strike _OTM_STEPS above (CE) or below (PE) ATM from the chain."""
    if atm is None or ctx.current_df.empty:
        step = ctx.spot * 0.005 if ctx.spot else 50.0
        return float(atm or 0) + (step * _OTM_STEPS if side == "CE" else -step * _OTM_STEPS)

    strikes = sorted(ctx.current_df["strike"].tolist())
    try:
        idx = strikes.index(float(atm))
    except ValueError:
        return float(atm)

    if side == "CE":
        target = min(idx + _OTM_STEPS, len(strikes) - 1)
    else:
        target = max(idx - _OTM_STEPS, 0)
    return float(strikes[target])
