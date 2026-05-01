"""
Naked Option Buyer strategy.

Signals target directional option buyers who want:
  - Trending / explosive environments (negative GEX regime)
  - OI buildup confirming direction
  - Relatively cheap implied vol
  - Extreme PCR as a contrarian reversal signal
"""

from __future__ import annotations

from ochain_v2.analyzers.strategies.base import (
    BEARISH_OI_BUILDUP, BULLISH_OI_BUILDUP, LOW_IV_CHEAP,
    NEGATIVE_GEX_TRENDING, PCR_HIGH_BUY_CE, PCR_LOW_BUY_PE,
    AnalysisContext,
)

_PCR_HIGH_THRESHOLD = 1.4   # put-heavy → CE bounce candidate
_PCR_LOW_THRESHOLD  = 0.6   # call-heavy → PE bounce candidate
_IV_CHEAP_THRESHOLD = 12.0  # ATM IV below this is "cheap"
_MIN_BUILDUP_FRAC   = 0.50  # >50% of strikes must agree for OI signal


class NakedBuyerStrategy:
    name         = "naked_buyer"
    display_name = "Naked Option Buyer"
    description  = "Momentum and OI-based signals for directional option buying"

    # ------------------------------------------------------------------
    # signals
    # ------------------------------------------------------------------

    def signals(self, ctx: AnalysisContext) -> list[dict]:
        sigs: list[dict] = []
        atm_strike = ctx.atm.get("atm_strike")

        # 1. OI buildup direction
        if ctx.buildups is not None and not ctx.buildups.empty:
            ce_long_frac = float(
                (ctx.buildups["ce_buildup"] == "Long Buildup").mean()
            )
            pe_long_frac = float(
                (ctx.buildups["pe_buildup"] == "Long Buildup").mean()
            )
            if ce_long_frac >= _MIN_BUILDUP_FRAC:
                sigs.append(_sig(
                    BULLISH_OI_BUILDUP, round(ce_long_frac, 3), "CE", atm_strike,
                    f"{ce_long_frac:.0%} of CE strikes show Long Buildup",
                    {"ce_long_buildup_frac": ce_long_frac},
                ))
            if pe_long_frac >= _MIN_BUILDUP_FRAC:
                sigs.append(_sig(
                    BEARISH_OI_BUILDUP, round(pe_long_frac, 3), "PE", atm_strike,
                    f"{pe_long_frac:.0%} of PE strikes show Long Buildup",
                    {"pe_long_buildup_frac": pe_long_frac},
                ))

        # 2. PCR extreme → contrarian reversal
        pcr_oi = ctx.pcr.get("pcr_oi")
        if pcr_oi is not None:
            if pcr_oi > _PCR_HIGH_THRESHOLD:
                strength = round(min((pcr_oi - _PCR_HIGH_THRESHOLD) / 0.6, 1.0), 3)
                sigs.append(_sig(
                    PCR_HIGH_BUY_CE, strength, "CE", atm_strike,
                    f"PCR={pcr_oi:.2f} — put-heavy OI supports CE bounce",
                    {"pcr_oi": pcr_oi, "threshold": _PCR_HIGH_THRESHOLD},
                ))
            elif pcr_oi < _PCR_LOW_THRESHOLD:
                strength = round(min((_PCR_LOW_THRESHOLD - pcr_oi) / 0.4, 1.0), 3)
                sigs.append(_sig(
                    PCR_LOW_BUY_PE, strength, "PE", atm_strike,
                    f"PCR={pcr_oi:.2f} — call-heavy OI supports PE bounce",
                    {"pcr_oi": pcr_oi, "threshold": _PCR_LOW_THRESHOLD},
                ))

        # 3. Negative GEX → trending environment
        if ctx.gex.get("regime") == "negative":
            total_gex = ctx.gex.get("total_gex", 0.0)
            sigs.append(_sig(
                NEGATIVE_GEX_TRENDING, 0.65, "neutral", None,
                "Negative GEX: dealers net short gamma → trending / explosive",
                {"total_gex": total_gex, "regime": "negative"},
            ))

        # 4. Cheap ATM IV
        atm_iv = ctx.atm.get("avg_iv")
        if atm_iv is not None and atm_iv < _IV_CHEAP_THRESHOLD:
            strength = round(max(0.0, (_IV_CHEAP_THRESHOLD - atm_iv) / _IV_CHEAP_THRESHOLD), 3)
            sigs.append(_sig(
                LOW_IV_CHEAP, strength, "neutral", atm_strike,
                f"ATM IV={atm_iv:.1f}% below {_IV_CHEAP_THRESHOLD}% — relatively cheap",
                {"atm_iv": atm_iv, "threshold": _IV_CHEAP_THRESHOLD},
            ))

        return sigs

    # ------------------------------------------------------------------
    # metrics
    # ------------------------------------------------------------------

    def metrics(self, ctx: AnalysisContext) -> dict:
        m: dict = {
            "pcr_oi":        ctx.pcr.get("pcr_oi"),
            "pcr_regime":    ctx.pcr.get("oi_regime"),
            "atm_iv":        ctx.atm.get("avg_iv"),
            "atm_strike":    ctx.atm.get("atm_strike"),
            "straddle_price": ctx.atm.get("straddle_price"),
            "gex_regime":    ctx.gex.get("regime"),
            "total_gex":     ctx.gex.get("total_gex"),
            "expected_move": ctx.expected_move.get("expected_move"),
            "em_pct":        ctx.expected_move.get("pct_move"),
        }
        if ctx.buildups is not None and not ctx.buildups.empty:
            m["ce_long_buildup_pct"] = round(
                float((ctx.buildups["ce_buildup"] == "Long Buildup").mean()) * 100, 1
            )
            m["pe_long_buildup_pct"] = round(
                float((ctx.buildups["pe_buildup"] == "Long Buildup").mean()) * 100, 1
            )
        return m


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _sig(
    signal_type: str,
    strength: float,
    side: str,
    strike: object,
    reason: str,
    metadata: dict,
) -> dict:
    return {
        "signal_type": signal_type,
        "strength":    max(0.0, min(1.0, strength)),
        "side":        side,
        "strike":      float(strike) if strike is not None else None,
        "reason":      reason,
        "metadata":    metadata,
    }
