"""
Strategy layer base types.

AnalysisContext  — carries all pre-computed analytics into every strategy.
TradingStrategy  — Protocol that each strategy class must implement.
Signal constants — shared signal-type strings.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Protocol, runtime_checkable

import pandas as pd

# ---------------------------------------------------------------------------
# Signal type constants
# ---------------------------------------------------------------------------

# Directional — naked buyer
BULLISH_OI_BUILDUP    = "BULLISH_OI_BUILDUP"
BEARISH_OI_BUILDUP    = "BEARISH_OI_BUILDUP"
PCR_HIGH_BUY_CE       = "PCR_HIGH_BUY_CE"
PCR_LOW_BUY_PE        = "PCR_LOW_BUY_PE"
NEGATIVE_GEX_TRENDING = "NEGATIVE_GEX_TRENDING"
LOW_IV_CHEAP          = "LOW_IV_CHEAP"

# Premium collection — naked seller / straddle / strangle / iron condor
POSITIVE_GEX_DAMPENING = "POSITIVE_GEX_DAMPENING"
HIGH_IV_SELL_PREMIUM   = "HIGH_IV_SELL_PREMIUM"
OI_PIN_EFFECT          = "OI_PIN_EFFECT"
SHORT_BUILDUP_DOMINANT = "SHORT_BUILDUP_DOMINANT"
PCR_NEUTRAL_BALANCED   = "PCR_NEUTRAL_BALANCED"

# Vol direction — straddle / strangle
CHEAP_STRADDLE         = "CHEAP_STRADDLE"
EXPENSIVE_STRADDLE     = "EXPENSIVE_STRADDLE"
IV_SKEW_ASYMMETRIC     = "IV_SKEW_ASYMMETRIC"

# Range / spread
BULL_SPREAD_SIGNAL     = "BULL_SPREAD_SIGNAL"
BEAR_SPREAD_SIGNAL     = "BEAR_SPREAD_SIGNAL"
RANGE_BOUND_SIGNAL     = "RANGE_BOUND_SIGNAL"

# Gamma / flip
ABOVE_FLIP_POINT       = "ABOVE_FLIP_POINT"
BELOW_FLIP_POINT       = "BELOW_FLIP_POINT"
NEAR_FLIP_POINT        = "NEAR_FLIP_POINT"


# ---------------------------------------------------------------------------
# Signal shape reference (TypedDict not enforced at runtime — kept for docs)
# ---------------------------------------------------------------------------
#
# {
#   "signal_type" : str,          # one of the constants above
#   "strength"    : float,        # 0.0 (weak) … 1.0 (strong)
#   "side"        : str,          # "CE" | "PE" | "neutral"
#   "strike"      : float | None, # recommended strike, if applicable
#   "reason"      : str,          # human-readable explanation
#   "metadata"    : dict,         # raw numbers that drove the signal
# }


# ---------------------------------------------------------------------------
# AnalysisContext
# ---------------------------------------------------------------------------

@dataclass
class AnalysisContext:
    """
    Carries all pre-computed analytics for a single (symbol, expiry, snapshot).

    Construct with ``AnalysisContext.build()`` to auto-run every primitive,
    or build directly in tests by supplying only the fields you need.
    """
    current_df: pd.DataFrame
    spot: float

    symbol:          str                    = ""
    expiry:          str                    = ""
    lot_size:        int                    = 1
    dte:             float                  = 0.0
    prev_df:         Optional[pd.DataFrame] = None
    session_base_df: Optional[pd.DataFrame] = None

    # Pre-computed results (populated by build(), or set manually in tests)
    pcr:               dict                    = field(default_factory=dict)
    atm:               dict                    = field(default_factory=dict)
    gex:               dict                    = field(default_factory=dict)
    buildups:          Optional[pd.DataFrame]  = None
    support_resistance: dict                   = field(default_factory=dict)
    expected_move:     dict                    = field(default_factory=dict)
    iv_smile:          dict                    = field(default_factory=dict)

    @classmethod
    def build(
        cls,
        current_df:      pd.DataFrame,
        spot:            float,
        symbol:          str                    = "",
        expiry:          str                    = "",
        lot_size:        int                    = 1,
        dte:             float                  = 0.0,
        prev_df:         Optional[pd.DataFrame] = None,
        session_base_df: Optional[pd.DataFrame] = None,
    ) -> "AnalysisContext":
        """Run all primitives automatically and return a fully-populated context."""
        from ochain_v2.analyzers.primitives import (
            compute_atm, compute_buildups, compute_pcr,
            compute_support_resistance,
        )
        from ochain_v2.analyzers.gex import compute_gex
        from ochain_v2.analyzers.expected_move import compute_expected_move

        if current_df.empty or spot <= 0:
            return cls(current_df=current_df, spot=spot, symbol=symbol,
                       expiry=expiry, lot_size=lot_size, dte=dte,
                       prev_df=prev_df, session_base_df=session_base_df)

        pcr      = compute_pcr(current_df)
        atm      = compute_atm(current_df, spot)
        buildups = compute_buildups(current_df, prev_df) if prev_df is not None else None
        sr       = compute_support_resistance(current_df)
        gex      = compute_gex(current_df, spot, lot_size, dte=max(dte, 1.0))
        em       = compute_expected_move(
            spot,
            atm.get("avg_iv"),
            dte if dte > 0 else None,
            atm.get("straddle_price"),
        )

        return cls(
            current_df=current_df,
            spot=spot,
            symbol=symbol,
            expiry=expiry,
            lot_size=lot_size,
            dte=dte,
            prev_df=prev_df,
            session_base_df=session_base_df,
            pcr=pcr,
            atm=atm,
            gex=gex,
            buildups=buildups,
            support_resistance=sr,
            expected_move=em,
        )


# ---------------------------------------------------------------------------
# TradingStrategy Protocol
# ---------------------------------------------------------------------------

@runtime_checkable
class TradingStrategy(Protocol):
    """
    Protocol for all trading strategies.

    Implement ``signals()`` and ``metrics()``.  Both methods must be
    free of side effects and fast enough for sub-second API calls.
    """
    name:         str
    display_name: str
    description:  str

    def signals(self, ctx: AnalysisContext) -> list[dict]:
        """
        Return a list of signal dicts for the given context.
        Each dict must contain: signal_type, strength, side, strike, reason, metadata.
        """
        ...

    def metrics(self, ctx: AnalysisContext) -> dict:
        """
        Return strategy-specific scalar metrics for UI display.
        All values must be JSON-serialisable (no numpy scalars).
        """
        ...
