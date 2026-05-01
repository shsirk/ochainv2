"""
Strategy package.

Importing this package registers all built-in strategies.
"""

from ochain_v2.analyzers.strategies.base import (
    AnalysisContext,
    TradingStrategy,
)
from ochain_v2.analyzers.strategies.registry import (
    clear_registry,
    get_strategy,
    list_names,
    list_strategies,
    register,
)

# Register all built-in strategies on import
from ochain_v2.analyzers.strategies.naked_buyer   import NakedBuyerStrategy
from ochain_v2.analyzers.strategies.naked_seller  import NakedSellerStrategy
from ochain_v2.analyzers.strategies.straddle      import StraddleStrategy
from ochain_v2.analyzers.strategies.strangle      import StrangleStrategy
from ochain_v2.analyzers.strategies.spread        import SpreadStrategy
from ochain_v2.analyzers.strategies.iron_condor   import IronCondorStrategy
from ochain_v2.analyzers.strategies.gamma_scalper import GammaScalperStrategy

_BUILTIN_INSTANCES = [
    NakedBuyerStrategy(),
    NakedSellerStrategy(),
    StraddleStrategy(),
    StrangleStrategy(),
    SpreadStrategy(),
    IronCondorStrategy(),
    GammaScalperStrategy(),
]

for _s in _BUILTIN_INSTANCES:
    register(_s)

__all__ = [
    "AnalysisContext",
    "TradingStrategy",
    "register",
    "get_strategy",
    "list_strategies",
    "list_names",
    "clear_registry",
]
