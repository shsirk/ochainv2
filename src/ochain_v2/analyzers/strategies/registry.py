"""
Strategy registry — maps strategy names to instances.

Usage
-----
    from ochain_v2.analyzers.strategies.registry import get_strategy, list_strategies

    strategy = get_strategy("naked_buyer")
    signals  = strategy.signals(ctx)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ochain_v2.analyzers.strategies.base import TradingStrategy

_REGISTRY: dict[str, "TradingStrategy"] = {}


def register(strategy: "TradingStrategy") -> "TradingStrategy":
    """
    Register a strategy instance.  Raises ValueError on duplicate name.
    Can be used as a decorator on the class or called directly after instantiation.
    """
    if strategy.name in _REGISTRY:
        raise ValueError(f"Strategy '{strategy.name}' is already registered.")
    _REGISTRY[strategy.name] = strategy
    return strategy


def get_strategy(name: str) -> "TradingStrategy":
    """Return registered strategy by name. Raises KeyError if not found."""
    if name not in _REGISTRY:
        raise KeyError(
            f"Strategy '{name}' not found. Available: {sorted(_REGISTRY)}"
        )
    return _REGISTRY[name]


def list_strategies() -> list["TradingStrategy"]:
    """Return all registered strategy instances, sorted by name."""
    return [_REGISTRY[k] for k in sorted(_REGISTRY)]


def list_names() -> list[str]:
    """Return all registered strategy names, sorted."""
    return sorted(_REGISTRY)


def clear_registry() -> None:
    """Remove all registrations.  Intended for tests only."""
    _REGISTRY.clear()
