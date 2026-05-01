"""Broker adapters for OChain v2."""

from ochain_v2.ingestion.brokers.base import BrokerProtocol, BrokerStatus
from ochain_v2.ingestion.brokers.fixtures import FixtureBroker
from ochain_v2.ingestion.brokers.kite import KiteBroker

__all__ = [
    "BrokerProtocol",
    "BrokerStatus",
    "FixtureBroker",
    "KiteBroker",
]

# DhanBroker is conditionally importable — only when the [dhan] extra is installed.
try:
    from ochain_v2.ingestion.brokers.dhan import DhanBroker  # noqa: F401
    __all__.append("DhanBroker")
except ImportError:
    pass
