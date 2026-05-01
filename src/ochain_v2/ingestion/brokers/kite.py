"""
Kite (Zerodha) broker adapter — placeholder stub.

Not yet implemented.  Raises ``NotImplementedError`` on every method call.
"""

from __future__ import annotations

import pandas as pd

from ochain_v2.ingestion.brokers.base import BrokerProtocol, BrokerStatus


class KiteBroker:
    """Placeholder.  Raises NotImplementedError until implemented."""

    name = "kite"

    def __init__(self) -> None:
        self._status = BrokerStatus(broker_name="kite")

    @property
    def is_connected(self) -> bool:
        return False

    @property
    def status(self) -> BrokerStatus:
        return self._status

    async def connect(self) -> None:
        raise NotImplementedError("KiteBroker is not yet implemented.")

    async def disconnect(self) -> None:
        raise NotImplementedError("KiteBroker is not yet implemented.")

    async def get_expiries(self, symbol: str) -> list[str]:
        raise NotImplementedError("KiteBroker is not yet implemented.")

    async def get_option_chain(self, symbol: str, expiry: str) -> pd.DataFrame:
        raise NotImplementedError("KiteBroker is not yet implemented.")


assert isinstance(KiteBroker(), BrokerProtocol)
