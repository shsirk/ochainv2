"""
Typed exception hierarchy for OChain v2.

Catch the base OChainError to handle any application error;
use the sub-classes to distinguish the origin layer.
"""

from __future__ import annotations


class OChainError(Exception):
    """Base class for all OChain application errors."""


# ---------------------------------------------------------------------------
# Ingestion / collection layer
# ---------------------------------------------------------------------------

class BrokerError(OChainError):
    """
    Raised when a broker API call fails (connection drop, auth error,
    rate-limit, malformed response).
    """


class BrokerAuthError(BrokerError):
    """Raised when broker credentials are invalid or expired."""


class BrokerRateLimitError(BrokerError):
    """Raised when the broker's rate limit is exceeded."""


class BrokerTimeoutError(BrokerError):
    """Raised when a broker request times out."""


class IngestError(OChainError):
    """
    Raised when a fetched DataFrame cannot be persisted (bad shape,
    missing columns, DB write failure).
    """


# ---------------------------------------------------------------------------
# Storage layer
# ---------------------------------------------------------------------------

class StorageError(OChainError):
    """Raised on DuckDB or SQLite failures."""


class MigrationError(StorageError):
    """Raised when a DB schema migration fails."""


# ---------------------------------------------------------------------------
# Analysis layer
# ---------------------------------------------------------------------------

class AnalysisError(OChainError):
    """
    Raised when an analyzer receives unexpected input or encounters a
    computation error (e.g. missing ATM strike, singular matrix).
    """


# ---------------------------------------------------------------------------
# Configuration layer
# ---------------------------------------------------------------------------

class ConfigError(OChainError):
    """Raised when settings are missing, malformed, or inconsistent."""


class InstrumentNotFoundError(ConfigError):
    """Raised when a requested instrument is not in instruments.yaml."""
