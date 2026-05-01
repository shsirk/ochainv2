"""
FastAPI dependency injectors.

All dependencies are request-scoped singletons: the underlying reader/meta
objects are process-level singletons created at lifespan startup and injected
via module-level references.
"""

from __future__ import annotations

from typing import Annotated, Optional

from fastapi import Depends

from ochain_v2.api.cache import ApiCache, get_api_cache
from ochain_v2.db.duckdb_reader import DuckDBReader
from ochain_v2.db.meta_sqlite import MetaDB

# ---------------------------------------------------------------------------
# Module-level references — set during lifespan startup in main.py
# ---------------------------------------------------------------------------

_reader: Optional[DuckDBReader] = None
_meta:   Optional[MetaDB] = None


def set_reader(reader: DuckDBReader) -> None:
    global _reader
    _reader = reader


def set_meta(meta: MetaDB) -> None:
    global _meta
    _meta = meta


def get_reader() -> DuckDBReader:
    if _reader is None:
        raise RuntimeError(
            "DuckDBReader not initialised. "
            "Did lifespan startup run? Check api/main.py."
        )
    return _reader


def get_meta() -> MetaDB:
    if _meta is None:
        raise RuntimeError(
            "MetaDB not initialised. "
            "Did lifespan startup run? Check api/main.py."
        )
    return _meta


def get_cache() -> ApiCache:
    return get_api_cache()


# ---------------------------------------------------------------------------
# Annotated type aliases (convenience for route signatures)
# ---------------------------------------------------------------------------

ReaderDep = Annotated[DuckDBReader, Depends(get_reader)]
MetaDep   = Annotated[MetaDB,       Depends(get_meta)]
CacheDep  = Annotated[ApiCache,     Depends(get_cache)]
