"""
OChain v2 — FastAPI application factory.

Lifespan
--------
On startup: initialise DuckDBReader, MetaDB, LivePublisher, ApiCache; wire
into dep-injection globals.
On shutdown: close the reader's thread-local connection.

Routers
-------
v1-compatible:  /api/symbols, /api/dates, /api/expiry_list, /api/snapshots,
                /api/analyze, /api/heatmap, /api/gex, /api/strike,
                /api/iv_surface, /api/alerts, /api/scalper, /api/strategy/payoff,
                /api/expiries
v2 new:         /api/v2/instruments, /api/v2/session, /api/v2/composite,
                /api/v2/strategies, /api/v2/export, /api/v2/replay, /api/v2/views
collector ctrl: /collector/api/*
WebSocket:      /ws/live/{symbol}, /ws/alerts
Health:         /healthz
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI
import pathlib

from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Startup / shutdown hook."""
    from ochain_v2.api import deps
    from ochain_v2.api import ws as ws_pkg
    from ochain_v2.api.cache import get_api_cache
    from ochain_v2.db.duckdb_reader import DuckDBReader
    from ochain_v2.db.meta_sqlite import MetaDB
    from ochain_v2.ingestion.live_publisher import LivePublisher

    try:
        from ochain_v2.core.settings import get_settings
        cfg = get_settings()
        db_path   = cfg.db.duckdb_path
        meta_path = cfg.db.meta_sqlite_path
    except Exception:
        db_path   = "data/ochain.duckdb"
        meta_path = "data/ochain_meta.sqlite"

    reader = DuckDBReader(db_path)
    meta   = MetaDB(meta_path)
    pub    = LivePublisher()

    deps.set_reader(reader)
    deps.set_meta(meta)

    # Wire WS modules
    from ochain_v2.api.ws.live   import set_publisher
    from ochain_v2.api.ws.alerts import set_meta as ws_set_meta
    set_publisher(pub)
    ws_set_meta(meta)

    # Pre-warm cache singleton
    get_api_cache()

    log.info("OChain v2 API startup complete (db=%s)", db_path)
    yield

    reader.close()
    log.info("OChain v2 API shutdown complete")


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

def create_app(
    *,
    allow_origins: list[str] | None = None,
    include_collector_api: bool = False,
) -> FastAPI:
    """
    Create and configure the FastAPI application.

    Parameters
    ----------
    allow_origins:
        CORS origins.  Defaults to ``["*"]`` (permissive — restrict in prod).
    include_collector_api:
        Mount the ``/collector/api/`` router (only in single-process mode).
    """
    app = FastAPI(
        title="OChain v2",
        description="Indian market option chain collector and analyzer",
        version="2.0.0",
        lifespan=lifespan,
    )

    # CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allow_origins or ["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Health
    @app.get("/healthz", tags=["health"])
    async def healthz():
        from ochain_v2.api.deps import get_reader
        try:
            reader = get_reader()
            symbols = reader.get_symbols()
            return JSONResponse({"status": "ok", "symbols": symbols})
        except Exception as exc:
            return JSONResponse({"status": "error", "detail": str(exc)}, status_code=503)

    # v1-compatible routes
    from ochain_v2.api.routes import (
        alerts, chain, expiry, gex, heatmap, iv, scalper, strategy, strike,
    )
    for mod in (chain, heatmap, gex, strike, iv, alerts, scalper, strategy, expiry):
        app.include_router(mod.router, tags=["v1"])

    # v2 routes
    from ochain_v2.api.routes import (
        composite, export, instruments, replay, session, strategies_list, views,
    )
    for mod in (instruments, session, composite, strategies_list, export, replay, views):
        app.include_router(mod.router, tags=["v2"])

    # WebSocket
    from ochain_v2.api.ws import alerts as ws_alerts_mod
    from ochain_v2.api.ws import live as ws_live_mod
    app.include_router(ws_live_mod.router,   tags=["ws"])
    app.include_router(ws_alerts_mod.router, tags=["ws"])

    # Collector control API (optional)
    if include_collector_api:
        from ochain_v2.api.collector_api import router as col_router
        app.include_router(col_router)

    # Static files + HTML UI
    _root = pathlib.Path(__file__).parent.parent.parent.parent  # project root
    _static = _root / "static"
    _templates = _root / "templates"
    if _static.exists():
        app.mount("/static", StaticFiles(directory=str(_static)), name="static")
    if _templates.exists():
        @app.get("/", include_in_schema=False)
        async def serve_index():
            return FileResponse(str(_templates / "index.html"))

        @app.get("/collector", include_in_schema=False)
        async def serve_collector():
            p = _templates / "collector.html"
            if p.exists():
                return FileResponse(str(p))
            return FileResponse(str(_templates / "index.html"))

    # Cache-Control + ETag on all GET responses
    @app.middleware("http")
    async def add_cache_headers(request, call_next):
        response = await call_next(request)
        if request.method == "GET" and response.status_code == 200:
            if not response.headers.get("Cache-Control"):
                response.headers["Cache-Control"] = "public, max-age=30"
        return response

    return app


# ---------------------------------------------------------------------------
# Module-level app instance (used by uvicorn: "ochain_v2.api.main:app")
# ---------------------------------------------------------------------------

app = create_app()
