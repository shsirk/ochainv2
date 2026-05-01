"""
Pydantic response models for all OChain v2 API endpoints.

All models use ``model_config = ConfigDict(extra="allow")`` so that
callers can attach extra keys without validation errors — useful when
passing through raw analyzer dicts that may contain additional metrics.
"""

from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, ConfigDict


# ---------------------------------------------------------------------------
# Shared primitives
# ---------------------------------------------------------------------------

class SnapshotRef(BaseModel):
    snapshot_id: int
    ts: str
    bucket_1m: Optional[int] = None


class StrikeRow(BaseModel):
    model_config = ConfigDict(extra="allow")
    strike: float
    ce_oi: Optional[float] = None
    ce_volume: Optional[float] = None
    ce_ltp: Optional[float] = None
    ce_iv: Optional[float] = None
    ce_bid: Optional[float] = None
    ce_ask: Optional[float] = None
    ce_delta: Optional[float] = None
    ce_gamma: Optional[float] = None
    ce_theta: Optional[float] = None
    ce_vega: Optional[float] = None
    pe_oi: Optional[float] = None
    pe_volume: Optional[float] = None
    pe_ltp: Optional[float] = None
    pe_iv: Optional[float] = None
    pe_bid: Optional[float] = None
    pe_ask: Optional[float] = None
    pe_delta: Optional[float] = None
    pe_gamma: Optional[float] = None
    pe_theta: Optional[float] = None
    pe_vega: Optional[float] = None


# ---------------------------------------------------------------------------
# Chain / Analyze
# ---------------------------------------------------------------------------

class SummaryBlock(BaseModel):
    model_config = ConfigDict(extra="allow")
    pcr: Optional[dict] = None
    atm: Optional[dict] = None
    gex: Optional[dict] = None
    support_resistance: Optional[dict] = None
    expected_move: Optional[dict] = None
    iv_smile: Optional[dict] = None


class AnalyzeResponse(BaseModel):
    model_config = ConfigDict(extra="allow")
    symbol: str
    expiry: str
    trade_date: str
    base_ts: Optional[str] = None
    compare_ts: Optional[str] = None
    snapshot_ts: Optional[str] = None
    from_idx: int = 0
    to_idx: int = 0
    tf_seconds: int = 60
    total_snapshots: int = 0
    underlying_ltp: Optional[float] = None
    strikes: list[StrikeRow] = []
    summary: SummaryBlock = SummaryBlock()
    # Delta columns (optional — present when from_idx != to_idx)
    deltas: Optional[list[dict]] = None


# ---------------------------------------------------------------------------
# Heatmap
# ---------------------------------------------------------------------------

class HeatmapResponse(BaseModel):
    symbol: str
    expiry: str
    trade_date: str
    metric: str
    strikes: list[float]
    timestamps: list[str]
    matrix: list[list[Optional[float]]]


# ---------------------------------------------------------------------------
# GEX
# ---------------------------------------------------------------------------

class GexResponse(BaseModel):
    model_config = ConfigDict(extra="allow")
    symbol: str
    expiry: str
    trade_date: str
    snapshot_ts: Optional[str] = None
    underlying_ltp: Optional[float] = None
    net_gex: Optional[float] = None
    flip_point: Optional[float] = None
    regime: Optional[str] = None
    strikes: list[dict] = []


# ---------------------------------------------------------------------------
# Strike drill
# ---------------------------------------------------------------------------

class StrikeDrillResponse(BaseModel):
    model_config = ConfigDict(extra="allow")
    symbol: str
    expiry: str
    strike: float
    trade_date: str
    rows: list[dict] = []


# ---------------------------------------------------------------------------
# IV Surface
# ---------------------------------------------------------------------------

class IvSurfaceResponse(BaseModel):
    model_config = ConfigDict(extra="allow")
    symbol: str
    expiry: str
    trade_date: str
    underlying_ltp: Optional[float] = None
    iv_smile: dict = {}
    iv_surface: list[dict] = []


# ---------------------------------------------------------------------------
# Alerts
# ---------------------------------------------------------------------------

class AlertEvent(BaseModel):
    id: int
    ts: str
    symbol: str
    expiry: str
    strike: Optional[float] = None
    side: Optional[str] = None
    alert_type: str
    detail: Optional[str] = None
    magnitude: Optional[float] = None


class AlertsResponse(BaseModel):
    symbol: str
    alerts: list[AlertEvent]


# ---------------------------------------------------------------------------
# Scalper
# ---------------------------------------------------------------------------

class ScalperResponse(BaseModel):
    model_config = ConfigDict(extra="allow")
    symbol: str
    expiry: str
    trade_date: str
    underlying_ltp: Optional[float] = None
    signals: list[dict] = []
    metrics: dict = {}


# ---------------------------------------------------------------------------
# Strategy payoff
# ---------------------------------------------------------------------------

class PayoffLeg(BaseModel):
    option_type: str        # "CE" or "PE"
    strike: float
    premium: float
    quantity: int           # +ve = long, -ve = short
    expiry: Optional[str] = None


class PayoffRequest(BaseModel):
    legs: list[PayoffLeg]
    spot: float
    iv: float = 15.0
    dte: float = 30.0
    spot_range_pct: float = 10.0


class PayoffResponse(BaseModel):
    model_config = ConfigDict(extra="allow")
    spot_range: list[float]
    pnl: list[float]
    pop: Optional[float] = None
    max_profit: Optional[float] = None
    max_loss: Optional[float] = None
    breakevens: list[float] = []


# ---------------------------------------------------------------------------
# Expiry / dates / snapshots
# ---------------------------------------------------------------------------

class ExpiriesResponse(BaseModel):
    symbol: str
    expiries: list[str]


class DatesResponse(BaseModel):
    symbol: str
    dates: list[str]


class SnapshotsResponse(BaseModel):
    symbol: str
    expiry: str
    trade_date: str
    total: int
    snapshots: list[SnapshotRef]


# ---------------------------------------------------------------------------
# v2 — Instruments
# ---------------------------------------------------------------------------

class InstrumentMeta(BaseModel):
    model_config = ConfigDict(extra="allow")
    symbol: str
    lot_size: int
    tick_size: float
    strike_step: float
    num_strikes: int = 20
    exchange: str = "NSE"
    is_index: bool = True


class InstrumentsResponse(BaseModel):
    instruments: list[InstrumentMeta]


# ---------------------------------------------------------------------------
# v2 — Session
# ---------------------------------------------------------------------------

class SessionResponse(BaseModel):
    model_config = ConfigDict(extra="allow")
    symbol: str
    expiry: str
    market_open: bool
    latest_snapshot_id: Optional[int] = None
    latest_ts: Optional[str] = None
    session_base_snapshot_id: Optional[int] = None
    underlying_ltp: Optional[float] = None


# ---------------------------------------------------------------------------
# v2 — Composite
# ---------------------------------------------------------------------------

class CompositeResponse(BaseModel):
    model_config = ConfigDict(extra="allow")
    symbol: str
    expiry: str
    trade_date: str
    underlying_ltp: Optional[float] = None
    chain: Optional[dict] = None
    gex: Optional[dict] = None
    scalper: Optional[dict] = None
    alerts: Optional[list] = None


# ---------------------------------------------------------------------------
# v2 — Strategies
# ---------------------------------------------------------------------------

class StrategyInfo(BaseModel):
    name: str
    display_name: str
    description: str


class StrategiesResponse(BaseModel):
    strategies: list[StrategyInfo]


class StrategySignalsResponse(BaseModel):
    model_config = ConfigDict(extra="allow")
    symbol: str
    expiry: str
    strategy: str
    signals: list[dict]
    metrics: dict


# ---------------------------------------------------------------------------
# v2 — Views
# ---------------------------------------------------------------------------

class ViewQuery(BaseModel):
    model_config = ConfigDict(extra="allow")
    symbol: Optional[str] = None
    expiry: Optional[str] = None
    trade_date: Optional[str] = None
    from_idx: Optional[int] = None
    to_idx: Optional[int] = None
    tf: Optional[str] = None
    tab: Optional[str] = None


class SavedView(BaseModel):
    id: int
    name: str
    query: ViewQuery
    created_at: str
    updated_at: str


class ViewsResponse(BaseModel):
    views: list[SavedView]


# ---------------------------------------------------------------------------
# WebSocket event
# ---------------------------------------------------------------------------

class LiveSnapshotEvent(BaseModel):
    event: str = "snapshot"
    symbol: str
    expiry: str
    snapshot_id: int
    ts: str
    summary: Optional[dict] = None


class AlertPushEvent(BaseModel):
    event: str = "alert"
    id: int
    ts: str
    symbol: str
    expiry: str
    alert_type: str
    detail: Optional[str] = None
    magnitude: Optional[float] = None
