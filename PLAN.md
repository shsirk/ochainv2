# OChain v2 — Implementation Plan

> **Goal**: Production-grade Indian market option chain collector and analyzer supporting multiple
> instruments, multiple expiries, 1-minute polling during market hours (9:15–15:30 IST), and
> concurrent multi-trader analysis. v1 (`../OChain/`) stays frozen and untouched.

---

## Guiding Principles

1. **DuckDB** for columnar time-series storage; SQLite only for operational metadata.
2. **Deltas pre-computed at ingest** — read path never recomputes vs-base or vs-prev.
3. **Collector and API are separate processes** — writer isolation, no read/write contention.
4. **asyncio + ThreadPoolExecutor** for concurrent multi-instrument polling; per-broker token bucket.
5. **Market hours enforced at the scheduler** — 9:15–15:30 IST, NSE holiday calendar.
6. **v1 UI contract preserved** — `{symbol, expiry, date, tf, from_idx, to_idx}` on every endpoint.
7. **FastAPI** replaces Flask; native async, WebSocket push, Pydantic schemas.
8. **Zero-build vanilla JS** — ES modules, no bundler.
9. **Strategy abstraction** — pluggable per-strategy signals; not naked-buyer-only.
10. **No secrets in the project tree** — credentials under `%LOCALAPPDATA%\OChain\`.

---

## Phase 0 — Scaffold

> Set up the project skeleton, tooling, and dependencies. No logic yet.

- [x] **P0-1** Create `OChain_v2/` folder structure (all dirs, empty `__init__.py` files)
- [x] **P0-2** Write `pyproject.toml` with dependencies and optional extras `[dhan]`, `[dev]`
- [x] **P0-3** Write `requirements.txt` (pinned) and `requirements-dev.txt`
- [x] **P0-4** Write `.gitignore` (exclude `data/`, `*.duckdb`, `*.sqlite`, credentials, `.env`)
- [x] **P0-5** Write `.python-version` (3.12)
- [x] **P0-6** Write `.pre-commit-config.yaml` (ruff + mypy)
- [x] **P0-7** Write `config/settings.example.yaml` (ports, symbols, intervals, broker path)
- [x] **P0-8** Write `config/instruments.yaml` (NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY — lot size, tick, strike step)
- [x] **P0-9** Write `config/nse_holidays.yaml` (2025 + 2026 NSE holiday calendar)
- [x] **P0-10** Write `config/strategies.yaml` (which strategies are enabled by default)
- [x] **P0-11** Write `tests/conftest.py` scaffold (fixtures: in-memory DuckDB, sample DataFrame)
- [x] **P0-12** Verify: `python -m ochain_v2 --help` runs without error

---

## Phase 1 — Core & Database Layer

> Persistent storage, schema, migrations, v1 import, and analytical primitives.

### 1a — Core Utilities

- [x] **P1-1** `core/settings.py` — pydantic-settings loading YAML + env overrides
- [x] **P1-2** `core/timezones.py` — IST tz-aware `now_ist()`, `to_ist()`, `trade_date_ist(ts)`
- [x] **P1-3** `core/market_hours.py` — `is_market_open(dt)`, `next_open(dt)`, holiday lookup, `session_bounds(date)` returning (09:15, 15:30)
- [x] **P1-4** `core/ulid.py` — monotonic `snapshot_id` generator (sortable bigint)
- [x] **P1-5** `core/logging.py` — structured JSON logger (stdout + rotating file)
- [x] **P1-6** `core/errors.py` — typed exceptions: `BrokerError`, `IngestError`, `AnalysisError`
- [x] **P1-7** Unit tests for `market_hours.py` (open/closed/holiday/edge times)

### 1b — DuckDB Schema & Storage

- [x] **P1-8** `db/schema.sql` — full DDL: `instruments`, `expiries`, `snapshots`, `chain_rows`, `chain_deltas_base`, `chain_deltas_prev`, `chain_rollup_5m`, `chain_rollup_15m`
- [x] **P1-9** `db/duckdb_store.py` — writer class: `init_schema()`, `upsert_instrument()`, `save_snapshot(df, symbol, expiry, ts)` (transactional: writes `snapshots` + `chain_rows` + both delta tables in one txn)
- [x] **P1-10** `db/duckdb_reader.py` — read-only connection pool: `get_snapshots(symbol, expiry, date)`, `get_chain_rows(snapshot_id)`, `get_chain_rows_range(symbol, expiry, date, from_ts, to_ts)`, `get_delta_base(snapshot_id)`, `get_heatmap_matrix(symbol, expiry, date, metric, from_ts, to_ts)`
- [x] **P1-11** `db/meta_sqlite.py` — SQLite metadata: `collector_status`, `error_log`, `alert_events`, `user_views` tables; CRUD helpers
- [x] **P1-12** `db/migrations/` — migration runner that applies numbered `.sql` files in order; idempotent
- [x] **P1-13** Unit tests: ingest one snapshot, assert `chain_rows` count, assert `chain_deltas_prev` all-null for first row, assert `chain_deltas_base` correct vs `is_session_base`

### 1c — Delta Writer (inline at ingest)

- [x] **P1-14** `ingestion/delta_writer.py` — `compute_and_write_deltas(conn, snapshot_id)`: reads `chain_rows` for current and previous snapshot + session base, computes per-strike diffs, writes both delta tables; sets `ref_available` correctly

### 1d — Analyzer Primitives (pure functions, DataFrame in/out)

- [x] **P1-15** `analyzers/primitives.py` — `compute_delta(current_df, ref_df)`, `compute_pcr(df)`, `compute_atm(df, spot)`, `compute_max_pain(df)`, `compute_buildups(current_df, prev_df)`, `compute_support_resistance(df)`
- [x] **P1-16** `analyzers/greeks.py` — Black-Scholes: `bs_price`, `bs_delta`, `bs_gamma`, `bs_theta`, `bs_vega`, `bs_iv`; `compute_payoff(legs, spot_range)`, `compute_pop(legs, spot, iv, dte)`
- [x] **P1-17** `analyzers/gex.py` — `compute_gex(df, spot, lot_size)` with explicit gamma units; `gex_flip_point`, `gex_regime` with deadband
- [x] **P1-18** `analyzers/heatmap.py` — fully vectorized (no Python cell loop); `build_heatmap_matrix(chain_rows_df, metric)` returning `{strikes, timestamps, matrix}`
- [x] **P1-19** `analyzers/alerts.py` — incremental: `detect_alerts(current_df, prev_df, thresholds)` returns list of alert dicts; no full-day recompute
- [x] **P1-20** `analyzers/iv_surface.py` — `compute_iv_smile(df, expiry_date, spot)`, `compute_iv_surface(expiries_dict, spot)` with None-gap handling
- [x] **P1-21** `analyzers/rollover.py` — `detect_rollover(current_df, prev_df)` OI shift detection
- [x] **P1-22** `analyzers/expected_move.py` — IV+DTE formula; straddle fallback
- [x] **P1-23** Unit tests for every analyzer primitive against known inputs; Black-Scholes against tabulated values; max pain curve verification

### 1e — Strategy Layer

- [x] **P1-24** `analyzers/strategies/base.py` — `TradingStrategy` Protocol: `name`, `signals(ctx)`, `metrics(ctx)`, `description`; `AnalysisContext` dataclass
- [x] **P1-25** `analyzers/strategies/registry.py` — `register(strategy)`, `get(name)`, `list_all()`
- [x] **P1-26** `analyzers/strategies/naked_buyer.py` — ported from v1 scalper logic
- [x] **P1-27** `analyzers/strategies/naked_seller.py` — writer-positioning, IV harvest signals
- [x] **P1-28** `analyzers/strategies/straddle.py` — vol direction, event playbook
- [x] **P1-29** `analyzers/strategies/strangle.py`
- [x] **P1-30** `analyzers/strategies/spread.py` — bull/bear vertical analysis
- [x] **P1-31** `analyzers/strategies/iron_condor.py` — range-bound regime detection
- [x] **P1-32** `analyzers/strategies/gamma_scalper.py` — dealer gamma map signals

### 1f — Legacy Migration Tool

- [x] **P1-33** `db/legacy_sqlite.py` — reads v1 `ochain.db`, parses `raw_json` blobs, streams into DuckDB via `duckdb_store.save_snapshot()`
- [x] **P1-34** `cli/migrate.py` — `python -m ochain_v2 migrate --from /path/to/ochain.db` CLI command
- [x] **P1-35** Integration test: migrate v1 `ochain.db` → v2 DuckDB, assert row counts match

---

## Phase 2 — Collector / Ingestion Layer

> Market-hours-aware async polling for multiple instruments and expiries.

### 2a — Broker Abstraction

- [x] **P2-1** `ingestion/brokers/base.py` — `BrokerProtocol`: `connect()`, `disconnect()`, `get_expiries(symbol)`, `get_option_chain(symbol, expiry) -> pd.DataFrame`, `is_connected`
- [x] **P2-2** `ingestion/brokers/fixtures.py` — deterministic fake broker for tests; seeded RNG, broker-native column names, `get_expiries()` returns next 2 Thursdays
- [x] **P2-3** `ingestion/brokers/dhan.py` — adapter wrapping Tradehull; optional import; async via `run_in_executor`; per-day expiry cache; status fields behind lock
- [x] **P2-4** `ingestion/brokers/kite.py` — placeholder stub (raises `NotImplementedError`)

### 2b — Rate Limiting & Resilience

- [x] **P2-5** `ingestion/token_bucket.py` — async token-bucket rate limiter; lazy `asyncio.Lock`; configurable rate + burst
- [x] **P2-6** `ingestion/circuit_breaker.py` — per-key breaker (CLOSED/OPEN/HALF_OPEN); `@asynccontextmanager guard(key)`; auto OPEN→HALF_OPEN after timeout

### 2c — Scheduler & Job

- [x] **P2-7** `ingestion/market_hours.py` — thin wrapper re-exporting `core/market_hours.py`; adds `should_poll_now()` and `sleep_until_open()` coroutine
- [x] **P2-8** `ingestion/job.py` — `InstrumentExpiryJob`: token_bucket → circuit_breaker → get_option_chain → save_snapshot → publish; `_session_base_date` tracking; `run_loop(stop_event)` coroutine
- [x] **P2-9** `ingestion/scheduler.py` — `Collector`: asyncio.gather all jobs; market-hours gating via `sleep_until_open` + `_wait_for_close`; SIGTERM/SIGINT handling; `status()` method
- [x] **P2-10** `ingestion/live_publisher.py` — asyncio.Queue fan-out per subscriber; `publish(event)`, `subscribe()` async generator, `subscriber_count`
- [x] **P2-11** `ingestion/__main__.py` — entry point: loads settings, creates broker/store/publisher/collector, runs event loop; top-level `__main__._run_collector()` wired to it

### 2d — Replay Harness (off-hours dev/testing)

- [x] **P2-12** `ingestion/replay/csv_replay.py` — `CsvReplay.stream()` async generator; configurable replay speed; symbol/expiry filter
- [x] **P2-13** `ingestion/replay/parquet_replay.py` — `ParquetReplay.stream()` same contract; supports single file or directory of partitions
- [x] **P2-14** Integration tests: `FixtureBroker` + `InstrumentExpiryJob` + DuckDB; 8 tests verifying snapshot counts, delta population, session-base flag, live-event publish

---

## Phase 3 — API Layer

> FastAPI backend; v1-compatible routes + v2 enhancements; WebSocket live push.

### 3a — App Factory & Infrastructure

- [x] **P3-1** `api/main.py` — FastAPI factory: CORS, `/healthz`, all routers, lifespan (DuckDBReader + MetaDB + LivePublisher + ApiCache); `Cache-Control` middleware
- [x] **P3-2** `api/deps.py` — DI providers: `get_reader()`, `get_meta()`, `get_cache()`; `ReaderDep`/`MetaDep`/`CacheDep` annotated aliases
- [x] **P3-3** `api/cache.py` — `LRUCache` (OrderedDict + TTL + thread-safe lock); `ApiCache` (snapshot + window levels); `invalidate_symbol()`; module-level singleton
- [x] **P3-4** `api/schemas/responses.py` — Pydantic models: SnapshotRef, StrikeRow, AnalyzeResponse, HeatmapResponse, GexResponse, IvSurfaceResponse, ScalperResponse, PayoffRequest/Response, AlertEvent, SavedView, LiveSnapshotEvent, AlertPushEvent, all v2 types

### 3b — v1-Compatible Routes

- [x] **P3-5** No separate compat.py needed — all v1 paths implemented directly in per-topic routers mounted on the same prefix
- [x] **P3-6** `api/routes/chain.py` — `/api/symbols`, `/api/dates`, `/api/expiry_list`, `/api/snapshots`, `/api/analyze` with summary (PCR, ATM, GEX, SR, EM, IV smile) + window cache
- [x] **P3-7** `api/routes/heatmap.py` — `/api/heatmap/{symbol}?metric=` via `DuckDBReader.get_heatmap_matrix`
- [x] **P3-8** `api/routes/strike.py` — `/api/strike/{symbol}/{strike}` — full-day OI/IV time series at one strike
- [x] **P3-9** `api/routes/iv.py` — `/api/iv_surface/{symbol}` — smile + multi-expiry surface
- [x] **P3-10** `api/routes/gex.py` — `/api/gex/{symbol}` — GEX dict + per-strike gamma/OI breakdown
- [x] **P3-11** `api/routes/alerts.py` — `/api/alerts/{symbol}?limit=&since_id=`
- [x] **P3-12** `api/routes/scalper.py` — `/api/scalper/{symbol}?strategy=` — pluggable strategy signals via registry
- [x] **P3-13** `api/routes/strategy.py` — `POST /api/strategy/payoff` — payoff curve + POP + breakevens
- [x] **P3-14** `api/routes/expiry.py` — `/api/expiries/{symbol}`
- [x] **P3-15** `api/collector_api.py` — `/collector/api/status`, `/errors`, `/connect`, `/disconnect`, `/symbols` CRUD, `/expiries/{symbol}`
- [x] **P3-16** `Cache-Control: public, max-age=30` middleware in `main.py` for all GET 200s

### 3c — v2 New Routes

- [x] **P3-17** `api/routes/instruments.py` — `GET /api/v2/instruments` from `instruments.yaml`
- [x] **P3-18** `api/routes/session.py` — `GET /api/v2/session/{symbol}` — market_open + latest_snapshot + session_base
- [x] **P3-19** `api/routes/composite.py` — `GET /api/v2/composite/{symbol}` — chain + gex + scalper + alerts via `asyncio.gather`
- [x] **P3-20** `api/routes/strategies_list.py` — `GET /api/v2/strategies`, `GET /api/v2/strategies/{name}/signals/{symbol}`
- [x] **P3-21** `api/routes/export.py` — `GET /api/v2/export/{symbol}.csv` and `.parquet`
- [x] **P3-22** `api/routes/replay.py` — `GET /api/v2/replay/{symbol}/{date}?page=&page_size=` paginated snapshot list
- [x] **P3-23** `api/routes/views.py` — `GET/POST/DELETE /api/v2/views`

### 3d — WebSocket / Live Push

- [x] **P3-24** `api/ws/live.py` — `WS /ws/live/{symbol}` — subscribes to `LivePublisher`, filters by symbol, handles disconnect
- [x] **P3-25** `api/ws/alerts.py` — `WS /ws/alerts` — polls `MetaDB.get_alerts(since_id=)` every 2 s, pushes new rows to all clients
- [x] **P3-26** Integration tests: `test_ws_live.py` — 10 snapshots → 10 events in order; symbol-filter test; 368 total tests passing

---

## Phase 4 — Parallel Run & Verification

> Run v1 and v2 side-by-side; prove no functional regressions before touching the UI.

- [x] **P4-1** `scripts/run_api.ps1` — starts uvicorn on port 5051
- [x] **P4-2** `scripts/run_collector.ps1` — starts ingestion process
- [x] **P4-3** `scripts/compare_v1_v2.py` — picks N random `(symbol, expiry, ts)` triples from v1 DB; calls v1 `:5050/api/analyze` and v2 `:5051/api/analyze`; diffs JSON with declared tolerances; reports pass/fail
- [x] **P4-4** Run A/B verification against migrated v1 data; fix any discrepancies (5/5 PASS after Timestamp serialization fix)
- [x] **P4-5** Load test: `scripts/locustfile.py` — 5 concurrent users, chain+heatmap+gex tasks, p99 < 200ms hook; run with `locust -f scripts/locustfile.py --headless -u 5 -r 1 --run-time 60s --host http://localhost:5051`

---

## Phase 5 — Frontend (Clean Rebuild)

> Full rewrite of the UI as ES modules with the same design language as v1, but with proper
> loading states, cancellable fetches, WebSocket live mode, strategy switcher, and responsive
> layout. No bundler — vanilla JS with `type="module"` scripts only.
>
> **File layout**
> ```
> static/
>   css/style.css          ← ported design tokens + skeleton + toast + responsive improvements
>   js/
>     core/state.js        ← reactive pub/sub state; localStorage persistence
>     core/api.js          ← fetch wrapper: AbortController, non-200 errors, JSON parse errors
>     core/ws.js           ← WebSocket manager: auto-reconnect, symbol filter, onSnapshot/onAlert
>     components/
>       header.js          ← symbol pills, expiry/date/TF selects, live-toggle, theme button
>       slider.js          ← dual-thumb range slider, play mode, timeline ticks
>       summary.js         ← summary card updates from chain data
>       skeleton.js        ← showSkeleton(el, rows)/hideSkeleton(el) helpers
>       error-state.js     ← showError(el, msg, retryFn)/clearError(el)
>       alert-toast.js     ← WS-driven toast queue (auto-dismiss, click-to-jump)
>     tabs/
>       chain.js           ← OI charts + chain table (click row → strike drill)
>       flow.js            ← GEX/DEX charts + key levels + unusual activity
>       heatmap.js         ← canvas heatmap (OI Change / OI / IV), click → strike drill
>       volume.js          ← volume heatmap + insights panel
>       strike.js          ← 4 line charts + buildup timeline; strike selector
>       iv.js              ← IV smile + ATM IV intraday + Plotly 3D surface
>       expiry.js          ← multi-expiry OI bar + per-expiry table + rollover
>       strategy.js        ← leg builder + payoff chart + Greeks
>       scalper.js         ← mode selector + market read + signals + COI flow + writer map
>       signals.js         ← strategy-aware 4-column signals board
>     app.js               ← boot: wires state → header → slider → tabs → WS
>   index.html             ← single HTML file; all scripts type="module"
> ```

### 5a — Shell & CSS

- [x] **P5-1** `static/css/style.css` — port all v1 design tokens (CSS vars, dark/light themes, chart containers, chain table, cards, slider, tabs); add: skeleton pulse animation, toast stack, live-mode indicator, loading overlay; improve responsive to 3 breakpoints (1200px tablet, 768px mobile)
- [x] **P5-2** `static/index.html` — single HTML shell; header with symbol pills + controls; summary cards section; slider section; 10 tab buttons + tab panes; `<script type="module" src="/static/js/app.js">`; no inline scripts except FOUC-prevention theme snippet

### 5b — Core JS Layer

- [x] **P5-3** `static/js/core/state.js` — `State` class: `get(key)`, `set(key, val)`, `subscribe(key, cb)`, `unsubscribe`; `localStorage` persistence for `symbol/expiry/date/activeTab/timeframe`; cross-tab symbol-state map so slider position is remembered per instrument
- [x] **P5-4** `static/js/core/api.js` — `apiFetch(url, signal?)`: throws `ApiError(status, message)` on non-200; throws on network error; returns parsed JSON; `buildAnalyzeUrl(state)`, `buildHeatmapUrl(state, metric)` helpers
- [x] **P5-5** `static/js/core/ws.js` — `WsManager`: connects to `/ws/live/{symbol}` on `connect(symbol)`; exponential back-off reconnect (1s→2s→4s→max 30s); `onSnapshot(cb)`, `onAlert(cb)`, `disconnect()`; exposes `isConnected` reactive property

### 5c — Shared Components

- [x] **P5-6** `static/js/components/skeleton.js` — `showSkeleton(container, rows, cols?)` injects pulsing placeholder rows; `hideSkeleton(container)` removes them; used by every tab before data arrives
- [x] **P5-7** `static/js/components/error-state.js` — `showError(container, message, retryFn)` replaces content with error card + Retry button; `clearError(container)`
- [x] **P5-8** `static/js/components/summary.js` — `updateSummary(data)` writes all 10 summary card values; `updateBiasBanner(bias)`; `clearSummary()`
- [x] **P5-9** `static/js/components/alert-toast.js` — `ToastQueue`: max 5 visible; auto-dismiss after 6s; click handler calls `state.set('jumpTo', {strike, ts})`; subscribes to `ws.onAlert`

### 5d — Header & Slider

- [x] **P5-10** `static/js/components/header.js` — symbol pills populated from `/api/symbols`; expiry select from `/api/expiry_list`; date select from `/api/dates`; TF select (1m/5m/15m/30m/1h); live-toggle button (hidden outside market hours, glowing green when active); theme toggle; every change writes to `state` which triggers cascade
- [x] **P5-11** `static/js/components/slider.js` — dual-thumb range slider; `AbortController` per drag so only the last released position fires a full fetch; play-mode auto-advance (1500ms); reset-base button; timeline tick rendering with base/in-range colors

### 5e — Tabs (all use skeleton → fetch → render or error)

- [x] **P5-12** `static/js/tabs/chain.js` — OI Distribution + OI Change + IV Skew charts (Chart.js); chain table with OI bars, delta, buildup badges; max-pain + ATM row highlights; click row → `state.set('strikeDrillTarget', strike)` → switch to Strike tab; CSV export button
- [x] **P5-13** `static/js/tabs/flow.js` — GEX/DEX horizontal bar charts; key levels panel (walls, flip, 1σ/2σ ranges); unusual activity alerts feed
- [x] **P5-14** `static/js/tabs/heatmap.js` — canvas heatmap (OI Change / OI / IV mode selector); click cell → `state.set('strikeDrillTarget', {strike, ts})` + switch tab; tooltip overlay
- [x] **P5-15** `static/js/tabs/volume.js` — volume heatmap (CE/PE side-by-side); insights panel (totals, ratio, top 5 spikes)
- [x] **P5-16** `static/js/tabs/strike.js` — strike selector dropdown (from chain data); 4 line charts (OI, Volume, LTP, IV); buildup timeline bar
- [x] **P5-17** `static/js/tabs/iv.js` — IV smile line chart (per expiry); ATM IV intraday chart; Plotly 3D surface (lazy-load Plotly from CDN only when tab first activated)
- [x] **P5-18** `static/js/tabs/expiry.js` — multi-expiry OI stacked bar; per-expiry summary table (PCR, max pain, support, resistance); rollover activity table
- [x] **P5-19** `static/js/tabs/strategy.js` — strategy template buttons; leg builder (add/remove rows); spot/IV/DTE inputs; POST to `/api/strategy/payoff`; payoff chart; Greeks + breakeven table
- [x] **P5-20** `static/js/tabs/scalper.js` — mode selector (CE Buy / PE Buy / CE Sell / PE Sell); market-read indicator; signals list with strength coloring; top strikes table; COI flow bar chart; writer positioning table
- [x] **P5-21** `static/js/tabs/signals.js` — strategy selector dropdown (from `/api/v2/strategies`); 4-column signals board; each column shows dominance strikes + signal cards; updates when strategy changes without page reload

### 5f — Live Mode & Polish

- [x] **P5-22** `static/js/app.js` — boot sequence: restore state from localStorage → init header → load snapshots → init slider → init all tabs → connect WS if live-toggle on; subscribe to `state` changes to cascade header → slider → active tab reload; subscribe to `ws.onSnapshot` → append to snapshots + if live-toggle on advance `toIdx` + reload active tab
- [x] **P5-23** Serve static files from FastAPI: add `StaticFiles` mount at `/static` in `api/main.py`; add template route `GET /` → `index.html`; add `GET /collector` → `collector.html` (port v1 collector UI)

---

## Phase 6 — Production Hardening

> Make it deployable, observable, and maintainable.

### 6a — Operations

- [ ] **P6-1** `api/routes/health.py` — `/healthz` (DuckDB readable, collector process alive, last snapshot age); `/readyz`
- [ ] **P6-2** `core/metrics.py` — Prometheus counters: `snapshots_ingested_total`, `api_request_duration_seconds`, `broker_errors_total`, `active_ws_connections`; expose at `/metrics`
- [ ] **P6-3** Structured JSON logging throughout (ingestion + API); log level from settings
- [ ] **P6-4** `scripts/run_api.ps1` — production uvicorn command (workers=2, no reload, bind 0.0.0.0:5051)
- [ ] **P6-5** `scripts/run_collector.ps1` — collector startup with env-based settings override

### 6b — Data Archival

- [ ] **P6-6** `db/archival.py` — `archive_before(date)`: exports `chain_rows` + `snapshots` to `data/archive/<year>/<month>.parquet` then DELETEs from DuckDB
- [ ] **P6-7** `cli/archive.py` — `python -m ochain_v2 archive --before 2026-01-01` CLI command
- [ ] **P6-8** Integration test: archive 30 days, assert DuckDB smaller, assert Parquet files readable via DuckDB external scan

### 6c — Developer & Ops Tooling

- [ ] **P6-9** `cli/doctor.py` — `python -m ochain_v2 doctor`: checks DB integrity, last snapshot age, DuckDB WAL, credentials file presence
- [ ] **P6-10** `cli/seed.py` — `python -m ochain_v2 seed`: imports bundled `option_chain.csv` for demo/testing (port of v1 `seed_demo_data`)
- [ ] **P6-11** `scripts/archive_old.ps1` — daily scheduled archival wrapper
- [ ] **P6-12** README with: quickstart, config reference, broker setup, running in dev vs production

---

## Phase 7 — Decommission v1 (deferred)

> After v2 runs stably for one full month with zero manual interventions.

- [ ] **P7-1** Freeze v1 — add a `DEPRECATED` notice to `OChain/README.md`
- [ ] **P7-2** Point any external links / docs to v2
- [ ] **P7-3** Remove Dhan credentials from v1 working tree permanently

---

## Milestone Summary

| Milestone | Deliverable | Phases |
|-----------|-------------|--------|
| **M0** | Runnable scaffold, no logic | P0 |
| **M1** | Data layer proven: ingest + query + migrate from v1 | P1 |
| **M2** | Collector running live against Dhan in market hours | P2 |
| **M3** | Full API; existing v1 frontend plugs into v2 unchanged | P3 |
| **M4** | A/B verified: zero functional regressions | P4 |
| **M5** | Enhanced UI; multi-instrument + live mode | P5 |
| **M6** | Production-deployable; observable; archival running | P6 |

---

## Key Technical Decisions Log

| Decision | Choice | Reason |
|----------|--------|--------|
| OLAP store | DuckDB | Embedded, columnar, zero-ops, native Arrow/pandas, handles 7 GB/year |
| Metadata store | SQLite | Tiny OLTP tables (status, alerts, views); no time-series |
| API framework | FastAPI | Native async, WebSocket, Pydantic, auto OpenAPI |
| Async model | asyncio + ThreadPoolExecutor | Broker SDK is sync; I/O wait dominates; avoids GIL for wait |
| Scheduler | APScheduler (asyncio) | Market-hours-aware pause/resume; per-job backoff |
| Rate limiting | Token bucket (in-process) | Dhan ~5 req/s; replaces global lock + sleep |
| JS architecture | Vanilla ES modules | No build step; zero new tooling; existing v1 style |
| Credentials | `%LOCALAPPDATA%\OChain\` | Never in project tree |
| Delta storage | Pre-computed at ingest | Read path never recomputes; serves 5 concurrent traders without CPU spike |
| Rollup granularity | 5m and 15m pre-materialized | Most common trader timeframes; 1m is raw, 30m+ served by query |

---

## Security Checklist (Day 1)

- [ ] Rotate live Dhan access token (current one is exposed in v1 working tree)
- [ ] Move credentials to `%LOCALAPPDATA%\OChain\credentials.json`
- [ ] Collector API (`/collector/api/*`) bound to localhost only by default
- [ ] No `debug=True` anywhere in v2
- [ ] `.gitignore` excludes `data/`, `*.duckdb`, `*.sqlite`, `.env`, `credentials*`
