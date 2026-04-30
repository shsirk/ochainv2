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

- [ ] **P1-15** `analyzers/primitives.py` — `compute_delta(current_df, ref_df)`, `compute_pcr(df)`, `compute_atm(df, spot)`, `compute_max_pain(df)`, `compute_buildups(current_df, prev_df)`, `compute_support_resistance(df)`
- [ ] **P1-16** `analyzers/greeks.py` — Black-Scholes: `bs_price`, `bs_delta`, `bs_gamma`, `bs_theta`, `bs_vega`, `bs_iv`; `compute_payoff(legs, spot_range)`, `compute_pop(legs, spot, iv, dte)`
- [ ] **P1-17** `analyzers/gex.py` — `compute_gex(df, spot, lot_size)` with explicit gamma units; `gex_flip_point`, `gex_regime` with deadband
- [ ] **P1-18** `analyzers/heatmap.py` — fully vectorized (no Python cell loop); `build_heatmap_matrix(chain_rows_df, metric)` returning `{strikes, timestamps, matrix}`
- [ ] **P1-19** `analyzers/alerts.py` — incremental: `detect_alerts(current_df, prev_df, thresholds)` returns list of alert dicts; no full-day recompute
- [ ] **P1-20** `analyzers/iv_surface.py` — `compute_iv_smile(df, expiry_date, spot)`, `compute_iv_surface(expiries_dict, spot)` with None-gap handling
- [ ] **P1-21** `analyzers/rollover.py` — `detect_rollover(current_df, prev_df)` OI shift detection
- [ ] **P1-22** `analyzers/expected_move.py` — IV+DTE formula; straddle fallback
- [ ] **P1-23** Unit tests for every analyzer primitive against known inputs; Black-Scholes against tabulated values; max pain curve verification

### 1e — Strategy Layer

- [ ] **P1-24** `analyzers/strategies/base.py` — `TradingStrategy` Protocol: `name`, `signals(ctx)`, `metrics(ctx)`, `description`; `AnalysisContext` dataclass
- [ ] **P1-25** `analyzers/strategies/registry.py` — `register(strategy)`, `get(name)`, `list_all()`
- [ ] **P1-26** `analyzers/strategies/naked_buyer.py` — ported from v1 scalper logic
- [ ] **P1-27** `analyzers/strategies/naked_seller.py` — writer-positioning, IV harvest signals
- [ ] **P1-28** `analyzers/strategies/straddle.py` — vol direction, event playbook
- [ ] **P1-29** `analyzers/strategies/strangle.py`
- [ ] **P1-30** `analyzers/strategies/spread.py` — bull/bear vertical analysis
- [ ] **P1-31** `analyzers/strategies/iron_condor.py` — range-bound regime detection
- [ ] **P1-32** `analyzers/strategies/gamma_scalper.py` — dealer gamma map signals

### 1f — Legacy Migration Tool

- [ ] **P1-33** `db/legacy_sqlite.py` — reads v1 `ochain.db`, parses `raw_json` blobs, streams into DuckDB via `duckdb_store.save_snapshot()`
- [ ] **P1-34** `cli/migrate.py` — `python -m ochain_v2 migrate --from /path/to/ochain.db` CLI command
- [ ] **P1-35** Integration test: migrate v1 `ochain.db` → v2 DuckDB, assert row counts match

---

## Phase 2 — Collector / Ingestion Layer

> Market-hours-aware async polling for multiple instruments and expiries.

### 2a — Broker Abstraction

- [ ] **P2-1** `ingestion/brokers/base.py` — `BrokerProtocol`: `connect()`, `disconnect()`, `get_expiries(symbol)`, `get_option_chain(symbol, expiry) -> pd.DataFrame`, `is_connected`
- [ ] **P2-2** `ingestion/brokers/fixtures.py` — deterministic fake broker for tests; deterministic chain from seeded CSV
- [ ] **P2-3** `ingestion/brokers/dhan.py` — adapter wrapping Tradehull; implements `BrokerProtocol`; expiry cache with date-aware TTL; status fields behind a lock; atomic credential file write
- [ ] **P2-4** `ingestion/brokers/kite.py` — placeholder stub (raises `NotImplementedError`)

### 2b — Rate Limiting & Resilience

- [ ] **P2-5** `ingestion/token_bucket.py` — async token-bucket rate limiter; configurable per-broker rate (e.g. Dhan: 5 req/s)
- [ ] **P2-6** `ingestion/circuit_breaker.py` — per-`(symbol, expiry)` breaker; 5 consecutive failures → 5-minute pause + error log entry

### 2c — Scheduler & Job

- [ ] **P2-7** `ingestion/market_hours.py` — thin wrapper that extends `core/market_hours.py` with scheduler-friendly `seconds_until_open()`, `seconds_until_close()`
- [ ] **P2-8** `ingestion/job.py` — `InstrumentExpiryJob` async coroutine: fetch → validate DataFrame → `duckdb_store.save_snapshot()` → `delta_writer.compute_and_write_deltas()` → publish live event; respects token bucket and circuit breaker
- [ ] **P2-9** `ingestion/scheduler.py` — APScheduler or `asyncio` scheduler; loads instrument config; creates one job per `(symbol, expiry)`; pauses all jobs outside market hours; resumes at next open; handles SIGTERM gracefully (finish in-flight fetches, flush WAL, exit)
- [ ] **P2-10** `ingestion/live_publisher.py` — `publish(symbol, expiry, snapshot_id)` — writes to in-mem `asyncio.Queue` (single-process) or Redis channel (multi-process); API WebSocket subscribers read from this
- [ ] **P2-11** `ingestion/__main__.py` — entry point: loads settings, initializes broker, starts scheduler event loop

### 2d — Replay Harness (off-hours dev/testing)

- [ ] **P2-12** `ingestion/replay/csv_replay.py` — feed rows from a CSV at configurable speed; mimics live polling
- [ ] **P2-13** `ingestion/replay/parquet_replay.py` — feed from archived Parquet partitions
- [ ] **P2-14** Integration test: replay 1 day of v1 CSV → DuckDB → assert 375 snapshots, correct deltas

---

## Phase 3 — API Layer

> FastAPI backend; v1-compatible routes + v2 enhancements; WebSocket live push.

### 3a — App Factory & Infrastructure

- [ ] **P3-1** `api/main.py` — FastAPI factory: CORS, `/healthz`, mounts all routers, lifespan for DB pool init/teardown
- [ ] **P3-2** `api/deps.py` — DI providers: `get_db_reader()`, `get_meta_db()`, `get_cache()`
- [ ] **P3-3** `api/cache.py` — two-level LRU: per-snapshot sidecar (key=`snapshot_id`) + per-window (key=`(symbol,expiry,date,from_idx,to_idx,tf,endpoint)`); invalidation hook on new ingest
- [ ] **P3-4** `api/schemas/` — Pydantic response models for every endpoint (chain, gex, heatmap, strike, iv, alerts, scalper, strategy, ws events)

### 3b — v1-Compatible Routes

- [ ] **P3-5** `api/routes/compat.py` — registers all v1 paths so the existing frontend works unchanged
- [ ] **P3-6** `api/routes/chain.py` — `/api/analyze/{symbol}` — returns chain payload + summary; reads from `chain_rows` + `chain_deltas_base`
- [ ] **P3-7** `api/routes/heatmap.py` — `/api/heatmap/{symbol}?metric=` — calls vectorized `analyzers/heatmap.py`
- [ ] **P3-8** `api/routes/strike.py` — `/api/strike/{symbol}/{strike}`
- [ ] **P3-9** `api/routes/iv.py` — `/api/iv_surface/{symbol}`
- [ ] **P3-10** `api/routes/gex.py` — `/api/gex/{symbol}`
- [ ] **P3-11** `api/routes/alerts.py` — `/api/alerts/{symbol}` — reads from `alert_events` in meta SQLite
- [ ] **P3-12** `api/routes/scalper.py` — `/api/scalper/{symbol}`
- [ ] **P3-13** `api/routes/strategy.py` — `POST /api/strategy/payoff`
- [ ] **P3-14** `api/routes/expiry.py` — `/api/expiries/{symbol}`, `/api/dates/{symbol}`, `/api/snapshots/{symbol}`
- [ ] **P3-15** `api/collector_api.py` — `/collector/api/start`, `/stop`, `/status`, `/connect`, `/disconnect` (localhost-only bind enforced in settings)
- [ ] **P3-16** Add `Cache-Control` and `ETag` headers to all read-only GET routes

### 3c — v2 New Routes

- [ ] **P3-17** `api/routes/instruments.py` — `GET /api/v2/instruments` (full metadata from `instruments.yaml`)
- [ ] **P3-18** `api/routes/session.py` — `GET /api/v2/session/{symbol}` (session base, current snapshot, market-hours state, last poll time)
- [ ] **P3-19** `api/routes/composite.py` — `GET /api/v2/composite/{symbol}` (chain + gex + scalper + alerts in one round-trip; fanned out internally via `asyncio.gather`)
- [ ] **P3-20** `api/routes/strategies_list.py` — `GET /api/v2/strategies`, `GET /api/v2/strategies/{name}/signals/{symbol}`
- [ ] **P3-21** `api/routes/export.py` — `GET /api/v2/export/{symbol}.csv` and `.parquet`
- [ ] **P3-22** `api/routes/replay.py` — `GET /api/v2/replay/{symbol}/{date}` (returns full day as paginated snapshots for back-test harness)
- [ ] **P3-23** `api/routes/views.py` — `GET/POST/DELETE /api/v2/views` (saved trader views; persisted in meta SQLite)

### 3d — WebSocket / Live Push

- [ ] **P3-24** `api/ws/live.py` — `WS /ws/live/{symbol}` — subscribes to `live_publisher` queue; pushes `{symbol, expiry, ts, snapshot_id, summary}` to all connected clients on new snapshot; drops connection outside market hours
- [ ] **P3-25** `api/ws/alerts.py` — `WS /ws/alerts` — pushes new `alert_events` rows as they're written; all symbols
- [ ] **P3-26** Integration test: replay 10 snapshots via fixture broker → assert WS client receives 10 push events in order

---

## Phase 4 — Parallel Run & Verification

> Run v1 and v2 side-by-side; prove no functional regressions before touching the UI.

- [ ] **P4-1** `scripts/run_api.ps1` — starts uvicorn on port 5051
- [ ] **P4-2** `scripts/run_collector.ps1` — starts ingestion process
- [ ] **P4-3** `scripts/compare_v1_v2.py` — picks N random `(symbol, expiry, ts)` triples from v1 DB; calls v1 `:5050/api/analyze` and v2 `:5051/api/analyze`; diffs JSON with declared tolerances; reports pass/fail
- [ ] **P4-4** Run A/B verification against migrated v1 data; fix any discrepancies
- [ ] **P4-5** Load test: `locust` or `wrk` with 5 concurrent users, heatmap + chain tabs; target p99 < 200 ms

---

## Phase 5 — Frontend Enhancements

> Enhance the existing UI for multi-instrument, live mode, and strategy switcher.

### 5a — Core JS Refactor

- [ ] **P5-1** Reorganize `static/js/` to ES-module structure (`type="module"` in HTML, no bundler)
- [ ] **P5-2** `js/core/state.js` — centralized app state with `localStorage` persistence keyed by `(symbol, expiry, date)`; slider position survives instrument/date switch
- [ ] **P5-3** `js/core/api.js` — typed fetch client with error handling; replaces raw `OC.fetchJSON`; catches non-200 and parse errors
- [ ] **P5-4** `js/core/ws.js` — WebSocket manager for `/ws/live/{symbol}` and `/ws/alerts`; auto-reconnect on drop; exposes `onSnapshot(cb)` and `onAlert(cb)` hooks
- [ ] **P5-5** `js/components/error-boundary.js` — per-tab error display replacing silent failures

### 5b — Multi-Instrument & Live

- [ ] **P5-6** `js/components/header.js` — symbol pills (NIFTY / BANKNIFTY / FINNIFTY / MIDCPNIFTY) in top bar; switching persists slider position per instrument
- [ ] **P5-7** `js/components/live-toggle.js` — visible only during market hours; when ON, subscribes to WS and auto-advances `to_idx` on each new snapshot
- [ ] **P5-8** `js/components/alert-toast.js` — subscribes to `/ws/alerts`; shows toast per alert; click jumps to that strike + timestamp

### 5c — Strategy & Analysis UX

- [ ] **P5-9** `js/components/strategy-switcher.js` — dropdown over strategies from `GET /api/v2/strategies`; changes which strategy's signals are shown in the Signals tab
- [ ] **P5-10** `js/tabs/signals.js` — new Signals tab backed by `/api/v2/strategies/{name}/signals/{symbol}`
- [ ] **P5-11** `js/components/saved-views.js` — save/restore named views (symbol + expiry + slider + tab) via `/api/v2/views`
- [ ] **P5-12** Update heatmap tab to cross-link cells to Strike Drill tab (click cell → open strike drill at that strike + ts)
- [ ] **P5-13** Add CSV export button to chain table → calls `/api/v2/export/{symbol}.csv`

### 5d — Multi-Expiry Compare

- [ ] **P5-14** `js/tabs/expiry.js` — side-by-side two-expiry chain comparison view backed by existing expiry API

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
