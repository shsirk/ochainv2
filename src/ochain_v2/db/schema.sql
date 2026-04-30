-- OChain v2 — DuckDB schema
-- All CREATE statements use IF NOT EXISTS so this file is idempotent
-- and safe to run on first init or after a failed partial migration.

-- -----------------------------------------------------------------------
-- Reference data
-- -----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS instruments (
    symbol       VARCHAR     PRIMARY KEY,
    exchange     VARCHAR     NOT NULL DEFAULT 'NSE',
    lot_size     INTEGER     NOT NULL,
    tick_size    DOUBLE      NOT NULL,
    strike_step  DOUBLE      NOT NULL,
    num_strikes  INTEGER     NOT NULL DEFAULT 20,
    is_index     BOOLEAN     NOT NULL DEFAULT TRUE,
    active       BOOLEAN     NOT NULL DEFAULT TRUE,
    metadata     JSON
);

CREATE TABLE IF NOT EXISTS expiries (
    symbol          VARCHAR     NOT NULL,
    expiry_date     DATE        NOT NULL,
    expiry_type     VARCHAR     NOT NULL DEFAULT 'WEEKLY',   -- WEEKLY | MONTHLY | QUARTERLY
    first_seen_at   TIMESTAMPTZ NOT NULL,
    last_seen_at    TIMESTAMPTZ,
    active          BOOLEAN     NOT NULL DEFAULT TRUE,
    PRIMARY KEY (symbol, expiry_date)
);

-- -----------------------------------------------------------------------
-- Snapshot header (1 row per broker fetch)
-- -----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS snapshots (
    snapshot_id      BIGINT      PRIMARY KEY,
    symbol           VARCHAR     NOT NULL,
    expiry_date      DATE        NOT NULL,
    ts               TIMESTAMPTZ NOT NULL,
    trade_date       DATE        NOT NULL,
    -- Minutes offset from 09:15 IST; negative for pre-open captures
    bucket_1m        INTEGER     NOT NULL,
    underlying_ltp   DOUBLE,
    is_session_base  BOOLEAN     NOT NULL DEFAULT FALSE,
    source           VARCHAR     NOT NULL DEFAULT 'dhan',
    ingested_at      TIMESTAMPTZ NOT NULL,
    UNIQUE (symbol, expiry_date, ts)
);

CREATE INDEX IF NOT EXISTS ix_snapshots_lookup
    ON snapshots (symbol, expiry_date, trade_date);

-- -----------------------------------------------------------------------
-- Strike-level rows (the time-series body)
-- bucket_1m denormalised here to avoid joins in heatmap / rollup queries
-- -----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS chain_rows (
    snapshot_id  BIGINT      NOT NULL,
    symbol       VARCHAR     NOT NULL,
    expiry_date  DATE        NOT NULL,
    ts           TIMESTAMPTZ NOT NULL,
    trade_date   DATE        NOT NULL,
    bucket_1m    INTEGER     NOT NULL,
    strike       DOUBLE      NOT NULL,
    -- CE side
    ce_oi        BIGINT,
    ce_volume    BIGINT,
    ce_ltp       DOUBLE,
    ce_iv        DOUBLE,
    ce_bid       DOUBLE,
    ce_ask       DOUBLE,
    ce_bid_qty   INTEGER,
    ce_ask_qty   INTEGER,
    ce_delta     DOUBLE,
    ce_gamma     DOUBLE,
    ce_theta     DOUBLE,
    ce_vega      DOUBLE,
    -- PE side
    pe_oi        BIGINT,
    pe_volume    BIGINT,
    pe_ltp       DOUBLE,
    pe_iv        DOUBLE,
    pe_bid       DOUBLE,
    pe_ask       DOUBLE,
    pe_bid_qty   INTEGER,
    pe_ask_qty   INTEGER,
    pe_delta     DOUBLE,
    pe_gamma     DOUBLE,
    pe_theta     DOUBLE,
    pe_vega      DOUBLE,
    PRIMARY KEY (snapshot_id, strike)
);

CREATE INDEX IF NOT EXISTS ix_chain_rows_lookup
    ON chain_rows (symbol, expiry_date, trade_date, ts);

CREATE INDEX IF NOT EXISTS ix_chain_rows_strike
    ON chain_rows (symbol, expiry_date, trade_date, strike, ts);

-- -----------------------------------------------------------------------
-- Pre-computed deltas vs session base (first snapshot of day)
-- Written at ingest time — read path never recomputes these.
-- -----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS chain_deltas_base (
    snapshot_id   BIGINT      NOT NULL,
    symbol        VARCHAR     NOT NULL,
    expiry_date   DATE        NOT NULL,
    ts            TIMESTAMPTZ NOT NULL,
    strike        DOUBLE      NOT NULL,
    ce_oi_chg     BIGINT,
    ce_vol_chg    BIGINT,
    ce_ltp_chg    DOUBLE,
    ce_iv_chg     DOUBLE,
    pe_oi_chg     BIGINT,
    pe_vol_chg    BIGINT,
    pe_ltp_chg    DOUBLE,
    pe_iv_chg     DOUBLE,
    -- FALSE when no session-base snapshot exists yet (e.g. first fetch of day)
    ref_available BOOLEAN     NOT NULL DEFAULT FALSE,
    PRIMARY KEY (snapshot_id, strike)
);

-- -----------------------------------------------------------------------
-- Pre-computed deltas vs previous 1-minute snapshot
-- -----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS chain_deltas_prev (
    snapshot_id   BIGINT      NOT NULL,
    symbol        VARCHAR     NOT NULL,
    expiry_date   DATE        NOT NULL,
    ts            TIMESTAMPTZ NOT NULL,
    strike        DOUBLE      NOT NULL,
    ce_oi_chg     BIGINT,
    ce_vol_chg    BIGINT,
    ce_ltp_chg    DOUBLE,
    ce_iv_chg     DOUBLE,
    pe_oi_chg     BIGINT,
    pe_vol_chg    BIGINT,
    pe_ltp_chg    DOUBLE,
    pe_iv_chg     DOUBLE,
    ref_available BOOLEAN     NOT NULL DEFAULT FALSE,
    PRIMARY KEY (snapshot_id, strike)
);

-- -----------------------------------------------------------------------
-- Rollup views (DuckDB views are zero-copy — no separate materialisation)
-- Picks the LAST snapshot in each N-minute bucket per (symbol, expiry, date, strike)
-- -----------------------------------------------------------------------

CREATE OR REPLACE VIEW chain_rollup_5m AS
WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY symbol, expiry_date, trade_date, strike,
                            (bucket_1m / 5)
               ORDER BY ts DESC
           ) AS _rn
    FROM chain_rows
)
SELECT
    snapshot_id, symbol, expiry_date, ts, trade_date, bucket_1m, strike,
    ce_oi, ce_volume, ce_ltp, ce_iv, ce_bid, ce_ask, ce_bid_qty, ce_ask_qty,
    ce_delta, ce_gamma, ce_theta, ce_vega,
    pe_oi, pe_volume, pe_ltp, pe_iv, pe_bid, pe_ask, pe_bid_qty, pe_ask_qty,
    pe_delta, pe_gamma, pe_theta, pe_vega
FROM ranked
WHERE _rn = 1;

CREATE OR REPLACE VIEW chain_rollup_15m AS
WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY symbol, expiry_date, trade_date, strike,
                            (bucket_1m / 15)
               ORDER BY ts DESC
           ) AS _rn
    FROM chain_rows
)
SELECT
    snapshot_id, symbol, expiry_date, ts, trade_date, bucket_1m, strike,
    ce_oi, ce_volume, ce_ltp, ce_iv, ce_bid, ce_ask, ce_bid_qty, ce_ask_qty,
    ce_delta, ce_gamma, ce_theta, ce_vega,
    pe_oi, pe_volume, pe_ltp, pe_iv, pe_bid, pe_ask, pe_bid_qty, pe_ask_qty,
    pe_delta, pe_gamma, pe_theta, pe_vega
FROM ranked
WHERE _rn = 1;

-- -----------------------------------------------------------------------
-- Migration tracking (applied by db/migrations/__init__.py)
-- -----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS _migrations (
    name        VARCHAR     PRIMARY KEY,
    applied_at  TIMESTAMP   NOT NULL
);
