-- Reference SQL schema for the signal bot.
-- Tables are created via SQLAlchemy metadata in production; this file
-- documents the intended schema and can be used for ad-hoc migrations.

CREATE TABLE IF NOT EXISTS instruments (
    symbol VARCHAR(20) PRIMARY KEY,
    base_asset VARCHAR(10) NOT NULL,
    quote_asset VARCHAR(10) NOT NULL,
    tick_size NUMERIC(32, 16) NOT NULL,
    min_qty NUMERIC(32, 16) NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS candles_1m (
    symbol VARCHAR(20) NOT NULL,
    open_time TIMESTAMPTZ NOT NULL,
    open NUMERIC(32, 16) NOT NULL,
    high NUMERIC(32, 16) NOT NULL,
    low NUMERIC(32, 16) NOT NULL,
    close NUMERIC(32, 16) NOT NULL,
    volume NUMERIC(32, 16) NOT NULL,
    quote_volume NUMERIC(32, 16) NOT NULL,
    trade_count INTEGER NOT NULL,
    taker_buy_volume NUMERIC(32, 16) NOT NULL,
    closed BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (symbol, open_time)
);

CREATE INDEX IF NOT EXISTS idx_candles_1m_symbol_open_time
    ON candles_1m (symbol, open_time);

CREATE TABLE IF NOT EXISTS candles_5m (
    symbol VARCHAR(20) NOT NULL,
    open_time TIMESTAMPTZ NOT NULL,
    open NUMERIC(32, 16) NOT NULL,
    high NUMERIC(32, 16) NOT NULL,
    low NUMERIC(32, 16) NOT NULL,
    close NUMERIC(32, 16) NOT NULL,
    volume NUMERIC(32, 16) NOT NULL,
    quote_volume NUMERIC(32, 16) NOT NULL,
    trade_count INTEGER NOT NULL,
    taker_buy_volume NUMERIC(32, 16) NOT NULL,
    closed BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (symbol, open_time)
);

CREATE INDEX IF NOT EXISTS idx_candles_5m_symbol_open_time
    ON candles_5m (symbol, open_time);

CREATE TABLE IF NOT EXISTS agg_trades (
    symbol VARCHAR(20) NOT NULL,
    trade_id INTEGER NOT NULL,
    price NUMERIC(32, 16) NOT NULL,
    qty NUMERIC(32, 16) NOT NULL,
    quote_qty NUMERIC(32, 16) NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    is_buyer_maker BOOLEAN NOT NULL,
    PRIMARY KEY (symbol, trade_id)
);

CREATE INDEX IF NOT EXISTS idx_agg_trades_symbol_ts
    ON agg_trades (symbol, ts);

CREATE TABLE IF NOT EXISTS htf_levels (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    symbol VARCHAR(20) NOT NULL,
    timeframe VARCHAR(10) NOT NULL,
    level_type VARCHAR(20) NOT NULL CHECK (level_type IN ('swing_high', 'swing_low')),
    price NUMERIC(32, 16) NOT NULL,
    formed_at TIMESTAMPTZ NOT NULL,
    broken_at TIMESTAMPTZ,
    active BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_htf_levels_symbol_timeframe
    ON htf_levels (symbol, timeframe);

CREATE INDEX IF NOT EXISTS idx_htf_levels_active
    ON htf_levels (active) WHERE active = TRUE;

CREATE TABLE IF NOT EXISTS signals (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    symbol VARCHAR(20) NOT NULL,
    side VARCHAR(10) NOT NULL CHECK (side IN ('long', 'short')),
    entry NUMERIC(32, 16) NOT NULL,
    stop_loss NUMERIC(32, 16) NOT NULL,
    take_profit NUMERIC(32, 16) NOT NULL,
    leverage INTEGER NOT NULL,
    risk_pct NUMERIC(10, 4) NOT NULL,
    account_size_assumed NUMERIC(32, 8) NOT NULL,
    est_hold_minutes INTEGER NOT NULL,
    pattern_features JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status VARCHAR(20) NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'triggered', 'submitted', 'sl', 'tp', 'expired', 'expired_no_fill', 'cancelled')),
    client_order_id VARCHAR(50),
    delta_order_id VARCHAR(50),
    delta_symbol VARCHAR(20),
    delta_entry_intended NUMERIC(32, 16),
    delta_entry_actual NUMERIC(32, 16),
    delta_basis NUMERIC(16, 8),
    entry_fees_usdt NUMERIC(16, 8),
    slippage_bps NUMERIC(16, 8),
    cancelled_reason VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_signals_symbol
    ON signals (symbol);

CREATE INDEX IF NOT EXISTS idx_signals_status
    ON signals (status);

CREATE TABLE IF NOT EXISTS signal_outcomes (
    signal_id UUID PRIMARY KEY REFERENCES signals(id) ON DELETE CASCADE,
    exit_price NUMERIC(32, 16) NOT NULL,
    exit_ts TIMESTAMPTZ NOT NULL,
    pnl_pct NUMERIC(16, 8) NOT NULL,
    pnl_r NUMERIC(16, 8) NOT NULL,
    hold_minutes INTEGER NOT NULL,
    exit_reason VARCHAR(20) NOT NULL
);

CREATE TABLE IF NOT EXISTS candle_features (
    symbol VARCHAR(20) NOT NULL,
    timeframe VARCHAR(10) NOT NULL,
    open_time TIMESTAMPTZ NOT NULL,
    cvd_delta NUMERIC(32, 16) NOT NULL,
    cvd_cumulative NUMERIC(32, 16) NOT NULL,
    vol_zscore NUMERIC(16, 8) NOT NULL,
    vol_ratio_20 NUMERIC(16, 8) NOT NULL,
    PRIMARY KEY (symbol, timeframe, open_time)
);

CREATE INDEX IF NOT EXISTS idx_candle_features_symbol_timeframe_open_time
    ON candle_features (symbol, timeframe, open_time);

CREATE TABLE IF NOT EXISTS risk_state (
    id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    kill_switch_active BOOLEAN NOT NULL DEFAULT FALSE,
    kill_switch_reason TEXT,
    kill_switch_set_at TIMESTAMPTZ,
    daily_pnl_r NUMERIC(16, 8) NOT NULL DEFAULT 0,
    daily_trade_count INTEGER NOT NULL DEFAULT 0,
    consecutive_losses INTEGER NOT NULL DEFAULT 0,
    last_reset_date DATE,
    current_open_positions INTEGER NOT NULL DEFAULT 0
);
