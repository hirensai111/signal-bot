"""Prometheus metrics.  One process-wide registry; the ops server exposes it at /metrics.

Metric names follow the ``signalbot_<subsystem>_<what>_<unit>`` convention so the
bundled Grafana dashboard (deploy/grafana) can be imported unchanged.
"""

from __future__ import annotations

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    PlatformCollector,
    ProcessCollector,
    generate_latest,
)

REGISTRY = CollectorRegistry(auto_describe=True)
# process_start_time_seconds (restarts), process_resident_memory_bytes, CPU, open fds.
ProcessCollector(registry=REGISTRY)
PlatformCollector(registry=REGISTRY)

# --- build ------------------------------------------------------------------
BUILD_INFO = Gauge(
    "signalbot_build_info",
    "Always 1; labels identify what is running",
    ["version", "commit", "strategy", "strategy_version", "exec_mode", "venue", "delta_mode"],
    registry=REGISTRY,
)

# --- market data -----------------------------------------------------------
MARKET_DATA_AGE = Gauge(
    "signalbot_market_data_age_seconds",
    "Seconds since the last closed candle was received, per symbol/interval",
    ["symbol", "interval"],
    registry=REGISTRY,
)
CANDLES_DROPPED = Counter(
    "signalbot_candles_dropped_total",
    "Closed candles not forwarded to feature engines",
    ["reason"],
    registry=REGISTRY,
)

# --- event bus -------------------------------------------------------------
EVENTS_PUBLISHED = Counter(
    "signalbot_events_published_total",
    "Events published on the bus",
    ["event_type"],
    registry=REGISTRY,
)
EVENTS_DUPLICATE = Counter(
    "signalbot_events_duplicate_total",
    "Events suppressed because the journal already held their event_id",
    ["event_type"],
    registry=REGISTRY,
)
EVENT_QUEUE_LAG = Gauge(
    "signalbot_event_queue_lag",
    "Undelivered events waiting in a subscriber queue",
    ["subscriber"],
    registry=REGISTRY,
)
EVENT_PROCESSING_FAILURES = Counter(
    "signalbot_event_processing_failures_total",
    "Exceptions raised while a consumer processed an event",
    ["consumer"],
    registry=REGISTRY,
)
JOURNAL_WRITE_FAILURES = Counter(
    "signalbot_journal_write_failures_total",
    "Durable events that could not be written to the journal (buffered for retry)",
    registry=REGISTRY,
)
JOURNAL_PENDING = Gauge(
    "signalbot_journal_pending_events",
    "Durable events buffered in memory awaiting a successful journal write",
    registry=REGISTRY,
)

# --- exchange --------------------------------------------------------------
EXCHANGE_LATENCY = Histogram(
    "signalbot_exchange_request_latency_seconds",
    "Latency of exchange adapter calls",
    ["venue", "operation", "outcome"],
    buckets=(0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30),
    registry=REGISTRY,
)
EXCHANGE_STREAM_CONNECTED = Gauge(
    "signalbot_exchange_stream_connected",
    "1 while the private exchange stream is connected",
    ["venue"],
    registry=REGISTRY,
)

# --- orders & risk ---------------------------------------------------------
ORDER_TRANSITIONS = Counter(
    "signalbot_order_transitions_total",
    "Persisted order state transitions",
    ["from_state", "to_state"],
    registry=REGISTRY,
)
ORDERS_REJECTED = Counter(
    "signalbot_orders_rejected_total",
    "Orders rejected by the exchange",
    ["venue"],
    registry=REGISTRY,
)
SUBMISSIONS_UNKNOWN = Counter(
    "signalbot_order_submission_unknown_total",
    "Order submissions whose outcome could not be determined from the response",
    ["venue"],
    registry=REGISTRY,
)
RISK_DENIALS = Counter(
    "signalbot_risk_denials_total",
    "Signals denied by the risk governor",
    ["rule"],
    registry=REGISTRY,
)
KILL_SWITCH_ACTIVE = Gauge(
    "signalbot_kill_switch_active",
    "1 while the persisted kill switch is engaged",
    registry=REGISTRY,
)
CIRCUIT_BREAKERS_ACTIVE = Gauge(
    "signalbot_circuit_breakers_active",
    "Number of transient trading circuit breakers currently open",
    registry=REGISTRY,
)
GOVERNOR_HEALTHY = Gauge(
    "signalbot_risk_governor_healthy",
    "1 while the risk governor can evaluate rules against fresh state",
    registry=REGISTRY,
)
OPEN_POSITIONS = Gauge(
    "signalbot_open_positions",
    "Open positions according to the local order store",
    registry=REGISTRY,
)
RECONCILIATION_DISCREPANCIES = Counter(
    "signalbot_reconciliation_discrepancies_total",
    "Differences found between local state and exchange state",
    ["kind"],
    registry=REGISTRY,
)
RECONCILIATION_DURATION = Histogram(
    "signalbot_reconciliation_duration_seconds",
    "Wall time of a reconciliation pass",
    buckets=(0.1, 0.5, 1, 2, 5, 10, 30, 60),
    registry=REGISTRY,
)
PROTECTIVE_FAILURES = Counter(
    "signalbot_protective_order_failures_total",
    "Filled entries whose protective stop could not be confirmed",
    registry=REGISTRY,
)

# --- paper account ---------------------------------------------------------
TRAILING_STOP_UPDATES = Counter(
    "signalbot_trailing_stop_updates_total",
    "Trailing-stop amendments by outcome (moved, rejected, unknown, verified_venue_tighter)",
    ["outcome"],
    registry=REGISTRY,
)
TIME_STOPS = Counter(
    "signalbot_time_stops_total",
    "Positions closed by the strategy's time stop",
    registry=REGISTRY,
)
TREND_DECISIONS = Counter(
    "signalbot_trend_decisions_total",
    "Trend-breakout decisions by portfolio outcome (accepted or the refusal reason)",
    ["outcome"],
    registry=REGISTRY,
)
PORTFOLIO_OPEN_RISK = Gauge(
    "signalbot_portfolio_open_risk_ratio",
    "Sum of open stop-loss risk as a fraction of equity",
    registry=REGISTRY,
)
PORTFOLIO_CORRELATED_RISK = Gauge(
    "signalbot_portfolio_correlated_risk_ratio",
    "Correlation-adjusted open risk sqrt(r'Cr) as a fraction of equity",
    registry=REGISTRY,
)
PORTFOLIO_DRAWDOWN = Gauge(
    "signalbot_portfolio_drawdown_ratio",
    "Drawdown of marked-to-market equity from its peak",
    registry=REGISTRY,
)
PAPER_EQUITY = Gauge(
    "signalbot_paper_equity",
    "Mark-to-market equity of the paper account",
    registry=REGISTRY,
)
PORTFOLIO_EQUITY = Gauge(
    "signalbot_portfolio_equity",
    "Marked-to-market equity the portfolio rules size on",
    registry=REGISTRY,
)
PORTFOLIO_PEAK_EQUITY = Gauge(
    "signalbot_portfolio_peak_equity",
    "Highest marked-to-market equity seen (drawdown reference)",
    registry=REGISTRY,
)
PORTFOLIO_POSITIONS = Gauge(
    "signalbot_portfolio_positions",
    "Positions counted by the portfolio rules (filled or in flight)",
    registry=REGISTRY,
)
PORTFOLIO_GROSS_LEVERAGE = Gauge(
    "signalbot_portfolio_gross_leverage_ratio",
    "Total notional divided by equity",
    registry=REGISTRY,
)
PORTFOLIO_LIMIT = Gauge(
    "signalbot_portfolio_limit",
    "Portfolio rule thresholds. Entry limits gate NEW entries only; open risk can exceed them",
    ["limit"],
    registry=REGISTRY,
)

# --- trend pipeline ---------------------------------------------------------
TREND_SYMBOL_READY = Gauge(
    "signalbot_trend_symbol_ready",
    "1 if the symbol has enough completed 4h history to trade",
    ["symbol"],
    registry=REGISTRY,
)
TREND_SYMBOL_TRADABLE = Gauge(
    "signalbot_trend_symbol_tradable",
    "1 if the venue lists the symbol as a live perpetual",
    ["symbol"],
    registry=REGISTRY,
)
TREND_HISTORY_BARS = Gauge(
    "signalbot_trend_history_bars",
    "Completed 4h bars held for the symbol",
    ["symbol"],
    registry=REGISTRY,
)
TREND_LAST_BAR_CLOSE = Gauge(
    "signalbot_trend_last_bar_close_timestamp_seconds",
    "Close time of the newest completed 4h bar processed for the symbol",
    ["symbol"],
    registry=REGISTRY,
)
TREND_CANDLES = Counter(
    "signalbot_trend_candles_total",
    "4h candles offered to the trend pipeline, by outcome (appended or why refused)",
    ["symbol", "outcome"],
    registry=REGISTRY,
)
TREND_BATCHES = Counter(
    "signalbot_trend_batches_total",
    "Bar-close decision batches evaluated by the portfolio rules",
    registry=REGISTRY,
)

# --- protection & reconciliation ------------------------------------------------
POSITIONS_UNPROTECTED = Gauge(
    "signalbot_positions_unprotected",
    "Filled positions the last reconciliation found without a stop on the venue",
    registry=REGISTRY,
)
RECONCILIATION_LAST_SUCCESS = Gauge(
    "signalbot_reconciliation_last_success_timestamp_seconds",
    "Unix time of the last successful reconciliation pass",
    registry=REGISTRY,
)
RECONCILIATION_FAILURES = Counter(
    "signalbot_reconciliation_failures_total",
    "Reconciliation passes that raised",
    registry=REGISTRY,
)


def render() -> bytes:
    return generate_latest(REGISTRY)
