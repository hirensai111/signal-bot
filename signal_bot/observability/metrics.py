"""Prometheus metrics.  One process-wide registry; the ops server exposes it at /metrics.

Metric names follow the ``signalbot_<subsystem>_<what>_<unit>`` convention so the
bundled Grafana dashboard (deploy/grafana) can be imported unchanged.
"""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

REGISTRY = CollectorRegistry(auto_describe=True)

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
PAPER_EQUITY = Gauge(
    "signalbot_paper_equity",
    "Mark-to-market equity of the paper account",
    registry=REGISTRY,
)


def render() -> bytes:
    return generate_latest(REGISTRY)
