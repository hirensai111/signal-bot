# Changelog

## 2.0.0

Signal Bot 2.0: from a signal bot to a fault-tolerant trading platform.

### Fixed (from the code review)
- The startup path used an always-approve governor; the real `RiskGovernor` now gates every submission and fails closed.
- The execution engine and outcome monitor shared one `asyncio.Queue` and could steal each other's exchange events; each consumer now has its own subscription.
- The HTF scanner and microstructure engine shared the closed-candle queue and discarded each other's candles; each now has an independent, interval-filtered subscription.
- A placement timeout marked the order cancelled; it is now `submission_unknown` and resolved by client-id lookup, never resubmitted.
- Position sizing ignored price (`notional / contract_value`); it is now `notional / (contract_value × price)`.
- The reconciler marked orders with an unknown exchange state as cancelled; they now stay blocked until resolved.
- Live 1h/4h candles were never subscribed, so HTF levels froze after backfill.
- The still-forming backfill candle was stored and forwarded as closed.
- Backfilled (historical) candles could generate live signals; candles are now gated on freshness.
- `agg_trades` had no `(symbol, ts)` index, so the microstructure query did a full scan on every candle.
- A Binance outage crashed the whole process, including stop management.
- `db/connection.py` imported a second copy of the settings module.

### Added
- Event contract, fan-out bus, PostgreSQL event journal with checkpoints and replay.
- Persistent order state machine (`orders`, `order_transitions`, `fills`).
- Exchange adapter interface; Delta adapter with an error taxonomy and timeouts.
- Protective-order verification and recovery (standalone stop, then flatten).
- Risk circuit breakers, worst-case daily loss budget, submission-time trade counting, UTC day roll-over.
- Fault-injecting simulated venue; paper broker on live order books.
- Nine end-to-end failure scenarios, run on SQLite and PostgreSQL.
- Research: deterministic fill model, exhaustion-reversion and regime-filter strategies, IS/OOS, walk-forward, cost sensitivity, random baselines, versioned reports, session reports.
- Observability: JSON logs, Prometheus metrics and alerts, Grafana dashboard, control room, authenticated halt/resume with audit trail.
- CI (lint, format, multi-Python tests, PostgreSQL, replay, pip-audit, build), release workflow, Dependabot, Docker Compose stack.

### Changed
- `EXEC_MODE=dry_run` (instant fills at the signal price) is replaced by `paper`; `dry_run` remains an alias.
- Production trading requires `LIVE_TRADING_ACKNOWLEDGED=true`.
- The package is installable (`pip install -e .`) and lives in `signal_bot/`.
