# Signal Bot

A fault-tolerant, event-driven crypto-futures trading platform. It researches
strategies on replayed history, paper-trades them against live order books,
executes on Delta Exchange India behind a fail-closed risk governor, and recovers
safely from infrastructure failures.

The point is the engineering, not a profitability claim: live data, money-moving
side effects, asynchronous processing and partial failures, each handled
deliberately and each proven by an automated test.

| | |
|---|---|
| Execution | Persistent order state machine; `submission_unknown` resolved by client-id lookup and never resubmitted; protective stop verified after every fill |
| Events | Typed events with deterministic ids, journaled in PostgreSQL before delivery, a queue per consumer, checkpoints and replay |
| Risk | Persisted kill switch, worst-case daily loss budget, circuit breakers for stream, auth, unresolved-order and orphan-exposure conditions; fails closed |
| Testing | 9 failure scenarios run end to end against a fault-injecting exchange, on SQLite and PostgreSQL |
| Paper trading | Fills from live Delta order books with latency, depth, fees, funding and a persisted ledger |
| Research | Deterministic fill model, IS/OOS, walk-forward, cost sensitivity, random-entry baselines, a shared-capital portfolio simulator, versioned JSON/HTML reports |
| Strategy | 4h trend breakout over the Delta perpetuals, decided on completed candles by the same code in research and live (replay-tested), stepped trailing stops on the exchange, portfolio limits on one account. Not validated: it runs in paper mode to be measured forward ([results](docs/research.md#portfolio-riskportfoliopy-researchportfoliopy)) |
| Operations | JSON logs with correlation ids, Prometheus metrics and alerts, Grafana dashboard, control room with an authenticated halt control and audit trail |

## Quick start

```bash
pip install -e ".[dev]"
pytest -q                                   # unit + failure scenarios + replay (SQLite)
python scripts/control_room_demo.py         # full stack on a synthetic market: http://127.0.0.1:8081
```

Paper trading with Docker (PostgreSQL, Prometheus, Grafana):

```bash
cp .env.example .env    # set OPERATOR_TOKEN
SIGNAL_BOT_COMMIT=$(git rev-parse HEAD) docker compose up --build
```

* Control room: <http://127.0.0.1:8080>
* Grafana: <http://127.0.0.1:3000>
* Prometheus: <http://127.0.0.1:9090>

Without Docker: `EXEC_MODE=paper signal-bot` (needs `DATABASE_URL`).

Live trading needs `EXEC_MODE=live`, Delta credentials and, for production, the go-live
checklist in [docs/runbook.md](docs/runbook.md) (the bot refuses to start without
`LIVE_TRADING_ACKNOWLEDGED=true`).

## Layout

| Path | Responsibility |
|---|---|
| `signal_bot/ingest`, `storage/writer.py` | Binance backfill and streams; persist, then fan out closed candles per subscriber |
| `signal_bot/signals/trend*.py` | Trend decision rule (shared with research), live 4h pipeline |
| `signal_bot/risk/portfolio.py` | Shared-capital portfolio rules (live and research) |
| `signal_bot/execution/trailing.py` | Stepped trailing stops, time stops, restart re-derivation |
| `signal_bot/features`, `signals` | Candle history, HTF levels, microstructure (CVD, volume), sweep detection, sizing |
| `signal_bot/events` | Event contract, bus, journal, replay |
| `signal_bot/execution` | Adapter interface, Delta adapter, order state machine, execution engine |
| `signal_bot/venues` | Fault-injecting simulator, paper broker |
| `signal_bot/risk/governor.py` | Kill switch, limits, circuit breakers, operator CLI |
| `signal_bot/tracking` | Outcome monitor (signal vs execution quality), reconciler |
| `signal_bot/observability` | Logs, metrics, status, ops server, control room |
| `signal_bot/research` | Data, fill model, strategies, validation, reports |
| `tests/scenarios` | End-to-end failure scenarios |
| `deploy/` | Prometheus, alerts, Grafana |

## Documentation

* [Architecture](docs/architecture.md): event contract, order state machine, safety properties
* [Failure scenarios](docs/failure-scenarios.md): each scenario mapped to the test that proves it
* [Research](docs/research.md): reproducibility, fill model, validation criteria
* [Runbook](docs/runbook.md): modes, go-live checklist, halting, alerts, tracing a trade
* [Reports](docs/reports/): published research reports (JSON and HTML)

## Development

```bash
ruff check . && ruff format --check .
pytest -q -m "not postgres"
TEST_POSTGRES_URL=postgresql+asyncpg://u:p@localhost/signalbot_test pytest -q -m postgres
SCENARIO_DB_URL=$TEST_POSTGRES_URL pytest -q tests/scenarios     # scenarios on PostgreSQL
```

CI (`.github/workflows/ci.yml`) runs lint, format, unit tests on Python 3.11–3.13,
PostgreSQL integration, a deterministic replay check, `pip-audit` and a wheel build.
Pull-request jobs get read-only permissions and no secrets. Tagged releases publish a
manifest that ties the version to its commit and strategy config hashes.

## License

Private, for personal use only.
