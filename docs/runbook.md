# Operations runbook

Deploying on a server, dashboards, alerts, backups and the soak test: see
[deployment.md](deployment.md). This page covers what the bot does and how to respond.

## Modes

| `EXEC_MODE` | Venue | Notes |
|---|---|---|
| `paper` (default) | `PaperBroker` on live Delta order books | No credentials needed. `dry_run` is accepted as an alias. |
| `live` | Delta India | Needs `DELTA_API_KEY`/`DELTA_API_SECRET`. With `DELTA_MODE=production` the bot also refuses to start unless `LIVE_TRADING_ACKNOWLEDGED=true`. |

## Go-live checklist (before setting `LIVE_TRADING_ACKNOWLEDGED=true`)

1. CI is green on the exact commit you will deploy; record the commit and the strategy `config_hash`.
2. The strategy's latest research report says **Validated candidate**. A *Not validated* strategy does not go live.
3. Paper trading for at least 2 weeks with a session report (`signal-bot-research session-report`) showing no unresolved discrepancies, no protective failures and an execution drag in line with research.
4. On Delta **testnet**, run the read-only socket probe with your testnet key: `DELTA_MODE=testnet python -m signal_bot.delta.client --ws-check`. It must print `auth={... "success": true ...}` and a `subscriptions` line without errors. It places no orders. Then, with `EXEC_MODE=live` and a tiny position, confirm the remaining `VERIFY` mappings in `execution/delta_adapter.py`: order, position and `user_trades` messages for a real fill (including whether bracket child fills carry your `client_order_id`), the bracket-leg fields on positions, `GET /v2/orders/client_order_id/{id}`, `POST /v2/orders/bracket`, and cancel with `product_id`.
5. Run the trailing-stop check on testnet (see *Trend portfolio* below) and record its JSON output here. Stepped trailing needs `stepped_edit_works: true`; `TRAIL_MODE=native` additionally needs the sign Delta accepted and `native_trail_moves: true`.
6. Run the halt drill below on testnet.
7. Set `OPERATOR_TOKEN`, keep the ops port on localhost or behind an authenticated proxy, and make sure Telegram alerts arrive.
8. Start with `RISK_PCT_PER_TRADE` at 0.25 or less and `MAX_CONCURRENT_POSITIONS=1`. In trend mode these cap the portfolio rules; a set value only ever makes them stricter.

## Trend portfolio (default strategy since 2.1)

`STRATEGY_CONFIG=signal_bot/research/strategies/trend_portfolio.toml` (the default) runs
the 4h trend breakout over the Delta perpetual universe on one account. It is **not
validated** (docs/research.md, *Portfolio*); it is the default so that paper trading
measures it forward. `STRATEGY_CONFIG=.../liquidity_sweep.toml` runs the 2.0 sweep.

| Setting | Default | Notes |
|---|---|---|
| `BINANCE_TESTNET` | `false` | Market data only (no orders go to Binance). Trend mode refuses `true`: the testnet feed is synthetic. |
| `TRAIL_MODE` | `stepped` | The researched exit: the exchange stop is moved after each completed 4h bar. `native` uses Delta's continuous `bracket_trail_amount` (not backtested; needs the testnet check). |
| `DELTA_TRAIL_AMOUNT_SIGN` | `positive` | Only for `native`; set from the testnet check (`positive` or `negative_for_long`). |
| `TREND_MIN_HISTORY_BARS` | `1000` | Completed 4h bars a symbol needs before it may trade (loaded from Binance REST at start). |
| `TREND_BATCH_TIMEOUT_SECONDS` | `120` | How long after a 4h close to wait for every symbol's candle before deciding the batch. |

Startup order: Binance REST warm-up (1500 bars per symbol), then the 4h stream. If the
warm-up fails (HTTP 451 in restricted regions), the control room shows "Warm-up failed"
per symbol and nothing trades. The governor's limits are derived from the portfolio
rules (6 positions, -6R/day, 18 trades/day, 15 consecutive losses); explicitly set
stricter values win. The startup log prints the effective limits.

### Testnet trailing-stop check (places testnet orders)

```bash
DELTA_MODE=testnet DELTA_API_KEY=... DELTA_API_SECRET=... \
  python -m signal_bot.delta.trail_check --yes-place-testnet-orders --seconds 120
```

Opens one contract with a `bracket_trail_amount` stop and samples its stop price, then
opens one with a fixed bracket stop and edits it with `PUT /v2/orders`, flattening and
cancelling after each leg (and again in `finally`). It refuses to run unless
`DELTA_MODE=testnet`. Run it with `--sign negative_for_long` if the positive sign is
rejected. Record the `conclusion` block:

| Field | Needed for |
|---|---|
| `stepped_edit_works` | `TRAIL_MODE=stepped` in live mode (the default) |
| `native_trail_accepted_with_sign`, `native_trail_moves` | `TRAIL_MODE=native` and `DELTA_TRAIL_AMOUNT_SIGN` |
| `a_position_fields`, `a_children` | Whether positions carry bracket fields and how a trailing child looks (update the `VERIFY` notes in `execution/delta_adapter.py`) |

If any `*_cleanup` field is not `ok`, close the testnet position by hand.

## Delta private socket (as documented at docs.delta.exchange)

| Item | Value |
|---|---|
| Testnet / production private socket | `wss://socket-ind.testnet.deltaex.org` / `wss://socket.india.delta.exchange` (override with `DELTA_WS_URL`) |
| Authentication | `{"type":"key-auth","payload":{"api-key","signature","timestamp"}}`; the signature is HMAC-SHA256 of `"GET" + timestamp + "/live"` (`DELTA_WS_AUTH_PATH`). The old `auth` type was removed on 31 Oct 2026. |
| Order of operations | key-auth, wait for `{"type":"key-auth","success":true}`, `enable_heartbeat`, then subscribe |
| Channels | `orders`, `positions`, `user_trades`, each with the tradable symbols (explicit symbols get snapshots) |
| Liveness | server heartbeat every 30 s; the bot reconnects after 35 s of silence |
| Confirmed against testnet without credentials | endpoint reachable; `key-auth` answered with the documented response shape (`api_key_not_found`); the legacy `auth` type returns only `{"type":"error"}` |
| Not yet confirmed (needs a testnet key) | signature acceptance, subscription response, real order/position/fill payloads |

Symbols Delta doesn't list as live perpetuals (testnet has no LINKUSD or AVAXUSD) are logged at startup, shown in the control room as not tradable, and their signals are refused with `symbol_unmapped`. Startup fails only if none of `SYMBOLS` is tradable.

## Halting and resuming

Halt (any of):

* Control room: enter the token and a reason, then **Halt trading now**.
* `curl -X POST -H "Authorization: Bearer $OPERATOR_TOKEN" -H 'Content-Type: application/json' -d '{"reason":"...","operator":"me"}' http://127.0.0.1:8080/api/risk/halt`
* CLI (works while the bot is down): `python -m signal_bot.risk.governor --kill "reason"`

Halting stops new orders only. Open positions keep their venue-side stops.

Resume needs a reason plus the typed confirmation `RESUME` in the control room, or
`python -m signal_bot.risk.governor --clear --reason "what was checked"`. Every attempt,
including refused ones, appears in the operator audit trail. Resuming does not clear
circuit breakers: those clear when their condition recovers, and the response lists any
still open.

## Alerts and what to do

| Alert / control-room state | Meaning | Action |
|---|---|---|
| **Stopped: kill switch engaged**, reason `protective_order_failed:<id>` | An entry filled and no stop could be confirmed. The engine attached a standalone stop or flattened. | Check the venue position for that symbol. If protected or flat, review why the bracket failed, then resume. If `protective_status=failed`, protect or close it manually first. |
| `auto: max_consecutive_losses` | Loss streak limit | Review the trades; resume the next day or after review. |
| **Paused: an interlock is open**, `unresolved_order:<id>` | A submission's outcome is unknown | Usually resolves within seconds by lookup. If it persists, find the client id on the venue. The reconciler cancels it only after `UNKNOWN_ORDER_GRACE_SECONDS` with no venue order or position. |
| `exchange_stream` | Private socket down | Clears automatically after reconnect and a successful reconciliation. If it persists, check connectivity and credentials. |
| `exchange_auth` | Key rejected | Rotate or fix the key; clears after the next successful reconciliation. |
| `orphan_position:<symbol>` | A position on the venue that no order owns (manual trade, or lost state) | Close it, or adopt it manually; the breaker clears once it is gone. Set `BLOCK_ON_ORPHAN_POSITIONS=false` only if you deliberately trade the account by hand. |
| `task_crashed` | A pipeline task died; the process is shutting down | Check the logs (filter on `level=CRITICAL`); restart. Startup reconciliation and replay recover state. |
| `JournalUnavailable` / Event journal degraded | PostgreSQL unreachable | New orders are already refused (governor fails closed). Restore the database; buffered events flush automatically. |
| Market data degraded | Binance unreachable or stale candles | No signals are generated. Check connectivity; Binance blocks some regions (HTTP 451). In trend mode, freshness is judged on the 4h stream. |
| `Trailing stop for <symbol> not moved (...)` | The venue refused a stop amendment | The previous stop is still on the exchange, so the position is protected but at a looser level than the strategy wants. The next 4h close retries; if it keeps failing, check the order on Delta and move the stop by hand to the "Next stop" shown in the control room. |
| Discrepancy `trailing_stop_mismatch` | The venue's stop differs from the recorded one | Reconciliation re-derives it: a looser venue stop is amended, a tighter one adopted. Persistent mismatches point at a stop edited outside the bot. |
| `time_stop close failed` | The 30-day time stop could not close the position | Retries every minute; close by hand if it persists. |
| `portfolio_drawdown` breaker | Equity is 20% or more below its peak | New entries stop; open positions keep their stops. It clears when the drawdown recovers below 20%. Review before deciding to continue. |
| Trend decision refused `portfolio:<rule>` | The shared-account rules refused an entry (`correlated_risk`, `max_open_risk`, `max_entries_per_bar`, ...) | Normal. Each refusal is a cancelled signal row and a `RISK_DENIED` event with the rule. |

## Tracing a trade

Every log line for a trade carries `correlation_id=<signal id>`:

```bash
docker compose logs bot | jq -c 'select(.correlation_id=="<signal-uuid>")'
```

Order history: `order_transitions` (each state change with its event id) and `fills`.
Results: `trade_results` (execution quality) and `signal_outcomes` (signal quality).

## Restart behaviour

On start the bot replays journal events after each consumer's checkpoint, runs a full
reconciliation (the engine waits for it), expires pending signals that only existed in
memory, resumes no-fill timers for open orders and re-verifies protection on filled
orders. In trend mode the first reconciliation also re-derives every trailing stop from
candle history (amending stops the process missed while down, exiting positions whose
derived stop is already through the market) and runs overdue time stops.

A crash between submit and acknowledgement is recovered by looking up the persisted
client order id; nothing is resubmitted.
