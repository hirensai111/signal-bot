# Operations runbook

## Modes

| `EXEC_MODE` | Venue | Notes |
|---|---|---|
| `paper` (default) | `PaperBroker` on live Delta order books | No credentials needed. `dry_run` is accepted as an alias. |
| `live` | Delta India | Needs `DELTA_API_KEY`/`DELTA_API_SECRET`. With `DELTA_MODE=production` the bot also refuses to start unless `LIVE_TRADING_ACKNOWLEDGED=true`. |

## Go-live checklist (before setting `LIVE_TRADING_ACKNOWLEDGED=true`)

1. CI is green on the exact commit you will deploy; record the commit and the strategy `config_hash`.
2. The strategy's latest research report says **Validated candidate**. A *Not validated* strategy does not go live.
3. Paper trading for at least 2 weeks with a session report (`signal-bot-research session-report`) showing no unresolved discrepancies, no protective failures and an execution drag in line with research.
4. On Delta **testnet** with `EXEC_MODE=live`, confirm every `VERIFY` mapping in `execution/delta_adapter.py`: the order-update and position socket payloads, the bracket-leg fields on positions, `GET /v2/orders/client_order_id/{id}`, `POST /v2/orders/bracket`, and that `client_order_id` accepts 32 characters.
5. Run the halt drill below on testnet.
6. Set `OPERATOR_TOKEN`, keep the ops port on localhost or behind an authenticated proxy, and make sure Telegram alerts arrive.
7. Start with `RISK_PCT_PER_TRADE` at 0.25 or less and `MAX_CONCURRENT_POSITIONS=1`.

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
| Market data degraded | Binance unreachable or stale candles | No signals are generated. Check connectivity; Binance blocks some regions (HTTP 451). |

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
orders. A crash between submit and acknowledgement is recovered by looking up the
persisted client order id; nothing is resubmitted.
