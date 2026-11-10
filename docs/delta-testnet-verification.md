# Guarded Delta demo fill verification

`scripts/test_delta_fill_guarded.py` is a standalone integration probe. It places
real **testnet** orders; it does not start the trading engine. Run it only on an
otherwise idle demo account. Production configuration and custom socket hosts
are rejected. An explicit confirmation flag is required before any network calls.

```powershell
$env:DELTA_MODE = "testnet"
$env:EXEC_MODE = "paper"
python -m scripts.test_delta_fill_guarded --confirm-testnet-fill
```

The probe buys one BTCUSD contract with a marketable IOC limit, a maximum entry
price, and embedded SL/TP trigger prices. One contract must fit the hard USD 100
notional cap. `--max-notional-usd` may lower that cap, never increase it. Contract
metadata, price ticks, a fresh ticker, spread, price bands, an empty account,
authentication, subscription acknowledgments and private snapshots are checked
before entry. No leverage or account settings are changed.

The entry is submitted at most once. If its response is lost, the probe queries
the same client order ID; it never retries the entry. A nonzero realtime position
is checked for a separate exchange-side, pending/open, reduce-only market stop
with the exact product, side, size and stop price. Bracket fields echoed on the
entry are insufficient. Missing protection blocks further entry and immediately
starts reduce-only flattening. The cleanup path prioritizes position reduction
over order lookups and asynchronous evidence collection.

REST fill history is matched to `user_trades` by fill and order ID, client ID,
size, price, side, liquidity role and commission. Aggregate fees and average price
are compared with the filled order, and actual `orders` and `positions` updates
are required. Stop protection is rechecked while entry evidence is collected.
The position is closed with a single reduce-only market order; an ambiguous
close is queried by client ID rather than blindly resubmitted. Remaining known
protection orders are cancelled after flattening. Both realtime and margined
positions, along with resting orders, are reconciled before reporting success.

Cleanup is bounded and requires exchange connectivity and a running process.
An unreconciled close produces `CLEANUP INCOMPLETE` and a failing report; it must
not be interpreted as proof of a flat account. Process termination and extended
network outages remain operational limits of this client-side probe.

The report is `tests/fixtures/delta_testnet_fill.txt`; observed, sanitized exchange
events are in `tests/fixtures/delta_testnet_fill.json`. The probe also saves local
timestamped JSON copies of each attempt, including failures. Account fields, balances, credentials,
IP addresses and arbitrary exchange metadata are excluded by an allowlist. Order,
fill and client IDs are replaced consistently so cross-channel relationships can
still be tested. Test execution prices, quantities and commissions are retained.
Regression tests in `tests/test_delta_fill_guarded.py` exercise these observations
and offline timeout, missing-protection and production-rejection scenarios.

The captured 2026-11-01 demo run passed genuine entry fill, protective stop
existence, REST/WebSocket/fee matching, controlled reduce-only close, and final
flat-account checks. It did **not** deliberately trigger a stop or take profit,
and it did not test restart recovery or validate strategy profitability.

API reference: [Delta Exchange API](https://docs.delta.exchange/).

## Isolated automatic-exit and restart scenarios

Run these separately, in order, on the otherwise idle testnet account. Each
invocation requires its own explicit confirmation, rejects production mode and
retains the single-contract / USD 100 hard exposure cap.

```powershell
python -m scripts.test_delta_exit_guarded --scenario stop-loss --confirm-testnet-scenario --observe-seconds 45 --trigger-bps 0.1
python -m scripts.test_delta_exit_guarded --scenario take-profit --confirm-testnet-scenario --observe-seconds 45 --trigger-bps 0.1
python -m scripts.test_delta_exit_guarded --scenario restart --confirm-testnet-scenario
```

Trigger scenarios move only the intended bracket trigger near the current market;
the opposite leg stays distant. They use `mark_price` triggers and observe natural
price changes. They do not place offsetting trades to manufacture a trigger. A
deadline without the intended execution is `INCONCLUSIVE` (exit code 3). A trigger
that does not close exposure enters bounded reduce-only flatten/reconciliation
and fails. The observer waits for final order state if a realtime position read
becomes flat between successive, non-atomic REST reads.

The parent process retains an authenticated private stream and all cleanup
authority. A separate bot worker runs the production `ExecutionEngine`,
`Reconciler`, `OrderStore`, SQL event journal, stream bridge, risk governor and
`OutcomeMonitor`, using an isolated database under ignored
`data/delta_exit_validation/`. The worker's exchange client strictly rejects
mutations; any attempted mutation fails the no-duplicate-submission check.

For restart, the first worker exits after persisting the submitting intent. The
guardian opens the protected position while that worker is offline, then launches
a new worker against the same database. This must recover the missed entry fill
and protection before the engine opens its signal-consumer startup gate. The
same exchange protective-order IDs must remain live across the process restart.
Redelivering the original signal must not place another order. The guardian then
closes and reconciles the demo position.

For real exits, REST fills must match the private fill stream by actual order and
fill IDs, quantity, price and fees. The exchange order's stop/TP type establishes
exit purpose, rather than inferring it from price proximity. A flat update that
arrives before fill-history publication must defer outcome finalization; later
reconciliation retries. Nameless or differently tagged child/cleanup fills can be
associated with their parent once REST identifies the same execution. Existing
owned fills are never transferred to another local trade. Duplicate delivery and
explicit duplicate close attempts must leave exactly one outcome, trade result,
and `POSITION_CLOSED` journal event.

Sanitized per-scenario evidence/reports are `tests/fixtures/delta_stop_loss.*`,
`delta_take_profit.*`, and `delta_restart.*`. Local timestamped snapshots retain
failed and inconclusive attempts. Validation databases retain raw execution identifiers
locally for crash diagnostics; they are ignored by Git and are not the shareable
regression evidence. These tests do not validate strategy profitability or replace
a sustained paper-trading soak test with outages and stale-data injection.

The 2026-11-01 automatic-exit validation completed all three scenarios. The
combined observed report is `tests/fixtures/delta_automatic_exit_validation.txt`.
The canonical stop-loss evidence includes an additional read-only final-order
confirmation after an order/position observation race; its earlier inconclusive
snapshot remains in the local archive. Initial failures are retained rather than presented
as successful runs. Stop-loss and take-profit outcomes each have one associated
entry fill, one actual exit fill, one outcome, and one close journal event. The
restart run recovered the offline fill before signal consumption and preserved
the same protective-order IDs. Every completed scenario ended flat with zero
resting orders.
