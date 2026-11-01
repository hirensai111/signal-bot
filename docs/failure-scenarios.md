# Failure scenarios

Every row is an automated test that runs the production execution stack (engine,
order store, risk governor, outcome monitor, reconciler, event bus and SQL journal)
against the fault-injecting `SimulatedVenue`. CI runs the suite twice: on SQLite in
the unit job and on PostgreSQL 16 in the integration job.

| Injected failure | Required safe behaviour | Test (`tests/scenarios/test_failure_scenarios.py` unless noted) |
|---|---|---|
| Exchange accepts the order but the HTTP response times out | Look the order up by its original client id; never submit a second order | `test_accepted_but_response_lost_is_resolved_by_lookup_not_resubmitted`, `test_server_error_after_accept_on_resting_order` |
| Request lost before the exchange saw it | Stay `reconcile_required` (not cancelled) and block trading; cancel only after the grace window with no venue order or position | `test_request_lost_before_accept_blocks_trading_until_reconciled` |
| Same fill arrives twice (same event id, or a different transport id) | Financial effect recorded exactly once: one fill row, one outcome, one risk-state update | `test_duplicate_fill_has_financial_effect_once[duplicate_events]`, `[duplicate_fills_new_id]` |
| Exchange WebSocket disconnects | Block new orders; on reconnect, reconcile (recovering the close that happened while disconnected), then unblock | `test_stream_disconnect_blocks_orders_then_reconnect_reconciles` |
| Process crashes between submission and database update | Recover from the persisted intent plus venue state on restart; no resubmission; protection verified | `test_crash_after_submit_recovers_from_intent_and_venue_state`, `test_restart_expires_signals_that_were_only_in_memory` |
| Database becomes unavailable | Stop new submissions (governor fails closed); keep delivering venue events and position visibility; buffer journal writes; recover on reconnect | `test_database_outage_stops_submissions_keeps_position_visibility`; `tests/test_observability.py::test_status_survives_database_outage` |
| Risk limit or kill switch activates | Reject every subsequent submission until explicitly cleared; breakers don't clear the kill switch | `test_kill_switch_rejects_every_submission_until_explicitly_cleared`, `test_consecutive_losses_trip_kill_switch`, `test_daily_loss_limit_blocks_new_trades` |
| Candle or fill event arrives late or out of order | Sequence rules: stale updates are ignored, filled size never decreases, late candles are not forwarded, stale signals are not traded | `test_reordered_venue_events_converge_to_correct_state`, `test_late_signal_from_old_candle_is_not_traded`; `tests/test_candle_fanout.py::test_duplicate_and_out_of_order_candles_are_not_forwarded`; `tests/test_order_state_machine.py::test_reordered_updates_never_move_backwards` |
| Protective order fails after the entry fill | Critical state (kill switch, alert, journal event), then recovery: standalone stop, else flatten, else `reconcile_required` | `test_missing_protective_stop_halts_and_attaches_standalone_stop`, `test_protective_recovery_flattens_when_stop_cannot_be_attached`, `test_unprotectable_and_unflattenable_position_requires_reconciliation` |

## Further fault coverage

| Fault | Test |
|---|---|
| Authentication failure on submit or reconcile | `tests/test_execution_engine.py::test_auth_failure_rejects_and_opens_breaker`, `tests/test_reconciliation.py::test_auth_failure_during_reconcile_opens_breaker` |
| Stale or empty order book | `test_stale_order_book_aborts`, `test_empty_order_book_aborts` |
| Thin book: partial fill | `test_thin_book_partial_fill_becomes_filled_with_partial_size`, `tests/test_outcome_monitor.py::test_partial_exit_keeps_trade_open` |
| Stop gaps through | `tests/test_simulated_venue.py::test_stop_gaps_through_and_fills_at_the_book` |
| Fast stop-out before the protection check | `test_fast_stop_out_before_protection_check_is_not_a_failure` |
| Second order on a symbol with exposure | `test_second_signal_on_exposed_symbol_is_refused` |
| Orphan position or orphan order on the venue | `tests/test_reconciliation.py::test_orphan_position_blocks_trading_until_gone`, `test_orphan_order_with_our_prefix_alerts` |
| Stop silently removed on the venue | `tests/test_reconciliation.py::test_missing_protective_stop_found_by_reconciler` |
| Governor missing, unhealthy or raising | `tests/test_execution_engine.py::test_missing_governor_fails_closed`, `test_unhealthy_governor_fails_closed`, `test_governor_exception_fails_closed` |
| Concurrent duplicate transitions and journal appends (PostgreSQL) | `tests/test_postgres_integration.py` |

## Proving the tests have teeth

Reverting the engine to the old behaviour (mark an ambiguous submission `cancelled`)
and ignoring stream disconnects makes six of these scenarios fail. Re-run that check
after changing the execution path.

## Fault injection API

```python
venue.faults.add("place_order", "timeout_after_accept")
venue.faults.add("get_order", "not_found", times=3)
venue.faults.add("stream", "duplicate_fills_new_id", times=-1)  # -1 = forever
venue.faults.add("order_book", "stale_book", age_seconds=60)
venue.faults.add("place_order", "no_protective_legs")
await venue.disconnect_stream()
await venue.reconnect_stream()
```

Actions: `timeout_before_accept`, `timeout_after_accept`, `server_error_after_accept`,
`reject`, `auth_failure`, `unavailable`, `delay`, `not_found`, `stale_book`,
`empty_book`, `no_protective_legs`, `duplicate_events`, `duplicate_fills_new_id`,
`reorder_events`, `drop_events`.
