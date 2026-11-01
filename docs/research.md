# Strategy research

`signal-bot-research` runs reproducible experiments and writes a versioned JSON
report (the record) plus an HTML rendering (for reading). A strategy is called a
*validated candidate* only if it passes every criterion below. A high historical
win rate on its own is never enough.

## Running

```bash
# 12 months of real 1m USD-M futures data from data.binance.vision (checksum-verified, cached)
signal-bot-research run --config signal_bot/research/strategies/exhaustion_reversion.toml \
  --symbols BTCUSDT,ETHUSDT,SOLUSDT --start 2025-10 --end 2026-09 --out reports/

# the production strategy, replayed through the live modules (slow: ~20 min per symbol-year)
signal-bot-research run --config signal_bot/research/strategies/liquidity_sweep.toml \
  --symbols BTCUSDT,ETHUSDT,SOLUSDT --out reports/

# deterministic synthetic data (what CI uses to prove reproducibility)
signal-bot-research run --config ... --source synthetic --seed 7 --bars 20000 --out reports/
```

## What makes a run reproducible

| Pinned | How |
|---|---|
| Code | Git commit and dirty flag (`provenance.code`) |
| Strategy | `name`, `version` and `config_hash` (sha256 of the TOML) |
| Data | `snapshot_id` per symbol (sha256 of the OHLCV bytes), plus coverage and gaps |
| Execution assumptions | The full cost model and sizing in the report |
| Randomness | None in the fill model. The random-entry baseline uses a fixed `seed` that is recorded. |

`report_id` is derived from strategy, data snapshots, costs and commit, so identical
inputs give an identical id. CI runs the same experiment twice and
`scripts/compare_reports.py` fails the build if any result differs.

## Fill model (`research/fill_model.py`)

Deterministic and price/liquidity-informed. This replaces the old
`random() < no_fill_rate`:

* Decision at the close of bar *i*; execution at the open of bar *i+1*.
* No fill if the next open gaps more than `max_entry_slippage_bps` past the signal.
* Fill price is the next open plus half the spread plus square-root impact,
  `impact_bps = k × bar_range_bps × √(order_notional / bar_notional)`.
* Size is capped at `max_participation` of the bar's notional, so thin bars give partial fills.
* A stop is a stop-market order: it fills at the open if the bar gaps through it, then pays spread, impact and the taker fee.
* A target is a resting limit: it fills at its price (maker fee), or at the open if the bar gaps through it.
* If stop and target are both touched in one bar, the stop is assumed first.
* Funding is charged per 8h boundary; a time stop applies after `max_hold_bars`.

Each trade records **signal R** (the idea at its intended levels) and **realised R**
(net PnL after every cost, divided by the intended risk). The difference is
*execution drag*. Reports show signal quality and execution quality side by side.

## Validation (`research/experiment.py`)

| Check | Default |
|---|---|
| In-sample / out-of-sample split by time | last 33% is out of sample; never used for choices |
| Walk-forward | 90-day train, 30-day test, rolling monthly; parameters are chosen on train (from the config's `[walk_forward.grid]`) and frozen for test |
| Cost sensitivity | every friction scaled ×0, 0.5, 1, 1.5, 2 and 3; reports the break-even multiplier |
| Baselines | buy-and-hold over the same window; 50 seeded random-entry runs with the same trade count, stop distance, reward:risk and holding time |
| Losing periods | monthly table for every symbol, with the count of losing symbol-months |

Pass criteria (all required): at least 30 out-of-sample trades; positive
out-of-sample and walk-forward expectancy after costs; at least 60% of walk-forward
windows positive; break-even cost multiplier of at least 1.5× on every symbol; beats
the 95th percentile of random entries on every symbol.

## Strategies

| Config | What it is |
|---|---|
| `liquidity_sweep.toml` | The production strategy. It is *not* re-implemented: `backtest/runner.py:replay_live_signals` feeds candles through the live HTF scanner, microstructure engine, sweep detector and risk sizer, so research and production cannot drift apart. |
| `exhaustion_reversion.toml` | Fades climactic runs (N same-direction bars covering k×ATR with a volume spike), gated by the no-trade regime filter. |
| `exhaustion_reversion_unfiltered.toml` | The same without the filter, to measure what the filter contributes. |

The **no-trade regime filter** (`RegimeFilter`) wraps any strategy and vetoes trades
when volatility is in its bottom 20% (costs dominate), top 3% (disorderly), or when
Kaufman's efficiency ratio shows a strong trend (bad for mean reversion). Reports show
how many candidates each rule removed.

## Session reports (paper and live)

`signal-bot-research session-report --since 2026-10-01` builds the same report
categories from the bot's database: realised performance and execution quality from
`trade_results`, and reliability from the event journal (stream outages and recovery
times, discrepancies, unknown submissions, protective failures, halts).
