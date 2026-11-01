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
* A trailing stop keeps a constant distance from the best price reached on earlier
  bars and never loosens. It is checked like any other stop, so a gap fills at the open.
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
| `trend_breakout.toml` | 4h Donchian breakout in the direction of the 200-bar EMA, on above-average volume, with a 2 ATR initial stop and a 3 ATR constant-distance trailing stop (what Delta's `bracket_trail_amount` does) and no fixed target. |
| `liquidity_sweep_v2.toml` | The production sweep entries and stops, skipping stops under 40 bps and replacing the far HTF target with a fixed R target or a trailing exit. |

The **no-trade regime filter** (`RegimeFilter`) wraps any strategy and vetoes trades
when volatility is in its bottom 20% (costs dominate), top 3% (disorderly), or when
Kaufman's efficiency ratio shows a strong trend (bad for mean reversion). Reports show
how many candidates each rule removed.

## Session reports (paper and live)

`signal-bot-research session-report --since 2026-10-01` builds the same report
categories from the bot's database: realised performance and execution quality from
`trade_results`, and reliability from the event journal (stream outages and recovery
times, discrepancies, unknown submissions, protective failures, halts).

## Results: October 2025 to September 2026 (BTC, ETH, SOL)

Twelve months of Binance USD-M 1m data per symbol (525,600 bars each, 100% coverage,
no gaps). Reports are in [docs/reports](reports/). Each JSON file is the record and
pins the code commit, config hash and data snapshot ids.

| Strategy | Verdict | Trades (full year) | Signal R before costs (t-stat) | Realised R after costs | Net return | Break-even cost multiple |
|---|---|---|---|---|---|---|
| Liquidity sweep (production, live modules) | Not validated | 186 / 215 / 183 | −0.01 (−0.1) / +0.12 (0.8) / +0.11 (0.5) | −0.40 / −0.16 / −0.23 | −32% / −17% / −20% | 0x / 0.44x / 0.32x |
| Exhaustion reversion + regime filter | Not validated | 316 / 461 / 475 | −0.02 / −0.03 / −0.06 | −0.28 / −0.26 / −0.30 | −36% / −46% / −51% | 0x |
| Exhaustion reversion, unfiltered | Not validated | 387 / 559 / 581 | −0.02 / −0.05 / −0.03 | −0.28 / −0.28 / −0.26 | −42% / −54% / −53% | 0x |

(BTC / ETH / SOL; 0.5% of equity risked per trade.)

What the reports show:

* **The production strategy has no edge that survives costs.** Its signal edge at the
  intended levels is small and not statistically distinguishable from zero (every 95%
  interval spans zero). Execution costs take 0.28–0.39R per trade, mostly taker fees
  against median stops of only 35–53 bps. ETH and SOL would need costs below about
  0.3–0.45x today's to break even.
* **In-sample results flatter.** The sweep's ETH signal R was +0.25R in sample and −0.22R
  out of sample, and SOL's was +0.39R and −0.42R. A single backtest on the full period
  would have hidden this.
* **The regime filter removes 18–20% of exhaustion trades** and cuts the losses
  (BTC −36% vs −42%, ETH −46% vs −54%) but creates no edge.
* **Neither strategy should trade live.** The obvious next experiments target execution
  rather than signal generation: post-only (maker) entries, a minimum stop distance
  measured in multiples of round-trip cost, and wider structural stops. Each is a config
  change that the same pipeline can validate.

## Strategy iteration (November 2026)

### Why the production strategy loses

* **80% of sweep trades stop out.** The median target sits 3.6–4.8R away (the next HTF level), so few trades reach it.
* **Stops are very tight.** 28–40% of stops are under 25 bps from entry, which puts round-trip taker fees and spread at 0.2–0.4R per trade.
* **The entry has no direction.** Signal R, measured before costs at the intended levels, is indistinguishable from zero.

### Method

| Rule | How it was applied |
|---|---|
| Development and holdout split | Ideas were tried only on **2021-01 to 2025-09** (`scripts/research_dev.py` refuses later dates). **2025-10 to 2026-09** was held back. |
| Every variant is on record | `docs/research-log.jsonl` holds every development run, including the bad ones: 19 trend-breakout variants and 2 sweep v2 variants. |
| Pre-registration | The chosen config was committed and pushed (`6234af6`) before the holdout was run. `scripts/research_holdout.py` refuses a config with uncommitted changes and counts each look at the holdout. |
| Choosing a variant | The pick was the centre of a parameter plateau, not the best single run. Neighbours were tested: volume threshold 1.25/2.0×, channel 40/70 bars, EMA 100/300. All were positive on every development symbol. |
| Warm-up | Holdout runs load bars from 2025-07 to warm up indicators, but trade and measure only from 2025-10 (`validation.evaluation_start`). |

### Candidates

* **Sweep v2** (`liquidity_sweep_v2.toml`): the production entries, skipping stops under 40 bps, with a 2R target.
  * Development (2024-07 to 2025-09): −0.06 / −0.18 / −0.15R per trade.
  * A 60 bps minimum stop with a 2R trailing exit: −0.14 / −0.11 / −0.06R.
  * Signal R stayed near zero (|t| < 1) under every exit tried, so there is no edge to manage. It was dropped without using the holdout.
* **Trend breakout 0.2.0** (`trend_breakout.toml`): long or short only in the direction of the EMA200, and only on 1.5× average volume.
  * 4h Donchian 55 breakout, 2 ATR initial stop, 3 ATR trailing stop, no target.
  * The trailing stop is constant-distance, which is what Delta's `bracket_trail_amount` places on the exchange.

### Trend breakout 0.2.0 results

Risk is 0.5% of equity per trade. R is net of every modelled cost.

| Data | Trades | R per trade | Coins positive | Average net return per coin |
|---|---|---|---|---|
| Development, BTC/ETH/SOL (used for the choices) | 478 | +0.13 | 3 / 3 | +10.1% over 4.75 years |
| Development, 8 coins never used for the choices: XRP, DOGE, BNB, ADA, LINK, LTC, AVAX, DOT | 1284 | +0.05 | 6 / 8 | +4.3% over 4.75 years |
| **Holdout year, BTC/ETH/SOL** (look 1) | 109 | −0.02 | 1 / 3 | −0.4% |
| **Holdout year, the 8 unseen coins** (look 2, config unchanged) | 272 | +0.15 | 6 / 8 | +2.6% |
| **Holdout year, all 11 coins** | 381 | **+0.10** (95% CI −0.05 to +0.26, t = 1.3) | 7 / 11 | +1.8% |
| *Production sweep, holdout year, BTC/ETH/SOL* | 584 | *−0.26* | *0 / 3* | *−23%* |

The holdout year was a bear market: buy-and-hold lost 24–69% on these coins.

Reports:

* [BTC/ETH/SOL holdout](reports/trend_breakout-v0.2.0-e3bbba552778704c.html)
* [Unseen-coin holdout](reports/trend_breakout-v0.2.0-caf2c79e29364b34.html)

### What this does and does not show

* **Costs no longer decide the outcome.**
  * Execution drag is 0.06–0.11R per trade on BTC/ETH/SOL and up to 0.40R on thinner coins, against 0.28–0.39R for the production sweep.
  * On every coin where it is positive in the holdout (7 of 11), it stays positive at 3× costs.
* **The result is much less bad than production, and possibly a small edge.**
  * The same frozen rules lost nothing over the holdout year, against −0.26R per trade for production.
  * They made money on 7 of 11 coins.
* **It is not validated.**
  * The pooled holdout result is not statistically significant: the 95% interval includes zero.
  * Per symbol, the strict criteria fail. Only DOT beats the 95th percentile of random entries.
  * BTC/ETH/SOL, the coins it was tuned on, were flat in the holdout. That is the shrinkage expected after choosing among 19 variants.
* **The holdout year is now used up.**
  * It has been looked at twice: once per coin set, with the same frozen config.
  * Any further change has to be judged on data that does not exist yet, which means forward paper trading.

### Next step

Run trend breakout 0.2.0 as a small, diversified **paper** portfolio across the liquid Delta perpetuals. That needs:

* a 4h signal generator;
* trailing-stop support in the paper broker and the Delta bracket (`bracket_trail_amount`).

Go live only if a few months of paper results confirm a positive expectancy at the costs the session reports measure.
