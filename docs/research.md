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
| `trend_breakout.toml` | 4h Donchian breakout in the direction of the 200-bar EMA, on above-average volume, with a 2 ATR initial stop and a 3 ATR trailing stop that moves once per completed bar, and no fixed target. |
| `trend_portfolio.toml` | The same frozen signal run as one portfolio on shared capital over the Delta India perpetuals, with the portfolio rules in `[portfolio]` (see *Portfolio* below). This is the default live strategy config. |
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
  * The trailing stop is *stepped*: after each completed bar it moves to the best price so far minus 3 ATR (fixed at entry), never looser. This is not Delta's `bracket_trail_amount`, which trails continuously from the live price; see *Trailing stops in live trading* below.

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

### Next step (done in 2.1)

The paper-trading pieces this called for are built and described below:

* a live 4h signal generator;
* trailing stops through the whole execution path;
* portfolio-level risk.

The portfolio backtest also changes the picture: see *Portfolio*.

## Live parity (`signals/trend.py`, `signals/trend_live.py`)

There is one implementation of the entry rule, `signals.trend.trend_decisions`.

* **Research:** `TrendBreakout.generate` calls it on the full history. A regression check gave identical intents on 11 coins x 57 months and an identical experiment report.
* **Live:** the trend pipeline calls it on the completed 4h candles it holds and keeps the newest bar's decision.
* **Why the two agree:** every indicator is causal and computed left to right, so a bar's value does not depend on later bars.

| Parity test | Result |
|---|---|
| Real Binance BTCUSDT 4h bars, 2024-01..2025-06 (3282 bars, `tests/fixtures`), fed one candle at a time | Every decision identical, bit for bit (bar, side, entry, stop, trail) |
| Live seeded with only 1000 bars (the REST warm-up) vs research running since the first bar | Same decisions; stop and trail within 1e-9 relative (EMA/ATR start-up residual) |
| Research on resampled 1h data vs live on native 4h klines | Same decisions (synthetic) |
| Native Binance 4h klines vs 1h resampled to 4h (BTC, DOGE, 6 months of real data) | OHLC identical; volume differs by at most one float ULP (2.2e-16) on some bars. A decision could differ only if volume sat within 1e-16 of 1.5x its mean |
| **Full stack:** 1500 real 4h bars through pipeline, risk governor, execution engine, simulated venue, trailing manager and outcome monitor | The same 28 trades as the research backtest: same signal bar, exit bar and kind of exit (`tests/test_live_replay.py`) |

What can still differ in live trading, by design:

* **Completed candles only.** A bar trades only if it is decided within `MAX_CANDLE_LAG_SECONDS` of its close. Backfilled or late bars update history but never trade, because research executes at the next open.
* **Warm-up.** A symbol needs 1000 completed bars (`TREND_MIN_HISTORY_BARS`), loaded from Binance REST at start. The EMA200's dependence on its first bar decays to about 5e-5 by then. If Binance REST is unreachable (HTTP 451 in some regions), the symbol stays "warming up" and does not trade.
* **Cross-venue basis.**
  * The engine places entry and stops on Delta at the Binance levels plus the basis measured at submission: Delta's mid minus the Binance close. That measurement includes the few seconds of price movement since the close, and submission aborts beyond 0.15%.
  * The trailing manager keeps that basis for the life of the trade.
  * The full-stack replay found this: when a replayed entry executed at the *next bar's open* instead of seconds after the close, the bar-to-bar gap became a permanent 0.12% stop offset and changed one exit. At production timing the replay matches exactly.
* **Market data source.** Live candles must come from the production Binance feed the research used. `BINANCE_TESTNET` now defaults to false; it only selects market data, since the bot never trades on Binance. The trend mode refuses to start on the synthetic testnet feed.

## Trailing stops in live trading

The researched exit is a **stepped** trail. After every completed bar:

* the stop moves to max(initial stop, highest high since the entry bar opened - 3 ATR) for a long, mirrored for a short;
* it applies to the next bar, and it never loosens.

`TRAIL_MODE=stepped` (default) reproduces it exactly:

* the protective stop is always a resting stop-market order on the exchange;
* `execution/trailing.py` amends it after each 4h close;
* `tests/test_trailing.py` checks the live stop equals the fill model's stop on every bar.

| Situation | What happens |
|---|---|
| Amend refused by the venue | The previous stop stays in place (the position is still protected), an alert is sent, and the next bar retries |
| Amend outcome unknown (timeout) | The venue's stop is re-read and recorded |
| Restart, or bars missed while down | Reconciliation re-derives the stop from candle history and amends a looser venue stop |
| A tighter stop on the venue (e.g. moved by an operator) | Adopted; the stop never loosens |
| The re-derived stop is already through the market | The position is closed at market, as the fill model would gap out at the open |
| Venue lost the stop entirely | The recovery stop is placed at the current trailed level, not the initial one |
| Strategy's last bar (`max_hold_bars` = 180, 30 days) | Time stop: closed at market and recorded as `time_stop` |

`TRAIL_MODE=native` sends Delta's `bracket_trail_amount` instead.

* That stop trails *continuously* from the live price, and Delta documents it as "required if bracket stop price is empty", so there is no separate 2 ATR initial stop. It is a different exit from the researched one and has not been backtested.
* Delta does not document the trail amount's sign or how a trailing stop appears on orders and positions.
* `python -m signal_bot.delta.trail_check --yes-place-testnet-orders` measures all of this on testnet (one contract per leg, always flattened). It also checks that a bracket stop's price can be edited (`PUT /v2/orders`), which stepped mode depends on.
* Run it with a testnet key before any live use, and record the output in the runbook.

## Portfolio (`risk/portfolio.py`, `research/portfolio.py`)

Eleven single-symbol backtests, each with its own $10k and every signal taken, are not one executable portfolio. The portfolio rules decide, at each 4h close, which of that bar's breakouts one shared account can take. The same pure function runs:

* live, before batching, with a governor re-check just before submission;
* in the research simulator, which replays all symbols on one timeline with the unchanged fill model and stepped trail.

| Rule (`trend_portfolio.toml`, set a priori) | Value | Why |
|---|---|---|
| Risk per trade | 0.5% of marked-to-market equity | The researched per-trade risk |
| Size | Whole Delta contracts, rounded down, capped by Delta's `position_size_limit` | What can actually be ordered (BTC 0.001, ETH 0.01, BNB/LTC 0.1, DOGE 100, others 1 per contract) |
| Positions | At most 6, one per symbol | About half the universe |
| Open risk | At most 3% of equity: the loss if every stop is hit from the current price | 6 x 0.5% |
| Correlated risk | sqrt(r' C r) <= 2% of equity, with C the 30-day correlation of 4h returns (unknown pairs = 1) | At typical crypto correlations (0.7) this allows four same-side positions; a long and a short in correlated coins partly offset |
| Gross leverage | Notional at most 3x equity, each position at 5x isolated margin | Margin in use stays under 60% |
| Margin buffer | Stops beyond half the distance to liquidation are refused | The stop, not liquidation, closes a loser |
| Simultaneous entries | At most 3 per bar close, ranked by breakout strength (ATRs beyond the channel), ties by symbol | Breakouts cluster: arrival order on the socket must not decide |
| Drawdown | Risk halved at 10% from the equity peak; entries stop at 20% (governor breaker `portfolio_drawdown`) | Aggregate loss control |
| Universe | Delta India perpetuals as listed (`research/delta_universe.json`), tradable only from each listing date | Delta listed BTC on 2023-12-18 and the others in April 2024 |

The limits gate **new** entries. Open risk is measured from the current price to the current stop, and a 3 ATR trail on a winner can sit further from the price than the 2 ATR initial stop. Open risk therefore drifts above the entry limits as winners run: up to 5.6% (development) and 6.9% (holdout). No position is cut to bring it back, because that would be a different strategy.

In trend mode the 2.0 governor defaults are derived from these rules instead of being taken from the sweep era: 6 positions, -6R per day, 18 trades per day, a kill switch after 15 consecutive losses. The 2.0 defaults were 3 positions, -3R and 5 losses. A trend strategy that wins about 37% of trades loses five in a row routinely, so the 2.0 settings would have halted it repeatedly and capped it at 3 positions. Values an operator sets explicitly are kept when stricter.

### Portfolio results

Each run uses the same frozen signal and the same pre-registered rules (commit `1653067`, config hash `875c250051698836`). The first two runs below cover the development period (2021-01..2025-09). The third is the holdout year: it is the third look at that year and the first at the portfolio level.

| Run | Trading period | Return | Max drawdown | Trades | R per trade (95% CI) | 90-day windows >= 0 | Random percentile | 11 backtests summed onto one account |
|---|---|---|---|---|---|---|---|---|
| Development, Delta listing dates | 2023-12-18..2025-09 | +25.1% (13.4%/yr, Sharpe 0.92) | -10.3% | 353 | +0.15 (-0.00 to +0.30) | 57% | 92 | +37.4% from 599 trades |
| Development, every coin treated as listed since 2021 | 2021-01..2025-09 | +22.3% (4.3%/yr, Sharpe 0.39) | -18.7%, 1048-day drawdown | 1170 | +0.08 (0.00 to +0.17) | 53% | 94 | +64.9% from 1762 trades |
| **Holdout** | 2025-10..2026-09 | **+3.4%** (Sharpe 0.24) | **-16.4%**, 313-day drawdown | 250 | +0.06 (-0.14 to +0.25) | 2 of 4 | 84 | +19.6% from 381 trades |

* "Random percentile" is the share of 50 random-entry portfolios beaten. Those portfolios use the same portfolio rules, symbols, trade counts and ATR stop and trail geometry.
* "11 backtests summed onto one account" is what treating the per-coin results as one portfolio would claim.

Reports:

* [development](reports/trend_portfolio-dev-875c250051698836.json)
* [development without listing dates](reports/trend_portfolio-dev-unlisted-875c250051698836.json)
* [holdout](reports/trend_portfolio-holdout-875c250051698836.json)

What it shows:

* **Shared capital changes the answer.**
  * The holdout's per-coin results (+0.10R per trade across 11 coins, 7 profitable) summed to +19.6% on one account.
  * Executed as one account the year made +3.4%, with a -16.4% drawdown that lasted most of the year.
  * Breakouts arrive together: about 40% of decision bars had two or more candidates, up to 10. They are also correlated, so the correlation cap refused the most entries (134 in the holdout).
* **Not validated.** Every run fails two criteria: at least 60% of 90-day windows non-negative, and beating 95% of random-entry portfolios.
* **What it does have.**
  * Expectancy has stayed positive in every run, but its interval includes zero.
  * It beat 84-94% of random portfolios.
  * With listing dates respected it still made money at 3x costs on development data. The holdout broke even between 2x and 3x costs (+1.7% at 2x, -0.5% at 3x).
* **Two issues in my own first report.** Both were found and fixed before the final runs, and the superseded log entries are kept.
  * Windows and annual figures included 2021-2023, before Delta listed anything, which counted the flat curve as "non-negative" windows.
  * The independent-backtest comparison ignored listing dates.

Forward paper trading is now the only clean test left. The trend portfolio is the default `STRATEGY_CONFIG` for exactly that; nothing here supports live trading.

### Exit variants (November 2026, after the holdout)

A question from forward testing: the trailing stop gives back 1.5R from the best price,
so would a fixed profit target do better? Four exits were compared on the same frozen
entries and portfolio rules. **The holdout year had already been seen**, so this is a
fourth look at it and none of these figures is out-of-sample.

| Exit | 2021-01..2025-09 (all coins) | Development, Delta listing dates | Holdout 2025-10..2026-09 |
|---|---|---|---|
| A. 3 ATR trailing stop, no target (0.2.0) | +4.3%/yr, max DD -18.7%, 1170 trades, 37% wins | +13.4%/yr, -10.3% | +3.4%, -16.4% |
| B. Target +1.5R, trailing until then | +7.9%/yr, -14.5%, 1489 trades, 38% | +16.2%/yr, -9.0% | +6.1%, -16.9% |
| **C. Target +1.5R, fixed 2 ATR stop, no trailing** | **+9.1%/yr, -15.8%, 1390 trades, 45%** | **+18.6%/yr, -9.8%** | **+4.5%, -17.4%** |
| D. Target +3R, trailing until then | +1.3%/yr, -17.6%, 1313 trades, 36% | +9.0%/yr, -9.3% | +5.5%, -16.7% |

What it shows:

* **A +1.5R target beat the trailing-only exit in every period** (B and C), with similar
  or smaller drawdowns. Per trade, B averages less than A (+0.069R vs +0.084R); the gain
  comes from trades closing sooner (1.5-2.0 days vs 2.2), which frees position slots for
  breakouts the portfolio rules otherwise refuse, and from banking moves that reach
  +1.5R and then reverse.
* **B and C swap places** across periods; neither is clearly better. C is simpler (no
  stop amendments) and won on the two development views.
* **+3R is the worst of both:** rarely reached, and it still cuts the big trends.

Because these numbers come from data already seen, variant C is not promoted. It runs as
a **second paper bot** next to the unchanged 0.2.0 exit
(`strategies/trend_portfolio_tp15.toml`, `take_profit_r = 1.5`, `trail_atr = 0`;
docs/deployment.md, *Second paper bot*), so the forward test compares them on the same
prices from the same day. The research code reproduces C exactly from that config
(1390 trades, +9.1%/yr; 312 trades, +4.5% in the holdout).
