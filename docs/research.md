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

The limits gate **new** entries. Open risk is measured from the current price to the current stop, and a 3 ATR trail on a winner can sit further from the price than the 2 ATR initial stop. Open risk can therefore drift above the entry limits as winners run. Measured with the stop the live bot actually holds at each close, it peaked at 3.2% (development) and 3.0% (holdout). No position is cut to bring it back, because that would be a different strategy. (The 5.6% and 6.9% first reported here came from the stop-timing error described under *Correction* below.)

In trend mode the 2.0 governor defaults are derived from these rules instead of being taken from the sweep era: 6 positions, -6R per day, 18 trades per day, a kill switch after 15 consecutive losses. The 2.0 defaults were 3 positions, -3R and 5 losses. A trend strategy that wins about 37% of trades loses five in a row routinely, so the 2.0 settings would have halted it repeatedly and capped it at 3 positions. Values an operator sets explicitly are kept when stricter.

### Portfolio results

Each run uses the same frozen signal and the same pre-registered rules (config hash `875c250051698836`). The figures below were regenerated at commit `bcbaaa0`, after the stop-timing correction. The first two runs cover the development period (2021-01..2025-09). The third is the holdout year: it was first run at the portfolio level as look 3, and this regeneration is logged as look 4.

| Run | Trading period | Return | Max drawdown | Trades | R per trade (95% CI) | 90-day windows >= 0 | Random percentile | 11 backtests summed onto one account |
|---|---|---|---|---|---|---|---|---|
| Development, Delta listing dates | 2023-12-18..2025-09 | +38.0% (19.8%/yr, Sharpe 1.22) | -8.1% | 380 | +0.19 (+0.03 to +0.34) | 71% (5 of 7) | 94 | +37.4% from 599 trades |
| Development, every coin treated as listed since 2021 | 2021-01..2025-09 | +53.6% (9.5%/yr, Sharpe 0.68) | -16.7%, 790-day drawdown | 1199 | +0.11 (+0.03 to +0.20) | 63% | 100 | +64.9% from 1762 trades |
| **Holdout** | 2025-10..2026-09 | **+14.8%** (Sharpe 0.90) | **-16.6%**, 237-day drawdown | 261 | +0.16 (-0.05 to +0.36) | 3 of 4 | 98 | +19.6% from 381 trades |

* "Random percentile" is the share of 50 random-entry portfolios beaten. Those portfolios use the same portfolio rules, symbols, trade counts and ATR stop and trail geometry.
* "11 backtests summed onto one account" is what treating the per-coin results as one portfolio would claim.

Reports:

* [development](reports/trend_portfolio-dev-875c250051698836.json)
* [development without listing dates](reports/trend_portfolio-dev-unlisted-875c250051698836.json)
* [holdout](reports/trend_portfolio-holdout-875c250051698836.json)

What it shows:

* **Shared capital changes the answer.**
  * The holdout's per-coin results (+0.10R per trade across 11 coins, 7 profitable) summed to +19.6% on one account.
  * Executed as one account the year made +14.8%, with a -16.6% drawdown that lasted 237 days.
  * Breakouts arrive together: 43% of decision bars in the holdout had two or more candidates. They are also correlated, so the correlation cap refused the most entries (141 in the holdout).
* **Criteria.**
  * The holdout and the all-coins development run pass all six criteria (`validated_candidate`).
  * The development run with Delta listing dates fails one: it beat 94% of random-entry portfolios, not 95%.
* **Why that is weaker than it sounds.**
  * The holdout is no longer clean. Besides the logged looks, the exit-variant study below and later forward-test questions were all evaluated on this year.
  * The holdout's expectancy interval still includes zero (-0.05 to +0.36R).
  * The correction that produced these numbers moved results a lot through a few dozen extra entries. That shows how sensitive a portfolio of this size is to which trades get through.
* **Costs.** With listing dates respected it still made +19.0% at 3x costs on development data. The holdout made +12.7% at 2x and +10.5% at 3x. The all-coins run turned negative at 3x (-10.4%).
* **Two issues in my own first report.** Both were found and fixed before the final runs, and the superseded log entries are kept.
  * Windows and annual figures included 2021-2023, before Delta listed anything, which counted the flat curve as "non-negative" windows.
  * The independent-backtest comparison ignored listing dates.

#### Correction (14 November 2026): trailing-stop timing

The first portfolio reports (log entries of 2 November) were too pessimistic for the trailing-stop exit.

* **Live:** the trailing manager moves the stop with bar *t* at its close, before the portfolio decides on that close's breakouts.
* **The simulator:** it applied bar *t*'s extreme only when it stepped bar *t+1*. Exits were identical, but every portfolio decision saw each trailing stop one bar stale. Open and correlated risk were overstated, and entries the live bot would take were refused.
* **Found by:** replaying the first forward-test days against the paper bot's decisions. Fixed in `bcbaaa0` (`research/portfolio.py`, `_ratchet_at_close`), with a test that pins the stop each decision sees.
* **Before the fix** the three runs read +25.1% / +22.3% / +3.4% (353 / 1170 / 250 trades), and none passed the 90-day-window or random-portfolio criteria.
* **What it affects:** the trailing exits only. The fixed-stop +1.5R exit (variant C below, the second paper bot) is unaffected.
* The superseded log entries are kept.

Forward paper trading is still the only clean test left. The trend portfolio is the default `STRATEGY_CONFIG` for exactly that; nothing here supports live trading.

### Exit variants (November 2026, after the holdout)

A question from forward testing: the trailing stop gives back 1.5R from the best price,
so would a fixed profit target do better? Four exits were compared on the same frozen
entries and portfolio rules. **The holdout year had already been seen**, so this is a
fourth look at it and none of these figures is out-of-sample.

| Exit | 2021-01..2025-09 (all coins) | Development, Delta listing dates | Holdout 2025-10..2026-09 |
|---|---|---|---|
| **A. 3 ATR trailing stop, no target (0.2.0)** | **+9.5%/yr, max DD -16.7%, 1199 trades, 38% wins** | **+19.8%/yr, -8.1%** | **+14.8%, -16.6%** |
| B. Target +1.5R, trailing until then | +7.6%/yr, -15.0%, 1524 trades, 38% | +16.4%/yr, -10.3% | +6.2%, -17.6% |
| C. Target +1.5R, fixed 2 ATR stop, no trailing | +9.1%/yr, -15.8%, 1390 trades, 45% | +18.6%/yr, -9.8% | +4.5%, -17.4% |
| D. Target +3R, trailing until then | +3.9%/yr, -17.3%, 1355 trades, 37% | +13.3%/yr, -7.8% | +17.2%, -17.1% |

A, B and D were recomputed after the stop-timing correction (`bcbaaa0`); C has no trail
and is unchanged. Before the correction the table read A +4.3%/yr, +13.4%/yr, +3.4% and
B +7.9%/yr, +16.2%/yr, +6.1%, which made a +1.5R target look better in every period.

What it shows:

* **The trailing-only exit (A) is level with or ahead of every alternative** on both
  development views. Against C the gap is small (+9.5% vs +9.1%/yr, +19.8% vs +18.6%/yr)
  and within noise. In the holdout A is far ahead of C (+14.8% vs +4.5%).
* **The earlier finding was mostly the timing error.** A trailing stop one bar stale made
  open risk look higher, so the portfolio refused entries; exits that close sooner (B, C)
  freed slots and hid the cost. With the correction, B is worse than A everywhere.
* **+3R (D) is mixed:** weakest on development data, best in the holdout year.

Because these numbers come from data already seen, no variant is promoted. C runs as
a **second paper bot** next to the unchanged 0.2.0 exit
(`strategies/trend_portfolio_tp15.toml`, `take_profit_r = 1.5`, `trail_atr = 0`;
docs/deployment.md, *Second paper bot*), so the forward test compares them on the same
prices from the same day. The research code reproduces C exactly from that config
(1390 trades, +9.1%/yr; 312 trades, +4.5% in the holdout).

### Cooling-off pause (November 2026, after the holdout)

Forward testing raised two account-rule questions for the second bot (variant C): the 2%
correlated-risk limit was refusing most new signals while four shorts were open, and
losses arrive in clusters. Variants were compared on the same entries and exits, with
$10k and the live order of operations (stops move before new trades are admitted, on
5-minute candles). **The holdout year had been seen many times by now**, so none of this
is out-of-sample. Each cell is return per year and worst drawdown:

| Variant | 2021-01..2025-09 | Development 2023..2025-09 | Holdout 2025-10..2026-09 |
|---|---|---|---|
| C as deployed (correlated 2%, no pause) | +8.5%, -15.8% | +18.6%, -9.8% | +5.1%, -17.4% |
| Correlated 5% | +15.0%, -15.3% | +17.9%, -13.4% | +6.1%, -17.8% |
| 5% + 24 h cooldown per coin after a loss | +16.1%, -14.4% | +11.8%, -13.1% | +7.6%, -16.7% |
| 5% + 48 h cooldown per coin after a loss | +12.8%, -14.5% | +23.5%, -11.7% | +5.4%, -16.6% |
| **5% + 48 h pause after 5 losing trades in a row** | **+18.6%, -10.9%** | **+19.7%, -10.8%** | **+5.9%, -15.3%** |
| 5% + both | +16.3%, -12.8% | +19.8%, -10.3% | +5.6%, -15.3% |

Average R per trade, deployed C vs 5% + pause: +0.099 vs +0.131 (2021-2025), +0.148 vs
+0.157 (development), +0.045 vs +0.073 (holdout), with fewer trades (1342 vs 1394).

What it shows:

* **The pause after a losing streak was the only variant better in every period.** It
  skips part of the choppy phases in which breakouts keep failing, and resumes by itself.
* **Per-coin cooldowns are noise:** 24 h and 48 h swap between best and worst.
* **5% correlated risk alone is mixed;** most of its gain is in 2021-2022.
* It changes two rules at once, and it was the last of many variants tried on the same
  data, so part of the improvement is probably luck.

So it is not promoted either. It runs as a **third paper bot** next to the second
(`strategies/trend_portfolio_tp15_pause.toml`: `max_correlated_risk = 0.05`,
`loss_pause_after = 5`, `loss_pause_hours = 48`; docs/deployment.md, *Third paper bot*),
started as a copy of the second bot's account. The portfolio rule (`risk/portfolio.py`,
rule 9) counts consecutive closed trades with net P&L <= 0 and refuses new entries until
48 hours after the latest of them; research counts from the simulated closes and the live
bot from its own `trade_results`. The repository's research runner reproduces the variant
from that config (1342 trades, +18.3%/yr, max DD -10.8%; holdout 289 trades, +4.4%,
-15.3%); the small difference from the table is that the table replayed exits on
5-minute candles, while the runner steps whole 4h bars.

### The three paper bots (official reports, 16 November 2026)

All three bots trade the same frozen breakout entries on the same 11 Delta perpetuals with
0.5% risk per trade, $10,000 and the same portfolio rules, except where noted:

| Bot | Config (hash) | Exit | Account rules that differ |
|---|---|---|---|
| 1. Main | `trend_portfolio.toml` (`875c2500…`) | 2 ATR stop, 3 ATR trailing stop, no target | – |
| 2. +1.5R | `trend_portfolio_tp15.toml` (`f1999311…`) | Fixed 2 ATR stop, +1.5R target | – |
| 3. +1.5R + pause | `trend_portfolio_tp15_pause.toml` (`6318e58e…`) | Same as bot 2 | Correlated risk 5% (not 2%); 48 h pause after 5 losses in a row |

Each was run through `scripts/research_portfolio.py` (6 criteria, cost sensitivity,
90-day windows, 50 random-entry portfolios). Bot 1's reports were regenerated at `bcbaaa0`
after the stop-timing correction; bots 2 and 3 at `39a0821`. Bots 2 and 3 were chosen
**after** the holdout year had been seen, so their holdout runs (logged looks 5 and 6) are
in-sample.

| Bot | Period | Return | Per year | Max DD | Trades | Wins | R per trade (95% CI) | 90-day windows >= 0 | Random pct | Verdict |
|---|---|---|---|---|---|---|---|---|---|---|
| Main | Development (Delta dates) | +38.0% | +19.8% | -8.1% | 380 | 40% | +0.19 (+0.03 to +0.34) | 71% | 94 | Not validated (random) |
| Main | 2021-2025, all coins | +53.6% | +9.5% | -16.7% | 1199 | 38% | +0.11 (+0.03 to +0.20) | 63% | 100 | Validated candidate |
| Main | Holdout | +14.8% | +14.9% | -16.6% | 261 | 36% | +0.16 (-0.05 to +0.36) | 75% | 98 | Validated candidate |
| +1.5R | Development | +35.7% | +18.6% | -9.8% | 440 | 47% | +0.15 (+0.03 to +0.27) | 71% | 100 | Validated candidate |
| +1.5R | 2021-2025, all coins | +50.9% | +9.1% | -15.8% | 1390 | 45% | +0.09 (+0.03 to +0.16) | 58% | 100 | Not validated (windows) |
| +1.5R | Holdout | +4.4% | +4.5% | -17.4% | 312 | 43% | +0.04 (-0.10 to +0.18) | 50% | 74 | Not validated (windows, random) |
| Pause | Development | +37.9% | +19.7% | -10.8% | 451 | 47% | +0.16 (+0.04 to +0.27) | 86% | 100 | Validated candidate |
| Pause | 2021-2025, all coins | +121.5% | +18.3% | -10.8% | 1342 | 46% | +0.13 (+0.06 to +0.20) | 79% | 100 | Validated candidate |
| Pause | Holdout | +4.4% | +4.4% | -15.3% | 289 | 44% | +0.06 (-0.08 to +0.21) | 75% | 76 | Not validated (random) |

Costs: at 3x costs bot 1 still made +19.0% (development) and +10.5% (holdout); bot 2
+18.9% and -0.8%; bot 3 +8.6% and 0.0%.

Lowest balance from $10,000 and the largest fall from a peak, in dollars:

| Bot | Holdout | Development | 2021-2025 |
|---|---|---|---|
| Main | $10,000 (never below); -$2,139 | $9,801; -$1,148 | $9,669; -$1,934 |
| +1.5R | $9,912; -$2,093 | $9,938; -$1,259 | $9,818; -$2,023 |
| Pause | $9,797; -$1,767 | $9,938; -$1,294 | $9,818; -$2,091 |

What it shows:

* **In the holdout year the main bot is far ahead** (+14.8% vs +4.4% and +4.4%); on
  development data all three are level (+36% to +38%). The main bot is the only one whose
  rules were fixed before the holdout was run.
* **Bot 3's long view (+121.5%) comes mostly from 2021-2022**, the years in which every
  coin is treated as listed; its looser correlation limit lets it hold more same-direction
  trades in those big trends. Delta-listed periods show no gain over bot 2.
* **No bot fell more than $331 below its starting balance**, but each gave back about
  $1,800-2,100 from a high at least once. A bad stretch at the very start would take the
  account to about $8,000.

### Account size, risk and leverage (November 2026, after the holdout)

Questions from forward testing, answered with `research/portfolio.py` at the corrected
timing. Run with in-memory variants of the configs above (scratch scripts, not in the
repository). Every result is in-sample.

**Margin actually used.** Across all bots and periods, margin locked at 5x averaged
$360-440 (3-4% of the account) and peaked at $2,000-3,250 (17-22%); bot 3 peaked at $4,058.
The bots were flat 25-35% of the time and held 1.5-1.8 positions on average.

**Leverage (`position_leverage`).** Risk per trade fixes the position size, so leverage
only changes margin and how close liquidation is. Higher leverage refuses more trades
whose stop is beyond half the distance to liquidation:

| Bot | Period | 5x (today) | 7x | 10x |
|---|---|---|---|---|
| Main | Holdout / development / 2021-2025 | +14.8% / +38.0% / +53.6% | +13.8% / +28.6% / +56.8% | +5.1% / +17.2% / +70.7% |
| +1.5R | Holdout / development / 2021-2025 | +4.4% / +35.7% / +50.9% | +5.8% / +29.4% / +68.2% | +0.7% / +24.8% / +52.5% |

On Delta-listed data higher leverage lowers returns; the 2021-2022 gain at 10x is from
skipping wide-stop trades in an unusually volatile market. Lifting the 3x gross-notional
cap changes nothing: peak gross leverage was about 1.1x.

**Risk per trade.** On $10,000:

| Bot | Period | 0.5% (today) | 1%, other limits unchanged | 1%, every rule doubled (6% / 4% risk caps, halve at 20%, halt at 40%) |
|---|---|---|---|---|
| Main | Holdout | +14.8%, DD -16.6% | +5.4%, **halted** | +29.0%, DD -31.5% |
| Main | Development | +38.0%, -8.1% | +25.5%, -14.1% | +77.6%, -16.8% |
| Main | 2021-2025 | +53.6%, -16.7% | +50.5%, -19.5% | +101.7%, -34.8% |
| +1.5R | Holdout | +4.4%, -17.4% | +0.4%, **halted** | +6.6%, -32.9% |
| +1.5R | Development | +35.7%, -9.8% | +35.1%, -11.6% | +82.5%, -19.2% |
| +1.5R | 2021-2025 | +50.9%, -15.8% | +55.0%, -16.9% | +161.7%, -25.8% |
| Pause | Holdout | +4.4%, -15.3% | – | +11.9%, -29.3% |
| Pause | Development | +37.9%, -10.8% | – | +86.4%, -20.7% |
| Pause | 2021-2025 | +121.5%, -10.8% | – | +367.8%, -20.9% |

* 1% with today's limits leaves room for only about three positions and hits the 20% halt.
* 1% with every rule doubled roughly doubles both profit and drawdown and never reached the
  40% halt. Drawdowns of -30% to -35% are the price; a future drawdown twice as deep would
  stop the bot at 40%.
* Removing the 20% halt at 0.5% changes nothing: no run at 0.5% came near it.

**Smaller account, same dollar trades.** Scaling every percentage rule by 10,000 / account
keeps each trade at about $50. Profit in dollars stays close, but not equal: risk is a
percentage of the current balance, the drawdown brakes are measured from the peak, and
whole-contract rounding admits different trades.

| Bot | $10,000 at 0.5% | $7,000, rules x10/7 | $5,000, rules x2 |
|---|---|---|---|
| Main (holdout / development / 2021-2025) | +$1,480 / +$3,801 / +$5,356 | +$1,396 / +$4,435 / +$3,018 | +$1,510 / +$3,880 / +$5,534 |
| +1.5R | +$444 / +$3,567 / +$5,087 | +$334 / +$4,395 / +$5,437 | +$349 / +$3,788 / +$5,658 |
| Pause | +$443 / +$3,793 / +$12,153 | +$319 / +$3,566 / +$14,572 | – |

The main bot's $7,000 long view is lower because its 2022-2023 drawdown went to -26% and
kept the risk-halving rule on for 41% of the period (21% at $10,000), through the best
years. Removing the halving rule at $7,000 restored it (+$8,228) but made bot 2 lose $569
and hit the halt in the holdout; moving it to 20% was worse than both. At $7,000 no bot
went below $6,501. At 1% with every rule doubled, $7,000 and $10,000 give nearly the same
percentages (7 of 9 runs within 3 points).

**$1,000.** At 0.5% a trade risks about $5, below one SOL contract (1 SOL, about $6-10 of
risk at a typical stop), so 27-141 SOL signals per period were skipped (plus some AVAX).
Returns: main +11.5% / +33.6% / +46.1%, bot 2 +3.1% / +30.5% / +62.0%, bot 3 +6.7% /
+12.5% / +65.9%. At 1% (rules doubled) far fewer were skipped: main +25.4% / +82.9% /
+101.0%. A server costing about $140 a year needs roughly $1,400 at 10% a year to break
even.

### Exit and timing ideas tested on the main bot (November 2026, after the holdout)

All on the corrected simulator, $10,000, 0.5% risk. Return for holdout / development /
2021-2025:

| Idea | Holdout | Development | 2021-2025 | Verdict |
|---|---|---|---|---|
| **Main bot as deployed** | **+14.8%** | **+38.0%** | **+53.6%** | – |
| R-ladder exit: no trail; at +1.5R lock +1R, then +1R per further 1R | -1.5% | +15.7% | +37.6% | Worse: 150 full -1R losses vs 94; winners cut at +1R |
| Trail 1.5 ATR, moved each 4h close | -3.6% | +10.9% | +4.7% | Worse: wins shrink to +0.8R, 33-77% more trades, fees up to nearly double |
| Trail 1.5 ATR, moved every 5 minutes | -1.5% | +12.6% | -2.1% | Worse |
| Trail 3 ATR, moved every hour | +17.6% | +33.4% | +53.9% | Mixed |
| Trail 3 ATR, moved every 5 minutes | +25.9% | +34.3% | +56.8% | Mixed; would need intrabar stop management live |
| Cooldown per coin after a winning trade, 1 / 3 / 7 / 14 / 30 days | +13.2 / +10.7 / +11.8 / +7.8 / +7.1% | +32.3 / +35.4 / +34.3 / +45.7 / +29.9% | +53.6 / +34.3 / -11.9 (halted) / +33.7 / +50.4% | No length helps consistently |
| 48 h pause for the whole bot after a win of 2R+ / 3R+ | +13.7 / +12.3% | +27.8 / +35.0% | +26.5 / +35.6% | Worse |
| 48 h pause for that coin after a win of 2R+ / 3R+ | +14.9 / +14.9% | +27.9 / +29.6% | +31.5 / +50.4% | Worse |
| Correlated risk 5% | +12.9% | +39.6% | +46.6% | Mixed |
| 48 h pause after 5 losses in a row | +26.8% | +11.5% | +18.7% | Worse on development data |
| Both (bot 3's account rules) | +16.4% | +14.2% | +12.1% | Worse |

The 5-minute trail used Binance 5-minute klines (checksum-verified, cached like the 1h
data); within each 5-minute candle the stop is checked before the best price is updated.
Run with the research timing reproduced exactly, the same code matched the main bot to the
decimal.

What it shows:

* **The trailing exit's profit comes from a few large trends.** Rules that cut winners
  sooner (a tighter trail, an R-ladder, pauses after wins) remove exactly those trades.
* **Pauses after losses suit the +1.5R exit, not the trailing one.** The main bot wins 36-40%
  of trades, so five losses in a row are routine, and its big winners often follow a run
  of failed breakouts. Bot 2 wins about 45%, where five losses in a row more often mean a
  choppy market.
* **A faster-moving 3 ATR trail is the only idea that is not clearly worse**, and it is
  better in two periods of three. It would need the live bot to manage stops intrabar
  (or Delta's native trailing stop); it is not adopted on in-sample evidence.

Bot 2's account under the same per-coin cooldown after a win: holdout +1.5% / +2.7% /
+2.1% / +0.4% / -0.4% and development +33.2% / +17.9% / +24.2% / +27.7% / +18.4% for 1 /
3 / 7 / 14 / 30 days, against +4.4% and +35.7% without. Its trades right after a winning
exit on the same coin were among its best (+0.40R average within 3 days), because the
+1.5R target leaves the rest of the trend for the next breakout.
