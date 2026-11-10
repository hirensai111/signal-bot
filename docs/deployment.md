# Paper deployment on an always-on server

One Ubuntu server runs the bot in **paper mode only**, with its database, metrics,
logs, alerts and dashboards. You reach it over Tailscale from your laptop or phone;
nothing is exposed to the internet. No Delta API credentials are needed.

```
 laptop / phone ──Tailscale──► Grafana (dashboards, logs)      ◄── Prometheus ◄── bot /metrics, node-exporter
                             └► control room (status, halt)       Loki ◄── Alloy ◄── container logs
 Telegram ◄── Alertmanager ◄── Prometheus alert rules             PostgreSQL ── daily backup ──► ./backups
```

## What runs (`docker-compose.yml`)

| Service | Purpose | Port (localhost only) | Persistent volume |
|---|---|---|---|
| `bot` | Signal Bot 2.x, `EXEC_MODE=paper`, `PAPER_ONLY=true` (it refuses to start in live mode) | 8080 control room, `/metrics` | `botdata` (paper broker state) |
| `postgres` | Trades, orders, event journal, risk state | none | `pgdata` |
| `db-roles` | One-shot: read-only `grafana_ro` role for trade panels | none | |
| `prometheus` | Metrics, alert rules; 400 days of history, at most 10 GB | 9090 | `prometheus-data` |
| `alertmanager` | Routes alerts to Telegram and the optional dead man's switch | 9093 | `alertmanager-data` |
| `loki` + `alloy` | Searchable logs, 90 days. Alloy reads container output (Promtail is no longer maintained) | 3100 | `loki-data`, `alloy-data` |
| `grafana` | Dashboards; home is *Signal Bot: overview* | 3000 | `grafana-data` |
| `node-exporter` | Server disk, memory, CPU, plus the backup timestamp | none | |
| `backup` | Daily `pg_dump` and paper state into `./backups`, kept 14 days | none | `./backups` on the host |

## Server

| Resource | Starting specification |
|---|---|
| CPU | 4 vCPUs |
| RAM | 8 GB |
| Storage | 80 GB SSD |
| OS | Ubuntu 22.04 or 24.04 |

These are conservative starting points, not measurements. In the sandbox run the bot
used about 200 MB of memory. The soak report prints peak memory, disk growth per day
and days until the disk is full, so you can resize after the first week.

**Region.** The strategy reads Binance USD-M public market data, and Binance refuses
some hosting locations with HTTP 451. Choose a provider region where Binance's terms
allow access. Do not route around a restriction. The preflight checks this from the
server itself before the stack starts.

## Deploy

1. **Choose an exact, CI-green commit** on `main`, e.g. `git rev-parse origin/main`.
2. On the new server:

   ```bash
   curl -fsSLO https://raw.githubusercontent.com/hirensai111/signal-bot/main/deploy/bootstrap-ubuntu.sh
   ```

   ```bash
   sudo bash bootstrap-ubuntu.sh --commit <sha>
   ```

   The script:
   - installs Docker, Tailscale and chrony (NTP);
   - closes inbound ports except SSH and the tailnet;
   - clones the repository at that commit into `/opt/signal-bot`;
   - generates the secrets in `.env` (mode 600);
   - **runs the preflight from the server**;
   - starts the stack;
   - publishes Grafana (`https://<server>.<tailnet>.ts.net/`) and the control room (`:8443`) on your tailnet with `tailscale serve`.

   It stops if the preflight fails.
3. **Telegram.**
   - Create a bot with @BotFather and get your chat id.
   - Set `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` in `/opt/signal-bot/.env`.
   - Then:

   ```bash
   cd /opt/signal-bot && docker compose up -d
   ```

   ```bash
   docker compose run --rm --no-deps bot python -m signal_bot.ops.preflight --skip-db --telegram-test
   ```

4. **Dead man's switch (recommended).** Alertmanager cannot tell you that the whole server is down. Create a check at a heartbeat service such as healthchecks.io (5-minute period), put its ping URL in `DEADMANS_SWITCH_URL`, and run `docker compose up -d alertmanager`. The always-firing `Watchdog` alert pings it; the service alerts you when the pings stop.
5. **Check the routing.** In the control room you should see:
   - Mode **Paper**, venue `paper`;
   - strategy `trend_portfolio 0.1.0`;
   - every symbol **Ready** after warm-up;
   - the Delta universe you expect (`DELTA_MODE=production` lists all 11 coins, read-only public data).

   In Grafana, *Last 4h candle* should count up from 0 and reset at each 4h close (00, 04, 08, 12, 16, 20 UTC).

Preflight output on a good server:

```
PASS  exec mode           EXEC_MODE=paper
PASS  paper-only lock     PAPER_ONLY=true
PASS  binance rest        4h klines ok (last close ...)
PASS  binance websocket   4h kline stream delivering
PASS  clock skew          0.02s vs fapi.binance.com
PASS  delta products      11/11 tradable on Delta production
...
READY: 0 failed, 0 warnings
```

### Updating

```bash
cd /opt/signal-bot && sudo deploy/update.sh <new sha>
```

It builds the image, runs the preflight, restarts the bot and appends to
`deploy-history.log`. Data, `.env` and backups are kept. On restart, reconciliation
and the trailing-stop re-derivation restore open positions' stops.

## Second paper bot (fixed +1.5R exit)

An optional second bot forward-tests one exit variant next to the main bot, on the same
live prices from the same day:

| | Main bot (`bot`) | Second bot (`bot-tp`) |
|---|---|---|
| Strategy | `trend_portfolio.toml`: 2 ATR stop, 3 ATR trailing stop, no target | `trend_portfolio_tp15.toml`: the **same entries**, 2 ATR fixed stop, **take profit at +1.5R**, no trailing |
| Portfolio rules | 0.5% risk per trade, 6 positions, 3% / 2% risk limits, drawdown brakes | identical |
| Paper account | $10,000 | its own $10,000 |
| Database / state | `signal_bot`, volume `botdata` | `signal_bot_tp` (created by `db-roles`), volume `botdata-tp` |
| Control room | `127.0.0.1:8080` | `127.0.0.1:8081` |
| Telegram | answers `/status` | same chat, every message tagged `[TP 1.5R]`, send-only |

Why this variant: see docs/research.md, *Exit variants*. It was chosen after the holdout
year had been seen, so only this forward test can judge it.

**Turn it on** (once, on the server):

```bash
cd /opt/signal-bot && echo 'COMPOSE_PROFILES=tp' | sudo tee -a .env
sudo docker compose up -d
sudo tailscale serve --bg --https=8444 http://127.0.0.1:8081
```

With `COMPOSE_PROFILES=tp` in `.env`, every `docker compose` command, including
`deploy/update.sh`, includes the second bot, so both always run the same code. (The
update script warns if a second bot exists without that setting.) It adds about 250 MB
of memory.

**Watching both:**

- Control rooms: `https://<server>.<tailnet>.ts.net:8443` (main) and `:8444` (second).
- Grafana: the **Bot** selector at the top switches every metric and log panel; set
  **Trade database** to the matching database for the trade-performance panels.
- Alerts are per bot. Telegram alerts about the second bot start with `[TP 1.5R bot]`.
  Prometheus finds the second bot by its container name, so while it is stopped it has
  no target and raises no "bot down" alert. Docker restarts it if it crashes.
- Backups include `signal_bot_tp-<stamp>.dump` and `paper_state_tp-<stamp>.json`;
  `deploy/restore.sh` restores either bot from its own dump.
- Soak report for the second bot:
  `sudo docker compose exec bot-tp python -m signal_bot.ops.soak --prometheus http://prometheus:9090 --instance bot-tp:8080`

**First launch with the main bot's existing positions.** Use this instead of
the fresh-account startup above when comparing exits on matched entries. On the
server, check out the approved release and run the fork with `tp` still disabled
in `.env`:

```bash
cd /opt/signal-bot
sudo bash deploy/fork-paper-tp.sh
```

The command builds and runs preflight before stopping the main bot to capture
its database and paper checkpoint together. It imports the snapshot into the
empty TP database and volume. The copy retains the entry fills, sizes, ledger,
and time exits. The TP copy uses each original fixed stop and the +1.5R target
derived from its original signal, then both bots start on the same image.

Before either bot starts, the fork checks completed one-minute Delta trade
candles after each original entry. If the first bracket touch is in that
history, the TP copy records a simulated fill, trade result and ledger entries
through the ordinary paper execution path. Funding after that simulated exit
is removed from the TP copy. The replay uses candle extrema as price evidence,
the end of the touch minute as the exit time, and the stop first if both levels
were touched in the same minute. An ambiguous touch in the entry minute aborts
the fork. The TP bot uses live books after startup.

`backups/paper-fork-*/fork.json` records the handoff time, inherited levels and
any historical fills. Use it to distinguish replayed outcomes from subsequent
forward paper results. The command requires a fresh TP database and state volume.
If it stops after importing data, retain the snapshot and investigate before
launching the second bot. After a successful fork, add `tp` to
`COMPOSE_PROFILES` in `.env` for future updates and expose port 8444 with the
Tailscale command above.

**Turn it off:** remove `COMPOSE_PROFILES=tp` from `.env` and run
`sudo docker compose --profile tp stop bot-tp`. Its database and volume are kept.

## Access and permissions

- **Nothing public.** Every port is bound to `127.0.0.1`. `tailscale serve` makes
  Grafana and the control room reachable only from devices on your tailnet. Use
  Tailscale ACLs to limit which devices that is.
- **Read-only by default.**
  - Grafana has no anonymous access. Create a Viewer account for day-to-day use and
    keep `admin` for changes.
  - The trade panels use a database role that cannot write (tested: a `DELETE` is
    refused).
  - Grafana has no trading controls.
- **Halting is separately authenticated.** The control room shows status to anyone on
  the tailnet, but halt and resume need `OPERATOR_TOKEN`. Every attempt is in the
  operator audit trail. From the server, `docker compose exec bot python -m
  signal_bot.risk.governor --kill "reason"` also works.

## The dashboard (*Signal Bot: overview*)

| Row | Shows |
|---|---|
| Overview | Bot online/offline, uptime, strategy and commit, mode (must say paper), symbols ready, age of the last 4h candle, equity, open positions vs limit, kill switch, breakers, restarts in 24h, last reconciliation, firing alerts |
| Trading performance | Equity and peak; drawdown against the 10%/20% levels. From the trade tables: closed trades, win rate, average R, net P&L, fees, funding, cumulative P&L, R per trade, per-symbol table, recent trades, exit reasons, signals refused by the portfolio rules, latest decisions |
| Risk & operations | Open risk, correlation-adjusted risk and gross leverage, **each drawn against its entry limit**. Also: decisions by outcome, 4h candle age per symbol, symbol readiness, protection problems (unprotected positions, protective failures, refused trailing-stop moves, time stops), reconciliation, bot memory and CPU, server disk and memory, last backup |
| Logs | Volume by level, and the bot's logs filtered by level, subsystem, free text, symbol and order id |

**Entry limit vs actual exposure.** The portfolio's 3% open-risk and 2% correlated-risk
limits only decide whether a *new* trade may open. Open risk is measured from the
current price to each position's current stop, and a 3 ATR trailing stop sits further
from the price than the 2 ATR initial stop. So as winners run, open risk rises above
the limit: the research peak was 6.9% against 3%. Nothing cuts positions back. The
panels draw the limit as a dashed red line next to the actual value. Alerts fire at
1.5x the limit (warning) and 2.5x (critical).

The dashboard is generated by `deploy/grafana/build_overview.py`. Edit that and run it;
CI fails if the JSON and the script disagree, or if a panel uses a metric the bot does
not export.

### Logs (Grafana → Explore → Loki, or the Logs row)

```logql
{service="bot", level=~"ERROR|CRITICAL"}                       # everything that went wrong
{service="bot", subsystem="execution"} | symbol="BTCUSDT"      # one symbol's orders
{service="bot"} | correlation_id="<signal uuid>"               # one trade, end to end
{service="bot"} |= "Trend batch"                               # every 4h decision batch
```

`level` and `subsystem` (`execution`, `signals`, `risk`, `tracking`, `ingest`, ...)
are labels. `symbol`, `order_id`, `signal_id`, `correlation_id` and `logger` are
structured metadata, so they are searchable without bloating the index.

## Alerts (to Telegram)

The rules are in `deploy/prometheus/alerts.yml` and unit-tested in `alerts.test.yml`.

| Alert | When | Severity |
|---|---|---|
| `BotDown` | No metrics for 2 minutes (crashed, or the server is overloaded) | critical |
| `BotRestarted` | The bot restarted in the last 15 minutes | warning |
| `NotPaperTrading` | The deployment reports any mode other than paper | critical |
| `NoTradableSymbolReady` | No tradable symbol has 4h history after 30 minutes (Binance warm-up failed) | critical |
| `Stale4hCandles` / `...ForSymbol` | No completed 4h candle for over 4h15m | critical / warning |
| `PositionUnprotected`, `ProtectiveOrderFailure` | A position without a stop on the venue | critical |
| `TrailingStopNotMoved` | A trailing-stop amendment was refused or its outcome unknown | warning |
| `ReconciliationFailing` | No successful reconciliation for 15 minutes | critical |
| `PortfolioOpenRiskHigh` / `Extreme`, `PortfolioCorrelatedRiskHigh` | Open exposure 1.5x / 2.5x its entry limit | warning / critical |
| `PortfolioDrawdownRiskReduced` / `Halt` | Drawdown reaches 10% / 20% | warning / critical |
| `KillSwitchEngaged`, `TaskOrBreakerOpen`, `RiskGovernorUnhealthy`, `JournalUnavailable` | Trading blocked | critical / warning |
| `HostDiskFilling`, `HostDiskCritical`, `HostMemoryLow`, `BackupStale`, `BackupMissing` | Server resources and backups | warning / critical |
| `Watchdog` | Always firing; goes only to the dead man's switch | none |

Each Telegram message gives the summary and what to do. While `BotDown` fires, the
bot's other alerts are muted, because they are consequences of it.

## Backups

- **Schedule.** The `backup` service takes a `pg_dump` every 24 hours, plus the paper
  broker's state, into `/opt/signal-bot/backups`. It keeps `BACKUP_RETENTION_DAYS`
  (14) days of dumps.
- **Monitoring.** Grafana shows the age and size of the last backup. `BackupStale`
  alerts after 36 hours.
- **Copy them off the server.** A backup on the same disk does not survive losing the
  server. For example, nightly with `restic` (or `rclone sync`) to object storage:

  ```bash
  # /etc/cron.d/signal-bot-offsite
  30 3 * * * root restic -r s3:<bucket>/signal-bot backup /opt/signal-bot/backups --quiet
  ```

  Grafana dashboards are provisioned from the repository. Prometheus and Loki history
  is useful but not essential.
- **Restore**, after confirming the dump:

  ```bash
  cd /opt/signal-bot && sudo deploy/restore.sh backups/signal_bot-<stamp>.dump backups/paper_state-<stamp>.json
  ```

  For the second paper bot, pass its own files (`signal_bot_tp-<stamp>.dump`,
  `paper_state_tp-<stamp>.json`); the script restores that bot's database instead.

  It stops the bot, restores the database (replacing it), restores the paper state
  and starts the bot. Practise it once during the soak: restore into a scratch
  database (`createdb restore_test`, `pg_restore -d restore_test`) and compare row
  counts.

## 14-day soak test

The soak test checks that the deployment runs correctly, not whether the strategy
makes money; the strategy's forward results need months. Start it once the preflight
is READY and Telegram works.

**Daily, about 2 minutes:**
- **Grafana overview:**
  - Bot online; mode paper.
  - All tradable symbols ready.
  - Last 4h candle under 4 hours old.
  - No firing alerts you have not looked at.
- **Risk & operations:**
  - No unprotected positions.
  - Trailing stops moving when positions are open.
  - Reconciliation recent.
  - Backup under 24 hours old.
- **Logs:**
  - `{service="bot", level=~"ERROR|CRITICAL"}`: the only expected errors are transient
    network retries.
- **Telegram:** every alert received was explained.

**Once in the first week:**
- Run the halt drill: halt from the control room with the token, wait for the
  `KillSwitchEngaged` Telegram alert (a minute or two: alerts are grouped for 30
  seconds), then resume.
- Restart the server (`sudo reboot`). The stack comes back by itself, and the bot
  restores state and stops.
- Practise a restore.

**Reports.** Run the soak report weekly and at the end:

```bash
cd /opt/signal-bot && docker compose exec bot python -m signal_bot.ops.soak \
  --prometheus http://prometheus:9090 --days 14 --markdown /home/bot/data/soak.md
```

**Pass criteria** (the report checks all of them):

| Criterion | Pass |
|---|---|
| Bot reachable | at least 99% of scrapes |
| Unplanned restarts | at most 1 a week (note deploys and reboots) |
| 4h closes processed | at least 95% of expected for every tradable symbol (6 a day) |
| Positions without a stop | never |
| Trailing-stop amendments | none refused or unknown |
| Reconciliation | succeeding (last success under 15 minutes ago) |
| Backups | daily |
| Alerting | the Watchdog fires, and the drills reached Telegram |
| Disk | at least 20% free, and projected days-until-full comfortably long |

After a passed soak, the forward test simply continues. A failed criterion means
fixing the cause and restarting the 14-day clock.

## What was verified before shipping

The stack below ran end to end in the development sandbox with Docker:

- **Containers:** every service started; all scrape targets were up.
- **Alerts:** the bot's real alerts fired (restart, warm-up failure). One reached a
  mock Telegram API as a correctly rendered `sendMessage`, with the token read from a
  file.
- **Logs:** reached Loki with `level`/`subsystem` labels, and structured-metadata
  filters worked.
- **Grafana:** all datasources healthy, and every query on the overview dashboard
  returned without error. The read-only role was refused a `DELETE`.
- **Backups and halt:** a backup restored into a scratch database (17 tables). The
  halt API refused requests without the token and engaged the kill switch with it.

That run found and fixed two deployment bugs that tests could not catch:

- the bot's data volume was root-owned, so the paper broker crashed on its first state
  save;
- the strategy config path did not resolve inside the image, so the bot silently ran
  the old sweep strategy.

The latter now stops startup instead.

Not verified there:

- Tailscale and real Telegram delivery: no account was available.
- Binance market data: the sandbox gets HTTP 451, which is exactly what the preflight
  reports.
