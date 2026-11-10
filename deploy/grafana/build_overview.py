"""Generate deploy/grafana/dashboards/signal-bot-overview.json (the Grafana home dashboard).

    python deploy/grafana/build_overview.py

Rows: Overview, Trading performance, Risk & operations, Logs.  Every metric it uses
is checked against the bot's registry by tests/test_deploy.py, so a renamed metric
fails CI instead of silently blanking a panel.

Two paper bots can run side by side (docs/deployment.md, "Second paper bot").  The "Bot"
selector at the top switches every Prometheus and log panel between them; the "Trade
database" selector does the same for the PostgreSQL panels (each bot has its own database).
"""

from __future__ import annotations

import json
import re
from pathlib import Path

OUT = Path(__file__).parent / "dashboards" / "signal-bot-overview.json"
PROM = {"type": "prometheus", "uid": "prometheus"}
LOKI = {"type": "loki", "uid": "loki"}
DB = {"type": "grafana-postgresql-datasource", "uid": "${tradedb}"}
# Every bot metric is filtered to the bot picked in the "Bot" selector.
INSTANCE = 'instance="${bot}:8080"'
BOT = f'job="signal-bot", {INSTANCE}'
# Metrics that are not the bot's own (the backup service writes these via node-exporter).
_NOT_BOT = ("signalbot_backup_",)
_BOT_METRIC = re.compile(r"\b((?:signalbot|process)_[a-z0-9_]+)(\{)?")
_LABELS = re.compile(r'((?:[^{}"]|"[^"]*")*)\}')

_id = 0
_y = 0


def _next_id() -> int:
    global _id
    _id += 1
    return _id


def for_bot(expr: str) -> str:
    """Add the selected bot's instance to every bot metric in *expr* that lacks it."""

    def add(m: re.Match) -> str:
        name, brace = m.group(1), m.group(2)
        if name.startswith(_NOT_BOT):
            return m.group(0)
        if brace:
            labels = _LABELS.match(expr, m.end())  # quoted values may contain "}"
            if labels and re.search(r"\binstance\s*=", labels.group(1)):
                return m.group(0)
            return f"{name}{{{INSTANCE}, "
        return f"{name}{{{INSTANCE}}}"

    return _BOT_METRIC.sub(add, expr)


def prom(expr: str, legend: str = "", instant: bool = False, ref: str = "A") -> dict:
    return {
        "datasource": PROM,
        "expr": for_bot(expr),
        "legendFormat": legend,
        "refId": ref,
        "instant": instant,
        "range": not instant,
    }


def sql(query: str, fmt: str = "table", ref: str = "A") -> dict:
    return {
        "datasource": DB,
        "rawSql": query,
        "format": fmt,
        "rawQuery": True,
        "editorMode": "code",
        "refId": ref,
    }


def loki(expr: str, ref: str = "A", query_type: str = "range") -> dict:
    return {"datasource": LOKI, "expr": expr, "refId": ref, "queryType": query_type}


def panel(kind, title, targets, x, y, w, h, *, desc="", unit=None, ds=PROM, **extra) -> dict:
    p = {
        "id": _next_id(),
        "type": kind,
        "title": title,
        "description": desc,
        "datasource": ds,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": targets,
        "fieldConfig": {"defaults": {}, "overrides": []},
        "options": {},
    }
    if unit:
        p["fieldConfig"]["defaults"]["unit"] = unit
    for k, v in extra.items():
        if k == "defaults":
            p["fieldConfig"]["defaults"].update(v)
        elif k == "overrides":
            p["fieldConfig"]["overrides"] = v
        else:
            p[k] = v
    return p


def stat(title, targets, x, y, w=4, h=4, **kw) -> dict:
    kw.setdefault("options", {})
    kw["options"] = {
        "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
        "colorMode": "background",
        "graphMode": "none",
        "textMode": "auto",
        "justifyMode": "center",
        **kw["options"],
    }
    return panel("stat", title, targets, x, y, w, h, **kw)


def thresholds(*steps) -> dict:
    return {"mode": "absolute", "steps": [{"color": c, "value": v} for c, v in steps]}


def row(title: str, y: int) -> dict:
    return {
        "id": _next_id(),
        "type": "row",
        "title": title,
        "collapsed": False,
        "gridPos": {"x": 0, "y": y, "w": 24, "h": 1},
        "panels": [],
    }


def dashed(name: str) -> dict:
    """Override: draw a series as a dashed threshold line."""
    return {
        "matcher": {"id": "byName", "options": name},
        "properties": [
            {"id": "custom.lineStyle", "value": {"fill": "dash", "dash": [10, 10]}},
            {"id": "custom.lineWidth", "value": 2},
            {"id": "color", "value": {"mode": "fixed", "fixedColor": "red"}},
        ],
    }


def build() -> dict:
    panels = []
    y = 0
    # ------------------------------------------------------------------ overview
    panels.append(row("Overview", y))
    y += 1
    panels += [
        stat(
            "Bot",
            [prom(f"up{{{BOT}}}", instant=True)],
            0,
            y,
            3,
            defaults={
                "mappings": [
                    {
                        "type": "value",
                        "options": {
                            "1": {"text": "Online", "color": "green"},
                            "0": {"text": "Offline", "color": "red"},
                        },
                    }
                ],
                "thresholds": thresholds(("red", None), ("green", 1)),
            },
            desc="Prometheus can scrape the bot's /metrics.",
        ),
        stat(
            "Uptime",
            [prom(f"time() - process_start_time_seconds{{{BOT}}}", instant=True)],
            3,
            y,
            3,
            unit="s",
            defaults={"thresholds": thresholds(("orange", None), ("green", 3600))},
        ),
        stat(
            "Strategy",
            [prom("signalbot_build_info", "{{strategy}} {{strategy_version}}", instant=True)],
            6,
            y,
            4,
            options={"textMode": "name", "colorMode": "none"},
            desc="Strategy release and commit running now (labels of signalbot_build_info).",
        ),
        stat(
            "Mode",
            [prom("signalbot_build_info", "{{exec_mode}} · {{commit}}", instant=True)],
            10,
            y,
            4,
            options={"textMode": "name", "colorMode": "none"},
            desc="Must say paper: this deployment refuses live trading (PAPER_ONLY).",
        ),
        stat(
            "Symbols ready",
            [
                prom(
                    "sum(signalbot_trend_symbol_ready * signalbot_trend_symbol_tradable)",
                    "ready",
                    True,
                    "A",
                ),
                prom("sum(signalbot_trend_symbol_tradable)", "tradable", True, "B"),
            ],
            14,
            y,
            3,
            options={"textMode": "value_and_name"},
            defaults={"thresholds": thresholds(("red", None), ("green", 1))},
            desc="Tradable symbols with enough completed 4h history. 0 means the Binance warm-up failed.",
        ),
        stat(
            "Last 4h candle",
            [prom("time() - max(signalbot_trend_last_bar_close_timestamp_seconds)", instant=True)],
            17,
            y,
            3,
            unit="s",
            defaults={
                "thresholds": thresholds(("green", None), ("red", 4 * 3600 + 900)),
                "noValue": "none yet",
            },
            desc="Time since the newest completed 4h bar closed. A new one is due every 4 hours.",
        ),
        stat(
            "Equity",
            [prom("signalbot_portfolio_equity", instant=True)],
            20,
            y,
            4,
            unit="currencyUSD",
            options={"colorMode": "none"},
            desc="Paper account, marked to market.",
        ),
    ]
    y += 4
    panels += [
        stat(
            "Open positions",
            [
                prom("signalbot_portfolio_positions", "open", True, "A"),
                prom('signalbot_portfolio_limit{limit="max_positions"}', "limit", True, "B"),
            ],
            0,
            y,
            4,
            options={"textMode": "value_and_name", "colorMode": "none"},
        ),
        stat(
            "Kill switch",
            [prom("signalbot_kill_switch_active", instant=True)],
            4,
            y,
            3,
            defaults={
                "mappings": [
                    {
                        "type": "value",
                        "options": {
                            "0": {"text": "Off", "color": "green"},
                            "1": {"text": "ENGAGED", "color": "red"},
                        },
                    }
                ]
            },
        ),
        stat(
            "Breakers open",
            [prom("signalbot_circuit_breakers_active", instant=True)],
            7,
            y,
            3,
            defaults={"thresholds": thresholds(("green", None), ("orange", 1))},
        ),
        stat(
            "Restarts (24h)",
            [prom(f"changes(process_start_time_seconds{{{BOT}}}[24h])", instant=True)],
            10,
            y,
            3,
            defaults={"thresholds": thresholds(("green", None), ("orange", 1), ("red", 3))},
        ),
        stat(
            "Last reconciliation",
            [
                prom(
                    "time() - signalbot_reconciliation_last_success_timestamp_seconds", instant=True
                )
            ],
            13,
            y,
            3,
            unit="s",
            defaults={"thresholds": thresholds(("green", None), ("red", 900))},
        ),
        panel(
            "table",
            "Firing alerts",
            [prom('ALERTS{alertstate="firing", alertname!="Watchdog"}', instant=True)],
            16,
            y,
            8,
            4,
            desc="Also sent to Telegram. Silence or inspect them in Alertmanager.",
            transformations=[
                {"id": "labelsToFields", "options": {"mode": "columns"}},
                {
                    "id": "organize",
                    "options": {
                        "excludeByName": {
                            "Time": True,
                            "Value": True,
                            "__name__": True,
                            "alertstate": True,
                            "job": True,
                            "instance": True,
                        }
                    },
                },
            ],
        ),
    ]
    y += 4
    # ------------------------------------------------------------------ performance
    panels.append(row("Trading performance (paper)", y))
    y += 1
    panels += [
        panel(
            "timeseries",
            "Equity, marked to market",
            [
                prom("signalbot_portfolio_equity", "equity"),
                prom("signalbot_portfolio_peak_equity", "peak", ref="B"),
            ],
            0,
            y,
            12,
            8,
            unit="currencyUSD",
            overrides=[dashed("peak")],
        ),
        panel(
            "timeseries",
            "Drawdown from peak, and the portfolio's drawdown levels",
            [
                prom("signalbot_portfolio_drawdown_ratio", "drawdown"),
                prom(
                    'signalbot_portfolio_limit{limit="drawdown_reduce"}', "risk halved at", ref="B"
                ),
                prom(
                    'signalbot_portfolio_limit{limit="drawdown_halt"}', "entries stop at", ref="C"
                ),
            ],
            12,
            y,
            12,
            8,
            unit="percentunit",
            overrides=[dashed("risk halved at"), dashed("entries stop at")],
        ),
    ]
    y += 8
    perf_sql = """SELECT count(*) AS "Closed trades",
  avg((realized_r > 0)::int) AS "Win rate",
  avg(realized_r) AS "Avg R per trade",
  sum(net_pnl) AS "Net P&L",
  sum(fees) AS "Fees",
  sum(funding) AS "Funding"
FROM trade_results WHERE $__timeFilter(closed_at)"""
    panels.append(
        stat(
            "Closed trades in range",
            [sql(perf_sql)],
            0,
            y,
            24,
            4,
            ds=DB,
            options={
                "colorMode": "none",
                "textMode": "value_and_name",
                "reduceOptions": {"calcs": ["lastNotNull"], "fields": "/.*/", "values": False},
            },
            overrides=[
                {
                    "matcher": {"id": "byName", "options": "Win rate"},
                    "properties": [{"id": "unit", "value": "percentunit"}],
                },
                {
                    "matcher": {"id": "byRegexp", "options": "Net P&L|Fees|Funding"},
                    "properties": [{"id": "unit", "value": "currencyUSD"}],
                },
                {
                    "matcher": {"id": "byName", "options": "Avg R per trade"},
                    "properties": [{"id": "decimals", "value": 3}],
                },
            ],
            desc="From the trade_results table: each closed paper trade after fees and funding. R = net P&L / intended risk.",
        )
    )
    y += 4
    panels += [
        panel(
            "timeseries",
            "Cumulative net P&L (closed trades, all time)",
            [
                sql(
                    """SELECT closed_at AS time, sum(net_pnl) OVER (ORDER BY closed_at) AS "net P&L"
FROM trade_results ORDER BY 1""",
                    "time_series",
                )
            ],
            0,
            y,
            12,
            8,
            unit="currencyUSD",
            ds=DB,
        ),
        panel(
            "barchart",
            "R per closed trade",
            [
                sql(
                    """SELECT closed_at AS time, realized_r AS "R" FROM trade_results
WHERE $__timeFilter(closed_at) ORDER BY 1"""
                )
            ],
            12,
            y,
            12,
            8,
            ds=DB,
            desc="Realised R after costs for each trade, oldest first.",
        ),
    ]
    y += 8
    panels += [
        panel(
            "table",
            "Per-symbol performance",
            [
                sql(
                    """SELECT symbol AS "Symbol", count(*) AS "Trades",
  sum((realized_r > 0)::int) AS "Wins", count(*) - sum((realized_r > 0)::int) AS "Losses",
  round(avg(realized_r)::numeric, 3) AS "Avg R", round(sum(net_pnl)::numeric, 2) AS "Net P&L",
  round(sum(fees)::numeric, 2) AS "Fees"
FROM trade_results WHERE $__timeFilter(closed_at) GROUP BY symbol ORDER BY "Net P&L" DESC"""
                )
            ],
            0,
            y,
            12,
            8,
            ds=DB,
        ),
        panel(
            "table",
            "Recent closed trades",
            [
                sql(
                    """SELECT closed_at AS "Closed", symbol AS "Symbol", side AS "Side",
  round(actual_entry::numeric, 6) AS "Entry", round(exit_price::numeric, 6) AS "Exit price",
  exit_reason AS "Exit reason", round(realized_r::numeric, 3) AS "R", round(net_pnl::numeric, 2) AS "Net P&L"
FROM trade_results ORDER BY closed_at DESC LIMIT 50"""
                )
            ],
            12,
            y,
            12,
            8,
            ds=DB,
        ),
    ]
    y += 8
    panels += [
        panel(
            "piechart",
            "Exit reasons",
            [
                sql(
                    """SELECT exit_reason AS metric, count(*) AS value FROM trade_results
WHERE $__timeFilter(closed_at) GROUP BY 1"""
                )
            ],
            0,
            y,
            8,
            8,
            ds=DB,
            options={
                "reduceOptions": {"values": True, "calcs": ["lastNotNull"], "fields": ""},
                "displayLabels": ["name", "value"],
            },
        ),
        panel(
            "table",
            "Signals refused by the portfolio rules",
            [
                sql(
                    """SELECT replace(cancelled_reason, 'portfolio:', '') AS "Rule", count(*) AS "Refused",
  max(created_at) AS "Latest"
FROM signals WHERE cancelled_reason LIKE 'portfolio:%' AND $__timeFilter(created_at)
GROUP BY 1 ORDER BY 2 DESC"""
                )
            ],
            8,
            y,
            8,
            8,
            ds=DB,
            desc="Breakouts the shared account could not take (correlated risk, open risk, entries per bar, ...).",
        ),
        panel(
            "table",
            "Latest trend decisions",
            [
                sql(
                    """SELECT created_at AS "When", symbol AS "Symbol", side AS "Side",
  CASE WHEN status = 'cancelled' THEN coalesce(cancelled_reason, 'cancelled') ELSE status END AS "Outcome",
  planned_contracts AS "Contracts"
FROM signals WHERE strategy = 'trend_breakout' ORDER BY created_at DESC LIMIT 30"""
                )
            ],
            16,
            y,
            8,
            8,
            ds=DB,
        ),
    ]
    y += 8
    # ------------------------------------------------------------------ risk & ops
    panels.append(row("Risk & operations", y))
    y += 1
    entry_note = (
        "Dashed red = the ENTRY limit. Limits gate new entries only: as winners run, "
        "risk to a 3 ATR trailing stop grows past them (research peak 6.9% vs 3%)."
    )
    panels += [
        panel(
            "timeseries",
            "Open risk to stops vs the entry limit",
            [
                prom("signalbot_portfolio_open_risk_ratio", "actual open risk"),
                prom('signalbot_portfolio_limit{limit="max_open_risk"}', "entry limit", ref="B"),
            ],
            0,
            y,
            8,
            8,
            unit="percentunit",
            desc=entry_note,
            overrides=[dashed("entry limit")],
        ),
        panel(
            "timeseries",
            "Correlation-adjusted risk vs the entry limit",
            [
                prom("signalbot_portfolio_correlated_risk_ratio", "actual sqrt(r'Cr)"),
                prom(
                    'signalbot_portfolio_limit{limit="max_correlated_risk"}', "entry limit", ref="B"
                ),
            ],
            8,
            y,
            8,
            8,
            unit="percentunit",
            desc=entry_note,
            overrides=[dashed("entry limit")],
        ),
        panel(
            "timeseries",
            "Gross leverage vs limit",
            [
                prom("signalbot_portfolio_gross_leverage_ratio", "gross leverage"),
                prom(
                    'signalbot_portfolio_limit{limit="max_gross_leverage"}', "entry limit", ref="B"
                ),
            ],
            16,
            y,
            8,
            8,
            unit="x",
            overrides=[dashed("entry limit")],
        ),
    ]
    y += 8
    panels += [
        panel(
            "timeseries",
            "Trend decisions by outcome (per 4h)",
            [
                prom(
                    "sum by (outcome) (increase(signalbot_trend_decisions_total[4h]))",
                    "{{outcome}}",
                )
            ],
            0,
            y,
            8,
            8,
            defaults={
                "custom": {"drawStyle": "bars", "fillOpacity": 60, "stacking": {"mode": "normal"}}
            },
            desc="accepted, or the portfolio rule that refused it.",
        ),
        panel(
            "timeseries",
            "Age of the newest 4h candle by symbol",
            [prom("time() - signalbot_trend_last_bar_close_timestamp_seconds", "{{symbol}}")],
            8,
            y,
            8,
            8,
            unit="s",
            defaults={
                "thresholds": thresholds(("green", None), ("red", 4 * 3600 + 900)),
                "custom": {"thresholdsStyle": {"mode": "line"}},
            },
            desc="Saw-tooth up to 4h is normal; above the red line the feed is stale.",
        ),
        panel(
            "table",
            "Symbols",
            [
                {
                    **prom("signalbot_trend_symbol_tradable", instant=True, ref="A"),
                    "format": "table",
                },
                {**prom("signalbot_trend_symbol_ready", instant=True, ref="B"), "format": "table"},
                {**prom("signalbot_trend_history_bars", instant=True, ref="C"), "format": "table"},
            ],
            16,
            y,
            8,
            8,
            transformations=[
                {"id": "merge", "options": {}},
                {
                    "id": "organize",
                    "options": {
                        "excludeByName": {
                            "Time": True,
                            "job": True,
                            "instance": True,
                            "__name__": True,
                        },
                        "renameByName": {
                            "symbol": "Symbol",
                            "Value #A": "Tradable",
                            "Value #B": "Ready",
                            "Value #C": "4h bars",
                        },
                    },
                },
            ],
        ),
    ]
    y += 8
    panels += [
        panel(
            "timeseries",
            "Protection problems",
            [
                prom("signalbot_positions_unprotected", "positions without a stop"),
                prom(
                    "sum(increase(signalbot_protective_order_failures_total[1h]))",
                    "protective failures (1h)",
                    ref="B",
                ),
                prom(
                    'sum(increase(signalbot_trailing_stop_updates_total{outcome=~"rejected|unknown"}[1h]))',
                    "trailing amend refused/unknown (1h)",
                    ref="C",
                ),
                prom(
                    'sum(increase(signalbot_trailing_stop_updates_total{outcome="moved"}[4h]))',
                    "trailing stops moved (4h)",
                    ref="D",
                ),
                prom("sum(increase(signalbot_time_stops_total[4h]))", "time stops (4h)", ref="E"),
            ],
            0,
            y,
            8,
            8,
        ),
        panel(
            "timeseries",
            "Reconciliation and data issues",
            [
                prom(
                    "sum by (kind) (increase(signalbot_reconciliation_discrepancies_total[1h]))",
                    "{{kind}}",
                ),
                prom(
                    "increase(signalbot_reconciliation_failures_total[1h])",
                    "failed passes",
                    ref="B",
                ),
                prom(
                    "sum by (reason) (increase(signalbot_candles_dropped_total[1h]))",
                    "candles dropped: {{reason}}",
                    ref="C",
                ),
            ],
            8,
            y,
            8,
            8,
        ),
        panel(
            "timeseries",
            "Bot process",
            [
                prom(f"process_resident_memory_bytes{{{BOT}}}", "bot memory (RSS)"),
                prom(f"rate(process_cpu_seconds_total{{{BOT}}}[5m])", "bot CPU (cores)", ref="B"),
            ],
            16,
            y,
            8,
            8,
            unit="bytes",
            overrides=[
                {
                    "matcher": {"id": "byName", "options": "bot CPU (cores)"},
                    "properties": [
                        {"id": "unit", "value": "short"},
                        {"id": "custom.axisPlacement", "value": "right"},
                    ],
                }
            ],
        ),
    ]
    y += 8
    panels += [
        panel(
            "timeseries",
            "Server disk and memory",
            [
                prom('node_filesystem_avail_bytes{mountpoint="/"}', "disk free"),
                prom("node_memory_MemAvailable_bytes", "memory available", ref="B"),
            ],
            0,
            y,
            8,
            8,
            unit="bytes",
            desc="Measure during the first week to size the server (start: 80 GB disk, 8 GB RAM).",
        ),
        stat(
            "Last backup",
            [prom("time() - signalbot_backup_last_success_timestamp_seconds", instant=True)],
            8,
            y,
            4,
            4,
            unit="s",
            defaults={"thresholds": thresholds(("green", None), ("red", 36 * 3600))},
        ),
        stat(
            "Backup size",
            [prom("signalbot_backup_last_size_bytes", instant=True)],
            12,
            y,
            4,
            4,
            unit="bytes",
            options={"colorMode": "none"},
        ),
        stat(
            "Paper equity (broker)",
            [prom("signalbot_paper_equity", instant=True)],
            8,
            y + 4,
            8,
            4,
            unit="currencyUSD",
            options={"colorMode": "none"},
            desc="The paper broker's own ledger: cash plus unrealised P&L on live Delta books.",
        ),
        panel(
            "timeseries",
            "Risk denials by rule (per hour)",
            [prom("sum by (rule) (increase(signalbot_risk_denials_total[1h]))", "{{rule}}")],
            16,
            y,
            8,
            8,
        ),
    ]
    y += 8
    # ------------------------------------------------------------------ logs
    panels.append(row("Logs", y))
    y += 1
    selector = '{service="$bot", level=~"$level", subsystem=~"$subsystem"}'
    filters = ' |~ "(?i)$search" | symbol=~"(?i).*$symbol.*" | order_id=~".*$order.*"'
    # Readable lines (level, logger, message, symbol); the raw JSON stays in the details.
    pretty = (
        ' | json msg="msg", exc="exc"'
        ' | line_format `{{ printf "%-8s" .level }} {{ .logger }}  '
        "{{ if .msg }}{{ .msg }}{{ else }}{{ __line__ }}{{ end }}"
        "{{ if .symbol }}  [{{ .symbol }}]{{ end }}{{ if .exc }}\n{{ .exc }}{{ end }}`"
    )
    volume = loki('sum by (level) (count_over_time({service="$bot"} [$__auto]))')
    volume["legendFormat"] = "{{level}}"
    panels += [
        panel(
            "timeseries",
            "Log lines by level",
            [volume],
            0,
            y,
            24,
            6,
            ds=LOKI,
            defaults={
                "custom": {"drawStyle": "bars", "fillOpacity": 70, "stacking": {"mode": "normal"}}
            },
            overrides=[
                {
                    "matcher": {"id": "byName", "options": n},
                    "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": c}}],
                }
                for n, c in (
                    ("ERROR", "red"),
                    ("CRITICAL", "dark-red"),
                    ("WARNING", "orange"),
                    ("INFO", "green"),
                )
            ],
        ),
        panel(
            "logs",
            "Bot logs (filter with the variables at the top)",
            [loki(selector + filters + pretty)],
            0,
            y + 6,
            24,
            16,
            ds=LOKI,
            options={
                "showTime": True,
                "wrapLogMessage": True,
                "sortOrder": "Descending",
                "enableLogDetails": True,
                "prettifyLogMessage": False,
                "dedupStrategy": "none",
            },
            desc="Level, logger and message per line; expand a line for symbol, order and "
            "correlation ids. Paste a signal's correlation id into 'search' to follow one trade.",
        ),
    ]
    return {
        "uid": "signal-bot-overview",
        "title": "Signal Bot: overview",
        "description": "Paper deployment: health, trading performance, risk and logs.",
        "tags": ["signal-bot"],
        "timezone": "utc",
        "schemaVersion": 39,
        "version": 1,
        "editable": True,
        "graphTooltip": 1,
        "refresh": "1m",
        "time": {"from": "now-7d", "to": "now"},
        "links": [
            {
                "title": "Control room (halt needs the operator token)",
                "type": "link",
                "url": "${control_room}",
                "targetBlank": True,
                "icon": "external link",
            },
            {
                "title": "Operations detail",
                "type": "dashboards",
                "tags": ["signal-bot"],
                "asDropdown": True,
            },
        ],
        "templating": {
            "list": [
                {
                    "name": "bot",
                    "label": "Bot",
                    "type": "custom",
                    "query": "Main (trailing stop) : bot,TP 1.5R (fixed exit) : bot-tp",
                    "current": {"text": "Main (trailing stop)", "value": "bot"},
                    "options": [
                        {"text": "Main (trailing stop)", "value": "bot", "selected": True},
                        {"text": "TP 1.5R (fixed exit)", "value": "bot-tp", "selected": False},
                    ],
                },
                {
                    "name": "tradedb",
                    "label": "Trade database",
                    "type": "datasource",
                    "query": "grafana-postgresql-datasource",
                    "regex": "/^Signal Bot DB/",
                    "current": {"text": "Signal Bot DB", "value": "signalbot-db"},
                },
                {
                    "name": "control_room",
                    "label": "Control room URL",
                    "type": "textbox",
                    "query": "http://127.0.0.1:8080/",
                    "current": {
                        "text": "http://127.0.0.1:8080/",
                        "value": "http://127.0.0.1:8080/",
                    },
                    "hide": 2,
                },
                {
                    "name": "level",
                    "label": "Level",
                    "type": "custom",
                    "multi": True,
                    "includeAll": True,
                    "allValue": ".*",
                    "query": "DEBUG,INFO,WARNING,ERROR,CRITICAL",
                    "current": {"text": ["All"], "value": ["$__all"]},
                },
                {
                    "name": "subsystem",
                    "label": "Subsystem",
                    "type": "query",
                    "datasource": LOKI,
                    "query": {
                        "label": "subsystem",
                        "stream": '{service="$bot"}',
                        "type": 1,
                        "refId": "LokiVariableQueryEditor-VariableQuery",
                    },
                    "multi": True,
                    "includeAll": True,
                    "allValue": ".*",
                    "refresh": 2,
                    "current": {"text": ["All"], "value": ["$__all"]},
                },
                {
                    "name": "search",
                    "label": "Search",
                    "type": "textbox",
                    "query": "",
                    "current": {"text": "", "value": ""},
                },
                {
                    "name": "symbol",
                    "label": "Symbol",
                    "type": "textbox",
                    "query": "",
                    "current": {"text": "", "value": ""},
                },
                {
                    "name": "order",
                    "label": "Order id",
                    "type": "textbox",
                    "query": "",
                    "current": {"text": "", "value": ""},
                },
            ]
        },
        "annotations": {
            "list": [
                {
                    "name": "Bot restarts",
                    "datasource": PROM,
                    "enable": True,
                    "iconColor": "orange",
                    "expr": f"changes(process_start_time_seconds{{{BOT}}}[2m]) > 0",
                    "step": "60s",
                    "titleFormat": "Bot restarted",
                },
            ]
        },
        "panels": panels,
    }


if __name__ == "__main__":
    OUT.write_text(json.dumps(build(), indent=1) + "\n")
    print(f"wrote {OUT}")
