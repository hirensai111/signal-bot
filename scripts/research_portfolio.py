"""Portfolio backtest of the trend strategy on shared capital and Delta's universe.

    # development period (2021-01..2025-09), Delta listing dates respected
    python scripts/research_portfolio.py --period dev
    # sensitivity: pretend every coin was listed on Delta since 2021 (longer, survivorship-biased)
    python scripts/research_portfolio.py --period dev --ignore-listing
    # holdout year (2025-10..2026-09): config must be committed; every run is a counted look
    python scripts/research_portfolio.py --period holdout

Writes docs/reports/trend_portfolio-<period>-<config hash>.json and appends an entry
to docs/research-log.jsonl.
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

from signal_bot.provenance import git_info
from signal_bot.research.data import load_binance_vision
from signal_bot.research.experiment import load_config
from signal_bot.research.portfolio import evaluate_portfolio, independent_baseline

CONFIG = "signal_bot/research/strategies/trend_portfolio.toml"
LOG = Path("docs/research-log.jsonl")
PERIODS = {
    # (load from, trade from, trade until (exclusive), load until month)
    "dev": ("2021-01", "2021-01-01", "2025-10-01", "2025-09"),
    "holdout": ("2025-01", "2025-10-01", "2026-10-01", "2026-09"),
}
SIGNAL_NAMES = ("trend_breakout", "trend_portfolio")  # same frozen signal: one holdout count


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=CONFIG)
    ap.add_argument("--period", choices=sorted(PERIODS), required=True)
    ap.add_argument("--ignore-listing", action="store_true")
    ap.add_argument("--random-runs", type=int, default=None)
    ap.add_argument("--note", default="")
    ap.add_argument("--out", default="docs/reports")
    ap.add_argument("--no-log", action="store_true")
    args = ap.parse_args()
    logging.basicConfig(level=logging.WARNING)

    if args.period == "holdout":
        dirty = subprocess.run(
            ["git", "status", "--porcelain", "--", args.config], capture_output=True, text=True
        ).stdout.strip()
        if dirty:
            raise SystemExit(f"{args.config} has uncommitted changes: commit it first")
    cfg = load_config(args.config)
    load_from, start, end, load_to = PERIODS[args.period]
    symbols = cfg["universe"]["symbols"]
    t0 = time.time()
    data = [load_binance_vision(s, "1h", load_from, load_to) for s in symbols]
    respect = not args.ignore_listing
    result = evaluate_portfolio(
        cfg, data, start=start, end=end, respect_listing=respect, random_runs=args.random_runs
    )
    result["independent_backtests"] = independent_baseline(cfg, data, start=start, end=end)
    result["meta"] = {
        "strategy": cfg["name"],
        "version": cfg.get("version"),
        "config_hash": cfg["_hash"],
        "commit": git_info()["commit"][:12],
        "period": args.period,
        "trade_window": [start, end],
        "loaded_from": load_from,
        "symbols": symbols,
        "respect_listing_dates": respect,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "runtime_seconds": round(time.time() - t0, 1),
        "data": [{"symbol": d.symbol, "snapshot_id": d.snapshot_id} for d in data],
    }
    suffix = "" if respect else "-unlisted"
    out = Path(args.out) / f"trend_portfolio-{args.period}{suffix}-{cfg['_hash']}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=1, default=str))

    s = result["summary"]
    entry = {
        "at": result["meta"]["generated_at"],
        "period": f"{start}..{end}",
        "strategy": cfg["name"],
        "version": cfg.get("version"),
        "config_hash": cfg["_hash"],
        "commit": result["meta"]["commit"],
        "respect_listing_dates": respect,
        "note": args.note,
        "status": result["status"],
        "criteria": {c["name"]: c["passed"] for c in result["criteria"]},
        "trades": s["trades"],
        "net_return_pct": s["net_return_pct"],
        "max_drawdown_pct": s["max_drawdown_pct"],
        "expectancy_r": s["expectancy_r"],
        "random_percentile": result["random_baseline"]["strategy_percentile"],
        "positive_window_share": result["positive_window_share"],
        "refusals": s["refusals"],
        "report": str(out),
    }
    if args.period == "holdout":
        previous = sum(
            1
            for line in LOG.read_text().splitlines()
            if line
            and json.loads(line).get("holdout")
            and json.loads(line).get("strategy") in SIGNAL_NAMES
        )
        entry["holdout"] = True
        entry["holdout_look"] = previous + 1
    if not args.no_log:
        with LOG.open("a") as fh:
            fh.write(json.dumps(entry, default=str) + "\n")
    print(json.dumps({k: v for k, v in entry.items() if k != "criteria"}, indent=1, default=str))
    for c in result["criteria"]:
        print(f"  [{'x' if c['passed'] else ' '}] {c['name']}")
    print(out)


if __name__ == "__main__":
    main()
