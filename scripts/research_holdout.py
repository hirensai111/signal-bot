"""Run PRE-REGISTERED strategy configs once on the holdout year and publish the reports.

Refuses to run if the config file has uncommitted changes (the commit is the
pre-registration), and appends a holdout entry to docs/research-log.jsonl so every
look at the holdout is on record.

    python scripts/research_holdout.py signal_bot/research/strategies/trend_breakout.toml
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from signal_bot.provenance import git_info
from signal_bot.research.data import load_binance_vision
from signal_bot.research.experiment import load_config, run_experiment
from signal_bot.research.report import write_report

HOLDOUT_START, HOLDOUT_END = "2025-10", "2026-09"
# Bars loaded before the holdout only warm up indicators (EMA200 on 4h needs ~33 days);
# the experiment trades and measures from HOLDOUT_START.
WARMUP_START = "2025-07"
LOG = Path("docs/research-log.jsonl")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("config")
    ap.add_argument("--symbols", default="BTCUSDT,ETHUSDT,SOLUSDT")
    ap.add_argument("--interval", default="1h", help="1m for strategies that replay the live sweep")
    ap.add_argument("--out", default="docs/reports")
    args = ap.parse_args()
    logging.basicConfig(level=logging.WARNING)

    dirty = subprocess.run(
        ["git", "status", "--porcelain", "--", args.config], capture_output=True, text=True
    ).stdout.strip()
    if dirty:
        raise SystemExit(
            f"{args.config} has uncommitted changes: commit it first (pre-registration)"
        )

    cfg = load_config(args.config)
    # A 12-month holdout can't hold a 365-day train window.  With no parameter grid the
    # "train" window selects nothing, so evaluate the frozen strategy on rolling 90-day
    # windows instead (the config hash still identifies the pre-registered file).
    overrides = {}
    v = cfg.setdefault("validation", {})
    if v.get("walk_forward_train_days", 90) + v.get("walk_forward_test_days", 30) > 300:
        overrides = {"walk_forward_train_days": 90, "walk_forward_test_days": 90}
    overrides["evaluation_start"] = f"{HOLDOUT_START}-01"
    v.update(overrides)
    data = [
        load_binance_vision(s.strip(), args.interval, WARMUP_START, HOLDOUT_END)
        for s in args.symbols.split(",")
    ]
    report = run_experiment(cfg, data)
    j, h = write_report(report, args.out)
    previous = (
        sum(
            1
            for line in LOG.read_text().splitlines()
            if line
            and json.loads(line).get("holdout")
            and json.loads(line).get("strategy") == cfg["name"]
        )
        if LOG.exists()
        else 0
    )
    entry = {
        "at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "holdout": True,
        "holdout_look": previous + 1,
        "period": f"{HOLDOUT_START}..{HOLDOUT_END}",
        "strategy": cfg["name"],
        "version": cfg.get("version"),
        "config_hash": cfg["_hash"],
        "commit": git_info()["commit"][:12],
        "status": report["validation"]["status"],
        "criteria": {c["name"]: c["passed"] for c in report["validation"]["criteria"]},
        "oos_pooled": report["summary"]["out_of_sample_pooled"],
        "report": str(j),
        "holdout_validation_overrides": overrides,
    }
    with LOG.open("a") as fh:
        fh.write(json.dumps(entry, default=str) + "\n")
    print(json.dumps(entry, indent=2, default=str))
    print(h)


if __name__ == "__main__":
    main()
