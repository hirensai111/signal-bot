"""Evaluate strategy variants on the DEVELOPMENT period only and log every attempt.

The holdout year (2025-10..2026-09) is never loaded here.  Each run appends one
line per variant to docs/research-log.jsonl so the number of variants tried is on
record when the final holdout result is judged.

    python scripts/research_dev.py signal_bot/research/strategies/trend_breakout.toml \
        --set entry_lookback=20 --set trail_atr=3.5 --note "shorter channel"
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from signal_bot.provenance import git_info
from signal_bot.research.data import load_binance_vision
from signal_bot.research.experiment import load_config, run_experiment

DEV_START, DEV_END = "2021-01", "2025-09"
LOG = Path("docs/research-log.jsonl")


def parse_value(v: str):
    for cast in (int, float):
        try:
            return cast(v)
        except ValueError:
            pass
    return {"true": True, "false": False}.get(v.lower(), v)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("config")
    ap.add_argument("--symbols", default="BTCUSDT,ETHUSDT,SOLUSDT")
    ap.add_argument("--interval", default="1h")
    ap.add_argument("--start", default=DEV_START)
    ap.add_argument("--end", default=DEV_END)
    ap.add_argument("--set", action="append", default=[], help="param=value override")
    ap.add_argument("--note", default="")
    ap.add_argument("--no-log", action="store_true")
    args = ap.parse_args()
    if args.end > DEV_END:
        raise SystemExit(f"--end {args.end} reaches into the holdout; development ends {DEV_END}")
    logging.basicConfig(level=logging.WARNING)

    cfg = load_config(args.config)
    for kv in args.set:
        k, v = kv.split("=", 1)
        section, _, key = k.rpartition(".")
        target = cfg.setdefault(section, {}) if section else cfg.setdefault("params", {})
        target[key] = parse_value(v)
    data = [
        load_binance_vision(s.strip(), args.interval, args.start, args.end)
        for s in args.symbols.split(",")
    ]
    report = run_experiment(cfg, data)
    rows = []
    for r in report["results"]:
        p = r["performance"]
        rows.append(
            {
                "symbol": r["symbol"],
                "trades": p["full"]["trades"],
                "signal_r": p["full"]["signal_expectancy_r"],
                "t": p["full"]["signal_expectancy_t_stat"],
                "exp_r": p["full"]["expectancy_r"],
                "oos_exp_r": p["out_of_sample"]["expectancy_r"],
                "wf_exp_r": r["walk_forward"]["expectancy_r"],
                "wf_pos": r["walk_forward"]["positive_window_share"],
                "ret": p["full"]["net_return_pct"],
                "dd": p["full"]["max_drawdown_pct"],
                "be": r["cost_sensitivity"]["break_even_multiplier"],
                "rand_pct": (r["baselines"]["random_entries"] or {}).get("strategy_percentile"),
            }
        )
    summary = {
        "at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "period": f"{args.start}..{args.end}",
        "strategy": cfg.get("name"),
        "params": report["strategy"]["params"],
        "overrides": args.set,
        "note": args.note,
        "status": report["validation"]["status"],
        "criteria": {c["name"]: c["passed"] for c in report["validation"]["criteria"]},
        "oos_pooled": report["summary"]["out_of_sample_pooled"],
        "symbols": rows,
        "commit": git_info()["commit"][:12],
    }
    if not args.no_log:
        with LOG.open("a") as fh:
            fh.write(json.dumps(summary, default=str) + "\n")
    print(
        json.dumps(
            {k: summary[k] for k in ("strategy", "overrides", "status", "oos_pooled")}, default=str
        )
    )
    for row in rows:
        print("  ", json.dumps(row, default=str))
    failed = [k for k, ok in summary["criteria"].items() if not ok]
    print("   failed criteria:", failed or "none")


if __name__ == "__main__":
    main()
