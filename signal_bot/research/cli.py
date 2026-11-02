"""signal-bot-research: reproducible strategy experiments and session reports.

Examples::

    # real data from Binance's public archive (cached + checksum-verified)
    signal-bot-research run --config signal_bot/research/strategies/exhaustion_reversion.toml \\
        --symbols BTCUSDT,ETHUSDT,SOLUSDT --start 2025-10 --end 2026-09 --out reports/

    # deterministic synthetic data (CI replay test)
    signal-bot-research run --config ... --source synthetic --seed 7 --bars 20000 --out reports/

    # paper/live session report from the bot's database
    signal-bot-research session-report --since 2026-10-01 --out reports/
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from signal_bot.research.data import Dataset, load_binance_vision, load_csv, synthetic
from signal_bot.research.experiment import load_config, run_experiment
from signal_bot.research.report import write_report


def _datasets(args) -> list[Dataset]:
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    out = []
    for i, sym in enumerate(symbols):
        if args.source == "binance-vision":
            out.append(
                load_binance_vision(
                    sym, args.data_interval, args.start, args.end, cache_dir=args.cache_dir
                )
            )
        elif args.source == "synthetic":
            out.append(synthetic(sym, args.interval, bars=args.bars, seed=args.seed + i))
        else:
            out.append(load_csv(args.csv, sym, args.interval))
    return out


def cmd_run(args) -> int:
    cfg = load_config(args.config)
    report = run_experiment(cfg, _datasets(args))
    j, h = write_report(report, args.out)
    print(
        json.dumps(
            {
                "json": str(j),
                "html": str(h),
                "status": report["validation"]["status"],
                "report_id": report["report_id"],
            },
            indent=2,
        )
    )
    return 0


def cmd_session(args) -> int:
    from signal_bot.db.connection import AsyncSessionLocal
    from signal_bot.research.session import session_report

    since = datetime.fromisoformat(args.since).replace(tzinfo=timezone.utc)
    report = asyncio.run(session_report(AsyncSessionLocal, since))
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"session-{report['report_id']}.json"
    path.write_text(json.dumps(report, indent=2, default=str))
    print(path)
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    parser = argparse.ArgumentParser(
        prog="signal-bot-research", description=__doc__.splitlines()[0]
    )
    sub = parser.add_subparsers(dest="cmd", required=True)
    run = sub.add_parser("run", help="run an experiment and write JSON + HTML reports")
    run.add_argument("--config", required=True)
    run.add_argument("--symbols", default="BTCUSDT")
    run.add_argument(
        "--source", choices=["binance-vision", "synthetic", "csv"], default="binance-vision"
    )
    run.add_argument("--start", default="2025-10", help="first month (YYYY-MM) for binance-vision")
    run.add_argument("--end", default="2026-09", help="last month (YYYY-MM) for binance-vision")
    run.add_argument("--interval", default="5m", help="bar size for synthetic/csv sources")
    run.add_argument(
        "--data-interval",
        default="1m",
        help="kline size to download for binance-vision (1m for the sweep replay, 1h for trend)",
    )
    run.add_argument("--csv")
    run.add_argument("--seed", type=int, default=7)
    run.add_argument("--bars", type=int, default=20_000)
    run.add_argument("--cache-dir", default="data/cache")
    run.add_argument("--out", default="reports")
    run.set_defaults(fn=cmd_run)
    sess = sub.add_parser("session-report", help="report on a paper/live session from the database")
    sess.add_argument("--since", required=True, help="ISO date/time (UTC)")
    sess.add_argument("--out", default="reports")
    sess.set_defaults(fn=cmd_session)
    args = parser.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
