"""Fail if two report directories contain different experiment results.

Used by CI to prove that identical inputs (code, config, data snapshot, seed)
produce identical reports.  Timestamps and runtime details are excluded.
"""

import json
import sys
from pathlib import Path

from signal_bot.research.experiment import deterministic_view


def load(directory: str) -> dict[str, dict]:
    reports = {p.name: json.loads(p.read_text()) for p in Path(directory).glob("*.json")}
    if not reports:
        sys.exit(f"no reports in {directory}")
    return reports


def main(a: str, b: str) -> int:
    ra, rb = load(a), load(b)
    if ra.keys() != rb.keys():
        print(f"different report sets: {sorted(ra)} vs {sorted(rb)}")
        return 1
    for name in ra:
        if deterministic_view(ra[name]) != deterministic_view(rb[name]):
            print(f"{name}: results differ between runs")
            return 1
        print(f"{name}: identical (report_id {ra[name]['report_id']})")
    return 0


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:3]))
