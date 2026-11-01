"""Performance, execution-quality and period metrics for simulated trades."""

from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd

from signal_bot.research.fill_model import Trade


def _round(x: float, n: int = 4) -> float | None:
    if x is None or (isinstance(x, float) and not math.isfinite(x)):
        return None
    return round(float(x), n)


def max_drawdown(
    equity: pd.Series,
) -> tuple[float, float, pd.Timestamp | None, pd.Timestamp | None]:
    """(max drawdown %, longest underwater duration in days, peak time, trough time)."""
    if equity.empty:
        return 0.0, 0.0, None, None
    peak = equity.cummax()
    dd = equity / peak - 1
    trough = dd.idxmin()
    peak_time = equity.loc[:trough].idxmax()
    underwater = dd < -1e-12
    longest, start = pd.Timedelta(0), None
    for ts, flag in underwater.items():
        if flag and start is None:
            start = ts
        elif not flag and start is not None:
            longest = max(longest, ts - start)
            start = None
    if start is not None:
        longest = max(longest, equity.index[-1] - start)
    return float(dd.min() * 100), longest.total_seconds() / 86400, peak_time, trough


def performance(trades: list[Trade], equity: pd.Series, initial: float) -> dict[str, Any]:
    attempted = len(trades)
    filled = [t for t in trades if t.filled]
    n = len(filled)
    final = float(equity.iloc[-1]) if len(equity) else initial
    dd, dd_days, peak, trough = max_drawdown(equity)
    daily = equity.resample("1D").last().dropna()
    rets = daily.pct_change().dropna()
    sharpe = (
        float(rets.mean() / rets.std() * math.sqrt(365))
        if len(rets) > 2 and rets.std() > 0
        else None
    )
    wins = [t for t in filled if t.net_pnl > 0]
    losses = [t for t in filled if t.net_pnl <= 0]
    gross_win = sum(t.net_pnl for t in wins)
    gross_loss = -sum(t.net_pnl for t in losses)
    hold = [
        (t.exit_time - t.entry_time).total_seconds() / 60
        for t in filled
        if t.exit_time is not None and t.entry_time is not None
    ]
    req = sum(t.qty_requested for t in trades if t.exit_reason != "invalid")
    got = sum(t.qty_filled for t in trades)
    out = {
        "trades": n,
        "signals": attempted,
        "net_return_pct": _round((final / initial - 1) * 100),
        "final_equity": _round(final, 2),
        "net_pnl": _round(final - initial, 2),
        "win_rate": _round(len(wins) / n if n else None),
        "expectancy_r": _round(float(np.mean([t.realized_r for t in filled])) if n else None),
        "expectancy_usd": _round(float(np.mean([t.net_pnl for t in filled])) if n else None, 2),
        "signal_expectancy_r": _round(float(np.mean([t.signal_r for t in filled])) if n else None),
        "profit_factor": _round(gross_win / gross_loss) if gross_loss > 0 else None,
        "max_drawdown_pct": _round(dd),
        "longest_drawdown_days": _round(dd_days, 2),
        "drawdown_peak": str(peak) if peak is not None else None,
        "drawdown_trough": str(trough) if trough is not None else None,
        "sharpe_daily_annualised": _round(sharpe) if sharpe is not None else None,
        "avg_hold_minutes": _round(float(np.mean(hold)) if hold else None, 1),
        "exit_reasons": pd.Series([t.exit_reason for t in trades]).value_counts().to_dict()
        if trades
        else {},
    }
    out["execution"] = {
        "fill_ratio_qty": _round(got / req if req else None),
        "signals_filled": n,
        "no_fill_gap": sum(1 for t in trades if t.exit_reason == "no_fill_gap"),
        "no_fill_liquidity": sum(1 for t in trades if t.exit_reason == "no_fill_liquidity"),
        "partial_fills": sum(1 for t in filled if t.partial),
        "fees": _round(sum(t.fees for t in filled), 2),
        "spread_cost": _round(sum(t.spread_cost for t in filled), 2),
        "slippage_impact_cost": _round(sum(t.impact_cost for t in filled), 2),
        "funding": _round(sum(t.funding for t in filled), 2),
        "execution_drag_r": _round(
            float(np.mean([t.signal_r - t.realized_r for t in filled])) if n else None
        ),
    }
    return out


def monthly(trades: list[Trade], equity: pd.Series) -> list[dict[str, Any]]:
    if equity.empty:
        return []
    month_end = equity.resample("ME").last()
    month_start = equity.resample("ME").first()
    out = []
    filled = [t for t in trades if t.filled and t.exit_time is not None]
    for ts in month_end.index:
        in_month = [
            t for t in filled if t.exit_time.year == ts.year and t.exit_time.month == ts.month
        ]
        ret = (month_end[ts] / month_start[ts] - 1) * 100 if month_start[ts] else 0.0
        out.append(
            {
                "month": ts.strftime("%Y-%m"),
                "return_pct": _round(ret),
                "trades": len(in_month),
                "net_pnl": _round(sum(t.net_pnl for t in in_month), 2),
                "win_rate": _round(sum(t.net_pnl > 0 for t in in_month) / len(in_month))
                if in_month
                else None,
            }
        )
    return out


def drawdown_series(equity: pd.Series) -> pd.Series:
    return (equity / equity.cummax() - 1) * 100
