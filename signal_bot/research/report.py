"""Render a versioned experiment report (JSON) as a self-contained HTML page.

The JSON is the source of truth (reproducibility); the HTML is for reading and
for a portfolio.  Charts are server-side SVG with native tooltips and every
chart has a table equivalent, so nothing depends on JavaScript.
"""

from __future__ import annotations

import html
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SERIES = ["var(--s1)", "var(--s2)", "var(--s3)"]

CSS = """
:root{color-scheme:light;--plane:#eef1f0;--surface:#fbfcfb;--ink:#1b2a26;--ink2:#4b5a55;--muted:#74817c;
--rule:#d5dcd9;--grid:#e3e8e6;--accent:#1d4e89;--s1:#2a78d6;--s2:#eb6834;--s3:#1baf7a;--good:#006300;
--bad:#a32424;--good-dot:#0ca30c;--bad-dot:#d03b3b;--shade:rgba(29,78,137,.07)}
@media (prefers-color-scheme:dark){:root:not([data-theme=light]){color-scheme:dark;--plane:#0f1715;--surface:#16201d;
--ink:#e6ece9;--ink2:#b9c4c0;--muted:#8a9893;--rule:#2a3733;--grid:#22302c;--accent:#8fb4e6;--s1:#3987e5;--s2:#d95926;
--s3:#199e70;--good:#3fc33f;--bad:#f08a8a;--shade:rgba(143,180,230,.08)}}
:root[data-theme=dark]{color-scheme:dark;--plane:#0f1715;--surface:#16201d;--ink:#e6ece9;--ink2:#b9c4c0;--muted:#8a9893;
--rule:#2a3733;--grid:#22302c;--accent:#8fb4e6;--s1:#3987e5;--s2:#d95926;--s3:#199e70;--good:#3fc33f;--bad:#f08a8a;
--shade:rgba(143,180,230,.08)}
*{box-sizing:border-box}body{margin:0;background:var(--plane);color:var(--ink);
font:400 15px/1.55 Barlow,system-ui,-apple-system,"Segoe UI",sans-serif}
.page{max-width:1120px;margin:0 auto;padding:28px 16px 64px}
h1{font-family:"Barlow Semi Condensed",Barlow,sans-serif;font-weight:700;font-size:clamp(28px,4vw,40px);line-height:1.1;margin:0}
h2{font-size:21px;margin:36px 0 10px;font-weight:600}h3{font-size:16px;margin:18px 0 8px;font-weight:600}
p{max-width:72ch}.lede{color:var(--ink2);margin:8px 0 0}
.verdict{margin-top:20px;padding:16px 20px;border-radius:4px;background:var(--surface);border:1px solid var(--rule);border-left:8px solid var(--v)}
.verdict[data-ok=true]{--v:var(--good-dot)}.verdict[data-ok=false]{--v:var(--bad-dot)}
.verdict h2{margin:0;font-family:"Barlow Semi Condensed",sans-serif;font-size:26px}
.criteria{list-style:none;padding:0;margin:10px 0 0}.criteria li{padding:4px 0;display:flex;gap:8px}
.criteria li::before{content:"";flex:0 0 10px;height:10px;margin-top:7px;border-radius:50%;background:var(--c)}
.criteria .ok{--c:var(--good-dot)}.criteria .no{--c:var(--bad-dot)}
.kpis{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));background:var(--surface);border:1px solid var(--rule);border-radius:4px;margin-top:18px}
.kpi{padding:14px 18px;border-left:1px solid var(--rule)}.kpi:first-child{border-left:0}
.kpi b{display:block;font-family:"Barlow Semi Condensed",sans-serif;font-size:28px;font-weight:600}.kpi span{color:var(--ink2);font-size:14px}
.panel{background:var(--surface);border:1px solid var(--rule);border-radius:4px;padding:16px 18px;margin-top:14px;overflow-x:auto}
table{border-collapse:collapse;width:100%;font-size:14px}th{text-align:left;color:var(--ink2);font-weight:600;border-bottom:1px solid var(--muted);padding:5px 8px;white-space:nowrap}
td{padding:5px 8px;border-bottom:1px solid var(--rule);font-variant-numeric:tabular-nums}td.r,th.r{text-align:right}
.pos{color:var(--good)}.neg{color:var(--bad)}.muted{color:var(--muted)}
svg.chart{width:100%;height:auto;display:block}svg .grid{stroke:var(--grid)}svg .axis{fill:var(--muted);font-size:11px}
svg .zero{stroke:var(--muted);stroke-dasharray:4 4}svg .line{fill:none;stroke-width:2;stroke-linejoin:round}
svg .label{font-size:12px;fill:var(--ink2)}.legend{display:flex;gap:16px;flex-wrap:wrap;font-size:14px;color:var(--ink2);margin:6px 0}
.legend i{display:inline-block;width:14px;height:3px;border-radius:2px;vertical-align:middle;margin-right:6px}
.note{color:var(--ink2);font-size:14px}.badge{display:inline-block;border:2px solid var(--accent);color:var(--accent);border-radius:3px;padding:0 8px;font-weight:600;font-size:13px;margin-left:8px;vertical-align:middle}
code{font-size:13px}
@media(max-width:720px){.kpis{grid-template-columns:repeat(2,minmax(0,1fr))}.kpi:nth-child(3){border-left:0}.kpi:nth-child(n+3){border-top:1px solid var(--rule)}}
"""


def esc(x: Any) -> str:
    return html.escape("–" if x is None else str(x))


def num(x: Any, d: int = 2, signed: bool = False, suffix: str = "") -> str:
    if x is None:
        return "–"
    if isinstance(x, float) and x == float("inf"):
        return "never"
    v = float(x)
    s = f"{v:+,.{d}f}" if signed else f"{v:,.{d}f}"
    return s + suffix


def money(x: Any) -> str:
    if x is None:
        return "–"
    return f"-${abs(float(x)):,.2f}" if float(x) < 0 else f"${float(x):,.2f}"


def cls(x: Any) -> str:
    if x is None:
        return ""
    return "pos" if float(x) > 0 else "neg" if float(x) < 0 else ""


def line_chart(
    series: list[tuple[str, list[list]]],
    *,
    y_fmt=lambda v: f"{v:,.0f}",
    shade_from: str | None = None,
    zero: float | None = None,
    x_is_time: bool = True,
    width: int = 760,
    height: int = 260,
    title: str = "",
) -> str:
    pts = [(name, [(p[0], float(p[1])) for p in data]) for name, data in series if data]
    if not pts:
        return '<p class="note">No data.</p>'
    L, R, T, B = 64, 110, 10, 26

    def xval(v):
        return datetime.fromisoformat(v).timestamp() if x_is_time else float(v)

    xs = [xval(x) for _, d in pts for x, _ in d]
    ys = [y for _, d in pts for _, y in d] + ([zero] if zero is not None else [])
    x0, x1, y0, y1 = min(xs), max(xs), min(ys), max(ys)
    if y1 - y0 < 1e-9:
        y0, y1 = y0 - 1, y1 + 1
    pad = (y1 - y0) * 0.08
    y0, y1 = y0 - pad, y1 + pad

    def X(v):
        return L + (xval(v) - x0) / max(1e-9, x1 - x0) * (width - L - R)

    def Y(v):
        return T + (1 - (v - y0) / (y1 - y0)) * (height - T - B)

    out = [
        f'<svg class="chart" viewBox="0 0 {width} {height}" role="img" aria-label="{esc(title)}">'
    ]
    if shade_from:
        sx = X(shade_from)
        out.append(
            f'<rect x="{sx:.1f}" y="{T}" width="{width - R - sx:.1f}" height="{height - T - B}" fill="var(--shade)"/>'
            f'<text class="axis" x="{sx + 6:.1f}" y="{T + 12}">out of sample</text>'
        )
    for i in range(5):
        v = y0 + (y1 - y0) * i / 4
        y = Y(v)
        out.append(f'<line class="grid" x1="{L}" x2="{width - R}" y1="{y:.1f}" y2="{y:.1f}"/>')
        out.append(
            f'<text class="axis" x="{L - 8}" y="{y + 4:.1f}" text-anchor="end">{esc(y_fmt(v))}</text>'
        )
    for i in range(5):
        xv = x0 + (x1 - x0) * i / 4
        lab = (
            datetime.fromtimestamp(xv, tz=timezone.utc).strftime("%b %Y")
            if x_is_time
            else f"{xv:g}x"
        )
        anchor = "start" if i == 0 else "end" if i == 4 else "middle"
        out.append(
            f'<text class="axis" x="{L + (xv - x0) / max(1e-9, x1 - x0) * (width - L - R):.1f}" y="{height - 6}" text-anchor="{anchor}">{esc(lab)}</text>'
        )
    if zero is not None:
        out.append(
            f'<line class="zero" x1="{L}" x2="{width - R}" y1="{Y(zero):.1f}" y2="{Y(zero):.1f}"/>'
        )
    ends = []
    for k, (name, data) in enumerate(pts):
        color = SERIES[k % len(SERIES)]
        d = "".join(f"{'L' if j else 'M'}{X(x):.1f},{Y(y):.1f}" for j, (x, y) in enumerate(data))
        out.append(f'<path class="line" d="{d}" stroke="{color}"><title>{esc(name)}</title></path>')
        if not x_is_time:
            for x, y in data:
                out.append(
                    f'<circle cx="{X(x):.1f}" cy="{Y(y):.1f}" r="4" fill="{color}" stroke="var(--surface)" stroke-width="2">'
                    f"<title>{esc(name)}: {esc(y_fmt(y))} at {x}x costs</title></circle>"
                )
        lx, ly = data[-1]
        ends.append([Y(ly), X(lx), f"{name} {y_fmt(ly)}"])
    # Direct labels at line ends, nudged apart so close finishes stay readable.
    ends.sort()
    for i in range(1, len(ends)):
        ends[i][0] = max(ends[i][0], ends[i - 1][0] + 15)
    for y, x, text in ends:
        out.append(f'<text class="label" x="{x + 8:.1f}" y="{y + 4:.1f}">{esc(text)}</text>')
    out.append("</svg>")
    legend = "".join(
        f'<span><i style="background:{SERIES[k % len(SERIES)]}"></i>{esc(n)}</span>'
        for k, (n, _) in enumerate(pts)
    )
    return (f'<div class="legend">{legend}</div>' if len(pts) > 1 else "") + "".join(out)


def _pct_series(equity: list[list]) -> list[list]:
    if not equity:
        return []
    base = equity[0][1] or 1
    return [[t, (v / base - 1) * 100] for t, v in equity]


def render_html(report: dict[str, Any]) -> str:
    s = report["strategy"]
    v = report["validation"]
    ok = v["status"] == "validated_candidate"
    synthetic = any(d["synthetic"] for d in report["data"])
    pooled = report["summary"]["out_of_sample_pooled"]
    results = report["results"]
    split = results[0]["series"]["split"] if results else None
    parts = [
        "<!doctype html><html lang=en><head><meta charset=utf-8>"
        '<meta name=viewport content="width=device-width,initial-scale=1">'
        f"<title>{esc(str(s['name']).replace('_', ' ').capitalize())} research report</title>"
        '<link rel=preconnect href="https://fonts.googleapis.com"><link rel=preconnect href="https://fonts.gstatic.com" crossorigin>'
        '<link href="https://fonts.googleapis.com/css2?family=Barlow:wght@400;500;600&family=Barlow+Semi+Condensed:wght@600;700&display=swap" rel=stylesheet>'
        f"<style>{CSS}</style></head><body><div class=page>",
        f"<h1>{esc(str(s['name']).replace('_', ' ').capitalize())} "
        f"<span class=badge>v{esc(s['version'])}</span></h1>",
        f"<p class=lede>{esc(s.get('description') or '')}</p>",
        f"<p class=note>{esc(', '.join(s['symbols']))} on {esc(s['timeframe'])} bars. "
        f"Report {esc(report['report_id'])}, config {esc(s['config_hash'])}, "
        f"code {esc(report['provenance']['code']['commit'][:12])}"
        f"{' (uncommitted changes)' if report['provenance']['code'].get('dirty') else ''}.</p>",
    ]
    if synthetic:
        parts.append(
            "<p class=note><b>Synthetic data.</b> These numbers test the pipeline, not a market.</p>"
        )
    parts.append(
        f"<section class=verdict data-ok={'true' if ok else 'false'}>"
        f"<h2>{'Validated candidate for paper trading' if ok else 'Not validated'}</h2>"
        f"<p class=note>{esc(v['note'])}</p><ul class=criteria>"
        + "".join(
            f"<li class={'ok' if c['passed'] else 'no'}><span>{esc(c['name'])}: <b>{esc(c['value'])}</b></span></li>"
            for c in v["criteria"]
        )
        + "</ul></section>"
    )
    parts.append(
        "<div class=kpis>"
        f"<div class=kpi><span>Out-of-sample trades</span><b>{esc(pooled.get('trades'))}</b></div>"
        f"<div class=kpi><span>Out-of-sample expectancy</span><b class={cls(pooled.get('expectancy_r'))}>{num(pooled.get('expectancy_r'), 3, True, 'R')}</b></div>"
        f"<div class=kpi><span>Out-of-sample win rate</span><b>{num((pooled.get('win_rate') or 0) * 100, 1, suffix='%')}</b></div>"
        f"<div class=kpi><span>Worst out-of-sample drawdown</span><b>{num(pooled.get('worst_drawdown_pct'), 2, suffix='%')}</b></div>"
        "</div>"
    )

    parts.append("<h2>Equity, all symbols</h2><div class=panel>")
    parts.append(
        line_chart(
            [(r["symbol"], _pct_series(r["series"]["equity"])) for r in results],
            y_fmt=lambda x: f"{x:+.1f}%",
            shade_from=split,
            zero=0.0,
            title="Cumulative return after costs",
        )
    )
    parts.append(
        "<p class=note>Return after fees, spread, impact and funding; each symbol simulated on its own account. "
        "The shaded region was never used to choose parameters.</p></div>"
    )
    parts.append("<h2>Drawdown</h2><div class=panel>")
    parts.append(
        line_chart(
            [(r["symbol"], r["series"]["drawdown_pct"]) for r in results],
            y_fmt=lambda x: f"{x:.1f}%",
            shade_from=split,
            zero=0.0,
            title="Drawdown from running peak",
        )
    )
    parts.append("</div>")

    parts.append(
        "<h2>Performance by window</h2><div class=panel><table><thead><tr><th>Symbol</th><th>Window</th>"
        "<th class=r>Trades</th><th class=r>Win rate</th><th class=r>Expectancy</th><th class=r>Signal R</th>"
        "<th class=r>Profit factor</th><th class=r>Net return</th><th class=r>Max drawdown</th><th class=r>Buy &amp; hold</th></tr></thead><tbody>"
    )
    for r in results:
        for key, label in (
            ("in_sample", "In sample"),
            ("out_of_sample", "Out of sample"),
            ("full", "Full"),
        ):
            p = r["performance"][key]
            bh = r["baselines"]["buy_and_hold_pct"].get(
                "out_of_sample" if key == "out_of_sample" else "full"
            )
            parts.append(
                f"<tr><td>{esc(r['symbol'])}</td><td>{label}</td><td class=r>{esc(p['trades'])}</td>"
                f"<td class=r>{num((p['win_rate'] or 0) * 100, 1, suffix='%') if p['win_rate'] is not None else '–'}</td>"
                f"<td class='r {cls(p['expectancy_r'])}'>{num(p['expectancy_r'], 3, True, 'R')}</td>"
                f"<td class=r>{num(p['signal_expectancy_r'], 3, True, 'R')}</td>"
                f"<td class=r>{num(p['profit_factor'], 2)}</td>"
                f"<td class='r {cls(p['net_return_pct'])}'>{num(p['net_return_pct'], 2, True, '%')}</td>"
                f"<td class=r>{num(p['max_drawdown_pct'], 2, suffix='%')}</td>"
                f"<td class=r>{num(bh if key != 'in_sample' else None, 2, True, '%')}</td></tr>"
            )
    parts.append(
        "</tbody></table><p class=note>Signal R measures the idea at its intended levels; expectancy is what the "
        "account realised after execution. The gap between them is execution cost.</p>"
    )
    parts.append(
        "<h3>Is the edge distinguishable from zero?</h3><table><thead><tr><th>Symbol</th>"
        "<th class=r>Trades</th><th class=r>Signal R, 95% interval</th><th class=r>t-statistic</th>"
        "<th class=r>Realised R, 95% interval</th></tr></thead><tbody>"
    )

    def interval(ci):
        return f"{num(ci[0], 3, True)} to {num(ci[1], 3, True)}" if ci else "–"

    for r in results:
        p = r["performance"]["full"]
        parts.append(
            f"<tr><td>{esc(r['symbol'])}</td><td class=r>{esc(p['trades'])}</td>"
            f"<td class=r>{interval(p.get('signal_expectancy_r_ci95'))}</td>"
            f"<td class=r>{num(p.get('signal_expectancy_t_stat'), 2)}</td>"
            f"<td class=r>{interval(p.get('expectancy_r_ci95'))}</td></tr>"
        )
    parts.append(
        "</tbody></table><p class=note>An interval that spans zero means this sample cannot tell the edge "
        "apart from luck; roughly |t| &gt; 2 is needed before an edge is worth paying costs for.</p></div>"
    )

    parts.append(
        "<h2>Execution quality</h2><div class=panel><table><thead><tr><th>Symbol</th><th class=r>Fill ratio</th>"
        "<th class=r>Gap no-fills</th><th class=r>Liquidity no-fills</th><th class=r>Partial fills</th><th class=r>Fees</th>"
        "<th class=r>Spread</th><th class=r>Slippage</th><th class=r>Funding</th><th class=r>Execution drag</th></tr></thead><tbody>"
    )
    for r in results:
        e = r["performance"]["full"]["execution"]
        parts.append(
            f"<tr><td>{esc(r['symbol'])}</td><td class=r>{num((e['fill_ratio_qty'] or 0) * 100, 1, suffix='%')}</td>"
            f"<td class=r>{esc(e['no_fill_gap'])}</td><td class=r>{esc(e['no_fill_liquidity'])}</td><td class=r>{esc(e['partial_fills'])}</td>"
            f"<td class=r>{money(e['fees'])}</td><td class=r>{money(e['spread_cost'])}</td><td class=r>{money(e['slippage_impact_cost'])}</td>"
            f"<td class=r>{money(e['funding'])}</td><td class=r>{num(e['execution_drag_r'], 3, suffix='R')}</td></tr>"
        )
    parts.append("</tbody></table></div>")

    parts.append("<h2>Cost sensitivity</h2><div class=panel>")
    parts.append(
        line_chart(
            [
                (
                    r["symbol"],
                    [
                        [p["multiplier"], p["expectancy_r"] or 0]
                        for p in r["cost_sensitivity"]["points"]
                    ],
                )
                for r in results
            ],
            y_fmt=lambda x: f"{x:+.2f}R",
            zero=0.0,
            x_is_time=False,
            title="Expectancy as every cost is scaled",
        )
    )
    parts.append(
        "<table><thead><tr><th>Symbol</th><th class=r>Break-even cost multiple</th>"
        + "".join(
            f"<th class=r>{p['multiplier']:g}x</th>"
            for p in results[0]["cost_sensitivity"]["points"]
        )
        + "</tr></thead><tbody>"
    )
    for r in results:
        cs = r["cost_sensitivity"]
        parts.append(
            f"<tr><td>{esc(r['symbol'])}</td><td class=r>{num(cs['break_even_multiplier'], 2, suffix='x')}</td>"
            + "".join(
                f"<td class='r {cls(p['expectancy_r'])}'>{num(p['expectancy_r'], 3, True)}</td>"
                for p in cs["points"]
            )
            + "</tr>"
        )
    parts.append(
        "</tbody></table><p class=note>Fees, spread, impact and funding all scaled together. "
        "0x shows the raw signal edge before any cost.</p></div>"
    )

    parts.append(
        "<h2>Walk-forward</h2><div class=panel><table><thead><tr><th>Symbol</th><th>Train</th><th>Test</th>"
        "<th>Chosen parameters</th><th class=r>Test trades</th><th class=r>Test expectancy</th><th class=r>Test return</th></tr></thead><tbody>"
    )
    for r in results:
        for w in r["walk_forward"]["windows"]:
            params = ", ".join(f"{k}={val}" for k, val in w["params"].items()) or "fixed"
            parts.append(
                f"<tr><td>{esc(r['symbol'])}</td><td>{esc(' to '.join(w['train']))}</td><td>{esc(' to '.join(w['test']))}</td>"
                f"<td>{esc(params)}</td><td class=r>{esc(w['test_trades'])}</td>"
                f"<td class='r {cls(w['test_expectancy_r'])}'>{num(w['test_expectancy_r'], 3, True, 'R')}</td>"
                f"<td class='r {cls(w['test_return_pct'])}'>{num(w['test_return_pct'], 2, True, '%')}</td></tr>"
            )
    parts.append(
        "</tbody></table><p class=note>Parameters are chosen on each train window only and then frozen for the "
        "following test window.</p></div>"
    )

    parts.append(
        "<h2>Losing periods</h2><div class=panel><table><thead><tr><th>Month</th>"
        + "".join(f"<th class=r>{esc(r['symbol'])}</th>" for r in results)
        + "</tr></thead><tbody>"
    )
    months = sorted({m["month"] for r in results for m in r["monthly"]})
    by = {r["symbol"]: {m["month"]: m for m in r["monthly"]} for r in results}
    for mo in months:
        cells = []
        for r in results:
            m = by[r["symbol"]].get(mo)
            val = m["return_pct"] if m else None
            cells.append(
                f"<td class='r {cls(val)}'>{num(val, 2, True, '%')} <span class=muted>({m['trades'] if m else 0})</span></td>"
            )
        parts.append(f"<tr><td>{esc(mo)}</td>{''.join(cells)}</tr>")
    losing = sum(1 for r in results for m in r["monthly"] if (m["return_pct"] or 0) < 0)
    total = sum(len(r["monthly"]) for r in results)
    parts.append(
        f"</tbody></table><p class=note>{losing} of {total} symbol-months lost money. Trade counts in brackets.</p></div>"
    )

    parts.append(
        "<h2>Baselines</h2><div class=panel><table><thead><tr><th>Symbol</th><th class=r>Strategy expectancy</th>"
        "<th class=r>Random entries, mean</th><th class=r>Random 5th–95th pct</th><th class=r>Strategy percentile</th></tr></thead><tbody>"
    )
    for r in results:
        b = r["baselines"]["random_entries"]
        if not b:
            parts.append(
                f"<tr><td>{esc(r['symbol'])}</td><td colspan=4 class=muted>Too few trades for a baseline</td></tr>"
            )
            continue
        parts.append(
            f"<tr><td>{esc(r['symbol'])}</td><td class='r {cls(b['strategy_expectancy_r'])}'>{num(b['strategy_expectancy_r'], 3, True, 'R')}</td>"
            f"<td class=r>{num(b['mean_expectancy_r'], 3, True, 'R')}</td>"
            f"<td class=r>{num(b['p05_expectancy_r'], 3, True)} to {num(b['p95_expectancy_r'], 3, True)}</td>"
            f"<td class=r>{num(b['strategy_percentile'], 0, suffix='th')}</td></tr>"
        )
    seed = report["provenance"].get("seed")
    parts.append(
        f"</tbody></table><p class=note>Random entries keep the strategy's trade count, stop distance, reward:risk and holding "
        f"time; only timing and direction are random (seed {esc(seed)}).</p></div>"
    )

    rf = [(r["symbol"], r["regime_filter"]) for r in results if r.get("regime_filter")]
    if rf:
        parts.append(
            "<h2>No-trade regime filter</h2><div class=panel><table><thead><tr><th>Symbol</th><th class=r>Candidates</th>"
            "<th class=r>Kept</th><th class=r>Low volatility</th><th class=r>High volatility</th><th class=r>Trending</th></tr></thead><tbody>"
        )
        for sym, st in rf:
            parts.append(
                f"<tr><td>{esc(sym)}</td><td class=r>{esc(st['candidates'])}</td><td class=r>{esc(st['kept'])}</td>"
                f"<td class=r>{esc(st['low_volatility'])}</td><td class=r>{esc(st['high_volatility'])}</td><td class=r>{esc(st['trending'])}</td></tr>"
            )
        parts.append("</tbody></table></div>")

    parts.append(
        "<h2>Data</h2><div class=panel><table><thead><tr><th>Symbol</th><th>Source</th><th>Snapshot</th>"
        "<th class=r>Bars</th><th class=r>Coverage</th><th class=r>Gaps</th><th>Range</th></tr></thead><tbody>"
    )
    for d in report["data"]:
        q = d["quality"]
        parts.append(
            f"<tr><td>{esc(d['symbol'])}</td><td>{esc(d['source'])}</td><td><code>{esc(d['snapshot_id'])}</code></td>"
            f"<td class=r>{num(q['bars'], 0)}</td><td class=r>{num(q['coverage'] * 100, 3, suffix='%')}</td>"
            f"<td class=r>{esc(q.get('gap_count', 0))}</td><td>{esc(q.get('start', '')[:10])} to {esc(q.get('end', '')[:10])}</td></tr>"
        )
    parts.append("</tbody></table></div>")

    parts.append(
        "<h2>Limitations</h2><div class=panel><ul>"
        "<li>Prices and volumes are Binance USD-M futures; live execution is on Delta Exchange India, whose "
        "basis, depth and fees differ. The paper broker measures that gap on live Delta books.</li>"
        "<li>Fees assume a 5 bps taker and 2 bps maker schedule and a constant funding rate; real funding "
        "varies and can be large in trends.</li>"
        "<li>Bars hide the order of the high and low: when a stop and a target fall in the same bar the stop "
        "is assumed first, which understates results for wide-range bars.</li>"
        "<li>One 12-month period. The out-of-sample window is its last third, so it covers a single regime.</li>"
        "<li>One position per symbol at a time, as in live trading: signals arriving while a position is open "
        "are skipped.</li></ul></div>"
    )
    c = report["costs"]
    parts.append(
        "<h2>Assumptions and provenance</h2><div class=panel><table><tbody>"
        f"<tr><td>Fees</td><td>taker {c['taker_fee'] * 1e4:g} bps, maker {c['maker_fee'] * 1e4:g} bps</td></tr>"
        f"<tr><td>Spread and impact</td><td>half-spread {c['half_spread_bps']:g} bps, square-root impact coefficient {c['impact_coef']:g}, "
        f"max {c['max_participation'] * 100:g}% of bar volume</td></tr>"
        f"<tr><td>Entry</td><td>next bar open; no fill if it gaps more than {c['max_entry_slippage_bps']:g} bps past the signal</td></tr>"
        f"<tr><td>Funding</td><td>{c['funding_rate_8h'] * 1e4:g} bps per 8h, longs pay</td></tr>"
        f"<tr><td>Sizing</td><td>{report['sizing']['risk_fraction'] * 100:g}% of equity risked per trade, "
        f"max leverage {report['sizing']['max_leverage']:g}x, start ${report['sizing']['initial_equity']:,.0f}</td></tr>"
        f"<tr><td>Strategy parameters</td><td><code>{esc(json.dumps(s['params'], default=str))}</code></td></tr>"
        f"<tr><td>Code</td><td><code>{esc(report['provenance']['code']['commit'])}</code></td></tr>"
        f"<tr><td>Runtime</td><td>{esc(report['provenance']['runtime'])}</td></tr>"
        f"<tr><td>Generated</td><td>{esc(report['generated_at'])}</td></tr>"
        "</tbody></table></div>"
    )
    parts.append("</div></body></html>")
    return "".join(parts)


def write_report(report: dict[str, Any], out_dir: str | Path) -> tuple[Path, Path]:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    stem = f"{report['strategy']['name']}-v{report['strategy']['version']}-{report['report_id']}"
    j = out / f"{stem}.json"
    h = out / f"{stem}.html"
    j.write_text(json.dumps(report, indent=2, default=str))
    h.write_text(render_html(report))
    return j, h
