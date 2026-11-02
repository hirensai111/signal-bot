"""Historical market data: loaders, quality checks and snapshot identity.

Sources
-------
* ``binance-vision`` — Binance's public USD-M futures kline archive
  (data.binance.vision), downloaded per month, checksum-verified and cached.
* ``csv`` — any OHLCV CSV with Binance kline columns.
* ``synthetic`` — a seeded regime-switching random walk for CI and demos.
  Results on synthetic data say nothing about real markets and reports label
  them as such.

Every dataset gets a ``snapshot_id`` (sha256 of its canonical OHLCV bytes) so
a report pins exactly which data produced it.
"""

from __future__ import annotations

import functools
import hashlib
import io
import logging
import urllib.request
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

VISION_URL = (
    "https://data.binance.vision/data/futures/um/monthly/klines/{sym}/{iv}/{sym}-{iv}-{ym}.zip"
)
KLINE_COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "trade_count",
    "taker_buy_volume",
    "taker_buy_quote_volume",
    "ignore",
]
OHLCV = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "trade_count",
    "taker_buy_volume",
]
INTERVAL_MINUTES = {"1m": 1, "5m": 5, "15m": 15, "1h": 60, "4h": 240, "1d": 1440}


@dataclass
class Dataset:
    symbol: str
    interval: str
    frame: pd.DataFrame  # index: UTC open_time; columns: OHLCV
    source: str
    synthetic: bool = False

    @functools.cached_property
    def snapshot_id(self) -> str:
        return snapshot_id(self.frame)

    def quality(self) -> dict:
        return data_quality(self.frame, self.interval)

    def slice(self, start=None, end=None) -> Dataset:
        frame = self.frame
        if start is not None:
            frame = frame[frame.index >= _utc_ts(start)]
        if end is not None:
            frame = frame[frame.index < _utc_ts(end)]
        return Dataset(self.symbol, self.interval, frame, self.source, self.synthetic)


def _utc_ts(value) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def snapshot_id(frame: pd.DataFrame) -> str:
    canon = frame[["open", "high", "low", "close", "volume"]].astype("float64").round(10)
    payload = canon.to_csv(date_format="%Y-%m-%dT%H:%M:%SZ", float_format="%.10g").encode()
    return hashlib.sha256(payload).hexdigest()[:20]


def data_quality(frame: pd.DataFrame, interval: str) -> dict:
    step = pd.Timedelta(minutes=INTERVAL_MINUTES[interval])
    if frame.empty:
        return {"bars": 0, "expected_bars": 0, "coverage": 0.0, "gaps": [], "duplicates": 0}
    expected = int((frame.index[-1] - frame.index[0]) / step) + 1
    diffs = frame.index.to_series().diff().dropna()
    gap_rows = diffs[diffs > step]
    gaps = [
        {
            "after": str(ts - d),
            "missing_bars": int(d / step) - 1,
        }
        for ts, d in gap_rows.items()
    ]
    bad = int(
        (
            (frame["high"] < frame[["open", "close"]].max(axis=1))
            | (frame["low"] > frame[["open", "close"]].min(axis=1))
        ).sum()
    )
    return {
        "bars": int(len(frame)),
        "expected_bars": expected,
        "coverage": round(len(frame) / expected, 6) if expected else 0.0,
        "gaps": gaps[:50],
        "gap_count": len(gaps),
        "missing_bars": int(sum(g["missing_bars"] for g in gaps)),
        "duplicates": int(frame.index.duplicated().sum()),
        "ohlc_violations": bad,
        "start": str(frame.index[0]),
        "end": str(frame.index[-1]),
    }


def resample(frame: pd.DataFrame, interval: str) -> pd.DataFrame:
    rule = f"{INTERVAL_MINUTES[interval]}min"
    agg = frame.resample(rule, label="left", closed="left").agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
            "quote_volume": "sum",
            "trade_count": "sum",
            "taker_buy_volume": "sum",
        }
    )
    return agg.dropna(subset=["open"])


# ---------------------------------------------------------------------------
# loaders
# ---------------------------------------------------------------------------


def _parse_kline_csv(raw: bytes) -> pd.DataFrame:
    text = raw.decode()
    has_header = text.split(",", 1)[0].strip().isalpha() or text.startswith("open_time")
    frame = pd.read_csv(io.StringIO(text), header=0 if has_header else None)
    frame.columns = KLINE_COLUMNS[: len(frame.columns)]
    frame["open_time"] = pd.to_datetime(frame["open_time"].astype("int64"), unit="ms", utc=True)
    frame = frame.set_index("open_time").sort_index()
    for col in OHLCV:
        frame[col] = pd.to_numeric(frame[col])
    return frame[OHLCV]


def _months(start: str, end: str) -> list[str]:
    s = datetime.strptime(start, "%Y-%m")
    e = datetime.strptime(end, "%Y-%m")
    out = []
    while s <= e:
        out.append(s.strftime("%Y-%m"))
        s = datetime(s.year + (s.month // 12), s.month % 12 + 1, 1)
    return out


def _fetch(url: str, timeout: float = 60) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "signal-bot-research/2"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 - fixed https host
        return resp.read()


def load_binance_vision(
    symbol: str, interval: str, start_month: str, end_month: str, cache_dir: str = "data/cache"
) -> Dataset:
    cache = Path(cache_dir) / "binance_vision" / symbol / interval
    cache.mkdir(parents=True, exist_ok=True)
    frames = []
    for ym in _months(start_month, end_month):
        path = cache / f"{symbol}-{interval}-{ym}.csv.gz"
        if not path.exists():
            url = VISION_URL.format(sym=symbol, iv=interval, ym=ym)
            logger.info("Downloading %s", url)
            blob = _fetch(url)
            expected = _fetch(url + ".CHECKSUM").decode().split()[0]
            actual = hashlib.sha256(blob).hexdigest()
            if actual != expected:
                raise ValueError(f"checksum mismatch for {url}: {actual} != {expected}")
            with zipfile.ZipFile(io.BytesIO(blob)) as zf:
                raw = zf.read(zf.namelist()[0])
            frame = _parse_kline_csv(raw)
            frame.to_csv(path, compression="gzip")
        frame = pd.read_csv(path, index_col=0, parse_dates=True, compression="gzip")
        frame.index = pd.to_datetime(frame.index, utc=True)
        frames.append(frame)
    full = pd.concat(frames).sort_index()
    full = full[~full.index.duplicated(keep="first")]
    return Dataset(
        symbol, interval, full, f"binance-vision:{symbol}:{interval}:{start_month}..{end_month}"
    )


def load_csv(path: str, symbol: str, interval: str) -> Dataset:
    frame = _parse_kline_csv(Path(path).read_bytes())
    return Dataset(symbol, interval, frame, f"csv:{Path(path).name}")


def synthetic(
    symbol: str = "SYNTHUSDT",
    interval: str = "5m",
    bars: int = 20_000,
    seed: int = 42,
    start: str = "2024-01-01",
    price: float = 30_000.0,
) -> Dataset:
    """Regime-switching random walk with volume clustering.  Deterministic per seed."""
    rng = np.random.default_rng(seed)
    regimes = np.zeros(bars, dtype=int)
    state = 0
    for i in range(bars):
        if rng.random() < 0.002:
            state = int(rng.integers(0, 3))
        regimes[i] = state
    vol = np.choose(regimes, [0.0012, 0.0030, 0.0018])
    drift = np.choose(regimes, [0.0, 0.0, 0.00025]) * np.sign(rng.standard_normal(bars).cumsum())
    rets = drift + vol * rng.standard_t(df=4, size=bars) / np.sqrt(2)
    close = price * np.exp(np.cumsum(rets))
    open_ = np.concatenate([[price], close[:-1]])
    wick = np.abs(rng.normal(0, vol, size=(2, bars))) * close
    high = np.maximum(open_, close) + wick[0]
    low = np.minimum(open_, close) - wick[1]
    volume = np.exp(rng.normal(3.0, 0.6, bars)) * (1 + 40 * np.abs(rets))
    taker = volume * np.clip(0.5 + rets / (4 * vol) + rng.normal(0, 0.05, bars), 0.05, 0.95)
    index = pd.date_range(start, periods=bars, freq=f"{INTERVAL_MINUTES[interval]}min", tz="UTC")
    frame = pd.DataFrame(
        {
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            "quote_volume": volume * close,
            "trade_count": (volume * 10).astype(int),
            "taker_buy_volume": taker,
        },
        index=index,
    )
    frame.index.name = "open_time"
    return Dataset(symbol, interval, frame, f"synthetic:seed={seed}", synthetic=True)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)
