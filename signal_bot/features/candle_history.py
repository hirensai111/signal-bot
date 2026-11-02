"""In-memory history of *completed* candles per symbol on one timeframe.

The live trend generator, the trailing-stop manager and the portfolio correlation
estimate all read the same history, so they agree on what has happened.  Only
closed candles enter it, strictly in time order: a candle that is still forming,
on another interval, a duplicate, or older than the newest stored bar is refused.
Prices are converted with ``float(Decimal)``, which parses the exchange's decimal
string exactly as the research loader parses the CSV files.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

import pandas as pd

from signal_bot.ingest.binance_ws import Kline

logger = logging.getLogger(__name__)

COLUMNS = ("open", "high", "low", "close", "volume", "quote_volume")
INTERVAL = {"1h": timedelta(hours=1), "4h": timedelta(hours=4), "1d": timedelta(days=1)}


class CandleHistory:
    def __init__(self, interval: str = "4h") -> None:
        if interval not in INTERVAL:
            raise ValueError(f"unsupported interval {interval}")
        self.interval = interval
        self.step = INTERVAL[interval]
        self._times: dict[str, list[datetime]] = {}
        self._rows: dict[str, list[tuple[float, ...]]] = {}
        self._frames: dict[str, pd.DataFrame] = {}
        self.gaps: dict[str, int] = {}
        self.refused: dict[str, int] = {}

    # ------------------------------------------------------------------
    def append(self, k: Kline) -> str:
        """Add one candle.  Returns ``appended`` or why it was refused."""
        if k.interval != self.interval:
            return self._refuse("wrong_interval")
        if not k.closed:
            return self._refuse("not_closed")
        times = self._times.setdefault(k.symbol, [])
        if times:
            last = times[-1]
            if k.open_time == last:
                return self._refuse("duplicate")
            if k.open_time < last:
                return self._refuse("out_of_order")
            if k.open_time - last != self.step:
                # Same as the research resampler: an exchange outage leaves no bar.
                self.gaps[k.symbol] = self.gaps.get(k.symbol, 0) + 1
                logger.warning("%s %s gap: %s -> %s", k.symbol, self.interval, last, k.open_time)
        times.append(k.open_time)
        self._rows.setdefault(k.symbol, []).append(tuple(float(getattr(k, c)) for c in COLUMNS))
        self._frames.pop(k.symbol, None)
        return "appended"

    def seed(self, klines: list[Kline]) -> int:
        """Load history (e.g. from REST); returns how many candles were accepted."""
        return sum(self.append(k) == "appended" for k in sorted(klines, key=lambda k: k.open_time))

    def _refuse(self, why: str) -> str:
        self.refused[why] = self.refused.get(why, 0) + 1
        return why

    # ------------------------------------------------------------------
    def symbols(self) -> list[str]:
        return sorted(self._times)

    def bars(self, symbol: str) -> int:
        return len(self._times.get(symbol, ()))

    def last_open(self, symbol: str) -> datetime | None:
        times = self._times.get(symbol)
        return times[-1] if times else None

    def last_close_price(self, symbol: str) -> float | None:
        rows = self._rows.get(symbol)
        return rows[-1][3] if rows else None

    def frame(self, symbol: str) -> pd.DataFrame:
        """All stored bars as a DataFrame indexed by open time (UTC), oldest first."""
        cached = self._frames.get(symbol)
        if cached is not None:
            return cached
        rows = self._rows.get(symbol, [])
        idx = pd.DatetimeIndex(pd.to_datetime(self._times.get(symbol, []), utc=True))
        frame = pd.DataFrame(rows, columns=list(COLUMNS), index=idx, dtype=float)
        self._frames[symbol] = frame
        return frame

    def bars_since(self, symbol: str, start: datetime) -> pd.DataFrame:
        frame = self.frame(symbol)
        return frame[frame.index >= pd.Timestamp(start)]

    def closes(self, until: datetime | None = None) -> dict[str, pd.Series]:
        out = {}
        for s in self.symbols():
            series = self.frame(s)["close"]
            if until is not None:
                series = series[series.index <= pd.Timestamp(until)]
            out[s] = series
        return out

    def status(self) -> dict[str, Any]:
        return {
            "interval": self.interval,
            "symbols": {
                s: {
                    "bars": self.bars(s),
                    "last_open": self.last_open(s).isoformat() if self.last_open(s) else None,
                    "gaps": self.gaps.get(s, 0),
                }
                for s in self.symbols()
            },
            "refused": dict(self.refused),
        }
