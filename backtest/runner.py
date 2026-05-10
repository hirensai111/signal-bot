"""Backtest runner: replays historical candles through the live signal pipeline."""

import argparse
import asyncio
import logging
import os
import random
import statistics
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from signal_bot.config import settings
from signal_bot.db.models import (
    AggTrade,
    Base,
    Candle1m,
    Candle5m,
    CandleFeature,
    HtfLevel,
    Signal,
    SignalOutcome,
)
from signal_bot.features.htf_levels import HtfLevelScanner
from signal_bot.features.microstructure import MicrostructureEngine
from signal_bot.ingest.binance_ws import Kline
from signal_bot.signals.risk_sizer import RiskSizer
from signal_bot.signals.sweep_detector import SweepDetector

logger = logging.getLogger(__name__)

# Delta India cost model
ENTRY_FEE = Decimal("0.0005")          # taker fee on limit-IOC entry
ENTRY_SLIPPAGE = Decimal("0.0005")     # 5 bps capped slippage on entry
TP_FEE = Decimal("0.0002")             # maker fee on TP limit exit
TP_SLIPPAGE = Decimal("0")             # zero slippage on TP limit
SL_FEE = Decimal("0.0005")             # taker fee on SL stop-market exit
SL_SLIPPAGE = Decimal("0.0005")        # 5 bps slippage on SL stop-market
EXPIRED_FEE = Decimal("0.0005")        # taker-like fee on expired/time-out
EXPIRED_SLIPPAGE = Decimal("0.0005")   # 5 bps slippage on time-out exit


class BacktestRunner:
    """Fetches historical data, replays through the pipeline, and reports metrics."""

    def __init__(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        timeframe: str = "5m",
        cache_dir: str = "data/cache",
        no_fill_rate: float = 0.10,
    ) -> None:
        self.symbol = symbol.upper()
        self.start = start
        self.end = end
        self.timeframe = timeframe
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.no_fill_rate = no_fill_rate

    async def run(self) -> None:
        logger.info(
            "Backtest start: %s %s from %s to %s",
            self.symbol,
            self.timeframe,
            self.start.isoformat(),
            self.end.isoformat(),
        )

        # 1. In-memory SQLite DB for backtest isolation
        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        SessionLocal = async_sessionmaker(
            bind=engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autoflush=False,
            autocommit=False,
        )

        # 2. Fetch historical data
        candles_1m = await self._fetch_candles("1m")
        trades = await self._fetch_agg_trades(candles_1m)

        # 3. Form LTF and HTF candles
        if self.timeframe == "5m":
            ltf_candles = self._form_ltf_from_1m(candles_1m, 5)
        else:
            ltf_candles = candles_1m

        candles_1h = self._form_htf_from_1m(candles_1m, "1h", 60)
        candles_4h = self._form_htf_from_1m(candles_1m, "4h", 240)

        # 4. Seed DB with all LTF candles and trades upfront
        await self._seed_candles(SessionLocal, ltf_candles)
        await self._seed_trades(SessionLocal, trades)

        # 5. Instantiate pipeline modules with isolated session factory
        htf_scanner = HtfLevelScanner(
            asyncio.Queue(), lookback=3, session_factory=SessionLocal
        )
        microstructure = MicrostructureEngine(
            asyncio.Queue(), session_factory=SessionLocal, settle_delay=0
        )
        sweep_detector = SweepDetector(
            asyncio.Queue(), session_factory=SessionLocal
        )
        risk_sizer = RiskSizer(session_factory=SessionLocal)

        # 6. Replay
        results: list[dict[str, Any]] = []
        open_signals: list[dict[str, Any]] = []

        htf_1h_map = {c["open_time"]: c for c in candles_1h}
        htf_4h_map = {c["open_time"]: c for c in candles_4h}
        ltf_delta = timedelta(minutes=1 if self.timeframe == "1m" else 5)

        for ltf_candle in ltf_candles:
            close_time = ltf_candle["open_time"] + ltf_delta

            # Process HTF candles that close at this timestamp
            if close_time.minute == 0 and close_time.second == 0:
                htf_key = close_time - timedelta(hours=1)
                if htf_key in htf_1h_map:
                    await self._process_htf(htf_scanner, htf_1h_map[htf_key])
                if close_time.hour % 4 == 0:
                    htf_key = close_time - timedelta(hours=4)
                    if htf_key in htf_4h_map:
                        await self._process_htf(htf_scanner, htf_4h_map[htf_key])

            kline = self._dict_to_kline(ltf_candle)

            # Microstructure features
            await microstructure._process_candle(kline)

            # Sweep detection
            candidate = await sweep_detector._evaluate(kline)
            if candidate:
                signal = await risk_sizer.size_and_persist(
                    candidate, self.timeframe, created_at=kline.open_time
                )
                if signal:
                    if random.random() < self.no_fill_rate:
                        # Limit-IOC expired unfilled — skip entirely, not a loss
                        continue
                    open_signals.append(
                        {
                            "signal": signal,
                            "entry_slipped": self._slipped_entry(signal),
                        }
                    )

            # Check open signal exits
            for sig_data in open_signals[:]:
                exit_info = self._check_exit(sig_data["signal"], kline)
                if exit_info:
                    result = self._compute_result(sig_data, exit_info)
                    results.append(result)
                    open_signals.remove(sig_data)

        # Close any remaining open signals at last price
        if ltf_candles:
            last_kline = self._dict_to_kline(ltf_candles[-1])
            for sig_data in open_signals[:]:
                exit_info = {
                    "exit_reason": "expired",
                    "exit_price": last_kline.close,
                    "exit_ts": last_kline.open_time,
                    "hold_minutes": int(
                        (last_kline.open_time - sig_data["signal"].created_at).total_seconds()
                        / 60
                    ),
                }
                result = self._compute_result(sig_data, exit_info)
                results.append(result)
                open_signals.remove(sig_data)

        # 7. Output
        self._save_results(results)
        metrics = self._compute_metrics(results)
        self._print_metrics(metrics)

        logger.info("Backtest complete. Trades: %d", len(results))

    # ------------------------------------------------------------------
    # Data fetching
    # ------------------------------------------------------------------
    async def _fetch_candles(self, interval: str) -> list[dict]:
        from binance import AsyncClient

        cache_path = (
            self.cache_dir
            / f"{self.symbol}_{interval}_{self.start.date()}_{self.end.date()}.parquet"
        )
        if cache_path.exists():
            df = pd.read_parquet(cache_path)
            return self._df_to_candles(df)

        client = await AsyncClient.create(
            api_key=settings.binance_api_key or None,
            api_secret=settings.binance_api_secret or None,
            testnet=settings.binance_testnet,
        )
        try:
            start_ms = int(self.start.timestamp() * 1000)
            end_ms = int(self.end.timestamp() * 1000)
            all_klines: list[list] = []
            while start_ms < end_ms:
                klines = await client.futures_klines(
                    symbol=self.symbol,
                    interval=interval,
                    startTime=start_ms,
                    endTime=end_ms,
                    limit=1500,
                )
                if not klines:
                    break
                all_klines.extend(klines)
                start_ms = klines[-1][0] + 1
        finally:
            await client.close_connection()

        candles = [self._rest_kline_to_dict(k) for k in all_klines]
        df = pd.DataFrame(candles)
        df.to_parquet(cache_path)
        return candles

    def _synthesize_agg_trades(self, candles_1m: list[dict]) -> list[dict]:
        """Derive aggregate trades from 1m candle taker_buy_volume.

        Avoids fetching millions of individual trades from Binance while still
        giving the microstructure engine the buyer/seller volume split it needs.
        """
        trades: list[dict] = []
        trade_id = 0
        for c in candles_1m:
            ts = c["open_time"]
            price = c["close"]
            # Taker buys (market buy orders)
            taker_buy = c["taker_buy_volume"]
            if taker_buy > 0:
                trade_id += 1
                trades.append(
                    {
                        "symbol": self.symbol,
                        "trade_id": trade_id,
                        "price": price,
                        "qty": taker_buy,
                        "quote_qty": price * taker_buy,
                        "ts": ts,
                        "is_buyer_maker": False,
                    }
                )
            # Taker sells (market sell orders) = remaining volume
            taker_sell = c["volume"] - taker_buy
            if taker_sell > 0:
                trade_id += 1
                trades.append(
                    {
                        "symbol": self.symbol,
                        "trade_id": trade_id,
                        "price": price,
                        "qty": taker_sell,
                        "quote_qty": price * taker_sell,
                        "ts": ts,
                        "is_buyer_maker": True,
                    }
                )
        return trades

    async def _fetch_agg_trades(self, candles_1m: list[dict]) -> list[dict]:
        cache_path = (
            self.cache_dir
            / f"{self.symbol}_aggTrades_{self.start.date()}_{self.end.date()}.parquet"
        )
        if cache_path.exists():
            df = pd.read_parquet(cache_path)
            return self._df_to_trades(df)

        trades = self._synthesize_agg_trades(candles_1m)
        df = pd.DataFrame(trades)
        df.to_parquet(cache_path)
        return trades

    @staticmethod
    def _rest_kline_to_dict(raw: list) -> dict:
        return {
            "symbol": "",
            "open_time": datetime.fromtimestamp(raw[0] / 1000.0, tz=timezone.utc),
            "open": Decimal(raw[1]),
            "high": Decimal(raw[2]),
            "low": Decimal(raw[3]),
            "close": Decimal(raw[4]),
            "volume": Decimal(raw[5]),
            "quote_volume": Decimal(raw[7]),
            "trade_count": int(raw[8]),
            "taker_buy_volume": Decimal(raw[9]),
            "closed": True,
        }

    @staticmethod
    def _rest_trade_to_dict(raw: dict) -> dict:
        return {
            "symbol": "",
            "trade_id": int(raw["a"]),
            "price": Decimal(raw["p"]),
            "qty": Decimal(raw["q"]),
            "quote_qty": Decimal(raw["p"]) * Decimal(raw["q"]),
            "ts": datetime.fromtimestamp(raw["T"] / 1000.0, tz=timezone.utc),
            "is_buyer_maker": raw["m"],
        }

    @staticmethod
    def _df_to_candles(df: pd.DataFrame) -> list[dict]:
        candles = []
        for _, row in df.iterrows():
            d = row.to_dict()
            d["open_time"] = pd.Timestamp(d["open_time"]).to_pydatetime().replace(tzinfo=timezone.utc)
            d["open"] = Decimal(str(d["open"]))
            d["high"] = Decimal(str(d["high"]))
            d["low"] = Decimal(str(d["low"]))
            d["close"] = Decimal(str(d["close"]))
            d["volume"] = Decimal(str(d["volume"]))
            d["quote_volume"] = Decimal(str(d["quote_volume"]))
            d["taker_buy_volume"] = Decimal(str(d["taker_buy_volume"]))
            candles.append(d)
        return candles

    @staticmethod
    def _df_to_trades(df: pd.DataFrame) -> list[dict]:
        trades = []
        for _, row in df.iterrows():
            d = row.to_dict()
            d["ts"] = pd.Timestamp(d["ts"]).to_pydatetime().replace(tzinfo=timezone.utc)
            d["price"] = Decimal(str(d["price"]))
            d["qty"] = Decimal(str(d["qty"]))
            d["quote_qty"] = Decimal(str(d["quote_qty"]))
            trades.append(d)
        return trades

    # ------------------------------------------------------------------
    # Candle formation
    # ------------------------------------------------------------------
    def _form_ltf_from_1m(self, candles_1m: list[dict], n: int) -> list[dict]:
        ltf: list[dict] = []
        for i in range(0, len(candles_1m), n):
            chunk = candles_1m[i : i + n]
            if len(chunk) < n:
                break
            ltf.append(self._aggregate_chunk(chunk, self.timeframe))
        return ltf

    def _form_htf_from_1m(
        self, candles_1m: list[dict], interval: str, n: int
    ) -> list[dict]:
        htf: list[dict] = []
        for i in range(0, len(candles_1m), n):
            chunk = candles_1m[i : i + n]
            if len(chunk) < n:
                break
            htf.append(self._aggregate_chunk(chunk, interval))
        return htf

    def _aggregate_chunk(self, chunk: list[dict], interval: str) -> dict:
        return {
            "symbol": self.symbol,
            "interval": interval,
            "open_time": chunk[0]["open_time"],
            "open": chunk[0]["open"],
            "high": max(c["high"] for c in chunk),
            "low": min(c["low"] for c in chunk),
            "close": chunk[-1]["close"],
            "volume": sum(c["volume"] for c in chunk),
            "quote_volume": sum(c["quote_volume"] for c in chunk),
            "trade_count": sum(c["trade_count"] for c in chunk),
            "taker_buy_volume": sum(c["taker_buy_volume"] for c in chunk),
            "closed": True,
        }

    # ------------------------------------------------------------------
    # DB seeding
    # ------------------------------------------------------------------
    async def _seed_candles(self, session_factory, candles: list[dict]) -> None:
        model = Candle1m if self.timeframe == "1m" else Candle5m
        for i in range(0, len(candles), 1000):
            batch = candles[i : i + 1000]
            async with session_factory() as session:
                for c in batch:
                    c = dict(c)
                    c["symbol"] = self.symbol
                    c.pop("interval", None)
                    session.add(model(**c))
                await session.commit()

    async def _seed_trades(self, session_factory, trades: list[dict]) -> None:
        for i in range(0, len(trades), 1000):
            batch = trades[i : i + 1000]
            async with session_factory() as session:
                for t in batch:
                    t = dict(t)
                    t["symbol"] = self.symbol
                    session.add(AggTrade(**t))
                await session.commit()

    async def _process_htf(self, scanner: HtfLevelScanner, candle: dict) -> None:
        kline = Kline(
            symbol=self.symbol,
            interval="1h" if "1h" in str(candle.get("interval", "")) else "4h",
            open_time=candle["open_time"],
            open=candle["open"],
            high=candle["high"],
            low=candle["low"],
            close=candle["close"],
            volume=candle["volume"],
            quote_volume=candle["quote_volume"],
            trade_count=candle["trade_count"],
            taker_buy_volume=candle["taker_buy_volume"],
            closed=True,
        )
        await scanner._process_candle(kline)

    # ------------------------------------------------------------------
    # Simulation helpers
    # ------------------------------------------------------------------
    def _dict_to_kline(self, c: dict) -> Kline:
        return Kline(
            symbol=self.symbol,
            interval=self.timeframe,
            open_time=c["open_time"],
            open=c["open"],
            high=c["high"],
            low=c["low"],
            close=c["close"],
            volume=c["volume"],
            quote_volume=c["quote_volume"],
            trade_count=c["trade_count"],
            taker_buy_volume=c["taker_buy_volume"],
            closed=True,
        )

    def _slipped_entry(self, signal: Signal) -> Decimal:
        if signal.side == "long":
            return signal.entry * (1 + ENTRY_SLIPPAGE)
        return signal.entry * (1 - ENTRY_SLIPPAGE)

    def _slipped_exit(
        self, signal: Signal, price: Decimal, exit_reason: str
    ) -> Decimal:
        if exit_reason == "tp":
            slippage = TP_SLIPPAGE
        elif exit_reason == "sl":
            slippage = SL_SLIPPAGE
        else:
            slippage = EXPIRED_SLIPPAGE
        if signal.side == "long":
            return price * (1 - slippage)
        return price * (1 + slippage)

    def _check_exit(self, signal: Signal, kline: Kline) -> dict | None:
        created = signal.created_at
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        hold_minutes = int((kline.open_time - created).total_seconds() / 60)

        if signal.side == "long":
            if kline.low <= signal.stop_loss:
                return {
                    "exit_reason": "sl",
                    "exit_price": signal.stop_loss,
                    "exit_ts": kline.open_time,
                    "hold_minutes": hold_minutes,
                }
            if kline.high >= signal.take_profit:
                return {
                    "exit_reason": "tp",
                    "exit_price": signal.take_profit,
                    "exit_ts": kline.open_time,
                    "hold_minutes": hold_minutes,
                }
        else:
            if kline.high >= signal.stop_loss:
                return {
                    "exit_reason": "sl",
                    "exit_price": signal.stop_loss,
                    "exit_ts": kline.open_time,
                    "hold_minutes": hold_minutes,
                }
            if kline.low <= signal.take_profit:
                return {
                    "exit_reason": "tp",
                    "exit_price": signal.take_profit,
                    "exit_ts": kline.open_time,
                    "hold_minutes": hold_minutes,
                }

        if hold_minutes > signal.est_hold_minutes * 3:
            return {
                "exit_reason": "expired",
                "exit_price": kline.close,
                "exit_ts": kline.open_time,
                "hold_minutes": hold_minutes,
            }

        return None

    def _compute_result(
        self, sig_data: dict[str, Any], exit_info: dict[str, Any]
    ) -> dict[str, Any]:
        signal = sig_data["signal"]
        entry_slipped = sig_data["entry_slipped"]
        exit_slipped = self._slipped_exit(
            signal, exit_info["exit_price"], exit_info["exit_reason"]
        )

        if signal.side == "long":
            gross_pnl = (exit_slipped - entry_slipped) / entry_slipped
        else:
            gross_pnl = (entry_slipped - exit_slipped) / entry_slipped

        # Outcome-dependent friction
        exit_reason = exit_info["exit_reason"]
        if exit_reason == "tp":
            exit_fee = TP_FEE
            exit_slip = TP_SLIPPAGE
        elif exit_reason == "sl":
            exit_fee = SL_FEE
            exit_slip = SL_SLIPPAGE
        else:
            exit_fee = EXPIRED_FEE
            exit_slip = EXPIRED_SLIPPAGE

        total_cost = ENTRY_FEE + ENTRY_SLIPPAGE + exit_fee + exit_slip
        net_pnl = gross_pnl - total_cost

        # R multiple (based on intended prices, no fees)
        if signal.side == "long":
            pnl_r = (exit_info["exit_price"] - signal.entry) / (
                signal.entry - signal.stop_loss
            )
        else:
            pnl_r = (signal.entry - exit_info["exit_price"]) / (
                signal.stop_loss - signal.entry
            )

        return {
            "symbol": signal.symbol,
            "side": signal.side,
            "entry": float(signal.entry),
            "stop": float(signal.stop_loss),
            "target": float(signal.take_profit),
            "leverage": signal.leverage,
            "exit_reason": exit_info["exit_reason"],
            "exit_price": float(exit_info["exit_price"]),
            "gross_pnl_pct": float(gross_pnl * 100),
            "net_pnl_pct": float(net_pnl * 100),
            "pnl_r": float(pnl_r),
            "hold_minutes": exit_info["hold_minutes"],
            "open_time": signal.created_at,
            "close_time": exit_info["exit_ts"],
        }

    # ------------------------------------------------------------------
    # Output
    # ------------------------------------------------------------------
    def _save_results(self, results: list[dict[str, Any]]) -> None:
        if not results:
            logger.warning("No trades to save.")
            return
        df = pd.DataFrame(results)
        out_path = (
            self.cache_dir
            / f"backtest_{self.symbol}_{self.timeframe}_{self.start.date()}_{self.end.date()}.csv"
        )
        df.to_csv(out_path, index=False)
        logger.info("Results saved to %s", out_path)

    def _compute_metrics(self, results: list[dict[str, Any]]) -> dict[str, Any]:
        if not results:
            return {"trades": 0}

        total = len(results)
        wins = sum(1 for r in results if r["net_pnl_pct"] > 0)
        win_rate = wins / total * 100

        net_pnls = [r["net_pnl_pct"] for r in results]
        avg_pnl = sum(net_pnls) / total

        pnl_rs = [r["pnl_r"] for r in results]
        avg_r = sum(pnl_rs) / total

        gross_profit = sum(r["net_pnl_pct"] for r in results if r["net_pnl_pct"] > 0)
        gross_loss = abs(
            sum(r["net_pnl_pct"] for r in results if r["net_pnl_pct"] < 0)
        )
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")

        # Max drawdown
        equity = [0.0]
        for r in results:
            equity.append(equity[-1] + r["net_pnl_pct"])
        peak = equity[0]
        max_dd = 0.0
        for val in equity:
            if val > peak:
                peak = val
            dd = peak - val
            if dd > max_dd:
                max_dd = dd

        # Sharpe (daily)
        daily_returns: defaultdict = defaultdict(float)
        for r in results:
            day = r["close_time"].date() if hasattr(r["close_time"], "date") else r["close_time"]
            daily_returns[day] += r["net_pnl_pct"]
        daily = list(daily_returns.values())
        if len(daily) > 1:
            mean_daily = statistics.mean(daily)
            std_daily = statistics.stdev(daily)
            sharpe = (mean_daily / std_daily) * (365**0.5) if std_daily > 0 else 0.0
        else:
            sharpe = 0.0

        # Expectancy
        if wins > 0:
            avg_win = sum(r["net_pnl_pct"] for r in results if r["net_pnl_pct"] > 0) / wins
        else:
            avg_win = 0.0
        losses = total - wins
        if losses > 0:
            avg_loss = sum(r["net_pnl_pct"] for r in results if r["net_pnl_pct"] < 0) / losses
        else:
            avg_loss = 0.0
        expectancy = (win_rate / 100) * avg_win + (1 - win_rate / 100) * avg_loss

        return {
            "trades": total,
            "win_rate": win_rate,
            "avg_pnl_pct": avg_pnl,
            "avg_r": avg_r,
            "profit_factor": profit_factor,
            "max_drawdown_pct": max_dd,
            "sharpe_annual": sharpe,
            "expectancy_pct": expectancy,
        }

    def _print_metrics(self, metrics: dict[str, Any]) -> None:
        lines = [
            "",
            "========== BACKTEST RESULTS ==========",
            f"Trades:        {metrics.get('trades', 0)}",
            f"Win rate:      {metrics.get('win_rate', 0):.1f}%",
            f"Avg PnL:       {metrics.get('avg_pnl_pct', 0):.3f}%",
            f"Avg R:         {metrics.get('avg_r', 0):.2f}",
            f"Profit factor: {metrics.get('profit_factor', 0):.2f}",
            f"Max drawdown:  {metrics.get('max_drawdown_pct', 0):.2f}%",
            f"Sharpe (ann):  {metrics.get('sharpe_annual', 0):.2f}",
            f"Expectancy:    {metrics.get('expectancy_pct', 0):.3f}%",
            "======================================",
        ]
        print("\n".join(lines))


async def main() -> None:
    parser = argparse.ArgumentParser(description="Backtest runner")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    parser.add_argument("--timeframe", default="5m", choices=["1m", "5m"])
    parser.add_argument(
        "--no-fill-rate",
        type=float,
        default=0.10,
        help="Probability that a limit-IOC entry expires unfilled (default: 0.10)",
    )
    args = parser.parse_args()

    start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = datetime.strptime(args.end, "%Y-%m-%d").replace(
        tzinfo=timezone.utc
    ) + timedelta(days=1)

    runner = BacktestRunner(
        args.symbol, start, end, args.timeframe, no_fill_rate=args.no_fill_rate
    )
    await runner.run()


if __name__ == "__main__":
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )
    asyncio.run(main())
