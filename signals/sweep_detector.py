"""Core pattern logic: liquidity sweep + reclaim + volume spike + CVD divergence."""

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional

from sqlalchemy import select

from signal_bot.config import settings
from signal_bot.db.connection import AsyncSessionLocal
from signal_bot.db.models import Candle1m, Candle5m, CandleFeature, HtfLevel
from signal_bot.ingest.binance_ws import Kline

logger = logging.getLogger(__name__)

_TIMEFRAME_PRIORITY = {"4h": 2, "1h": 1}


@dataclass(frozen=True)
class SignalCandidate:
    symbol: str
    side: str
    entry: Decimal
    stop: Decimal
    proposed_tp: Decimal
    pattern_features: dict
    swept_level_timeframe: str


class SweepDetector:
    """Detects sweep+reclaim setups with volume and CVD confluence."""

    COOLDOWN_CANDLES = 6
    MIN_RR = Decimal("1.5")

    def __init__(
        self,
        candle_queue: asyncio.Queue,
        session_factory=None,
    ) -> None:
        self.candle_queue = candle_queue
        self.session_factory = session_factory or AsyncSessionLocal
        self._shutdown = asyncio.Event()
        # key: "symbol|side" → (cooldown_expires_at, level_price, level_timeframe)
        self._cooldowns: dict[str, tuple[datetime, Decimal, str]] = {}

    def shutdown(self) -> None:
        logger.info("Sweep detector shutdown requested.")
        self._shutdown.set()

    @staticmethod
    def _cooldown_key(symbol: str, side: str) -> str:
        return f"{symbol}|{side}"

    def _is_in_cooldown(
        self,
        symbol: str,
        side: str,
        current_time: datetime,
        active_levels: list[HtfLevel],
    ) -> bool:
        key = self._cooldown_key(symbol, side)
        if key not in self._cooldowns:
            return False

        expires_at, level_price, level_timeframe = self._cooldowns[key]

        # Early-clear if the triggering HTF level is broken/invalidated
        level_still_active = any(
            l.price == level_price and l.timeframe == level_timeframe and l.active
            for l in active_levels
        )
        if not level_still_active:
            del self._cooldowns[key]
            return False

        if current_time < expires_at:
            return True

        del self._cooldowns[key]
        return False

    def _set_cooldown(
        self,
        symbol: str,
        side: str,
        current_time: datetime,
        interval: str,
        level: HtfLevel,
    ) -> None:
        key = self._cooldown_key(symbol, side)
        candle_minutes = 1 if interval == "1m" else 5
        expires_at = current_time + timedelta(minutes=candle_minutes * self.COOLDOWN_CANDLES)
        self._cooldowns[key] = (expires_at, level.price, level.timeframe)

    async def run(self) -> None:
        logger.info("Sweep detector started.")
        while not self._shutdown.is_set():
            try:
                candle = await asyncio.wait_for(
                    self.candle_queue.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                continue

            if candle.interval not in ("1m", "5m"):
                continue

            candidate = await self._evaluate(candle)
            if candidate:
                logger.info("Signal candidate fired: %s", candidate)

    async def _evaluate(self, candle: Kline) -> Optional[SignalCandidate]:
        table = Candle1m if candle.interval == "1m" else Candle5m

        async with self.session_factory() as session:
            # Load active HTF levels
            levels_result = await session.execute(
                select(HtfLevel).where(
                    HtfLevel.symbol == candle.symbol,
                    HtfLevel.active.is_(True),
                )
            )
            all_levels = levels_result.scalars().all()
            swing_lows = sorted(
                [l for l in all_levels if l.level_type == "swing_low"],
                key=lambda l: _TIMEFRAME_PRIORITY.get(l.timeframe, 0),
                reverse=True,
            )
            swing_highs = sorted(
                [l for l in all_levels if l.level_type == "swing_high"],
                key=lambda l: _TIMEFRAME_PRIORITY.get(l.timeframe, 0),
                reverse=True,
            )

            # Load the 3 candles preceding the current one
            prior_result = await session.execute(
                select(table)
                .where(
                    table.symbol == candle.symbol,
                    table.open_time < candle.open_time,
                )
                .order_by(table.open_time.desc())
                .limit(3)
            )
            prior3 = list(reversed(prior_result.scalars().all()))

        if len(prior3) < 3:
            logger.debug(
                "%s %s @ %s: insufficient history (%d candles)",
                candle.symbol,
                candle.interval,
                candle.open_time,
                len(prior3),
            )
            return None

        # Evaluate long first, then short
        candidate = await self._evaluate_long(
            candle, prior3, swing_lows, swing_highs, table
        )
        if candidate:
            return candidate

        candidate = await self._evaluate_short(
            candle, prior3, swing_highs, swing_lows, table
        )
        return candidate

    async def _evaluate_long(
        self,
        candle: Kline,
        prior3: list,
        swing_lows: list[HtfLevel],
        swing_highs: list[HtfLevel],
        table,
    ) -> Optional[SignalCandidate]:
        if not swing_lows:
            return None

        if self._is_in_cooldown(
            candle.symbol, "long", candle.open_time, swing_lows + swing_highs
        ):
            return None

        # 1. Find sweep candle (most recent of the 3 that pierced a level)
        #    Levels are sorted by HTF priority, so higher-TF sweeps win.
        sweep_candle = None
        swept_level = None
        for c in reversed(prior3):
            for level in swing_lows:
                if c.low < level.price:
                    sweep_candle = c
                    swept_level = level
                    break
            if sweep_candle:
                break

        # 2. Reclaim
        reclaim = candle.close > swept_level.price if swept_level else False

        # 3. Volume spike
        volume_ok = False
        sweep_feature = None
        if sweep_candle:
            async with self.session_factory() as session:
                sweep_feature = await session.scalar(
                    select(CandleFeature).where(
                        CandleFeature.symbol == candle.symbol,
                        CandleFeature.timeframe == candle.interval,
                        CandleFeature.open_time == sweep_candle.open_time,
                    )
                )
            if sweep_feature:
                volume_ok = (
                    sweep_feature.vol_zscore >= Decimal("2.0")
                    or sweep_feature.vol_ratio_20 >= Decimal("2.5")
                )

        # 4. CVD divergence
        divergence_ok = False
        prior_low_feature = None
        if sweep_candle:
            async with self.session_factory() as session:
                prior_result = await session.execute(
                    select(table)
                    .where(
                        table.symbol == candle.symbol,
                        table.open_time < sweep_candle.open_time,
                    )
                    .order_by(table.open_time.desc())
                    .limit(50)
                )
                prior50 = prior_result.scalars().all()

            if prior50:
                prior_low = min(prior50, key=lambda c: c.low)
                async with self.session_factory() as session:
                    prior_low_feature = await session.scalar(
                        select(CandleFeature).where(
                            CandleFeature.symbol == candle.symbol,
                            CandleFeature.timeframe == candle.interval,
                            CandleFeature.open_time == prior_low.open_time,
                        )
                    )
                if prior_low_feature and sweep_feature:
                    new_low = sweep_candle.low <= prior_low.low
                    cvd_divergence = sweep_feature.cvd_delta > prior_low_feature.cvd_delta
                    divergence_ok = new_low and cvd_divergence

        logger.debug(
            "Long eval %s %s @ %s: sweep=%s reclaim=%s volume=%s divergence=%s",
            candle.symbol,
            candle.interval,
            candle.open_time,
            sweep_candle is not None,
            reclaim,
            volume_ok,
            divergence_ok,
        )

        if not (sweep_candle and reclaim and volume_ok and divergence_ok):
            return None

        # Construct candidate
        stop = sweep_candle.low * Decimal("0.9995")
        r_distance = candle.close - stop
        r_tp = candle.close + 2 * r_distance

        if swing_highs:
            valid_tps = [l.price for l in swing_highs if l.price > candle.close]
            if valid_tps:
                level_tp = min(valid_tps)
                proposed_tp = max(level_tp, r_tp)
            else:
                proposed_tp = r_tp
        else:
            proposed_tp = r_tp

        # R:R floor check
        reward = proposed_tp - candle.close
        risk = candle.close - stop
        if risk > 0 and reward / risk < self.MIN_RR:
            logger.debug(
                "Long reward too small: %s %s rr=%.2f",
                candle.symbol,
                candle.interval,
                float(reward / risk),
            )
            return None

        self._set_cooldown(candle.symbol, "long", candle.open_time, candle.interval, swept_level)

        return SignalCandidate(
            symbol=candle.symbol,
            side="long",
            entry=candle.close,
            stop=stop,
            proposed_tp=proposed_tp,
            pattern_features={
                "sweep_candle_time": sweep_candle.open_time.isoformat(),
                "swept_level_price": str(swept_level.price),
                "sweep_low": str(sweep_candle.low),
                "reclaim_close": str(candle.close),
                "vol_zscore": str(sweep_feature.vol_zscore),
                "vol_ratio_20": str(sweep_feature.vol_ratio_20),
                "sweep_cvd_delta": str(sweep_feature.cvd_delta),
                "prior_low_cvd_delta": str(prior_low_feature.cvd_delta),
                "r_distance": str(r_distance),
            },
            swept_level_timeframe=swept_level.timeframe,
        )

    async def _evaluate_short(
        self,
        candle: Kline,
        prior3: list,
        swing_highs: list[HtfLevel],
        swing_lows: list[HtfLevel],
        table,
    ) -> Optional[SignalCandidate]:
        if not swing_highs:
            return None

        if self._is_in_cooldown(
            candle.symbol, "short", candle.open_time, swing_lows + swing_highs
        ):
            return None

        sweep_candle = None
        swept_level = None
        for c in reversed(prior3):
            for level in swing_highs:
                if c.high > level.price:
                    sweep_candle = c
                    swept_level = level
                    break
            if sweep_candle:
                break

        reclaim = candle.close < swept_level.price if swept_level else False

        volume_ok = False
        sweep_feature = None
        if sweep_candle:
            async with self.session_factory() as session:
                sweep_feature = await session.scalar(
                    select(CandleFeature).where(
                        CandleFeature.symbol == candle.symbol,
                        CandleFeature.timeframe == candle.interval,
                        CandleFeature.open_time == sweep_candle.open_time,
                    )
                )
            if sweep_feature:
                volume_ok = (
                    sweep_feature.vol_zscore >= Decimal("2.0")
                    or sweep_feature.vol_ratio_20 >= Decimal("2.5")
                )

        divergence_ok = False
        prior_high_feature = None
        if sweep_candle:
            async with self.session_factory() as session:
                prior_result = await session.execute(
                    select(table)
                    .where(
                        table.symbol == candle.symbol,
                        table.open_time < sweep_candle.open_time,
                    )
                    .order_by(table.open_time.desc())
                    .limit(50)
                )
                prior50 = prior_result.scalars().all()

            if prior50:
                prior_high = max(prior50, key=lambda c: c.high)
                async with self.session_factory() as session:
                    prior_high_feature = await session.scalar(
                        select(CandleFeature).where(
                            CandleFeature.symbol == candle.symbol,
                            CandleFeature.timeframe == candle.interval,
                            CandleFeature.open_time == prior_high.open_time,
                        )
                    )
                if prior_high_feature and sweep_feature:
                    new_high = sweep_candle.high >= prior_high.high
                    cvd_divergence = sweep_feature.cvd_delta < prior_high_feature.cvd_delta
                    divergence_ok = new_high and cvd_divergence

        logger.debug(
            "Short eval %s %s @ %s: sweep=%s reclaim=%s volume=%s divergence=%s",
            candle.symbol,
            candle.interval,
            candle.open_time,
            sweep_candle is not None,
            reclaim,
            volume_ok,
            divergence_ok,
        )

        if not (sweep_candle and reclaim and volume_ok and divergence_ok):
            return None

        stop = sweep_candle.high * Decimal("1.0005")
        r_distance = stop - candle.close
        r_tp = candle.close - 2 * r_distance

        if swing_lows:
            valid_tps = [l.price for l in swing_lows if l.price < candle.close]
            if valid_tps:
                level_tp = max(valid_tps)
                proposed_tp = min(level_tp, r_tp)
            else:
                proposed_tp = r_tp
        else:
            proposed_tp = r_tp

        # R:R floor check
        reward = candle.close - proposed_tp
        risk = stop - candle.close
        if risk > 0 and reward / risk < self.MIN_RR:
            logger.debug(
                "Short reward too small: %s %s rr=%.2f",
                candle.symbol,
                candle.interval,
                float(reward / risk),
            )
            return None

        self._set_cooldown(candle.symbol, "short", candle.open_time, candle.interval, swept_level)

        return SignalCandidate(
            symbol=candle.symbol,
            side="short",
            entry=candle.close,
            stop=stop,
            proposed_tp=proposed_tp,
            pattern_features={
                "sweep_candle_time": sweep_candle.open_time.isoformat(),
                "swept_level_price": str(swept_level.price),
                "sweep_high": str(sweep_candle.high),
                "reclaim_close": str(candle.close),
                "vol_zscore": str(sweep_feature.vol_zscore),
                "vol_ratio_20": str(sweep_feature.vol_ratio_20),
                "sweep_cvd_delta": str(sweep_feature.cvd_delta),
                "prior_high_cvd_delta": str(prior_high_feature.cvd_delta),
                "r_distance": str(r_distance),
            },
            swept_level_timeframe=swept_level.timeframe,
        )
