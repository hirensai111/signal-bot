"""SQLAlchemy 2.0 declarative models for the signal bot schema."""

import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    PrimaryKeyConstraint,
    String,
    Text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Instrument(Base):
    __tablename__ = "instruments"

    symbol: Mapped[str] = mapped_column(String(20), primary_key=True)
    base_asset: Mapped[str] = mapped_column(String(10), nullable=False)
    quote_asset: Mapped[str] = mapped_column(String(10), nullable=False)
    tick_size: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    min_qty: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)


class Candle1m(Base):
    __tablename__ = "candles_1m"
    __table_args__ = (PrimaryKeyConstraint("symbol", "open_time", name="pk_candles_1m"),)

    symbol: Mapped[str] = mapped_column(String(20), nullable=False)
    open_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    open: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    high: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    low: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    close: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    volume: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    quote_volume: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    trade_count: Mapped[int] = mapped_column(Integer, nullable=False)
    taker_buy_volume: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    closed: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)


class Candle5m(Base):
    __tablename__ = "candles_5m"
    __table_args__ = (PrimaryKeyConstraint("symbol", "open_time", name="pk_candles_5m"),)

    symbol: Mapped[str] = mapped_column(String(20), nullable=False)
    open_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    open: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    high: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    low: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    close: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    volume: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    quote_volume: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    trade_count: Mapped[int] = mapped_column(Integer, nullable=False)
    taker_buy_volume: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    closed: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)


class AggTrade(Base):
    __tablename__ = "agg_trades"
    __table_args__ = (PrimaryKeyConstraint("symbol", "trade_id", name="pk_agg_trades"),)

    symbol: Mapped[str] = mapped_column(String(20), nullable=False)
    trade_id: Mapped[int] = mapped_column(Integer, nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    qty: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    quote_qty: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    is_buyer_maker: Mapped[bool] = mapped_column(Boolean, nullable=False)


class HtfLevel(Base):
    __tablename__ = "htf_levels"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4, sort_order=-1)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    timeframe: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    level_type: Mapped[str] = mapped_column(String(20), nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    formed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    broken_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)


class Signal(Base):
    __tablename__ = "signals"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4, sort_order=-1)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    side: Mapped[str] = mapped_column(String(10), nullable=False)
    entry: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    stop_loss: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    take_profit: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    leverage: Mapped[int] = mapped_column(Integer, nullable=False)
    risk_pct: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    account_size_assumed: Mapped[Decimal] = mapped_column(Numeric(32, 8), nullable=False)
    est_hold_minutes: Mapped[int] = mapped_column(Integer, nullable=False)
    pattern_features: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, nullable=False
    )
    status: Mapped[str] = mapped_column(String(20), default="pending", nullable=False)

    # Delta execution fields
    client_order_id: Mapped[str | None] = mapped_column(String(50), nullable=True)
    delta_order_id: Mapped[str | None] = mapped_column(String(50), nullable=True)
    delta_symbol: Mapped[str | None] = mapped_column(String(20), nullable=True)
    delta_entry_intended: Mapped[Decimal | None] = mapped_column(Numeric(32, 16), nullable=True)
    delta_entry_actual: Mapped[Decimal | None] = mapped_column(Numeric(32, 16), nullable=True)
    delta_basis: Mapped[Decimal | None] = mapped_column(Numeric(16, 8), nullable=True)
    entry_fees_usdt: Mapped[Decimal | None] = mapped_column(Numeric(16, 8), nullable=True)
    slippage_bps: Mapped[Decimal | None] = mapped_column(Numeric(16, 8), nullable=True)
    cancelled_reason: Mapped[str | None] = mapped_column(String(50), nullable=True)

    outcome: Mapped[Optional["SignalOutcome"]] = relationship(
        "SignalOutcome", back_populates="signal", uselist=False
    )


class RiskState(Base):
    __tablename__ = "risk_state"
    __table_args__ = (CheckConstraint("id = 1", name="check_single_row"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    kill_switch_active: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    kill_switch_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    kill_switch_set_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    daily_pnl_r: Mapped[Decimal] = mapped_column(
        Numeric(16, 8), default=Decimal("0"), nullable=False
    )
    daily_trade_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    consecutive_losses: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    last_reset_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    current_open_positions: Mapped[int] = mapped_column(Integer, default=0, nullable=False)


class SignalOutcome(Base):
    __tablename__ = "signal_outcomes"

    signal_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("signals.id", ondelete="CASCADE"), primary_key=True
    )
    exit_price: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    exit_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    pnl_pct: Mapped[Decimal] = mapped_column(Numeric(16, 8), nullable=False)
    pnl_r: Mapped[Decimal] = mapped_column(Numeric(16, 8), nullable=False)
    hold_minutes: Mapped[int] = mapped_column(Integer, nullable=False)
    exit_reason: Mapped[str] = mapped_column(String(20), nullable=False)

    signal: Mapped["Signal"] = relationship("Signal", back_populates="outcome")


class CandleFeature(Base):
    __tablename__ = "candle_features"
    __table_args__ = (
        PrimaryKeyConstraint("symbol", "timeframe", "open_time", name="pk_candle_features"),
    )

    symbol: Mapped[str] = mapped_column(String(20), nullable=False)
    timeframe: Mapped[str] = mapped_column(String(10), nullable=False)
    open_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    cvd_delta: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    cvd_cumulative: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    vol_zscore: Mapped[Decimal] = mapped_column(Numeric(16, 8), nullable=False)
    vol_ratio_20: Mapped[Decimal] = mapped_column(Numeric(16, 8), nullable=False)
