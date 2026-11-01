"""SQLAlchemy 2.0 declarative models for the signal bot schema."""

import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Optional

from sqlalchemy import (
    JSON,
    BigInteger,
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
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# SQLite only auto-increments INTEGER PRIMARY KEY columns.
_BigIntPK = BigInteger().with_variant(Integer, "sqlite")


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
        DateTime(timezone=True), default=_utcnow, nullable=False
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


# ---------------------------------------------------------------------------
# Signal Bot 2.0: event journal, order state machine, fills, results, audit
# ---------------------------------------------------------------------------


class EventRecord(Base):
    """Append-only event journal.  ``sequence`` is the global replay order."""

    __tablename__ = "event_journal"

    sequence: Mapped[int] = mapped_column(_BigIntPK, primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    event_type: Mapped[str] = mapped_column(String(48), nullable=False, index=True)
    source: Mapped[str] = mapped_column(String(48), nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    correlation_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    causation_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)


class ConsumerCheckpoint(Base):
    """Highest journal sequence a consumer has fully processed (at-least-once replay)."""

    __tablename__ = "consumer_checkpoints"

    consumer: Mapped[str] = mapped_column(String(64), primary_key=True)
    last_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )


class Order(Base):
    """Entry order + position lifecycle for one signal (see execution/orders.py)."""

    __tablename__ = "orders"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    signal_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("signals.id", ondelete="CASCADE"), nullable=False, unique=True
    )
    client_order_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    exchange_order_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    venue: Mapped[str] = mapped_column(String(20), nullable=False)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False)
    side: Mapped[str] = mapped_column(String(4), nullable=False)
    order_type: Mapped[str] = mapped_column(String(20), nullable=False)
    size: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    contract_value: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False, default=1)
    limit_price: Mapped[Decimal | None] = mapped_column(Numeric(32, 16), nullable=True)
    intended_entry: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    stop_loss_price: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    take_profit_price: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    state: Mapped[str] = mapped_column(String(24), nullable=False, index=True)
    state_reason: Mapped[str | None] = mapped_column(String(200), nullable=True)
    filled_size: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False, default=0)
    avg_fill_price: Mapped[Decimal | None] = mapped_column(Numeric(32, 16), nullable=True)
    fees: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False, default=0)
    protective_status: Mapped[str] = mapped_column(String(20), nullable=False, default="none")
    last_exchange_update: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    correlation_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )
    submitted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class OrderTransition(Base):
    """Every persisted order state change.  ``event_id`` makes replays idempotent."""

    __tablename__ = "order_transitions"

    id: Mapped[int] = mapped_column(_BigIntPK, primary_key=True, autoincrement=True)
    order_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("orders.id", ondelete="CASCADE"), nullable=False, index=True
    )
    event_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    from_state: Mapped[str] = mapped_column(String(24), nullable=False)
    to_state: Mapped[str] = mapped_column(String(24), nullable=False)
    reason: Mapped[str | None] = mapped_column(String(200), nullable=True)
    exchange_order_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    payload: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )


class Fill(Base):
    """One execution.  The primary key guarantees a fill's financial effect is applied once."""

    __tablename__ = "fills"

    fill_id: Mapped[str] = mapped_column(String(96), primary_key=True)
    order_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("orders.id", ondelete="SET NULL"), nullable=True, index=True
    )
    client_order_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    venue: Mapped[str] = mapped_column(String(20), nullable=False)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False)
    side: Mapped[str] = mapped_column(String(4), nullable=False)
    purpose: Mapped[str] = mapped_column(String(16), nullable=False)
    qty: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    fee: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False, default=0)
    liquidity: Mapped[str] = mapped_column(String(8), nullable=False, default="taker")
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class TradeResult(Base):
    """Execution-quality record for a closed trade (signal quality lives in SignalOutcome)."""

    __tablename__ = "trade_results"

    signal_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("signals.id", ondelete="CASCADE"), primary_key=True
    )
    venue: Mapped[str] = mapped_column(String(20), nullable=False)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False)
    side: Mapped[str] = mapped_column(String(10), nullable=False)
    intended_entry: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    actual_entry: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    exit_price: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    exit_reason: Mapped[str] = mapped_column(String(20), nullable=False)
    size_requested: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    size_filled: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    gross_pnl: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    fees: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    funding: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False, default=0)
    net_pnl: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    signal_r: Mapped[Decimal] = mapped_column(Numeric(16, 8), nullable=False)
    realized_r: Mapped[Decimal] = mapped_column(Numeric(16, 8), nullable=False)
    entry_slippage_bps: Mapped[Decimal] = mapped_column(Numeric(16, 8), nullable=False)
    closed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class OperatorAudit(Base):
    """Who did what to the risk controls, and when."""

    __tablename__ = "operator_audit"

    id: Mapped[int] = mapped_column(_BigIntPK, primary_key=True, autoincrement=True)
    actor: Mapped[str] = mapped_column(String(64), nullable=False)
    action: Mapped[str] = mapped_column(String(32), nullable=False)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_ip: Mapped[str | None] = mapped_column(String(64), nullable=True)
    succeeded: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )


class PaperLedgerEntry(Base):
    """Virtual account ledger for the paper broker (unique entry_id => exactly-once)."""

    __tablename__ = "paper_ledger"
    __table_args__ = (UniqueConstraint("entry_id", name="uq_paper_ledger_entry"),)

    id: Mapped[int] = mapped_column(_BigIntPK, primary_key=True, autoincrement=True)
    entry_id: Mapped[str] = mapped_column(String(96), nullable=False)
    kind: Mapped[str] = mapped_column(String(20), nullable=False)
    symbol: Mapped[str | None] = mapped_column(String(20), nullable=True)
    amount: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    balance_after: Mapped[Decimal] = mapped_column(Numeric(32, 16), nullable=False)
    reference: Mapped[str | None] = mapped_column(String(96), nullable=True)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
