"""Startup schema upgrade: a 2.0 database gains the 2.1 columns, idempotently."""

from sqlalchemy import inspect
from sqlalchemy.ext.asyncio import create_async_engine

from signal_bot.db.migrate import upgrade
from signal_bot.db.models import Base

OLD_2_0 = [
    """CREATE TABLE risk_state (id INTEGER PRIMARY KEY, kill_switch_active BOOLEAN NOT NULL,
       kill_switch_reason TEXT, kill_switch_set_at DATETIME, daily_pnl_r NUMERIC NOT NULL,
       daily_trade_count INTEGER NOT NULL, consecutive_losses INTEGER NOT NULL,
       last_reset_date DATE, current_open_positions INTEGER NOT NULL)""",
    "INSERT INTO risk_state VALUES (1, 0, NULL, NULL, 0, 0, 0, NULL, 0)",
    """CREATE TABLE orders (id CHAR(32) PRIMARY KEY, client_order_id VARCHAR(64) NOT NULL,
       state VARCHAR(24) NOT NULL, take_profit_price NUMERIC NOT NULL)""",
    "INSERT INTO orders VALUES ('a', 'sig_a', 'filled', 1)",
]


async def test_upgrade_adds_new_columns_once(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'old.db'}")
    async with engine.begin() as conn:
        for stmt in OLD_2_0:
            await conn.exec_driver_sql(stmt)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)  # creates missing tables only
        ran = await conn.run_sync(upgrade)
        again = await conn.run_sync(upgrade)
        cols = await conn.run_sync(
            lambda c: {
                t: {x["name"] for x in inspect(c).get_columns(t)} for t in ("orders", "risk_state")
            }
        )
        row = (
            await conn.exec_driver_sql("SELECT trail_mode, current_stop_price FROM orders")
        ).one()
    await engine.dispose()
    assert any("peak_equity" in s for s in ran) and again == []
    assert {"trail_mode", "current_stop_price", "time_stop_at", "price_basis"} <= cols["orders"]
    assert {"peak_equity", "peak_equity_at"} <= cols["risk_state"]
    assert row == ("none", None)  # existing orders are fixed-stop orders
