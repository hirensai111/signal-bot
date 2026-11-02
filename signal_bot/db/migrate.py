"""Additive, idempotent schema upgrades for databases created by an earlier release.

Tables are created from SQLAlchemy metadata (``create_all``), which never alters an
existing table.  This adds columns introduced since 2.0 and relaxes NOT NULL on the
take-profit columns (trend signals have no fixed target).  Every statement checks the
live schema first, so it is safe to run on every start, on PostgreSQL and SQLite.
"""

from __future__ import annotations

import logging

from sqlalchemy import inspect
from sqlalchemy.engine import Connection

from signal_bot.db.models import Base

logger = logging.getLogger(__name__)

# (table, column) pairs whose NOT NULL constraint was dropped in 2.1.
NULLABLE_SINCE_2_1 = (("signals", "take_profit"), ("orders", "take_profit_price"))


def upgrade(conn: Connection) -> list[str]:
    """Bring an existing schema up to the current models.  Returns the statements run."""
    insp = inspect(conn)
    existing_tables = set(insp.get_table_names())
    dialect = conn.dialect.name
    ran: list[str] = []
    for table in Base.metadata.sorted_tables:
        if table.name not in existing_tables:
            continue  # create_all makes it with every column
        have = {c["name"]: c for c in insp.get_columns(table.name)}
        for column in table.columns:
            if column.name in have:
                continue
            ddl_type = column.type.compile(dialect=conn.dialect)
            default = ""
            if column.default is not None and getattr(column.default, "is_scalar", False):
                value = column.default.arg
                default = f" DEFAULT '{value}'" if isinstance(value, str) else f" DEFAULT {value}"
            # Existing rows need a value: new NOT NULL columns always carry a default.
            stmt = f"ALTER TABLE {table.name} ADD COLUMN {column.name} {ddl_type}{default}"
            conn.exec_driver_sql(stmt)
            ran.append(stmt)
        if dialect == "postgresql":
            for tname, cname in NULLABLE_SINCE_2_1:
                if tname == table.name and cname in have and not have[cname]["nullable"]:
                    stmt = f"ALTER TABLE {tname} ALTER COLUMN {cname} DROP NOT NULL"
                    conn.exec_driver_sql(stmt)
                    ran.append(stmt)
    for stmt in ran:
        logger.info("schema upgrade: %s", stmt)
    return ran
