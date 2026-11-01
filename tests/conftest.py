"""Shared pytest fixtures."""

from collections.abc import AsyncGenerator

import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from signal_bot.db.models import Base

# Use an in-memory SQLite DB for tests to avoid PostgreSQL dependency.
TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"

# Shared engine created once at module load; each test gets a connection
# from this engine so the in-memory DB is shared across the session.
_test_engine = create_async_engine(
    TEST_DATABASE_URL,
    echo=False,
    future=True,
)


@pytest_asyncio.fixture(scope="session", autouse=True)
async def setup_test_db() -> AsyncGenerator[None, None]:
    """Create all tables once per test session, tear them down at the end."""
    async with _test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with _test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await _test_engine.dispose()


@pytest_asyncio.fixture
async def db_session() -> AsyncGenerator[AsyncSession, None]:
    """Provide a clean transactional session for each test."""
    TestSessionLocal = async_sessionmaker(
        bind=_test_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
        autocommit=False,
    )
    async with TestSessionLocal() as session:
        yield session
        await session.rollback()


@pytest_asyncio.fixture(autouse=True)
async def legacy_sqlite_isolation(request):
    """Legacy test files define module-level PostgreSQL TestSessionLocal.
    Replace with a per-test SQLite in-memory engine so they run without
    a local PostgreSQL server.
    """
    import signal_bot.db.connection as conn_mod
    import signal_bot.delivery.telegram_bot as telegram_mod

    mod = request.module
    needs_patch = hasattr(mod, "TestSessionLocal") or hasattr(mod, "_test_engine")

    if not needs_patch:
        yield
        return

    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False, future=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print(
        f"[DEBUG] legacy_sqlite_isolation: created engine {id(engine)} for {mod.__name__}::{request.function.__name__}"
    )

    TestSessionLocal = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
        autocommit=False,
    )

    # Save and replace module-level attributes
    old_attrs = {}
    for attr in ("TestSessionLocal", "_test_engine", "TEST_DB_URL"):
        if hasattr(mod, attr):
            old_attrs[attr] = getattr(mod, attr)

    mod.TestSessionLocal = TestSessionLocal
    if "_test_engine" in old_attrs:
        mod._test_engine = engine
    if "TEST_DB_URL" in old_attrs:
        mod.TEST_DB_URL = "sqlite+aiosqlite:///:memory:"

    # Patch hard-coded production session locals so components that don't
    # accept session_factory (e.g. TelegramNotifier._cmd_status) use SQLite.
    old_conn_session = conn_mod.AsyncSessionLocal
    old_telegram_session = telegram_mod.AsyncSessionLocal
    conn_mod.AsyncSessionLocal = TestSessionLocal
    telegram_mod.AsyncSessionLocal = TestSessionLocal

    yield

    # Restore
    for attr, val in old_attrs.items():
        setattr(mod, attr, val)
    conn_mod.AsyncSessionLocal = old_conn_session
    telegram_mod.AsyncSessionLocal = old_telegram_session
    await engine.dispose()
