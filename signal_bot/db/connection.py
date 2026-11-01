"""Async SQLAlchemy engine and session management."""

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from signal_bot.config import settings

async_engine = create_async_engine(
    settings.database_url,
    echo=settings.log_level.upper() == "DEBUG",
    future=True,
)

AsyncSessionLocal = async_sessionmaker(
    bind=async_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False,
)


async def get_db_session() -> AsyncSession:
    """Yield an async DB session."""
    async with AsyncSessionLocal() as session:
        yield session
