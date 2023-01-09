"""DB connection definition."""
import os

from sqlalchemy.orm import sessionmaker
from sqlmodel import create_engine
from sqlmodel.ext.asyncio.session import AsyncEngine
from sqlmodel.ext.asyncio.session import AsyncSession

engine = AsyncEngine(create_engine(os.environ.get('CONFIG_DB_CONN'),
                                   echo=True, future=True))


async def get_session() -> AsyncSession:
  async_session = sessionmaker(
      engine, class_=AsyncSession, expire_on_commit=False
  )
  async with async_session() as session:
    yield session
