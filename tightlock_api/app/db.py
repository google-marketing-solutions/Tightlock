import os
from sqlmodel import SQLModel, create_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel.ext.asyncio.session import AsyncSession, AsyncEngine


engine = AsyncEngine(create_engine(os.environ.get('CONFIG_DB_CONN'), echo=True, future=True))

# TODO(caiotomazelli): delete once migrations are set-up
async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.drop_all)
        await conn.run_sync(SQLModel.metadata.create_all)


async def get_session() -> AsyncSession:
    async_session = sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )
    async with async_session() as session:
        yield session