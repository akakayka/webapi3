from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from sqlalchemy import Column, Integer, String, Float

SQLALCHEMY_DATABASE_URL = "sqlite+aiosqlite:///./sql_dns.db"

engine = create_async_engine(SQLALCHEMY_DATABASE_URL, echo=True)
async_session = async_sessionmaker(
    bind=engine, expire_on_commit=False
)

class Base(DeclarativeBase):
    pass

class Product(Base):
    __tablename__ = "products"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255))
    category = Column(String(255))
    price = Column(Float)

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def get_async_db():
    async with async_session() as session:
        yield session
