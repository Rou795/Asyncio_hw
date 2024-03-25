import os

from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncAttrs

POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "Tdutybq2020")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "swapi_db")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

PG_DSN = f"postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

engine = create_async_engine(PG_DSN)
Session = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase, AsyncAttrs):
    pass


class People(Base):
    __tablename__ = "peoples"

    id: Mapped[int] = mapped_column(primary_key=True)
    birth_year: Mapped[str] = mapped_column(String(15))
    eye_color: Mapped[str] = mapped_column(String(100))
    films: Mapped[str] = mapped_column(String(1000))
    gender: Mapped[str] = mapped_column(String(100))
    hair_color: Mapped[str] = mapped_column(String(100))
    height: Mapped[str] = mapped_column()
    homeworld: Mapped[str] = mapped_column(String(100))
    mass: Mapped[str] = mapped_column()
    name: Mapped[str] = mapped_column(String(100))
    skin_color: Mapped[str] = mapped_column(String(100))
    species: Mapped[str] = mapped_column(String(1000))
    starships: Mapped[str] = mapped_column(String(1000))
    vehicles: Mapped[str] = mapped_column(String(1000))


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
