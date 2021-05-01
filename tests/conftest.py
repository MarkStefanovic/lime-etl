import datetime
import os
import typing

import dotenv
import pytest
import sqlalchemy as sa
from sqlalchemy import orm

import lime_etl as le


@pytest.fixture
def in_memory_db_uri() -> str:
    return "sqlite:///:memory:"


@pytest.fixture
def in_memory_db(in_memory_db_uri: str) -> sa.engine.Engine:
    engine = sa.create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=sa.pool.StaticPool,
        echo=True,
    )
    with engine.begin() as con:
        con.execute("ATTACH ':memory:' as etl")
    le.admin_metadata.create_all(engine)
    return engine


@pytest.fixture
def session_factory(
    in_memory_db: sa.engine.Engine,
) -> typing.Generator[orm.sessionmaker, None, None]:
    le.start_mappers()
    yield orm.sessionmaker(bind=in_memory_db)
    orm.clear_mappers()


@pytest.fixture
def session(session_factory: orm.sessionmaker) -> orm.Session:
    return session_factory()


def static_timestamp_adapter(dt: datetime.datetime, /) -> le.TimestampAdapter:
    return StaticTimestampAdapter(dt)


class StaticTimestampAdapter(le.TimestampAdapter):
    def __init__(self, dt: datetime.datetime):
        self.dt = dt

    @staticmethod
    def key() -> str:
        return le.TimestampAdapter.__name__

    def now(self) -> le.Timestamp:
        return le.Timestamp(self.dt)


def drop_tables(engine: sa.engine.Engine, *tables: str) -> None:
    with engine.begin() as con:
        for table in tables:
            try:
                con.execute(f"DROP TABLE etl.{table} CASCADE")
            except:
                pass


@pytest.fixture(scope="function")
def postgres_db() -> sa.engine.Engine:
    dotenv.load_dotenv(dotenv.find_dotenv(".testenv"))
    uri = os.environ["TEST_DB_SQLALCHEMY_URI"]
    engine = sa.create_engine(uri, isolation_level="SERIALIZABLE", echo=True)
    drop_tables(
        engine,
        "batch_log",
        "job_log",
        "batches",
        "job_test_results",
        "jobs",
    )
    le.set_schema(le.SchemaName("etl"))
    # le.admin_metadata.drop_all(engine)
    le.admin_metadata.create_all(engine)
    return engine


class TestConfig(le.Config):
    @property
    def admin_engine_uri(self) -> le.DbUri:
        return le.DbUri(os.environ["TEST_DB_SQLALCHEMY_URI"])


@pytest.fixture(scope="session")
def test_config() -> le.Config:
    return TestConfig()
