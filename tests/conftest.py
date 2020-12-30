import datetime
import os
import typing

import lime_uow as lu
import pytest
import sqlalchemy as sa
import dotenv
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

    @classmethod
    def interface(cls) -> typing.Type[le.TimestampAdapter]:
        return le.TimestampAdapter

    def now(self) -> le.Timestamp:
        return le.Timestamp(self.dt)


# @pytest.fixture
# def postgres_db_uri() -> str:
#     user = "tester"
#     db_name = "testdb"
#     pwd = "abc123"
#     # host = "0.0.0.0"
#     host = "postgres"
#     port = 5432
#     return f"postgresql://{user}:{pwd}@{host}:{port}/{db_name}"


# @pytest.fixture
# def postgres_db_uri() -> str:
#     dotenv.load_dotenv(dotenv.find_dotenv(".testenv"))
#     uri = os.environ["TEST_DB_SQLALCHEMY_URI"]
#     engine = sa.create_engine(
#         postgres_db_uri, isolation_level="SERIALIZABLE", echo=True
#     )
#     with t


@pytest.fixture(scope="function")
def postgres_db() -> sa.engine.Engine:
    dotenv.load_dotenv(dotenv.find_dotenv(".testenv"))
    uri = os.environ["TEST_DB_SQLALCHEMY_URI"]
    engine = sa.create_engine(uri, isolation_level="SERIALIZABLE", echo=True)
    le.admin_metadata.drop_all(engine)
    le.admin_metadata.create_all(engine)
    return engine


@pytest.fixture(scope="function")
def postgres_session(
    postgres_db: sa.engine.Engine,
) -> typing.Generator[orm.sessionmaker, None, None]:
    le.start_mappers()
    yield orm.sessionmaker(bind=postgres_db)()
    orm.clear_mappers()
