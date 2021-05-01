import datetime

import pytest
import sqlalchemy as sa
from sqlalchemy import orm

import lime_etl as le
from tests import conftest


@pytest.fixture
def job_logger(session: orm.Session) -> le.SqlAlchemyJobLogger:
    return le.SqlAlchemyJobLogger(
        session=session,
        batch_id=le.UniqueId("a" * 32),
        job_id=le.UniqueId("b" * 32),
        ts_adapter=conftest.StaticTimestampAdapter(datetime.datetime(2020, 1, 1)),
    )


def test_job_logger_log_error(
    in_memory_db: sa.engine.Engine,
    job_logger: le.SqlAlchemyJobLogger,
) -> None:
    job_logger.error("Test Message")
    with in_memory_db.begin() as con:
        actual = con.execute(sa.text("SELECT * FROM job_log")).fetchall()
    assert len(actual) == 1
    first_entry = actual[0]
    assert first_entry.batch_id == "a" * 32
    assert first_entry.job_id == "b" * 32
    assert first_entry.log_level == "Error"
    assert first_entry.message == "Test Message"


def test_job_logger_log_info(
    in_memory_db: sa.engine.Engine,
    job_logger: le.SqlAlchemyJobLogger,
) -> None:
    job_logger.info("Test Message")
    with in_memory_db.begin() as con:
        actual = con.execute(sa.text("SELECT * FROM job_log")).fetchall()
    assert len(actual) == 1
    first_entry = actual[0]
    assert first_entry.batch_id == "a" * 32
    assert first_entry.job_id == "b" * 32
    assert first_entry.log_level == "Info"
    assert first_entry.message == "Test Message"
