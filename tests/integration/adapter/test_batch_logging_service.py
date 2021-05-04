import datetime

import pytest
import sqlalchemy as sa
from sqlalchemy import orm

import lime_etl as le
from tests import conftest


@pytest.fixture
def batch_logger(
    session: orm.Session,
) -> le.SqlAlchemyBatchLogger:
    return le.SqlAlchemyBatchLogger(
        batch_name=le.BatchName("test_batch"),
        batch_id=le.UniqueId("a" * 32),
        session=session,
        ts_adapter=conftest.StaticTimestampAdapter(datetime.datetime(2020, 1, 2)),
    )


def test_batch_logger_log_error(
    in_memory_db: sa.engine.Engine,
    batch_logger: le.BatchLogger,
) -> None:
    batch_logger.error("Test Message")
    with in_memory_db.begin() as con:
        actual = con.execute(sa.text("SELECT * FROM batch_log")).fetchall()
    assert len(actual) == 1
    first_entry = actual[0]
    assert first_entry.batch_id == "a" * 32
    assert first_entry.log_level == "Error"
    assert first_entry.message == "Test Message"


def test_batch_logger_log_info(
    in_memory_db: sa.engine.Engine,
    batch_logger: le.BatchLogger,
) -> None:
    batch_logger.info("Test Message")
    with in_memory_db.begin() as con:
        actual = con.execute(sa.text("SELECT * FROM batch_log")).fetchall()
    assert len(actual) == 1
    first_entry = actual[0]
    assert first_entry.batch_id == "a" * 32
    assert first_entry.log_level == "Info"
    assert first_entry.message == "Test Message"
