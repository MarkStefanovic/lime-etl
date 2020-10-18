import datetime

import pytest
import sqlalchemy as sa
from sqlalchemy import orm

from lime_etl.domain import value_objects
from lime_etl.services import batch_logging_service
from tests import conftest


@pytest.fixture
def batch_logger(
    session: orm.Session,
) -> batch_logging_service.BatchLoggingService:
    return batch_logging_service.BatchLoggingService(
        batch_id=value_objects.UniqueId("a" * 32),
        session=session,
        ts_adapter=conftest.StaticTimestampAdapter(datetime.datetime(2020, 1, 2)),
    )


def test_batch_logger_log_error(
    in_memory_db: sa.engine.Engine,
    batch_logger: batch_logging_service.AbstractBatchLoggingService,
) -> None:
    batch_logger.log_error("Test Message")
    with in_memory_db.begin() as con:
        actual = con.execute(sa.text("SELECT * FROM batch_log")).fetchall()
    assert len(actual) == 1
    first_entry = actual[0]
    assert first_entry.batch_id == "a" * 32
    assert first_entry.log_level == "Error"
    assert first_entry.message == "Test Message"


def test_batch_logger_log_info(
    in_memory_db: sa.engine.Engine,
    batch_logger: batch_logging_service.AbstractBatchLoggingService,
) -> None:
    batch_logger.log_info("Test Message")
    with in_memory_db.begin() as con:
        actual = con.execute(sa.text("SELECT * FROM batch_log")).fetchall()
    assert len(actual) == 1
    first_entry = actual[0]
    assert first_entry.batch_id == "a" * 32
    assert first_entry.log_level == "Info"
    assert first_entry.message == "Test Message"
