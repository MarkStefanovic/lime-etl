import datetime

import pytest
import sqlalchemy as sa
from sqlalchemy import orm

from lime_etl.domain import value_objects
from lime_etl.services import job_logging_service
from tests import conftest


@pytest.fixture
def job_logger(session: orm.Session) -> job_logging_service.JobLoggingService:
    return job_logging_service.JobLoggingService(
        session=session,
        batch_id=value_objects.UniqueId("a" * 32),
        job_id=value_objects.UniqueId("b" * 32),
        ts_adapter=conftest.StaticTimestampAdapter(datetime.datetime(2020, 1, 1)),
    )


def test_job_logger_log_error(
    in_memory_db: sa.engine.Engine,
    job_logger: job_logging_service.JobLoggingService,
) -> None:
    job_logger.log_error("Test Message")
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
    job_logger: job_logging_service.JobLoggingService,
) -> None:
    job_logger.log_info("Test Message")
    with in_memory_db.begin() as con:
        actual = con.execute(sa.text("SELECT * FROM job_log")).fetchall()
    assert len(actual) == 1
    first_entry = actual[0]
    assert first_entry.batch_id == "a" * 32
    assert first_entry.job_id == "b" * 32
    assert first_entry.log_level == "Info"
    assert first_entry.message == "Test Message"
