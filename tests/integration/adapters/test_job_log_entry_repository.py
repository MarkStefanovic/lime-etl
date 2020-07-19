import datetime

import pytest
from sqlalchemy.orm import Session

from src.adapters import timestamp_adapter
from src.adapters.job_log_repository import SqlAlchemyJobLogRepository
from src.domain import value_objects
from src.domain.job_log_entry import JobLogEntry, JobLogEntryDTO
from src.domain.value_objects import (
    LogLevel,
    LogLevelOption,
    LogMessage,
    Timestamp,
    UniqueId,
)
from tests import conftest


@pytest.fixture
def ts_adapter() -> timestamp_adapter.TimestampAdapter:
    return conftest.static_timestamp_adapter(datetime.datetime(2020, 1, 1))


def test_log_works_on_sqlite(session: Session, ts_adapter: timestamp_adapter.TimestampAdapter) -> None:
    log_entry_id = "x" * 32
    log_entry = JobLogEntry(
        id=UniqueId(log_entry_id),
        batch_id=UniqueId.generate(),
        job_id=UniqueId.generate(),
        log_level=LogLevel(LogLevelOption.Info),
        message=LogMessage("test message"),
        ts=Timestamp(datetime.datetime(2020, 1, 1)),
    )
    repo = SqlAlchemyJobLogRepository(session=session, ts_adapter=ts_adapter)
    actual = repo.add(log_entry)
    assert actual == log_entry
    rows = session.query(JobLogEntryDTO).all()
    assert rows == [log_entry.to_dto()]


def test_log_works_on_postgres(postgres_session: Session, ts_adapter: timestamp_adapter.TimestampAdapter) -> None:
    log_entry_id = "x" * 32
    log_entry = JobLogEntry(
        id=UniqueId(log_entry_id),
        batch_id=UniqueId.generate(),
        job_id=UniqueId.generate(),
        log_level=LogLevel(LogLevelOption.Info),
        message=LogMessage("test message"),
        ts=Timestamp(datetime.datetime(2020, 1, 1)),
    )
    repo = SqlAlchemyJobLogRepository(session=postgres_session, ts_adapter=ts_adapter)
    actual = repo.add(log_entry)
    assert actual == log_entry
    rows = postgres_session.query(JobLogEntryDTO).all()
    assert rows == [log_entry.to_dto()]


def test_delete_old_entries_works_on_sqlite(
    session: Session, ts_adapter: timestamp_adapter.TimestampAdapter
) -> None:
    session.execute(
        f"""
        INSERT INTO job_log (id, batch_id, job_id, log_level, message, ts)
        VALUES 
            ({'a'*32!r}, {'b'*32!r}, {'g' * 32!r}, 'Info', 'test message 1', '2010-01-01'),
            ({'c'*32!r}, {'d'*32!r}, {'e' * 32!r}, 'Error', 'test message 2', '2020-01-02 02:01:03'),
            ({'f'*32!r}, {'b'*32!r}, {'a' * 32!r}, 'Error', 'test message 2', '2010-01-03 02:01:03');
    """
    )
    repo = SqlAlchemyJobLogRepository(session=session, ts_adapter=ts_adapter)
    actual = repo.delete_old_entries(days_to_keep=value_objects.DaysToKeep(10))
    assert actual == 2


def test_delete_old_entries_works_on_postgres(
    postgres_session: Session, ts_adapter: timestamp_adapter.TimestampAdapter
) -> None:
    postgres_session.execute(
        f"""
        INSERT INTO job_log (id, batch_id, job_id, log_level, message, ts)
        VALUES 
            ({'a'*32!r}, {'b'*32!r}, {'g' * 32!r}, 'Info', 'test message 1', '2010-01-01'),
            ({'c'*32!r}, {'d'*32!r}, {'e' * 32!r}, 'Error', 'test message 2', '2020-01-02 02:01:03'),
            ({'f'*32!r}, {'b'*32!r}, {'a' * 32!r}, 'Error', 'test message 2', '2010-01-03 02:01:03');
    """
    )
    repo = SqlAlchemyJobLogRepository(session=postgres_session, ts_adapter=ts_adapter)
    actual = repo.delete_old_entries(days_to_keep=value_objects.DaysToKeep(10))
    assert actual == 2
