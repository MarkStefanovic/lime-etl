import datetime

import pytest
from sqlalchemy.orm import Session

import lime_etl as le
from tests import conftest


@pytest.fixture
def ts_adapter() -> le.TimestampAdapter:
    return conftest.static_timestamp_adapter(datetime.datetime(2020, 1, 1))


def test_sqlalchemy_batch_log_entry_repsitory_delete_old_entries(
    session: Session, ts_adapter: le.TimestampAdapter
) -> None:
    session.execute(
        f"""
        INSERT INTO batch_log (id, batch_id, log_level, message, ts)
        VALUES 
            ({'a'*32!r}, {'b'*32!r}, 'Info', 'test message 1', '2010-01-01'),
            ({'c'*32!r}, {'d'*32!r}, 'Error', 'test message 2', '2020-01-02 02:01:03'),
            ({'f'*32!r}, {'b'*32!r}, 'Error', 'test message 2', '2010-01-03 02:01:03');
    """
    )
    repo = le.SqlAlchemyBatchLogRepository(
        session=session, ts_adapter=ts_adapter
    )
    result = repo.delete_old_entries(days_to_keep=le.DaysToKeep(10))
    assert result == 2
