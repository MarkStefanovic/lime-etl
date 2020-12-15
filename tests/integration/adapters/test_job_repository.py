import datetime

from sqlalchemy.orm import Session

import lime_etl as le
from tests import conftest


def test_sqlalchemy_job_repository_add(session: Session) -> None:
    batch_id = le.UniqueId("a" * 32)
    job_id = le.UniqueId("b" * 32)
    test_result = le.JobTestResult(
        id=batch_id,
        job_id=job_id,
        test_name=le.TestName("dummy_test"),
        test_success_or_failure=le.Result.success(),
        execution_millis=le.ExecutionMillis(10),
        execution_success_or_failure=le.Result.success(),
        ts=le.Timestamp(datetime.datetime(2010, 1, 1, 1, 1, 2)),
    )
    new_job = le.JobResult(
        id=job_id,
        batch_id=batch_id,
        job_name=le.JobName("test_table"),
        status=le.JobStatus.success(),
        execution_millis=le.ExecutionMillis(10),
        test_results=frozenset([test_result]),
        ts=le.Timestamp(datetime.datetime(2010, 1, 1, 1, 1, 1)),
    )
    ts_adapter = conftest.static_timestamp_adapter(
        datetime.datetime(2001, 1, 2, 3, 4, 5)
    )
    repo = le.SqlAlchemyJobRepository(
        session=session,
        ts_adapter=ts_adapter,
    )
    repo.add(new_job.to_dto())
    session.commit()
    actual_jobs = [dict(row) for row in session.execute("SELECT * FROM jobs")]
    expected_jobs = [
        {
            "batch_id": "a" * 32,
            "execution_error_message": None,
            "execution_error_occurred": 0,
            "execution_millis": 10,
            "id": "b" * 32,
            "job_name": "test_table",
            "running": 0,
            "ts": "2010-01-01 01:01:01.000000",
        }
    ]
    assert actual_jobs == expected_jobs


def test_sqlalchemy_job_repository_update(
    session: Session,
) -> None:
    job_id = le.UniqueId("a" * 32)
    batch_id = le.UniqueId("b" * 32)
    ts_adapter = conftest.static_timestamp_adapter(
        datetime.datetime(2001, 1, 2, 3, 4, 5)
    )
    repo = le.SqlAlchemyJobRepository(
        session=session,
        ts_adapter=ts_adapter,
    )
    new_job = le.JobResult(
        id=job_id,
        batch_id=batch_id,
        job_name=le.JobName("test_job"),
        execution_millis=None,
        status=le.JobStatus.in_progress(),
        test_results=frozenset(),
        ts=le.Timestamp(datetime.datetime(2001, 1, 2, 3, 4, 5)),
    )
    repo.add(new_job.to_dto())
    updated_job = le.JobResult(
        id=job_id,
        batch_id=batch_id,
        job_name=le.JobName("test_job"),
        execution_millis=le.ExecutionMillis(10),
        status=le.JobStatus.success(),
        test_results=frozenset(),
        ts=le.Timestamp(datetime.datetime(2001, 1, 2, 3, 4, 5)),
    )
    repo.update(updated_job.to_dto())
    session.commit()
    actual_jobs = [dict(row) for row in session.execute("SELECT * FROM jobs")]
    expected_jobs = [
        {
            "batch_id": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "execution_error_message": None,
            "execution_error_occurred": 0,
            "execution_millis": 10,
            "id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "job_name": "test_job",
            "running": 0,
            "ts": "2001-01-02 03:04:05.000000",
        }
    ]
    assert actual_jobs == expected_jobs


def test_job_repository_get_latest(session: Session) -> None:
    batch_id_1 = "a" * 32
    batch_id_2 = "b" * 32
    batch_id_3 = "c" * 32
    job_id_1 = "d" * 32
    job_id_2 = "e" * 32
    job_id_3 = "f" * 32
    session.execute(
        f"""
        INSERT INTO jobs 
            (id, batch_id, job_name, execution_millis, execution_error_occurred, execution_error_message, running, ts)
        VALUES 
            ({job_id_1!r}, {batch_id_1!r}, 'test_table', 100, 0, NULL, 0, '2010-01-01 01:01:01.000000'),
            ({job_id_2!r}, {batch_id_3!r}, 'test_table', 100, 0, NULL, 0, '2020-01-01 04:01:01.000000'),
            ({job_id_3!r}, {batch_id_2!r}, 'test_table', 100, 0, NULL, 0, '2020-01-01 01:01:05.000000');
    """
    )
    session.commit()
    ts_adapter = conftest.static_timestamp_adapter(datetime.datetime(2020, 1, 1))
    repo = le.SqlAlchemyJobRepository(
        session=session, ts_adapter=ts_adapter
    )
    result = repo.get_latest(le.JobName("test_table"))
    expected = le.JobResultDTO(
        id="eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        batch_id="cccccccccccccccccccccccccccccccc",
        job_name="test_table",
        test_results=[],
        execution_millis=100,
        execution_error_occurred=False,
        execution_error_message=None,
        running=False,
        ts=datetime.datetime(2020, 1, 1, 4, 1, 1),
    )
    assert result == expected


def test_job_repository_get_last_successful_ts(session: Session) -> None:
    session.execute(
        f"""
        INSERT INTO jobs 
            (id, batch_id, job_name, execution_millis, execution_error_occurred, execution_error_message, running, ts)
        VALUES 
            ('j1396d94bd55a455baf80a26209349d6', 'b1396d94bd55a455baf80a26209349d6', 'test_job', 100, 0, NULL, 0, '2010-01-01 01:01:01.000000'),
            ('j2396d94bd55a455baf80a26209349d6', 'b2396d94bd55a455baf80a26209349d6', 'test_job_2', 100, 0, NULL, 0, '2020-01-01 01:01:05.000000');
    """
    )
    session.commit()
    ts_adapter = conftest.static_timestamp_adapter(
        datetime.datetime(2020, 1, 1, 5, 1, 1)
    )
    repo = le.SqlAlchemyJobRepository(
        session=session, ts_adapter=ts_adapter
    )
    actual = repo.get_last_successful_ts(le.JobName("test_job"))
    expected = le.Timestamp(datetime.datetime(2010, 1, 1, 1, 1, 1))
    assert actual == expected
