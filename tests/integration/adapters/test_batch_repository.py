import datetime

from sqlalchemy.orm import Session

import adapter.sqlalchemy_batch_repository
import lime_etl as le
from tests import conftest


def test_sqlalchemy_batch_repository_add(session: Session) -> None:
    batch_id = le.UniqueId("f00052d73ca54f649190d80aa26ea779")
    job_id = le.UniqueId.generate()
    test_result = le.JobTestResult(
        id=batch_id,
        job_id=job_id,
        test_name=le.TestName("dummy_test"),
        test_success_or_failure=le.Result.success(),
        execution_millis=le.ExecutionMillis(10),
        execution_success_or_failure=le.Result.success(),
        ts=le.Timestamp(datetime.datetime(2010, 1, 1, 1, 1, 2)),
    )
    job = le.JobResult(
        id=job_id,
        batch_id=batch_id,
        job_name=le.JobName("test_table"),
        status=le.JobStatus.success(),
        execution_millis=le.ExecutionMillis(10),
        test_results=frozenset([test_result]),
        ts=le.Timestamp(datetime.datetime(2010, 1, 1, 1, 1, 1)),
    )
    new_batch = le.BatchStatus(
        id=batch_id,
        name=le.BatchName("test_batch"),
        execution_millis=le.ExecutionMillis(10),
        job_results=frozenset([job]),
        execution_success_or_failure=le.Result.success(),
        running=le.Flag(False),
        ts=le.Timestamp(datetime.datetime(2001, 1, 2, 3, 4, 5)),
    )
    ts_adapter = conftest.static_timestamp_adapter(
        datetime.datetime(2001, 1, 2, 3, 4, 5)
    )
    repo = adapter.sqlalchemy_batch_repository.SqlAlchemyBatchRepository(
        session=session, ts_adapter=ts_adapter
    )
    repo.add(new_batch.to_dto())
    session.commit()
    actual_batches = [dict(row) for row in (session.execute("SELECT * FROM batches"))]
    expected_batches = [
        {
            "execution_error_message": None,
            "execution_error_occurred": 0,
            "execution_millis": 10,
            "id": "f00052d73ca54f649190d80aa26ea779",
            "name": "test_batch",
            "running": 0,
            "ts": "2001-01-02 03:04:05.000000",
        }
    ]
    assert actual_batches == expected_batches


def test_sqlalchemy_batch_repository_update(
    session: Session,
) -> None:
    batch_id = le.UniqueId("f00052d73ca54f649190d80aa26ea779")
    ts_adapter = conftest.static_timestamp_adapter(
        datetime.datetime(2001, 1, 2, 3, 4, 5)
    )

    repo = adapter.sqlalchemy_batch_repository.SqlAlchemyBatchRepository(
        session=session, ts_adapter=ts_adapter
    )
    new_batch = le.BatchStatus(
        id=batch_id,
        name=le.BatchName("test_batch"),
        execution_millis=None,
        job_results=frozenset([]),
        execution_success_or_failure=None,
        running=le.Flag(True),
        ts=le.Timestamp(datetime.datetime(2001, 1, 2, 3, 4, 5)),
    )
    repo.add(new_batch.to_dto())
    new_batch2 = le.BatchStatus(
        id=batch_id,
        name=le.BatchName("test_batch"),
        execution_millis=le.ExecutionMillis(10),
        job_results=frozenset([]),
        execution_success_or_failure=le.Result.success(),
        running=le.Flag(False),
        ts=le.Timestamp(datetime.datetime(2001, 1, 2, 3, 5, 1)),
    )
    repo.update(new_batch2.to_dto())
    session.commit()
    actual_batches = [dict(row) for row in session.execute("SELECT * FROM batches")]
    expected_batches = [
        {
            "execution_error_message": None,
            "execution_error_occurred": 0,
            "execution_millis": 10,
            "id": "f00052d73ca54f649190d80aa26ea779",
            "name": "test_batch",
            "running": 0,
            "ts": "2001-01-02 03:05:01.000000",
        }
    ]
    assert actual_batches == expected_batches


def test_sqlalchemy_batch_repository_delete_old_entries(session: Session) -> None:
    session.execute(
        """
            INSERT INTO batches 
                (id, name, execution_millis, execution_error_occurred, execution_error_message, running, ts)
            VALUES 
                ('b1396d94bd55a455baf80a26209349d6', 'test_batch', 10, 0, NULL, 0, '2010-01-01 01:01:01.000000'),
                ('b2396d94bd55a455baf80a26209349d6', 'test_batch', 10, 0, NULL, 0, '2020-01-01 01:01:01.000000');
        """
    )
    session.execute(
        """
        INSERT INTO jobs 
            (id, batch_id, job_name, execution_millis, execution_error_occurred, execution_error_message, running, ts)
        VALUES 
            ('j1396d94bd55a455baf80a26209349d6', 'b1396d94bd55a455baf80a26209349d6', 'test_table', 100, 0, NULL, 0, '2010-01-01 01:01:01.000000'),
            ('j2396d94bd55a455baf80a26209349d6', 'b2396d94bd55a455baf80a26209349d6', 'test_table', 100, 0, NULL, 0, '2020-01-01 01:01:01.000000');
    """
    )
    session.commit()
    ts_adapter = conftest.static_timestamp_adapter(
        datetime.datetime(2020, 1, 1, 1, 1, 1)
    )
    repo = adapter.sqlalchemy_batch_repository.SqlAlchemyBatchRepository(
        session=session,
        ts_adapter=ts_adapter,
    )
    rows_deleted = repo.delete_old_entries(le.DaysToKeep(10))
    session.commit()
    assert rows_deleted == 1

    actual_batch_rows = [dict(row) for row in session.execute("SELECT * FROM batches")]
    expected_batch_rows = [
        {
            "execution_error_message": None,
            "execution_error_occurred": 0,
            "execution_millis": 10,
            "id": "b2396d94bd55a455baf80a26209349d6",
            "name": "test_batch",
            "running": 0,
            "ts": "2020-01-01 01:01:01.000000",
        }
    ]
    assert actual_batch_rows == expected_batch_rows

    actual_table_update_rows = [
        dict(row) for row in session.execute("SELECT * FROM jobs")
    ]
    expected_table_update_rows = [
        {
            "batch_id": "b2396d94bd55a455baf80a26209349d6",
            "execution_error_message": None,
            "execution_error_occurred": 0,
            "execution_millis": 100,
            "id": "j2396d94bd55a455baf80a26209349d6",
            "job_name": "test_table",
            "running": 0,
            "ts": "2020-01-01 01:01:01.000000",
        }
    ]
    assert actual_table_update_rows == expected_table_update_rows


def test_sqlalchemy_batch_repository_get_latest(session: Session) -> None:
    batch_id_1 = "b1396d94bd55a455baf80a26209349d6"
    batch_id_2 = "b2396d94bd55a455baf80a26209349d6"
    batch_id_3 = "b3396d94bd55a455baf80a26209349d6"
    job_id_1 = "j1396d94bd55a455baf80a26209349d6"
    job_id_2 = "j2396d94bd55a455baf80a26209349d6"
    job_id_3 = "j3396d94bd55a455baf80a26209349d6"
    session.execute(
        f"""
            INSERT INTO batches 
                (id, name, execution_millis, execution_error_occurred, execution_error_message, running, ts)
            VALUES 
                ({batch_id_1!r}, 'test_batch', 10, 0, NULL, 0, '2010-01-01 01:01:01.000000'),
                ({batch_id_2!r}, 'test_batch', 10, 0, NULL, 0, '2010-01-02 01:01:01.000000'),
                ({batch_id_3!r}, 'test_batch', 10, 0, NULL, 0, '2010-01-01 04:01:01.000000');
        """
    )
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
    repo = adapter.sqlalchemy_batch_repository.SqlAlchemyBatchRepository(
        session=session, ts_adapter=ts_adapter
    )
    result = repo.get_latest()
    expected = le.BatchStatusDTO(
        id="b2396d94bd55a455baf80a26209349d6",
        name="test_batch",
        execution_error_message=None,
        execution_error_occurred=False,
        execution_millis=10,
        job_results=[
            le.JobResultDTO(
                id="j3396d94bd55a455baf80a26209349d6",
                batch_id="b2396d94bd55a455baf80a26209349d6",
                job_name="test_table",
                test_results=[],
                execution_millis=100,
                execution_error_occurred=False,
                execution_error_message=None,
                running=False,
                ts=datetime.datetime(2020, 1, 1, 1, 1, 5),
            )
        ],
        running=False,
        ts=datetime.datetime(2010, 1, 2, 1, 1, 1),
    )
    assert result == expected
