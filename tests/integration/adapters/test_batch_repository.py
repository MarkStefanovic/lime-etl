import datetime

from sqlalchemy.orm import Session

from lime_etl.adapters import batch_repository
from lime_etl.domain import batch, job_result, job_test_result, value_objects
from tests import conftest


def test_sqlalchemy_batch_repository_add(session: Session) -> None:
    batch_id = value_objects.UniqueId("f00052d73ca54f649190d80aa26ea779")
    job_id = value_objects.UniqueId.generate()
    test_result = job_test_result.JobTestResult(
        id=batch_id,
        job_id=job_id,
        test_name=value_objects.TestName("dummy_test"),
        test_success_or_failure=value_objects.Result.success(),
        ts=value_objects.Timestamp(datetime.datetime(2010, 1, 1, 1, 1, 2)),
    )
    job = job_result.JobResult(
        id=job_id,
        batch_id=batch_id,
        job_name=value_objects.JobName("test_table"),
        execution_success_or_failure=value_objects.Result.success(),
        execution_millis=value_objects.ExecutionMillis(10),
        test_results=frozenset([test_result]),
        ts=value_objects.Timestamp(datetime.datetime(2010, 1, 1, 1, 1, 1)),
    )
    new_batch = batch.Batch(
        id=batch_id,
        execution_millis=value_objects.ExecutionMillis(10),
        job_results=frozenset([job]),
        execution_success_or_failure=value_objects.Result.success(),
        running=value_objects.Flag(False),
        ts=value_objects.Timestamp(datetime.datetime(2001, 1, 2, 3, 4, 5)),
    )
    ts_adapter = conftest.static_timestamp_adapter(
        datetime.datetime(2001, 1, 2, 3, 4, 5)
    )
    repo = batch_repository.SqlAlchemyBatchRepository(
        session=session, ts_adapter=ts_adapter
    )
    repo.add(new_batch=new_batch)
    session.commit()
    actual_batches = [dict(row) for row in (session.execute("SELECT * FROM batches"))]
    expected_batches = [
        {
            "execution_error_message": None,
            "execution_error_occurred": 0,
            "execution_millis": 10,
            "id": "f00052d73ca54f649190d80aa26ea779",
            "running": 0,
            "ts": "2001-01-02 03:04:05.000000",
        }
    ]
    assert actual_batches == expected_batches


def test_sqlalchemy_batch_repository_add_job_result(session: Session) -> None:
    batch_id = value_objects.UniqueId("f00052d73ca54f649190d80aa26ea779")
    ts_adapter = conftest.static_timestamp_adapter(
        datetime.datetime(2001, 1, 2, 3, 4, 5)
    )

    repo = batch_repository.SqlAlchemyBatchRepository(
        session=session, ts_adapter=ts_adapter
    )
    new_batch = batch.Batch(
        id=batch_id,
        execution_millis=None,
        job_results=frozenset([]),
        execution_success_or_failure=None,
        running=value_objects.Flag(True),
        ts=value_objects.Timestamp(datetime.datetime(2001, 1, 2, 3, 4, 5)),
    )
    repo.add(new_batch=new_batch)

    job_id = value_objects.UniqueId("g00052d73ca54f649190d80aa26ea779")
    test_result = job_test_result.JobTestResult(
        id=batch_id,
        job_id=job_id,
        test_name=value_objects.TestName("dummy_test"),
        test_success_or_failure=value_objects.Result.success(),
        ts=value_objects.Timestamp(datetime.datetime(2010, 1, 1, 1, 1, 2)),
    )
    new_job = job_result.JobResult(
        id=job_id,
        batch_id=batch_id,
        job_name=value_objects.JobName("test_table"),
        execution_success_or_failure=value_objects.Result.success(),
        execution_millis=value_objects.ExecutionMillis(10),
        test_results=frozenset([test_result]),
        ts=value_objects.Timestamp(datetime.datetime(2010, 1, 1, 1, 1, 1)),
    )
    repo.add_job_result(new_job)
    session.commit()
    actual_batches = [dict(row) for row in session.execute("SELECT * FROM batches")]
    expected_batches = [
        {
            "execution_error_message": None,
            "execution_error_occurred": None,
            "execution_millis": None,
            "id": "f00052d73ca54f649190d80aa26ea779",
            "running": 1,
            "ts": "2001-01-02 03:04:05.000000",
        }
    ]
    assert actual_batches == expected_batches

    actual_jobs = [dict(row) for row in session.execute("SELECT * FROM jobs")]
    expected_jobs = [
        {
            "batch_id": "f00052d73ca54f649190d80aa26ea779",
            "execution_error_message": None,
            "execution_error_occurred": 0,
            "execution_millis": 10,
            "id": "g00052d73ca54f649190d80aa26ea779",
            "job_name": "test_table",
            "ts": "2010-01-01 01:01:01.000000",
        }
    ]
    assert actual_jobs == expected_jobs


def test_sqlalchemy_batch_repository_update(
    session: Session,
) -> None:
    batch_id = value_objects.UniqueId("f00052d73ca54f649190d80aa26ea779")
    ts_adapter = conftest.static_timestamp_adapter(
        datetime.datetime(2001, 1, 2, 3, 4, 5)
    )

    repo = batch_repository.SqlAlchemyBatchRepository(
        session=session, ts_adapter=ts_adapter
    )
    new_batch = batch.Batch(
        id=batch_id,
        execution_millis=None,
        job_results=frozenset([]),
        execution_success_or_failure=None,
        running=value_objects.Flag(True),
        ts=value_objects.Timestamp(datetime.datetime(2001, 1, 2, 3, 4, 5)),
    )
    repo.add(new_batch=new_batch)
    new_batch2 = batch.Batch(
        id=batch_id,
        execution_millis=value_objects.ExecutionMillis(10),
        job_results=frozenset([]),
        execution_success_or_failure=value_objects.Result.success(),
        running=value_objects.Flag(False),
        ts=value_objects.Timestamp(datetime.datetime(2001, 1, 2, 3, 5, 1)),
    )
    repo.update(new_batch2)
    session.commit()
    actual_batches = [dict(row) for row in session.execute("SELECT * FROM batches")]
    expected_batches = [
        {
            "execution_error_message": None,
            "execution_error_occurred": 0,
            "execution_millis": 10,
            "id": "f00052d73ca54f649190d80aa26ea779",
            "running": 0,
            "ts": "2001-01-02 03:05:01.000000",
        }
    ]
    assert actual_batches == expected_batches


def test_sqlalchemy_table_repository_delete_old_entries(session: Session) -> None:
    session.execute(
        """
            INSERT INTO batches 
                (id, execution_millis, execution_error_occurred, execution_error_message, running, ts)
            VALUES 
                ('b1396d94bd55a455baf80a26209349d6', 10, 0, NULL, 0, '2010-01-01 01:01:01.000000'),
                ('b2396d94bd55a455baf80a26209349d6', 10, 0, NULL, 0, '2020-01-01 01:01:01.000000');
        """
    )
    session.execute(
        """
        INSERT INTO jobs 
            (id, batch_id, job_name, execution_millis, execution_error_occurred, execution_error_message, ts)
        VALUES 
            ('j1396d94bd55a455baf80a26209349d6', 'b1396d94bd55a455baf80a26209349d6', 'test_table', 100, 0, NULL, '2010-01-01 01:01:01.000000'),
            ('j2396d94bd55a455baf80a26209349d6', 'b2396d94bd55a455baf80a26209349d6', 'test_table', 100, 0, NULL, '2020-01-01 01:01:01.000000');
    """
    )
    session.commit()
    ts_adapter = conftest.static_timestamp_adapter(
        datetime.datetime(2020, 1, 1, 1, 1, 1)
    )
    repo = batch_repository.SqlAlchemyBatchRepository(
        session=session,
        ts_adapter=ts_adapter,
    )
    rows_deleted = repo.delete_old_entries(days_to_keep=value_objects.DaysToKeep(10))
    session.commit()
    assert rows_deleted == 1

    actual_batch_rows = [dict(row) for row in session.execute("SELECT * FROM batches")]
    expected_batch_rows = [
        {
            "execution_error_message": None,
            "execution_error_occurred": 0,
            "execution_millis": 10,
            "id": "b2396d94bd55a455baf80a26209349d6",
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
            "ts": "2020-01-01 01:01:01.000000",
        }
    ]
    assert actual_table_update_rows == expected_table_update_rows


def test_get_batch_by_id(session: Session) -> None:
    batch_id_1 = "b1396d94bd55a455baf80a26209349d6"
    batch_id_2 = "b2396d94bd55a455baf80a26209349d6"
    batch_id_3 = "b3396d94bd55a455baf80a26209349d6"
    job_id_1 = "j1396d94bd55a455baf80a26209349d6"
    job_id_2 = "j2396d94bd55a455baf80a26209349d6"
    job_id_3 = "j3396d94bd55a455baf80a26209349d6"
    session.execute(
        f"""
            INSERT INTO batches 
                (id, execution_millis, execution_error_occurred, execution_error_message, running, ts)
            VALUES 
                ({batch_id_1!r}, 10, 0, NULL, 0, '2010-01-01 01:01:01.000000'),
                ({batch_id_2!r}, 10, 0, NULL, 0, '2010-01-01 02:01:01.000000'),
                ({batch_id_3!r}, 10, 0, NULL, 0, '2010-01-01 04:01:01.000000');
        """
    )
    session.execute(
        f"""
        INSERT INTO jobs 
            (id, batch_id, job_name, execution_millis, execution_error_occurred, execution_error_message, ts)
        VALUES 
            ({job_id_1!r}, {batch_id_1!r}, 'test_table', 100, 0, NULL, '2010-01-01 01:01:01.000000'),
            ({job_id_2!r}, {batch_id_3!r}, 'test_table', 100, 0, NULL, '2020-01-04 01:01:01.000000'),
            ({job_id_3!r}, {batch_id_2!r}, 'test_table', 100, 0, NULL, '2020-01-01 01:01:05.000000');
    """
    )
    session.commit()
    ts_adapter = conftest.static_timestamp_adapter(datetime.datetime(2020, 1, 1))
    repo = batch_repository.SqlAlchemyBatchRepository(
        session=session, ts_adapter=ts_adapter
    )
    result = repo.get_batch_by_id(value_objects.UniqueId(batch_id_2))
    expected = batch.Batch(
        id=value_objects.UniqueId(batch_id_2),
        execution_millis=value_objects.ExecutionMillis(10),
        job_results=frozenset(
            {
                job_result.JobResult(
                    id=value_objects.UniqueId("j3396d94bd55a455baf80a26209349d6"),
                    batch_id=value_objects.UniqueId("b2396d94bd55a455baf80a26209349d6"),
                    job_name=value_objects.JobName("test_table"),
                    test_results=frozenset(),
                    execution_success_or_failure=value_objects.Result.success(),
                    execution_millis=value_objects.ExecutionMillis(100),
                    ts=value_objects.Timestamp(datetime.datetime(2020, 1, 1, 1, 1, 5)),
                )
            }
        ),
        execution_success_or_failure=value_objects.Result.success(),
        running=value_objects.Flag(False),
        ts=value_objects.Timestamp(datetime.datetime(2010, 1, 1, 2, 1, 1)),
    )
    assert result == expected


def test_get_latest(session: Session) -> None:
    batch_id_1 = "b1396d94bd55a455baf80a26209349d6"
    batch_id_2 = "b2396d94bd55a455baf80a26209349d6"
    batch_id_3 = "b3396d94bd55a455baf80a26209349d6"
    job_id_1 = "j1396d94bd55a455baf80a26209349d6"
    job_id_2 = "j2396d94bd55a455baf80a26209349d6"
    job_id_3 = "j3396d94bd55a455baf80a26209349d6"
    session.execute(
        f"""
            INSERT INTO batches 
                (id, execution_millis, execution_error_occurred, execution_error_message, running, ts)
            VALUES 
                ({batch_id_1!r}, 10, 0, NULL, 0, '2010-01-01 01:01:01.000000'),
                ({batch_id_2!r}, 10, 0, NULL, 0, '2010-01-02 01:01:01.000000'),
                ({batch_id_3!r}, 10, 0, NULL, 0, '2010-01-01 04:01:01.000000');
        """
    )
    session.execute(
        f"""
        INSERT INTO jobs 
            (id, batch_id, job_name, execution_millis, execution_error_occurred, execution_error_message, ts)
        VALUES 
            ({job_id_1!r}, {batch_id_1!r}, 'test_table', 100, 0, NULL, '2010-01-01 01:01:01.000000'),
            ({job_id_2!r}, {batch_id_3!r}, 'test_table', 100, 0, NULL, '2020-01-01 04:01:01.000000'),
            ({job_id_3!r}, {batch_id_2!r}, 'test_table', 100, 0, NULL, '2020-01-01 01:01:05.000000');
    """
    )
    session.commit()
    ts_adapter = conftest.static_timestamp_adapter(datetime.datetime(2020, 1, 1))
    repo = batch_repository.SqlAlchemyBatchRepository(
        session=session, ts_adapter=ts_adapter
    )
    result = repo.get_latest()
    expected = batch.Batch(
        id=value_objects.UniqueId("b2396d94bd55a455baf80a26209349d6"),
        execution_millis=value_objects.ExecutionMillis(10),
        job_results=frozenset(
            {
                job_result.JobResult(
                    id=value_objects.UniqueId("j3396d94bd55a455baf80a26209349d6"),
                    batch_id=value_objects.UniqueId("b2396d94bd55a455baf80a26209349d6"),
                    job_name=value_objects.JobName("test_table"),
                    test_results=frozenset(),
                    execution_success_or_failure=value_objects.Result.success(),
                    execution_millis=value_objects.ExecutionMillis(100),
                    ts=value_objects.Timestamp(datetime.datetime(2020, 1, 1, 1, 1, 5)),
                )
            }
        ),
        execution_success_or_failure=value_objects.Result.success(),
        running=value_objects.Flag(False),
        ts=value_objects.Timestamp(datetime.datetime(2010, 1, 2, 1, 1, 1)),
    )
    assert result == expected


def test_get_latest_results_for_job(session: Session) -> None:
    session.execute(
        f"""
        INSERT INTO batches 
            (id, execution_millis, execution_error_occurred, execution_error_message, running, ts)
        VALUES 
            ('b1396d94bd55a455baf80a26209349d6', 10, 0, NULL, 0, '2010-01-01 01:01:01.000000'),
            ('b2396d94bd55a455baf80a26209349d6', 10, 0, NULL, 0, '2010-01-02 01:01:01.000000'),
            ('b3396d94bd55a455baf80a26209349d6', 10, 0, NULL, 0, '2010-01-01 04:01:01.000000');
    """
    )
    session.execute(
        f"""
        INSERT INTO jobs 
            (id, batch_id, job_name, execution_millis, execution_error_occurred, execution_error_message, ts)
        VALUES 
            ('j1396d94bd55a455baf80a26209349d6', 'b1396d94bd55a455baf80a26209349d6', 'test_job', 100, 0, NULL, '2010-01-01 01:01:01.000000'),
            ('j2396d94bd55a455baf80a26209349d6', 'b2396d94bd55a455baf80a26209349d6', 'test_job_2', 100, 0, NULL, '2020-01-01 01:01:05.000000');
    """
    )
    session.execute(
        f"""
        INSERT INTO job_test_results 
            (id, job_id, test_name, test_passed, test_failure_message, ts)
        VALUES 
            ('i1396d94bd55a455baf80a26209349d6', 'j1396d94bd55a455baf80a26209349d6', 'dummy_test_1', 1, NULL, '2010-01-01 01:01:01.000000'),
            ('i2396d94bd55a455baf80a26209349d6', 'j1396d94bd55a455baf80a26209349d6', 'dummy_test_2', 1, NULL, '2020-01-01 04:01:01.000000'),
            ('i3396d94bd55a455baf80a26209349d6', 'j2396d94bd55a455baf80a26209349d6', 'dummy_test_2', 1, NULL, '2020-01-01 01:01:05.000000');
    """
    )
    session.commit()
    ts_adapter = conftest.static_timestamp_adapter(
        datetime.datetime(2020, 1, 1, 5, 1, 1)
    )
    repo = batch_repository.SqlAlchemyBatchRepository(
        session=session, ts_adapter=ts_adapter
    )
    actual = repo.get_latest_result_for_job(
        value_objects.JobName("test_job")
    ).test_results
    expected = frozenset(
        {
            job_test_result.JobTestResult(
                id=value_objects.UniqueId("i1396d94bd55a455baf80a26209349d6"),
                job_id=value_objects.UniqueId("j1396d94bd55a455baf80a26209349d6"),
                test_name=value_objects.TestName("dummy_test_1"),
                test_success_or_failure=value_objects.Result(value_objects.Success()),
                ts=value_objects.Timestamp(datetime.datetime(2010, 1, 1, 1, 1, 1)),
            ),
            job_test_result.JobTestResult(
                id=value_objects.UniqueId("i2396d94bd55a455baf80a26209349d6"),
                job_id=value_objects.UniqueId("j1396d94bd55a455baf80a26209349d6"),
                test_name=value_objects.TestName("dummy_test_2"),
                test_success_or_failure=value_objects.Result(value_objects.Success()),
                ts=value_objects.Timestamp(datetime.datetime(2020, 1, 1, 4, 1, 1)),
            ),
        }
    )
    assert actual == expected
