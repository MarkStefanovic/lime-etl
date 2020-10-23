from __future__ import annotations

import abc
import dataclasses
import typing

import pytest
import sqlalchemy as sa
from sqlalchemy import orm

import lime_etl as le

meta = sa.MetaData()

messages_table = sa.Table(
    "messages",
    meta,
    sa.Column("id", sa.Integer, autoincrement=True, primary_key=True),
    sa.Column("message", sa.Text, nullable=False),
)


@dataclasses.dataclass(unsafe_hash=True)
class Message:
    id: int
    message: str


# class MessageDbSession(le.SqlAlchemySession):
#     def __init__(self, session_factory: orm.sessionmaker, /):
#         super().__init__(session_factory)


class AbstractMessageRepo(le.Repository[Message], abc.ABC):
    @property
    def entity_type(self) -> typing.Type[Message]:
        return Message


class MessageRepo(AbstractMessageRepo, le.SqlAlchemyRepository[Message]):
    def __init__(self, session: orm.Session, /):
        super().__init__(session)

    @classmethod
    def interface(cls) -> typing.Type[AbstractMessageRepo]:
        return AbstractMessageRepo


@pytest.fixture
def messages_session_factory(
    in_memory_db: sa.engine.Engine,
) -> typing.Generator[orm.sessionmaker, None, None]:
    orm.mapper(Message, messages_table)
    meta.create_all(bind=in_memory_db)
    yield orm.sessionmaker(bind=in_memory_db)
    orm.clear_mappers()


class MessageUOW(le.UnitOfWork):
    def __init__(self, shared_resources: le.SharedResources):
        super().__init__(shared_resources)

    def create_resources(
        self, shared_resources: le.SharedResources
    ) -> typing.Set[le.Resource[typing.Any]]:
        return {MessageRepo(shared_resources.get(le.SqlAlchemySession))}

    @property
    def message_repo(self) -> MessageRepo:
        return self.get_resource(MessageRepo)


class MessageJob(le.JobSpec):
    def __init__(
        self,
        messages: typing.Iterable[Message],
        dependencies: typing.Iterable[str],
        job_name: str,
    ):
        self._messages = list(messages)
        self._dependencies = [le.JobName(n) for n in dependencies]
        self._job_name = le.JobName(job_name)

    def run(
        self,
        batch_uow: le.UnitOfWork,
        logger: le.AbstractJobLoggingService,
    ) -> le.Result:
        with batch_uow as uow:
            uow = typing.cast(MessageUOW, uow)
            uow.message_repo.add_all(self._messages)
            uow.save()
        return le.Result.success()

    @property
    def dependencies(self) -> typing.List[le.JobName]:
        return self._dependencies

    @property
    def job_name(self) -> le.JobName:
        return self._job_name

    def on_execution_error(self, error_message: str) -> typing.Optional[le.JobSpec]:
        return None

    def on_test_failure(
        self, test_results: typing.FrozenSet[le.JobTestResult]
    ) -> typing.Optional[le.JobSpec]:
        return None

    @property
    def seconds_between_refreshes(self) -> le.SecondsBetweenRefreshes:
        return le.SecondsBetweenRefreshes(300)

    @property
    def timeout_seconds(self) -> le.TimeoutSeconds:
        return le.TimeoutSeconds(60)

    @property
    def max_retries(self) -> le.MaxRetries:
        return le.MaxRetries(1)

    def test(
        self,
        batch_uow: le.UnitOfWork,
        logger: le.AbstractJobLoggingService,
    ) -> typing.List[le.SimpleJobTestResult]:
        with batch_uow as uow:
            uow = typing.cast(MessageUOW, uow)
            result = uow.message_repo.all()
            missing_messages = [msg for msg in self._messages if msg not in result]
            test_name = le.TestName("Messages saved")
            if missing_messages:
                return [
                    le.SimpleJobTestResult(
                        test_name=test_name,
                        test_success_or_failure=le.Result.failure(
                            f"Missing Messages = {missing_messages}"
                        ),
                    )
                ]
            else:
                return [
                    le.SimpleJobTestResult(
                        test_name=test_name,
                        test_success_or_failure=le.Result.success(),
                    )
                ]


def test_run_admin(in_memory_db: sa.engine.Engine) -> None:
    actual = le.run_admin(
        engine_or_uri=in_memory_db,
        schema=None,
        skip_tests=False,
        days_logs_to_keep=3,
    )
    assert actual.running == le.Flag(False)
    assert actual.broken_jobs == frozenset()
    assert len(actual.job_results) == 1


def test_run_with_sqlite_using_default_parameters_happy_path(
    in_memory_db: sa.engine.Engine,
    messages_session_factory: orm.sessionmaker,
) -> None:
    jobs = [
        MessageJob(
            job_name="hello_world_job",
            dependencies=[],
            messages=[
                Message(id=1, message="Hello"),
                Message(id=2, message="World"),
            ],
        ),
        MessageJob(
            job_name="hello_world_job2",
            dependencies=["hello_world_job"],
            messages=[
                Message(id=3, message="Have"),
                Message(id=4, message="Fun"),
            ],
        ),
    ]
    shared_resources = le.SharedResources(
        le.SqlAlchemySession(messages_session_factory)
    )
    actual = le.run(
        batch_name="test_batch",
        engine_or_uri=in_memory_db,
        jobs=jobs,
        batch_uow=MessageUOW(shared_resources),
    )
    job_execution_results = [
        jr.execution_success_or_failure for jr in actual.job_results
    ]
    assert all(
        jr.execution_success_or_failure == le.Result.success()
        for jr in actual.job_results
    ), f"Expected all Success values, but got {job_execution_results}"
    assert actual.job_names == {
        le.JobName("hello_world_job"),
        le.JobName("hello_world_job2"),
    }
    assert actual.broken_jobs == set()
    assert actual.running.value is False
    assert actual.execution_millis is not None
    assert actual.execution_millis.value > 0
    assert actual.ts is not None

    sql = """
        SELECT execution_error_occurred, execution_error_message, ts
        FROM batches
        ORDER BY ts DESC
    """
    with in_memory_db.begin() as con:
        result = con.execute(sa.text(sql)).fetchall()
    assert (
        len(result) == 1
    ), f"{len(result)} batches were added.  There should only be 1 batch entry."
    row = result[0]
    assert row["execution_error_occurred"] == 0
    assert row["execution_error_message"] is None
    assert row["ts"] is not None

    sql = """
        SELECT job_name, execution_millis, execution_error_occurred, execution_error_message, ts
        FROM jobs
        ORDER BY ts
    """
    with in_memory_db.begin() as con:
        result = con.execute(sa.text(sql)).fetchall()
    assert (
        len(result) == 2
    ), f"{len(result)} job results were added, but 2 jobs were scheduled."
    job_names = {row["job_name"] for row in result}
    assert job_names == {"hello_world_job2", "hello_world_job"}
    assert all(row["execution_error_occurred"] == 0 for row in result)
    assert all(row["execution_error_message"] is None for row in result)
    assert all({row["ts"] is not None for row in result})

    sql = """
        SELECT batch_id, job_id, log_level, message, ts
        FROM job_log
        ORDER BY ts DESC
    """
    with in_memory_db.begin() as con:
        result = con.execute(sa.text(sql)).fetchall()
    job_log_messages = {row["message"] for row in result}
    assert job_log_messages == {
        "Finished running [hello_world_job2].",
        "Finished running [hello_world_job].",
        "Running the tests for [hello_world_job2]...",
        "Running the tests for [hello_world_job]...",
        "Starting [hello_world_job2]...",
        "Starting [hello_world_job]...",
        "[hello_world_job2] finished successfully.",
        "[hello_world_job] finished successfully.",
        "hello_world_job test results: tests_passed=1, tests_failed=0",
        "hello_world_job2 test results: tests_passed=1, tests_failed=0",
    }


def test_run_with_unresolved_dependencies(
    in_memory_db: sa.engine.Engine,
    messages_session_factory: orm.sessionmaker,
) -> None:
    jobs = [
        MessageJob(
            job_name="hello_world_job",
            dependencies=[],
            messages=[
                Message(id=1, message="Hello"),
                Message(id=2, message="World"),
            ],
        ),
        MessageJob(
            job_name="hello_world_job2",
            dependencies=["hello_world_job3"],
            messages=[
                Message(id=3, message="Have"),
                Message(id=4, message="Fun"),
            ],
        ),
    ]
    shared_resources = le.SharedResources(
        le.SqlAlchemySession(messages_session_factory)
    )
    with pytest.raises(le.DependencyErrors) as e:
        le.run(
            batch_name="test_batch",
            engine_or_uri=in_memory_db,
            jobs=jobs,
            batch_uow=MessageUOW(shared_resources),
        )
    assert (
        "[hello_world_job2] has the following unresolved dependencies: [hello_world_job3]"
        in str(e.value)
    )

    sql = """
        SELECT execution_error_occurred, execution_error_message, ts
        FROM batches
        ORDER BY ts DESC
    """
    with in_memory_db.begin() as con:
        result = con.execute(sa.text(sql)).fetchall()
    assert (
        len(result) == 1
    ), f"{len(result)} batches were added.  There should only be 1 batch entry."
    row = result[0]
    assert row["execution_error_occurred"] == 1
    assert (
        row["execution_error_message"]
        == "[hello_world_job2] has the following unresolved dependencies: [hello_world_job3]."
    )
    assert row["ts"] is not None

    sql = """
        SELECT job_name, execution_millis, execution_error_occurred, execution_error_message, ts
        FROM jobs
        ORDER BY ts
    """
    with in_memory_db.begin() as con:
        result = con.execute(sa.text(sql)).fetchall()
    assert len(result) == 0, (
        f"The job spec is invalid, so the batch should have failed at initialization and no "
        f"jobs should have been run, but {len(result)} job entries were added to the jobs table."
    )
    sql = """
        SELECT batch_id, job_id, log_level, message, ts
        FROM job_log
        ORDER BY ts DESC
    """
    with in_memory_db.begin() as con:
        result = con.execute(sa.text(sql)).fetchall()
    job_log_messages = {row["message"] for row in result}
    assert job_log_messages == set()


def test_run_with_dependencies_out_of_order(
    in_memory_db: sa.engine.Engine,
    messages_session_factory: orm.sessionmaker,
) -> None:
    jobs = [
        MessageJob(
            job_name="hello_world_job2",
            dependencies=["hello_world_job"],
            messages=[
                Message(id=1, message="Hello"),
                Message(id=2, message="World"),
            ],
        ),
        MessageJob(
            job_name="hello_world_job",
            dependencies=[],
            messages=[
                Message(id=3, message="Have"),
                Message(id=4, message="Fun"),
            ],
        ),
    ]
    shared_resources = le.SharedResources(
        le.SqlAlchemySession(messages_session_factory)
    )
    with pytest.raises(le.DependencyErrors) as e:
        le.run(
            batch_name="test_batch",
            engine_or_uri=in_memory_db,
            jobs=jobs,
            batch_uow=MessageUOW(shared_resources),
        )
    assert (
        "[hello_world_job2] depends on the following jobs which come after it: [hello_world_job]."
        in str(e.value)
    )

    sql = """
        SELECT job_name, execution_millis, execution_error_occurred, execution_error_message, ts
        FROM jobs
        ORDER BY ts
    """
    with in_memory_db.begin() as con:
        result = con.execute(sa.text(sql)).fetchall()
    assert len(result) == 0, (
        f"The job spec is invalid, so the batch should have failed at initialization and no "
        f"jobs should have been run, but {len(result)} job entries were added to the jobs table."
    )
    sql = """
        SELECT batch_id, job_id, log_level, message, ts
        FROM job_log
        ORDER BY ts DESC
    """
    with in_memory_db.begin() as con:
        result = con.execute(sa.text(sql)).fetchall()
    job_log_messages = {row["message"] for row in result}
    assert job_log_messages == set()
