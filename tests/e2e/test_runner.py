from __future__ import annotations

import abc
import dataclasses
import typing

import pytest
import sqlalchemy as sa
from sqlalchemy import orm

import lime_etl as le
from lime_uow import sqlalchemy_resources as lsa
from tests import test_utils

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


class AbstractMessageRepo(le.Repository[Message], abc.ABC):
    @property
    def entity_type(self) -> typing.Type[Message]:
        return Message


class MessageRepo(AbstractMessageRepo, lsa.SqlAlchemyRepository[Message]):
    def __init__(self, session: orm.Session, /):
        super().__init__(session)

    @classmethod
    def interface(cls) -> typing.Type[AbstractMessageRepo]:
        return AbstractMessageRepo


@pytest.fixture(scope="function")
def messages_session_factory(
    postgres_db: sa.engine.Engine,
) -> typing.Generator[orm.sessionmaker, None, None]:
    meta.drop_all(bind=postgres_db)
    if not orm.base._is_mapped_class(Message):  # type: ignore
        orm.mapper(Message, messages_table)
    meta.create_all(bind=postgres_db)
    yield orm.sessionmaker(bind=postgres_db)
    meta.drop_all(bind=postgres_db)


class MessageUOW(le.UnitOfWork):
    def __init__(self, /, session_factory: orm.sessionmaker):
        super().__init__()
        self._session_factory = session_factory

    def create_shared_resources(self) -> typing.List[le.Resource[typing.Any]]:
        return [lsa.SqlAlchemySession(self._session_factory)]

    def create_resources(
        self, shared_resources: le.SharedResources
    ) -> typing.Set[le.Resource[typing.Any]]:
        return {MessageRepo(shared_resources.get(lsa.SqlAlchemySession))}

    @property
    def message_repo(self) -> AbstractMessageRepo:
        return self.get(AbstractMessageRepo)  # type: ignore


class MessageJob(le.JobSpec[MessageUOW]):
    def __init__(
        self,
        messages: typing.Iterable[Message],
        dependencies: typing.Iterable[str],
        job_name: str,
    ):
        self._messages = list(messages)
        self._dependencies = tuple(le.JobName(n) for n in dependencies)
        self._job_name = le.JobName(job_name)

    def run(
        self,
        uow: MessageUOW,
        logger: le.JobLoggingService,
    ) -> le.JobStatus:
        with uow:
            uow.message_repo.add_all(self._messages)
            uow.save()
        return le.JobStatus.success()

    @property
    def dependencies(self) -> typing.Tuple[le.JobName, ...]:
        return self._dependencies

    @property
    def job_name(self) -> le.JobName:
        return self._job_name

    def on_execution_error(
        self, error_message: str
    ) -> typing.Optional[le.JobSpec[MessageUOW]]:
        return None

    def on_test_failure(
        self, test_results: typing.FrozenSet[le.JobTestResult]
    ) -> typing.Optional[le.JobSpec[MessageUOW]]:
        return None

    @property
    def min_seconds_between_refreshes(self) -> le.MinSecondsBetweenRefreshes:
        return le.MinSecondsBetweenRefreshes(300)

    @property
    def timeout_seconds(self) -> le.TimeoutSeconds:
        return le.TimeoutSeconds(60)

    @property
    def max_retries(self) -> le.MaxRetries:
        return le.MaxRetries(1)

    def test(
        self,
        uow: MessageUOW,
        logger: le.JobLoggingService,
    ) -> typing.List[le.SimpleJobTestResult]:
        with uow:
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


class MessageBatchHappyPath(le.BatchSpec[MessageUOW]):
    def __init__(self, session_factory: orm.sessionmaker):
        self._session_factory = session_factory

    @property
    def batch_name(self) -> le.BatchName:
        return le.BatchName("test_batch")

    def create_jobs(self, uow: MessageUOW) -> typing.List[le.JobSpec[MessageUOW]]:
        return [
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

    def create_uow(self) -> MessageUOW:
        return MessageUOW(self._session_factory)


class MessageBatchWithMissingDependencies(le.BatchSpec[MessageUOW]):
    def __init__(self, session_factory: orm.sessionmaker):
        self._session_factory = session_factory

    @property
    def batch_name(self) -> le.BatchName:
        return le.BatchName("test_batch")

    def create_jobs(self, uow: MessageUOW) -> typing.List[le.JobSpec[MessageUOW]]:
        return [
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

    def create_uow(self) -> MessageUOW:
        return MessageUOW(self._session_factory)


class MessageBatchWithDependenciesOutOfOrder(le.BatchSpec[MessageUOW]):
    def __init__(self, session_factory: orm.sessionmaker):
        self._session_factory = session_factory

    @property
    def batch_name(self) -> le.BatchName:
        return le.BatchName("test_batch")

    def create_jobs(self, uow: MessageUOW) -> typing.List[le.JobSpec[MessageUOW]]:
        return [
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

    def create_shared_resource(self) -> le.SharedResources:
        return le.SharedResources(lsa.SqlAlchemySession(self._session_factory))

    def create_uow(self) -> MessageUOW:
        return MessageUOW(self._session_factory)


class MessageBatchWithDuplicateJobNames(le.BatchSpec[MessageUOW]):
    def __init__(self, session_factory: orm.sessionmaker):
        self._session_factory = session_factory

    @property
    def batch_name(self) -> le.BatchName:
        return le.BatchName("test_batch")

    def create_jobs(self, uow: MessageUOW) -> typing.List[le.JobSpec[MessageUOW]]:
        return [
            MessageJob(
                job_name="hello_world_job",
                dependencies=[],
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

    def create_uow(self) -> MessageUOW:
        return MessageUOW(self._session_factory)


class PickleableMessageBatch(le.BatchSpec[MessageUOW]):
    def __init__(
        self,
        db_uri: le.DbUri,
    ):
        self._db_uri = db_uri

        super().__init__()

    @property
    def batch_name(self) -> le.BatchName:
        return le.BatchName("test_batch")

    def create_jobs(self, uow: MessageUOW) -> typing.List[le.JobSpec[MessageUOW]]:
        return [
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

    def create_uow(self) -> MessageUOW:
        engine = sa.create_engine(self._db_uri.value)
        meta.create_all(bind=engine)
        if not orm.base._is_mapped_class(Message):  # type: ignore
            orm.mapper(Message, messages_table)
        session_factory = orm.sessionmaker(bind=engine)
        return MessageUOW(session_factory)


@pytest.mark.docker
def test_run_admin(postgres_db: sa.engine.Engine, postgres_db_uri: str) -> None:
    # Even though we're not using postgres_db in this test, we need to import it so the database is cleaned up afterwards.
    admin_schema = le.SchemaName(None)
    batch = le.AdminBatch(
        admin_engine_uri=le.DbUri(postgres_db_uri),
        days_logs_to_keep=le.DaysToKeep(3),
        admin_schema=admin_schema,
    )
    actual = le.run_batch(
        batch=batch, admin_engine_uri=postgres_db_uri, admin_schema=admin_schema.value
    )
    assert actual.running == le.Flag(False)
    assert actual.broken_jobs == frozenset()
    assert len(actual.job_results) == 1


@pytest.mark.docker
def test_run_with_default_parameters_happy_path(
    postgres_db: sa.engine.Engine,
    postgres_db_uri: str,
    messages_session_factory: orm.sessionmaker,
) -> None:
    batch = MessageBatchHappyPath(session_factory=messages_session_factory)
    actual = le.run_batch(
        batch=batch, admin_engine_uri=postgres_db_uri, admin_schema=None
    )
    expected = {
        "execution_error_message": None,
        "execution_error_occurred": False,
        "execution_millis": "positive",
        "id": "32 chars",
        "job_results": [
            {
                "batch_id": "32 chars",
                "execution_error_message": None,
                "execution_error_occurred": False,
                "execution_millis": "positive",
                "id": "32 chars",
                "job_name": "hello_world_job",
                "running": False,
                "test_results": [
                    {
                        "execution_error_message": None,
                        "execution_error_occurred": False,
                        "execution_millis": "positive",
                        "id": "32 chars",
                        "job_id": "32 chars",
                        "test_failure_message": None,
                        "test_name": "Messages saved",
                        "test_passed": True,
                        "ts": "present",
                    }
                ],
                "ts": "present",
            },
            {
                "batch_id": "32 chars",
                "execution_error_message": None,
                "execution_error_occurred": False,
                "execution_millis": "positive",
                "id": "32 chars",
                "job_name": "hello_world_job2",
                "running": False,
                "test_results": [
                    {
                        "execution_error_message": None,
                        "execution_error_occurred": False,
                        "execution_millis": "positive",
                        "id": "32 chars",
                        "job_id": "32 chars",
                        "test_failure_message": None,
                        "test_name": "Messages saved",
                        "test_passed": True,
                        "ts": "present",
                    }
                ],
                "ts": "present",
            },
        ],
        "name": "test_batch",
        "running": False,
        "ts": "present",
    }
    assert test_utils.batch_result_to_deterministic_dict(actual) == expected

    sql = """
        SELECT execution_error_occurred, execution_error_message, ts
        FROM batches
        ORDER BY ts DESC
    """
    with postgres_db.begin() as con:
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
    with postgres_db.begin() as con:
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
    with postgres_db.begin() as con:
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


@pytest.mark.docker
def test_run_with_unresolved_dependencies(
    postgres_db: sa.engine.Engine,
    postgres_db_uri: str,
    messages_session_factory: orm.sessionmaker,
) -> None:
    batch = MessageBatchWithMissingDependencies(messages_session_factory)
    with pytest.raises(le.exceptions.DependencyErrors) as e:
        le.run_batch(batch=batch, admin_engine_uri=postgres_db_uri, admin_schema=None)
    assert (
        "[hello_world_job2] has the following unresolved dependencies: [hello_world_job3]"
        in str(e.value)
    )

    sql = """
        SELECT execution_error_occurred, execution_error_message, ts
        FROM batches
        ORDER BY ts DESC
    """
    with postgres_db.begin() as con:
        result = con.execute(sa.text(sql)).fetchall()
    assert (
        len(result) == 1
    ), f"{len(result)} batches were added.  There should be 1 batch entry."
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
    with postgres_db.begin() as con:
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
    with postgres_db.begin() as con:
        result = con.execute(sa.text(sql)).fetchall()
    job_log_messages = {row["message"] for row in result}
    assert job_log_messages == set()


@pytest.mark.docker
def test_run_with_dependencies_out_of_order(
    postgres_db: sa.engine.Engine,
    postgres_db_uri: str,
    messages_session_factory: orm.sessionmaker,
) -> None:
    batch = MessageBatchWithDependenciesOutOfOrder(messages_session_factory)
    with pytest.raises(le.exceptions.DependencyErrors) as e:
        le.run_batch(batch=batch, admin_engine_uri=postgres_db_uri, admin_schema=None)
    assert (
        "[hello_world_job2] depends on the following jobs which come after it: [hello_world_job]."
        in str(e.value)
    )

    sql = """
        SELECT job_name, execution_millis, execution_error_occurred, execution_error_message, ts
        FROM jobs
        ORDER BY ts
    """
    with postgres_db.begin() as con:
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
    with postgres_db.begin() as con:
        result = con.execute(sa.text(sql)).fetchall()
    job_log_messages = {row["message"] for row in result}
    assert job_log_messages == set()


@pytest.mark.docker
def test_run_with_duplicate_job_names(
    postgres_db: sa.engine.Engine,
    postgres_db_uri: str,
    messages_session_factory: orm.sessionmaker,
) -> None:
    batch = MessageBatchWithDuplicateJobNames(messages_session_factory)
    with pytest.raises(le.exceptions.DuplicateJobNamesError) as e:
        le.run_batch(batch=batch, admin_engine_uri=postgres_db_uri, admin_schema=None)
    assert (
        "The following job names were included more than once: [hello_world_job] (2)."
        in str(e.value)
    )

    sql = """
        SELECT job_name, execution_millis, execution_error_occurred, execution_error_message, ts
        FROM jobs
        ORDER BY ts
    """
    with postgres_db.begin() as con:
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
    with postgres_db.begin() as con:
        result = con.execute(sa.text(sql)).fetchall()
    job_log_messages = {row["message"] for row in result}
    assert job_log_messages == set()


@pytest.mark.docker
def test_run_batches_in_parallel(
    postgres_db: sa.engine.Engine,
    postgres_db_uri: str,
) -> None:
    admin_batch = le.AdminBatch(
        admin_engine_uri=le.DbUri(postgres_db_uri), admin_schema=le.SchemaName(None)
    )
    message_batch = PickleableMessageBatch(
        db_uri=le.DbUri(postgres_db_uri),
    )
    results = le.run_batches_in_parallel(
        admin_engine_uri=postgres_db_uri,
        admin_schema=None,
        batches=[admin_batch, message_batch],  # type: ignore
        max_processes=3,
        timeout=10,
    )
    expected = [
        {
            "execution_error_message": None,
            "execution_error_occurred": False,
            "execution_millis": "positive",
            "id": "32 chars",
            "job_results": [
                {
                    "batch_id": "32 chars",
                    "execution_error_message": None,
                    "execution_error_occurred": False,
                    "execution_millis": "positive",
                    "id": "32 chars",
                    "job_name": "delete_old_logs",
                    "running": False,
                    "test_results": [
                        {
                            "execution_error_message": None,
                            "execution_error_occurred": False,
                            "execution_millis": "positive",
                            "id": "32 chars",
                            "job_id": "32 chars",
                            "test_failure_message": None,
                            "test_name": "No log entries more than 3 days old",
                            "test_passed": True,
                            "ts": "present",
                        },
                    ],
                    "ts": "present",
                },
            ],
            "name": "admin",
            "running": False,
            "ts": "present",
        },
        {
            "execution_error_message": None,
            "execution_error_occurred": False,
            "execution_millis": "positive",
            "id": "32 chars",
            "job_results": [
                {
                    "batch_id": "32 chars",
                    "execution_error_message": None,
                    "execution_error_occurred": False,
                    "execution_millis": "positive",
                    "id": "32 chars",
                    "job_name": "hello_world_job",
                    "running": False,
                    "test_results": [
                        {
                            "execution_error_message": None,
                            "execution_error_occurred": False,
                            "execution_millis": "positive",
                            "id": "32 chars",
                            "job_id": "32 chars",
                            "test_failure_message": None,
                            "test_name": "Messages saved",
                            "test_passed": True,
                            "ts": "present",
                        },
                    ],
                    "ts": "present",
                },
                {
                    "batch_id": "32 chars",
                    "execution_error_message": None,
                    "execution_error_occurred": False,
                    "execution_millis": "positive",
                    "id": "32 chars",
                    "job_name": "hello_world_job2",
                    "running": False,
                    "test_results": [
                        {
                            "execution_error_message": None,
                            "execution_error_occurred": False,
                            "execution_millis": "positive",
                            "id": "32 chars",
                            "job_id": "32 chars",
                            "test_failure_message": None,
                            "test_name": "Messages saved",
                            "test_passed": True,
                            "ts": "present",
                        },
                    ],
                    "ts": "present",
                },
            ],
            "name": "test_batch",
            "running": False,
            "ts": "present",
        },
    ]
    actual = sorted(
        (test_utils.batch_result_to_deterministic_dict(r) for r in results),
        key=lambda d: d["name"],
    )
    assert actual == expected

    sql = """
        SELECT execution_error_occurred, execution_error_message, ts
        FROM batches
        ORDER BY ts DESC
    """
    with postgres_db.begin() as con:
        result = con.execute(sa.text(sql)).fetchall()
    assert (
        len(result) == 2
    ), f"{len(result)} batches were added.  There should have been 2 added."
    assert all(row["execution_error_occurred"] == 0 for row in result)
    assert all(row["execution_error_message"] is None for row in result)
    assert all(row["ts"] is not None for row in result)
