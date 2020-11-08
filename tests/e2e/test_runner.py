from __future__ import annotations

import abc
import dataclasses
import typing

import lime_uow as lu
import pytest
import sqlalchemy as sa
from sqlalchemy import orm

import lime_etl as le
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


class AbstractMessageRepo(lu.Repository[Message], abc.ABC):
    @property
    def entity_type(self) -> typing.Type[Message]:
        return Message


class MessageRepo(AbstractMessageRepo, lu.SqlAlchemyRepository[Message]):
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


class MessageUOW(lu.UnitOfWork):
    def __init__(self, shared_resources: lu.SharedResources):
        super().__init__(shared_resources)

    def create_resources(
        self, shared_resources: lu.SharedResources
    ) -> typing.Set[lu.Resource[typing.Any]]:
        return {MessageRepo(shared_resources.get(lu.SqlAlchemySession))}

    @property
    def message_repo(self) -> MessageRepo:
        return self.get_resource(MessageRepo)


class MessageJob(le.JobSpec):
    def __init__(
        self,
        uow: MessageUOW,
        messages: typing.Iterable[Message],
        dependencies: typing.Iterable[str],
        job_name: str,
        job_id: str,
    ):
        self._uow = uow
        self._messages = list(messages)
        self._dependencies = tuple(le.JobName(n) for n in dependencies)
        self._job_name = le.JobName(job_name)
        super().__init__(
            job_name=le.JobName(job_name),
            dependencies=[le.JobName(d) for d in dependencies],
            job_id=le.UniqueId(job_id),
            max_retries=le.MaxRetries(0),
        )

    def run(
        self,
        uow: lu.UnitOfWork,
        logger: le.AbstractJobLoggingService,
    ) -> le.Result:
        with self._uow as uow:
            uow.message_repo.add_all(self._messages)
            uow.save()
        return le.Result.success()

    @property
    def dependencies(self) -> typing.Tuple[le.JobName, ...]:
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
        uow: lu.UnitOfWork,
        logger: le.AbstractJobLoggingService,
    ) -> typing.List[le.SimpleJobTestResult]:
        with self._uow as uow:
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
    def __init__(
        self,
        batch_name: le.BatchName,
        batch_id: le.UniqueId,
        session_factory: orm.sessionmaker,
    ):
        self._session_factory = session_factory

        super().__init__(
            batch_name=batch_name,
            batch_id=batch_id,
        )

    def create_job_specs(self, uow: MessageUOW) -> typing.List[le.JobSpec]:
        return [
            MessageJob(
                uow=uow,
                job_id="b" * 32,
                job_name="hello_world_job",
                dependencies=[],
                messages=[
                    Message(id=1, message="Hello"),
                    Message(id=2, message="World"),
                ],
            ),
            MessageJob(
                uow=uow,
                job_id="c" * 32,
                job_name="hello_world_job2",
                dependencies=["hello_world_job"],
                messages=[
                    Message(id=3, message="Have"),
                    Message(id=4, message="Fun"),
                ],
            ),
        ]

    def create_shared_resource(self) -> lu.SharedResources:
        return lu.SharedResources(lu.SqlAlchemySession(self._session_factory))

    def create_uow(self, shared_resources: lu.SharedResources) -> MessageUOW:
        return MessageUOW(shared_resources)


class MessageBatchWithMissingDependencies(le.BatchSpec[MessageUOW]):
    def __init__(
        self,
        batch_name: le.BatchName,
        batch_id: le.UniqueId,
        session_factory: orm.sessionmaker,
    ):
        self._session_factory = session_factory

        super().__init__(
            batch_name=batch_name,
            batch_id=batch_id,
        )

    def create_job_specs(self, uow: MessageUOW) -> typing.List[le.JobSpec]:
        return [
            MessageJob(
                uow=uow,
                job_id="b" * 32,
                job_name="hello_world_job",
                dependencies=[],
                messages=[
                    Message(id=1, message="Hello"),
                    Message(id=2, message="World"),
                ],
            ),
            MessageJob(
                uow=uow,
                job_id="c" * 32,
                job_name="hello_world_job2",
                dependencies=["hello_world_job3"],
                messages=[
                    Message(id=3, message="Have"),
                    Message(id=4, message="Fun"),
                ],
            ),
        ]

    def create_shared_resource(self) -> lu.SharedResources:
        return lu.SharedResources(lu.SqlAlchemySession(self._session_factory))

    def create_uow(self, shared_resources: lu.SharedResources) -> MessageUOW:
        return MessageUOW(shared_resources)


class MessageBatchWithDependenciesOutOfOrder(le.BatchSpec[MessageUOW]):
    def __init__(
        self,
        batch_name: le.BatchName,
        batch_id: le.UniqueId,
        session_factory: orm.sessionmaker,
    ):
        self._session_factory = session_factory

        super().__init__(
            batch_name=batch_name,
            batch_id=batch_id,
        )

    def create_job_specs(self, uow: MessageUOW) -> typing.List[le.JobSpec]:
        return [
            MessageJob(
                uow=uow,
                job_name="hello_world_job2",
                job_id="b" * 32,
                dependencies=["hello_world_job"],
                messages=[
                    Message(id=1, message="Hello"),
                    Message(id=2, message="World"),
                ],
            ),
            MessageJob(
                uow=uow,
                job_name="hello_world_job",
                job_id="c" * 32,
                dependencies=[],
                messages=[
                    Message(id=3, message="Have"),
                    Message(id=4, message="Fun"),
                ],
            ),
        ]

    def create_shared_resource(self) -> lu.SharedResources:
        return lu.SharedResources(lu.SqlAlchemySession(self._session_factory))

    def create_uow(self, shared_resources: lu.SharedResources) -> MessageUOW:
        return MessageUOW(shared_resources)


class MessageBatchWithDuplicateJobNames(le.BatchSpec[MessageUOW]):
    def __init__(
        self,
        batch_name: le.BatchName,
        batch_id: le.UniqueId,
        session_factory: orm.sessionmaker,
    ):
        self._session_factory = session_factory

        super().__init__(
            batch_name=batch_name,
            batch_id=batch_id,
        )

    def create_job_specs(self, uow: MessageUOW) -> typing.List[le.JobSpec]:
        return [
            MessageJob(
                uow=uow,
                job_name="hello_world_job",
                job_id="b" * 32,
                dependencies=[],
                messages=[
                    Message(id=1, message="Hello"),
                    Message(id=2, message="World"),
                ],
            ),
            MessageJob(
                uow=uow,
                job_name="hello_world_job",
                job_id="c" * 32,
                dependencies=[],
                messages=[
                    Message(id=3, message="Have"),
                    Message(id=4, message="Fun"),
                ],
            ),
        ]

    def create_shared_resource(self) -> lu.SharedResources:
        return lu.SharedResources(lu.SqlAlchemySession(self._session_factory))

    def create_uow(self, shared_resources: lu.SharedResources) -> MessageUOW:
        return MessageUOW(shared_resources)


class PickleableMessageBatch(le.BatchSpec[MessageUOW]):
    def __init__(
        self,
        batch_name: le.BatchName,
        batch_id: le.UniqueId,
        db_uri: le.DbUri,
    ):
        self._db_uri = db_uri

        super().__init__(
            batch_name=batch_name,
            batch_id=batch_id,
        )

    def create_job_specs(self, uow: MessageUOW) -> typing.List[le.JobSpec]:
        return [
            MessageJob(
                uow=uow,
                job_name="hello_world_job",
                job_id="b" * 32,
                dependencies=[],
                messages=[
                    Message(id=1, message="Hello"),
                    Message(id=2, message="World"),
                ],
            ),
            MessageJob(
                uow=uow,
                job_name="hello_world_job2",
                job_id="c" * 32,
                dependencies=["hello_world_job"],
                messages=[
                    Message(id=3, message="Have"),
                    Message(id=4, message="Fun"),
                ],
            ),
        ]

    def create_shared_resource(self) -> lu.SharedResources:
        engine = sa.create_engine(self._db_uri.value)
        meta.create_all(bind=engine)
        orm.mapper(Message, messages_table)
        session_factory = orm.sessionmaker(bind=engine)
        return lu.SharedResources(lu.SqlAlchemySession(session_factory))

    def create_uow(self, shared_resources: lu.SharedResources) -> MessageUOW:
        return MessageUOW(shared_resources)


def test_run_admin(in_memory_db: sa.engine.Engine) -> None:
    actual = le.run_admin(
        admin_engine_or_uri=in_memory_db,
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
    batch = MessageBatchHappyPath(
        batch_name=le.BatchName("test_batch"),
        batch_id=le.UniqueId("a" * 32),
        session_factory=messages_session_factory,
    )
    actual = le.run_batch(
        admin_engine_or_uri=in_memory_db,
        admin_schema=None,
        batch=batch,
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
    batch = MessageBatchWithMissingDependencies(
        batch_name=le.BatchName("test_batch"),
        batch_id=le.UniqueId("a" * 32),
        session_factory=messages_session_factory,
    )
    with pytest.raises(le.exceptions.DependencyErrors) as e:
        le.run_batch(
            admin_engine_or_uri=in_memory_db,
            admin_schema=None,
            batch=batch,
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
    batch = MessageBatchWithDependenciesOutOfOrder(
        batch_name=le.BatchName("test_batch"),
        batch_id=le.UniqueId("a" * 32),
        session_factory=messages_session_factory,
    )
    with pytest.raises(le.exceptions.DependencyErrors) as e:
        le.run_batch(
            admin_engine_or_uri=in_memory_db,
            admin_schema=None,
            batch=batch,
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


def test_run_with_duplicate_job_names(
    in_memory_db: sa.engine.Engine,
    messages_session_factory: orm.sessionmaker,
) -> None:
    batch = MessageBatchWithDuplicateJobNames(
        batch_name=le.BatchName("test_batch"),
        batch_id=le.UniqueId("a" * 32),
        session_factory=messages_session_factory,
    )
    with pytest.raises(le.exceptions.DuplicateJobNamesError) as e:
        le.run_batch(
            admin_engine_or_uri=in_memory_db,
            admin_schema=None,
            batch=batch,
        )
    assert (
        "The following job names included more than once: [hello_world_job] (2)."
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


@pytest.mark.slow
def test_run_batches_in_parallel(
    postgres_db: sa.engine.Engine,
    postgres_db_uri: str,
) -> None:
    admin_batch = le.AdminBatch(
        admin_db_uri=le.DbUri(postgres_db_uri),
        admin_schema=le.SchemaName(None),
        batch_id=le.UniqueId("e" * 32),
    )
    message_batch = PickleableMessageBatch(
        batch_name=le.BatchName("test_batch"),
        batch_id=le.UniqueId("f" * 32),
        db_uri=le.DbUri(postgres_db_uri),
    )
    results = le.run_batches_in_parallel(
        admin_db_uri=postgres_db_uri,
        batches=(admin_batch, message_batch),
        max_processes=3,
        schema=None,
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
