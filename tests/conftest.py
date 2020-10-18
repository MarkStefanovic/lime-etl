import datetime
import typing

import pytest
from lime_uow import resources
from sqlalchemy import create_engine, orm
from sqlalchemy.engine import Engine
from sqlalchemy.orm import clear_mappers, Session, sessionmaker

from lime_etl.adapters import job_repository
from lime_etl.adapters import (
    batch_log_repository,
    batch_repository,
    job_log_repository,
    timestamp_adapter,
)
from lime_etl.adapters.orm import metadata, start_mappers
from lime_etl.domain import (
    batch,
    batch_log_entry,
    job_log_entry,
    job_result,
    value_objects,
)
from lime_etl.services import admin_unit_of_work


@pytest.fixture
def in_memory_db() -> Engine:
    engine = create_engine("sqlite:///:memory:", echo=True)
    # engine = create_engine("sqlite:///:memory:")
    metadata.create_all(engine)
    return engine


@pytest.fixture
def session_factory(in_memory_db: Engine) -> typing.Generator[sessionmaker, None, None]:
    start_mappers()
    yield sessionmaker(bind=in_memory_db)
    clear_mappers()


@pytest.fixture
def session(session_factory: sessionmaker) -> Session:
    return session_factory()


def static_timestamp_adapter(
    dt: datetime.datetime,
) -> timestamp_adapter.TimestampAdapter:
    return StaticTimestampAdapter(dt)


class StaticTimestampAdapter(timestamp_adapter.TimestampAdapter):
    def __init__(self, dt: datetime.datetime):
        self.dt = dt

    def rollback(self) -> None:
        pass

    def save(self) -> None:
        pass

    def now(self) -> value_objects.Timestamp:
        return value_objects.Timestamp(self.dt)


class DummyBatchLogRepository(
    batch_log_repository.BatchLogRepository,
    resources.DummyRepository[batch_log_entry.BatchLogEntryDTO],
):
    def __init__(
        self,
        initial_values: typing.Optional[
            typing.List[batch_log_entry.BatchLogEntryDTO]
        ] = None,
    ) -> None:
        super().__init__(initial_values=initial_values, key_fn=lambda b: b.id)

    def delete_old_entries(self, days_to_keep: value_objects.DaysToKeep) -> int:
        cutoff = datetime.datetime(2020, 1, 1) - datetime.timedelta(
            days=days_to_keep.value
        )
        self._current_state = [e for e in self.all() if e.ts > cutoff]
        return len(self._current_state)

    def get_earliest_timestamp(self) -> typing.Optional[datetime.datetime]:
        return sorted(self._current_state, key=lambda b: b.ts)[0].ts


class DummyJobRepository(
    job_repository.JobRepository,
    resources.DummyRepository[job_result.JobResultDTO],
):
    def __init__(
        self,
        initial_values: typing.Optional[typing.List[job_result.JobResultDTO]] = None,
        /,
    ) -> None:
        super().__init__(initial_values=initial_values, key_fn=lambda o: o.id)

    def get_latest(
        self, job_name: value_objects.JobName, /
    ) -> typing.Optional[job_result.JobResultDTO]:
        return sorted(self._current_state, key=lambda e: e.ts)[-1]

    def get_last_successful_ts(
        self, job_name: value_objects.JobName, /
    ) -> typing.Optional[value_objects.Timestamp]:
        last_successful_run = next(
            o
            for o in sorted(self._current_state, key=lambda e: e.ts, reverse=True)
            if all(r.test_passed for r in o.test_results)
        )
        return value_objects.Timestamp(last_successful_run.ts)


class DummyJobLogRepository(
    job_log_repository.JobLogRepository,
    resources.DummyRepository[job_log_entry.JobLogEntryDTO],
):
    def __init__(
        self,
        initial_values: typing.Optional[
            typing.List[job_log_entry.JobLogEntryDTO]
        ] = None,
        /,
    ) -> None:
        super().__init__(initial_values=initial_values, key_fn=lambda o: o.id)

    def delete_old_entries(self, days_to_keep: value_objects.DaysToKeep) -> int:
        cutoff = datetime.datetime(2020, 1, 1) - datetime.timedelta(
            days=days_to_keep.value
        )
        self._current_state = [e for e in self.all() if e.ts > cutoff]
        return len(self._current_state)


class DummyBatchRepository(
    batch_repository.BatchRepository,
    resources.DummyRepository[batch.BatchDTO],
):
    def __init__(
        self,
        initial_values: typing.Optional[typing.List[batch.BatchDTO]] = None,
    ) -> None:
        super().__init__(initial_values=initial_values, key_fn=lambda o: o.id)

    def delete_old_entries(self, days_to_keep: value_objects.DaysToKeep) -> int:
        cutoff = datetime.datetime(2020, 1, 1) - datetime.timedelta(
            days=days_to_keep.value
        )
        self._current_state = [e for e in self.all() if e.ts > cutoff]
        return len(self._current_state)

    def get_latest(self) -> typing.Optional[batch.BatchDTO]:
        return sorted(self._current_state, key=lambda e: e.ts)[-1]


class DummyAdminUnitOfWork(admin_unit_of_work.AdminUnitOfWork):
    def __init__(
        self,
        session_factory: orm.sessionmaker,
        /,
    ):
        super().__init__(session_factory)

    @property
    def batch_repo(self) -> batch_repository.BatchRepository:
        return self.get_resource(batch_repository.BatchRepository)  # type: ignore

    @property
    def batch_log_repo(self) -> batch_log_repository.BatchLogRepository:
        return self.get_resource(batch_log_repository.BatchLogRepository)  # type: ignore

    @property
    def job_repo(self) -> job_repository.JobRepository:
        return self.get_resource(job_repository.JobRepository)  # type: ignore

    @property
    def job_log_repo(self) -> job_log_repository.JobLogRepository:
        return self.get_resource(job_log_repository.JobLogRepository)  # type: ignore

    @property
    def ts_adapter(self) -> timestamp_adapter.TimestampAdapter:
        return self.get_resource(timestamp_adapter.TimestampAdapter)  # type: ignore

    def create_resources(self) -> typing.AbstractSet[resources.Resource[typing.Any]]:
        return {
            DummyBatchRepository(),
            DummyBatchLogRepository(),
            DummyJobRepository(),
            DummyJobLogRepository(),
            static_timestamp_adapter(datetime.datetime(2020, 1, 1)),
        }


@pytest.fixture
def dummy_batch_repository() -> DummyBatchRepository:
    return DummyBatchRepository()


@pytest.fixture
def dummy_batch_log_entry_repository() -> DummyBatchLogRepository:
    return DummyBatchLogRepository()


@pytest.fixture
def dummy_job_log_entry_repository() -> DummyJobLogRepository:
    return DummyJobLogRepository()


@pytest.fixture
def dummy_admin_uow(session_factory: orm.sessionmaker) -> DummyAdminUnitOfWork:
    return DummyAdminUnitOfWork(session_factory)


@pytest.fixture(scope="session")
def postgres_db() -> Engine:
    user = "tester"
    db_name = "testdb"
    pwd = "abc123"
    # host = "0.0.0.0"
    host = "postgres"
    port = 5432
    uri = f"postgresql://{user}:{pwd}@{host}:{port}/{db_name}"
    print(f"{uri=}")
    engine = create_engine(uri)
    metadata.create_all(engine)
    return engine


@pytest.fixture
def postgres_session(postgres_db: Engine) -> typing.Generator[sessionmaker, None, None]:
    start_mappers()
    yield sessionmaker(bind=postgres_db)()
    clear_mappers()
