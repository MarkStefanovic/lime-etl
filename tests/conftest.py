import datetime
import typing

import lime_uow as lu
import pytest
import sqlalchemy as sa
from sqlalchemy import orm

import lime_etl as le


@pytest.fixture
def in_memory_db_uri() -> str:
    return "sqlite:///:memory:"


@pytest.fixture
def in_memory_db(in_memory_db_uri: str) -> sa.engine.Engine:
    engine = sa.create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=sa.pool.StaticPool,
        echo=True,
    )
    # engine = sa.create_engine(in_memory_db_uri, echo=True)
    # engine = create_engine("sqlite:///:memory:")
    le.admin_metadata.create_all(engine)
    return engine


@pytest.fixture
def session_factory(
    in_memory_db: sa.engine.Engine,
) -> typing.Generator[orm.sessionmaker, None, None]:
    le.start_mappers()
    yield orm.sessionmaker(bind=in_memory_db)
    orm.clear_mappers()


@pytest.fixture
def session(session_factory: orm.sessionmaker) -> orm.Session:
    return session_factory()


def static_timestamp_adapter(
    dt: datetime.datetime, /
) -> le.TimestampAdapter:
    return StaticTimestampAdapter(dt)


class StaticTimestampAdapter(le.TimestampAdapter):
    def __init__(self, dt: datetime.datetime):
        self.dt = dt

    @classmethod
    def interface(cls) -> typing.Type[le.TimestampAdapter]:
        return le.TimestampAdapter

    def now(self) -> le.Timestamp:
        return le.Timestamp(self.dt)


class DummyBatchLogRepository(
    le.BatchLogRepository,
    lu.DummyRepository[le.BatchLogEntryDTO],
):
    def __init__(
        self,
        initial_values: typing.Optional[
            typing.List[le.BatchLogEntryDTO]
        ] = None,
    ) -> None:
        super().__init__(initial_values=initial_values, key_fn=lambda b: b.id)

    def delete_old_entries(self, days_to_keep: le.DaysToKeep) -> int:
        cutoff = datetime.datetime(2020, 1, 1) - datetime.timedelta(
            days=days_to_keep.value
        )
        self._current_state = [e for e in self.all() if e.ts > cutoff]
        return len(self._current_state)

    def get_earliest_timestamp(self) -> typing.Optional[datetime.datetime]:
        return sorted(self._current_state, key=lambda b: b.ts)[0].ts

    @classmethod
    def interface(cls) -> typing.Type[le.BatchLogRepository]:
        return le.BatchLogRepository


class DummyJobRepository(
    le.JobRepository,
    lu.DummyRepository[le.JobResultDTO],
):
    def __init__(
        self,
        initial_values: typing.Optional[typing.List[le.JobResultDTO]] = None,
        /,
    ) -> None:
        super().__init__(initial_values=initial_values, key_fn=lambda o: o.id)

    def get_latest(
        self, job_name: le.JobName, /
    ) -> typing.Optional[le.JobResultDTO]:
        return sorted(self._current_state, key=lambda e: e.ts)[-1]

    def get_last_successful_ts(
        self, job_name: le.JobName, /
    ) -> typing.Optional[le.Timestamp]:
        last_successful_run = next(
            o
            for o in sorted(self._current_state, key=lambda e: e.ts, reverse=True)
            if all(r.test_passed for r in o.test_results)
        )
        return le.Timestamp(last_successful_run.ts)

    @classmethod
    def interface(cls) -> typing.Type[le.JobRepository]:
        return le.JobRepository


class DummyJobLogRepository(
    le.JobLogRepository,
    lu.DummyRepository[le.JobLogEntryDTO],
):
    def __init__(
        self,
        initial_values: typing.Optional[
            typing.List[le.JobLogEntryDTO]
        ] = None,
        /,
    ) -> None:
        super().__init__(initial_values=initial_values, key_fn=lambda o: o.id)

    def delete_old_entries(self, days_to_keep: le.DaysToKeep) -> int:
        cutoff = datetime.datetime(2020, 1, 1) - datetime.timedelta(
            days=days_to_keep.value
        )
        self._current_state = [e for e in self.all() if e.ts > cutoff]
        return len(self._current_state)

    @classmethod
    def interface(cls) -> typing.Type[le.JobLogRepository]:
        return le.JobLogRepository


class DummyBatchRepository(
    le.BatchRepository,
    lu.DummyRepository[le.BatchStatusDTO],
):
    def __init__(
        self,
        initial_values: typing.Optional[
            typing.List[le.BatchStatusDTO]
        ] = None,
    ) -> None:
        super().__init__(initial_values=initial_values, key_fn=lambda o: o.id)

    def delete_old_entries(self, days_to_keep: le.DaysToKeep) -> int:
        cutoff = datetime.datetime(2020, 1, 1) - datetime.timedelta(
            days=days_to_keep.value
        )
        self._current_state = [e for e in self.all() if e.ts > cutoff]
        return len(self._current_state)

    def get_latest(self) -> typing.Optional[le.BatchStatusDTO]:
        return sorted(self._current_state, key=lambda e: e.ts)[-1]

    @classmethod
    def interface(cls) -> typing.Type[le.BatchRepository]:
        return le.BatchRepository


class DummyAdminUnitOfWork(le.AdminUnitOfWork):
    def __init__(self) -> None:
        super().__init__()

    @property
    def batch_repo(self) -> le.BatchRepository:
        return self.get(le.BatchRepository)  # type: ignore

    @property
    def batch_log_repo(self) -> le.BatchLogRepository:
        return self.get(le.BatchLogRepository)  # type: ignore

    def create_resources(
        self, shared_resources: lu.SharedResources
    ) -> typing.Iterable[lu.Resource[typing.Any]]:
        return {
            DummyBatchRepository(),
            DummyBatchLogRepository(),
            DummyJobRepository(),
            DummyJobLogRepository(),
            static_timestamp_adapter(datetime.datetime(2020, 1, 1)),
        }

    def create_shared_resources(self) -> typing.List[lu.Resource[typing.Any]]:
        return []

    @property
    def job_repo(self) -> le.JobRepository:
        return self.get(le.JobRepository)  # type: ignore

    @property
    def job_log_repo(self) -> le.JobLogRepository:
        return self.get(le.JobLogRepository)  # type: ignore

    @property
    def ts_adapter(self) -> le.TimestampAdapter:
        return self.get(le.TimestampAdapter)  # type: ignore


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
def dummy_admin_uow() -> DummyAdminUnitOfWork:
    return DummyAdminUnitOfWork()


@pytest.fixture
def postgres_db_uri() -> str:
    user = "tester"
    db_name = "testdb"
    pwd = "abc123"
    # host = "0.0.0.0"
    host = "postgres"
    port = 5432
    return f"postgresql://{user}:{pwd}@{host}:{port}/{db_name}"


@pytest.fixture(scope="function")
def postgres_db(postgres_db_uri: str) -> typing.Generator[sa.engine.Engine, None, None]:
    engine = sa.create_engine(
        postgres_db_uri, isolation_level="SERIALIZABLE", echo=True
    )
    le.admin_metadata.drop_all(engine)
    le.admin_metadata.create_all(engine)
    yield engine
    le.admin_metadata.drop_all(engine)


@pytest.fixture(scope="function")
def postgres_session(
    postgres_db: sa.engine.Engine,
) -> typing.Generator[orm.sessionmaker, None, None]:
    le.start_mappers()
    yield orm.sessionmaker(bind=postgres_db)()
    orm.clear_mappers()
