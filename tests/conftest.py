import datetime
from typing import Generator, List, Optional

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, clear_mappers, sessionmaker

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
from lime_etl.services import unit_of_work  # type: ignore


@pytest.fixture
def in_memory_db() -> Engine:
    engine = create_engine("sqlite:///:memory:", echo=True)
    # engine = create_engine("sqlite:///:memory:")
    metadata.create_all(engine)
    return engine


@pytest.fixture
def session_factory(in_memory_db: Engine) -> Generator[sessionmaker, None, None]:
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

    def now(self) -> value_objects.Timestamp:
        return value_objects.Timestamp(self.dt)


class DummyBatchLogRepository(batch_log_repository.BatchLogRepository):
    def __init__(self) -> None:
        self.batch_log: List[batch_log_entry.BatchLogEntry] = []
        super().__init__()

    def add(
        self, log_entry: batch_log_entry.BatchLogEntry
    ) -> batch_log_entry.BatchLogEntry:
        self.batch_log.append(log_entry)
        return log_entry

    def delete_old_entries(self, days_to_keep: value_objects.DaysToKeep) -> int:
        cutoff = datetime.datetime(2020, 1, 1) - datetime.timedelta(
            days=days_to_keep.value
        )
        self.batch_log = [e for e in self.batch_log if e.ts.value > cutoff]
        return len(self.batch_log)

    def get_earliest(self) -> batch_log_entry.BatchLogEntry:
        return self.batch_log[0]

    def get_latest(self) -> batch_log_entry.BatchLogEntry:
        return self.batch_log[-1]


class DummyJobLogRepository(job_log_repository.JobLogRepository):
    def __init__(self) -> None:
        self.job_log: List[job_log_entry.JobLogEntry] = []
        super().__init__()

    def add(self, log_entry: job_log_entry.JobLogEntry) -> job_log_entry.JobLogEntry:
        self.job_log.append(log_entry)
        return log_entry

    def delete_old_entries(self, days_to_keep: value_objects.DaysToKeep) -> int:
        cutoff = datetime.datetime(2020, 1, 1) - datetime.timedelta(
            days=days_to_keep.value
        )
        self.job_log = [e for e in self.job_log if e.ts.value > cutoff]
        return len(self.job_log)


class DummyBatchRepository(batch_repository.BatchRepository):
    def __init__(self) -> None:
        self.entries: List[batch.Batch] = []
        super().__init__()

    def add(self, batch: batch.Batch) -> batch.Batch:
        self.entries.append(batch)
        return batch

    def add_job_result(self, result: job_result.JobResult) -> job_result.JobResult:
        return result

    def delete_old_entries(self, days_to_keep: value_objects.DaysToKeep) -> int:
        cutoff = datetime.datetime(2020, 1, 1) - datetime.timedelta(
            days=days_to_keep.value
        )
        self.entries = [e for e in self.entries if e.ts.value > cutoff]
        return len(self.entries)

    def get_batch_by_id(
        self, batch_id: value_objects.UniqueId
    ) -> Optional[batch.Batch]:
        return None

    def get_last_successful_ts_for_job(
        self, job_name: value_objects.JobName
    ) -> Optional[value_objects.Timestamp]:
        return self.get_latest().ts  # type: ignore

    def get_latest(self) -> Optional[batch.Batch]:
        latest: batch.Batch = sorted(
            self.entries, key=lambda e: e.ts.value, reverse=True
        )[0]
        return latest

    def get_latest_result_for_job(
        self, job_name: value_objects.JobName
    ) -> List[job_result.JobResult]:
        return []

    def update(self, batch_to_update: batch.Batch) -> batch.Batch:
        return batch_to_update


class DummyUnitOfWork(unit_of_work.UnitOfWork):
    def __init__(
        self,
        batches: batch_repository.BatchRepository,
        batch_log_entry_repo: batch_log_repository.BatchLogRepository,
        job_log_entry_repo: job_log_repository.JobLogRepository,
    ):
        self.batch_id = value_objects.UniqueId("a" * 32)
        self.batches = batches
        self.batch_log = batch_log_entry_repo
        self.job_log = job_log_entry_repo
        self.ts_adapter = static_timestamp_adapter(datetime.datetime(2020, 1, 1))
        self.committed = False

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        pass


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
def dummy_uow(
    dummy_batch_repository: batch_repository.BatchRepository,
    dummy_batch_log_entry_repository: batch_log_repository.BatchLogRepository,
    dummy_job_log_entry_repository: job_log_repository.JobLogRepository,
) -> DummyUnitOfWork:
    return DummyUnitOfWork(
        batches=dummy_batch_repository,
        batch_log_entry_repo=dummy_batch_log_entry_repository,
        job_log_entry_repo=dummy_job_log_entry_repository,
    )


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
def postgres_session(postgres_db: Engine) -> sessionmaker:
    start_mappers()
    yield sessionmaker(bind=postgres_db)()
    clear_mappers()
