from __future__ import annotations

import abc
import typing

from lime_uow import resources, unit_of_work
from sqlalchemy import orm

from lime_etl.adapters import (
    batch_log_repository,
    batch_repository,
    job_log_repository,
    job_repository,
    timestamp_adapter,
)


class AdminUnitOfWork(unit_of_work.SqlAlchemyUnitOfWork, abc.ABC):
    def __enter__(self) -> AdminUnitOfWork:
        return typing.cast(AdminUnitOfWork, super().__enter__())

    def __exit__(self, *args) -> None:  # type: ignore
        super().__exit__(*args)

    @property
    @abc.abstractmethod
    def batch_repo(self) -> batch_repository.BatchRepository:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def batch_log_repo(self) -> batch_log_repository.BatchLogRepository:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def job_repo(self) -> job_repository.JobRepository:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def job_log_repo(self) -> job_log_repository.JobLogRepository:
        raise NotImplementedError

    # @property
    # @abc.abstractmethod
    # def ts_adapter(self) -> timestamp_adapter.TimestampAdapter:
    #     raise NotImplementedError


class SqlAlchemyAdminUnitOfWork(AdminUnitOfWork):
    def __init__(
        self,
        session_factory: orm.sessionmaker,
        ts_adapter: timestamp_adapter.TimestampAdapter,
    ):
        self._ts_adapter = ts_adapter
        super().__init__(session_factory)

    @property
    def batch_repo(self) -> batch_repository.BatchRepository:
        return self.get_resource(batch_repository.BatchRepository)  # type: ignore  # see mypy issue 5374

    @property
    def batch_log_repo(self) -> batch_log_repository.BatchLogRepository:
        return self.get_resource(batch_log_repository.BatchLogRepository)  # type: ignore  # see mypy issue 5374

    @property
    def job_repo(self) -> job_repository.JobRepository:
        return self.get_resource(job_repository.JobRepository)  # type: ignore  # see mypy issue 5374

    @property
    def job_log_repo(self) -> job_log_repository.JobLogRepository:
        return self.get_resource(job_log_repository.JobLogRepository)  # type: ignore  # see mypy issue 5374

    @property
    def ts_adapter(self) -> timestamp_adapter.TimestampAdapter:
        return self.get_resource(timestamp_adapter.TimestampAdapter)  # type: ignore  # see mypy issue 5374

    def create_resources(self) -> typing.Set[resources.Resource[typing.Any]]:
        return {
            batch_repository.SqlAlchemyBatchRepository(
                session=self.session, ts_adapter=self._ts_adapter
            ),
            batch_log_repository.SqlAlchemyBatchLogRepository(
                session=self.session, ts_adapter=self._ts_adapter
            ),
            job_repository.SqlAlchemyJobRepository(
                session=self.session, ts_adapter=self._ts_adapter
            ),
            job_log_repository.SqlAlchemyJobLogRepository(
                session=self.session, ts_adapter=self._ts_adapter
            )
        }
