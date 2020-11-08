from __future__ import annotations

import abc
import typing

import lime_uow as lu

from lime_etl.adapters import (
    admin_session,
    batch_log_repository,
    batch_repository,
    job_log_repository,
    job_repository,
    timestamp_adapter,
)

__all__ = (
    "AdminUnitOfWork",
    "SqlAlchemyAdminUnitOfWork",
)


class AdminUnitOfWork(lu.UnitOfWork, abc.ABC):
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

    @property
    @abc.abstractmethod
    def ts_adapter(self) -> timestamp_adapter.TimestampAdapter:
        raise NotImplementedError


class SqlAlchemyAdminUnitOfWork(AdminUnitOfWork):
    def __init__(
        self,
        shared_resources: lu.SharedResources,
        ts_adapter: timestamp_adapter.TimestampAdapter = timestamp_adapter.LocalTimestampAdapter(),
    ):
        self._ts_adapter = ts_adapter

        super().__init__(shared_resources)

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
        return self._ts_adapter

    def create_resources(
        self, shared_resources: lu.SharedResources
    ) -> typing.Set[lu.Resource[typing.Any]]:
        session = shared_resources.get(admin_session.SqlAlchemyAdminSession)
        return {
            batch_repository.SqlAlchemyBatchRepository(
                session=session, ts_adapter=self._ts_adapter
            ),
            batch_log_repository.SqlAlchemyBatchLogRepository(
                session=session, ts_adapter=self._ts_adapter
            ),
            job_repository.SqlAlchemyJobRepository(
                session=session, ts_adapter=self._ts_adapter
            ),
            job_log_repository.SqlAlchemyJobLogRepository(
                session=session, ts_adapter=self._ts_adapter
            ),
        }
