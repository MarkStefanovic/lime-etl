from __future__ import annotations

import abc
import typing

import lime_uow as lu
from sqlalchemy import orm

from lime_etl import domain, adapters

__all__ = (
    "AdminUnitOfWork",
    "SqlAlchemyAdminUnitOfWork",
)


class AdminUnitOfWork(lu.UnitOfWork, abc.ABC):
    @property
    @abc.abstractmethod
    def batch_repo(self) -> adapters.BatchRepository:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def batch_log_repo(self) -> adapters.BatchLogRepository:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def job_repo(self) -> adapters.JobRepository:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def job_log_repo(self) -> adapters.JobLogRepository:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def ts_adapter(self) -> domain.TimestampAdapter:
        raise NotImplementedError


class SqlAlchemyAdminUnitOfWork(AdminUnitOfWork):

    def __init__(
        self,
        session_factory: orm.sessionmaker,
        ts_adapter: domain.TimestampAdapter = adapters.LocalTimestampAdapter(),
    ):
        super().__init__()
        self._session_factory = session_factory
        self._ts_adapter = ts_adapter

    @property
    def batch_repo(self) -> adapters.BatchRepository:
        return self.get(adapters.BatchRepository)  # type: ignore  # see mypy issue 5374

    @property
    def batch_log_repo(self) -> adapters.BatchLogRepository:
        return self.get(adapters.BatchLogRepository)  # type: ignore  # see mypy issue 5374

    def create_shared_resources(self) -> typing.List[lu.Resource[typing.Any]]:
        return [adapters.SqlAlchemyAdminSession(self._session_factory)]

    @property
    def job_repo(self) -> adapters.JobRepository:
        return self.get(adapters.JobRepository)  # type: ignore  # see mypy issue 5374

    @property
    def job_log_repo(self) -> adapters.JobLogRepository:
        return self.get(adapters.JobLogRepository)  # type: ignore  # see mypy issue 5374

    @property
    def ts_adapter(self) -> domain.TimestampAdapter:
        return self._ts_adapter

    def create_resources(
        self, shared_resources: lu.SharedResources
    ) -> typing.Set[lu.Resource[typing.Any]]:
        session = shared_resources.get(adapters.SqlAlchemyAdminSession)
        return {
            adapters.SqlAlchemyBatchRepository(
                session=session, ts_adapter=self._ts_adapter
            ),
            adapters.SqlAlchemyBatchLogRepository(
                session=session, ts_adapter=self._ts_adapter
            ),
            adapters.SqlAlchemyJobRepository(
                session=session, ts_adapter=self._ts_adapter
            ),
            adapters.SqlAlchemyJobLogRepository(
                session=session, ts_adapter=self._ts_adapter
            ),
        }
