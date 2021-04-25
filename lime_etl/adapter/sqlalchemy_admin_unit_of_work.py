from __future__ import annotations

import typing

import lime_uow as lu
from sqlalchemy import orm

from lime_etl import domain
from lime_etl.adapter import (
    local_timestamp_adapter,
    sqlalchemy_admin_session,
    sqlalchemy_batch_repository,
    sqlalchemy_batch_log_repository,
    sqlalchemy_job_log_repository,
    sqlalchemy_job_repository,
)

__all__ = ("SqlAlchemyAdminUnitOfWork",)


class SqlAlchemyAdminUnitOfWork(domain.AdminUnitOfWork):
    def __init__(
        self,
        session_factory: orm.sessionmaker,
        ts_adapter: domain.TimestampAdapter = local_timestamp_adapter.LocalTimestampAdapter(),
    ):
        super().__init__()
        self._session_factory = session_factory
        self._ts_adapter = ts_adapter

    @property
    def batch_repo(self) -> domain.BatchRepository:
        return self.get(domain.BatchRepository)  # type: ignore  # see mypy issue 5374

    @property
    def batch_log_repo(self) -> domain.BatchLogRepository:
        return self.get(domain.BatchLogRepository)  # type: ignore  # see mypy issue 5374

    def create_shared_resources(self) -> typing.List[lu.Resource[typing.Any]]:
        return [sqlalchemy_admin_session.SqlAlchemyAdminSession(self._session_factory)]

    @property
    def job_repo(self) -> domain.JobRepository:
        return self.get(domain.JobRepository)  # type: ignore  # see mypy issue 5374

    @property
    def job_log_repo(self) -> domain.JobLogRepository:
        return self.get(domain.JobLogRepository)  # type: ignore  # see mypy issue 5374

    @property
    def ts_adapter(self) -> domain.TimestampAdapter:
        return self._ts_adapter

    def create_resources(
        self, shared_resources: lu.SharedResourceManager
    ) -> typing.Set[lu.Resource[typing.Any]]:
        session = shared_resources.get(sqlalchemy_admin_session.SqlAlchemyAdminSession)
        return {
            sqlalchemy_batch_repository.SqlAlchemyBatchRepository(
                session=session, ts_adapter=self._ts_adapter
            ),
            sqlalchemy_batch_log_repository.SqlAlchemyBatchLogRepository(
                session=session, ts_adapter=self._ts_adapter
            ),
            sqlalchemy_job_repository.SqlAlchemyJobRepository(
                session=session, ts_adapter=self._ts_adapter
            ),
            sqlalchemy_job_log_repository.SqlAlchemyJobLogRepository(
                session=session, ts_adapter=self._ts_adapter
            ),
        }
