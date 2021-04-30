import typing

import sqlalchemy as sa
from sqlalchemy import orm

from lime_etl import adapter, domain
from lime_etl.service import admin, batch_runner

__all__ = ("AdminBatch",)


class AdminBatch(domain.BatchSpec[domain.Config, domain.AdminUnitOfWork]):
    def __init__(
        self,
        *,
        config: domain.Config,
        min_seconds_between_runs: domain.MinSecondsBetweenRefreshes = domain.MinSecondsBetweenRefreshes(
            12 * 60 * 60
        ),
        ts_adapter: domain.TimestampAdapter = domain.LocalTimestampAdapter(),
    ):
        self._config = config
        self._min_seconds_between_runs = min_seconds_between_runs
        self._ts_adapter = ts_adapter

    @property
    def batch_name(self) -> domain.BatchName:
        return domain.BatchName("admin")

    def create_jobs(
        self, uow: domain.AdminUnitOfWork
    ) -> typing.List[domain.JobSpec[domain.AdminUnitOfWork]]:
        return [
            admin.delete_old_logs.DeleteOldLogs(
                days_logs_to_keep=self._config.days_logs_to_keep,
                min_seconds_between_runs=self._min_seconds_between_runs,
            ),
        ]

    def create_uow(self, config: domain.Config) -> domain.AdminUnitOfWork:
        admin_engine = sa.create_engine(self._config.admin_engine_uri.value)
        adapter.admin_metadata.create_all(bind=admin_engine)
        adapter.admin_orm.set_schema(schema=self._config.admin_schema)
        adapter.admin_orm.start_mappers()
        admin_session_factory = orm.sessionmaker(bind=admin_engine)
        return adapter.SqlAlchemyAdminUnitOfWork(
            session_factory=admin_session_factory, ts_adapter=self._ts_adapter
        )

    def run(self) -> domain.BatchStatus:
        return batch_runner.run_batch(
            config=self._config,
            batch=self,
            ts_adapter=self._ts_adapter,
        )
